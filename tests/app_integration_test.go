//go:build test_all || test_integration
// +build test_all test_integration

package tests

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/msaf1980/k6-stat/app"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

var (
	t1, t2, t3          time.Time
	test1, test2, test3 app.Test
	dbDSN               string
)

func init() {
	t1, _ = time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	t1 = t1.UTC()
	test1 = app.Test{Id: t1, Name: "graphite-clickhouse 2006-01-02T15:04:05Z", Params: "RENDER_FORMAT=carbonapi_v3_pb FIND_FORMAT=carbonapi_v3_pb DELAY=1 DURATION=1h USERS_FIND=1 USERS_TAGS=1 USERS_1H_0=1"}
	t2, _ = time.Parse(time.RFC3339, "2006-01-03T15:04:05Z")
	t2 = t2.UTC()
	test2 = app.Test{Id: t2, Name: "graphite-clickhouse 2006-01-03T15:04:05Z", Params: "RENDER_FORMAT=carbonapi_v3_pb FIND_FORMAT=carbonapi_v3_pb DELAY=1 DURATION=1h USERS_FIND=2 USERS_TAGS=2 USERS_1H_0=2"}
	t3, _ = time.Parse(time.RFC3339, "2006-01-04T15:04:05Z")
	t3 = t3.UTC()
	test3 = app.Test{Id: t3, Name: "graphite-clickhouse 2006-01-04T15:04:05Z", Params: "RENDER_FORMAT=carbonapi_v3_pb FIND_FORMAT=carbonapi_v3_pb DELAY=1 DURATION=1h USERS_FIND=2 USERS_TAGS=2 USERS_1H_0=2"}

	dbInit()
}

func dbInit() {
	chAddress := app.Getenv("K6_STAT_DB_ADDR", "http://localhost:8123")
	chDB := app.Getenv("K6_STAT_DB", "default")
	chPparam := app.Getenv("K6_STAT_DB_PARAM", "dial_timeout=200ms&max_execution_time=60")
	dbDSN = chAddress + "/" + chDB + "?" + chPparam

	db, err := sql.Open("clickhouse", dbDSN)
	if err != nil {
		panic(err)
	}

	// See https://github.com/msaf1980/xk6-output-clickhouse
	var schema = []string{
		`DROP TABLE IF EXISTS t_k6_samples`,
		`DROP TABLE IF EXISTS t_k6_tests`,
		`CREATE TABLE t_k6_samples (
			id DateTime64(9, 'UTC'),
			ts DateTime64(9, 'UTC'),
			metric String,
			name String,
			tags Map(String, String),
			value Float64,
			version DateTime64(9, 'UTC')
		) ENGINE = ReplacingMergeTree(version)
		PARTITION BY toYYYYMM(id)
		ORDER BY (id, ts, metric, name);`,
		`CREATE TABLE t_k6_tests (
			id DateTime64(9, 'UTC'),
			name String,
			params String
		) ENGINE = ReplacingMergeTree(id)
		PARTITION BY toYYYYMM(id)
		ORDER BY (name, id);`,
	}
	for _, s := range schema {
		_, err = db.Exec(s)
		if err != nil {
			panic(err)
		}
	}

	scope, err := db.Begin()
	if err != nil {
		panic(err)
	}
	batch, err := scope.Prepare(`INSERT INTO t_k6_tests (id, name, params)`)
	if err != nil {
		panic(err)
	}
	tests := []app.Test{test1, test2, test3}
	for _, test := range tests {
		if _, err = batch.Exec(test.Id, test.Name, test.Params); err != nil {
			scope.Rollback()
			panic(err)
		}
	}
	err = scope.Commit()
	if err != nil {
		panic(err)
	}
}

func TestIntegrationAppTests(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	statApp, err := app.New(dbDSN, 2, &logger, "t_k6_tests", "t_k6_samples")
	if err != nil {
		log.Fatal(err)
	}

	address := "127.0.0.1:8081"
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		statApp.Listen(address)
	}()
	wg.Wait()
	defer statApp.Shutdown()
	time.Sleep(time.Millisecond * 10)

	tests := []struct {
		name        string
		filter      app.TestFilter
		contentType string
		wantStatus  int
		want        []app.Test
	}{
		{
			name:       "all",
			wantStatus: http.StatusOK,
			want:       []app.Test{test1, test2, test3},
		},
		{
			name:       t1.Format("2006-01-02T15:04:05Z") + " " + t3.Format("2006-01-02T15:04:05Z"),
			filter:     app.TestFilter{From: t1.Unix(), Until: t3.Unix()},
			wantStatus: http.StatusOK,
			want:       []app.Test{test1, test2},
		},
		{
			name:       "graphite-clickhouse 2006-01-03",
			filter:     app.TestFilter{From: t1.Unix(), Until: t3.Unix(), NamePrefix: "graphite-clickhouse 2006-01-03"},
			wantStatus: http.StatusOK,
			want:       []app.Test{test2},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("[%d] %s", i, tt.name), func(t *testing.T) {
			var r io.Reader
			if tt.filter.From != 0 || tt.filter.Until != 0 || tt.filter.NamePrefix != "" {
				b, err := json.Marshal(tt.filter)
				if err != nil {
					t.Fatal(err)
				}
				r = bytes.NewBuffer(b)
			}

			req, err := http.NewRequest("POST", "http://"+address+"/api/tests", r)
			if err != nil {
				t.Fatalf("http.NewRequest() error = %v", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("/api/tests error = %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("/api/tests = %d (%s)", resp.StatusCode, string(body))
			}
			if resp.StatusCode == http.StatusOK {
				var tests []app.Test
				err = json.Unmarshal(body, &tests)
				if err != nil {
					t.Fatalf("/api/tests decode = %v", err)
				}
				assert.Equal(t, tt.want, tests)
			}
		})
	}
}
