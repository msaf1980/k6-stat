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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog"

	app "github.com/msaf1980/k6-stat/app/k6-stat"
	"github.com/msaf1980/k6-stat/dbs"
	"github.com/msaf1980/k6-stat/utils/env"
)

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func diffSamplesQuantiles(expected, actual []dbs.SampleQuantiles) string {
	maxLen := max(len(expected), len(actual))
	var sb strings.Builder
	sb.Grow(1024)
	for i := 0; i < maxLen; i++ {
		if i >= len(expected) {
			sb.WriteString(fmt.Sprintf("+ [%d] = %+v\n", i, actual[i]))
		} else if i >= len(actual) {
			sb.WriteString(fmt.Sprintf("- [%d] = %+v\n", i, expected[i]))
		} else if !reflect.DeepEqual(actual[i], expected[i]) {
			sb.WriteString(fmt.Sprintf("- [%d] = %+v\n", i, expected[i]))
			sb.WriteString(fmt.Sprintf("+ [%d] = %+v\n", i, actual[i]))
		}
	}
	return sb.String()
}

func diffSamplesStatus(expected, actual []dbs.SampleStatus) string {
	maxLen := max(len(expected), len(actual))
	var sb strings.Builder
	sb.Grow(1024)
	for i := 0; i < maxLen; i++ {
		if i >= len(expected) {
			sb.WriteString(fmt.Sprintf("+ [%d] = %+v\n", i, actual[i]))
		} else if i >= len(actual) {
			sb.WriteString(fmt.Sprintf("- [%d] = %+v\n", i, expected[i]))
		} else if !reflect.DeepEqual(actual[i], expected[i]) {
			sb.WriteString(fmt.Sprintf("- [%d] = %+v\n", i, expected[i]))
			sb.WriteString(fmt.Sprintf("+ [%d] = %+v\n", i, actual[i]))
		}
	}
	return sb.String()
}

var (
	t1, t2, t3          time.Time
	test1, test2, test3 dbs.Test

	samples1_1_d, samples1_2_d, samples1_3_d, samples1_4_d dbs.Sample
	samples1_1_r, samples1_2_r, samples1_3_r, samples1_4_r dbs.Sample

	samples2_1_d dbs.Sample
	samples2_1_r dbs.Sample

	samples3_1_d, samples3_2_d dbs.Sample
	samples3_1_r, samples3_2_r dbs.Sample

	dbDSN string
)

func init() {
	t1, _ = time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	t1 = t1.UTC()
	test1 = dbs.Test{Id: uint64(t1.UnixNano()), Ts: t1, Name: "graphite-clickhouse 2006-01-02T15:04:05Z", Params: "RENDER_FORMAT=carbonapi_v3_pb FIND_FORMAT=carbonapi_v3_pb DELAY=1 DURATION=1h USERS_FIND=1 USERS_TAGS=1 USERS_1H_0=1"}
	t2, _ = time.Parse(time.RFC3339, "2006-01-03T15:04:05Z")
	t2 = t2.UTC().Add(time.Nanosecond)
	test2 = dbs.Test{Id: uint64(t2.UnixNano()), Ts: t2, Name: "graphite-clickhouse 2006-01-03T15:04:05Z", Params: "RENDER_FORMAT=carbonapi_v3_pb FIND_FORMAT=carbonapi_v3_pb DELAY=1 DURATION=1h USERS_FIND=2 USERS_TAGS=2 USERS_1H_0=2"}
	t3, _ = time.Parse(time.RFC3339, "2006-01-04T15:04:05Z")
	t3 = t3.UTC()
	test3 = dbs.Test{Id: uint64(t3.UnixNano()), Ts: t3, Name: "graphite-clickhouse 2006-01-04T15:04:05Z", Params: "RENDER_FORMAT=carbonapi_v3_pb FIND_FORMAT=carbonapi_v3_pb DELAY=1 DURATION=1h USERS_FIND=2 USERS_TAGS=2 USERS_1H_0=2"}

	samples1_1_d = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(time.Millisecond),
		Metric: "http_req_duration",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs_duration;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 0.8,
	}
	samples1_1_r = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(time.Millisecond),
		Metric: "http_reqs",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 1,
	}

	samples1_2_d = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts.Add(10 * time.Second),
		Ts:     test1.Ts.Add(10 * time.Second),
		Metric: "http_req_duration",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs_duration;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 0.4,
	}
	samples1_2_r = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts.Add(10 * time.Second),
		Ts:     test1.Ts.Add(10 * time.Second),
		Metric: "http_reqs",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 1,
	}

	samples1_3_d = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(20 * time.Millisecond),
		Metric: "http_req_duration",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs_duration;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 0.2,
	}
	samples1_3_r = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(20 * time.Millisecond),
		Metric: "http_reqs",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 1,
	}

	samples1_4_d = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(30 * time.Millisecond),
		Metric: "http_req_duration",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs_duration;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 0.2,
	}
	samples1_4_r = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(30 * time.Millisecond),
		Metric: "http_reqs",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "400",
		Name:   "http_reqs;proto=HTTP/1.1;method=POST;group=;status=400;expected_response=false;error_code=1400;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "400", "expected_response": "false", "error_code": "1400",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 1,
	}

	samples2_1_d = dbs.Sample{
		Id:     test2.Id,
		Start:  test2.Ts,
		Ts:     test2.Ts.Add(time.Millisecond),
		Metric: "http_req_duration",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "200",
		Name:   "http_reqs_duration;proto=HTTP/1.1;method=POST;group=;status=200;expected_response=true;error_code=0;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "200", "expected_response": "true", "error_code": "0",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 0.8,
	}
	samples2_1_r = dbs.Sample{
		Id:     test2.Id,
		Start:  test2.Ts,
		Ts:     test2.Ts.Add(time.Millisecond),
		Metric: "http_reqs",
		Url:    "render format=carbonapi_v3_pb target=a.*",
		Label:  "render_1h_offset_0",
		Status: "200",
		Name:   "http_reqs;proto=HTTP/1.1;method=POST;group=;status=200;expected_response=true;error_code=0;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "200", "expected_response": "false", "error_code": "0",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=a.* label=render_1h_offset_0",
		},
		Value: 1,
	}

	samples3_1_d = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(time.Millisecond),
		Metric: "http_req_duration",
		Url:    "render format=carbonapi_v3_pb target=b.*",
		Label:  "render_1h_offset_0",
		Status: "200",
		Name:   "http_reqs_duration;proto=HTTP/1.1;method=POST;group=;status=200;expected_response=true;error_code=0;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "200", "expected_response": "true", "error_code": "0",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		},
		Value: 0.4,
	}
	samples3_1_r = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(time.Millisecond),
		Metric: "http_reqs",
		Url:    "render format=carbonapi_v3_pb target=b.*",
		Label:  "render_1h_offset_0",
		Status: "200",
		Name:   "http_reqs;proto=HTTP/1.1;method=POST;group=;status=200;expected_response=true;error_code=1200;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "200", "expected_response": "false", "error_code": "0",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		},
		Value: 1,
	}
	samples3_2_d = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(time.Second),
		Metric: "http_req_duration",
		Url:    "render format=carbonapi_v3_pb target=b.*",
		Label:  "render_1h_offset_0",
		Status: "200",
		Name:   "http_reqs_duration;proto=HTTP/1.1;method=POST;group=;status=200;expected_response=true;error_code=0;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "200", "expected_response": "true", "error_code": "0",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		},
		Value: 0.2,
	}
	samples3_2_r = dbs.Sample{
		Id:     test1.Id,
		Start:  test1.Ts,
		Ts:     test1.Ts.Add(time.Second),
		Metric: "http_reqs",
		Url:    "render format=carbonapi_v3_pb target=b.*",
		Label:  "render_1h_offset_0",
		Status: "200",
		Name:   "http_reqs;proto=HTTP/1.1;method=POST;group=;status=200;expected_response=true;error_code=1200;scenario=render_1h_offset_0;label=render_1h_offset_0;url=render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		Tags: map[string]string{
			"proto": "HTTP/1.1", "method": "POST", "group": "", "status": "200", "expected_response": "false", "error_code": "0",
			"scenario": "render_1h_offset_0", "label": "render_1h_offset_0",
			"url":  "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
			"name": "render format=carbonapi_v3_pb target=b.* label=render_1h_offset_0",
		},
		Value: 1,
	}

	dbInit()
}

func dbInit() {
	chAddress := env.GetEnv("K6_STAT_DB_ADDR", "http://localhost:8123")
	chDB := env.GetEnv("K6_STAT_DB", "default")
	chPparam := env.GetEnv("K6_STAT_DB_PARAM", "dial_timeout=200ms&max_execution_time=60")
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
			id UInt64,
			start DateTime64(9, 'UTC'),
			ts DateTime64(9, 'UTC'),
			metric String,
			url String,
			label String,
			status String,
			name String,
			tags Map(String, String),
			value Float64
		) ENGINE = ReplacingMergeTree(start)
		PARTITION BY toYYYYMM(ts)
		ORDER BY (id, ts, metric, name);`,
		`CREATE TABLE t_k6_tests (
			id UInt64,
			ts DateTime64(9, 'UTC'),
			name String,
			params String
		) ENGINE = ReplacingMergeTree(id)
		PARTITION BY toYYYYMM(ts)
		ORDER BY (id, ts, name);`,
	}
	for _, s := range schema {
		_, err = db.Exec(s)
		if err != nil {
			panic(err)
		}
	}

	// tests
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO t_k6_tests (id, ts, name, params)`)
	if err != nil {
		panic(err)
	}
	tests := []dbs.Test{test1, test2, test3}
	for _, test := range tests {
		if _, err = stmt.Exec(test.Id, test.Ts, test.Name, test.Params); err != nil {
			tx.Rollback()
			panic(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	// samples
	tx, err = db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err = tx.Prepare(`INSERT INTO t_k6_samples (id, start, ts, metric, url, label, status, name, tags, value`)
	if err != nil {
		panic(err)
	}
	samples := []dbs.Sample{
		samples1_1_d, samples1_1_r, samples1_2_d, samples1_2_r, samples1_3_d, samples1_3_r, samples1_4_d, samples1_4_r,
		samples2_1_d, samples2_1_r,
		samples3_1_d, samples3_1_r, samples3_2_d, samples3_2_r,
	}
	for _, sample := range samples {
		_, err = stmt.Exec(
			sample.Id, sample.Start, sample.Ts, sample.Metric,
			sample.Url, sample.Label, sample.Status, sample.Name, sample.Tags, sample.Value,
		)
		if err != nil {
			tx.Rollback()
			panic(err)
		}
	}
	err = tx.Commit()
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
		filter      dbs.TestFilter
		contentType string
		wantStatus  int
		want        []dbs.Test
	}{
		{
			name:       "all",
			wantStatus: http.StatusOK,
			want:       []dbs.Test{test1, test2, test3},
		},
		{
			name:       t1.Format("2006-01-02T15:04:05Z") + " " + t3.Format("2006-01-02T15:04:05Z"),
			filter:     dbs.TestFilter{From: t1.Unix(), Until: t3.Unix()},
			wantStatus: http.StatusOK,
			want:       []dbs.Test{test1, test2},
		},
		{
			name:       "graphite-clickhouse 2006-01-03",
			filter:     dbs.TestFilter{From: t1.Unix(), Until: t3.Unix(), Name: "graphite-clickhouse 2006-01-03%"},
			wantStatus: http.StatusOK,
			want:       []dbs.Test{test2},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("[%d] %s", i, tt.name), func(t *testing.T) {
			var r io.Reader
			if tt.filter.From != 0 || tt.filter.Until != 0 || tt.filter.Name != "" {
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
				var tests []dbs.Test
				err = json.Unmarshal(body, &tests)
				if err != nil {
					t.Fatalf("/api/tests decode = %v", err)
				}
				if !reflect.DeepEqual(tt.want, tests) {
					t.Fatalf("/api/tests = %s", cmp.Diff(tt.want, tests))
				}
			}
		})
	}
}

func TestIntegrationAppSamplesStatus(t *testing.T) {
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
		filter      dbs.SampleFilter
		contentType string
		wantStatus  int
		want        []dbs.SampleStatus
	}{
		{
			name:       "test1 + test3",
			filter:     dbs.SampleFilter{Id: test1.Id, Start: test1.Ts.UnixNano()},
			wantStatus: http.StatusOK,
			want: []dbs.SampleStatus{
				{
					Id: test1.Id, Start: test1.Ts, Label: "render_1h_offset_0", Url: "render format=carbonapi_v3_pb target=a.*",
					Status: "400", Count: 3.0,
				},
				{
					Id: test1.Id, Start: test1.Ts, Label: "render_1h_offset_0", Url: "render format=carbonapi_v3_pb target=b.*",
					Status: "200", Count: 2.0,
				},
			},
		},
		{
			name:       "test2",
			filter:     dbs.SampleFilter{Id: test2.Id, Start: test2.Ts.UnixNano()},
			wantStatus: http.StatusOK,
			want: []dbs.SampleStatus{
				{
					Id: test2.Id, Start: test2.Ts, Label: "render_1h_offset_0", Url: "render format=carbonapi_v3_pb target=a.*",
					Status: "200", Count: 1.0,
				},
			},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("[%d] %s", i, tt.name), func(t *testing.T) {
			b, err := json.Marshal(tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			r := bytes.NewBuffer(b)

			req, err := http.NewRequest("POST", "http://"+address+"/api/test/http/status", r)
			if err != nil {
				t.Fatalf("http.NewRequest() error = %v", err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("/api/test/http/status error = %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("/api/test/http/status = %d (%s)", resp.StatusCode, string(body))
			}
			if resp.StatusCode == http.StatusOK {
				var samples []dbs.SampleStatus
				err = json.Unmarshal(body, &samples)
				if err != nil {
					t.Fatalf("/api/test/http/status decode = %v", err)
				}
				if diff := diffSamplesStatus(tt.want, samples); diff != "" {
					t.Errorf("samples status:\n%s", diff)
				}
			}
		})
	}
}
