//go:build !test_integration
// +build !test_integration

package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

var (
	t1, t2, t3                 time.Time
	test1, test2, test3        Test
	allRows, timeRows, gchRows *sqlmock.Rows
)

func init() {
	t1, _ = time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	test1 = Test{Id: t1, Name: "graphite-clickhouse 1", Params: "1"}
	t2, _ = time.Parse(time.RFC3339, "2006-01-02T18:04:05Z")
	test2 = Test{Id: t2, Name: "carbonapi 1", Params: "1"}
	t3, _ = time.Parse(time.RFC3339, "2006-01-03T15:04:05Z")
	test3 = Test{Id: t3, Name: "graphite-clickhouse 2", Params: "2"}

	allRows = sqlmock.NewRows([]string{"id", "name", "params"}).
		AddRow(test1.Id, test1.Name, test1.Params).
		AddRow(test2.Id, test2.Name, test2.Params).
		AddRow(test3.Id, test3.Name, test3.Params)

	timeRows = sqlmock.NewRows([]string{"id", "name", "params"}).
		AddRow(test1.Id, test1.Name, test1.Params).
		AddRow(test2.Id, test2.Name, test2.Params)

	gchRows = sqlmock.NewRows([]string{"id", "name", "params"}).
		AddRow(test1.Id, test1.Name, test1.Params).
		AddRow(test3.Id, test3.Name, test3.Params)
}

func newMockApp(sqlRegex string, rows *sqlmock.Rows, logger *zerolog.Logger) (*App, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}

	mock.ExpectQuery(sqlRegex).WillReturnRows(rows)

	return newApp(db, logger, "t_k6_tests", "t_k6_samples")
}

func TestUnitAppTests(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	tests := []struct {
		sqlRegex    string
		rows        *sqlmock.Rows
		contentType string
		params      string
		wantStatus  int
		want        []Test
	}{
		{
			sqlRegex:   `^SELECT id, name, params FROM t_k6_tests ORDER BY id, name$`,
			rows:       allRows,
			wantStatus: http.StatusOK,
			want:       []Test{test1, test2, test3},
		},
		{
			sqlRegex:    `^SELECT id, name, params FROM t_k6_tests WHERE id >= \? AND id < \? ORDER BY id, name$`,
			rows:        timeRows,
			contentType: "application/json",
			params:      `{ "from": 1, "until": 2}`,
			wantStatus:  http.StatusOK,
			want:        []Test{test1, test2},
		},
		{
			sqlRegex:    `^SELECT id, name, params FROM t_k6_tests WHERE id >= \? AND id < \? AND name LIKE \? ORDER BY id, name$`,
			rows:        gchRows,
			contentType: "application/json",
			params:      `{ "from": 1, "until": 2, "name_prefix": "graphite-clickhouse"}`,
			wantStatus:  http.StatusOK,
			want:        []Test{test1, test3},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("[%d] %s", i, tt.sqlRegex), func(t *testing.T) {
			app, err := newMockApp(tt.sqlRegex, tt.rows, &logger)
			if err != nil {
				t.Fatalf("newMockApp() error = %v", err)
			}

			address := "127.0.0.1:8081"

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				wg.Done()
				_ = app.Listen(address)
			}()
			wg.Wait()
			defer func() { _ = app.Shutdown() }()
			time.Sleep(time.Millisecond * 10)

			var r io.Reader
			if tt.params != "" {
				r = strings.NewReader(tt.params)
			}
			req, err := http.NewRequest("POST", "http://"+address+"/api/tests", r)
			if err != nil {
				t.Fatalf("http.NewRequest() error = %v", err)
			}
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("/api/tests error = %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("/api/tests = %d (%s)", resp.StatusCode, string(body))
			}
			if resp.StatusCode == http.StatusOK {
				var tests []Test
				err = json.Unmarshal(body, &tests)
				if err != nil {
					t.Fatalf("/api/tests decode = %v", err)
				}
				assert.Equal(t, tt.want, tests)
			}
		})
	}
}
