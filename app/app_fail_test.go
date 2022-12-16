//go:build !test_integration
// +build !test_integration

package app

import (
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog"
)

func TestUnitAppFail(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	app, err := New("http://localhost:8125/default?dial_timeout=200ms&max_execution_time=60", 10, &logger, "t_k6_tests", "t_k6_samples")
	if err != nil {
		t.Fatalf("NewApp() error = %v", err)
	}

	address := "127.0.0.1:8081"

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		app.Listen(address)
	}()
	wg.Wait()
	defer app.Shutdown()
	time.Sleep(time.Millisecond * 10)

	req, err := http.NewRequest("POST", "http://"+address+"/api/tests", nil)
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do(/api/tests) error = %v", err)
	} else if resp.StatusCode != http.StatusServiceUnavailable {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Do(/api/tests) = %d (%s)", resp.StatusCode, string(body))
	}
}
