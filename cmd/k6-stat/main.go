package main

import (
	"log"
	"os"

	"github.com/rs/zerolog"

	app "github.com/msaf1980/k6-stat/app/k6-stat"
	"github.com/msaf1980/k6-stat/utils/env"
)

var (
	listen       string
	dbDSN        string
	maxConn      int
	tableTests   string
	tableSamples string
)

func init() {
	listen = env.GetEnv("K6_STAT_LISTEN", ":8080")

	chAddress := env.GetEnv("K6_STAT_DB_ADDR", "http://localhost:8123")
	chDB := env.GetEnv("K6_STAT_DB", "default")
	chPparam := env.GetEnv("K6_STAT_DB_PARAM", "dial_timeout=200ms&max_execution_time=60")
	dbDSN = chAddress + "/" + chDB + "?" + chPparam

	maxConn, _ = env.GetEnvInt("K6_STAT_DB_MAX_CONN", 10)
	if maxConn <= 0 {
		panic("invalid max connections")
	}
	tableTests = env.GetEnv("K6_STAT_TABLE_TESTS", "k6_tests")
	tableSamples = env.GetEnv("K6_STAT_TABLE_SAMPLES", "k6_samples")
}

func main() {
	logger := zerolog.New(os.Stdout)
	app, err := app.New(dbDSN, maxConn, &logger, tableTests, tableSamples)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(app.Listen(listen))
}
