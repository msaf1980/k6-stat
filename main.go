package main

import (
	"log"
	"os"
	"strconv"

	"github.com/msaf1980/k6-stat/app"
	"github.com/rs/zerolog"
)

var (
	listen       string
	dbDSN        string
	maxConn      int
	tableTests   string
	tableSamples string
)

func init() {
	listen = app.Getenv("K6_STAT_LISTEN", ":8080")

	chAddress := app.Getenv("K6_STAT_DB_ADDR", "http://localhost:8123")
	chDB := app.Getenv("K6_STAT_DB", "default")
	chPparam := app.Getenv("K6_STAT_DB_PARAM", "dial_timeout=200ms&max_execution_time=60")
	dbDSN = chAddress + "/" + chDB + "?" + chPparam

	maxConn, _ = strconv.Atoi(app.Getenv("K6_STAT_DB_MAX_CONN", "10"))
	if maxConn <= 0 {
		panic("invalid max connections")
	}
	tableTests = app.Getenv("K6_STAT_TABLE_TESTS", "k6_tests")
	tableSamples = app.Getenv("K6_STAT_TABLE_SAMPLES", "k6_samples")
}

func main() {
	logger := zerolog.New(os.Stdout)
	app, err := app.New(dbDSN, maxConn, &logger, tableTests, tableSamples)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(app.Listen(listen))
}
