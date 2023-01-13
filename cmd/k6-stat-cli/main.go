package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/msaf1980/go-clipper"
	"github.com/peterh/liner"

	"github.com/msaf1980/k6-stat/dbs"
	"github.com/msaf1980/k6-stat/utils/env"
)

var (
	db          *dbs.DB
	testsFilter dbs.TestFilter
	testsHead   = headLine(9 + 19 + 30 + 45 + 15)
)

func headLine(n int) string {
	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, '-')
	}
	return string(out)
}

func printTests(db *dbs.DB, f dbs.TestFilter) error {
	tests, err := db.GetTests(f)
	if err != nil {
		return err
	}
	fmt.Printf("%9s | %19s | %30s | %45s | %s\n", "N", "Id", "Ts", "Name", "Params")
	fmt.Println(testsHead)
	for i, t := range tests {
		fmt.Printf("%9d | %19d | %30s | %45s | %s\n", i, t.Id, t.Ts.Format(time.RFC3339Nano), t.Name, t.Params)
	}
	return nil
}

func main() {
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	chAddress := env.GetEnv("K6_STAT_DB_ADDR", "http://localhost:8123")
	chDB := env.GetEnv("K6_STAT_DB", "default")
	chPparam := env.GetEnv("K6_STAT_DB_PARAM", "dial_timeout=200ms&max_execution_time=60")
	dsn := chAddress + "/" + chDB + "?" + chPparam
	tableTests := env.GetEnv("K6_STAT_TABLE_TESTS", "k6_tests")
	tableSamples := env.GetEnv("K6_STAT_TABLE_SAMPLES", "k6_samples")

	if d, err := sql.Open("clickhouse", dsn); err == nil {
		d.SetMaxIdleConns(1)
		d.SetMaxOpenConns(3)
		d.SetConnMaxIdleTime(time.Hour)
		db = dbs.New(d, tableTests, tableSamples)
	} else {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	timeLayout := "2006-01-02T15:04:05"
	now := time.Now().UTC()

	registry := clipper.NewRegistry("CLI for display xk6-output-clickhouse tests")

	var (
		testsFrom  time.Time
		testsUntil time.Time
	)

	registry.Register("", "")

	registry.Register("help", "Print help")

	testsCommand, _ := registry.Register("tests", "Print tests")
	testsCommand.AddTimeFromString("from", "f", now.Format(timeLayout), &testsFrom, timeLayout, "Select tests started after")
	testsCommand.AddTimeFromString("until", "u", now.Add(time.Hour*24).Format(timeLayout), &testsUntil, timeLayout, "Select tests started before")
	testsCommand.AddString("prefix", "p", "", &testsFilter.NamePrefix, "Tests filter")

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	line.SetCompleter(func(line string) []string {
		return registry.CompleterAppended(line)
	})

	for {
		cline, err := line.Prompt("k6-stat>")
		line.AppendHistory(cline)
		if err == nil {
			if cline == "exit" || cline == "quit" {
				break
			}
			if cline == "help" {
				clipper.PrintHelp(registry, "", registry.Commands[""], false)
			} else {
				args := clipper.SplitQuoted(cline)
				command, helpRequested, err := registry.ParseInteract(args, false)
				if err != nil {
					fmt.Fprintln(os.Stderr, "Error: ", err)
				} else if !helpRequested {
					// execute command
					switch command {
					case "tests":
						testsFilter.From = testsFrom.Unix()
						testsFilter.Until = testsUntil.Unix()
						if err := printTests(db, testsFilter); err != nil {
							fmt.Fprintf(os.Stderr, "Error: %v\n", err)
						}
					case "":
					default:
						fmt.Fprintf(os.Stderr, "Error: command %q not handled\n", cline)
					}
				}
			}
		} else if err == liner.ErrPromptAborted {
			fmt.Fprintln(os.Stderr, "Aborted.")
			break
		} else {
			fmt.Fprintln(os.Stderr, "Error reading line: ", err)
			break
		}
	}
}
