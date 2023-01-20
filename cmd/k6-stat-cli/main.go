package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/msaf1980/go-clipper"
	"github.com/peterh/liner"

	"github.com/msaf1980/k6-stat/dbs"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	db *dbs.DB

	testsHead   = headLine(9 + 19 + 30 + 45 + 18)
	topHead     = headLine(9*8 + 16)
	topDiffHead = headLine(20*8 + 14)
)

func headLine(n int) string {
	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, '-')
	}
	return string(out)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func printTests(w io.Writer, tests []dbs.Test) {
	fmt.Fprintf(w, "%9s | %19s | %30s | %s\n%s\n", "N", "Id", "Ts", "Name", "Params")
	fmt.Fprintln(w, testsHead)
	for i, t := range tests {
		fmt.Fprintf(w, "%9d | %19d | %30s | %s\n%s\n", i, t.Id, t.Ts.Format(time.RFC3339Nano), t.Name, t.Params)
	}
}

func printTest(w io.Writer, tests []dbs.Test, n int, descr string, head bool) (err error) {
	if head {
		if _, err = fmt.Fprintf(w, "%9s | %19s | %30s | %45s | %s\n",
			"N", "Id", "Ts", "Name", "Params"); err != nil {
			return
		}
		if _, err = fmt.Fprintln(w, testsHead); err != nil {
			return
		}
	}
	_, err = fmt.Fprintf(w, "%9s | %19d | %30s | %45s | %s\n",
		descr, tests[n].Id, tests[n].Ts.Format(time.RFC3339Nano), tests[n].Name, tests[n].Params,
	)

	return
}

func printHttpTop(w io.Writer, samplesDurations map[string][]dbs.SampleDurations, topNum int) (err error) {
	labels := make([]string, 0, len(samplesDurations))
	for k := range samplesDurations {
		labels = append(labels, k)
	}
	sort.Strings(labels)

	for _, label := range labels {
		durations := samplesDurations[label]
		if _, err = fmt.Fprintf(w, "\nLabel: %q, %d urls\n%s\n", label, len(durations), topHead); err != nil {
			return
		}
		if _, err = fmt.Fprintf(w, "%9s | %9s | %9s | %9s | %9s | %9s | %6s | %s\n%s\n",
			"P50", "P90", "P95", "P99", "Max", "Count", "Err%", "Status%", topHead); err != nil {
			return
		}
		n := min(topNum, len(durations))
		for i := 0; i < n; i++ {
			d := durations[i]
			if _, err = fmt.Fprintf(w, "%s\n%9.2f | %9.2f | %9.2f | %9.2f | %9.2f | %9.0f | %6.2f",
				d.Url, d.P50, d.P90, d.P95, d.P99, d.Max, d.Count, d.ErrorsPcnt); err != nil {
				return
			}
			if len(d.Status) > 0 {
				if _, err = fmt.Fprint(w, " |"); err != nil {
					return
				}
				if n := d.Status["200"]; n > 0 {
					if _, err = fmt.Fprintf(w, " 200: %.2f", n/d.Count*100); err != nil {
						return
					}
				} else {
					if _, err = fmt.Fprint(w, " 200: 0"); err != nil {
						return
					}
				}
				if n := d.Status["400"]; n > 0 {
					if _, err = fmt.Fprintf(w, ", 400: %.2f", n/d.Count*100); err != nil {
						return
					}
				}
				if n := d.Status["404"]; n > 0 {
					if _, err = fmt.Fprintf(w, ", 404: %.2f", n/d.Count*100); err != nil {
						return
					}
				}
				for k, v := range d.Status {
					if k != "200" && k != "400" && k != "404" {
						if _, err = fmt.Fprintf(w, ", %s: %.2f", k, v/d.Count*100); err != nil {
							return
						}
					}
				}
			}
			if _, err = fmt.Fprintln(w); err != nil {
				return
			}
		}
	}
	return
}

func countDiffString(count, countDiff float64) string {
	return fmt.Sprintf("%.0f (%.0f)", count, countDiff)
}

func diffString(v, vDiff float64) string {
	return fmt.Sprintf("%.2f (%.2f)", v, vDiff)
}

func printHttpTopDiff(w io.Writer, samplesDurations map[string][]dbs.SampleDurationsDiff, topNum int) (err error) {
	labels := make([]string, 0, len(samplesDurations))
	for k := range samplesDurations {
		labels = append(labels, k)
	}
	sort.Strings(labels)

	for _, label := range labels {
		durations := samplesDurations[label]
		if _, err = fmt.Fprintf(w, "\nLabel: %q, %d urls\n%s\n", label, len(durations), topDiffHead); err != nil {
			return
		}
		if _, err = fmt.Fprintf(w, "%20s | %20s | %20s | %20s | %20s | %20s | %14s | %s\n%s\n",
			"Url P50 (Diff)", "P90 (Diff)", "P95 (Diff)", "P99 (Diff)", "Max (Diff)",
			"Count (Diff)", "Err% (Diff)", "Status% (Reference)", topDiffHead,
		); err != nil {
			return
		}
		n := min(topNum, len(durations))
		for i := 0; i < n; i++ {
			d := durations[i]
			if _, err = fmt.Fprintf(w, "%s\n%20s | %20s | %20s | %20s | %20s | %20s | %14s",
				d.Url, diffString(d.P50, d.P50Diff), diffString(d.P90, d.P90Diff),
				diffString(d.P95, d.P95Diff), diffString(d.P99, d.P99Diff),
				diffString(d.Max, d.MaxDiff),
				countDiffString(d.Count, d.CountDiff), diffString(d.ErrorsPcnt, d.ErrorsPcntDiff),
			); err != nil {
				return
			}
			if _, err = fmt.Fprint(w, " |"); err != nil {
				return
			}
			if len(d.Status) > 0 {
				refCount := d.Count - d.CountDiff
				if v := d.Status["200"]; v > 0 {
					refV := (v - d.StatusDiff["200"]) / refCount * 100
					if _, err = fmt.Fprintf(w, " 200: %s", diffString(v/d.Count*100, refV)); err != nil {
						return
					}
				} else {
					refV := (v - d.StatusDiff["200"]) / refCount * 100
					if _, err = fmt.Fprintf(w, " 200: %s", diffString(0, refV)); err != nil {
						return
					}
				}
				if v := d.Status["400"]; v > 0 {
					refV := (v - d.StatusDiff["400"]) / refCount * 100
					if _, err = fmt.Fprintf(w, ", 400: %s", diffString(v/d.Count*100, refV)); err != nil {
						return
					}
				}
				if v := d.Status["404"]; v > 0 {
					refV := (v - d.StatusDiff["404"]) / refCount * 100
					if _, err = fmt.Fprintf(w, ", 404: %s", diffString(v/d.Count*100, refV)); err != nil {
						return
					}
				}
				for k, v := range d.Status {
					if k != "200" && k != "400" && k != "404" {
						refV := (v - d.StatusDiff[k]) / refCount * 100
						if _, err = fmt.Fprintf(w, ", %s: %s", k, diffString(v/d.Count*100, refV)); err != nil {
							return
						}
					}
				}
			}
			if _, err = fmt.Fprintln(w); err != nil {
				return
			}
		}
	}
	return
}

func saveTestSamples(test *dbs.TestSamples, path string) error {
	if b, err := json.Marshal(test); err != nil {
		return err
	} else {
		return os.WriteFile(path, b, 0644)
	}
}

func loadTestSamples(path string) (*dbs.TestSamples, error) {
	if b, err := os.ReadFile(path); err != nil {
		return nil, err
	} else {
		test := new(dbs.TestSamples)
		if err = json.Unmarshal(b, test); err != nil {
			return nil, err
		}
		return test, nil
	}
}

func main() {
	var (
		err  error
		line string

		dbErr *dbs.QueryError

		// registry attached vars
		chAddress, chPparam, chDB string
		tableTests, tableSamples  string

		testsFrom  time.Time
		testsUntil time.Time

		filterLabel   string
		filterUrl     string
		filterSkipUrl []string

		testsFilter dbs.TestFilter

		selectNum  int
		selectId   uint64
		selectTime time.Time
		refNum     int
		refId      uint64
		refTime    time.Time

		saveTest string
		saveRef  string
		loadTest string
		loadRef  string

		topTopCount  int
		topTopSortBy dbs.SortBy
		topSave      string
		topAppend    bool

		topRefCount  int
		topRefSortBy dbs.SortBy
		topRefSave   string
		topRefAppend bool

		diffTopCount int
		// topByLabel bool
		diffTopSortBy     dbs.SortBy
		diffTopSortByDiff bool
		diffTopSave       string
		diffTopAppend     bool

		// stored
		tests []dbs.Test // loaded with tests
		// filter
		filterByLabel   string
		filterByUrl     string
		filterBySkipUrl []string
		// set by select
		testSamplesDurations *dbs.TestSamples
		// set by reference (reference test for compare)
		refSamplesDurations *dbs.TestSamples
	)

	chRegistry := clipper.NewRegistry("CLI for display xk6-output-clickhouse tests")
	chCommand, _ := chRegistry.Register("", "Clickhouse settings")
	chCommand.AddString("db", "d", "default", &chDB, "Database name").
		AttachEnv("K6_STAT_DB")
	chCommand.AddString("tests", "t", "k6_tests", &tableTests, "Tests table").
		AttachEnv("K6_STAT_TABLE_TESTS")
	chCommand.AddString("samples", "s", "k6_samples", &tableSamples, "Samples table").
		AttachEnv("K6_STAT_TABLE_SAMPLES")

	chCommand.AddString("address", "a", "http://localhost:8123", &chAddress, "Database address").
		AttachEnv("K6_STAT_DB_ADDR")
	chCommand.AddString("params", "p", "dial_timeout=200ms&max_execution_time=60", &chPparam, "Connection params").
		AttachEnv("K6_STAT_DB_PARAM")

	if _, err := chRegistry.Parse(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	dsn := chAddress + "/" + chDB + "?" + chPparam
	if d, err := sql.Open("clickhouse", dsn); err == nil {
		d.SetMaxIdleConns(1)
		d.SetMaxOpenConns(3)
		d.SetConnMaxIdleTime(time.Hour)
		db = dbs.New(d, tableTests, tableSamples)
	} else {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	defer db.Close()

	timeLayout := "2006-01-02T15:04:05"
	now := time.Now().UTC()

	registry := clipper.NewRegistry("CLI for display xk6-output-clickhouse tests")

	registry.Register("", "")

	registry.Register("help", "Print help")

	testsCommand, _ := registry.Register("tests", "Load tests")
	testsCommand.AddTimeFromString("from", "f", now.Format(timeLayout), &testsFrom, timeLayout,
		"Select tests started after").
		SetCompeterValue(now.Format(timeLayout))
	testsCommand.AddTimeFromString("until", "u", now.Add(time.Hour*24).Format(timeLayout), &testsUntil, timeLayout,
		"Select tests started before").
		SetCompeterValue(now.Format(timeLayout))
	testsCommand.AddString("name", "n", "", &testsFilter.Name, "Tests name filter (LIKE format)")

	filterCommand, _ := registry.Register("filter", "Filter for load tests")
	filterCommand.AddString("label", "l", "", &filterLabel, "Label filter (LIKE format)")
	filterCommand.AddString("url", "u", "", &filterUrl, "Url filter (LIKE format)")
	filterCommand.AddStringArray("skip-url", "U", []string{}, &filterSkipUrl, "Url filter (LIKE format)")

	selectCommand, _ := registry.Register("select", "Select test")
	selectCommand.AddInt("number", "n", -1, &selectNum, "Select test from loaded tests by number")
	selectCommand.AddUint64("id", "i", 0, &selectId, "Test id (conflict with number")
	selectCommand.AddTimeFromString("time", "t", now.Format(time.RFC3339Nano), &selectTime, time.RFC3339Nano,
		"Test start time (used with id)").
		SetCompeterValue(now.Format(time.RFC3339Nano))

	refCommand, _ := registry.Register("reference", "Select reference test (used for compare)")
	refCommand.AddInt("number", "n", -1, &refNum, "Select test from loaded tests by number")
	refCommand.AddUint64("id", "i", 0, &refId, "Reference id (conflict with number")
	refCommand.AddTimeFromString("time", "t", now.Format(time.RFC3339Nano), &refTime, time.RFC3339Nano,
		"Reference start time (used with id)").
		SetCompeterValue(now.Format(time.RFC3339Nano))

	saveCommand, _ := registry.Register("save", "Save tests")
	saveCommand.AddString("test", "t", "", &saveTest, "Test file")
	saveCommand.AddString("ref", "r", "", &saveRef, "Reference test file")

	loadCommand, _ := registry.Register("load", "Load tests")
	loadCommand.AddString("test", "t", "", &loadTest, "Test file")
	loadCommand.AddString("ref", "r", "", &loadRef, "Reference test file")

	topCommand, _ := registry.Register("top", "Print top of test queries")
	topCommand.AddInt("count", "c", 10, &topTopCount, "Top of N queries")
	// topCommand.AddFlag("no-label", "N", &topByLabel, "Top per label")
	topCommand.AddValue("sort", "s", dbs.NewSortByValue(dbs.SortByP99, &topTopSortBy), false, "Sort by "+dbs.SortByValuesString()).
		SetValidValues(dbs.SortByValues())
	topCommand.AddString("out", "o", "", &topSave, "Save test top to file")
	topCommand.AddFlag("append", "a", &topAppend, "Append to file")

	topRefCommand, _ := registry.Register("ref-top", "Print top of reference test queries")
	topRefCommand.AddInt("count", "c", 10, &topRefCount, "Top of N queries")
	// topRefCommand.AddFlag("no-label", "N", &topRefByLabel, "Top per label")
	topRefCommand.AddValue("sort", "s", dbs.NewSortByValue(dbs.SortByP99, &topRefSortBy), false, "Sort by "+dbs.SortByValuesString()).
		SetValidValues(dbs.SortByValues())
	topRefCommand.AddString("out", "o", "", &topRefSave, "Save reference test top to file")
	topRefCommand.AddFlag("append", "a", &topRefAppend, "Append to file")

	diffCommand, _ := registry.Register("diff", "Print top of diff test/reference queries")
	diffCommand.AddInt("count", "c", 10, &diffTopCount, "Top of N queries")
	// topCommand.AddFlag("no-label", "N", &topByLabel, "Top per label")
	diffCommand.AddValue("sort", "s", dbs.NewSortByValue(dbs.SortByP99, &diffTopSortBy), false, "Sort by "+dbs.SortByValuesString()).
		SetValidValues(dbs.SortByValues())
	diffCommand.AddFlag("by-diff", "d", &diffTopSortByDiff, "Top by diff")
	diffCommand.AddString("out", "o", "", &diffTopSave, "Save top of diff between tests to file")
	diffCommand.AddFlag("append", "a", &diffTopAppend, "Append to file")

	reader := liner.NewLiner()
	defer reader.Close()

	reader.SetCtrlCAborts(true)

	reader.SetCompleter(func(line string) []string {
		return registry.Completer(line)
	})

	for {
		line, err = reader.Prompt("k6-stat> ")
		reader.AppendHistory(line)
		if err == nil {
			if line == "exit" || line == "quit" {
				break
			}
			if line == "help" {
				clipper.PrintHelp(registry, "", registry.Commands[""], false)
			} else {
				args := clipper.SplitQuoted(line)
				command, helpRequested, err := registry.ParseInteract(args, false)
				if err != nil {
					registry.ResetCommand(command)
					fmt.Fprintln(os.Stderr, "Error: ", err)
				} else if !helpRequested {
					// execute command
					switch command {
					case "tests":
						testsFilter.From = testsFrom.Unix()
						testsFilter.Until = testsUntil.Unix()
						if tests, dbErr = db.GetTests(testsFilter); dbErr == nil {
							printTests(os.Stdout, tests)
						} else {
							fmt.Fprintf(os.Stderr, "Error: %s, sql: %s\n", dbErr.Error(), dbErr.Query())
						}
					case "filter":
						filterByLabel = filterLabel
						filterByUrl = filterUrl
						filterBySkipUrl = filterSkipUrl
					case "select":
						var test dbs.Test
						if selectId > 0 {
							f := dbs.TestIdFilter{
								Id:   selectId,
								Time: selectTime.UnixNano(),
							}
							if test, dbErr = db.GetTestById(f); dbErr != nil {
								registry.ResetCommand(command)
								fmt.Fprintf(os.Stderr, "Error: %s, sql: %s\n", dbErr.Error(), dbErr.Query())
								continue
							}
							_ = printTest(os.Stdout, []dbs.Test{test}, 0, "test", true)
						} else if selectNum >= 0 && selectNum < len(tests) {
							test = tests[selectNum]
							_ = printTest(os.Stdout, tests, selectNum, strconv.Itoa(selectNum), true)
						}
						fmt.Print("Filter:")
						if filterByLabel != "" {
							fmt.Printf(" Label %q", filterByLabel)
						}
						if filterByUrl != "" {
							fmt.Printf(" Url %q ", filterByUrl)
						}
						if len(filterBySkipUrl) > 0 {
							fmt.Printf(" Skip url %q", filterBySkipUrl)
						}
						fmt.Println()

						filter := dbs.SampleFilter{
							Id:      test.Id,
							Start:   test.Ts.UnixNano(),
							Label:   filterByLabel,
							Url:     filterByUrl,
							SkipUrl: filterBySkipUrl,
						}

						var (
							samplesQ      []dbs.SampleQuantiles
							samplesStatus []dbs.SampleStatus
						)

						if samplesQ, dbErr = db.GetHttpSamplesDurations(filter); dbErr == nil {
							if len(samplesQ) == 0 {
								fmt.Fprintln(os.Stderr, "Warning: no duration samples")
							}
							samplesStatus, dbErr = db.GetHttpSamplesStatus(filter)
							if dbErr == nil {
								if len(samplesStatus) == 0 {
									fmt.Fprintln(os.Stderr, "Warning: no status samples")
								}
							}
						}

						if dbErr != nil {
							fmt.Fprintf(os.Stderr, "Error: %s, sql: %s\n", dbErr.Error(), dbErr.Query())
						} else {
							testSamplesDurations = dbs.MergeSamples(test, samplesQ, samplesStatus)
							fmt.Printf("Loaded %d duration samples, %d status samples\n", len(samplesQ), len(samplesStatus))
						}
					case "reference":
						var test dbs.Test
						if refId > 0 {
							f := dbs.TestIdFilter{
								Id:   refId,
								Time: refTime.UnixNano(),
							}
							if test, dbErr = db.GetTestById(f); dbErr != nil {
								fmt.Fprintf(os.Stderr, "Error: %s, sql: %s\n", dbErr.Error(), dbErr.Query())
							} else {
								refNum = 0
								_ = printTest(os.Stdout, []dbs.Test{test}, 0, "ref", true)
							}
						} else if refNum >= 0 && refNum < len(tests) {
							test = tests[refNum]
							_ = printTest(os.Stdout, tests, refNum, strconv.Itoa(refNum), true)
						}
						fmt.Print("Filter:")
						if filterByLabel != "" {
							fmt.Printf(" Label %q", filterByLabel)
						}
						if filterByUrl != "" {
							fmt.Printf(" Url %q ", filterByUrl)
						}
						if len(filterBySkipUrl) > 0 {
							fmt.Printf(" Skip url %q", filterBySkipUrl)
						}
						fmt.Println()

						filter := dbs.SampleFilter{
							Id:      test.Id,
							Start:   test.Ts.UnixNano(),
							Label:   filterByLabel,
							Url:     filterByUrl,
							SkipUrl: filterBySkipUrl,
						}

						var (
							samplesQ      []dbs.SampleQuantiles
							samplesStatus []dbs.SampleStatus
						)

						if samplesQ, dbErr = db.GetHttpSamplesDurations(filter); dbErr == nil {
							if len(samplesQ) == 0 {
								fmt.Fprintln(os.Stderr, "Warning: no duration samples")
							}

							samplesStatus, dbErr = db.GetHttpSamplesStatus(filter)
							if dbErr != nil {
								if len(samplesStatus) == 0 {
									fmt.Fprintln(os.Stderr, "Warning: no status samples")
								}
							}
						}

						if dbErr != nil {
							fmt.Fprintf(os.Stderr, "Error: %s, sql: %s\n", dbErr.Error(), dbErr.Query())
						} else {
							refSamplesDurations = dbs.MergeSamples(test, samplesQ, samplesStatus)
							fmt.Printf("Loaded reference %d duration samples, %d status samples\n", len(samplesQ), len(samplesStatus))
						}
					case "save":
						if saveTest != "" {
							if err := saveTestSamples(testSamplesDurations, saveTest); err != nil {
								fmt.Fprintf(os.Stderr, "Error: save 'test' samples with %v\n", err)
							}
						}
						if saveRef != "" {
							if err := saveTestSamples(refSamplesDurations, saveRef); err != nil {
								fmt.Fprintf(os.Stderr, "Error: save 'ref' samples with %v\n", err)
							}
						}
					case "load":
						var err error
						if loadTest != "" {
							if testSamplesDurations, err = loadTestSamples(loadTest); err != nil {
								fmt.Fprintf(os.Stderr, "Error: load 'test' samples with %v\n", err)
							}
						}
						if loadRef != "" {
							if refSamplesDurations, err = loadTestSamples(loadRef); err == nil {
								fmt.Fprintf(os.Stderr, "Error: load 'ref' samples with %v\n", err)
							}
						}
					case "top":
						if testSamplesDurations == nil {
							fmt.Fprintf(os.Stderr, "Error: select test with 'select' command\n")
						} else {
							for _, d := range testSamplesDurations.Samples {
								dbs.SortSamplesDurations(d, topTopSortBy)
							}

							_ = printTest(os.Stdout, []dbs.Test{testSamplesDurations.Test}, 0, "test", true)
							fmt.Println()
							_ = printHttpTop(os.Stdout, testSamplesDurations.Samples, topTopCount)

							if topSave != "" {
								var f *os.File
								if topAppend {
									if f, err = os.OpenFile(topSave, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0640); err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}

								} else {
									if f, err = os.OpenFile(topSave, os.O_WRONLY|os.O_CREATE, 0640); err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}
								}
								if f != nil {
									err = printTest(f, []dbs.Test{testSamplesDurations.Test}, 0, "test", true)
									if err == nil {
										_, err = fmt.Fprintln(f)
									}
									if err == nil {
										err = printHttpTop(f, testSamplesDurations.Samples, topTopCount)
									}
									if err != nil {
										f.Close()
									} else {
										err = f.Close()
									}
									if err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}
								}
							}
						}
					case "ref-top":
						if refSamplesDurations == nil {
							fmt.Fprintf(os.Stderr, "Error: select reference test with 'reference' command\n")
						} else {
							for _, d := range refSamplesDurations.Samples {
								dbs.SortSamplesDurations(d, topRefSortBy)
							}

							_ = printTest(os.Stdout, []dbs.Test{refSamplesDurations.Test}, 0, "ref", true)
							fmt.Println()
							_ = printHttpTop(os.Stdout, refSamplesDurations.Samples, topRefCount)

							if topRefSave != "" {
								var f *os.File
								if topRefAppend {
									if f, err = os.OpenFile(topRefSave, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0640); err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}

								} else {
									if f, err = os.OpenFile(topRefSave, os.O_WRONLY|os.O_CREATE, 0640); err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}
								}
								if f != nil {
									err = printTest(f, []dbs.Test{refSamplesDurations.Test}, 0, "ref", true)
									if err == nil {
										_, err = fmt.Fprintln(f)
									}
									if err == nil {
										err = printHttpTop(f, refSamplesDurations.Samples, topRefCount)
									}
									if err != nil {
										f.Close()
									} else {
										err = f.Close()
									}
									if err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}
								}
							}
						}
					case "diff":
						if testSamplesDurations == nil {
							fmt.Fprintf(os.Stderr, "Error: select test with 'select' command\n")
						}
						if refSamplesDurations == nil {
							fmt.Fprintf(os.Stderr, "Error: select reference test with 'reference' command\n")
						}
						if testSamplesDurations != nil && refSamplesDurations != nil {
							diff := dbs.DiffSamples(testSamplesDurations, refSamplesDurations)
							if diffTopSortByDiff {
								// sort by diff
								for _, d := range diff.Samples {
									dbs.SortSamplesDurationsByDiff(d, diffTopSortBy)
								}
							} else {
								for _, d := range diff.Samples {
									dbs.SortSamplesDurationsDiff(d, diffTopSortBy)
								}
							}

							_ = printTest(os.Stdout, []dbs.Test{testSamplesDurations.Test}, 0, "test", true)
							_ = printTest(os.Stdout, []dbs.Test{refSamplesDurations.Test}, 0, "ref", false)
							fmt.Println()
							_ = printHttpTopDiff(os.Stdout, diff.Samples, diffTopCount)

							if diffTopSave != "" {
								var f *os.File
								if diffTopAppend {
									if f, err = os.OpenFile(diffTopSave, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0640); err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}

								} else {
									if f, err = os.OpenFile(diffTopSave, os.O_WRONLY|os.O_CREATE, 0640); err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}
								}
								if f != nil {
									err = printTest(os.Stdout, []dbs.Test{testSamplesDurations.Test}, 0, "test", true)
									if err == nil {
										err = printTest(os.Stdout, []dbs.Test{refSamplesDurations.Test}, 0, "ref", false)
									}
									if err == nil {
										_, err = fmt.Fprintln(f)
									}
									if err == nil {
										err = printHttpTopDiff(f, diff.Samples, diffTopCount)
									}
									if err != nil {
										f.Close()
									} else {
										err = f.Close()
									}
									if err != nil {
										fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
									}
								}
							}
						}
					case "":
						// ignore empty command
					default:
						fmt.Fprintf(os.Stderr, "Error: command %q not handled\n", line)
					}
					registry.ResetCommand(command)
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
