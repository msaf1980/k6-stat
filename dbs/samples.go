package dbs

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/go-timeutils"
)

type Sample struct {
	Id     uint64
	Start  time.Time // ts from tests
	Ts     time.Time
	Metric string
	Label  string
	Url    string
	Status string
	Name   string
	Tags   map[string]string
	Value  float64
}

type SampleDurations struct {
	Url string `json:"url"`

	// query durations
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`

	// status count map
	Status     map[string]float64 `json:"status"`
	Count      float64            `json:"count"`
	ErrorsPcnt float64            `json:"errors"`
}

type SampleDurationsDiff struct {
	Url string `json:"url"`

	// query durations
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`

	P50Diff float64 `json:"p50-diff"`
	P90Diff float64 `json:"p90-diff"`
	P95Diff float64 `json:"p95-diff"`
	P99Diff float64 `json:"p99-diff"`
	MaxDiff float64 `json:"max-diff"`

	// status count map
	Status     map[string]float64 `json:"status"`
	Count      float64            `json:"count"`
	ErrorsPcnt float64            `json:"errors"`

	StatusDiff     map[string]float64 `json:"status-diff"`
	CountDiff      float64            `json:"count-diff"`
	ErrorsPcntDiff float64            `json:"errors-diff"`
}

type TestSamples struct {
	Test    Test                         `json:"test"`
	Samples map[string][]SampleDurations `json:"samples"`
}

type TestSamplesDiff struct {
	Test      Test                             `json:"test"`
	Reference Test                             `json:"ref"`
	Samples   map[string][]SampleDurationsDiff `json:"samples"`
}

type SampleQuantiles struct {
	Id    uint64    `json:"id"`
	Start time.Time `json:"start"` // ts from tests
	Label string    `json:"label,omitempty"`
	Url   string    `json:"url"`

	// query durations
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type SampleStatus struct {
	Id     uint64    `json:"id"`
	Start  time.Time `json:"start"` // ts from tests
	Label  string    `json:"label,omitempty"`
	Url    string    `json:"url"`
	Status string    `json:"status"`
	Count  float64   `json:"count"`
}

type SampleFilter struct {
	Id      uint64   `json:"id"`
	Start   int64    `json:"start"`
	Label   string   `json:"label,omitempty"`
	Url     string   `json:"url,omitempty"`
	SkipUrl []string `json:"no-url,omitempty"`
	// Metrics []string `json:"metrics,omitempty"`
}

func (d *DB) GetHttpSamplesDurations(f SampleFilter) ([]SampleQuantiles, *QueryError) {
	var query stringutils.Builder

	start := timeutils.UnixNano(f.Start).UTC()

	query.Grow(64)
	_, _ = query.WriteString("SELECT id, start, label, url, quantiles(0.5, 0.9, 0.95, 0.99)(value), max(value) as max FROM ")
	_, _ = query.WriteString(d.tableSamples)
	_, _ = query.WriteString(" WHERE id = @Id AND start = @Time AND metric = @Metric")
	if f.Label != "" {
		_, _ = query.WriteString(" AND label LIKE @Label")
	}
	if f.Url != "" {
		_, _ = query.WriteString(" AND url LIKE @Url")
	}
	if len(f.SkipUrl) > 0 {
		for _, n := range f.SkipUrl {
			if n != "" {
				_, _ = query.WriteString(" AND url NOT LIKE '")
				_, _ = query.WriteString(n)
				_, _ = query.WriteString("'")
			}
		}
	}

	_, _ = query.WriteString(" GROUP BY id, start, label, url ORDER BY label, url")

	rows, err := d.db.Query(
		query.String(), clickhouse.Named("Id", f.Id), clickhouse.DateNamed("Time", start, 3),
		clickhouse.Named("Label", f.Label),
		clickhouse.Named("Url", f.Url),
		clickhouse.Named("Metric", "http_req_duration"),
	)
	if err != nil {
		// app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get http samples duration")
		return nil, NewQueryError(err, 0, query.String())
	}
	defer rows.Close()
	samples := make([]SampleQuantiles, 0, 50)
	for rows.Next() {
		var (
			s SampleQuantiles
			q []float64
		)
		err = rows.Scan(&s.Id, &s.Start, &s.Label, &s.Url, &q, &s.Max)
		if err != nil {
			// handle this error
			return nil, NewQueryError(err, 0, query.String())
		}
		s.P50 = q[0]
		s.P90 = q[1]
		s.P95 = q[2]
		s.P99 = q[3]
		samples = append(samples, s)
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return nil, NewQueryError(err, 0, query.String())
	}

	return samples, nil
}

func (d *DB) GetHttpSamplesStatus(f SampleFilter) ([]SampleStatus, *QueryError) {
	var query stringutils.Builder

	start := timeutils.UnixNano(f.Start).UTC()

	query.Grow(64)
	_, _ = query.WriteString("SELECT id, start, label, url, status, sum(value) FROM ")
	_, _ = query.WriteString(d.tableSamples)
	_, _ = query.WriteString(" WHERE id = @Id AND start = @Time AND metric = @Metric")
	if f.Label != "" {
		_, _ = query.WriteString(" AND label LIKE @Label")
	}
	if f.Url != "" {
		_, _ = query.WriteString(" AND url LIKE @Url")
	}
	if len(f.SkipUrl) > 0 {
		for _, s := range f.SkipUrl {
			if s != "" {
				_, _ = query.WriteString(" AND url NOT LIKE '")
				_, _ = query.WriteString(s)
				_, _ = query.WriteString("'")
			}
		}
	}

	_, _ = query.WriteString(" GROUP BY id, start, label, url, status ORDER BY label, url, status")

	rows, err := d.db.Query(
		query.String(), clickhouse.Named("Id", f.Id), clickhouse.DateNamed("Time", start, 3),
		clickhouse.Named("Label", f.Label),
		clickhouse.Named("Url", f.Url),
		clickhouse.Named("Metric", "http_reqs"),
	)
	if err != nil {
		// app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get http samples status")
		return nil, NewQueryError(err, http.StatusInternalServerError, query.String())
	}
	defer rows.Close()
	samples := make([]SampleStatus, 0, 50)
	for rows.Next() {
		var s SampleStatus
		err = rows.Scan(&s.Id, &s.Start, &s.Label, &s.Url, &s.Status, &s.Count)
		if err != nil {
			// handle this error
			return nil, NewQueryError(err, 0, query.String())
		}
		samples = append(samples, s)
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return nil, NewQueryError(err, 0, query.String())
	}

	return samples, nil
}

type mergeKey struct {
	Id    uint64
	Start time.Time // ts from tests
	Label string
	Url   string
}

func MergeSamples(test Test, quantiles []SampleQuantiles, statuses []SampleStatus) *TestSamples {
	mDurations := make(map[mergeKey]*SampleDurations)
	for _, q := range quantiles {
		mDurations[mergeKey{Id: q.Id, Start: q.Start, Label: q.Label, Url: q.Url}] = &SampleDurations{
			Url: q.Url,
			P50: q.P50, P90: q.P90, P95: q.P95, P99: q.P99, Max: q.Max,
			Status: make(map[string]float64),
		}
	}

	for _, s := range statuses {
		key := mergeKey{Id: s.Id, Start: s.Start, Label: s.Label, Url: s.Url}
		m := mDurations[key]
		if m == nil {
			// it's some mistake, status without durations
			m = &SampleDurations{
				Url:    s.Url,
				Status: make(map[string]float64),
			}
			mDurations[key] = m
			fmt.Fprintf(os.Stderr, "Warning: no durations record for %#v", key)
		}
		m.Status[s.Status] += s.Count
	}

	durations := make(map[string][]SampleDurations)
	for k, m := range mDurations {
		m.Count, m.ErrorsPcnt = HttpErrosPcnt(m.Status)
		durations[k.Label] = append(durations[k.Label], *m)
	}

	return &TestSamples{Test: test, Samples: durations}
}

func DiffSamples(test *TestSamples, ref *TestSamples) *TestSamplesDiff {
	diff := &TestSamplesDiff{
		Test:      test.Test,
		Reference: ref.Test,
		Samples:   make(map[string][]SampleDurationsDiff),
	}
	refMap := make(map[string]*SampleDurations)
	for label, vt := range test.Samples {
		if vr, exist := ref.Samples[label]; exist {
			for k := range refMap {
				delete(refMap, k)
			}
			for _, v := range vr {
				refMap[v.Url] = &SampleDurations{
					Url:        v.Url,
					P50:        v.P50,
					P90:        v.P90,
					P95:        v.P95,
					P99:        v.P99,
					Max:        v.Max,
					Status:     v.Status,
					ErrorsPcnt: v.ErrorsPcnt,
					Count:      v.Count,
				}
			}
			samples := make([]SampleDurationsDiff, 0, len(vt))
			for _, v := range vt {
				var s SampleDurationsDiff
				if d, exist := refMap[v.Url]; exist {
					s = SampleDurationsDiff{
						Url:        v.Url,
						P50:        v.P50,
						P90:        v.P90,
						P95:        v.P95,
						P99:        v.P99,
						Max:        v.Max,
						ErrorsPcnt: v.ErrorsPcnt,
						Count:      v.Count,
					}
					s.Status = make(map[string]float64)
					for status, val := range v.Status {
						s.Status[status] = val
					}
					if v.Count > 0 && d.Count > 0 {
						s.P50Diff = v.P50 - d.P50
						s.P90Diff = v.P90 - d.P90
						s.P95Diff = v.P95 - d.P95
						s.P99Diff = v.P99 - d.P99
						s.MaxDiff = v.Max - d.Max
						s.CountDiff = v.Count - d.Count
						s.ErrorsPcntDiff = v.ErrorsPcnt - d.ErrorsPcnt
						s.StatusDiff = make(map[string]float64)
						for status, val := range v.Status {
							s.StatusDiff[status] = val
						}
						for status, val := range d.Status {
							if _, exist := s.Status[status]; !exist {
								s.Status[status] = 0
							}
							s.StatusDiff[status] -= val
						}
					}
				} else {
					// no url in reference samples by label
					s = SampleDurationsDiff{
						Url:        v.Url,
						P50:        v.P50,
						P90:        v.P90,
						P95:        v.P95,
						P99:        v.P99,
						Max:        v.Max,
						Status:     v.Status,
						ErrorsPcnt: v.ErrorsPcnt,
						Count:      v.Count,
					}
				}
				samples = append(samples, s)
			}
			diff.Samples[label] = samples
		} else {
			// no label in reference samples
			samples := make([]SampleDurationsDiff, 0, len(vt))
			for _, v := range vt {
				samples = append(samples, SampleDurationsDiff{
					Url:        v.Url,
					P50:        v.P50,
					P90:        v.P90,
					P95:        v.P95,
					P99:        v.P99,
					Max:        v.Max,
					Status:     v.Status,
					ErrorsPcnt: v.ErrorsPcnt,
					Count:      v.Count,
				})
			}
			diff.Samples[label] = samples
		}
	}

	return diff
}

func HttpErrosPcnt(status map[string]float64) (total, errorsPcnt float64) {
	var (
		errors float64
	)
	for name, n := range status {
		if name != "200" && name != "400" && name != "404" {
			errors += n
		}
		total += n
	}
	if total == 0.0 {
		return
	}
	errorsPcnt = errors / total * 100.0

	return
}

func SortSamplesDurations(durations []SampleDurations, sortBy SortBy) {
	switch sortBy {
	case SortByMax:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].Max == durations[j].Max {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].P99 > durations[j].P99
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].Max > durations[j].Max
		})
	case SortByP99:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P99 == durations[j].P99 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P99 > durations[j].P99
		})
	case SortByP95:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P95 == durations[j].P95 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P95 > durations[j].P95
		})
	case SortByP90:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P90 == durations[j].P90 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P90 > durations[j].P90
		})
	case SortByP50:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P50 == durations[j].P50 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].P99 > durations[j].P99
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P50 > durations[j].P50
		})
	case SortByErrors:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
				if durations[i].Max == durations[j].Max {
					return durations[i].P99 > durations[j].P99
				}
				return durations[i].Max > durations[j].Max
			}
			return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
		})
	case SortByCount:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].Count == durations[j].Count {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].Max > durations[j].Max
			}
			return durations[i].Count > durations[j].Count

		})
	}
}

func SortSamplesDurationsDiff(durations []SampleDurationsDiff, sortBy SortBy) {
	switch sortBy {
	case SortByMax:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].Max == durations[j].Max {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].P99 > durations[j].P99
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].Max > durations[j].Max
		})
	case SortByP99:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P99 == durations[j].P99 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P99 > durations[j].P99
		})
	case SortByP95:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P95 == durations[j].P95 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P95 > durations[j].P95
		})
	case SortByP90:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P90 == durations[j].P90 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P90 > durations[j].P90
		})
	case SortByP50:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P50 == durations[j].P50 {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].P99 > durations[j].P99
				}
				return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
			}
			return durations[i].P50 > durations[j].P50
		})
	case SortByErrors:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
				if durations[i].Max == durations[j].Max {
					return durations[i].P99 > durations[j].P99
				}
				return durations[i].Max > durations[j].Max
			}
			return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
		})
	case SortByCount:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].Count == durations[j].Count {
				if durations[i].ErrorsPcnt == durations[j].ErrorsPcnt {
					return durations[i].Max > durations[j].Max
				}
				return durations[i].Max > durations[j].Max
			}
			return durations[i].Count > durations[j].Count

		})
	}
}

func SortSamplesDurationsByDiff(durations []SampleDurationsDiff, sortBy SortBy) {
	switch sortBy {
	case SortByMax:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].MaxDiff == durations[j].MaxDiff {
				if durations[i].ErrorsPcntDiff == durations[j].ErrorsPcntDiff {
					if durations[i].P99Diff == durations[j].P99Diff {
						// no valid diff
						if durations[i].Max == durations[j].Max {
							return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
						}
						return durations[i].Max > durations[j].Max
					}
					return durations[i].P99Diff > durations[j].P99Diff
				}
				return durations[i].ErrorsPcntDiff > durations[j].ErrorsPcntDiff
			}
			return durations[i].MaxDiff > durations[j].MaxDiff
		})
	case SortByP99:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P99Diff == durations[j].P99Diff {
				if durations[i].MaxDiff == durations[j].MaxDiff {
					if durations[i].ErrorsPcntDiff == durations[j].ErrorsPcntDiff {
						// no valid diff
						if durations[i].Max == durations[j].Max {
							return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
						}
						return durations[i].Max > durations[j].Max
					}
					return durations[i].ErrorsPcntDiff > durations[j].ErrorsPcntDiff
				}
				return durations[i].MaxDiff > durations[j].MaxDiff
			}
			return durations[i].P99Diff > durations[j].P99Diff
		})
	case SortByP95:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P95Diff == durations[j].P95Diff {
				if durations[i].MaxDiff == durations[j].MaxDiff {
					if durations[i].ErrorsPcntDiff == durations[j].ErrorsPcntDiff {
						// no valid diff
						if durations[i].Max == durations[j].Max {
							return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
						}
						return durations[i].Max > durations[j].Max
					}
					return durations[i].ErrorsPcntDiff > durations[j].ErrorsPcntDiff
				}
				return durations[i].MaxDiff > durations[j].MaxDiff
			}
			return durations[i].P95Diff > durations[j].P95Diff
		})
	case SortByP90:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P90Diff == durations[j].P90Diff {
				if durations[i].MaxDiff == durations[j].MaxDiff {
					if durations[i].ErrorsPcntDiff == durations[j].ErrorsPcntDiff {
						// no valid diff
						if durations[i].Max == durations[j].Max {
							return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
						}
						return durations[i].Max > durations[j].Max
					}
					return durations[i].ErrorsPcntDiff > durations[j].ErrorsPcntDiff
				}
				return durations[i].MaxDiff > durations[j].MaxDiff
			}
			return durations[i].P90Diff > durations[j].P90Diff
		})
	case SortByP50:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].P50Diff == durations[j].P50Diff {
				if durations[i].MaxDiff == durations[j].MaxDiff {
					if durations[i].ErrorsPcntDiff == durations[j].ErrorsPcntDiff {
						// no valid diff
						if durations[i].Max == durations[j].Max {
							return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
						}
						return durations[i].Max > durations[j].Max
					}
					return durations[i].ErrorsPcntDiff > durations[j].ErrorsPcntDiff
				}
				return durations[i].MaxDiff > durations[j].MaxDiff
			}
			return durations[i].P50Diff > durations[j].P50Diff
		})
	case SortByErrors:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].ErrorsPcntDiff == durations[j].ErrorsPcntDiff {
				if durations[i].MaxDiff == durations[j].MaxDiff {
					if durations[i].P99Diff == durations[j].P99Diff {
						// no valid diff
						if durations[i].Max == durations[j].Max {
							return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
						}
						return durations[i].Max > durations[j].Max
					}
					return durations[i].P99Diff > durations[j].P99Diff
				}
				return durations[i].MaxDiff > durations[j].MaxDiff
			}
			return durations[i].ErrorsPcntDiff > durations[j].ErrorsPcntDiff
		})
	case SortByCount:
		sort.Slice(durations, func(i, j int) bool {
			if durations[i].CountDiff == durations[j].CountDiff {
				if durations[i].MaxDiff == durations[j].MaxDiff {
					if durations[i].ErrorsPcntDiff == durations[j].ErrorsPcntDiff {
						// no valid diff
						if durations[i].Max == durations[j].Max {
							return durations[i].ErrorsPcnt > durations[j].ErrorsPcnt
						}
						return durations[i].Max > durations[j].Max
					}
					return durations[i].ErrorsPcntDiff > durations[j].ErrorsPcntDiff
				}
				return durations[i].MaxDiff > durations[j].MaxDiff
			}
			return durations[i].CountDiff > durations[j].CountDiff
		})
	}
}
