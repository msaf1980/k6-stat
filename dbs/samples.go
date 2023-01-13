package dbs

import (
	"net/http"
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

type SampleQuantiles struct {
	Id        uint64    `json:"id"`
	Start     time.Time `json:"start"` // ts from tests
	Label     string    `json:"label,omitempty"`
	Url       string    `json:"url"`
	Quantiles []float64 `json:"quantiles"` // 0.5, 0.9, 0.95, 0.99
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
	Id    uint64 `json:"id"`
	Start int64  `json:"start"`
	// Metrics []string `json:"metrics,omitempty"`
}

func (d *DB) GetHttpSamplesDurations(f SampleFilter) ([]SampleQuantiles, *QueryError) {
	var query stringutils.Builder

	start := timeutils.UnixNano(f.Start).UTC()

	query.Grow(64)
	_, _ = query.WriteString("SELECT id, start, label, url, quantiles(0.5, 0.9, 0.95, 0.99)(value) FROM ")
	_, _ = query.WriteString(d.tableSamples)
	_, _ = query.WriteString(" WHERE id = @Id AND start = @Time AND metric = @Metric")

	_, _ = query.WriteString(" GROUP BY id, start, label, url ORDER BY label, url")

	rows, err := d.db.Query(
		query.String(), clickhouse.Named("Id", f.Id), clickhouse.DateNamed("Time", start, 3),
		clickhouse.Named("Metric", "http_req_duration"),
	)
	if err != nil {
		// app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get http samples duration")
		return nil, NewQueryError(err, 0, query.String())
	}
	defer rows.Close()
	samples := make([]SampleQuantiles, 0, 50)
	for rows.Next() {
		var s SampleQuantiles
		err = rows.Scan(&s.Id, &s.Start, &s.Label, &s.Url, &s.Quantiles)
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

func (d *DB) GetHttpSamplesStatus(f SampleFilter) ([]SampleStatus, *QueryError) {
	var query stringutils.Builder

	start := timeutils.UnixNano(f.Start).UTC()

	query.Grow(64)
	_, _ = query.WriteString("SELECT id, start, label, url, status, sum(value) FROM ")
	_, _ = query.WriteString(d.tableSamples)
	_, _ = query.WriteString(" WHERE id = @Id AND start = @Time AND metric = @Metric")

	_, _ = query.WriteString(" GROUP BY id, start, label, url, status ORDER BY label, url, status")

	rows, err := d.db.Query(
		query.String(), clickhouse.Named("Id", f.Id), clickhouse.DateNamed("Time", start, 3),
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
