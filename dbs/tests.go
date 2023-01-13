package dbs

import (
	"time"

	"github.com/msaf1980/go-stringutils"
)

type Test struct {
	Id     uint64
	Ts     time.Time
	Name   string
	Params string
}

type TestFilter struct {
	From       int64  `json:"from,omitempty"`  // epoch seconds
	Until      int64  `json:"until,omitempty"` // epoch seconds
	NamePrefix string `json:"name_prefix,omitempty"`
}

func (d *DB) GetTests(f TestFilter) ([]Test, *QueryError) {
	var (
		query    stringutils.Builder
		filtered bool
	)

	filter := make([]any, 0, 3)
	query.Grow(64)
	_, _ = query.WriteString("SELECT id, ts, name, params FROM ")
	_, _ = query.WriteString(d.tableTests)

	if f.From > 0 {
		if filtered {
			_, _ = query.WriteString(" AND ts >= ?")
		} else {
			filtered = true
			_, _ = query.WriteString(" WHERE ts >= ?")
		}
		filter = append(filter, time.Unix(f.From, 0).UTC())
	} else if f.From < 0 {
		// return c.Status(http.StatusBadRequest).SendString(invalidFrom)
		return nil, InvalidFrom
	}
	if f.Until > 0 {
		if filtered {
			_, _ = query.WriteString(" AND ts < ?")
		} else {
			filtered = true
			_, _ = query.WriteString(" WHERE ts < ?")
		}
		filter = append(filter, time.Unix(f.Until, 0).UTC())
	} else if f.Until < 0 {
		// return c.Status(http.StatusBadRequest).SendString(invalidUntil)
		return nil, InvalidUntil
	}
	if f.NamePrefix != "" {
		if filtered {
			_, _ = query.WriteString(" AND name LIKE ?")
		} else {
			_, _ = query.WriteString(" WHERE name LIKE ?")
		}
		filter = append(filter, f.NamePrefix+"%")
	}

	_, _ = query.WriteString(" ORDER BY id, ts, name")
	rows, err := d.db.Query(query.String(), filter...)
	if err != nil {
		// app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get tests")
		return nil, NewQueryError(err, 0, query.String())
	}
	defer rows.Close()
	tests := make([]Test, 0, 50)
	for rows.Next() {
		var (
			id           uint64
			ts           time.Time
			name, params string
		)
		err = rows.Scan(&id, &ts, &name, &params)
		if err != nil {
			// handle this error
			return nil, NewQueryError(err, 0, query.String())
		}
		tests = append(tests, Test{Id: id, Ts: ts, Name: name, Params: params})
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return nil, NewQueryError(err, 0, query.String())
	}

	return tests, nil
}
