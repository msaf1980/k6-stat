package app

import (
	"database/sql"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/msaf1980/fiberlog"
	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/go-timeutils"
	"github.com/rs/zerolog"
)

func Getenv(key, defaultValue string) (v string) {
	v = os.Getenv(key)
	if v == "" {
		v = defaultValue
	}
	return
}

type Test struct {
	Id     uint64
	Ts     time.Time
	Name   string
	Params string
}

type TestFilter struct {
	From       int64  `json:"from,omitempty"`
	Until      int64  `json:"until,omitempty"`
	NamePrefix string `json:"name_prefix,omitempty"`
}

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

type App struct {
	db       *sql.DB
	fiberApp *fiber.App
	logger   *zerolog.Logger

	tableTests   string
	tableSamples string
}

func newApp(db *sql.DB, logger *zerolog.Logger, tableTests, tableSamples string) (*App, error) {
	app := fiber.New(fiber.Config{
		JSONEncoder: json.Marshal,
		JSONDecoder: json.Unmarshal,
	})
	app.Use(cors.New())

	// Custom Config
	app.Use(fiberlog.New(fiberlog.Config{
		Logger: logger,
		Next: func(ctx *fiber.Ctx) bool {
			// skip if we hit /check
			// 	return ctx.Path() == "/check"
			return false
		},
	}))

	a := &App{db: db, fiberApp: app, logger: logger, tableTests: tableTests, tableSamples: tableSamples}

	app.Post("/api/tests", func(c *fiber.Ctx) error {
		return a.getTests(c)
	})

	app.Post("/api/test/http/duration", func(c *fiber.Ctx) error {
		return a.getHttpSamplesDurations(c)
	})

	app.Post("/api/test/http/status", func(c *fiber.Ctx) error {
		return a.getHttpSamplesStatus(c)
	})

	return a, nil
}

func New(dbDSN string, maxConn int, logger *zerolog.Logger, tableTests, tableSamples string) (*App, error) {
	db, err := sql.Open("clickhouse", dbDSN)
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(maxConn)
	db.SetConnMaxIdleTime(time.Hour)

	return newApp(db, logger, tableTests, tableSamples)
}

func (app *App) Listen(address string) error {
	return app.fiberApp.Listen(address)
}

func (app *App) Shutdown() error {
	return app.fiberApp.Shutdown()
}

func (app *App) Close() error {
	return app.db.Close()
}

func (app *App) getTests(c *fiber.Ctx) error {
	var (
		query    stringutils.Builder
		filters  TestFilter
		filtered bool
	)

	if err := c.BodyParser(&filters); err != nil {
		if err != fiber.ErrUnprocessableEntity && len(c.Request().Body()) > 0 {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}
	}

	filter := make([]any, 0, 3)
	query.Grow(64)
	_, _ = query.WriteString("SELECT id, ts, name, params FROM ")
	_, _ = query.WriteString(app.tableTests)

	if filters.From > 0 {
		if filtered {
			_, _ = query.WriteString(" AND ts >= ?")
		} else {
			filtered = true
			_, _ = query.WriteString(" WHERE ts >= ?")
		}
		filter = append(filter, time.Unix(filters.From, 0).UTC())
	} else if filters.From < 0 {
		return c.Status(http.StatusBadRequest).SendString(invalidFrom)
	}
	if filters.Until > 0 {
		if filtered {
			_, _ = query.WriteString(" AND ts < ?")
		} else {
			filtered = true
			_, _ = query.WriteString(" WHERE ts < ?")
		}
		filter = append(filter, time.Unix(filters.Until, 0).UTC())
	} else if filters.Until < 0 {
		return c.Status(http.StatusBadRequest).SendString(invalidUntil)
	}
	if filters.NamePrefix != "" {
		if filtered {
			_, _ = query.WriteString(" AND name LIKE ?")
		} else {
			_, _ = query.WriteString(" WHERE name LIKE ?")
		}
		filter = append(filter, filters.NamePrefix+"%")
	}

	_, _ = query.WriteString(" ORDER BY id, ts, name")
	rows, err := app.db.Query(query.String(), filter...)
	if err != nil {
		app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get tests")
		return fiber.ErrServiceUnavailable
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
			return err
		}
		tests = append(tests, Test{Id: id, Ts: ts, Name: name, Params: params})
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return err
	}

	return c.JSON(tests)
}

func (app *App) getHttpSamplesDurations(c *fiber.Ctx) error {
	var (
		query   stringutils.Builder
		filters SampleFilter
	)

	if err := c.BodyParser(&filters); err != nil {
		if err != fiber.ErrUnprocessableEntity && len(c.Request().Body()) > 0 {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}
	}

	start := timeutils.UnixNano(filters.Start).UTC()

	query.Grow(64)
	_, _ = query.WriteString("SELECT id, start, label, url, quantiles(0.5, 0.9, 0.95, 0.99)(value) FROM ")
	_, _ = query.WriteString(app.tableSamples)
	_, _ = query.WriteString(" WHERE id = @Id AND start = @Time AND metric = @Metric")

	_, _ = query.WriteString(" GROUP BY id, start, label, url ORDER BY label, url")

	rows, err := app.db.Query(
		query.String(), clickhouse.Named("Id", filters.Id), clickhouse.DateNamed("Time", start, 3),
		clickhouse.Named("Metric", "http_req_duration"),
	)
	if err != nil {
		app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get http samples duration")
		return fiber.ErrServiceUnavailable
	}
	defer rows.Close()
	samples := make([]SampleQuantiles, 0, 50)
	for rows.Next() {
		var s SampleQuantiles
		err = rows.Scan(&s.Id, &s.Start, &s.Label, &s.Url, &s.Quantiles)
		if err != nil {
			// handle this error
			return err
		}
		samples = append(samples, s)
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return err
	}

	return c.JSON(samples)
}

func (app *App) getHttpSamplesStatus(c *fiber.Ctx) error {
	var (
		query   stringutils.Builder
		filters SampleFilter
	)

	if err := c.BodyParser(&filters); err != nil {
		if err != fiber.ErrUnprocessableEntity && len(c.Request().Body()) > 0 {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}
	}

	start := timeutils.UnixNano(filters.Start).UTC()

	query.Grow(64)
	_, _ = query.WriteString("SELECT id, start, label, url, status, sum(value) FROM ")
	_, _ = query.WriteString(app.tableSamples)
	_, _ = query.WriteString(" WHERE id = @Id AND start = @Time AND metric = @Metric")

	_, _ = query.WriteString(" GROUP BY id, start, label, url, status ORDER BY label, url, status")

	rows, err := app.db.Query(
		query.String(), clickhouse.Named("Id", filters.Id), clickhouse.DateNamed("Time", start, 3),
		clickhouse.Named("Metric", "http_reqs"),
	)
	if err != nil {
		app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get http samples status")
		return fiber.ErrServiceUnavailable
	}
	defer rows.Close()
	samples := make([]SampleStatus, 0, 50)
	for rows.Next() {
		var s SampleStatus
		err = rows.Scan(&s.Id, &s.Start, &s.Label, &s.Url, &s.Status, &s.Count)
		if err != nil {
			// handle this error
			return err
		}
		samples = append(samples, s)
	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		return err
	}

	return c.JSON(samples)
}
