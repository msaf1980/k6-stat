package k6_stat

import (
	"database/sql"
	"net/http"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/msaf1980/fiberlog"
	"github.com/rs/zerolog"

	"github.com/msaf1980/k6-stat/dbs"
)

type App struct {
	db       *dbs.DB
	fiberApp *fiber.App
	logger   *zerolog.Logger
}

func NewWithDB(db *sql.DB, logger *zerolog.Logger, tableTests, tableSamples string) (*App, error) {
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

	a := &App{
		db:       dbs.New(db, tableTests, tableSamples),
		fiberApp: app,
		logger:   logger,
	}

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

	return NewWithDB(db, logger, tableTests, tableSamples)
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
	var filter dbs.TestFilter
	if err := c.BodyParser(&filter); err != nil {
		if err != fiber.ErrUnprocessableEntity && len(c.Request().Body()) > 0 {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}
	}
	tests, err := app.db.GetTests(filter)
	if err != nil {
		app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", err.Query()).Err(err.Wrapped()).Msg("get tests")
		return c.Status(err.Code()).SendString(err.Error())
	}

	return c.JSON(tests)
}

func (app *App) getHttpSamplesDurations(c *fiber.Ctx) error {
	var filters dbs.SampleFilter

	if err := c.BodyParser(&filters); err != nil {
		if err != fiber.ErrUnprocessableEntity && len(c.Request().Body()) > 0 {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}
	}

	samples, err := app.db.GetHttpSamplesDurations(filters)
	if err != nil {
		app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", err.Query()).Err(err.Wrapped()).Msg("get tests")
		return c.Status(err.Code()).SendString(err.Error())
	}

	return c.JSON(samples)
}

func (app *App) getHttpSamplesStatus(c *fiber.Ctx) error {
	var filters dbs.SampleFilter

	if err := c.BodyParser(&filters); err != nil {
		if err != fiber.ErrUnprocessableEntity && len(c.Request().Body()) > 0 {
			return c.Status(http.StatusBadRequest).SendString(err.Error())
		}
	}

	samples, err := app.db.GetHttpSamplesStatus(filters)
	if err != nil {
		app.logger.Error().Uint64("id", c.Context().ID()).Str("sql", err.Query()).Err(err.Wrapped()).Msg("get tests")
		return c.Status(err.Code()).SendString(err.Error())
	}

	return c.JSON(samples)
}
