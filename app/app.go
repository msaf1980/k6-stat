package app

import (
	"database/sql"
	"net/http"
	"os"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/msaf1980/fiberlog"
	"github.com/msaf1980/go-stringutils"
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
	Id     time.Time
	Name   string
	Params string
}

type TestFilter struct {
	From       int64  `json:"from,omitempty"`
	Until      int64  `json:"until,omitempty"`
	NamePrefix string `json:"name_prefix,omitempty"`
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

	app.Post("/api/tests", func(c *fiber.Ctx) error {
		var (
			query    stringutils.Builder
			filters  TestFilter
			filtered bool
		)

		filter := make([]any, 0, 3)
		tests := make([]Test, 0, 50)

		query.Grow(64)
		_, _ = query.WriteString("SELECT id, name, params FROM ")
		_, _ = query.WriteString(tableTests)

		if err := c.BodyParser(&filters); err != nil {
			if err != fiber.ErrUnprocessableEntity && len(c.Request().Body()) > 0 {
				return c.Status(http.StatusBadRequest).SendString(err.Error())
			}
		}

		if filters.From > 0 {
			if filtered {
				_, _ = query.WriteString(" AND id >= ?")
			} else {
				filtered = true
				_, _ = query.WriteString(" WHERE id >= ?")
			}
			filter = append(filter, time.Unix(filters.From, 0).UTC())
		} else if filters.From < 0 {
			return c.Status(http.StatusBadRequest).SendString(invalidFrom)
		}
		if filters.Until > 0 {
			if filtered {
				_, _ = query.WriteString(" AND id < ?")
			} else {
				filtered = true
				_, _ = query.WriteString(" WHERE id < ?")
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

		_, _ = query.WriteString(" ORDER BY id, name")
		rows, err := db.Query(query.String(), filter...)
		if err != nil {
			logger.Error().Uint64("id", c.Context().ID()).Str("sql", query.String()).Err(err).Msg("get tests")
			return fiber.ErrServiceUnavailable
		}
		defer rows.Close()
		for rows.Next() {
			var id time.Time
			var name, params string
			err = rows.Scan(&id, &name, &params)
			if err != nil {
				// handle this error
				return err
			}
			tests = append(tests, Test{Id: id, Name: name, Params: params})
		}
		// get any error encountered during iteration
		err = rows.Err()
		if err != nil {
			return err
		}

		return c.JSON(tests)
	})

	return &App{db: db, fiberApp: app, logger: logger, tableTests: tableTests, tableSamples: tableSamples}, nil
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
