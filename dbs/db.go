package dbs

import "database/sql"

type DB struct {
	db           *sql.DB
	tableTests   string
	tableSamples string
}

func New(db *sql.DB, tableTests, tableSamples string) *DB {
	return &DB{db: db, tableTests: tableTests, tableSamples: tableSamples}
}

func (d *DB) Close() error {
	return d.db.Close()
}
