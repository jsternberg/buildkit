//go:build cgo && (linux || darwin)

package sqlcachestorage

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/mattn/go-sqlite3" // sqlite3 driver
)

func sqliteOpen(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dsn(dbPath))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func dsn(dbPath string) string {
	params := url.Values{
		"_journal_mode": []string{"WAL"},
		"_foreign_keys": []string{"on"},
	}
	return fmt.Sprintf("file:%s?%s", dbPath, params.Encode())
}
