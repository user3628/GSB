package storage

import (
	"database/sql"
	"strconv"
)

const (
	MetaCurrentVersion   = "current_version"
	MetaCommittedVersion = "committed_version"
	MetaLastCompleteVer  = "last_complete_version"
	MetaNextIntentSeq    = "next_intent_seq"
)

// GetMeta reads a meta value, returning "" with no error when absent.
func GetMeta(q queryable, key string) (string, error) {
	var v string
	err := q.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// GetMetaInt reads an integer meta value, returning def when absent.
func GetMetaInt(q queryable, key string, def int64) (int64, error) {
	v, err := GetMeta(q, key)
	if err != nil || v == "" {
		return def, err
	}
	return strconv.ParseInt(v, 10, 64)
}

// SetMeta upserts a meta value inside a transaction.
func SetMeta(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

// Querier is satisfied by both *sql.DB and *sql.Tx.
type Querier interface {
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

type queryable = Querier
