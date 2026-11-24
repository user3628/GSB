package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"mitmpipeline/internal/domain"
)

func (s *Store) GetRecord(ctx context.Context, canonicalID string) (*domain.Record, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT record_json FROM records WHERE canonical_id=?`, canonicalID).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, domain.NewError(domain.CodeNotFound, "imported record not found").WithContext("connection_id", canonicalID)
	}
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "query record")
	}
	var record domain.Record
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "parse record")
	}
	return &record, nil
}

func (s *Store) GetIdentity(ctx context.Context, canonicalID string) (string, string, error) {
	var sni, serial sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT sni,leaf_serial FROM connections WHERE canonical_id=?`, canonicalID).Scan(&sni, &serial)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", domain.WrapError(domain.CodeInternal, err, "query identity")
	}
	return sni.String, serial.String, nil
}
