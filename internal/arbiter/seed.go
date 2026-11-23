package arbiter

import (
	"context"
	"database/sql"

	"leasearbiter/internal/store"
)

// SeedRow is one deterministic initial business row.
type SeedRow struct {
	RowID   string `json:"row_id"`
	Content string `json:"content"`
}

// SeedResult reports the versions produced by seeding.
type SeedResult struct {
	Rows           []RowView `json:"rows"`
	FinalCommitVer int64     `json:"final_commit_ver"`
}

// Seed installs deterministic initial rows when the database is empty. Each
// row becomes row_version 1 at its own monotonically increasing commit
// version. Seeding an already populated database is a no-op so restarts are
// idempotent.
func (e *Engine) Seed(ctx context.Context, rows []SeedRow) (*SeedResult, error) {
	existing, err := e.st.AllRowIDs()
	if err != nil {
		return nil, err
	}
	if len(existing) > 0 {
		current, err := e.st.CommitVer()
		if err != nil {
			return nil, err
		}
		return &SeedResult{FinalCommitVer: current}, nil
	}
	nowMs := store.Milli(e.now())
	var views []RowView
	err = e.st.TxFunc(func(tx *sql.Tx) error {
		for _, r := range rows {
			commitVer, err := store.MetaTx(tx)
			if err != nil {
				return err
			}
			hash := contentFingerprint(r.Content, false)
			rv := store.RowVersion{
				RowID:       r.RowID,
				RowVersion:  1,
				CommitVer:   commitVer,
				ContentHash: hash,
				Content:     r.Content,
				CreatedAtMs: nowMs,
			}
			if err := store.InsertRowVersion(tx, rv); err != nil {
				return err
			}
			views = append(views, RowView{
				RowID: r.RowID, RowVersion: 1, CommitVer: commitVer,
				ContentHash: hash, Content: r.Content, SnapshotVer: commitVer,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	current, err := e.st.CommitVer()
	if err != nil {
		return nil, err
	}
	for i := range views {
		views[i].CurrentVer = current
	}
	return &SeedResult{Rows: views, FinalCommitVer: current}, nil
}
