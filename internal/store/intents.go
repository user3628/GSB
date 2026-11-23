package store

import "localinfer/workbench/internal/domain"

func (s *Store) ListIntents(limit int) ([]domain.IntentFact, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT id,kind,trace_id,run_id,batch_id,state,created_at,detail_json FROM intents ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.IntentFact, 0)
	for rows.Next() {
		var f domain.IntentFact
		if err := rows.Scan(&f.ID, &f.Kind, &f.TraceID, &f.RunID, &f.BatchID, &f.State, &f.CreatedAt, &f.Detail); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
