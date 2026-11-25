package feedbackstream

import "context"

func (s *Store) ActivePartitionCounts(ctx context.Context) (map[int]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT partition_id, COUNT(*) FROM batches
		WHERE status IN ('RUNNING','BLOCKED') GROUP BY partition_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]int{}
	for rows.Next() {
		var partition, count int
		if err := rows.Scan(&partition, &count); err != nil {
			return nil, err
		}
		out[partition] = count
	}
	return out, rows.Err()
}
