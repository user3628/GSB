package store

import (
	"context"
	"database/sql"

	"gpusched/internal/domain"
)

func (s *Store) UpsertGroupTx(tx *sql.Tx, g domain.Group) error {
	_, err := tx.Exec(
		`INSERT INTO quota_groups(id, memory_limit, compute_limit_m, memory_reserved, compute_reserved)
		 VALUES(?,?,?,0,0)
		 ON CONFLICT(id) DO UPDATE SET memory_limit=excluded.memory_limit, compute_limit_m=excluded.compute_limit_m`,
		g.ID, g.MemoryLimit, g.ComputeLimitM)
	return err
}

func (s *Store) UpsertNodeTx(tx *sql.Tx, n domain.Node) error {
	_, err := tx.Exec(
		`INSERT INTO nodes(id, group_id, memory_total, compute_total_m, memory_used, compute_used_m, health)
		 VALUES(?,?,?,?,0,0,?)
		 ON CONFLICT(id) DO UPDATE SET
		   group_id=excluded.group_id, memory_total=excluded.memory_total,
		   compute_total_m=excluded.compute_total_m, health=excluded.health`,
		n.ID, n.GroupID, n.MemoryTotal, n.ComputeTotalM, n.Health)
	return err
}

func (s *Store) GetGroup(ctx context.Context, id string) (domain.Group, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, memory_limit, compute_limit_m, memory_reserved, compute_reserved, pick_counter
		 FROM quota_groups WHERE id=?`, id)
	var g domain.Group
	err := row.Scan(&g.ID, &g.MemoryLimit, &g.ComputeLimitM, &g.MemoryReserved, &g.ComputeReserved, &g.PickCounter)
	return g, err
}

func (s *Store) GetGroupTx(tx *sql.Tx, id string) (domain.Group, error) {
	row := tx.QueryRow(
		`SELECT id, memory_limit, compute_limit_m, memory_reserved, compute_reserved, pick_counter
		 FROM quota_groups WHERE id=?`, id)
	var g domain.Group
	err := row.Scan(&g.ID, &g.MemoryLimit, &g.ComputeLimitM, &g.MemoryReserved, &g.ComputeReserved, &g.PickCounter)
	return g, err
}

func (s *Store) ListGroups(ctx context.Context) ([]domain.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, memory_limit, compute_limit_m, memory_reserved, compute_reserved, pick_counter
		 FROM quota_groups ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGroups(rows)
}

func (s *Store) ListNodesByGroupTx(tx *sql.Tx, groupID string) ([]domain.Node, error) {
	rows, err := tx.Query(
		`SELECT id, group_id, memory_total, compute_total_m, memory_used, compute_used_m, health
		 FROM nodes WHERE group_id=? ORDER BY id`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

func (s *Store) ListNodes(ctx context.Context) ([]domain.Node, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, group_id, memory_total, compute_total_m, memory_used, compute_used_m, health
		 FROM nodes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

func (s *Store) GetNode(ctx context.Context, id string) (domain.Node, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, group_id, memory_total, compute_total_m, memory_used, compute_used_m, health
		 FROM nodes WHERE id=?`, id)
	var n domain.Node
	err := row.Scan(&n.ID, &n.GroupID, &n.MemoryTotal, &n.ComputeTotalM, &n.MemoryUsed, &n.ComputeUsedM, &n.Health)
	return n, err
}

func (s *Store) GetNodeTx(tx *sql.Tx, id string) (domain.Node, error) {
	row := tx.QueryRow(
		`SELECT id, group_id, memory_total, compute_total_m, memory_used, compute_used_m, health
		 FROM nodes WHERE id=?`, id)
	var n domain.Node
	err := row.Scan(&n.ID, &n.GroupID, &n.MemoryTotal, &n.ComputeTotalM, &n.MemoryUsed, &n.ComputeUsedM, &n.Health)
	return n, err
}

// AdjustGroupReservation changes reserved group resources. Negative deltas free
// reservations. A CHECK-style guard ensures counters never go negative.
func AdjustGroupReservation(tx *sql.Tx, groupID string, memDelta, computeDelta int64) error {
	res, err := tx.Exec(
		`UPDATE quota_groups
		 SET memory_reserved=memory_reserved+?, compute_reserved=compute_reserved+?
		 WHERE id=? AND memory_reserved+? >= 0 AND compute_reserved+? >= 0`,
		memDelta, computeDelta, groupID, memDelta, computeDelta)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return domain.NewError(domain.EQuotaLeak,
			"group reservation would turn negative", map[string]any{"group_id": groupID})
	}
	return nil
}

// AdjustNodeUsage changes the resources consumed on a physical node.
func AdjustNodeUsage(tx *sql.Tx, nodeID string, memDelta, computeDelta int64) error {
	res, err := tx.Exec(
		`UPDATE nodes
		 SET memory_used=memory_used+?, compute_used_m=compute_used_m+?
		 WHERE id=? AND memory_used+? >= 0 AND compute_used_m+? >= 0
		   AND memory_used+? <= memory_total AND compute_used_m+? <= compute_total_m`,
		memDelta, computeDelta, nodeID, memDelta, computeDelta, memDelta, computeDelta)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return domain.NewError(domain.EQuotaLeak,
			"node usage would turn negative or exceed capacity", map[string]any{"node_id": nodeID})
	}
	return nil
}

// BumpPickCounter advances the round-robin selection counter for a group.
func BumpPickCounter(tx *sql.Tx, groupID string) error {
	_, err := tx.Exec(`UPDATE quota_groups SET pick_counter=pick_counter+1 WHERE id=?`, groupID)
	return err
}

type rowsScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanGroups(rows rowsScanner) ([]domain.Group, error) {
	var out []domain.Group
	for rows.Next() {
		var g domain.Group
		if err := rows.Scan(&g.ID, &g.MemoryLimit, &g.ComputeLimitM,
			&g.MemoryReserved, &g.ComputeReserved, &g.PickCounter); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func scanNodes(rows rowsScanner) ([]domain.Node, error) {
	var out []domain.Node
	for rows.Next() {
		var n domain.Node
		if err := rows.Scan(&n.ID, &n.GroupID, &n.MemoryTotal, &n.ComputeTotalM,
			&n.MemoryUsed, &n.ComputeUsedM, &n.Health); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
