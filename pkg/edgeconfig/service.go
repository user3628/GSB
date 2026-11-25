package edgeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sync"
)

type Service struct {
	store *Store
	db    *sql.DB

	writeMu sync.Mutex
	nowFunc func() int64
}

func NewService(store *Store) *Service {
	return &Service{store: store, db: store.DB(), nowFunc: nowMS}
}

func (s *Service) Store() *Store { return s.store }

func (s *Service) NowMS() int64 {
	if s.nowFunc != nil {
		return s.nowFunc()
	}
	return nowMS()
}

type TopologyResult struct {
	BatchID   string `json:"batch_id"`
	KeyCount  int    `json:"key_count"`
	NodeCount int    `json:"node_count"`
	Persisted bool   `json:"persisted"`
}

type CommitResult struct {
	Kind        string          `json:"kind"`
	CommitID    string          `json:"commit_id"`
	Version     int64           `json:"version"`
	BaseVersion int64           `json:"base_version,omitempty"`
	Digest      string          `json:"digest,omitempty"`
	Affected    *AffectedReport `json:"affected,omitempty"`
	Views       []ExpectedView  `json:"views,omitempty"`
	Consistency *Consistency    `json:"consistency,omitempty"`
}

func (s *Service) CurrentVersion(ctx context.Context) (int64, error) {
	return s.store.metaInt(ctx, "current_version")
}

func (s *Service) Policy(ctx context.Context) (Policy, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='policy_json'`).Scan(&raw)
	if err == sql.ErrNoRows {
		return DefaultPolicy(), nil
	}
	if err != nil {
		return DefaultPolicy(), err
	}
	var policy Policy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return DefaultPolicy(), err
	}
	return withDefaultPolicy(policy), nil
}

func (s *Service) SaveTopology(ctx context.Context, topology Topology) (*TopologyResult, error) {
	if err := ValidateTopology(topology); err != nil {
		return nil, err
	}
	policy := withDefaultPolicy(topology.Policy)
	topology.Policy = policy
	return runWrite(s, ctx, "topology_load", topologyBatchID(topology), topology, func(job *writeJob[TopologyResult]) (*TopologyResult, *APIError) {
		result := &TopologyResult{BatchID: job.requestID, KeyCount: len(topology.Keys), NodeCount: len(topology.Nodes), Persisted: true}
		apiErr := job.Complete(func(tx *sql.Tx) error {
			now := s.NowMS()
			if _, err := tx.ExecContext(job.ctx, `DELETE FROM node_dependencies`); err != nil {
				return err
			}
			if _, err := tx.ExecContext(job.ctx, `DELETE FROM subscriptions`); err != nil {
				return err
			}
			if _, err := tx.ExecContext(job.ctx, `DELETE FROM nodes`); err != nil {
				return err
			}
			if _, err := tx.ExecContext(job.ctx, `DELETE FROM config_keys`); err != nil {
				return err
			}
			for _, key := range topology.Keys {
				valueJSON, err := json.Marshal(key.Value)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(job.ctx, `
					INSERT INTO config_keys(path, type, value_json, order_no, updated_version)
					VALUES(?, ?, ?, ?, 0)`, key.Path, key.Type, string(valueJSON), key.OrderNo); err != nil {
					return err
				}
			}
			for _, node := range topology.Nodes {
				payload, err := json.Marshal(node)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(job.ctx, `
					INSERT INTO nodes(id, region, endpoint, payload_json) VALUES(?, ?, ?, ?)`,
					node.ID, node.Region, node.Endpoint, string(payload)); err != nil {
					return err
				}
				for position, keyPath := range node.SubscribedKeys {
					if _, err := tx.ExecContext(job.ctx, `
						INSERT INTO subscriptions(node_id, key_path, position) VALUES(?, ?, ?)`,
						node.ID, keyPath, position); err != nil {
						return err
					}
				}
				for _, dep := range node.DependsOn {
					if _, err := tx.ExecContext(job.ctx, `
						INSERT INTO node_dependencies(node_id, depends_on) VALUES(?, ?)`, node.ID, dep); err != nil {
						return err
					}
				}
				weight := policy.Numeric.DefaultWeight
				if declared, ok := policy.Numeric.NodeWeights[node.ID]; ok {
					weight = declared
				}
				if _, err := tx.ExecContext(job.ctx, `
					INSERT INTO node_convergence(node_id, region, weight, updated_at_ms)
					VALUES(?, ?, ?, ?)
					ON CONFLICT(node_id) DO UPDATE SET region=excluded.region, weight=excluded.weight, updated_at_ms=excluded.updated_at_ms`,
					node.ID, node.Region, weight, now); err != nil {
					return err
				}
			}
			if _, err := tx.ExecContext(job.ctx, `
				DELETE FROM node_convergence
				WHERE node_id NOT IN (SELECT id FROM nodes)`); err != nil {
				return err
			}
			policyJSON, err := json.Marshal(policy)
			if err != nil {
				return err
			}
			if err := s.store.setMetaTx(job.ctx, tx, "policy_json", string(policyJSON)); err != nil {
				return err
			}
			return s.store.setMetaTx(job.ctx, tx, "topology_loaded_at_ms", now)
		}, result, 0)
		if apiErr != nil {
			return nil, apiErr
		}
		return result, nil
	})
}

func topologyBatchID(topology Topology) string {
	if topology.BatchID != "" {
		return topology.BatchID
	}
	hash, _ := RequestHash(topology)
	if len(hash) > 23 {
		return "hash-" + hash[7:23]
	}
	return "hash-" + hash
}

func (s *Service) LoadTopology(ctx context.Context) (*Topology, error) {
	policy, err := s.Policy(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT path, type, value_json, order_no FROM config_keys ORDER BY path`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := make([]ConfigKey, 0)
	for rows.Next() {
		var key ConfigKey
		var valueJSON string
		if err := rows.Scan(&key.Path, &key.Type, &valueJSON, &key.OrderNo); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(valueJSON), &key.Value); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	nodeRows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM nodes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer nodeRows.Close()
	nodes := make([]Node, 0)
	for nodeRows.Next() {
		var payload string
		if err := nodeRows.Scan(&payload); err != nil {
			return nil, err
		}
		var node Node
		if err := json.Unmarshal([]byte(payload), &node); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, err
	}
	return &Topology{Keys: keys, Nodes: nodes, Policy: policy}, nil
}

func (s *Service) LoadGraph(ctx context.Context) (*Graph, Policy, error) {
	topology, err := s.LoadTopology(ctx)
	if err != nil {
		return nil, DefaultPolicy(), err
	}
	return NewGraph(topology.Keys, topology.Nodes, topology.Policy), topology.Policy, nil
}

func loadExpectedViews(ctx context.Context, db *sql.DB, version int64) (map[string]ExpectedView, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT commit_version, node_id, region, expected_version, expected_digest, key_count, digest_json
		FROM expected_views WHERE commit_version=? ORDER BY node_id`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]ExpectedView)
	for rows.Next() {
		var view ExpectedView
		if err := rows.Scan(&view.CommitVersion, &view.NodeID, &view.Region, &view.ExpectedVersion,
			&view.ExpectedDigest, &view.KeyCount, &view.DigestJSON); err != nil {
			return nil, err
		}
		result[view.NodeID] = view
	}
	return result, rows.Err()
}

func validationError(httpStatus int, code, message string, details any) *APIError {
	return NewAPIError(httpStatus, code, message, details)
}

var _ = http.StatusBadRequest
