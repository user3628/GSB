package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"cdn-edge-consistency/internal/cache"
	"cdn-edge-consistency/internal/model"
)

type SchedulerPolicy struct {
	GlobalOriginConcurrency int   `json:"global_origin_concurrency"`
	PerOriginConcurrency    int   `json:"per_origin_concurrency"`
	QueueCapacity           int   `json:"queue_capacity"`
	RoundBudgetMs           int64 `json:"round_budget_ms"`
	DefaultTaskCostMs       int64 `json:"default_task_cost_ms"`
}

type Snapshot struct {
	Version             int64           `json:"version"`
	GeneratedAt         string          `json:"generated_at"`
	Nodes               []string        `json:"nodes"`
	TTLMs               int64           `json:"ttl_ms"`
	SWRMs               int64           `json:"swr_ms"`
	RevalidateWindowMs  int64           `json:"revalidate_window_ms"`
	PurgeWindowMs       int64           `json:"purge_window_ms"`
	OriginTimeoutMs     int64           `json:"origin_timeout_ms"`
	RevalidateTimeoutMs int64           `json:"revalidate_timeout_ms"`
	Vary                []string        `json:"vary"`
	KeyRules            cache.KeyRules  `json:"key_rules"`
	Scheduler           SchedulerPolicy `json:"scheduler"`
	Checksum            string          `json:"checksum"`
	CommittedAtMs       int64           `json:"committed_at_ms,omitempty"`
}

func (s *Snapshot) Clone() *Snapshot {
	if s == nil {
		return nil
	}
	cp := *s
	cp.Nodes = append([]string(nil), s.Nodes...)
	cp.Vary = append([]string(nil), s.Vary...)
	return &cp
}

func (s *Snapshot) CanonicalChecksum() (string, error) {
	cp := *s
	cp.Checksum = ""
	cp.CommittedAtMs = 0
	data, err := json.Marshal(cp)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Snapshot) NormalizeAndValidate(allowedNodes map[string]model.NodeConfig) error {
	if s.Version <= 0 {
		return model.NewBiz(model.CodeSnapshotInvalid, "version must be greater than zero")
	}
	if _, err := time.Parse(time.RFC3339Nano, s.GeneratedAt); err != nil {
		return model.WithDetails(model.NewBiz(model.CodeSnapshotInvalid, "generated_at must be RFC3339"), map[string]any{"value": s.GeneratedAt})
	}
	if len(s.Nodes) == 0 {
		return model.NewBiz(model.CodeSnapshotInvalid, "nodes must not be empty")
	}
	seen := map[string]bool{}
	nodes := make([]string, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		n = strings.TrimSpace(n)
		if n == "" || seen[n] {
			return model.WithDetails(model.NewBiz(model.CodeSnapshotInvalid, "nodes must be unique and non-empty"), map[string]any{"node": n})
		}
		if _, ok := allowedNodes[n]; !ok {
			return model.WithDetails(model.NewBiz(model.CodeSnapshotInvalid, "policy references unknown node"), map[string]any{"node": n})
		}
		seen[n] = true
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	s.Nodes = nodes
	s.Vary = canonicalNames(s.Vary)
	if s.TTLMs < 0 || s.SWRMs < 0 || s.RevalidateWindowMs < 0 || s.PurgeWindowMs < 0 || s.OriginTimeoutMs <= 0 || s.RevalidateTimeoutMs <= 0 {
		return model.NewBiz(model.CodeSnapshotInvalid, "TTL/window values must be non-negative and timeouts must be positive")
	}
	if s.Scheduler.GlobalOriginConcurrency <= 0 || s.Scheduler.PerOriginConcurrency <= 0 {
		return model.NewBiz(model.CodeSnapshotInvalid, "scheduler concurrency limits must be positive")
	}
	if s.Scheduler.QueueCapacity < 0 {
		return model.NewBiz(model.CodeSnapshotInvalid, "queue_capacity must be non-negative")
	}
	if s.Scheduler.RoundBudgetMs <= 0 || s.Scheduler.DefaultTaskCostMs <= 0 {
		return model.NewBiz(model.CodeSnapshotInvalid, "round_budget_ms and default_task_cost_ms must be positive")
	}
	if s.Scheduler.PerOriginConcurrency > s.Scheduler.GlobalOriginConcurrency {
		return model.NewBiz(model.CodeSnapshotInvalid, "per_origin_concurrency cannot exceed global_origin_concurrency")
	}
	return nil
}

func (s *Snapshot) Seal() error {
	want, err := s.CanonicalChecksum()
	if err != nil {
		return err
	}
	if s.Checksum != "" && !strings.EqualFold(s.Checksum, want) {
		return model.WithDetails(model.NewBiz(model.CodeSnapshotChecksum, "snapshot checksum does not match canonical payload"), map[string]any{
			"provided": s.Checksum,
			"computed": want,
		})
	}
	s.Checksum = want
	return nil
}

func canonicalNames(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (s *Snapshot) CoversNode(nodeID string) bool {
	for _, n := range s.Nodes {
		if n == nodeID {
			return true
		}
	}
	return false
}

func EffectiveOriginTimeout(policyMs, configMs int64) int64 {
	if policyMs > 0 {
		return policyMs
	}
	if configMs > 0 {
		return configMs
	}
	return 1000
}

func EffectiveRevalidateTimeout(policyMs, configMs int64) int64 {
	if policyMs > 0 {
		return policyMs
	}
	if configMs > 0 {
		return configMs
	}
	return 1500
}

func VersionKey(v int64) string { return fmt.Sprintf("v%d", v) }
