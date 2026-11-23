package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Op string

const (
	OpCreate Op = "create"
	OpRead   Op = "read"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
	OpGrant  Op = "grant"
	OpRevoke Op = "revoke"
)

const (
	CodeOK              = "OK"
	CodeWindowMalformed = "E_WINDOW_MALFORMED"
	CodeOrphanRef       = "E_ORPHAN_REF"
	CodeBudgetExhausted = "E_BUDGET_EXHAUSTED"
	CodeWindowBudget    = "E_WINDOW_BUDGET"
	CodeSnapshotVersion = "E_SNAPSHOT_VERSION"
)

type Verdict string

const (
	VerdictFull     Verdict = "full"
	VerdictDegraded Verdict = "degraded"
	VerdictDeferred Verdict = "deferred"
	VerdictRejected Verdict = "rejected"
)

type Event struct {
	ID         string `json:"id"`
	Subject    string `json:"subject"`
	Object     string `json:"object"`
	Op         Op     `json:"op"`
	TimeOffset int64  `json:"time_offset"`
	Weight     int    `json:"weight"`
}

type ResourceClaim struct {
	Depth  int   `json:"depth"`
	Memory int64 `json:"memory"`
}

type Window struct {
	ID     string        `json:"id"`
	Events []Event       `json:"events"`
	Claim  ResourceClaim `json:"claim"`
}

type BudgetParams struct {
	GlobalMemory int64
	GlobalCPU    int64
	WindowCPUMax int64
}

func (p BudgetParams) Fingerprint() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("mem=%d;cpu=%d;wmax=%d", p.GlobalMemory, p.GlobalCPU, p.WindowCPUMax)))
	return hex.EncodeToString(sum[:8])
}

// Conclusion is the forensic verdict record of one window. It is the unit
// committed per-window to SQLite and embedded into versioned snapshots.
type Conclusion struct {
	WindowID   string   `json:"window_id"`
	Verdict    Verdict  `json:"verdict"`
	Codes      []string `json:"codes"`
	Reason     string   `json:"reason,omitempty"`
	Chains     []Chain  `json:"chains,omitempty"`
	Orphans    []string `json:"orphans,omitempty"`
	Complexity int      `json:"complexity,omitempty"`
	Depth      int      `json:"depth,omitempty"`
	Digest     string   `json:"digest,omitempty"`
	CpuCost    int64    `json:"cpu_cost,omitempty"`
	MemCost    int64    `json:"mem_cost,omitempty"`
}
