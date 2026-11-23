package configimpact

import "encoding/json"

// Document is the complete, versioned input document. Loading a document is
// all-or-nothing: validation completes before any persisted state changes.
type Document struct {
	Version    int64       `json:"version"`
	Groups     []Group     `json:"groups"`
	Components []Component `json:"components"`
}

// Group supplies defaults inherited by every component in the group.
type Group struct {
	ID       string      `json:"id"`
	Defaults []ConfigKey `json:"defaults,omitempty"`
}

// Component declares its group, dependency edges, watched key paths and
// explicitly owned configuration values.
type Component struct {
	ID          string      `json:"id"`
	Group       string      `json:"group"`
	DependsOn   []string    `json:"depends_on,omitempty"`
	Watches     []string    `json:"watches,omitempty"`
	Config      []ConfigKey `json:"config,omitempty"`
	Description string      `json:"description,omitempty"`
}

// ConfigKey is one configuration declaration. Order is the effective order
// number; a larger value wins when the same key is provided more than once.
type ConfigKey struct {
	Key   string          `json:"key"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
	Order int64           `json:"order"`
}

// PatchChange targets one configuration declaration. Scope is "component" or
// "group". When Scope is empty the engine infers it, but only if exactly one
// declaration of Key exists. Type may be omitted when the key already exists.
type PatchChange struct {
	Key       string          `json:"key"`
	Type      string          `json:"type,omitempty"`
	Value     json.RawMessage `json:"value"`
	Order     int64           `json:"order"`
	Scope     string          `json:"scope,omitempty"`
	Group     string          `json:"group,omitempty"`
	Component string          `json:"component,omitempty"`
}

// Patch is a key-level, base-version-bound change set.
type Patch struct {
	ID          string        `json:"id"`
	BaseVersion int64         `json:"base_version"`
	Changes     []PatchChange `json:"changes"`
}

// Source describes one candidate contribution to an effective key. Sources is
// sorted by precedence and the first entry is also the winner.
type Source struct {
	Kind       string          `json:"kind"` // component or group
	Component  string          `json:"component"`
	Owner      string          `json:"owner"`
	Key        string          `json:"key"`
	Type       string          `json:"type"`
	Value      json.RawMessage `json:"value"`
	Order      int64           `json:"order"`
	Distance   int             `json:"dependency_distance"`
	Precedence int             `json:"precedence"`
	Via        []string        `json:"via"`
	Winner     bool            `json:"winner,omitempty"`
}

// EffectiveValue is a resolved value plus its full, auditable source chain.
type EffectiveValue struct {
	Key        string          `json:"key"`
	Type       string          `json:"type"`
	Value      json.RawMessage `json:"value"`
	Order      int64           `json:"order"`
	Winner     Source          `json:"winner"`
	Sources    []Source        `json:"sources"`
	Resolution string          `json:"resolution"`
}

// MaterializationDecision captures the explicit cost trade-off.
type MaterializationDecision struct {
	Path                string  `json:"path"` // incremental, full, or reuse
	Threshold           float64 `json:"threshold"`
	Ratio               float64 `json:"ratio"`
	TouchedKeys         int     `json:"touched_keys"`
	WatchKeys           int     `json:"watch_keys"`
	EstimatedDurationNS int64   `json:"estimated_duration_ns"`
	ActualDurationNS    int64   `json:"actual_duration_ns"`
}

// MaterializedConfig is the persisted effective configuration for one
// component in one committed document snapshot.
type MaterializedConfig struct {
	SnapshotVersion     int64                     `json:"snapshot_version"`
	ComponentID         string                    `json:"component_id"`
	MaterializedVersion int64                     `json:"materialized_version"`
	Values              map[string]EffectiveValue `json:"values"`
	Undefined           []string                  `json:"undefined_keys"`
	Decision            MaterializationDecision   `json:"decision"`
}

// AffectedComponent is one entry in a patch's impact frontier.
type AffectedComponent struct {
	ComponentID         string   `json:"component_id"`
	TriggerKeys         []string `json:"trigger_keys"`
	CauseSeeds          []string `json:"cause_seeds"`
	Hops                int      `json:"hops"`
	Downstream          []string `json:"downstream"`
	Path                string   `json:"path"`
	Threshold           float64  `json:"threshold"`
	Ratio               float64  `json:"ratio"`
	EstimatedDurationNS int64    `json:"estimated_duration_ns"`
	ActualDurationNS    int64    `json:"actual_duration_ns"`
}

// ImpactReport answers "what must be rebuilt, why, and how far did it spread".
type ImpactReport struct {
	AffectedComponents []AffectedComponent `json:"affected_components"`
	Downstream         map[string][]string `json:"downstream"`
}

// PatchResult is persisted verbatim for idempotent replay.
type PatchResult struct {
	PatchID          string               `json:"patch_id"`
	BaseVersion      int64                `json:"base_version"`
	DocumentVersion  int64                `json:"document_version"`
	Impact           ImpactReport         `json:"impact"`
	Materializations []MaterializedConfig `json:"materializations"`
	Duplicate        bool                 `json:"duplicate"`
}

// LoadResult reports whether a document load changed the active snapshot.
type LoadResult struct {
	Version         int64  `json:"version"`
	PreviousVersion int64  `json:"previous_version"`
	Changed         bool   `json:"changed"`
	ComponentCount  int    `json:"component_count"`
	DocumentHash    string `json:"document_hash"`
}

// ReadRequest binds a point-in-time view to an observation version.
type ReadRequest struct {
	ComponentID     string `json:"component_id"`
	ObservedVersion int64  `json:"observed_version"`
}

// ReadResult always states the version at which the returned materialization
// was actually computed. SnapshotVersion is the observation snapshot.
type ReadResult struct {
	ComponentID         string             `json:"component_id"`
	ObservedVersion     int64              `json:"observed_version"`
	SnapshotVersion     int64              `json:"snapshot_version"`
	MaterializedVersion int64              `json:"materialized_version"`
	Config              MaterializedConfig `json:"config"`
}

// RecoveredIntent contains only facts that were read back from SQLite.
type RecoveredIntent struct {
	IntentID      int64  `json:"intent_id"`
	Kind          string `json:"kind"`
	PatchID       string `json:"patch_id,omitempty"`
	BaseVersion   int64  `json:"base_version"`
	TargetVersion int64  `json:"target_version"`
	Status        string `json:"status"`
	FinishedAt    string `json:"finished_at"`
}

// RecoveryReport is produced during engine startup. It never claims a rebuild
// succeeded unless a complete version row was actually persisted.
type RecoveryReport struct {
	ScanPerformed               bool              `json:"scan_performed"`
	PendingIntentsScanned       int               `json:"pending_intents_scanned"`
	AbortedIntents              []RecoveredIntent `json:"aborted_intents"`
	LastCompleteVersion         int64             `json:"last_complete_version"`
	CompleteDocumentVersions    []int64           `json:"complete_document_versions"`
	DiscardedIncompleteVersions []int64           `json:"discarded_incomplete_versions,omitempty"`
	OrphanSnapshotRows          int64             `json:"orphan_snapshot_rows"`
}

// PatchRef is the reference shape persisted in the refs table.
type PatchRef struct {
	ComponentID string `json:"component_id"`
	Kind        string `json:"kind"`
	TargetID    string `json:"target_id"`
	Key         string `json:"key"`
}
