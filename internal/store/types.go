package store

import (
	"time"

	"example.com/gsb-router/internal/model"
	"example.com/gsb-router/internal/routing"
)

type Boundary struct {
	RangeID        string `json:"range_id"`
	PhysicalTable  string `json:"physical_table"`
	Lower          int64  `json:"lower"`
	Upper          int64  `json:"upper"`
	LowerInclusive bool   `json:"lower_inclusive"`
	UpperInclusive bool   `json:"upper_inclusive"`
	Weight         int64  `json:"weight"`
}

type CacheRef struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type RouteCache struct {
	TableID         string     `json:"table_id"`
	CacheVersion    int64      `json:"cache_version"`
	ExpectedVersion int64      `json:"expected_version"`
	PhysicalTables  []string   `json:"physical_tables"`
	BoundaryIndex   []Boundary `json:"boundary_index"`
	Checksum        string     `json:"checksum"`
	BuiltAt         string     `json:"built_at"`
	Refs            []CacheRef `json:"refs,omitempty"`
}

type AggregateCache struct {
	ViewID          string     `json:"view_id"`
	CacheVersion    int64      `json:"cache_version"`
	ExpectedVersion int64      `json:"expected_version"`
	DependsOn       []string   `json:"depends_on"`
	PhysicalTables  []string   `json:"physical_tables"`
	Payload         any        `json:"payload"`
	BuiltAt         string     `json:"built_at"`
	Refs            []CacheRef `json:"refs,omitempty"`
}

type CommitResult struct {
	CommitID       string   `json:"commit_id"`
	Kind           string   `json:"kind"`
	Version        int64    `json:"version"`
	Checksum       string   `json:"checksum"`
	AffectedTables []string `json:"affected_tables"`
	AffectedViews  []string `json:"affected_views"`
	Reused         bool     `json:"reused"`
	CommittedAt    string   `json:"committed_at"`
}

type SnapshotCommitRequest struct {
	CommitID     string         `json:"commit_id,omitempty"`
	Snapshot     model.Snapshot `json:"snapshot"`
	SealChecksum bool           `json:"seal_checksum,omitempty"`
	Inject       string         `json:"inject,omitempty"`
}

type RouteResponse struct {
	routing.Result
	ObservedVersion int64 `json:"observed_version"`
	CurrentVersion  int64 `json:"current_version"`
	CacheVersion    int64 `json:"cache_version"`
	ExpectedVersion int64 `json:"expected_version"`
	UsedVersion     int64 `json:"used_version"`
	VersionBound    bool  `json:"version_bound"`
	Deterministic   bool  `json:"deterministic"`
}

type StateSummary struct {
	CurrentVersion int64            `json:"current_version"`
	Snapshot       *model.Snapshot  `json:"snapshot,omitempty"`
	Tables         []TableState     `json:"tables"`
	Aggregates     []AggregateCache `json:"aggregates"`
	LogicalDBCount int              `json:"logical_db_count"`
	PhysicalTables int              `json:"physical_table_count"`
	CacheAligned   bool             `json:"cache_aligned"`
}

type TableState struct {
	TableID      string   `json:"table_id"`
	LogicalDB    string   `json:"logical_db"`
	ShardKey     string   `json:"shard_key"`
	Algorithm    string   `json:"algorithm"`
	RangeCount   int      `json:"range_count"`
	Physicals    []string `json:"physical_tables"`
	CacheVersion int64    `json:"cache_version"`
	Expected     int64    `json:"expected_version"`
}

type IntentRecord struct {
	ID            string `json:"id"`
	CommitID      string `json:"commit_id"`
	Kind          string `json:"kind"`
	BaseVersion   int64  `json:"base_version"`
	TargetVersion int64  `json:"target_version"`
	Phase         string `json:"phase"`
	Status        string `json:"status"`
	CreatedAt     string `json:"created_at"`
	FinishedAt    string `json:"finished_at,omitempty"`
}

type CommitRecord struct {
	CommitID    string `json:"commit_id"`
	Kind        string `json:"kind"`
	Status      string `json:"status"`
	Version     int64  `json:"version"`
	CreatedAt   string `json:"created_at"`
	ContentHash string `json:"content_hash"`
}

type RecoveryAction struct {
	IntentID      string `json:"intent_id,omitempty"`
	CommitID      string `json:"commit_id,omitempty"`
	Kind          string `json:"kind,omitempty"`
	BaseVersion   int64  `json:"base_version,omitempty"`
	TargetVersion int64  `json:"target_version,omitempty"`
	Decision      string `json:"decision"`
	Reason        string `json:"reason"`
}

type RecoveryReport struct {
	GeneratedAt    string           `json:"generated_at"`
	CurrentVersion int64            `json:"current_version"`
	PendingFound   int              `json:"pending_intents_found"`
	Actions        []RecoveryAction `json:"actions"`
	CacheAction    string           `json:"cache_action"`
	CacheTables    []string         `json:"cache_tables,omitempty"`
	CacheViews     []string         `json:"cache_views,omitempty"`
	FactsOnly      bool             `json:"facts_only"`
	FactsOnlyNote  string           `json:"facts_only_note"`
}

func nowString(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
