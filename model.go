package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

type EntryBatch struct {
	CommitID string        `json:"commit_id,omitempty"`
	Entries  []LedgerEntry `json:"entries"`
}

type LedgerEntry struct {
	TimestampMS      int64          `json:"timestamp_ms"`
	RouteKey         string         `json:"route_key"`
	ClientID         string         `json:"client_id"`
	HitVersion       string         `json:"hit_version"`
	SnapshotVersion  int64          `json:"snapshot_version,omitempty"`
	Candidates       []string       `json:"candidates"`
	CandidateWeights map[string]int `json:"candidate_weights,omitempty"`
	Weights          map[string]int `json:"weights"`
	RequestWeight    int64          `json:"request_weight"`
}

type WeightSnapshot struct {
	CommitID        string                    `json:"commit_id,omitempty"`
	SnapshotVersion int64                     `json:"snapshot_version"`
	GeneratedAtMS   int64                     `json:"generated_at_ms"`
	Weights         map[string]map[string]int `json:"weights"`
	Digest          string                    `json:"digest,omitempty"`
}

type MigrationFragment struct {
	CommitID      string `json:"commit_id,omitempty"`
	MigrationID   string `json:"migration_id"`
	BaseVersion   int64  `json:"base_version"`
	RouteKey      string `json:"route_key"`
	FromVersion   string `json:"from_version"`
	ToVersion     string `json:"to_version"`
	RatioPermille int    `json:"ratio_permille"`
	StartAtMS     int64  `json:"start_at_ms"`
	TransitionMS  int64  `json:"transition_ms"`
}

type MigrationRecord struct {
	MigrationFragment
	EndAtMS     int64  `json:"end_at_ms"`
	ViewVersion int64  `json:"view_version"`
	Status      string `json:"status"`
	Phase       string `json:"phase,omitempty"`
}

type ValidationIssue struct {
	Index    *int   `json:"index,omitempty"`
	RouteKey string `json:"route_key,omitempty"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

type RoundingInfo struct {
	DecimalPlaces              int    `json:"decimal_places"`
	Mode                       string `json:"mode"`
	ExpectedCumulativeError    string `json:"expected_cumulative_rounding_error"`
	ActualCumulativeError      string `json:"actual_cumulative_rounding_error"`
	ExpectedCumulativeAbsolute string `json:"expected_cumulative_abs_error,omitempty"`
}

type VersionConservation struct {
	Version                string `json:"version"`
	EntryCount             int64  `json:"entry_count"`
	HitWeight              int64  `json:"hit_weight"`
	ActualRatio            string `json:"actual_ratio"`
	ExpectedRatio          string `json:"expected_ratio"`
	DeclaredExpectedRatio  string `json:"declared_expected_ratio,omitempty"`
	SnapshotWeightPermille int    `json:"snapshot_weight_permille,omitempty"`
	SnapshotRatio          string `json:"snapshot_ratio,omitempty"`
	AbsDeviation           string `json:"abs_deviation"`
	WithinTolerance        bool   `json:"within_tolerance"`
}

type ConservationView struct {
	RouteKey           string                `json:"route_key"`
	ViewVersion        int64                 `json:"view_version"`
	SnapshotVersion    int64                 `json:"snapshot_version"`
	AsOfMS             int64                 `json:"as_of_ms"`
	TotalRequestWeight int64                 `json:"total_request_weight"`
	EntryCount         int64                 `json:"entry_count"`
	HasData            bool                  `json:"has_data"`
	Tolerance          string                `json:"tolerance"`
	Rounding           RoundingInfo          `json:"rounding"`
	Versions           []VersionConservation `json:"versions"`
	Balanced           bool                  `json:"balanced"`
	EffectiveWeights   map[string]int        `json:"effective_weights_permille,omitempty"`
	MigrationID        string                `json:"migration_id,omitempty"`
	MigrationPhase     string                `json:"migration_phase,omitempty"`
	ViewHash           string                `json:"view_hash"`
}

type CommitResult struct {
	CommitID         string `json:"commit_id"`
	Kind             string `json:"kind"`
	ViewVersion      int64  `json:"view_version"`
	SnapshotVersion  int64  `json:"snapshot_version,omitempty"`
	Accepted         int    `json:"accepted,omitempty"`
	IdempotentReplay bool   `json:"idempotent_replay,omitempty"`
}

type staleReadContext struct {
	ViewVersion     int64 `json:"view_version"`
	ObservedVersion int64 `json:"observed_version"`
}

func SnapshotDigest(snapshot WeightSnapshot) string {
	payload := struct {
		SnapshotVersion int64                     `json:"snapshot_version"`
		GeneratedAtMS   int64                     `json:"generated_at_ms"`
		Weights         map[string]map[string]int `json:"weights"`
	}{
		SnapshotVersion: snapshot.SnapshotVersion,
		GeneratedAtMS:   snapshot.GeneratedAtMS,
		Weights:         snapshot.Weights,
	}
	buf, _ := json.Marshal(payload)
	sum := sha256.Sum256(buf)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func canonicalHash(value any) string {
	buf, _ := json.Marshal(value)
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
