package verify

import (
	"encoding/json"
	"os"
	"path/filepath"

	"edgeconfig/pkg/edgeconfig"
)

type ObservationCase struct {
	Name            string `json:"name"`
	NodeID          string `json:"node_id"`
	ObservedVersion int64  `json:"observed_version"`
	Digest          string `json:"digest"`
}

type ProbeSample struct {
	RoundID      string   `json:"round_id"`
	RequestID    string   `json:"request_id"`
	Targets      []string `json:"targets"`
	SuccessNode  string   `json:"success_node"`
	RejectedNode string   `json:"rejected_node"`
	TimeoutNode  string   `json:"timeout_node"`
	RejectStatus int      `json:"reject_status"`
	RejectCode   string   `json:"reject_code"`
	HangMS       int64    `json:"hang_ms"`
	TimeoutMS    int64    `json:"timeout_ms"`
	Concurrency  int      `json:"concurrency"`
}

type Observations struct {
	SnapshotFile        string            `json:"snapshot_file"`
	IncrementFile       string            `json:"increment_file"`
	ExpectedAffectedIDs []string          `json:"expected_affected_ids"`
	ExpectedDirectIDs   []string          `json:"expected_direct_ids"`
	ExpectedPath        string            `json:"expected_path"`
	StaleRead           ObservationCase   `json:"stale_read"`
	FreshReports        []ObservationCase `json:"fresh_reports"`
	Probe               ProbeSample       `json:"probe"`
	Recovery            RecoverySample    `json:"recovery"`
}

type RecoverySample struct {
	Scenarios []string `json:"scenarios"`
}

type SampleSet struct {
	Dir          string
	Topology     edgeconfig.Topology
	Policy       edgeconfig.Policy
	Snapshot     edgeconfig.Snapshot
	Increment    edgeconfig.Increment
	Observations Observations
	InvalidRaw   json.RawMessage
}

func LoadSampleSet(dir string) (*SampleSet, error) {
	set := &SampleSet{Dir: dir}
	if err := readJSON(filepath.Join(dir, "topology.json"), &set.Topology); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(dir, "tolerance.json"), &set.Policy); err != nil {
		return nil, err
	}
	set.Topology.Policy = set.Policy
	if err := readJSON(filepath.Join(dir, "snapshot-v1.json"), &set.Snapshot); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(dir, "increment-v2.json"), &set.Increment); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(dir, "observations.json"), &set.Observations); err != nil {
		return nil, err
	}
	invalidRaw, err := os.ReadFile(filepath.Join(dir, "invalid-topology.json"))
	if err != nil {
		return nil, err
	}
	set.InvalidRaw = invalidRaw
	return set, nil
}

func readJSON(path string, target any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}
