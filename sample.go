package feedbackstream

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const (
	FixedSeed            int64 = 20261125
	FixedGeneratedAtMS   int64 = 1767225600000
	SchemaEventStream          = "feedback.event-stream/v1"
	SchemaPolicySnapshot       = "feedback.policy-snapshot/v1"
)

func sampleSignatures() (Signature, Signature, Signature) {
	event := Signature{
		Schema: "stream.event/v1",
		Fields: []FieldSignature{
			{Name: "timestamp_ms", Type: "int64_ms"},
			{Name: "partition_id", Type: "int"},
			{Name: "group_key", Type: "string"},
			{Name: "metric_fixed", Type: "int64_scale_1e6"},
			{Name: "sequence", Type: "int64"},
		},
	}
	projected := Signature{
		Schema: "stream.projected_event/v1",
		Fields: []FieldSignature{
			{Name: "window_start_ms", Type: "int64_ms"},
			{Name: "window_end_ms", Type: "int64_ms"},
			{Name: "partition_id", Type: "int"},
			{Name: "group_key", Type: "string"},
			{Name: "metric_fixed", Type: "int64_scale_1e6"},
		},
	}
	agg := Signature{
		Schema: "window.group_aggregate/v1",
		Fields: []FieldSignature{
			{Name: "window_start_ms", Type: "int64_ms"},
			{Name: "window_end_ms", Type: "int64_ms"},
			{Name: "group_key", Type: "string"},
			{Name: "count", Type: "int64"},
			{Name: "sum_fixed", Type: "int64_scale_1e6"},
			{Name: "min_fixed", Type: "int64_scale_1e6"},
			{Name: "max_fixed", Type: "int64_scale_1e6"},
		},
	}
	return event, projected, agg
}

func DefaultPolicy() *PolicySnapshot {
	eventSig, projectedSig, aggSig := sampleSignatures()
	rawSig := Signature{Schema: "source.raw_log/v1", Fields: []FieldSignature{{Name: "payload", Type: "bytes"}}}
	threshold := int64(5_000_000)
	p := &PolicySnapshot{
		SchemaVersion: SchemaPolicySnapshot,
		Version:       1,
		Kind:          "FULL",
		GeneratedAtMS: FixedGeneratedAtMS,
		Description:   "deterministic local operator fusion, backpressure and budget sample",
		MetricScale:   MetricScale,
		Window: WindowSpec{
			Type:          "tumbling",
			SizeMS:        1000,
			OriginMS:      FixedGeneratedAtMS,
			BoundaryRule:  BoundaryStartInclEndExcl,
			LatePolicy:    LateOriginalWindow,
			AllowedLateMS: 60_000,
		},
		Pipeline: PipelineSpec{Operators: []OperatorSpec{
			{
				ID: "scan", Kind: "scan", InputSignature: &rawSig, OutputSignature: eventSig,
			},
			{
				ID: "filter", Kind: "filter", InputSignature: &eventSig, OutputSignature: eventSig,
				Config: OperatorConfig{MetricGTE: &threshold},
			},
			{
				ID: "project", Kind: "project", InputSignature: &eventSig, OutputSignature: projectedSig,
				Config: OperatorConfig{SelectFields: []string{"window", "partition_id", "group_key", "metric_fixed"}},
			},
			{
				ID: "aggregate", Kind: "aggregate", InputSignature: &projectedSig, OutputSignature: aggSig,
				Config: OperatorConfig{
					GroupBy:      []string{"window", "group_key"},
					Aggregations: []string{"count", "sum", "min", "max"},
				},
			},
		}, Edges: []EdgeSpec{
			{From: "scan", To: "filter"},
			{From: "filter", To: "project"},
			{From: "project", To: "aggregate"},
		}},
		Fusion: FusionSpec{
			FusibleKinds: []string{"scan", "filter", "project", "aggregate"},
			Pairs: []FusionPair{
				{Left: "scan", Right: "filter"},
				{Left: "filter", Right: "project"},
			},
			MaxGroupSize: 3,
		},
		DefaultBuffer: BufferLimit{MaxInflightBatches: 4, MaxBytes: 64 * 1024, HighWatermarkRatio: 0.8},
		OperatorBuffers: map[string]BufferLimit{
			"fg-1":      {MaxInflightBatches: 8, MaxBytes: 1 << 20, HighWatermarkRatio: 0.8},
			"fg-2":      {MaxInflightBatches: 2, MaxBytes: 4096, HighWatermarkRatio: 0.75},
			"scan":      {MaxInflightBatches: 8, MaxBytes: 1 << 20, HighWatermarkRatio: 0.8},
			"filter":    {MaxInflightBatches: 8, MaxBytes: 1 << 20, HighWatermarkRatio: 0.8},
			"project":   {MaxInflightBatches: 8, MaxBytes: 1 << 20, HighWatermarkRatio: 0.8},
			"aggregate": {MaxInflightBatches: 2, MaxBytes: 4096, HighWatermarkRatio: 0.75},
		},
		PartitionConcurrency: 3,
		RoundTimeBudgetMS:    11,
		MaxWaitQueue:         2,
		CostModel:            CostModel{BaseCostMS: 1, PerEventCostMS: 1},
	}
	sum, err := PolicyChecksum(p)
	if err != nil {
		panic(err)
	}
	p.ChecksumSHA256 = sum
	return p
}

func DefaultEvents() []Event {
	keys := []string{"alpha", "beta", "gamma"}
	offsets := []int64{817, 143, 521, 947}
	events := make([]Event, 0, 36)
	sequences := map[int]int64{}
	for partition := 0; partition < 3; partition++ {
		for window := int64(0); window < 3; window++ {
			for j, offset := range offsets {
				sequences[partition]++
				raw := int64(2_000_000) + int64(partition*1_700_000) +
					int64(window*900_000) + int64(j*1_350_000)
				events = append(events, Event{
					TimestampMS: FixedGeneratedAtMS + window*1000 + offset,
					PartitionID: partition,
					GroupKey:    keys[(partition+int(window)+j)%len(keys)],
					MetricFixed: raw,
					Sequence:    sequences[partition],
				})
			}
		}
	}
	return events
}

type SampleManifest struct {
	Schema        string   `json:"schema"`
	DataDir       string   `json:"data_dir"`
	Seed          int64    `json:"seed"`
	Generator     string   `json:"generator"`
	GeneratedAtMS int64    `json:"generated_at_ms"`
	EventsFile    string   `json:"events_file"`
	SnapshotFile  string   `json:"events_snapshot_file"`
	PolicyFile    string   `json:"policy_file"`
	DatabaseFile  string   `json:"database_file"`
	EventCount    int      `json:"event_count"`
	PolicyVersion int64    `json:"policy_version"`
	Commands      []string `json:"commands"`
}

// EnsureInitialSample writes the fixed local sample on first run. Existing
// files are left untouched so repeated commands remain idempotent.
func EnsureInitialSample(dir string) (*SampleManifest, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	eventsPath := filepath.Join(dir, "events.jsonl")
	snapshotPath := filepath.Join(dir, "events-snapshot.json")
	policyPath := filepath.Join(dir, "policy-v1.json")
	manifestPath := filepath.Join(dir, "manifest.json")
	dbPath := filepath.Join(dir, "stream.db")

	if _, err := os.Stat(eventsPath); os.IsNotExist(err) {
		events := DefaultEvents()
		f, err := os.Create(eventsPath)
		if err != nil {
			return nil, err
		}
		enc := json.NewEncoder(f)
		for _, event := range events {
			if err := enc.Encode(event); err != nil {
				_ = f.Close()
				return nil, err
			}
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		snapshot := EventSnapshot{
			Schema: SchemaEventStream, MetricScale: MetricScale,
			Generator: "fixed-seed-linear-v1", Seed: FixedSeed, Events: DefaultEvents(),
		}
		data, err := json.MarshalIndent(snapshot, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(snapshotPath, data, 0o644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	if _, err := os.Stat(policyPath); os.IsNotExist(err) {
		policy := DefaultPolicy()
		data, err := json.MarshalIndent(policy, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(policyPath, data, 0o644); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	manifest := SampleManifest{
		Schema:        "feedback.sample-manifest/v1",
		DataDir:       dir,
		Seed:          FixedSeed,
		Generator:     "fixed-seed-linear-v1",
		GeneratedAtMS: FixedGeneratedAtMS,
		EventsFile:    eventsPath,
		SnapshotFile:  snapshotPath,
		PolicyFile:    policyPath,
		DatabaseFile:  dbPath,
		EventCount:    len(DefaultEvents()),
		PolicyVersion: 1,
		Commands: []string{
			"go run ./cmd/feedbackstream plan --data-dir " + dir,
			"go run ./cmd/feedbackstream demo --data-dir " + dir,
			"go run ./cmd/feedbackstream recover --data-dir " + dir,
		},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func DefaultDataDir() string {
	if v := os.Getenv("FEEDBACK_DATA_DIR"); v != "" {
		return v
	}
	return filepath.Join(".", "data")
}

func FormatMS(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}
