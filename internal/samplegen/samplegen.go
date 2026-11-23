// Package samplegen produces the local deterministic sample set: versioned
// policy snapshots, binary event logs (v1/v2) and JSONL summaries. It never
// touches the network or external data.
package samplegen

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"flowtrace/internal/model"
	"flowtrace/internal/parse"
	"flowtrace/internal/policy"
)

// Manifest describes the generated sample set.
type Manifest struct {
	PolicyFile string    `json:"policy_file"`
	Sessions   []Session `json:"sessions"`
}

// Session binds windows to event source files.
type Session struct {
	ID      string   `json:"id"`
	Windows []Window `json:"windows"`
}

// Window names one event source file and its format ("bin" or "jsonl").
type Window struct {
	ID     string `json:"id"`
	File   string `json:"file"`
	Format string `json:"format"`
}

// Policy returns the deterministic authorization policy snapshot (v1).
func Policy() *policy.Snapshot {
	return &policy.Snapshot{
		Version: 1,
		SensitiveFields: map[string][]string{
			"users.id_card":   {"ID_CARD"},
			"users.bank_card": {"BANK_CARD"},
			"vault.api_key":   {"KEY_MATERIAL"},
		},
		AuthorizedTargets: map[string]bool{
			"users.id_card":       true,
			"users.bank_card":     true,
			"vault.api_key":       true,
			"dw.users_copy":       true,
			"analytics.user_hash": true,
			"risk.features":       true,
			"stats.kpi":           true,
			"loop.a":              true,
			"loop.b":              true,
			"loop.c":              true,
		},
		AggregateSafe: map[string]bool{
			"ID_CARD": true, // aggregation de-identifies ID_CARD
		},
	}
}

// BaseEvents returns the canonical deterministic event chain (seq 1..8).
func BaseEvents() []model.Event {
	mk := func(seq uint64, subj, src, dst string, op model.Op, ts uint64) model.Event {
		return model.Event{Seq: seq, Subject: subj, SrcField: src, DstField: dst, Op: op, LogicalTime: ts}
	}
	return []model.Event{
		mk(1, "etl-job-1", "users.id_card", "dw.users_copy", model.OpCrossTableCopy, 100),
		mk(2, "etl-job-1", "dw.users_copy", "analytics.user_hash", model.OpCrossTableCopy, 110),
		mk(3, "exporter-7", "dw.users_copy", "ext.partner_feed", model.OpCrossServiceExport, 120), // escape
		mk(4, "agg-job-2", "users.bank_card", "risk.features", model.OpAggregateWrite, 130),       // aggregate keeps BANK_CARD
		mk(5, "svc-backup", "vault.api_key", "logs.raw_dump", model.OpCrossTableCopy, 140),        // escape
		mk(6, "agg-job-3", "users.id_card", "stats.kpi", model.OpAggregateWrite, 150),             // de-identified
		mk(7, "etl-job-4", "users.bank_card", "dw.users_copy", model.OpCrossTableCopy, 160),
		mk(8, "exporter-7", "analytics.user_hash", "ext.partner_feed", model.OpCrossServiceExport, 170), // escape
	}
}

// CycleEvents returns events forming a propagation loop (loop.a -> loop.b ->
// loop.c -> loop.a) seeded from a sensitive field. The engine must not
// diffuse indefinitely.
func CycleEvents() []model.Event {
	mk := func(seq uint64, src, dst string, ts uint64) model.Event {
		return model.Event{Seq: seq, Subject: "loop-svc", SrcField: src, DstField: dst, Op: model.OpCrossTableCopy, LogicalTime: ts}
	}
	return []model.Event{
		mk(101, "users.id_card", "loop.a", 200),
		mk(102, "loop.a", "loop.b", 210),
		mk(103, "loop.b", "loop.c", 220),
		mk(104, "loop.c", "loop.a", 230),
		mk(105, "loop.a", "loop.b", 240),
	}
}

// EscapeEvents returns an extra out-of-boundary export used by the escape
// injection scenario.
func EscapeEvents() []model.Event {
	return []model.Event{
		{Seq: 201, Subject: "exporter-9", SrcField: "users.bank_card", DstField: "ext.partner_feed", Op: model.OpCrossServiceExport, LogicalTime: 300},
	}
}

// WriteDir generates the full deterministic sample set into dir.
func WriteDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	pol := Policy()
	if err := os.WriteFile(filepath.Join(dir, "policy_v1.json"), pol.Marshal(), 0o644); err != nil {
		return err
	}
	// A second policy version to prove versioned snapshots load.
	pol2 := *pol
	pol2.Version = 2
	if err := os.WriteFile(filepath.Join(dir, "policy_v2.json"), pol2.Marshal(), 0o644); err != nil {
		return err
	}

	base := BaseEvents()
	v1, err := parse.EncodeBinary(parse.Version1, base[:4])
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "events_v1.bin"), v1, 0o644); err != nil {
		return err
	}
	v2, err := parse.EncodeBinary(parse.Version2, base[4:])
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "events_v2.bin"), v2, 0o644); err != nil {
		return err
	}

	// JSONL summary mirrors the same chain in one window.
	var jl []byte
	for _, ev := range base {
		rec := map[string]any{
			"seq": ev.Seq, "subject": ev.Subject, "src": ev.SrcField,
			"dst": ev.DstField, "op": ev.Op.String(), "ts": ev.LogicalTime,
		}
		raw, _ := json.Marshal(rec)
		jl = append(jl, raw...)
		jl = append(jl, '\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), jl, 0o644); err != nil {
		return err
	}

	manifest := Manifest{
		PolicyFile: "policy_v1.json",
		Sessions: []Session{
			{ID: "S1", Windows: []Window{
				{ID: "W1", File: "events_v1.bin", Format: "bin"},
				{ID: "W2", File: "events_v2.bin", Format: "bin"},
			}},
			{ID: "S2", Windows: []Window{
				{ID: "W3", File: "events.jsonl", Format: "jsonl"},
			}},
		},
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0o644); err != nil {
		return err
	}
	return nil
}

// LoadManifest reads the manifest from a sample directory.
func LoadManifest(dir string) (*Manifest, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return &m, nil
}
