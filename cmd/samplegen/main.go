package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"example.com/logicplan/editor/internal/codec"
	"example.com/logicplan/editor/internal/fixtures"
	"example.com/logicplan/editor/internal/model"
)

func main() {
	out := "samples"
	if len(os.Args) > 1 {
		out = os.Args[1]
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		log.Fatal(err)
	}
	writeBinary(filepath.Join(out, "plan_v1.bin"), fixtures.Legacy(1, 100))
	writeJSONL(filepath.Join(out, "plan_v1.jsonl"), fixtures.Legacy(1, 101))
	writeJSONL(filepath.Join(out, "plan_current.jsonl"), fixtures.Current(2, 200))
	writeBinary(filepath.Join(out, "plan_current.bin"), fixtures.Current(2, 201))
	writeJSONL(filepath.Join(out, "plan_next.jsonl"), fixtures.Current(2, 300))
	writeBinary(filepath.Join(out, "plan_next.bin"), fixtures.Current(2, 301))
	writeHighVersion(filepath.Join(out, "plan_v3_required.jsonl"))
	writeHighBinary(filepath.Join(out, "plan_v3_required.bin"))
	writeExpectedDegradations(filepath.Join(out, "plan_v1_degradations.expected.json"))
	fmt.Println("samples written to", out)
}

func writeBinary(path string, snapshot *model.Snapshot) {
	var buf bytes.Buffer
	if err := codec.EncodeBinary(&buf, snapshot, codec.BinaryOptions{}); err != nil {
		log.Fatalf("encode %s: %v", path, err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		log.Fatal(err)
	}
}

func writeJSONL(path string, snapshot *model.Snapshot) {
	var buf bytes.Buffer
	if err := codec.EncodeJSONL(&buf, snapshot, codec.JSONLOptions{StatsSeparate: true}); err != nil {
		log.Fatalf("encode %s: %v", path, err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		log.Fatal(err)
	}
}

func writeHighVersion(path string) {
	lines := []map[string]any{
		{
			"record": "snapshot", "format": "jsonl", "format_version": 3,
			"snapshot_seq": 9999, "generated_at": "2026-11-21T00:00:00Z",
			"required_features": []string{"vector_annex_v1"},
		},
		{
			"record": "operator", "id": "future_scan", "type": "Scan", "parent": "",
			"columns": []any{}, "vector_annex": map[string]any{"required": true},
		},
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, line := range lines {
		if err := enc.Encode(line); err != nil {
			log.Fatal(err)
		}
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		log.Fatal(err)
	}
}

func writeHighBinary(path string) {
	var buf bytes.Buffer
	snapshot := fixtures.Current(2, 9998)
	if err := codec.EncodeBinary(&buf, snapshot, codec.BinaryOptions{}); err != nil {
		log.Fatal(err)
	}
	data := buf.Bytes()
	// Version 3 is defined by required fields unknown to this editor. The
	// compatibility matrix rejects it from the header before any record body is
	// interpreted, so the body can deliberately remain a v2-shaped payload.
	data[4] = 3
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Fatal(err)
	}
}

func writeExpectedDegradations(path string) {
	data, err := os.ReadFile(filepath.Join(filepath.Dir(path), "plan_v1.bin"))
	if err != nil {
		log.Fatal(err)
	}
	snapshot, err := codec.DecodeBinary(data)
	if err != nil {
		log.Fatal(err)
	}
	payload := map[string]any{
		"format_version": 1,
		"snapshot_seq":   snapshot.SnapshotSeq,
		"decision":       "degrade",
		"count":          len(snapshot.Degradations),
		"degradations":   snapshot.Degradations,
	}
	data, _ = json.MarshalIndent(payload, "", "  ")
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Fatal(err)
	}
}
