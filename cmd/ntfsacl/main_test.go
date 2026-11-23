package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCLIEndToEndDeterministicPipeline(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if code, err := cmdInit(ctx, []string{"--workdir", root, "--reset"}); err != nil || code != 0 {
		t.Fatalf("init code=%d err=%v", code, err)
	}
	if code, err := cmdAudit(ctx, []string{"--workdir", root, "--output", "audit-1.json"}); err != nil || code != 0 {
		t.Fatalf("audit code=%d err=%v", code, err)
	}
	audit1 := readFile(t, filepath.Join(root, "audit-1.json"))
	var auditDoc struct {
		Snapshot struct {
			Version int64 `json:"version"`
		} `json:"snapshot"`
		Summary struct {
			ConflictCount int `json:"conflict_count"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(audit1, &auditDoc); err != nil {
		t.Fatal(err)
	}
	if auditDoc.Snapshot.Version != 1 || auditDoc.Summary.ConflictCount != 4 {
		t.Fatalf("unexpected audit: version=%d conflicts=%d", auditDoc.Snapshot.Version, auditDoc.Summary.ConflictCount)
	}
	if code, err := cmdReplay(ctx, []string{"--workdir", root, "--version", "1", "--output", "replay.json"}); err != nil || code != 0 {
		t.Fatalf("replay code=%d err=%v", code, err)
	}
	if !bytes.Equal(audit1, readFile(t, filepath.Join(root, "replay.json"))) {
		t.Fatal("replay bytes differ from committed audit bytes")
	}
	if code, err := cmdReplay(ctx, []string{"--workdir", root, "--version", "999", "--output", "fallback.json"}); err != nil || code != 3 {
		t.Fatalf("fallback code=%d err=%v", code, err)
	}
	if !bytes.Equal(audit1, readFile(t, filepath.Join(root, "fallback.json"))) {
		t.Fatal("fallback bytes differ from latest healthy snapshot")
	}
	if code, err := cmdConflicts(ctx, []string{"--workdir", root, "--output", "conflicts.json"}); err != nil || code != 0 {
		t.Fatalf("conflicts code=%d err=%v", code, err)
	}
	if code, err := cmdWriteback(ctx, []string{"--workdir", root, "--output", "writeback.json"}); err != nil || code != 0 {
		t.Fatalf("writeback code=%d err=%v", code, err)
	}
	var wb struct {
		Status string `json:"status"`
		Before int64  `json:"before_snapshot_version"`
		After  int64  `json:"after_snapshot_version"`
	}
	if err := json.Unmarshal(readFile(t, filepath.Join(root, "writeback.json")), &wb); err != nil {
		t.Fatal(err)
	}
	if wb.Status != "COMPLETED" || wb.Before != 2 || wb.After != 3 {
		t.Fatalf("unexpected writeback: %+v", wb)
	}
	if code, err := cmdReport(ctx, []string{"--workdir", root, "--before", "2", "--after", "3", "--output", "compare.json"}); err != nil || code != 0 {
		t.Fatalf("report code=%d err=%v", code, err)
	}
	var cmp struct {
		AfterConflicts []any `json:"after_conflicts"`
		Nodes          []struct {
			RelPath string `json:"rel_path"`
			Status  string `json:"status"`
		} `json:"nodes"`
		ChangeKind string `json:"change_kind"`
	}
	if err := json.Unmarshal(readFile(t, filepath.Join(root, "compare.json")), &cmp); err != nil {
		t.Fatal(err)
	}
	if len(cmp.AfterConflicts) != 0 || cmp.ChangeKind != "incremental" {
		t.Fatalf("unexpected comparison: %+v", cmp)
	}
}

func TestCLIMalformedStopsWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if code, err := cmdInit(ctx, []string{"--workdir", root, "--reset", "--malformed", "mask"}); err != nil || code != 0 {
		t.Fatalf("init code=%d err=%v", code, err)
	}
	code, err := cmdAudit(ctx, []string{"--workdir", root, "--output", "error.json"})
	if err != nil || code != 2 {
		t.Fatalf("audit code=%d err=%v", code, err)
	}
	var e struct {
		Code     string `json:"code"`
		NodePath string `json:"node_path"`
		ACEIndex int    `json:"ace_index"`
	}
	if err := json.Unmarshal(readFile(t, filepath.Join(root, "error.json")), &e); err != nil {
		t.Fatal(err)
	}
	if e.Code != "E_ACL_MALFORMED" || e.NodePath != "finance/budget.txt" || e.ACEIndex != 0 {
		t.Fatalf("unexpected error: %+v", e)
	}
	if _, err := os.Stat(filepath.Join(root, ".ntfsacl", "engine.db")); !os.IsNotExist(err) {
		t.Fatalf("malformed audit created a snapshot database: %v", err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCLIRejectsEscapingAndReservedOutputPaths(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if code, err := cmdInit(ctx, []string{"--workdir", root, "--reset"}); err != nil || code != 0 {
		t.Fatalf("init code=%d err=%v", code, err)
	}
	for _, out := range []string{"../escape.json", "tree/report.json", ".ntfsacl/report.json"} {
		code, err := cmdAudit(ctx, []string{"--workdir", root, "--output", out})
		if err == nil || code != 1 {
			t.Fatalf("output %q accepted: code=%d err=%v", out, code, err)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "escape.json")); !os.IsNotExist(err) {
		t.Fatalf("escaping output created a file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".ntfsacl", "engine.db")); !os.IsNotExist(err) {
		t.Fatalf("rejected output should not create a snapshot DB: %v", err)
	}
}
