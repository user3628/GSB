package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"recordlab/internal/workbench"
)

func TestCLIWorkflow(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	reportPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	if err := execute(context.Background(), []string{"run", "--db", dbPath, "--out", reportPath, "--concurrency", "3"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report workbench.Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.TaskCount != 9 || len(report.RejectedTasks) != 2 {
		t.Fatalf("unexpected report summary: %+v", report.Summary)
	}

	stdout.Reset()
	if err := execute(context.Background(), []string{"versions", "--db", dbPath, "--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var chain struct {
		Snapshots []workbench.ChainEntry `json:"snapshots"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &chain); err != nil {
		t.Fatal(err)
	}
	if len(chain.Snapshots) != 10 {
		t.Fatalf("snapshot count = %d, want 10", len(chain.Snapshots))
	}

	stdout.Reset()
	if err := execute(context.Background(), []string{"replay", "--db", dbPath, "--task", "t05-depth", "--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var task workbench.TaskReport
	if err := json.Unmarshal(stdout.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	if task.TaskID != "t05-depth" || task.DecisionCode != workbench.CodeDepthExceeded {
		t.Fatalf("unexpected replay task: %+v", task)
	}

	stdout.Reset()
	if err := execute(context.Background(), []string{"rejected", "--db", dbPath, "--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var rejected struct {
		Tasks []workbench.TaskReport `json:"tasks"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &rejected); err != nil {
		t.Fatal(err)
	}
	if len(rejected.Tasks) != 2 {
		t.Fatalf("rejected count = %d, want 2", len(rejected.Tasks))
	}
}

func TestCLIInjectionAndEdit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	reportPath := filepath.Join(dir, "report.json")
	var stdout, stderr bytes.Buffer
	if err := execute(context.Background(), []string{"run", "--db", dbPath, "--out", reportPath, "--concurrency", "2"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	injectionPath := filepath.Join(dir, "injections.json")
	stdout.Reset()
	if err := execute(context.Background(), []string{"inject", "--dir", filepath.Join(dir, "tmp"), "--kind", "length-wrap", "--out", injectionPath}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(injectionPath)
	if err != nil {
		t.Fatal(err)
	}
	var injection workbench.InjectionReport
	if err := json.Unmarshal(data, &injection); err != nil {
		t.Fatal(err)
	}
	if !injection.AllPassed || len(injection.Cases) != 1 {
		t.Fatalf("unexpected injection report: %+v", injection)
	}
	stdout.Reset()
	if err := execute(context.Background(), []string{"edit", "--db", dbPath, "--max-depth", "5", "--global-budget", "300000"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := execute(context.Background(), []string{"versions", "--db", dbPath, "--json"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	var chain struct {
		Snapshots []workbench.ChainEntry `json:"snapshots"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &chain); err != nil {
		t.Fatal(err)
	}
	if len(chain.Snapshots) != 11 || chain.Snapshots[10].Kind != "policy-edit" {
		t.Fatalf("unexpected chain after edit: %+v", chain.Snapshots)
	}
}
