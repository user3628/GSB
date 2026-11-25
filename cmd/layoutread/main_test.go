package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"layoutread/internal/service"
)

func TestCLIDemoIsByteDeterministic(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "demo-one.json")
	secondPath := filepath.Join(dir, "demo-two.json")
	if err := runDemo([]string{"-out", firstPath}); err != nil {
		t.Fatalf("first demo: %v", err)
	}
	if err := runDemo([]string{"-out", secondPath}); err != nil {
		t.Fatalf("second demo: %v", err)
	}
	first, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("two demo runs produced different bytes")
	}
	var output struct {
		Seed   int64                `json:"seed"`
		Run    service.RunReport    `json:"run"`
		Export service.ExportReport `json:"export"`
	}
	if err := json.Unmarshal(first, &output); err != nil {
		t.Fatal(err)
	}
	if output.Run.Processed != 4 || output.Export.ResultFingerprint == "" {
		t.Fatalf("incomplete CLI demo output: %+v", output)
	}
}

func TestCLIGenerateIsByteDeterministic(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "bundle-one.json")
	secondPath := filepath.Join(dir, "bundle-two.json")
	if err := runGenerate([]string{"-out", firstPath}); err != nil {
		t.Fatal(err)
	}
	if err := runGenerate([]string{"-out", secondPath}); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(firstPath)
	second, _ := os.ReadFile(secondPath)
	if string(first) != string(second) {
		t.Fatal("generated bundles differ")
	}
}
