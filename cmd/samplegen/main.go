package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"example.com/joinshuffle/internal/sample"
	"example.com/joinshuffle/internal/snapshot"
)

func main() {
	out := flag.String("out", "data", "output directory")
	seed := flag.Int64("seed", 20260101, "deterministic random seed")
	leftRows := flag.Int("left-rows", 2048, "left row count")
	rightRows := flag.Int("right-rows", 1536, "right row count")
	leftVersion := flag.Uint64("left-version", 1, "left snapshot version")
	rightVersion := flag.Uint64("right-version", 1, "right snapshot version")
	generatedAtText := flag.String("generated-at", sample.DefaultGeneratedAt.Format(time.RFC3339Nano), "deterministic RFC3339 generation timestamp")
	flag.Parse()

	generatedAt, err := time.Parse(time.RFC3339Nano, *generatedAtText)
	if err != nil {
		fail("parse generated-at: %v", err)
	}
	left, right, err := sample.GeneratePair(*seed, *leftRows, *rightRows, *leftVersion, *rightVersion, generatedAt)
	if err != nil {
		fail("generate samples: %v", err)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail("create output directory: %v", err)
	}
	leftPath := filepath.Join(*out, "left.snapshot")
	rightPath := filepath.Join(*out, "right.snapshot")
	leftMeta, err := snapshot.WriteFile(leftPath, left)
	if err != nil {
		fail("write left snapshot: %v", err)
	}
	rightMeta, err := snapshot.WriteFile(rightPath, right)
	if err != nil {
		fail("write right snapshot: %v", err)
	}
	payload := map[string]any{
		"seed":         *seed,
		"generated_at": generatedAt.UTC().Format(time.RFC3339Nano),
		"left":         leftMeta,
		"right":        rightMeta,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
