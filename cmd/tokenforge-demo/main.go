package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"example.com/tokenforge/internal/demo"
)

func main() {
	var (
		workDir   = flag.String("workdir", "", "writable directory for SQLite files (default: a fresh OS temp directory)")
		outPath   = flag.String("out", "tokenforge-report.json", "JSON report path, or - for stdout")
		threshold = flag.Int("threshold", 4, "adaptive family scan threshold; can be overridden per call in the package")
		inject    = flag.String("inject", "all", "fault samples: all, none, or comma list signature,replay,quota_overflow,snapshot_gap,contention")
	)
	flag.Parse()

	injections, err := demo.ParseInjections(*inject)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid -inject:", err)
		os.Exit(2)
	}
	dir := *workDir
	if dir == "" {
		dir, err = os.MkdirTemp("", "tokenforge-demo-*")
		if err != nil {
			fmt.Fprintln(os.Stderr, "create temp dir:", err)
			os.Exit(1)
		}
		defer os.RemoveAll(dir)
	}
	report, err := demo.Run(context.Background(), demo.Options{
		WorkDir: dir, Threshold: *threshold, Injections: injections,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
	data, err := demo.EncodeReport(report)
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode report:", err)
		os.Exit(1)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	if *outPath == "-" {
		if _, err := os.Stdout.Write(data); err != nil {
			fmt.Fprintln(os.Stderr, "write report:", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "sha256=%s\n", digest)
		return
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil && filepath.Dir(*outPath) != "." {
		fmt.Fprintln(os.Stderr, "create report directory:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*outPath, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		os.Exit(1)
	}
	fmt.Printf("report=%s\nsha256=%s\n", *outPath, digest)
}
