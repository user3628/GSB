package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"ticketpipeline/internal/engine"
	"ticketpipeline/internal/gen"
	"ticketpipeline/internal/httpx"
	"ticketpipeline/internal/model"
)

// runDemo executes the acceptance script in isolated demo directories so the
// primary data/out workspaces are not touched.
func runDemo(dataDir, outDir string) error {
	demoData := filepath.Join(dataDir, "demo")
	demoOut := filepath.Join(outDir, "demo")
	for _, d := range []string{demoData, demoOut} {
		if err := os.RemoveAll(d); err != nil {
			return err
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	step(1, "generate deterministic dataset (3 shards)")
	shards := gen.GenerateShards()
	for _, sh := range shards {
		if err := gen.WriteShard(demoData, sh); err != nil {
			return err
		}
	}
	if err := gen.WritePolicy(demoData, gen.GeneratePolicy()); err != nil {
		return err
	}
	before, err := digestTree(demoData)
	if err != nil {
		return err
	}
	fmt.Printf("  dataset digest=%s\n", short(before))

	step(2, "fresh full run through four stages")
	eng, err := engine.New(demoData, demoOut)
	if err != nil {
		return err
	}
	snap1, err := eng.Run(context.Background())
	if err != nil {
		_ = eng.Close()
		return err
	}
	if snap1.Report == nil {
		_ = eng.Close()
		return fmt.Errorf("missing validation report after full run")
	}
	freshDigest := snap1.Report.Digest
	fmt.Printf("  report digest=%s accuracy=%.4f\n", freshDigest, snap1.Report.IntentAccuracy)
	printErrorCodes(snap1)
	if err := eng.Close(); err != nil {
		return err
	}

	step(3, "force crash mid normalize then recover from committed facts")
	crashTarget := model.StageNormalize + ":" + shards[0].Tickets[10].TicketID
	// Start from a clean output directory so the crash subprocess actually
	// executes stage 1 again instead of reusing already committed checkpoints.
	if err := os.RemoveAll(demoOut); err != nil {
		return err
	}
	if err := os.MkdirAll(demoOut, 0o755); err != nil {
		return err
	}
	if err := runCrashSubprocess(demoData, demoOut, crashTarget); err != nil {
		return err
	}
	eng2, err := engine.New(demoData, demoOut)
	if err != nil {
		return err
	}
	snap2, err := eng2.Run(context.Background())
	if err != nil {
		_ = eng2.Close()
		return err
	}
	if snap2.Report == nil || snap2.Report.Digest != freshDigest {
		_ = eng2.Close()
		return fmt.Errorf("recovery digest mismatch: %s != %s",
			digestOf(snap2), freshDigest)
	}
	for _, stage := range model.Stages {
		if err := eng2.Store().AssertContiguous(stage); err != nil {
			_ = eng2.Close()
			return fmt.Errorf("post-recovery sequence broken: %w", err)
		}
	}
	fmt.Println("  recovery replay matches fresh run; sequences contiguous")
	if err := eng2.Close(); err != nil {
		return err
	}

	step(4, "single config change: only priority stage and downstream rebuild")
	th := []float64{-25, -15, -5}
	policy2, err := engine.BumpPolicy(demoData, engine.PolicyPatch{PriorityThresh: th})
	if err != nil {
		return err
	}
	eng3, err := engine.New(demoData, demoOut)
	if err != nil {
		return err
	}
	statusBefore, _ := engine.Status(eng3.Store())
	snap3, err := eng3.Run(context.Background())
	if err != nil {
		_ = eng3.Close()
		return err
	}
	statusAfter, _ := engine.Status(eng3.Store())
	showReuse(statusBefore, statusAfter)
	if snap3.Policy.Version != policy2.Version {
		_ = eng3.Close()
		return fmt.Errorf("policy version not advanced")
	}
	// Rebuild-from-scratch parity on a separate clean output directory.
	parityOut := filepath.Join(outDir, "demo-parity")
	_ = os.RemoveAll(parityOut)
	engParity, err := engine.New(demoData, parityOut)
	if err != nil {
		_ = eng3.Close()
		return err
	}
	snapParity, err := engParity.Run(context.Background())
	if err != nil {
		_ = eng3.Close()
		_ = engParity.Close()
		return err
	}
	if snapParity.Report.Digest != snap3.Report.Digest {
		_ = eng3.Close()
		_ = engParity.Close()
		return fmt.Errorf("incremental vs fresh digest mismatch: %s != %s",
			snap3.Report.Digest, snapParity.Report.Digest)
	}
	fmt.Printf("  incremental == fresh digest=%s\n", snap3.Report.Digest)
	if err := engParity.Close(); err != nil {
		return err
	}
	if err := eng3.Close(); err != nil {
		return err
	}

	step(5, "anomaly injections return stable business codes")
	if err := exerciseInjections(demoData, outDir); err != nil {
		return err
	}

	step(6, "loopback HTTP smoke test")
	if err := httpSmoke(demoData, demoOut); err != nil {
		return err
	}

	step(7, "fixed summary")
	fmt.Printf("  FINAL report digest=%s\n", freshDigest)
	fmt.Println("demo completed successfully")
	return nil
}

func step(n int, title string) { fmt.Printf("\n[%d] %s\n", n, title) }

func digestOf(snap *engine.Snapshot) string {
	if snap.Report == nil {
		return "<none>"
	}
	return snap.Report.Digest
}

func printErrorCodes(snap *engine.Snapshot) {
	codes := map[string]int{}
	for _, be := range snap.NormErr {
		codes[be.Code]++
	}
	for _, be := range snap.PrioErr {
		codes[be.Code]++
	}
	for _, be := range snap.IntentErr {
		codes[be.Code]++
	}
	for code, n := range codes {
		fmt.Printf("  %s x%d\n", code, n)
	}
}

func showReuse(before, after []engine.StageStatus) {
	b := map[string]engine.StageStatus{}
	for _, s := range before {
		b[s.Stage] = s
	}
	for _, s := range after {
		marker := "rebuilt"
		if s.LatestGen == b[s.Stage].LatestGen {
			marker = "reused"
		}
		fmt.Printf("  %-9s gen %d->%d %s\n", s.Stage, b[s.Stage].LatestGen, s.LatestGen, marker)
	}
}

// runCrashSubprocess re-executes this binary with the crash command. Exit code
// 97 is the deterministic hard-exit marker and is treated as success.
func runCrashSubprocess(dataDir, outDir, crashAt string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "crash", crashAt)
	cmd.Env = append(os.Environ(),
		"TICKET_DATA_DIR="+dataDir, "TICKET_OUT_DIR="+outDir)
	out, err := cmd.CombinedOutput()
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 97 {
		fmt.Printf("  forced termination observed (exit 97) at %s\n", crashAt)
		return nil
	}
	if err != nil {
		return fmt.Errorf("crash subprocess failed: %w output=%s", err, strings.TrimSpace(string(out)))
	}
	return fmt.Errorf("crash subprocess did not exit with code 97: %s", strings.TrimSpace(string(out)))
}

func exerciseInjections(dataDir, outRoot string) error {
	cases := []struct {
		name     string
		prepare  func(dir string) error
		wantCode string
		stage    string
	}{
		{"zero-smoothing", func(d string) error {
			v := 0.0
			_, err := engine.BumpPolicy(d, engine.PolicyPatch{Smoothing: &v})
			return err
		}, model.CodeConfig, model.StageIntent},
		{"zero-hashdim", func(d string) error {
			v := 0
			_, err := engine.BumpPolicy(d, engine.PolicyPatch{HashDim: &v})
			return err
		}, model.CodeConfig, model.StageNormalize},
	}
	for _, tc := range cases {
		dir := filepath.Join(outRoot, "inj-"+tc.name)
		_ = os.RemoveAll(dir)
		if err := copyDir(dataDir, filepath.Join(outRoot, "inj-data-"+tc.name)); err != nil {
			return err
		}
		injData := filepath.Join(outRoot, "inj-data-"+tc.name)
		if err := tc.prepare(injData); err != nil {
			return err
		}
		eng, err := engine.New(injData, dir)
		if err != nil {
			return err
		}
		_, err = eng.Run(context.Background())
		_ = eng.Close()
		be, ok := model.AsBizError(err)
		if !ok || be.Code != tc.wantCode || be.Stage != tc.stage {
			return fmt.Errorf("injection %s: want %s@%s got %v", tc.name, tc.wantCode, tc.stage, err)
		}
		fmt.Printf("  %-14s -> %s @ %s (ticket=%s)\n", tc.name, be.Code, be.Stage, be.TicketID)
	}

	// Shard mismatch injection: declared rows differ from actual.
	mismatchData := filepath.Join(outRoot, "inj-data-shard")
	_ = os.RemoveAll(mismatchData)
	if err := copyDir(dataDir, mismatchData); err != nil {
		return err
	}
	shards, err := engine.LoadShards(mismatchData)
	if err != nil {
		return err
	}
	policy, err := engine.LoadPolicy(mismatchData)
	if err != nil {
		return err
	}
	if err := engine.ReplaceShard(mismatchData, shards[0].Meta.ShardID,
		shards[0].Tickets, true, policy.Version); err != nil {
		return err
	}
	eng, err := engine.New(mismatchData, filepath.Join(outRoot, "inj-shard"))
	if err != nil {
		return err
	}
	_, err = eng.Run(context.Background())
	_ = eng.Close()
	if be, ok := model.AsBizError(err); !ok || be.Code != model.CodeShardMismatch {
		return fmt.Errorf("shard mismatch: want %s got %v", model.CodeShardMismatch, err)
	}
	fmt.Printf("  %-14s -> %s @ %s\n", "shard-mismatch", model.CodeShardMismatch, model.StageNormalize)
	return nil
}

func httpSmoke(dataDir, outDir string) error {
	eng, err := engine.New(dataDir, outDir)
	if err != nil {
		return err
	}
	defer eng.Close()
	srv := httpx.NewServer(eng, dataDir, outDir)
	addr, err := srv.Start(0)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	defer srv.Shutdown(ctx)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("healthz status %d", resp.StatusCode)
	}
	fmt.Printf("  HTTP healthz OK on http://%s/healthz\n", addr)
	return nil
}
