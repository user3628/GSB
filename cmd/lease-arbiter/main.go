package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"leasearbiter/internal/core"
	"leasearbiter/internal/engine"
	"leasearbiter/internal/loopback"
	"leasearbiter/internal/orchestrator"
	"leasearbiter/internal/sample"
	"leasearbiter/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "demo":
		err = runDemo(os.Args[2:])
	case "endpoint":
		err = runEndpoint(os.Args[2:])
	case "recover":
		err = runRecover(os.Args[2:])
	case "crash-mid-batch":
		err = runCrashMidBatch(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: lease-arbiter <demo|endpoint|recover|crash-mid-batch> [flags]

  demo            run the deterministic sample against a real loopback endpoint
  endpoint        serve the local renew/heal injection endpoint
  recover         scan PREPARED intents and print the recovery report
  crash-mid-batch block in a heal dispatch after persisting its intent`)
}

func runDemo(args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	dbPath := flags.String("db", "artifacts/demo.db", "SQLite database path")
	seed := flags.Int64("seed", sample.DefaultSeed, "fixed generator seed")
	concurrency := flags.Int("concurrency", 4, "global holder concurrency")
	queueCapacity := flags.Int("queue", 4, "bounded waiting queue capacity")
	budget := flags.Duration("budget", 3*time.Second, "round time budget")
	healTimeout := flags.Duration("heal-timeout", 400*time.Millisecond, "independent per-heal timeout")
	inject := flags.String("endpoint-inject", "", "endpoint injection: delay|reject|late|block")
	injectDelayMS := flags.Int64("endpoint-delay-ms", 0, "endpoint injected delay")
	reuse := flags.Bool("reuse", false, "reuse existing database instead of starting fresh")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil && filepath.Dir(*dbPath) != "." {
		return err
	}
	if !*reuse {
		removeSQLiteFiles(*dbPath)
	}
	stateStore, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer stateStore.Close()
	endpoint, err := loopback.Start("127.0.0.1:0", loopback.HandlerConfig{DefaultInject: *inject, DefaultDelayMS: *injectDelayMS})
	if err != nil {
		return err
	}
	defer endpoint.Close()
	coordinator, err := orchestrator.New(orchestrator.Config{Concurrency: *concurrency, QueueCapacity: *queueCapacity, Budget: *budget})
	if err != nil {
		return err
	}
	dispatcher := loopback.NewDispatcher(endpoint.URL(), *healTimeout)
	app, err := engine.New(engine.Config{
		Store:       stateStore,
		Dispatcher:  dispatcher,
		Coordinator: coordinator,
		Clock:       deterministicClock,
	})
	if err != nil {
		return err
	}
	ctx := context.Background()
	snapshotSubmission, bizErr := app.SubmitSnapshot(ctx, emptySnapshot(1, *seed))
	if bizErr != nil {
		return bizErr
	}
	dataset := sample.Generate(*seed)
	batch, batchErr := app.Ingest(ctx, dataset, engine.ProcessOptions{Source: "demo-seed", RunHeal: true})
	read, readErr := app.Read(ctx, batch.StateVersion)
	staleRead, staleReadErr := app.Read(ctx, batch.StateVersion+1)
	staleSnapshotSubmission, staleSnapshotErr := app.SubmitSnapshot(ctx, sample.Snapshot(1, *seed))
	duplicateBatch, duplicateErr := app.Ingest(ctx, dataset, engine.ProcessOptions{Source: "demo-duplicate-replay", RunHeal: false})
	conflicting := sample.WithConflictingDuplicate(dataset)
	_, mismatchErr := app.Ingest(ctx, conflicting, engine.ProcessOptions{Source: "demo-digest-mismatch", RunHeal: false})
	finalState, finalErr := app.Current(ctx)
	heals, renews, observations := endpoint.Handler().Counts()
	intents, intentsErr := stateStore.Intents(ctx)
	if intentsErr != nil {
		return intentsErr
	}

	var alpha core.RebuildResult
	for _, result := range duplicateBatch.Results {
		if result.HolderID == "holder-alpha" {
			alpha = result
		}
	}
	var staleReadPayload any = staleRead
	if staleReadErr != nil {
		staleReadPayload = nil
	}
	var staleSnapshotPayload any = staleSnapshotSubmission
	if staleSnapshotErr != nil {
		staleSnapshotPayload = nil
	}
	output := map[string]any{
		"seed":                      *seed,
		"database":                  *dbPath,
		"loopback_endpoint":         endpoint.URL(),
		"snapshot_submission":       snapshotSubmission,
		"snapshot_error":            bizErr,
		"batch_report":              batch,
		"batch_error":               batchErr,
		"read":                      read,
		"read_error":                readErr,
		"stale_read_error":          staleReadErr,
		"stale_read_payload":        staleReadPayload,
		"stale_snapshot_submission": staleSnapshotPayload,
		"stale_snapshot_error":      staleSnapshotErr,
		"duplicate_replay_report":   duplicateBatch,
		"duplicate_replay_error":    duplicateErr,
		"duplicate_replay_alpha":    alpha,
		"report_mismatch_error":     mismatchErr,
		"final_state_version":       finalState.Version,
		"final_snapshot_version":    finalState.SnapshotVersion,
		"final_state_error":         finalErr,
		"loopback_counts":           map[string]int64{"heals": heals, "renews": renews, "observations": observations},
		"intent_summary":            summarizeIntents(intents),
	}
	printJSON(output)
	return nil
}

func runEndpoint(args []string) error {
	flags := flag.NewFlagSet("endpoint", flag.ContinueOnError)
	address := flags.String("addr", "127.0.0.1:18080", "loopback listen address")
	inject := flags.String("inject", "", "default injection: delay|reject|late|block")
	delayMS := flags.Int64("delay-ms", 0, "default injected delay")
	if err := flags.Parse(args); err != nil {
		return err
	}
	server, err := loopback.Start(*address, loopback.HandlerConfig{DefaultInject: *inject, DefaultDelayMS: *delayMS})
	if err != nil {
		return err
	}
	defer server.Close()
	printJSON(map[string]any{"url": server.URL(), "inject": *inject, "delay_ms": *delayMS, "status": "listening"})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	return nil
}

func runRecover(args []string) error {
	flags := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := flags.String("db", "artifacts/demo.db", "SQLite database path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	stateStore, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer stateStore.Close()
	ctx := context.Background()
	report, err := stateStore.Recover(ctx)
	if err != nil {
		return err
	}
	current, err := stateStore.CurrentState(ctx)
	if err != nil {
		return err
	}
	intents, err := stateStore.Intents(ctx)
	if err != nil {
		return err
	}
	printJSON(map[string]any{
		"recovery_report":          report,
		"current_state_version":    current.Version,
		"current_snapshot_version": current.SnapshotVersion,
		"current_snapshot_digest":  current.SnapshotDigest,
		"intents":                  intents,
	})
	return nil
}

func runCrashMidBatch(args []string) error {
	flags := flag.NewFlagSet("crash-mid-batch", flag.ContinueOnError)
	dbPath := flags.String("db", "artifacts/crash.db", "SQLite database path")
	sentinel := flags.String("sentinel", "artifacts/crash-dispatch.ready", "file touched once the heal POST is in flight")
	seed := flags.Int64("seed", sample.DefaultSeed, "fixed generator seed")
	dispatchTimeout := flags.Duration("dispatch-timeout", 60*time.Second, "dispatch timeout; the script kills the process first")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil && filepath.Dir(*dbPath) != "." {
		return err
	}
	_ = os.Remove(*sentinel)
	stateStore, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer stateStore.Close()
	ctx := context.Background()
	if err := seedCrashBaseline(ctx, stateStore, *seed); err != nil {
		return err
	}
	dataset := sample.Generate(*seed)
	stored, mismatch := core.DeduplicateDataset(dataset)
	if mismatch != nil {
		return mismatch
	}
	results := core.RebuildAll(dataset.Holders, stored, dataset.Policy)
	var holder core.Holder
	var result core.RebuildResult
	for i := range dataset.Holders {
		if dataset.Holders[i].ID == "holder-bravo" {
			holder = dataset.Holders[i]
		}
	}
	for _, candidate := range results {
		if candidate.HolderID == "holder-bravo" {
			result = candidate
		}
	}
	decision := core.DecideHeal(holder, result, deterministicClock().UnixMilli())
	if decision == nil {
		return fmt.Errorf("sample did not produce a heal decision")
	}
	intent, err := stateStore.BeginIntent(ctx, "batch_rebuild", map[string]any{
		"source":  "crash-mid-batch",
		"holders": len(dataset.Holders),
		"planned_action": map[string]any{
			"holder_id": decision.HolderID,
			"action":    decision.Action,
			"trigger":   decision.Trigger,
		},
	})
	if err != nil {
		return err
	}
	server, err := loopback.Start("127.0.0.1:0", loopback.HandlerConfig{
		DefaultInject: "block",
		OnRequest: func(event loopback.RequestEvent) {
			_ = os.WriteFile(*sentinel, []byte(fmt.Sprintf("%s %s %s\n", event.Path, event.Remote, event.At.Format(time.RFC3339Nano))), 0o644)
		},
	})
	if err != nil {
		return err
	}
	defer server.Close()
	printJSON(map[string]any{
		"status":            "dispatch-in-flight",
		"intent_id":         intent.ID,
		"base_version":      intent.BaseVersion,
		"target_version":    intent.TargetVersion,
		"holder_id":         decision.HolderID,
		"action":            decision.Action,
		"sentinel":          *sentinel,
		"loopback_endpoint": server.URL(),
	})
	dispatcher := loopback.NewDispatcher(server.URL(), *dispatchTimeout)
	_, bizErr := dispatcher.DispatchRaw(ctx, *decision, loopback.DispatchOptions{Inject: "block"}, "crash-request", deterministicClock().UnixMilli())
	_ = stateStore.AbortIntent(ctx, intent.ID, "dispatch returned without forced termination")
	if bizErr != nil {
		return bizErr
	}
	return fmt.Errorf("dispatch unexpectedly completed; run through scripts/kill-mid-batch.sh to force termination")
}

func seedCrashBaseline(ctx context.Context, stateStore *store.Store, seed int64) error {
	current, err := stateStore.CurrentState(ctx)
	if err != nil {
		return err
	}
	if current.Version > 0 && current.SnapshotVersion > 0 {
		return nil
	}
	snapshot := emptySnapshot(1, seed)
	observations, mismatch := core.DeduplicateDataset(snapshot.Dataset)
	if mismatch != nil {
		return mismatch
	}
	intent, err := stateStore.BeginIntent(ctx, "snapshot_switch", map[string]any{
		"snapshot_version": snapshot.Version,
		"digest":           snapshot.Digest,
	})
	if err != nil {
		return err
	}
	state := core.NewState()
	state.Version = intent.TargetVersion
	state.SnapshotVersion = snapshot.Version
	state.SnapshotGeneratedAtMS = snapshot.GeneratedAtMS
	state.SnapshotDigest = snapshot.Digest
	state.Policy = snapshot.Dataset.Policy
	state.Holders = append([]core.Holder(nil), snapshot.Dataset.Holders...)
	state.Observations = observations
	return stateStore.CommitIntent(ctx, intent.ID, state)
}

func emptySnapshot(version int64, seed int64) core.Snapshot {
	dataset := sample.Generate(seed)
	dataset.Observations = nil
	snapshot := core.Snapshot{
		Version:       version,
		GeneratedAtMS: sample.BaseEpochMS + 60_000 + version,
		Dataset:       dataset,
	}
	digest, err := core.ComputeSnapshotDigest(snapshot)
	if err != nil {
		panic(err)
	}
	snapshot.Digest = digest
	return snapshot
}

func deterministicClock() time.Time {
	return time.UnixMilli(sample.BaseEpochMS + 100_000)
}

func removeSQLiteFiles(path string) {
	_ = os.Remove(path)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func summarizeIntents(intents []store.Intent) []map[string]any {
	summary := make([]map[string]any, 0, len(intents))
	for _, intent := range intents {
		summary = append(summary, map[string]any{
			"id":             intent.ID,
			"kind":           intent.Kind,
			"state":          intent.State,
			"base_version":   intent.BaseVersion,
			"target_version": intent.TargetVersion,
			"created_at_ms":  intent.CreatedAtMS,
		})
	}
	sort.Slice(summary, func(i, j int) bool {
		return summary[i]["id"].(string) < summary[j]["id"].(string)
	})
	return summary
}

func printJSON(value any) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintln(os.Stderr, "encode output:", err)
	}
}
