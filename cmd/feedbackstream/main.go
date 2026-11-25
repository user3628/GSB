package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	feedback "feedbackstream"
)

func main() {
	if len(os.Args) == 1 {
		usage()
		os.Exit(0)
	}
	command := os.Args[1]
	args := os.Args[2:]
	ctx := context.Background()
	var err error
	switch command {
	case "init":
		err = runInit(ctx, args)
	case "plan":
		err = runPlan(args)
	case "replace-policy":
		err = runReplacePolicy(ctx, args)
	case "patch-policy":
		err = runPatchPolicy(ctx, args)
	case "demo":
		err = runDemo(ctx, args)
	case "recover":
		err = runRecover(ctx, args)
	case "query":
		err = runQuery(ctx, args)
	case "crash":
		err = runCrash(ctx, args)
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", command)
	}
	if err != nil {
		fail(err)
	}
}

func usage() {
	fmt.Println(`feedbackstream commands:
  init      [--data-dir data]
  plan      [--data-dir data]
  replace-policy --policy-file policy-v2.json [--data-dir data]
  patch-policy --patch-file patch.json [--data-dir data]
  demo      [--data-dir data] [--reset]
  recover   [--data-dir data]
  query     windows|shed|bp [--data-dir data]
  crash     --mode batch|policy [--data-dir data]
  help`)
}

func dataDirFlag(args []string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet("feedbackstream", flag.ContinueOnError)
	dir := fs.String("data-dir", feedback.DefaultDataDir(), "local data directory")
	_ = fs.Parse(args)
	return fs, dir
}

func runInit(ctx context.Context, args []string) error {
	fs, dir := dataDirFlag(args)
	if err := fs.Parse(args); err != nil {
		return err
	}
	manifest, err := feedback.EnsureInitialSample(*dir)
	if err != nil {
		return err
	}
	store, err := feedback.OpenStore(ctx, manifest.DatabaseFile)
	if err != nil {
		return err
	}
	defer store.Close()
	policy, err := feedback.ReadPolicyFile(manifest.PolicyFile)
	if err != nil {
		return err
	}
	if err := store.SaveInitialPolicy(ctx, policy); err != nil {
		return err
	}
	return printJSON(manifest)
}

func runPlan(args []string) error {
	fs, dir := dataDirFlag(args)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := feedback.EnsureInitialSample(*dir); err != nil {
		return err
	}
	policy, err := feedback.ReadPolicyFile(filepath.Join(*dir, "policy-v1.json"))
	if err != nil {
		return err
	}
	plan, err := feedback.PlanFusion(policy)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"policy_version": policy.Version, "plan": plan})
}

func runReplacePolicy(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("replace-policy", flag.ContinueOnError)
	dir := fs.String("data-dir", feedback.DefaultDataDir(), "data directory")
	policyFile := fs.String("policy-file", "", "full policy snapshot JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *policyFile == "" {
		return fmt.Errorf("--policy-file is required")
	}
	policy, err := feedback.ReadPolicyFile(*policyFile)
	if err != nil {
		return err
	}
	store, err := feedback.OpenStore(ctx, filepath.Join(*dir, "stream.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	service := feedback.NewService(store)
	plan, err := service.ReplacePolicySnapshot(ctx, policy)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"active_version": policy.Version, "plan": plan})
}

func runPatchPolicy(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("patch-policy", flag.ContinueOnError)
	dir := fs.String("data-dir", feedback.DefaultDataDir(), "data directory")
	patchFile := fs.String("patch-file", "", "incremental policy patch JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *patchFile == "" {
		return fmt.Errorf("--patch-file is required")
	}
	data, err := os.ReadFile(*patchFile)
	if err != nil {
		return err
	}
	var patch feedback.PolicyPatch
	if err := json.Unmarshal(data, &patch); err != nil {
		return feedback.SpecInvalid("invalid policy patch JSON: " + err.Error())
	}
	store, err := feedback.OpenStore(ctx, filepath.Join(*dir, "stream.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	service := feedback.NewService(store)
	policy, plan, err := service.ApplyPolicyPatch(ctx, patch)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"active_version": policy.Version, "policy": policy, "plan": plan})
}

func runDemo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	dir := fs.String("data-dir", feedback.DefaultDataDir(), "local data directory")
	reset := fs.Bool("reset", false, "remove the local SQLite files before running")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := feedback.RunDemo(ctx, feedback.DemoOptions{DataDir: *dir, Reset: *reset})
	if err != nil {
		return err
	}
	return printJSON(report)
}

func runRecover(ctx context.Context, args []string) error {
	fs, dir := dataDirFlag(args)
	if err := fs.Parse(args); err != nil {
		return err
	}
	dbPath := filepath.Join(*dir, "stream.db")
	store, err := feedback.OpenStore(ctx, dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	report, err := store.Recover(ctx)
	if err != nil {
		return err
	}
	return printJSON(report)
}

func runQuery(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("query requires windows|shed|bp")
	}
	kind := args[0]
	rest := args[1:]
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	dir := fs.String("data-dir", feedback.DefaultDataDir(), "data directory")
	version := fs.Int64("version", 0, "policy version for windows; zero means active version")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	store, err := feedback.OpenStore(ctx, filepath.Join(*dir, "stream.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	switch kind {
	case "windows":
		v := *version
		if v == 0 {
			active, err := store.ActivePolicy(ctx)
			if err != nil {
				return err
			}
			if active != nil {
				v = active.Snapshot.Version
			}
		}
		rows, err := store.ListWindowResults(ctx, v, false)
		if err != nil {
			return err
		}
		return printJSON(rows)
	case "shed":
		rows, err := store.ListShedDecisions(ctx, "")
		if err != nil {
			return err
		}
		return printJSON(rows)
	case "bp":
		rows, err := store.ListBackpressureEvents(ctx, 100)
		if err != nil {
			return err
		}
		return printJSON(rows)
	default:
		return fmt.Errorf("unknown query %q", kind)
	}
}

func runCrash(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("crash", flag.ContinueOnError)
	dir := fs.String("data-dir", feedback.DefaultDataDir(), "data directory")
	mode := fs.String("mode", "batch", "batch|policy")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := feedback.EnsureInitialSample(*dir); err != nil {
		return err
	}
	dbPath := filepath.Join(*dir, "stream.db")
	store, err := feedback.OpenStore(ctx, dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	policy, err := feedback.ReadPolicyFile(filepath.Join(*dir, "policy-v1.json"))
	if err != nil {
		return err
	}
	if err := store.SaveInitialPolicy(ctx, policy); err != nil {
		return err
	}

	switch *mode {
	case "batch":
		events, err := feedback.ReadEventFile(filepath.Join(*dir, "events.jsonl"))
		if err != nil {
			return err
		}
		batches, err := feedback.BuildWindowBatches(events, policy.Window)
		if err != nil {
			return err
		}
		batch := batches[0]
		if err := store.SaveBatch(ctx, batch); err != nil {
			return err
		}
		intentID := "intent-crash-batch"
		if _, err := store.CreateIntent(ctx, feedback.IntentRecord{
			ID: intentID, Kind: feedback.IntentStartBatch, AggregateID: batch.ID,
			PayloadJSON: fmt.Sprintf(`{"batch_id":%q,"crash_point":"after_intent_before_result_commit"}`, batch.ID),
		}); err != nil {
			return err
		}
		if err := store.MarkBatchRunning(ctx, batch.ID); err != nil {
			return err
		}
		_ = printJSON(map[string]any{
			"crash": "about_to_sigkill", "mode": "batch", "intent_id": intentID,
			"batch_id": batch.ID, "db": dbPath,
		})
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	case "policy":
		next := *policy
		next.Version = 2
		next.Kind = "STAGED_CRASH"
		next.ChecksumSHA256 = ""
		sum, err := feedback.PolicyChecksum(&next)
		if err != nil {
			return err
		}
		next.ChecksumSHA256 = sum
		payload, _ := json.Marshal(&next)
		_, err = store.DB().ExecContext(ctx, `
			INSERT INTO policies(version,kind,base_version,status,checksum,payload_json,created_at_ms,activated_at_ms)
			VALUES(2,'FULL',1,'STAGED',?,?,?,0)
			ON CONFLICT(version) DO UPDATE SET status='STAGED'`,
			next.ChecksumSHA256, string(payload), feedback.FixedGeneratedAtMS+1)
		if err != nil {
			return err
		}
		intentID := "intent-crash-policy-v2"
		if _, err := store.CreateIntent(ctx, feedback.IntentRecord{
			ID: intentID, Kind: feedback.IntentSwitchPolicy, AggregateID: "policy-v2",
			PayloadJSON: `{"base_version":1,"new_version":2,"crash_point":"after_intent_before_atomic_switch"}`,
		}); err != nil {
			return err
		}
		_ = printJSON(map[string]any{
			"crash": "about_to_sigkill", "mode": "policy", "intent_id": intentID,
			"active_version_after_restart": 1, "db": dbPath,
		})
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	case "window":
		events, err := feedback.ReadEventFile(filepath.Join(*dir, "events.jsonl"))
		if err != nil {
			return err
		}
		batches, err := feedback.BuildWindowBatches(events, policy.Window)
		if err != nil {
			return err
		}
		batch := batches[0]
		if err := store.SaveBatch(ctx, batch); err != nil {
			return err
		}
		intentID := "intent-crash-window"
		if _, err := store.CreateIntent(ctx, feedback.IntentRecord{
			ID: intentID, Kind: feedback.IntentWindowConclusion, AggregateID: batch.ID,
			PayloadJSON: fmt.Sprintf(`{"batch_id":%q,"crash_point":"mid_window_commit_transaction"}`, batch.ID),
		}); err != nil {
			return err
		}
		tx, err := store.DB().BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		now := feedback.FixedGeneratedAtMS + 2
		_, err = tx.ExecContext(ctx, `
			INSERT INTO window_results(policy_version,fusion_group_id,signature_hash,partition_id,window_start_ms,window_end_ms,group_key,count,sum_fixed,min_fixed,max_fixed,status,source_batch_ids,committed_intent_id,committed_at_ms)
			VALUES(1,'fg-2','crash-signature',?,?,?,'crash-key',1,1000000,1000000,1000000,'DRAFT','[]',?,?)`,
			batch.PartitionID, batch.WindowStart, batch.WindowEnd, intentID, now)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO group_completions(batch_id,policy_version,fusion_group_id,signature_hash,status,window_result_count,committed_intent_id,completed_at_ms)
			VALUES(?,1,'fg-2','crash-signature','COMPLETED',1,?,?)`,
			batch.ID, intentID, now)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		_ = printJSON(map[string]any{
			"crash": "about_to_sigkill_mid_uncommitted_window_transaction",
			"mode":  "window", "intent_id": intentID, "batch_id": batch.ID,
		})
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	case "cascade":
		next := *policy
		next.Version = 2
		next.Kind = "FULL"
		next.ChecksumSHA256 = ""
		sum, err := feedback.PolicyChecksum(&next)
		if err != nil {
			return err
		}
		next.ChecksumSHA256 = sum
		payload, _ := json.Marshal(&next)
		intentID := "intent-crash-cascade-v2"
		if _, err := store.CreateIntent(ctx, feedback.IntentRecord{
			ID: intentID, Kind: feedback.IntentSwitchPolicy, AggregateID: "policy-v2",
			PayloadJSON: `{"base_version":1,"new_version":2,"crash_point":"mid_cascade_transaction"}`,
		}); err != nil {
			return err
		}
		tx, err := store.DB().BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		now := feedback.FixedGeneratedAtMS + 3
		if _, err := tx.ExecContext(ctx, `UPDATE policies SET status='SUPERSEDED' WHERE version=1 AND status='ACTIVE'`); err != nil {
			_ = tx.Rollback()
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO policies(version,kind,base_version,status,checksum,payload_json,created_at_ms,activated_at_ms)
			VALUES(2,'FULL',1,'ACTIVE',?,?,?,?)`,
			next.ChecksumSHA256, string(payload), now, now)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		_ = printJSON(map[string]any{
			"crash": "about_to_sigkill_mid_uncommitted_cascade_transaction",
			"mode":  "cascade", "intent_id": intentID,
		})
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	default:
		return fmt.Errorf("unknown crash mode %q", *mode)
	}
}

func printJSON(v any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(v)
}

func fail(err error) {
	if coded, ok := feedback.AsError(err); ok {
		_ = json.NewEncoder(os.Stderr).Encode(coded)
	} else {
		_ = json.NewEncoder(os.Stderr).Encode(map[string]string{"code": "E_INTERNAL", "message": err.Error()})
	}
	os.Exit(1)
}
