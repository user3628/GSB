package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	conv "multicluster-convergence"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	command := os.Args[1]
	args := os.Args[2:]
	var err error
	switch command {
	case "gen-samples":
		err = genSamples(args)
	case "target":
		err = runTarget(args)
	case "commit-full":
		err = runCommitFull(args)
	case "commit-delta":
		err = runCommitDelta(args)
	case "apply":
		err = runApply(args)
	case "recover":
		err = runRecover(args)
	case "view":
		err = runView(args)
	case "version":
		fmt.Println("multicluster-convergence dev")
	case "-h", "--help", "help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", command)
	}
	if err != nil {
		business := conv.AsError(err)
		payload := map[string]any{"ok": false, "error": err.Error()}
		if business != nil {
			payload["code"] = business.Code
			payload["details"] = business.Details
		}
		data, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Fprintln(os.Stderr, string(data))
		if business != nil && business.Code == conv.CodeDependencyCycle {
			os.Exit(3)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `converge commands:
  gen-samples --dir samples
  target --addr 127.0.0.1:18080 [--snapshot file] [--addr-file file]
  commit-full --db convergence.db --file snapshot.json [--endpoint url]
  commit-delta --db convergence.db --file delta.json [--endpoint url]
  apply --db convergence.db [--all|--batch B001] [--endpoint url] [--crash-after-write-intent --ready-file ready]
  recover --db convergence.db [--out recovery.json]
  view --db convergence.db`)
}

type commonFlags struct {
	db       string
	endpoint string
	out      string
}

func bindCommon(fs *flag.FlagSet) *commonFlags {
	common := &commonFlags{}
	fs.StringVar(&common.db, "db", "artifacts/convergence.db", "SQLite database path")
	fs.StringVar(&common.endpoint, "endpoint", "", "loopback endpoint override for all clusters")
	fs.StringVar(&common.out, "out", "", "optional JSON output file")
	return common
}

func outputJSON(out string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if out != "" {
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(out, data, 0o644); err != nil {
			return err
		}
	}
	_, err = os.Stdout.Write(data)
	return err
}

func genSamples(args []string) error {
	fs := flag.NewFlagSet("gen-samples", flag.ContinueOnError)
	dir := fs.String("dir", "samples", "sample directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	policy := conv.SamplePolicy()
	checksum, err := policy.ComputeChecksum()
	if err != nil {
		return err
	}
	policy.Checksum = checksum
	if err := writeJSONFile(filepath.Join(*dir, "policy-v1.json"), policy); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(*dir, "snapshot-v1.json"), conv.SampleSnapshotV1()); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(*dir, "delta-v2.json"), conv.SampleDeltaV2()); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(*dir, "cycle-v1.json"), conv.SampleCycleSnapshot()); err != nil {
		return err
	}
	invalid := invalidSample()
	if err := writeJSONFile(filepath.Join(*dir, "invalid-v1.json"), invalid); err != nil {
		return err
	}
	stale := conv.SampleDeltaV2()
	stale.BaseVersion = 99
	if err := writeJSONFile(filepath.Join(*dir, "delta-stale-v99.json"), stale); err != nil {
		return err
	}
	fmt.Printf("samples written to %s\n", *dir)
	return nil
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func invalidSample() map[string]any {
	policy := conv.SamplePolicy()
	policyChecksum, _ := policy.ComputeChecksum()
	policy.Checksum = policyChecksum
	return map[string]any{
		"schema_version":   "multicluster.convergence/v1",
		"snapshot_version": 1,
		"generated_at":     "2026-11-25T00:00:00Z",
		"policy":           policy,
		"clusters":         []map[string]any{{"id": "cn-east", "endpoint": "http://127.0.0.1:18080"}},
		"objects": []map[string]any{
			{
				"cluster_id": "cn-east", "id": "duplicate-id", "kind": "ConfigMap", "namespace": "default",
				"expected_fields": map[string]any{"a": map[string]any{"value": "x", "source": "git"}},
				"actual_fields":   map[string]any{"a": map[string]any{"value": "x", "source": "git"}},
			},
			{
				"cluster_id": "cn-east", "id": "duplicate-id", "kind": "ConfigMap", "namespace": "default",
				"depends_on":      []string{"missing-object"},
				"expected_fields": map[string]any{},
				"actual_fields":   map[string]any{},
			},
			{
				"cluster_id": "missing-cluster", "id": "negative-budget", "kind": "ConfigMap", "namespace": "default",
				"resource_budget": map[string]any{"batch_units": -3},
				"expected_fields": map[string]any{},
				"actual_fields":   nil,
			},
		},
		"checksum": "sha256:deliberately-invalid",
	}
}

func runTarget(args []string) error {
	fs := flag.NewFlagSet("target", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "listen address")
	snapshotFile := fs.String("snapshot", "", "optional snapshot used to seed actual fields")
	addrFile := fs.String("addr-file", "", "write actual loopback base URL to this file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	target := conv.NewLocalTarget()
	if *snapshotFile != "" {
		data, err := os.ReadFile(*snapshotFile)
		if err != nil {
			return err
		}
		snapshot, err := conv.ParseSnapshot(data)
		if err != nil {
			return err
		}
		target.SeedSnapshot(snapshot)
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	baseURL := "http://" + listener.Addr().String()
	if *addrFile != "" {
		if err := os.MkdirAll(filepath.Dir(*addrFile), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(*addrFile, []byte(baseURL+"\n"), 0o644); err != nil {
			return err
		}
	}
	fmt.Fprintln(os.Stdout, "loopback target listening on", baseURL)
	server := &http.Server{Handler: target.Handler(), ReadHeaderTimeout: 5 * time.Second}
	return server.Serve(listener)
}

func runCommitFull(args []string) error {
	fs := flag.NewFlagSet("commit-full", flag.ContinueOnError)
	common := bindCommon(fs)
	file := fs.String("file", "samples/snapshot-v1.json", "full snapshot JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	service, _, err := conv.OpenService(ctx, conv.ServiceOptions{DBPath: common.db, EndpointOverride: common.endpoint})
	if err != nil {
		return err
	}
	defer service.Close()
	return commitFullFile(ctx, service, *file, common.out)
}

func commitFullFile(ctx context.Context, service *conv.Service, file, out string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	snapshot, err := conv.ParseSnapshot(data)
	if err != nil {
		_ = outputJSON(out, map[string]any{"ok": false, "error": errorJSON(err)})
		return err
	}
	result, commitErr := service.CommitFull(ctx, snapshot)
	envelope := map[string]any{"ok": commitErr == nil || conv.HasCode(commitErr, conv.CodeDependencyCycle), "file": file}
	if result != nil {
		envelope["version"] = result.Version
		envelope["plan"] = result.Plan
		envelope["summary"] = result.Current.Summary
	}
	if commitErr != nil {
		envelope["error"] = errorJSON(commitErr)
	}
	if err := outputJSON(out, envelope); err != nil {
		return err
	}
	return commitErr
}

func runCommitDelta(args []string) error {
	fs := flag.NewFlagSet("commit-delta", flag.ContinueOnError)
	common := bindCommon(fs)
	file := fs.String("file", "samples/delta-v2.json", "delta JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	service, _, err := conv.OpenService(ctx, conv.ServiceOptions{DBPath: common.db, EndpointOverride: common.endpoint})
	if err != nil {
		return err
	}
	defer service.Close()
	data, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	delta, err := conv.ParseDelta(data)
	if err != nil {
		return err
	}
	result, commitErr := service.CommitDelta(ctx, delta)
	envelope := map[string]any{"ok": commitErr == nil, "file": *file}
	if result != nil {
		envelope["version"] = result.Version
		envelope["cascade"] = result.Cascade
		envelope["plan"] = result.Plan
		envelope["summary"] = result.Current.Summary
	}
	if commitErr != nil {
		envelope["error"] = errorJSON(commitErr)
	}
	if err := outputJSON(common.out, envelope); err != nil {
		return err
	}
	return commitErr
}

func runApply(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	common := bindCommon(fs)
	batchID := fs.String("batch", "", "apply one batch id idempotently")
	all := fs.Bool("all", false, "apply all dependency-ordered batches")
	crashAfterIntent := fs.Bool("crash-after-write-intent", false, "block after persisting write intent so an external force kill can occur")
	readyFile := fs.String("ready-file", "", "write ready marker after write intent is durable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	service, _, err := conv.OpenService(ctx, conv.ServiceOptions{DBPath: common.db, EndpointOverride: common.endpoint})
	if err != nil {
		return err
	}
	defer service.Close()
	if *crashAfterIntent {
		service.ConfigureCrashHooks(func(batchID string) {
			payload := fmt.Sprintf(`{"status":"write-intent-durable","batch_id":%q}`+"\n", batchID)
			if *readyFile != "" {
				_ = os.MkdirAll(filepath.Dir(*readyFile), 0o755)
				_ = os.WriteFile(*readyFile, []byte(payload), 0o644)
			}
			select {}
		})
	}

	if *batchID != "" {
		result, applyErr := service.ApplyBatch(ctx, conv.ApplyBatchRequest{BatchID: *batchID})
		envelope := map[string]any{"ok": err == nil, "result": result}
		if applyErr != nil {
			envelope["ok"] = false
			envelope["error"] = errorJSON(applyErr)
		}
		_ = outputJSON(common.out, envelope)
		return applyErr
	}
	if *all || *batchID == "" {
		result, applyErr := service.ApplyRound(ctx)
		envelope := map[string]any{"ok": err == nil, "round": result, "view": service.View().Summary}
		if applyErr != nil {
			envelope["ok"] = false
			envelope["error"] = errorJSON(applyErr)
		}
		_ = outputJSON(common.out, envelope)
		return applyErr
	}
	return nil
}

func runRecover(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	common := bindCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, recovery, err := conv.OpenService(ctx, conv.ServiceOptions{DBPath: common.db})
	if err != nil {
		return err
	}
	defer service.Close()
	envelope := map[string]any{"ok": true, "recovery": recovery}
	if view := service.View(); view != nil {
		envelope["summary"] = view.Summary
		envelope["plan"] = view.Plan
	}
	return outputJSON(common.out, envelope)
}

func runView(args []string) error {
	fs := flag.NewFlagSet("view", flag.ContinueOnError)
	common := bindCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service, _, err := conv.OpenService(ctx, conv.ServiceOptions{DBPath: common.db, EndpointOverride: common.endpoint})
	if err != nil {
		return err
	}
	defer service.Close()
	view := service.View()
	if view == nil {
		return conv.NewError(conv.CodeNotFound, "no committed version", nil)
	}
	return outputJSON(common.out, map[string]any{
		"ok":      true,
		"summary": view.Summary,
		"plan":    view.Plan,
		"drift":   view.DriftList,
		"cascade": view.Cascade,
		"batches": view.BatchStates,
	})
}

func outputMaybe(out string, value any) error {
	if out == "" {
		return nil
	}
	return outputJSON(out, value)
}

func errorJSON(err error) map[string]any {
	business := conv.AsError(err)
	result := map[string]any{"message": err.Error()}
	if business != nil {
		result["code"] = business.Code
		result["details"] = business.Details
	}
	return result
}
