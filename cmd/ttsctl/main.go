package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/user3628/GSB/ttsflow"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		writeError(err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage(os.Stdout)
		return nil
	}
	ctx := context.Background()
	switch args[0] {
	case "init":
		return cmdInit(ctx, args[1:])
	case "run":
		return cmdRun(ctx, args[1:], false)
	case "recover":
		return cmdRun(ctx, args[1:], true)
	case "status":
		return cmdStatus(ctx, args[1:])
	case "versions":
		return cmdVersions(ctx, args[1:])
	case "fingerprint":
		return cmdFingerprint(ctx, args[1:])
	case "stop":
		return cmdStop(ctx, args[1:])
	case "report":
		return cmdReport(ctx, args[1:])
	case "import-texts":
		return cmdImportTexts(ctx, args[1:])
	case "replace-config":
		return cmdReplaceConfig(ctx, args[1:])
	case "demo":
		return cmdDemo(ctx, args[1:])
	case "__demo-child":
		return cmdDemoChild(ctx, args[1:])
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `ttsctl - deterministic streaming TTS checkpoint component demo

Commands:
  init           Generate synthesis.yaml and texts.jsonl from the fixed seed
  run            Stream text frontend, acoustic, wave and consistency stages
  recover        Resume from committed SQLite facts (clears stale stop request)
  status         Query one active frame-segment checkpoint
  versions       Show current text/config content versions
  fingerprint    Show the four stage fingerprints for a segment
  stop           Persist a graceful stop request
  report         Export a JSON diagnostic report without synthesizing
  import-texts   Import JSONL with append, update or replace semantics
  replace-config Replace the YAML config and invalidate all derived state
  demo           Full run + real child-process kill/recover + incremental check

Common flags: --dir (default .ttsflow), --utt utt-001[,utt-002]`)
}

func parseFlagSet(name string, args []string) (*flag.FlagSet, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs, nil
}

func splitUTTs(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func openEngine(dir string) (ttsflow.WorkspacePaths, *ttsflow.Engine, func(), error) {
	paths, cfg, raw, samples, store, err := ttsflow.OpenWorkspace(dir)
	if err != nil {
		return paths, nil, nil, err
	}
	engine, err := ttsflow.NewEngine(cfg, raw, store, samples)
	if err != nil {
		_ = store.Close()
		return paths, nil, nil, err
	}
	return paths, engine, func() { _ = store.Close() }, nil
}

func cmdInit(_ context.Context, args []string) error {
	fs, err := parseFlagSet("init", args)
	if err != nil {
		return err
	}
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	seed := fs.Int64("seed", ttsflow.DefaultSeed, "fixed generator seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	paths, cfg, _, samples, err := ttsflow.EnsureWorkspace(*dir, *seed)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{
		"dir": paths.Dir, "config": paths.Config, "texts": paths.Texts, "database": paths.DB,
		"config_digest": cfg.Digest(), "text_set_digest": ttsflow.TextSetDigest(samples), "utterances": len(samples),
	})
}

func cmdRun(ctx context.Context, args []string, recoverMode bool) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	utt := fs.String("utt", "", "comma-separated utt_id list")
	stopAfter := fs.Int("stop-after", 0, "stop gracefully after N completed frame segments")
	delayMS := fs.Int("delay-ms", 0, "delay between committed segments")
	force := fs.Bool("force-rebuild", false, "discard active checkpoints before running")
	clearStops := fs.Bool("clear-stops", recoverMode, "clear stale stop requests before recovery")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, engine, closeFn, err := openEngine(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	result, err := engine.Run(ctx, ttsflow.RunOptions{
		UTTIDs: splitUTTs(*utt), ForceRebuild: *force, ClearStops: *clearStops,
		MaxSegments: *stopAfter, Delay: time.Duration(*delayMS) * time.Millisecond,
	})
	if err != nil && !errors.Is(err, ttsflow.ErrGracefulStop) {
		return err
	}
	return printJSON(summary(result))
}

func summary(result ttsflow.SynthesisResult) map[string]any {
	utts := make([]string, 0, len(result.UTTs))
	for _, utt := range result.UTTs {
		utts = append(utts, utt.Sample.UTTID)
	}
	return map[string]any{
		"manifest_digest": result.ManifestDigest, "config_digest": result.ConfigDigest,
		"text_set_digest": result.TextSetDigest, "utterances": utts,
		"checkpoint_count": result.CheckpointCount, "published_stage_checkpoints": result.PublishedStageCheckpoints,
		"reused_stage_checkpoints":     result.ReusedStageCheckpoints,
		"earliest_invalidated_segment": result.EarliestInvalidatedSegment,
		"earliest_invalidated_stage":   result.EarliestInvalidatedStage,
		"graceful_stopped":             result.GracefulStopped,
	}
}

func cmdStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	utt := fs.String("utt", "", "utterance id")
	segment := fs.Int("segment", -1, "frame segment sequence; -1 means latest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *utt == "" {
		return fmt.Errorf("--utt is required")
	}
	_, engine, closeFn, err := openEngine(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	status, ok, err := engine.Status(ctx, *utt, *segment)
	if err != nil {
		return err
	}
	if !ok {
		return printJSON(map[string]any{"utt_id": *utt, "found": false})
	}
	return printJSON(status)
}

func cmdVersions(ctx context.Context, args []string) error {
	fs, err := parseFlagSet("versions", args)
	if err != nil {
		return err
	}
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, engine, closeFn, err := openEngine(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	tv, tok, err := engine.Store.LatestVersion(ctx, "texts")
	if err != nil {
		return err
	}
	cv, cok, err := engine.Store.LatestVersion(ctx, "config")
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"texts": versionOrNil(tv, tok), "config": versionOrNil(cv, cok)})
}

func versionOrNil(v ttsflow.VersionRecord, ok bool) any {
	if !ok {
		return nil
	}
	return v
}

func cmdFingerprint(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fingerprint", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	utt := fs.String("utt", "", "utterance id")
	segment := fs.Int("segment", 0, "frame segment sequence")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *utt == "" {
		return fmt.Errorf("--utt is required")
	}
	_, engine, closeFn, err := openEngine(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	fp, err := engine.VersionFingerprint(ctx, *utt, *segment)
	if err != nil {
		return err
	}
	return printJSON(fp)
}

func cmdStop(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	utt := fs.String("utt", "", "utterance id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *utt == "" {
		return fmt.Errorf("--utt is required")
	}
	_, engine, closeFn, err := openEngine(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	if err := engine.Store.RequestStop(ctx, *utt); err != nil {
		return err
	}
	return printJSON(map[string]any{"utt_id": *utt, "stop_requested": true})
}

func cmdReport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	utt := fs.String("utt", "", "comma-separated utt_id list")
	out := fs.String("out", "", "output JSON file; stdout when empty")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, engine, closeFn, err := openEngine(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	report, err := engine.DiagnosticReport(ctx, splitUTTs(*utt))
	if err != nil {
		return err
	}
	if *out == "" {
		return printJSON(report)
	}
	if err := engine.ExportReport(ctx, splitUTTs(*utt), *out); err != nil {
		return err
	}
	return printJSON(map[string]any{"report": *out})
}

func cmdImportTexts(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import-texts", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	file := fs.String("file", "", "JSONL file to import")
	mode := fs.String("mode", "update", "append, update or replace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("--file is required")
	}
	snapshotMode := ttsflow.SnapshotMode(strings.ToLower(*mode))
	switch snapshotMode {
	case ttsflow.SnapshotAppend, ttsflow.SnapshotUpdate, ttsflow.SnapshotReplace:
	default:
		return fmt.Errorf("--mode must be append, update or replace")
	}
	paths, _, _, existing, store, err := ttsflow.OpenWorkspace(*dir)
	if err != nil {
		return err
	}
	defer store.Close()
	imported, err := ttsflow.LoadJSONL(*file)
	if err != nil {
		return err
	}
	merged := mergeSamples(existing, imported, snapshotMode)
	version, err := store.ObserveTextSet(ctx, merged, snapshotMode)
	if err != nil {
		return err
	}
	if err := ttsflow.WriteJSONL(paths.Texts, merged); err != nil {
		return err
	}
	return printJSON(map[string]any{"version": version, "utterances": len(merged), "text_set_digest": ttsflow.TextSetDigest(merged)})
}

func mergeSamples(existing, imported []ttsflow.TextSample, mode ttsflow.SnapshotMode) []ttsflow.TextSample {
	if mode == ttsflow.SnapshotReplace {
		return append([]ttsflow.TextSample(nil), imported...)
	}
	byID := map[string]ttsflow.TextSample{}
	order := make([]string, 0, len(existing)+len(imported))
	for _, sample := range existing {
		if _, ok := byID[sample.UTTID]; !ok {
			order = append(order, sample.UTTID)
		}
		byID[sample.UTTID] = sample
	}
	for _, sample := range imported {
		if _, ok := byID[sample.UTTID]; !ok {
			order = append(order, sample.UTTID)
		}
		byID[sample.UTTID] = sample
	}
	out := make([]ttsflow.TextSample, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out
}

func cmdReplaceConfig(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("replace-config", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", ttsflow.DefaultWorkspaceDir, "workspace directory")
	file := fs.String("file", "", "new YAML config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("--file is required")
	}
	cfg, raw, err := ttsflow.LoadConfigFile(*file)
	if err != nil {
		return err
	}
	paths, _, _, samples, store, err := ttsflow.OpenWorkspace(*dir)
	if err != nil {
		return err
	}
	defer store.Close()
	engine, err := ttsflow.NewEngine(cfg, raw, store, samples)
	if err != nil {
		return err
	}
	if err := engine.ObserveVersions(ctx, ttsflow.SnapshotAuto, ttsflow.SnapshotReplace); err != nil {
		return err
	}
	if err := writeFile(paths.Config, raw); err != nil {
		return err
	}
	return printJSON(map[string]any{"config_digest": cfg.Digest(), "config_version": engine.ConfigVersion,
		"invalidated_all_derived_state": true})
}

type demoReport struct {
	Dir                     string `json:"dir"`
	FullManifest            string `json:"full_manifest"`
	RecoveredManifest       string `json:"recovered_manifest"`
	IncrementalManifest     string `json:"incremental_manifest"`
	IncrementalFullManifest string `json:"incremental_full_manifest"`
	RecoveryEqual           bool   `json:"recovery_equal"`
	IncrementalEqual        bool   `json:"incremental_equal"`
	HardKillObserved        bool   `json:"hard_kill_observed"`
	RecoveryReusedStages    int    `json:"recovery_reused_stages"`
	IncrementalReusedStages int    `json:"incremental_reused_stages"`
	IncrementalFirstSegment int    `json:"incremental_first_segment"`
	IncrementalFirstStage   string `json:"incremental_first_stage"`
}

func cmdDemo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", "artifacts/demo", "demo parent directory")
	seed := fs.Int64("seed", ttsflow.DefaultSeed, "fixed seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}
	fullDir := filepath.Join(*dir, "full")
	interruptedDir := filepath.Join(*dir, "interrupted")
	incrementalDir := filepath.Join(*dir, "incremental")
	fullV2Dir := filepath.Join(*dir, "full-v2")
	for _, path := range []string{fullDir, interruptedDir, incrementalDir, fullV2Dir} {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	full, err := runWorkspace(ctx, fullDir, *seed, ttsflow.RunOptions{})
	if err != nil {
		return fmt.Errorf("full baseline: %w", err)
	}
	if _, _, _, _, err := ttsflow.EnsureWorkspace(interruptedDir, *seed); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(exe, "__demo-child", "--dir", interruptedDir)
	child.Stdout = io.Discard
	var childLog strings.Builder
	child.Stderr = &childLog
	if err := child.Start(); err != nil {
		return err
	}
	store, err := ttsflow.OpenStore(filepath.Join(interruptedDir, ttsflow.WorkspaceDatabaseName))
	if err != nil {
		_ = child.Process.Kill()
		return err
	}
	hardKill := false
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		done, _ := store.CompletedSegments(ctx, "utt-001")
		if done >= 1 {
			if err := child.Process.Kill(); err == nil {
				hardKill = true
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = child.Wait()
	_ = store.Close()
	if !hardKill {
		return fmt.Errorf("child process completed before hard kill; stderr=%s", childLog.String())
	}

	paths, cfg, raw, samples, store, err := ttsflow.OpenWorkspace(interruptedDir)
	if err != nil {
		return err
	}
	engine, err := ttsflow.NewEngine(cfg, raw, store, samples)
	if err != nil {
		_ = store.Close()
		return err
	}
	recovered, err := engine.Run(ctx, ttsflow.RunOptions{ClearStops: true})
	_ = store.Close()
	if err != nil && !errors.Is(err, ttsflow.ErrGracefulStop) {
		return fmt.Errorf("recover: %w", err)
	}
	if err := ttsflow.CompareSynthesisResults(recovered, full); err != nil {
		return fmt.Errorf("recovery comparison: %w", err)
	}

	if _, err := runWorkspace(ctx, incrementalDir, *seed, ttsflow.RunOptions{}); err != nil {
		return err
	}
	paths, cfg, _, samples, store, err = ttsflow.OpenWorkspace(incrementalDir)
	if err != nil {
		return err
	}
	cfg2 := cfg
	cfg2.Revision++
	cfg2.Acoustic.WeightSeed++
	raw2, err := cfg2.EncodeYAML()
	if err != nil {
		_ = store.Close()
		return err
	}
	engine2, err := ttsflow.NewEngine(cfg2, raw2, store, samples)
	if err != nil {
		_ = store.Close()
		return err
	}
	incremental, err := engine2.Run(ctx, ttsflow.RunOptions{})
	_ = store.Close()
	if err != nil {
		return fmt.Errorf("incremental: %w", err)
	}
	if err := writeFile(paths.Config, raw2); err != nil {
		return err
	}
	fullV2, err := runWorkspaceConfig(ctx, fullV2Dir, *seed, cfg2, raw2)
	if err != nil {
		return err
	}
	if err := ttsflow.CompareSynthesisResults(incremental, fullV2); err != nil {
		return fmt.Errorf("incremental comparison: %w", err)
	}
	report := demoReport{
		Dir: *dir, FullManifest: full.ManifestDigest, RecoveredManifest: recovered.ManifestDigest,
		IncrementalManifest: incremental.ManifestDigest, IncrementalFullManifest: fullV2.ManifestDigest,
		RecoveryEqual: true, IncrementalEqual: true, HardKillObserved: hardKill,
		RecoveryReusedStages:    recovered.ReusedStageCheckpoints,
		IncrementalReusedStages: incremental.ReusedStageCheckpoints,
		IncrementalFirstSegment: incremental.EarliestInvalidatedSegment,
		IncrementalFirstStage:   incremental.EarliestInvalidatedStage,
	}
	if err := os.WriteFile(filepath.Join(*dir, "demo-report.json"), []byte(prettyJSON(report)), 0o644); err != nil {
		return err
	}
	return printJSON(report)
}

func cmdDemoChild(ctx context.Context, args []string) error {
	fs, err := parseFlagSet("__demo-child", args)
	if err != nil {
		return err
	}
	dir := fs.String("dir", "", "workspace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, engine, closeFn, err := openEngine(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	_, err = engine.Run(ctx, ttsflow.RunOptions{Delay: 35 * time.Millisecond})
	if err != nil && !errors.Is(err, ttsflow.ErrGracefulStop) {
		return err
	}
	return nil
}

func runWorkspace(ctx context.Context, dir string, seed int64, opts ttsflow.RunOptions) (ttsflow.SynthesisResult, error) {
	_, cfg, raw, _, err := ttsflow.EnsureWorkspace(dir, seed)
	if err != nil {
		return ttsflow.SynthesisResult{}, err
	}
	return runWorkspaceConfig(ctx, dir, seed, cfg, raw)
}

func runWorkspaceConfig(ctx context.Context, dir string, seed int64, cfg ttsflow.Config, raw []byte) (ttsflow.SynthesisResult, error) {
	paths, _, _, samples, err := ttsflow.EnsureWorkspace(dir, seed)
	if err != nil {
		return ttsflow.SynthesisResult{}, err
	}
	if err := writeFile(paths.Config, raw); err != nil {
		return ttsflow.SynthesisResult{}, err
	}
	store, err := ttsflow.OpenStore(filepath.Join(dir, ttsflow.WorkspaceDatabaseName))
	if err != nil {
		return ttsflow.SynthesisResult{}, err
	}
	defer store.Close()
	engine, err := ttsflow.NewEngine(cfg, raw, store, samples)
	if err != nil {
		return ttsflow.SynthesisResult{}, err
	}
	result, err := engine.Run(ctx, ttsflow.RunOptions{})
	if err != nil && !errors.Is(err, ttsflow.ErrGracefulStop) {
		return result, err
	}
	return result, nil
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func printJSON(v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(data))
	return err
}

func prettyJSON(v any) string {
	data, _ := json.MarshalIndent(v, "", "  ")
	return string(data) + "\n"
}

func writeError(err error) {
	payload := map[string]any{"code": "E_INTERNAL", "message": err.Error()}
	if be, ok := ttsflow.AsBizError(err); ok {
		data, _ := json.Marshal(be)
		var m map[string]any
		_ = json.Unmarshal(data, &m)
		payload = m
	}
	data, _ := json.MarshalIndent(payload, "", "  ")
	fmt.Fprintln(os.Stderr, string(data))
}
