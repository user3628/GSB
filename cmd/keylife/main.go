package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"keylife/internal/adjudicator"
	"keylife/internal/model"
	"keylife/internal/report"
	"keylife/internal/sample"
	"keylife/internal/service"
	"keylife/internal/store"
	"keylife/internal/verify"
)

func main() {
	os.Exit(execute(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stdout)
		return 0
	}
	switch args[0] {
	case "demo":
		return commandDemo(ctx, args[1:], stdout, stderr)
	case "verify":
		return commandVerify(ctx, args[1:], stdout, stderr)
	case "report":
		return commandReport(ctx, args[1:], stdout, stderr)
	case "timeline":
		return commandTimeline(ctx, args[1:], stdout, stderr)
	case "replay":
		return commandReplay(ctx, args[1:], stdout, stderr)
	case "paths":
		return commandPaths(ctx, args[1:], stdout, stderr)
	case "alerts":
		return commandAlerts(ctx, args[1:], stdout, stderr)
	case "inject":
		return commandInject(ctx, args[1:], stdout, stderr)
	case "_crash":
		return commandCrashChild(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `keylife - local deterministic key lifecycle diagnostic workbench

Usage:
  keylife demo      [--dir DIR] [--threshold N] [--now-tick T] [--scale N] [--json] [--report FILE]
  keylife verify    [--root DIR] [--threshold N] [--now-tick T] [--scale N] [--config FILE]
                    [--max-concurrency N] [--subject-limit N] [--anomalies a,b]
                    [--skip-crash] [--out FILE]
  keylife report    --db FILE [--subject S] [--threshold N] [--now-tick T] [--out FILE]
  keylife timeline  --db FILE [--subject S] [--key K] [--now-tick T] [--json]
  keylife replay    --db FILE [--version N] [--json]
  keylife paths     --db FILE [--subject S] [--threshold N] [--now-tick T]
                    [--max-concurrency N] [--subject-limit N] [--compare] [--json]
  keylife alerts    --db FILE [--now-tick T] [--threshold N] [--json]
  keylife inject    --db FILE --type snapshot_gap|snapshot_replacement [--version N] [--to-version N]

All time output uses the fixed mono-local-sample/v1 marker. No network is used.`)
}

func commandDemo(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "workspace directory (temporary when empty)")
	threshold := fs.Int("threshold", 4, "adaptive adjudication threshold")
	nowTick := fs.Int64("now-tick", 420, "fixed monotonic sample tick")
	scale := fs.Int("scale", 2, "extra active alpha keys")
	maxConcurrency := fs.Int("max-concurrency", 8, "global concurrent session limit")
	subjectLimit := fs.Int("subject-limit", 64, "per-subject pending request limit")
	jsonOut := fs.Bool("json", false, "emit JSON diagnostic report")
	reportPath := fs.String("report", "", "write JSON diagnostic report to this path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ws, err := sample.Build(ctx, sample.Options{
		Dir: *dir, Threshold: *threshold, NowTick: *nowTick, Scale: *scale,
		MaxConcurrency: *maxConcurrency, SubjectLimit: *subjectLimit,
	})
	if err != nil {
		return fail(stderr, err)
	}
	defer ws.Close()
	rep, err := report.Build(ctx, ws.Store, report.Options{NowTick: ws.NowTick, Threshold: ws.Threshold})
	if err != nil {
		return fail(stderr, err)
	}
	data, err := report.Bytes(rep)
	if err != nil {
		return fail(stderr, err)
	}
	if *reportPath != "" {
		if err := os.WriteFile(*reportPath, data, 0o644); err != nil {
			return fail(stderr, err)
		}
	}
	if *jsonOut {
		_, _ = stdout.Write(data)
		return 0
	}
	fmt.Fprintln(stdout, "KEY LIFECYCLE WORKBENCH")
	fmt.Fprintf(stdout, "workspace=%s database=%s\n", rep.Workspace, ws.DBPath)
	fmt.Fprintf(stdout, "time=%s as_of=%s threshold=%d max_concurrency=%d subject_limit=%d\n\n", rep.TimeSample, formatTime(rep.AsOf), rep.Adjudication.Threshold, ws.MaxConcurrency, ws.SubjectLimit)
	printTimeline(stdout, rep)
	printPaths(stdout, rep.Adjudication)
	printAlerts(stdout, rep.Alerts)
	if *reportPath != "" {
		fmt.Fprintf(stdout, "\nreport_written=%s\n", *reportPath)
	}
	return 0
}

type fileConfig struct {
	Root           string   `json:"root"`
	Threshold      int      `json:"threshold"`
	NowTick        int64    `json:"now_tick"`
	Scale          int      `json:"scale"`
	MaxConcurrency int      `json:"max_concurrency"`
	SubjectLimit   int      `json:"subject_limit"`
	Anomalies      []string `json:"anomalies"`
	SkipCrash      bool     `json:"skip_crash"`
}

func commandVerify(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	root := fs.String("root", "", "verification root directory")
	threshold := fs.Int("threshold", 4, "adaptive adjudication threshold")
	nowTick := fs.Int64("now-tick", 420, "fixed monotonic sample tick")
	scale := fs.Int("scale", 3, "extra active alpha keys")
	maxConcurrency := fs.Int("max-concurrency", 8, "global concurrent session limit")
	subjectLimit := fs.Int("subject-limit", 64, "per-subject pending request limit")
	anomalies := fs.String("anomalies", "", "comma-separated anomaly types")
	configPath := fs.String("config", "", "JSON configuration file")
	skipCrash := fs.Bool("skip-crash", false, "skip child-process crash recovery checks")
	outPath := fs.String("out", "", "write verification JSON to this path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { visited[f.Name] = true })
	cfg := fileConfig{
		Root: *root, Threshold: *threshold, NowTick: *nowTick, Scale: *scale,
		MaxConcurrency: *maxConcurrency, SubjectLimit: *subjectLimit, SkipCrash: *skipCrash,
	}
	if *anomalies != "" {
		cfg.Anomalies = splitList(*anomalies)
	}
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return fail(stderr, err)
		}
		var fileCfg fileConfig
		if err := json.Unmarshal(data, &fileCfg); err != nil {
			return fail(stderr, fmt.Errorf("parse config: %w", err))
		}
		if !visited["root"] {
			cfg.Root = fileCfg.Root
		}
		if !visited["threshold"] && fileCfg.Threshold > 0 {
			cfg.Threshold = fileCfg.Threshold
		}
		if !visited["now-tick"] && fileCfg.NowTick > 0 {
			cfg.NowTick = fileCfg.NowTick
		}
		if !visited["scale"] && fileCfg.Scale > 0 {
			cfg.Scale = fileCfg.Scale
		}
		if !visited["max-concurrency"] && fileCfg.MaxConcurrency > 0 {
			cfg.MaxConcurrency = fileCfg.MaxConcurrency
		}
		if !visited["subject-limit"] && fileCfg.SubjectLimit > 0 {
			cfg.SubjectLimit = fileCfg.SubjectLimit
		}
		if !visited["anomalies"] && len(fileCfg.Anomalies) > 0 {
			cfg.Anomalies = fileCfg.Anomalies
		}
		if !visited["skip-crash"] {
			cfg.SkipCrash = fileCfg.SkipCrash
		}
	}
	self, err := os.Executable()
	if err != nil {
		return fail(stderr, err)
	}
	suite, err := verify.Run(ctx, verify.Options{
		Root: cfg.Root, Threshold: cfg.Threshold, NowTick: cfg.NowTick, Scale: cfg.Scale,
		MaxConcurrency: cfg.MaxConcurrency, SubjectLimit: cfg.SubjectLimit,
		Anomalies: cfg.Anomalies, SkipCrash: cfg.SkipCrash, SelfExe: self,
	})
	if err != nil {
		return fail(stderr, err)
	}
	data, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		return fail(stderr, err)
	}
	data = append(data, '\n')
	if *outPath != "" {
		if err := os.WriteFile(*outPath, data, 0o644); err != nil {
			return fail(stderr, err)
		}
	}
	_, _ = stdout.Write(data)
	if !suite.Passed {
		return 1
	}
	return 0
}

func commandReport(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	subject := fs.String("subject", "", "optional subject filter")
	threshold := fs.Int("threshold", 4, "adaptive adjudication threshold")
	nowTick := fs.Int64("now-tick", 420, "fixed monotonic sample tick")
	outPath := fs.String("out", "", "output path (stdout when empty)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dbPath == "" {
		return fail(stderr, model.NewError(model.CodeConfig, "--db is required"))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	defer st.Close()
	rep, err := report.Build(ctx, st, report.Options{NowTick: *nowTick, Threshold: *threshold, Subject: *subject})
	if err != nil {
		return fail(stderr, err)
	}
	data, err := report.Bytes(rep)
	if err != nil {
		return fail(stderr, err)
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, data, 0o644); err != nil {
			return fail(stderr, err)
		}
	} else {
		_, _ = stdout.Write(data)
	}
	if !rep.SnapshotChain.Valid {
		fmt.Fprintf(stderr, "snapshot chain %s: %s; recovered through %d\n", rep.SnapshotChain.Code, rep.SnapshotChain.Message, rep.SnapshotChain.RecoveredThrough)
		return 1
	}
	return 0
}

func commandTimeline(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("timeline", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	subject := fs.String("subject", "", "optional subject filter")
	keyID := fs.String("key", "", "optional key filter")
	nowTick := fs.Int64("now-tick", 420, "fixed monotonic sample tick")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dbPath == "" {
		return fail(stderr, model.NewError(model.CodeConfig, "--db is required"))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	defer st.Close()
	chain, chainErr := st.LoadChain(ctx)
	keys, _ := st.ListKeys(ctx, *subject)
	audits, _ := st.ListAudits(ctx)
	fallback := int64(0)
	if len(chain.Entries) > 0 {
		last := chain.Entries[len(chain.Entries)-1].State
		if last != nil {
			keys = filterKeys(last.Keys, *subject, *keyID)
			audits = last.Audits
			fallback = last.Version
		}
	}
	var filteredKeys []model.Key
	for _, key := range keys {
		if *keyID != "" && key.ID != *keyID {
			continue
		}
		filteredKeys = append(filteredKeys, key)
	}
	sort.Slice(filteredKeys, func(i, j int) bool {
		if filteredKeys[i].Subject == filteredKeys[j].Subject {
			return filteredKeys[i].ID < filteredKeys[j].ID
		}
		return filteredKeys[i].Subject < filteredKeys[j].Subject
	})
	if *jsonOut {
		type payload struct {
			Schema    string         `json:"schema"`
			Time      model.MonoTime `json:"as_of"`
			ChainCode model.Code     `json:"chain_code"`
			Fallback  int64          `json:"fallback_version"`
			Keys      []model.Key    `json:"keys"`
			Audits    []model.Audit  `json:"audits"`
		}
		data, _ := json.MarshalIndent(payload{
			Schema: "keylife.timeline/v1", Time: model.T(*nowTick), ChainCode: model.CodeOf(chainErr),
			Fallback: fallback, Keys: filteredKeys, Audits: filterAudits(audits, *subject, *keyID),
		}, "", "  ")
		_, _ = stdout.Write(append(data, '\n'))
		if chainErr != nil {
			return 1
		}
		return 0
	}
	if chainErr != nil {
		fmt.Fprintf(stdout, "CHAIN ALERT %s recovered_through=%d physical_max=%d\n\n", model.CodeOf(chainErr), chain.RecoveredThrough, chain.PhysicalMaxVersion)
	}
	if len(filteredKeys) == 0 {
		fmt.Fprintln(stdout, "no matching keys")
		return boolCode(chainErr == nil, 1)
	}
	for _, key := range filteredKeys {
		metrics := model.Metrics(key, model.T(*nowTick))
		fmt.Fprintf(stdout, "%s/%s phase=%s\n", key.Subject, key.ID, key.Phase)
		for _, audit := range audits {
			if audit.KeyID != key.ID {
				continue
			}
			fmt.Fprintf(stdout, "  %-18s %s -> %s at %s code=%s\n", audit.Action, phaseLabel(audit.FromPhase), phaseLabel(audit.ToPhase), formatTime(audit.At), audit.Code)
		}
		fmt.Fprintf(stdout, "  age=%s rotation=%s expiry_remaining=%s overdue=%t expired=%t\n\n",
			metrics.Age, metrics.RotationRemaining, metrics.LifetimeRemaining, metrics.RotationOverdue, metrics.Expired)
	}
	return boolCode(chainErr == nil, 1)
}

func commandReplay(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	version := fs.Int64("version", 0, "snapshot version (0 = latest contiguous)")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dbPath == "" {
		return fail(stderr, model.NewError(model.CodeConfig, "--db is required"))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	defer st.Close()
	chain, chainErr := st.LoadChain(ctx)
	target := *version
	if target == 0 {
		target = chain.RecoveredThrough
	}
	if target < 1 || target > chain.RecoveredThrough {
		err := &model.BizError{Code: model.CodeSnapshotVersion, Message: fmt.Sprintf("snapshot %d is not in the contiguous chain", target), RecoveredThrough: chain.RecoveredThrough}
		return emitReplayError(stdout, stderr, err, *jsonOut)
	}
	var selected *store.SnapshotEntry
	for i := range chain.Entries {
		if chain.Entries[i].Version == target {
			selected = &chain.Entries[i]
			break
		}
	}
	if selected == nil {
		return emitReplayError(stdout, stderr, model.NewError(model.CodeSnapshotVersion, "snapshot not found"), *jsonOut)
	}
	type replayPayload struct {
		Schema   string               `json:"schema"`
		Version  int64                `json:"version"`
		Meta     store.SnapshotEntry  `json:"meta"`
		State    *model.StateSnapshot `json:"state"`
		ChainOK  bool                 `json:"chain_ok"`
		ChainErr string               `json:"chain_error,omitempty"`
	}
	meta := *selected
	meta.Payload = nil
	meta.State = nil
	payload := replayPayload{Schema: "keylife.replay/v1", Version: target, Meta: meta, State: selected.State, ChainOK: chainErr == nil}
	if chainErr != nil {
		payload.ChainErr = chainErr.Error()
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fail(stderr, err)
	}
	_, _ = stdout.Write(append(data, '\n'))
	if chainErr != nil {
		return 1
	}
	return 0
}

func emitReplayError(stdout, stderr io.Writer, err error, jsonOut bool) int {
	if jsonOut {
		data, _ := json.MarshalIndent(struct {
			Schema  string     `json:"schema"`
			Code    model.Code `json:"code"`
			Message string     `json:"message"`
		}{Schema: "keylife.replay/v1", Code: model.CodeOf(err), Message: err.Error()}, "", "  ")
		_, _ = stdout.Write(append(data, '\n'))
	} else {
		fmt.Fprintln(stderr, err)
	}
	return 1
}

func commandPaths(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("paths", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	subject := fs.String("subject", "", "optional subject filter")
	threshold := fs.Int("threshold", 4, "adaptive adjudication threshold")
	nowTick := fs.Int64("now-tick", 420, "fixed monotonic sample tick")
	maxConcurrency := fs.Int("max-concurrency", 8, "global concurrent session limit")
	subjectLimit := fs.Int("subject-limit", 64, "per-subject pending request limit")
	compare := fs.Bool("compare", false, "run and compare both adjudication paths")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dbPath == "" {
		return fail(stderr, model.NewError(model.CodeConfig, "--db is required"))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	defer st.Close()
	clock := model.NewVirtualClock(*nowTick)
	svc := service.New(st, clock, service.Config{MaxConcurrency: *maxConcurrency, DefaultSubjectLimit: *subjectLimit}, service.Hooks{})
	keys, err := st.ListKeys(ctx, *subject)
	if err != nil {
		return fail(stderr, err)
	}
	var requests []model.DecisionRequest
	for _, key := range keys {
		if key.Phase == model.PhaseActive {
			requests = append(requests, model.DecisionRequest{RequestID: "path-" + key.ID, Subject: key.Subject, KeyID: key.ID, Actor: key.Subject, Operation: "use"})
		}
	}
	result, err := svc.Adjudicate(ctx, requests, *threshold)
	if err != nil {
		return fail(stderr, err)
	}
	var comparison *adjudicator.Comparison
	if *compare {
		state, err := svc.LoadEvaluationState(ctx, model.T(*nowTick))
		if err != nil {
			return fail(stderr, err)
		}
		cmp, err := adjudicator.ComparePaths(requests, state, *threshold)
		if err != nil {
			return fail(stderr, err)
		}
		comparison = &cmp
	}
	if *jsonOut {
		payload := struct {
			Batch      adjudicator.BatchResult `json:"batch"`
			Comparison *adjudicator.Comparison `json:"comparison,omitempty"`
		}{Batch: result, Comparison: comparison}
		data, _ := json.MarshalIndent(payload, "", "  ")
		_, _ = stdout.Write(append(data, '\n'))
		return 0
	}
	fmt.Fprintf(stdout, "threshold=%d input=%d pending_keys=%d digest=%s\n", result.Threshold, result.InputCount, result.PendingKeys, result.Digest)
	for _, path := range result.Paths {
		fmt.Fprintf(stdout, "subject=%s pending=%d path=%s rule=%q\n", path.Subject, path.PendingKeys, path.Selected, path.Rule)
	}
	fmt.Fprintf(stdout, "codes=%s\n", joinCodes(result.DecisionCodes))
	if comparison != nil {
		fmt.Fprintf(stdout, "accurate_digest=%s streaming_digest=%s byte_identical=%t\n", comparison.Accurate.Digest, comparison.Streaming.Digest, comparison.ByteIdentical)
	}
	return 0
}

func commandAlerts(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("alerts", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	nowTick := fs.Int64("now-tick", 420, "fixed monotonic sample tick")
	threshold := fs.Int("threshold", 4, "adjudication threshold")
	jsonOut := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dbPath == "" {
		return fail(stderr, model.NewError(model.CodeConfig, "--db is required"))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	defer st.Close()
	rep, err := report.Build(ctx, st, report.Options{NowTick: *nowTick, Threshold: *threshold})
	if err != nil {
		return fail(stderr, err)
	}
	if *jsonOut {
		data, _ := json.MarshalIndent(struct {
			Schema string         `json:"schema"`
			Alerts []report.Alert `json:"alerts"`
		}{Schema: "keylife.alerts/v1", Alerts: rep.Alerts}, "", "  ")
		_, _ = stdout.Write(append(data, '\n'))
		return boolCode(rep.SnapshotChain.Valid, 1)
	}
	if len(rep.Alerts) == 0 {
		fmt.Fprintln(stdout, "no active alerts")
		return 0
	}
	for _, alert := range rep.Alerts {
		fmt.Fprintf(stdout, "%-24s subject=%s key=%s %s\n", alert.Code, emptyDash(alert.Subject), emptyDash(alert.KeyID), alert.Detail)
	}
	return boolCode(rep.SnapshotChain.Valid, 1)
}

func commandInject(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	kind := fs.String("type", "", "snapshot_gap or snapshot_replacement")
	version := fs.Int64("version", 0, "source version")
	toVersion := fs.Int64("to-version", 0, "jump target version")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dbPath == "" || *kind == "" {
		return fail(stderr, model.NewError(model.CodeConfig, "--db and --type are required"))
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	defer st.Close()
	chain, err := st.LoadChain(ctx)
	if err != nil {
		return fail(stderr, err)
	}
	if *version == 0 {
		*version = chain.RecoveredThrough
	}
	switch *kind {
	case "snapshot_gap":
		if *toVersion == 0 {
			*toVersion = *version + 2
		}
		if err := st.InjectSnapshotGap(ctx, *version, *toVersion, 0); err != nil {
			return fail(stderr, err)
		}
	case "snapshot_replacement":
		entry, err := st.GetSnapshot(ctx, *version)
		if err != nil {
			return fail(stderr, err)
		}
		replacement := *entry.State
		replacement.Keys = append([]model.Key(nil), replacement.Keys...)
		replacement.Schema = "keylife.snapshot/replaced"
		if len(replacement.Keys) > 0 {
			replacement.Keys[0].Phase = model.PhaseDestroyed
		}
		payload, _ := json.Marshal(replacement)
		if err := st.InjectSnapshotReplacement(ctx, *version, payload); err != nil {
			return fail(stderr, err)
		}
	default:
		return fail(stderr, model.NewError(model.CodeConfig, "unknown injection type %q", *kind))
	}
	after, loadErr := st.LoadChain(ctx)
	payload := struct {
		Schema           string     `json:"schema"`
		Injected         string     `json:"injected"`
		LoadCode         model.Code `json:"load_code"`
		RecoveredThrough int64      `json:"recovered_through"`
		PhysicalMax      int64      `json:"physical_max"`
	}{Schema: "keylife.injection/v1", Injected: *kind, LoadCode: model.CodeOf(loadErr), RecoveredThrough: after.RecoveredThrough, PhysicalMax: after.PhysicalMaxVersion}
	data, _ := json.MarshalIndent(payload, "", "  ")
	_, _ = stdout.Write(append(data, '\n'))
	if loadErr == nil {
		return 1
	}
	return 0
}

func commandCrashChild(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("_crash", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "database")
	keyID := fs.String("key", "", "key")
	actor := fs.String("actor", "", "actor")
	point := fs.String("crash-point", "before_commit", "before_commit or after_commit")
	nowTick := fs.Int64("now-tick", 420, "monotonic tick")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return fail(stderr, err)
	}
	clock := model.NewVirtualClock(*nowTick)
	hooks := service.Hooks{}
	switch *point {
	case "before_commit":
		hooks.BeforeCommit = func(operation, key string) { os.Exit(91) }
	case "after_commit":
		hooks.AfterCommit = func(operation, key string) { os.Exit(91) }
	default:
		return fail(stderr, model.NewError(model.CodeConfig, "unknown crash point %q", *point))
	}
	svc := service.New(st, clock, service.Config{MaxConcurrency: 2, DefaultSubjectLimit: 8}, hooks)
	_, err = svc.Rotate(ctx, service.TransitionRequest{Actor: *actor, KeyID: *keyID})
	if err != nil {
		return fail(stderr, err)
	}
	return 0
}

func printTimeline(w io.Writer, rep report.Report) {
	fmt.Fprintln(w, "LIFECYCLE TIMELINES")
	byKey := make(map[string][]model.Audit)
	for _, audit := range rep.Transitions {
		byKey[audit.KeyID] = append(byKey[audit.KeyID], audit)
	}
	for _, item := range rep.Keys {
		key := item.Key
		fmt.Fprintf(w, "  %s/%s phase=%s age=%s rotation_remaining=%s\n", key.Subject, key.ID, key.Phase, item.Metrics.Age, item.Metrics.RotationRemaining)
		if len(byKey[key.ID]) == 0 {
			fmt.Fprintln(w, "    (no transition audit)")
			continue
		}
		fmt.Fprint(w, "    ")
		for i, audit := range byKey[key.ID] {
			if i > 0 {
				fmt.Fprint(w, " -> ")
			}
			fmt.Fprintf(w, "[%s]@%s", phaseLabel(audit.ToPhase), formatTime(audit.At))
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w)
}

func printPaths(w io.Writer, adj report.AdjudicationReport) {
	fmt.Fprintf(w, "ADJUDICATION threshold=%d input=%d digest=%s\n", adj.Threshold, adj.InputCount, adj.Digest)
	for _, path := range adj.Paths {
		fmt.Fprintf(w, "  subject=%s pending=%d selected=%s rule=%q\n", path.Subject, path.PendingKeys, path.Selected, path.Rule)
	}
	if len(adj.DecisionCodes) > 0 {
		fmt.Fprintf(w, "  codes=%s\n", joinCodes(adj.DecisionCodes))
	}
	fmt.Fprintln(w)
}

func printAlerts(w io.Writer, alerts []report.Alert) {
	fmt.Fprintf(w, "ALERTS count=%d\n", len(alerts))
	for _, alert := range alerts {
		fmt.Fprintf(w, "  %-24s subject=%s key=%s %s\n", alert.Code, emptyDash(alert.Subject), emptyDash(alert.KeyID), alert.Detail)
	}
}

func filterKeys(keys []model.Key, subject, keyID string) []model.Key {
	var out []model.Key
	for _, key := range keys {
		if subject != "" && key.Subject != subject {
			continue
		}
		if keyID != "" && key.ID != keyID {
			continue
		}
		out = append(out, key)
	}
	return out
}

func filterAudits(audits []model.Audit, subject, keyID string) []model.Audit {
	var out []model.Audit
	for _, audit := range audits {
		if subject != "" && audit.Subject != subject {
			continue
		}
		if keyID != "" && audit.KeyID != keyID {
			continue
		}
		out = append(out, audit)
	}
	return out
}

func splitList(value string) []string {
	var result []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func formatTime(t model.MonoTime) string {
	data, _ := json.Marshal(t)
	return strings.Trim(string(data), `"`)
}

func phaseLabel(phase model.Phase) string {
	if phase == "" {
		return "start"
	}
	return string(phase)
}

func joinCodes(codes []model.Code) string {
	parts := make([]string, len(codes))
	for i, code := range codes {
		parts[i] = string(code)
	}
	return strings.Join(parts, ",")
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func boolCode(ok bool, failure int) int {
	if ok {
		return 0
	}
	return failure
}

func fail(stderr io.Writer, err error) int {
	code := model.CodeOf(err)
	if code != model.CodeConfig {
		fmt.Fprintf(stderr, "%s: %v\n", code, err)
	} else {
		fmt.Fprintln(stderr, err)
	}
	return 2
}
