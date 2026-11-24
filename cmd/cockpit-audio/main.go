package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	engine "cockpitaudio"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "demo":
		err = runDemo(os.Args[2:], false)
	case "resume":
		err = runDemo(os.Args[2:], true)
	case "verify":
		err = runVerify(os.Args[2:])
	case "query":
		err = runQuery(os.Args[2:])
	case "inject":
		err = runInject(os.Args[2:])
	case "version":
		fmt.Println("cockpit-audio demo v1.0.0; modernc.org/sqlite v1.29.0")
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		var ae *engine.Error
		if errors.As(err, &ae) {
			b, _ := engine.CanonicalJSON(ae)
			fmt.Fprintln(os.Stderr, string(b))
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "E_INTERNAL: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  cockpit-audio demo [--workdir DIR] [--fresh] [--crash-after N] [--budget-ms MS]
  cockpit-audio resume [--workdir DIR]
  cockpit-audio query [--workdir DIR] --type active_zones --at 250 [--budget-ms 5]
  cockpit-audio verify [--workdir DIR]
  cockpit-audio inject --kind sample_count|rate_conflict|offset_overlap|max_speakers_zero|mask_iterations_negative|speed_non_positive [--workdir DIR]`)
}

func defaultWorkdir() string {
	return filepath.Join(os.TempDir(), "cockpit-audio-demo")
}

type demoFlags struct {
	workdir     string
	fresh       bool
	crashAfter  int
	budgetMS    float64
	publisher   string
	holdUnitMS  int
	progress    bool
	sampleEdits bool
}

func parseDemoFlags(args []string) (*demoFlags, error) {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	f := &demoFlags{}
	fs.StringVar(&f.workdir, "workdir", defaultWorkdir(), "temporary working directory")
	fs.BoolVar(&f.fresh, "fresh", false, "remove existing state.db files before run")
	fs.IntVar(&f.crashAfter, "crash-after", 0, "hard exit after N newly committed units")
	fs.Float64Var(&f.budgetMS, "budget-ms", engine.DefaultConfig().LatencyBudgetMS, "query latency budget")
	fs.StringVar(&f.publisher, "publisher", "demo-worker", "checkpoint publisher id")
	fs.IntVar(&f.holdUnitMS, "hold-unit-ms", 0, "test only: sleep after each committed unit")
	fs.BoolVar(&f.progress, "progress", false, "print committed checkpoints")
	fs.BoolVar(&f.sampleEdits, "sample-edits", false, "apply deterministic split/merge/priority/hit edits")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return f, nil
}

func runDemo(args []string, resume bool) error {
	f, err := parseDemoFlags(args)
	if err != nil {
		return err
	}
	if resume {
		f.fresh = false
	}
	if err := os.MkdirAll(f.workdir, 0o755); err != nil {
		return err
	}
	sampleDir := filepath.Join(f.workdir, "samples")
	ds, err := engine.GenerateDataset(engine.GenerateOptions{Root: sampleDir})
	if err != nil {
		return err
	}

	dbPath := filepath.Join(f.workdir, "state.db")
	if f.fresh {
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + "-wal")
		_ = os.Remove(dbPath + "-shm")
	}
	store, err := engine.OpenStore(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	cfg := engine.DefaultConfig()
	cfg.LatencyBudgetMS = f.budgetMS
	edit := (*engine.EditState)(nil)
	editsMarker := filepath.Join(f.workdir, "sample-edits.json")
	_, markerErr := os.Stat(editsMarker)
	if f.sampleEdits || markerErr == nil {
		sampleEdit, serr := engine.BuildSampleEditState(ds, cfg)
		if serr != nil {
			return serr
		}
		edit = sampleEdit
		if werr := writeStableJSON(editsMarker, edit.LogCopy()); werr != nil {
			return werr
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := engine.NewEngine(ds, cfg, store, f.publisher).WithEdit(edit)
	eng.OnUnitCommitted = func(committed int, shardID, stage string, sequence int) {
		if f.progress || f.crashAfter > 0 {
			fmt.Fprintf(os.Stderr, "committed %s/%s seq=%d (%d/%d)\n", shardID, stage, sequence, committed, f.crashAfter)
		}
		if f.holdUnitMS > 0 {
			time.Sleep(time.Duration(f.holdUnitMS) * time.Millisecond)
		}
		if f.crashAfter > 0 && committed >= f.crashAfter {
			fmt.Fprintln(os.Stderr, "hard exit now")
			os.Exit(137)
		}
	}

	report, eerr := eng.Run(ctx)
	if eerr != nil {
		if ae, ok := engine.AsError(eerr); ok && ae.Code == engine.CodeStopped {
			_ = writeStableJSON(filepath.Join(f.workdir, "stop-report.json"), report)
			fmt.Printf("graceful stop after %d committed, %d skipped; resume with: cockpit-audio resume --workdir %s\n",
				len(report.Committed), len(report.Skipped), f.workdir)
			return nil
		}
		return eerr
	}

	reportPath := filepath.Join(f.workdir, "report.json")
	if err := writeStableJSON(reportPath, report.Result); err != nil {
		return err
	}
	queryPath := filepath.Join(f.workdir, "query-results.json")
	if err := writeDemoQueries(queryPath, cfg, report.Result); err != nil {
		return err
	}
	fmt.Printf("demo complete: %s\n", reportPath)
	fmt.Printf("input=%s config=%s edit=v%d shards=%d intervals=%d overlaps=%d commands=%d ambiguous=%d\n",
		report.Result.InputFingerprint[:12], report.Result.ConfigVersion, report.Result.EditVersion,
		report.Result.Summary.ShardCount, report.Result.Summary.IntervalCount,
		report.Result.Summary.OverlapCount, report.Result.Summary.CommandCount, report.Result.Summary.AmbiguousCount)
	fmt.Printf("sqlite=%s\n", dbPath)
	return nil
}

func writeDemoQueries(path string, cfg engine.Config, result *engine.SessionResult) error {
	svc, err := engine.NewQueryService(cfg, result)
	if err != nil {
		return err
	}
	ctx := context.Background()
	requests := []engine.QueryRequest{
		{QueryID: "demo-active-250", Type: engine.QueryActiveZones, AtMS: 250},
		{QueryID: "demo-active-650", Type: engine.QueryActiveZones, AtMS: 650},
	}
	for _, shard := range result.Shards {
		if shard.Attribution == nil {
			continue
		}
		for _, a := range shard.Attribution.Items {
			requests = append(requests,
				engine.QueryRequest{QueryID: "demo-basis-" + a.CommandID, Type: engine.QueryAttributionBasis, CommandID: a.CommandID},
				engine.QueryRequest{QueryID: "demo-delay-" + a.CommandID, Type: engine.QueryDelayDetails, CommandID: a.CommandID},
			)
		}
	}
	type item struct {
		Request  engine.QueryRequest   `json:"request"`
		Response *engine.QueryResponse `json:"response"`
	}
	items := make([]item, 0, len(requests))
	for _, req := range requests {
		resp, qerr := svc.Query(ctx, req)
		if qerr != nil {
			return qerr
		}
		resp.ElapsedMS = 0
		items = append(items, item{Request: req, Response: resp})
	}
	return writeStableJSON(path, items)
}

func writeStableJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return f.Close()
}

func runInject(args []string) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	kind := fs.String("kind", "", "injection kind")
	workdir := fs.String("workdir", filepath.Join(defaultWorkdir(), "inject"), "work directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *kind == "" {
		return fmt.Errorf("--kind is required")
	}
	if err := os.MkdirAll(*workdir, 0o755); err != nil {
		return err
	}
	cfg := engine.DefaultConfig()
	inj := engine.Injection{Kind: *kind}
	switch *kind {
	case "sample_count", "rate_conflict", "offset_overlap":
		if *kind == "offset_overlap" {
			inj.OverlapZoneOffsets = true
			inj.Kind = ""
		}
	case "max_speakers_zero":
		cfg.MaxSpeakers = 0
	case "mask_iterations_negative":
		cfg.MaskIterations = -1
	case "speed_non_positive":
		cfg.SpeedMPS = 0
	default:
		return fmt.Errorf("unknown injection %q", *kind)
	}
	ds, err := engine.GenerateDataset(engine.GenerateOptions{Root: filepath.Join(*workdir, "samples"), Injection: inj})
	if err != nil {
		return err
	}
	_, berr := engine.BuildSession(ds, cfg, nil)
	if berr == nil {
		return fmt.Errorf("injection %s did not produce an error", *kind)
	}
	return berr
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	workdir := fs.String("workdir", filepath.Join(defaultWorkdir(), "verify"), "verification directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*workdir, 0o755); err != nil {
		return err
	}
	report, verr := engine.RunLocalVerification(*workdir)
	if report != nil {
		path := filepath.Join(*workdir, "verification.json")
		_ = writeStableJSON(path, report)
	}
	if verr != nil {
		return verr
	}
	path := filepath.Join(*workdir, "verification.json")
	fmt.Printf("verification ok: %s\n", path)
	return nil
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	workdir := fs.String("workdir", defaultWorkdir(), "work directory containing samples")
	queryType := fs.String("type", "active_zones", "active_zones|delay_details|attribution_basis")
	atMS := fs.Int("at", 250, "active query timestamp in milliseconds")
	startMS := fs.Int("start", 0, "range start in milliseconds")
	endMS := fs.Int("end", 0, "range end in milliseconds")
	commandID := fs.String("command", "", "command id")
	shardID := fs.String("shard", "", "shard id")
	queryID := fs.String("query-id", "cli-query", "query id")
	budgetMS := fs.Float64("budget-ms", engine.DefaultConfig().LatencyBudgetMS, "latency budget in milliseconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ds, err := engine.GenerateDataset(engine.GenerateOptions{Root: filepath.Join(*workdir, "samples")})
	if err != nil {
		return err
	}
	cfg := engine.DefaultConfig()
	cfg.LatencyBudgetMS = *budgetMS
	result, berr := engine.BuildSession(ds, cfg, nil)
	if berr != nil {
		return berr
	}
	svc, qerr := engine.NewQueryService(cfg, result)
	if qerr != nil {
		return qerr
	}
	req := engine.QueryRequest{
		QueryID: *queryID, Type: engine.QueryType(*queryType), AtMS: *atMS,
		StartMS: *startMS, EndMS: *endMS, CommandID: *commandID, ShardID: *shardID,
	}
	resp, qerr := svc.Query(context.Background(), req)
	if qerr != nil {
		return qerr
	}
	b, err := engine.CanonicalJSON(resp)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
