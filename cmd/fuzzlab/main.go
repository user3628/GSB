package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/demo"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/replay"
	"protocolfuzzlab/internal/report"
	"protocolfuzzlab/internal/store"
	"protocolfuzzlab/internal/target"
	"protocolfuzzlab/internal/verify"
	"protocolfuzzlab/internal/workbench"
)

const (
	programName  = "protocol-fuzzlab"
	buildVersion = "1.0.0"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage()
		return 2
	}
	var err error
	switch args[0] {
	case "init":
		err = cmdInit(args[1:])
	case "samples":
		err = cmdSamples(args[1:])
	case "apply":
		err = cmdApply(args[1:])
	case "status":
		err = cmdStatus(args[1:])
	case "closure":
		err = cmdClosure(args[1:])
	case "matrix":
		err = cmdMatrix(args[1:])
	case "trace":
		err = cmdTrace(args[1:])
	case "replay":
		err = cmdReplay(args[1:])
	case "report":
		err = cmdReport(args[1:])
	case "verify":
		err = cmdVerify(args[1:])
	case "serve":
		err = cmdServe(args[1:])
	case "version":
		fmt.Printf("%s %s\n", programName, buildVersion)
	case "help", "-h", "--help":
		printUsage()
	default:
		printUsage()
		return 2
	}
	if err != nil {
		coded := codes.From(err)
		fmt.Fprintf(os.Stderr, "error_code=%s message=%q", coded.Code, coded.Message)
		if len(coded.Chain) > 0 {
			fmt.Fprintf(os.Stderr, " root_chain=%s", strings.Join(coded.Chain, " -> "))
		}
		if coded.Code == codes.SnapshotVersion {
			fmt.Fprintf(os.Stderr, " fallback_version=%d", coded.FallbackVersion)
		}
		fmt.Fprintln(os.Stderr)
		return 1
	}
	return 0
}

func printUsage() {
	fmt.Print(`protocol-fuzzlab - deterministic custom-protocol fuzzing workbench

Usage:
  fuzzlab init    [--db data/fuzzlab.db] [--samples data]
  fuzzlab samples [--dir data]
  fuzzlab apply   [--db data/fuzzlab.db] --file data/change-01-incremental.json
  fuzzlab status  [--db data/fuzzlab.db]
  fuzzlab closure [--db data/fuzzlab.db] [--version N]
  fuzzlab matrix  [--db data/fuzzlab.db] [--version N]
  fuzzlab trace   [--db data/fuzzlab.db] [--version N] --case CASE_ID
  fuzzlab replay  [--db data/fuzzlab.db] [--version N] [--batch-size 16] [--concurrency 4]
                  [--case-budget 1000] [--tamper-case ID] [--tamper-step 0]
                  [--no-commit] [--crash-after-batches N]
  fuzzlab report  [--db data/fuzzlab.db] [--version N] [--out report/diagnostic-report.json]
  fuzzlab verify  [--inject all|missing-seed|operator-cycle|bad-checksum|snapshot-gap|concurrent-race|forced-restart]
                  [--out data/verification.json]
  fuzzlab serve
  fuzzlab version

All generated samples are deterministic; no external network target is used.
`)
}

func commonDB(fs *flag.FlagSet) *string {
	return fs.String("db", "data/fuzzlab.db", "SQLite database path")
}

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	dbPath := commonDB(fs)
	samplesDir := fs.String("samples", "data", "built-in sample output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll("report", 0o755); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	version, created, err := st.Initialize(demo.BaseCaseSet(), "baseline deterministic protocol corpus")
	if err != nil {
		return err
	}
	files, err := demo.WriteSamples(*samplesDir)
	if err != nil {
		return err
	}
	fmt.Printf("caseset_version=%d\n", version.Version)
	fmt.Printf("caseset_digest=%s\n", version.Digest)
	fmt.Printf("created=%t\n", created)
	fmt.Printf("samples=%s\n", strings.Join(files, ","))
	return nil
}

func cmdSamples(args []string) error {
	fs := flag.NewFlagSet("samples", flag.ContinueOnError)
	dir := fs.String("dir", "data", "sample output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	files, err := demo.WriteSamples(*dir)
	if err != nil {
		return err
	}
	for _, name := range files {
		fmt.Println(filepath.Join(*dir, name))
	}
	return nil
}

func cmdApply(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	dbPath := commonDB(fs)
	file := fs.String("file", "", "change set JSON file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return codes.New(codes.BadRequest, "--file is required")
	}
	data, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var changeSet model.ChangeSet
	if err := json.Unmarshal(data, &changeSet); err != nil {
		return codes.New(codes.BadRequest, "decode change set: %v", err)
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	result, err := st.ApplyChangeSet(changeSet)
	if err != nil {
		return err
	}
	fmt.Printf("before_version=%d before_cases=%d before_operators=%d\n", result.Previous.Version, len(result.Previous.CaseSet.Cases), len(result.Previous.CaseSet.Operators))
	fmt.Printf("after_version=%d after_cases=%d after_operators=%d\n", result.Current.Version, len(result.Current.CaseSet.Cases), len(result.Current.CaseSet.Operators))
	fmt.Printf("caseset_digest=%s\n", result.Current.Digest)
	fmt.Printf("description=%s\n", result.Summary.Description)
	fmt.Printf("added=%s\n", refsText(result.Summary.Added))
	fmt.Printf("modified=%s\n", refsText(result.Summary.Modified))
	fmt.Printf("deleted=%s\n", refsText(result.Summary.Deleted))
	fmt.Printf("affected_cases=%s\n", listText(result.Summary.AffectedCases))
	fmt.Printf("affected_operators=%s\n", listText(result.Summary.AffectedOperators))
	for _, d := range result.Summary.Diagnostics {
		fmt.Printf("diagnostic=%s entity=%s:%s root=%s\n", d.Code, d.Entity, d.EntityID, strings.Join(d.RootChain, " -> "))
	}
	return nil
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dbPath := commonDB(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	text, err := workbench.StatusText(st)
	fmt.Print(text)
	return err
}

func cmdClosure(args []string) error {
	fs := flag.NewFlagSet("closure", flag.ContinueOnError)
	dbPath := commonDB(fs)
	version := fs.Int("version", 0, "case set version (default current head)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	v, err := resolveVersion(st, *version)
	if err != nil {
		return err
	}
	text, err := workbench.ClosureText(st, v)
	fmt.Print(text)
	return err
}

func cmdMatrix(args []string) error {
	fs := flag.NewFlagSet("matrix", flag.ContinueOnError)
	dbPath := commonDB(fs)
	version := fs.Int("version", 0, "case set version (default current head)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	v, err := resolveVersion(st, *version)
	if err != nil {
		return err
	}
	text, err := workbench.MatrixText(st, v)
	fmt.Print(text)
	return err
}

func cmdTrace(args []string) error {
	fs := flag.NewFlagSet("trace", flag.ContinueOnError)
	dbPath := commonDB(fs)
	version := fs.Int("version", 0, "case set version (default current head)")
	caseID := fs.String("case", "", "case identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *caseID == "" {
		return codes.New(codes.BadRequest, "--case is required")
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	v, err := resolveVersion(st, *version)
	if err != nil {
		return err
	}
	text, err := workbench.TraceText(st, v, *caseID)
	fmt.Print(text)
	return err
}

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	dbPath := commonDB(fs)
	version := fs.Int("version", 0, "case set version (default current head)")
	batchSize := fs.Int("batch-size", 16, "cases per session batch")
	concurrency := fs.Int("concurrency", 4, "maximum concurrent sessions")
	caseBudget := fs.Int("case-budget", 1000, "maximum cases per session")
	tamperCase := fs.String("tamper-case", "", "case whose checksum will be tampered")
	tamperStep := fs.Int("tamper-step", 0, "zero-based frame index to tamper")
	noCommit := fs.Bool("no-commit", false, "execute without committing snapshots")
	crashAfter := fs.Int("crash-after-batches", 0, "force-exit after N newly committed batch snapshots")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	v, err := resolveVersion(st, *version)
	if err != nil {
		return err
	}
	opts := replay.Options{
		CaseSetVersion: v, BatchSize: *batchSize, MaxConcurrency: *concurrency,
		CaseBudget: *caseBudget, NoCommit: *noCommit, TamperCaseID: *tamperCase, TamperStep: *tamperStep,
	}
	if *crashAfter > 0 {
		opts.AfterCommit = func(committed int) {
			if committed == *crashAfter {
				fmt.Fprintf(os.Stderr, "forced_exit_after_committed_batches=%d\n", committed)
				os.Exit(99)
			}
		}
	}
	result, err := replay.Run(context.Background(), st, opts)
	printReplayResult(result)
	if err != nil {
		return err
	}
	if result.ChainError != nil {
		return result.ChainError
	}
	return nil
}

func printReplayResult(result replay.RunResult) {
	fmt.Printf("caseset_version=%d\n", result.CaseSetVersion)
	fmt.Printf("caseset_digest=%s\n", result.CaseSetDigest)
	fmt.Printf("target=%s\n", result.Target)
	fmt.Printf("content_digest=%s\n", result.ContentDigest)
	fmt.Printf("scheduler_limit=%d peak_in_flight=%d peak_queued=%d completed=%d failed=%d\n",
		result.Stats.Limit, result.Stats.PeakInFlight, result.Stats.PeakQueued, result.Stats.Completed, result.Stats.Failed)
	if len(result.InvalidCaseIDs) > 0 {
		fmt.Printf("invalid_cases=%s\n", strings.Join(result.InvalidCaseIDs, ","))
	}
	for _, batch := range result.Batches {
		fmt.Printf("batch=%s code=%s cases=%d unprocessed=%d coverage=%d snapshot=%d new=%t\n",
			batch.BatchID, batch.Execution.Code, len(batch.Execution.Cases), len(batch.Execution.Unprocessed),
			len(batch.Execution.Coverage), batch.Snapshot.Version, batch.NewSnapshot)
		for _, c := range batch.Execution.Cases {
			if c.Code != codes.OK {
				root := ""
				if c.Diagnostic != nil {
					root = " root=" + strings.Join(c.Diagnostic.RootChain, " -> ")
				}
				fmt.Printf("case=%s code=%s final_state=%s%s\n", c.CaseID, c.Code, c.FinalState, root)
			}
		}
	}
	if result.ChainError != nil {
		fmt.Printf("snapshot_chain_state=FALLBACK_TO_%d\n", result.ChainError.FallbackVersion)
	} else {
		fmt.Println("snapshot_chain_state=CONTIGUOUS")
	}
}

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dbPath := commonDB(fs)
	version := fs.Int("version", 0, "case set version (default current head)")
	out := fs.String("out", "report/diagnostic-report.json", "deterministic JSON report output")
	batchSize := fs.Int("batch-size", 16, "cases per session batch")
	concurrency := fs.Int("concurrency", 4, "maximum concurrent sessions")
	caseBudget := fs.Int("case-budget", 1000, "maximum cases per session")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	v, err := resolveVersion(st, *version)
	if err != nil {
		return err
	}
	runResult, err := replay.Run(context.Background(), st, replay.Options{
		CaseSetVersion: v, BatchSize: *batchSize, MaxConcurrency: *concurrency, CaseBudget: *caseBudget,
	})
	if err != nil {
		printReplayResult(runResult)
		return err
	}
	r, err := report.Build(st, runResult)
	if err != nil {
		return err
	}
	data, err := report.Marshal(r)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	fmt.Printf("report_path=%s\n", *out)
	fmt.Printf("report_sha256=%s\n", hex.EncodeToString(sum[:]))
	fmt.Printf("report_bytes=%d\n", len(data))
	fmt.Printf("caseset_version=%d\n", r.CaseSet.Version)
	fmt.Printf("snapshot_chain_length=%d\n", len(r.SnapshotChain))
	fmt.Printf("snapshot_chain_state=%s\n", r.SnapshotChainState)
	if r.ChainError != nil {
		return r.ChainError
	}
	return nil
}

type repeatedFlag []string

func (r *repeatedFlag) String() string { return strings.Join(*r, ",") }
func (r *repeatedFlag) Set(value string) error {
	*r = append(*r, value)
	return nil
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	var injections repeatedFlag
	fs.Var(&injections, "inject", "injection scenario; repeatable or comma-separated")
	out := fs.String("out", "", "optional verification JSON output path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	selected := []string(injections)
	if len(selected) == 0 {
		selected = []string{"all"}
	}
	result, err := verify.Run(context.Background(), exe, selected)
	if err != nil {
		return err
	}
	data, err := verify.Marshal(result)
	if err != nil {
		return err
	}
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "verification_report=%s\n", *out)
	}
	os.Stdout.Write(data)
	failed := 0
	for _, scenario := range result.Scenarios {
		if !scenario.Passed {
			failed++
		}
	}
	if failed > 0 {
		return codes.New(codes.BadRequest, "%d verification scenario(s) failed", failed)
	}
	return nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server, err := target.Start(ctx)
	if err != nil {
		return err
	}
	defer server.Close()
	fmt.Printf("target=loopback-deterministic-v1\n")
	fmt.Printf("listen=%s\n", server.Addr())
	fmt.Println("ready=true")
	<-ctx.Done()
	return nil
}

func resolveVersion(st *store.Store, requested int) (int, error) {
	if requested > 0 {
		if _, err := st.LoadCaseSet(requested); err != nil {
			return 0, err
		}
		return requested, nil
	}
	head, err := st.Head()
	if err != nil {
		return 0, err
	}
	return head.Version, nil
}

func refsText(refs []model.EntityRef) string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		out = append(out, ref.Entity+":"+ref.ID)
	}
	if len(out) == 0 {
		return "-"
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func listText(list []string) string {
	if len(list) == 0 {
		return "-"
	}
	return strings.Join(list, ",")
}
