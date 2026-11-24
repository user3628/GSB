// Command privesc is the local privilege-escalation diagnostic workbench.
// All subcommands operate on deterministic local files and a SQLite store.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"privescworkbench/internal/detector"
	"privescworkbench/internal/domain"
	"privescworkbench/internal/inject"
	"privescworkbench/internal/report"
	"privescworkbench/internal/sample"
	"privescworkbench/internal/scoring"
	"privescworkbench/internal/session"
	"privescworkbench/internal/snapshot"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = cmdGenerate(os.Args[2:])
	case "detect":
		err = cmdDetect(os.Args[2:])
	case "trace":
		err = cmdTrace(os.Args[2:])
	case "replay":
		err = cmdReplay(os.Args[2:])
	case "ledger":
		err = cmdLedger(os.Args[2:])
	case "report":
		err = cmdReport(os.Args[2:])
	case "inject":
		err = cmdInject(os.Args[2:])
	case "version":
		fmt.Println("privesc workbench 1.0.0")
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		emitErr(err)
		if ce, ok := err.(*domain.CodeError); ok {
			_ = ce
			os.Exit(1)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: privesc <subcommand> [flags]
subcommands:
  generate --dir DIR --scenario S [--seed N]
  detect   --dir DIR [--db PATH] [--threshold-micro N] [--no-snapshot]
  trace    --dir DIR --pid PID
  replay   --db PATH --version N
  ledger   --db PATH --version N
  report   --dir DIR [--db PATH] [--out PATH]
  inject   --db PATH
  version`)
}

type flags struct {
	dir, scenario, db, out string
	pid, version, seed     int64
	thresholdMicro         int64
	noSnapshot             bool
	normalize              bool
}

func parseFlags(args []string) flags {
	f := flags{scenario: string(sample.ScenarioBaseline), seed: 1,
		thresholdMicro: scoring.ThresholdMicro, pid: -1, version: -1}
	for i := 0; i < len(args); i++ {
		a := args[i]
		next := func() string {
			i++
			if i >= len(args) {
				fail("missing value for " + a)
			}
			return args[i]
		}
		switch a {
		case "--dir":
			f.dir = next()
		case "--scenario":
			f.scenario = next()
		case "--db":
			f.db = next()
		case "--out":
			f.out = next()
		case "--pid":
			f.pid = parseInt(next())
		case "--version":
			f.version = parseInt(next())
		case "--seed":
			f.seed = parseInt(next())
		case "--threshold-micro":
			f.thresholdMicro = parseInt(next())
		case "--no-snapshot":
			f.noSnapshot = true
		case "--normalize":
			f.normalize = true
		default:
			fail("unknown flag: " + a)
		}
	}
	return f
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "argument error: "+msg)
	os.Exit(2)
}

func parseInt(s string) int64 {
	var v int64
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		fail("not an integer: " + s)
	}
	return v
}

func manifestPath(dir string) string { return filepath.Join(dir, "manifest.jsonl") }
func streamPath(dir string) string   { return filepath.Join(dir, "events.lpf") }
func defaultDB(dir string) string    { return filepath.Join(dir, "snapshots.db") }

func readBundle(dir, version string) (*domain.Bundle, error) {
	mb, err := os.ReadFile(manifestPath(dir))
	if err != nil {
		return nil, err
	}
	sb, err := os.ReadFile(streamPath(dir))
	if err != nil {
		return nil, err
	}
	return sample.ParseBundle(version, mb, sb)
}

func cmdGenerate(args []string) error {
	f := parseFlags(args)
	if f.dir == "" {
		fail("--dir is required")
	}
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	gen := sample.Generate(f.seed, sample.Scenario(f.scenario))
	if err := os.WriteFile(manifestPath(f.dir), gen.ManifestBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(streamPath(f.dir), gen.StreamBytes, 0o644); err != nil {
		return err
	}
	fmt.Printf("generated scenario=%s procs=%d events=%d dir=%s\n",
		f.scenario, len(gen.Bundle.Processes), len(gen.Bundle.Events), f.dir)
	return nil
}

func openStore(dbPath string) (*snapshot.Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, err
	}
	return snapshot.Open(dbPath)
}

func cmdDetect(args []string) error {
	f := parseFlags(args)
	if f.dir == "" {
		fail("--dir is required")
	}
	if f.db == "" {
		f.db = defaultDB(f.dir)
	}
	b, err := readBundle(f.dir, "sample@"+filepath.Base(f.dir))
	if err != nil {
		return err
	}
	var store *snapshot.Store
	if !f.noSnapshot {
		store, err = openStore(f.db)
		if err != nil {
			return err
		}
		defer store.Close()
	}
	mgr := session.NewManager(session.DefaultLimits())
	res, st, err := mgr.Submit("cli-"+filepath.Base(f.dir), b, store, f.thresholdMicro)
	if err != nil {
		return err
	}
	out := map[string]any{
		"session":          st.ID,
		"status":           st.Status,
		"code":             st.Code,
		"snapshot_version": res.Version,
		"replacement":      res.Replaced,
		"sampled_at":       domain.FixedSampledAt,
		"elapsed_micros":   res.Round.ElapsedMicros,
		"orphans":          res.Round.Orphans,
		"conclusions":      res.Round.Conclusions,
	}
	return printJSON(out)
}

func cmdTrace(args []string) error {
	f := parseFlags(args)
	if f.dir == "" || f.pid < 0 {
		fail("--dir and --pid are required")
	}
	b, err := readBundle(f.dir, "trace")
	if err != nil {
		return err
	}
	res, err := detector.Run(b, nil, detector.Options{SessionID: "trace"})
	if err != nil {
		return err
	}
	for _, c := range res.Round.Conclusions {
		if c.PID == f.pid {
			return printJSON(map[string]any{
				"pid": c.PID, "code": c.Code, "trace": c.Trace,
				"sampled_at": domain.FixedSampledAt,
			})
		}
	}
	return domain.NewError(domain.CodeSampleParse, fmt.Sprintf("pid %d not found", f.pid))
}

func cmdReplay(args []string) error {
	f := parseFlags(args)
	if f.db == "" || f.version < 0 {
		fail("--db and --version are required")
	}
	store, err := openStore(f.db)
	if err != nil {
		return err
	}
	defer store.Close()
	last, verr := store.VerifyChain()
	if verr != nil {
		return verr
	}
	if f.version > last {
		return domain.NewError(domain.CodeSnapshotVersion,
			fmt.Sprintf("requested version %d beyond contiguous head %d", f.version, last))
	}
	round, ledger, err := store.LoadVersion(f.version)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"round": round, "ledger_entries": ledger})
}

func cmdLedger(args []string) error {
	f := parseFlags(args)
	if f.db == "" || f.version < 0 {
		fail("--db and --version are required")
	}
	store, err := openStore(f.db)
	if err != nil {
		return err
	}
	defer store.Close()
	_, ledger, err := store.LoadVersion(f.version)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"version": f.version, "ledger": ledger})
}

func cmdReport(args []string) error {
	f := parseFlags(args)
	if f.dir == "" {
		fail("--dir is required")
	}
	if f.db == "" {
		f.db = defaultDB(f.dir)
	}
	b, err := readBundle(f.dir, "sample@"+filepath.Base(f.dir))
	if err != nil {
		return err
	}
	store, err := openStore(f.db)
	if err != nil {
		return err
	}
	defer store.Close()
	res, _, err := session.NewManager(session.DefaultLimits()).
		Submit("report-"+filepath.Base(f.dir), b, store, f.thresholdMicro)
	if err != nil {
		return err
	}
	rep := report.FromRound(res.Round, res.Version, f.normalize)
	raw, err := rep.JSON()
	if err != nil {
		return err
	}
	if f.out != "" {
		if err := os.WriteFile(f.out, raw, 0o644); err != nil {
			return err
		}
		fmt.Printf("report written to %s\n", f.out)
		return nil
	}
	fmt.Print(string(raw))
	return nil
}

func cmdInject(args []string) error {
	f := parseFlags(args)
	if f.db == "" {
		fail("--db is required")
	}
	store, err := openStore(f.db)
	if err != nil {
		return err
	}
	defer store.Close()
	results, err := inject.NewRunner(store).RunAll()
	if err != nil {
		return err
	}
	return printJSON(map[string]any{
		"sampled_at": domain.FixedSampledAt,
		"results":    results,
	})
}

func printJSON(v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	_, err = os.Stdout.Write(raw)
	return err
}

func emitErr(err error) {
	code := domain.CodeInternal
	if ce, ok := err.(*domain.CodeError); ok {
		code = ce.Code
	}
	fmt.Fprintf(os.Stderr, `{"code":"%s","message":%q}`+"\n", code, err.Error())
}
