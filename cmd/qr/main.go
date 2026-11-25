package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"queryrewrite/internal/engine"
	"queryrewrite/internal/gen"
	"queryrewrite/internal/policy"
	"queryrewrite/internal/server"
	"queryrewrite/internal/spec"
	"queryrewrite/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var err error
	switch cmd {
	case "generate":
		err = cmdGenerate(fs, os.Args[2:])
	case "run":
		err = cmdRun(fs, os.Args[2:])
	case "status":
		err = cmdStatus(fs, os.Args[2:])
	case "report":
		err = cmdReport(fs, os.Args[2:])
	case "patch":
		err = cmdPatch(fs, os.Args[2:])
	case "replace":
		err = cmdReplace(fs, os.Args[2:])
	case "recover":
		err = cmdRecover(fs, os.Args[2:])
	case "serve":
		err = cmdServe(fs, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		emitError(err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `qr - query rewrite & prefix-cache adjudication

usage: qr <command> [flags]
  generate  -out DIR           write deterministic JSONL shards + policy.yaml
  run       -data DIR -db FILE [-turn N] [-crash-after STAGE]
  status    -data DIR -db FILE -session ID -turn N
  report    -data DIR -db FILE -out FILE
  patch     -data DIR -db FILE -field F -value V
  replace   -data DIR -db FILE -policy FILE
  recover   -data DIR -db FILE
  serve     -data DIR -db FILE -addr :8080`)
}

func openEngine(dataDir, dbPath string) (*engine.Engine, *store.Store, spec.Policy, error) {
	filePolicy, err := policy.LoadYAML(filepath.Join(dataDir, "policy.yaml"))
	if err != nil {
		return nil, nil, filePolicy, err
	}
	st, err := store.Open(dbPath, dbPath+".lock")
	if err != nil {
		return nil, nil, filePolicy, err
	}
	// The durable highest-version policy in SQLite is the source of truth. The
	// YAML file is only used to bootstrap an empty database.
	active := filePolicy
	if ver, _, verr := st.CurrentPolicyVersion(); verr == nil && ver > 0 {
		if dbPolicy, lerr := st.LoadPolicy(ver); lerr == nil && dbPolicy != nil {
			active = *dbPolicy
		}
	} else if err := st.SavePolicy(filePolicy, "baseline"); err != nil {
		st.Close()
		return nil, nil, filePolicy, err
	}
	e, err := engine.New(st, active)
	if err != nil {
		st.Close()
		return nil, nil, active, err
	}
	return e, st, active, nil
}

func openEngineCrash(dataDir, dbPath, crashAfter string) (*engine.Engine, *store.Store, spec.Policy, error) {
	e, st, p, err := openEngine(dataDir, dbPath)
	if err == nil {
		e.CrashAfter = crashAfter
	}
	return e, st, p, err
}

func cmdGenerate(fs *flag.FlagSet, args []string) error {
	out := fs.String("out", "data", "output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	paths, err := gen.WriteShards(*out, gen.DemoSessions())
	if err != nil {
		return err
	}
	policyPath, err := gen.WritePolicy(*out, gen.DemoPolicy())
	if err != nil {
		return err
	}
	fmt.Println("shards:")
	for _, p := range paths {
		fmt.Println("  " + p)
	}
	fmt.Println("policy: " + policyPath)
	fmt.Println("seed:  ", gen.FixedSeed)
	return nil
}

func loadShards(dataDir string) ([]spec.QueryTurn, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	all := []spec.QueryTurn{}
	for _, ent := range entries {
		if filepath.Ext(ent.Name()) != ".jsonl" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dataDir, ent.Name()))
		if err != nil {
			return nil, err
		}
		lines := splitLines(string(b))
		for _, line := range lines {
			if line == "" {
				continue
			}
			var q spec.QueryTurn
			if err := json.Unmarshal([]byte(line), &q); err != nil {
				return nil, err
			}
			all = append(all, q)
		}
	}
	return all, nil
}

func cmdRun(fs *flag.FlagSet, args []string) error {
	dataDir := fs.String("data", "data", "data directory")
	dbPath := fs.String("db", "data/qr.db", "sqlite path")
	onlyTurn := fs.Int("turn", 0, "only run this turn number (0 = all)")
	crashAfter := fs.String("crash-after", "", "simulate SIGKILL after this stage (then exit non-zero)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, st, _, err := openEngineCrash(*dataDir, *dbPath, *crashAfter)
	if err != nil {
		return err
	}
	defer st.Close()

	turns, err := loadShards(*dataDir)
	if err != nil {
		return err
	}
	for _, q := range turns {
		if *onlyTurn != 0 && q.Turn != *onlyTurn {
			continue
		}
		out, err := e.RunTurn(q)
		if err != nil {
			if berr, ok := err.(*spec.BizError); ok && berr.Code == spec.CodeStaleCheckpoint {
				fmt.Println("skip stale publish:", berr.Error())
				continue
			}
			if berr, ok := err.(*spec.BizError); ok {
				// Stable business errors are recorded as facts; continue the batch.
				fmt.Println("business error recorded:", berr.Error())
				continue
			}
			return err
		}
		printTurn(out)
	}
	return nil
}

func cmdStatus(fs *flag.FlagSet, args []string) error {
	dataDir := fs.String("data", "data", "")
	dbPath := fs.String("db", "data/qr.db", "")
	sessionID := fs.String("session", "", "session id")
	turn := fs.Int("turn", 1, "turn number")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, st, _, err := openEngine(*dataDir, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	status, err := e.StageStatus(*sessionID, *turn)
	if err != nil {
		return err
	}
	ver, dig := e.VersionFingerprint()
	fmt.Printf("policy v%d %s\n", ver, dig)
	for _, s := range spec.AllStages() {
		fmt.Printf("  %s: %s\n", s, status[s])
	}
	return nil
}

func cmdReport(fs *flag.FlagSet, args []string) error {
	dataDir := fs.String("data", "data", "")
	dbPath := fs.String("db", "data/qr.db", "")
	out := fs.String("out", "", "report output path (default stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, st, _, err := openEngine(*dataDir, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	rep, err := e.BuildReport()
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if *out == "" {
		fmt.Println(string(b))
	} else {
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			return err
		}
		fmt.Println("report written to " + *out)
	}
	fmt.Println("summary: " + rep.Summary)
	return nil
}

func cmdPatch(fs *flag.FlagSet, args []string) error {
	dataDir := fs.String("data", "data", "")
	dbPath := fs.String("db", "data/qr.db", "")
	field := fs.String("field", "", "candidate_depth|cache_capacity|min_prefix_runes|threshold|per_mille")
	value := fs.String("value", "", "new value (per_mille: history=700,synonym=300)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, st, p, err := openEngine(*dataDir, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	patch := spec.PolicyPatch{Version: p.Version + 1}
	switch *field {
	case "candidate_depth":
		v := atoiOrDie(*value)
		patch.CandidateDepth = &v
	case "cache_capacity":
		v := atoiOrDie(*value)
		patch.CacheCapacity = &v
	case "min_prefix_runes":
		v := atoiOrDie(*value)
		patch.MinPrefixRunes = &v
	case "threshold":
		v := atofOrDie(*value)
		patch.HitGainThreshold = &v
	case "per_mille":
		patch.StrategyPerMille = parseMille(*value)
	default:
		return fmt.Errorf("unknown field %q", *field)
	}
	next, earliest, err := policy.ApplyPatch(p, patch)
	if err != nil {
		return err
	}
	if err := e.SetPolicy(next, "incremental"); err != nil {
		return err
	}
	if _, err := gen.WritePolicy(*dataDir, next); err != nil {
		return err
	}
	fmt.Printf("incremental v%d applied; earliest affected stage: %s\n", next.Version, earliest)
	return nil
}

func cmdReplace(fs *flag.FlagSet, args []string) error {
	dataDir := fs.String("data", "data", "")
	dbPath := fs.String("db", "data/qr.db", "")
	policyFile := fs.String("policy", "", "replacement policy YAML")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, st, cur, err := openEngine(*dataDir, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	incoming, err := policy.LoadYAML(*policyFile)
	if err != nil {
		return err
	}
	incoming.ContentDigest = policy.Digest(incoming)
	kind, berr := policy.Classify(cur, incoming)
	if berr != nil {
		return berr
	}
	if kind != "replacement" {
		return fmt.Errorf("snapshot classified as %q, not replacement", kind)
	}
	if err := e.SetPolicy(incoming, "replacement"); err != nil {
		return err
	}
	fmt.Printf("replacement v%d applied; all slots and adjudication caches invalidated\n", incoming.Version)
	return nil
}

func cmdRecover(fs *flag.FlagSet, args []string) error {
	dataDir := fs.String("data", "data", "")
	dbPath := fs.String("db", "data/qr.db", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e, st, _, err := openEngine(*dataDir, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	turns, err := loadShards(*dataDir)
	if err != nil {
		return err
	}
	for _, q := range turns {
		if _, err := e.RunTurn(q); err != nil {
			if _, ok := err.(*spec.BizError); ok {
				// Both stale publishes (already committed) and recorded business
				// errors are expected during recovery.
				continue
			}
			return err
		}
	}
	rep, err := e.BuildReport()
	if err != nil {
		return err
	}
	fmt.Println("recovery complete; summary: " + rep.Summary)
	return nil
}

func cmdServe(fs *flag.FlagSet, args []string) error {
	dataDir := fs.String("data", "data", "")
	dbPath := fs.String("db", "data/qr.db", "")
	addr := fs.String("addr", ":8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return server.Run(*addr, *dataDir, *dbPath)
}

func printTurn(out spec.TurnOutput) {
	fmt.Printf("turn %s#%d policy=v%d candidates=%d accepted=%v\n",
		out.SessionID, out.Turn, out.PolicyVer, len(out.Candidates), out.Ledger.Accepted)
	if out.StageError != nil {
		b, _ := json.Marshal(out.StageError)
		fmt.Println("  stage_error: " + string(b))
	}
}

func emitError(err error) {
	if berr, ok := err.(*spec.BizError); ok {
		b, _ := json.Marshal(berr)
		fmt.Fprintln(os.Stderr, string(b))
		return
	}
	fmt.Fprintln(os.Stderr, "error:", err.Error())
}

func splitLines(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == '\n' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func atoiOrDie(s string) int {
	n := 0
	neg := false
	for i, r := range s {
		if i == 0 && r == '-' {
			neg = true
			continue
		}
		if r < '0' || r > '9' {
			panic("bad int: " + s)
		}
		n = n*10 + int(r-'0')
	}
	if neg {
		n = -n
	}
	return n
}

func atofOrDie(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%g", &f)
	return f
}

func parseMille(s string) map[string]int {
	out := map[string]int{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			kv(cur, out)
			cur = ""
			continue
		}
		cur += string(r)
	}
	kv(cur, out)
	return out
}

func kv(pair string, out map[string]int) {
	key, val := "", ""
	seen := false
	for _, r := range pair {
		if r == '=' {
			seen = true
			continue
		}
		if seen {
			val += string(r)
		} else {
			key += string(r)
		}
	}
	out[key] = atoiOrDie(val)
}
