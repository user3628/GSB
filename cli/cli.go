// Package cli implements the sandboxctl command-line workbench.
package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"sandboxpolicy/business"
	"sandboxpolicy/engine"
	"sandboxpolicy/policy"
	"sandboxpolicy/probe"
	"sandboxpolicy/report"
	"sandboxpolicy/snapshot"
)

// Run executes a command and returns a stable process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stdout)
		return 0
	}
	command := args[0]
	rest := args[1:]
	for _, arg := range rest {
		switch arg {
		case "-h", "-help", "--help":
			printUsage(stdout)
			return 0
		}
	}
	switch command {
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	case "init":
		return runInit(ctx, rest, stdout, stderr)
	case "apply":
		return runApply(ctx, rest, stdout, stderr)
	case "current":
		return runCurrent(ctx, rest, stdout, stderr)
	case "chain":
		return runChain(ctx, rest, stdout, stderr)
	case "snapshot":
		return runSnapshot(ctx, rest, stdout, stderr)
	case "diagnose", "replay":
		return runDiagnose(ctx, rest, stdout, stderr)
	case "closure":
		return runClosure(ctx, rest, stdout, stderr)
	case "trace":
		return runTrace(ctx, rest, stdout, stderr)
	case "report":
		return runReport(ctx, rest, stdout, stderr)
	case "probe":
		return runProbe(ctx, rest, stdout, stderr)
	case "events":
		return runEvents(ctx, rest, stdout, stderr)
	case "inject":
		return runInject(ctx, rest, stdout, stderr)
	case "namespaces":
		return runNamespaces(ctx, rest, stdout, stderr)
	case "shell":
		return runShell(ctx, rest, stdout, stderr)
	case "probe-helper":
		return 0
	default:
		return printError(stderr, business.New(business.CodePolicyMalformed, fmt.Sprintf("unknown command %q", command)))
	}
}

func runInit(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("init", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	policyPath := fs.String("policy", "", "baseline policy JSON")
	namespace := fs.String("namespace", "", "optional expected namespace")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	if *dbPath == "" || *policyPath == "" {
		return printError(stderr, business.New(business.CodePolicyMalformed, "init requires --db and --policy"))
	}
	baseline, err := policy.ParsePolicyFile(*policyPath)
	if err != nil {
		return printError(stderr, err)
	}
	if strings.TrimSpace(*namespace) != "" && strings.TrimSpace(*namespace) != baseline.Namespace {
		return printError(stderr, &business.Error{Code: business.CodeNamespaceScope, Entry: "namespace", Message: fmt.Sprintf("requested namespace %q does not match policy %q", *namespace, baseline.Namespace)})
	}
	store, err := snapshot.Open(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	committed, err := store.Initialize(ctx, baseline)
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, committed); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runApply(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("apply", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	changePath := fs.String("changeset", "", "incremental change-set JSON")
	namespace := fs.String("namespace", "", "optional expected namespace")
	maxInflight := fs.Int("max-inflight", 4, "maximum concurrent transactions")
	queueTimeout := fs.Duration("queue-timeout", 2*time.Second, "maximum queue wait before E_CHANGE_CONTENTION")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	if *dbPath == "" || *changePath == "" {
		return printError(stderr, business.New(business.CodePolicyMalformed, "apply requires --db and --changeset"))
	}
	changes, err := policy.ParseChangeSetFile(*changePath)
	if err != nil {
		return printError(stderr, err)
	}
	if strings.TrimSpace(*namespace) != "" && strings.TrimSpace(*namespace) != changes.Namespace {
		return printError(stderr, &business.Error{Code: business.CodeNamespaceScope, Entry: "namespace", Message: fmt.Sprintf("requested namespace %q does not match changeset %q", *namespace, changes.Namespace)})
	}
	store, err := snapshot.Open(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	manager := engine.NewManager(store, engine.Config{MaxInflight: *maxInflight, QueueTimeout: *queueTimeout})
	result, err := manager.Apply(ctx, changes)
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, result); err != nil {
		return printError(stderr, err)
	}
	return 0
}

type versionView struct {
	Namespace     string                 `json:"namespace"`
	Version       int                    `json:"version"`
	ParentVersion int                    `json:"parent_version"`
	Digest        string                 `json:"digest"`
	Entries       []snapshot.ChainEntry  `json:"snapshot_chain"`
	Warning       *snapshot.ChainWarning `json:"chain_warning,omitempty"`
}

func runCurrent(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("current", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	if *dbPath == "" {
		return printError(stderr, business.New(business.CodePolicyMalformed, "current requires --db"))
	}
	store, err := snapshot.Open(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	current, chain, err := store.Current(ctx, namespaceValue)
	if err != nil {
		return printError(stderr, err)
	}
	view := versionView{Namespace: current.Namespace, Version: current.Version, ParentVersion: current.ParentVersion, Digest: current.Digest, Entries: chain.Entries, Warning: chain.Warning}
	if err := printJSON(stdout, view); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runChain(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("chain", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	chain, err := store.LoadChain(ctx, namespaceValue)
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, chain); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runSnapshot(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("snapshot", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	version := fs.Int("version", 0, "snapshot version; 0 means current")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	snap, err := loadSnapshot(ctx, store, namespaceValue, *version)
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, snap); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runDiagnose(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("diagnose", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	version := fs.Int("version", 0, "snapshot version; 0 means current")
	doProbe := fs.Bool("probe", false, "run real file and process probes")
	timeoutMS := fs.Int("timeout-ms", 3000, "process probe timeout in milliseconds")
	outPath := fs.String("out", "", "optional report output path")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	return diagnoseCommand(ctx, *dbPath, *namespace, *version, *doProbe, *timeoutMS, *outPath, stdout, stderr)
}

func runClosure(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("closure", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	version := fs.Int("version", 0, "snapshot version; 0 means current")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	snap, err := loadSnapshot(ctx, store, namespaceValue, *version)
	if err != nil {
		return printError(stderr, err)
	}
	view := struct {
		Namespace     string                `json:"namespace"`
		Version       int                   `json:"version"`
		ChangeClosure policy.Closure        `json:"change_closure"`
		StateIssues   []policy.AffectedRule `json:"state_issues"`
	}{
		Namespace:     snap.Namespace,
		Version:       snap.Version,
		ChangeClosure: snap.Closure,
		StateIssues:   policy.StateIssues(snap.Policy),
	}
	if err := printJSON(stdout, view); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runTrace(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("trace", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	version := fs.Int("version", 0, "snapshot version; 0 means current")
	ruleID := fs.String("rule", "", "rule identifier")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	if *ruleID == "" {
		return printError(stderr, business.New(business.CodePolicyMalformed, "trace requires --rule"))
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	snap, err := loadSnapshot(ctx, store, namespaceValue, *version)
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, policy.TraceRule(snap.Policy, *ruleID)); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runReport(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("report", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	version := fs.Int("version", 0, "snapshot version; 0 means current")
	outPath := fs.String("out", "-", "report file path or - for stdout")
	noProbe := fs.Bool("no-probe", false, "skip real probes")
	timeoutMS := fs.Int("timeout-ms", 3000, "process probe timeout in milliseconds")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	return diagnoseCommand(ctx, *dbPath, *namespace, *version, !*noProbe, *timeoutMS, *outPath, stdout, stderr)
}

func diagnoseCommand(ctx context.Context, dbPath, namespace string, version int, doProbe bool, timeoutMS int, outPath string, stdout, stderr io.Writer) int {
	store, err := openStore(dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, namespace)
	if err != nil {
		return printError(stderr, err)
	}
	result, err := report.Build(ctx, store, namespaceValue, version, report.Options{Probe: doProbe, ProbeOpts: probe.Options{TimeoutMS: timeoutMS}})
	if err != nil {
		return printError(stderr, err)
	}
	if outPath != "" && outPath != "-" {
		if err := report.WriteFile(outPath, result); err != nil {
			return printError(stderr, err)
		}
		summary := struct {
			Written string `json:"written"`
			Version int    `json:"version"`
			Digest  string `json:"digest"`
		}{Written: outPath, Version: result.Version, Digest: result.Digest}
		if err := printJSON(stdout, summary); err != nil {
			return printError(stderr, err)
		}
		return 0
	}
	data, err := report.Marshal(result)
	if err != nil {
		return printError(stderr, err)
	}
	if _, err := stdout.Write(data); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runProbe(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("probe", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	version := fs.Int("version", 0, "snapshot version; 0 means current")
	timeoutMS := fs.Int("timeout-ms", 3000, "process probe timeout in milliseconds")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	snap, err := loadSnapshot(ctx, store, namespaceValue, *version)
	if err != nil {
		return printError(stderr, err)
	}
	probeReport, err := probe.Run(ctx, snap.Policy, snap.Version, probe.Options{TimeoutMS: *timeoutMS})
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, probeReport); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runEvents(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("events", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	limit := fs.Int("limit", 100, "maximum events")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	events, err := store.Events(ctx, namespaceValue, *limit)
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, events); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runNamespaces(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("namespaces", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaces, err := store.Namespaces(ctx)
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, namespaces); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runInject(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return printError(stderr, business.New(business.CodePolicyMalformed, "inject requires version-gap or digest-mismatch"))
	}
	mode := args[0]
	fs := newFlagSet("inject "+mode, stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	version := fs.Int("version", 0, "target version")
	if code := parseFlags(fs, args[1:], stderr); code != 0 {
		return code
	}
	if *dbPath == "" || *version == 0 {
		return printError(stderr, business.New(business.CodePolicyMalformed, "inject requires --db and --version"))
	}
	store, err := snapshot.Open(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	switch mode {
	case "version-gap":
		err = store.InjectVersionGap(ctx, namespaceValue, *version)
	case "digest-mismatch":
		err = store.InjectDigestMismatch(ctx, namespaceValue, *version)
	default:
		err = business.New(business.CodePolicyMalformed, fmt.Sprintf("unknown inject mode %q", mode))
	}
	if err != nil {
		return printError(stderr, err)
	}
	if err := printJSON(stdout, struct {
		Injected  string `json:"injected"`
		Namespace string `json:"namespace"`
		Version   int    `json:"version"`
	}{Injected: mode, Namespace: namespaceValue, Version: *version}); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func runShell(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("shell", stderr)
	dbPath := fs.String("db", "", "SQLite state database")
	namespace := fs.String("namespace", "", "policy namespace")
	if code := parseFlags(fs, args, stderr); code != 0 {
		return code
	}
	store, err := openStore(*dbPath)
	if err != nil {
		return printError(stderr, err)
	}
	defer store.Close()
	namespaceValue, err := store.ResolveNamespace(ctx, *namespace)
	if err != nil {
		return printError(stderr, err)
	}
	fmt.Fprintf(stdout, "sandboxctl shell namespace=%s; type help or quit\n", namespaceValue)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Fprint(stdout, "sandbox> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		switch parts[0] {
		case "quit", "exit":
			return 0
		case "help":
			fmt.Fprintln(stdout, "current | chain | snapshot [v] | closure [v] | trace <rule> [v] | diagnose [v] | probe [v] | report <path|-> [v] | quit")
		case "current":
			if code := shellCurrent(ctx, store, namespaceValue, stdout, stderr); code != 0 {
				continue
			}
		case "chain":
			chain, err := store.LoadChain(ctx, namespaceValue)
			if err != nil {
				printError(stderr, err)
				continue
			}
			_ = printJSON(stdout, chain)
		case "snapshot":
			version := shellVersion(parts, 1)
			snap, err := loadSnapshot(ctx, store, namespaceValue, version)
			if err != nil {
				printError(stderr, err)
				continue
			}
			_ = printJSON(stdout, snap)
		case "closure":
			version := shellVersion(parts, 1)
			snap, err := loadSnapshot(ctx, store, namespaceValue, version)
			if err != nil {
				printError(stderr, err)
				continue
			}
			_ = printJSON(stdout, struct {
				Version int                   `json:"version"`
				Closure policy.Closure        `json:"change_closure"`
				State   []policy.AffectedRule `json:"state_issues"`
			}{snap.Version, snap.Closure, policy.StateIssues(snap.Policy)})
		case "trace":
			if len(parts) < 2 {
				fmt.Fprintln(stderr, "usage: trace <rule> [version]")
				continue
			}
			version := 0
			if len(parts) >= 3 {
				version, _ = strconv.Atoi(parts[2])
			}
			snap, err := loadSnapshot(ctx, store, namespaceValue, version)
			if err != nil {
				printError(stderr, err)
				continue
			}
			_ = printJSON(stdout, policy.TraceRule(snap.Policy, parts[1]))
		case "diagnose", "probe":
			version := shellVersion(parts, 1)
			withProbe := parts[0] == "probe"
			result, err := report.Build(ctx, store, namespaceValue, version, report.Options{Probe: withProbe})
			if err != nil {
				printError(stderr, err)
				continue
			}
			_ = printJSON(stdout, result)
		case "report":
			if len(parts) < 2 {
				fmt.Fprintln(stderr, "usage: report <path|-> [version]")
				continue
			}
			version := 0
			if len(parts) >= 3 {
				version, _ = strconv.Atoi(parts[2])
			}
			result, err := report.Build(ctx, store, namespaceValue, version, report.Options{Probe: true})
			if err != nil {
				printError(stderr, err)
				continue
			}
			if parts[1] == "-" {
				data, _ := report.Marshal(result)
				_, _ = stdout.Write(data)
			} else if err := report.WriteFile(parts[1], result); err != nil {
				printError(stderr, err)
			} else {
				fmt.Fprintf(stdout, "wrote %s\n", parts[1])
			}
		default:
			fmt.Fprintf(stderr, "unknown shell command %q\n", parts[0])
		}
	}
	if err := scanner.Err(); err != nil {
		return printError(stderr, err)
	}
	return 0
}

func shellCurrent(ctx context.Context, store *snapshot.Store, namespace string, stdout, stderr io.Writer) int {
	current, chain, err := store.Current(ctx, namespace)
	if err != nil {
		return printError(stderr, err)
	}
	return ignoreWrite(printJSON(stdout, versionView{Namespace: current.Namespace, Version: current.Version, ParentVersion: current.ParentVersion, Digest: current.Digest, Entries: chain.Entries, Warning: chain.Warning}))
}

func shellVersion(parts []string, index int) int {
	if len(parts) <= index {
		return 0
	}
	version, err := strconv.Atoi(parts[index])
	if err != nil {
		return 0
	}
	return version
}

func ignoreWrite(err error) int {
	if err != nil {
		return 1
	}
	return 0
}

func loadSnapshot(ctx context.Context, store *snapshot.Store, namespace string, version int) (*snapshot.Snapshot, error) {
	if version == 0 {
		current, _, err := store.Current(ctx, namespace)
		return current, err
	}
	snap, _, err := store.SnapshotAt(ctx, namespace, version)
	return snap, err
}

func newFlagSet(name string, _ io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func parseFlags(fs *flag.FlagSet, args []string, stderr io.Writer) int {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return printError(stderr, business.New(business.CodePolicyMalformed, err.Error()))
	}
	if fs.NArg() != 0 {
		return printError(stderr, business.New(business.CodePolicyMalformed, fmt.Sprintf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))))
	}
	return 0
}

func openStore(path string) (*snapshot.Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, business.New(business.CodePolicyMalformed, "--db is required")
	}
	return snapshot.Open(path)
}

func printJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func printError(stderr io.Writer, err error) int {
	be := business.From(err)
	encoder := json.NewEncoder(stderr)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(be)
	return exitCode(be.Code)
}

func exitCode(code business.Code) int {
	switch code {
	case "":
		return 0
	case business.CodePolicyMalformed:
		return 2
	case business.CodeSnapshotVersion:
		return 3
	case business.CodeProbeFailed, business.CodeProbeMismatch:
		return 4
	case business.CodeChangeContention:
		return 5
	case business.CodeChangeConflict:
		return 6
	case business.CodeNamespaceScope:
		return 7
	case business.CodeBaselineMissing:
		return 8
	default:
		return 9
	}
}

func printUsage(writer io.Writer) {
	fmt.Fprint(writer, `sandboxctl - Windows sandbox policy diagnostic workbench

Usage:
  sandboxctl init      --db state.db --policy samples/policies/baseline.json
  sandboxctl apply     --db state.db --changeset samples/changes/01-add-cache.json
  sandboxctl current   --db state.db [--namespace NS]
  sandboxctl chain     --db state.db [--namespace NS]
  sandboxctl snapshot  --db state.db [--namespace NS] [--version N]
  sandboxctl diagnose  --db state.db [--namespace NS] [--version N] [--probe]
  sandboxctl replay    --db state.db [--namespace NS] --version N [--probe]
  sandboxctl closure   --db state.db [--namespace NS] [--version N]
  sandboxctl trace     --db state.db [--namespace NS] [--version N] --rule RULE_ID
  sandboxctl report    --db state.db [--namespace NS] [--version N] [--out -|PATH]
  sandboxctl probe     --db state.db [--namespace NS] [--version N]
  sandboxctl events    --db state.db [--namespace NS] [--limit N]
  sandboxctl namespaces --db state.db
  sandboxctl shell     --db state.db [--namespace NS]

Testing-only anomaly injection:
  sandboxctl inject version-gap      --db state.db --namespace NS --version N
  sandboxctl inject digest-mismatch  --db state.db --namespace NS --version N
`)
}
