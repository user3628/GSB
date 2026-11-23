package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"example.com/ntfsacl/internal/acl"
	"example.com/ntfsacl/internal/audit"
	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/fixture"
	"example.com/ntfsacl/internal/report"
	"example.com/ntfsacl/internal/safepath"
	"example.com/ntfsacl/internal/store"
	"example.com/ntfsacl/internal/writeback"
)

const errorSchema = "ntfsacl/error/v1"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	ctx := context.Background()
	var err error
	var code int
	switch os.Args[1] {
	case "init":
		code, err = cmdInit(ctx, os.Args[2:])
	case "audit":
		code, err = cmdAudit(ctx, os.Args[2:])
	case "replay":
		code, err = cmdReplay(ctx, os.Args[2:])
	case "conflicts":
		code, err = cmdConflicts(ctx, os.Args[2:])
	case "writeback":
		code, err = cmdWriteback(ctx, os.Args[2:])
	case "recover":
		code, err = cmdRecover(ctx, os.Args[2:])
	case "report":
		code, err = cmdReport(ctx, os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
		code = 1
	}
	if err != nil {
		writeError(err)
		os.Exit(code)
	}
	os.Exit(code)
}

func usage() {
	fmt.Fprintln(os.Stderr, `ntfsacl - deterministic NTFS ACL inheritance audit and controlled writeback

usage:
  ntfsacl init      --workdir DIR [--reset] [--malformed flags|empty|mask]
  ntfsacl audit     --workdir DIR [--subtree REL]... [--concurrency N] [--node-budget N] [--output FILE]
  ntfsacl replay    --workdir DIR --version N [--output FILE]
  ntfsacl conflicts --workdir DIR [--version N] [--output FILE]
  ntfsacl writeback --workdir DIR [--audit-version N] [--fail-node REL] [--crash-at N] [--crash-point after-pending|after-write|after-commit] [--output FILE]
  ntfsacl recover   --workdir DIR [--output FILE]
  ntfsacl report    --workdir DIR --before N --after M [--output FILE]

All output is deterministic JSON except live lock wait durations in writeback.
Paths supplied to the CLI must be relative to the workspace.`)
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func commonFS(workdir string) (*fixture.Store, *store.DB, error) {
	fs, err := fixture.Open(workdir)
	if err != nil {
		return nil, nil, err
	}
	db, err := store.Open(fs.DBPath())
	if err != nil {
		return nil, nil, err
	}
	return fs, db, nil
}

func cmdInit(_ context.Context, args []string) (int, error) {
	fl := flag.NewFlagSet("init", flag.ContinueOnError)
	fl.SetOutput(os.Stderr)
	workdir := fl.String("workdir", "", "workspace root")
	reset := fl.Bool("reset", false, "remove generated tree and state before creating the fixture")
	malformed := fl.String("malformed", "", "inject malformed ACL: flags, empty or mask")
	if err := fl.Parse(args); err != nil {
		return 1, err
	}
	if *workdir == "" {
		return 1, errors.New("--workdir is required")
	}
	root, err := safepath.Root(*workdir)
	if err != nil {
		return 1, err
	}
	if *reset {
		for _, rel := range []string{fixture.TreeDir, fixture.StateDir} {
			target, err := safepath.Join(root, rel)
			if err != nil {
				return 1, err
			}
			if err := os.RemoveAll(target); err != nil {
				return 1, err
			}
		}
	}
	if *malformed != "" && *malformed != "flags" && *malformed != "empty" && *malformed != "mask" {
		return 1, errors.New("--malformed must be flags, empty or mask")
	}
	if _, err := fixture.Generate(root, fixture.Options{Malformed: *malformed}); err != nil {
		return 1, err
	}
	out := struct {
		Schema    string   `json:"schema"`
		FixtureID string   `json:"fixture_id"`
		Tree      string   `json:"tree"`
		State     string   `json:"state"`
		KnownSIDs []string `json:"known_sids"`
	}{
		Schema: "ntfsacl/init/v1", FixtureID: fixture.FixtureID,
		Tree: fixture.TreeDir, State: fixture.StateDir,
		KnownSIDs: []string{fixture.SIDIntern, fixture.SIDFinance, fixture.SIDDocs},
	}
	return 0, emitJSON(root, "", out)
}

func cmdAudit(ctx context.Context, args []string) (int, error) {
	fl := flag.NewFlagSet("audit", flag.ContinueOnError)
	fl.SetOutput(os.Stderr)
	workdir := fl.String("workdir", "", "workspace root")
	concurrency := fl.Int("concurrency", 2, "maximum concurrent subtree sessions")
	budget := fl.Int("node-budget", 0, "per-session node limit; 0 is unlimited")
	output := fl.String("output", "", "relative output path, or - for stdout")
	var subtrees stringList
	fl.Var(&subtrees, "subtree", "relative subtree to audit (repeatable)")
	if err := fl.Parse(args); err != nil {
		return 1, err
	}
	if *workdir == "" {
		return 1, errors.New("--workdir is required")
	}
	root, err := safepath.Root(*workdir)
	if err != nil {
		return 1, err
	}
	if err := validateOutputPath(root, *output); err != nil {
		return 1, err
	}
	requests := make([]audit.Request, 0, len(subtrees))
	for i, s := range subtrees {
		requests = append(requests, audit.Request{ID: fmt.Sprintf("audit-%d", i+1), Subtree: s})
	}
	result, err := audit.Run(ctx, root, requests, audit.Config{Concurrency: *concurrency, NodeBudget: *budget})
	if err != nil {
		if me, ok := audit.IsSessionError(err); ok {
			return 2, emitError(root, *output, me)
		}
		return 1, err
	}
	fs, db, err := commonFS(root)
	if err != nil {
		return 1, err
	}
	defer db.Close()
	_ = fs
	snap, err := db.CommitSnapshot(ctx, store.KindAudit, func(version, parentVersion int64, parentHash string) ([]byte, error) {
		rep, err := result.Report(version, parentVersion, parentHash)
		if err != nil {
			return nil, err
		}
		return rep.Marshal()
	})
	if err != nil {
		return 1, err
	}
	return 0, emitBytes(root, *output, snap.Payload)
}

func cmdReplay(ctx context.Context, args []string) (int, error) {
	fl := flag.NewFlagSet("replay", flag.ContinueOnError)
	fl.SetOutput(os.Stderr)
	workdir := fl.String("workdir", "", "workspace root")
	version := fl.Int64("version", 0, "snapshot version")
	output := fl.String("output", "", "relative output path, or - for stdout")
	if err := fl.Parse(args); err != nil {
		return 1, err
	}
	if *workdir == "" || *version <= 0 {
		return 1, errors.New("--workdir and --version are required")
	}
	root, err := safepath.Root(*workdir)
	if err != nil {
		return 1, err
	}
	if err := validateOutputPath(root, *output); err != nil {
		return 1, err
	}
	_, db, err := commonFS(root)
	if err != nil {
		return 1, err
	}
	defer db.Close()
	snap, err := db.Replay(ctx, *version)
	if err != nil {
		var sv *store.SnapshotVersionError
		if !errors.As(err, &sv) || len(sv.FallbackBytes) == 0 {
			return 3, err
		}
		if emitErr := emitBytes(root, *output, sv.FallbackBytes); emitErr != nil {
			return 1, emitErr
		}
		fmt.Fprintln(os.Stderr, mustJSON(map[string]any{"code": sv.Code, "requested_version": sv.Requested, "fallback_version": sv.Fallback, "detail": sv.Detail}))
		return 3, nil
	}
	if err := emitBytes(root, *output, snap.Payload); err != nil {
		return 1, err
	}
	return 0, nil
}

type conflictReport struct {
	Schema          string                   `json:"schema"`
	SnapshotVersion int64                    `json:"snapshot_version"`
	Code            string                   `json:"code,omitempty"`
	SnapshotChain   []store.SnapshotInfo     `json:"snapshot_chain"`
	Summary         []domain.ConflictSummary `json:"summary"`
	Conflicts       []domain.Conflict        `json:"conflicts"`
}

func cmdConflicts(ctx context.Context, args []string) (int, error) {
	fl := flag.NewFlagSet("conflicts", flag.ContinueOnError)
	fl.SetOutput(os.Stderr)
	workdir := fl.String("workdir", "", "workspace root")
	version := fl.Int64("version", 0, "snapshot version; defaults to latest")
	output := fl.String("output", "", "relative output path, or - for stdout")
	if err := fl.Parse(args); err != nil {
		return 1, err
	}
	if *workdir == "" {
		return 1, errors.New("--workdir is required")
	}
	root, err := safepath.Root(*workdir)
	if err != nil {
		return 1, err
	}
	if err := validateOutputPath(root, *output); err != nil {
		return 1, err
	}
	_, db, err := commonFS(root)
	if err != nil {
		return 1, err
	}
	defer db.Close()
	var snap store.Snapshot
	code := ""
	if *version <= 0 {
		snap, err = db.Latest(ctx, "")
		if err != nil {
			return 1, err
		}
	} else {
		snap, err = db.Replay(ctx, *version)
		if err != nil {
			var sv *store.SnapshotVersionError
			if errors.As(err, &sv) && len(sv.FallbackBytes) > 0 {
				snap = store.Snapshot{Version: sv.Fallback, Payload: sv.FallbackBytes}
				code = sv.Code
			} else {
				return 3, err
			}
		}
	}
	var rep report.Report
	if err := json.Unmarshal(snap.Payload, &rep); err != nil {
		return 1, fmt.Errorf("snapshot is not an audit report: %w", err)
	}
	chain, chainErr := db.ListSnapshotInfo(ctx)
	if chainErr != nil && code == "" {
		code = store.CodeSnapshotVersion
	}
	out := conflictReport{Schema: "ntfsacl/conflicts/v1", SnapshotVersion: rep.Snapshot.Version, Code: code, SnapshotChain: chain, Summary: rep.Summary.Conflicts, Conflicts: rep.Conflicts}
	if out.Summary == nil {
		out.Summary = []domain.ConflictSummary{}
	}
	if out.Conflicts == nil {
		out.Conflicts = []domain.Conflict{}
	}
	if out.SnapshotChain == nil {
		out.SnapshotChain = []store.SnapshotInfo{}
	}
	return 0, emitJSON(root, *output, out)
}

func cmdWriteback(ctx context.Context, args []string) (int, error) {
	fl := flag.NewFlagSet("writeback", flag.ContinueOnError)
	fl.SetOutput(os.Stderr)
	workdir := fl.String("workdir", "", "workspace root")
	auditVersion := fl.Int64("audit-version", 0, "audit snapshot version; defaults to latest")
	failNode := fl.String("fail-node", "", "inject a write failure at this relative node after earlier nodes were applied")
	crashAt := fl.Int("crash-at", 0, "force termination after this 1-based plan node")
	crashPoint := fl.String("crash-point", "after-commit", "after-pending, after-write or after-commit")
	owner := fl.String("owner", "cli-writeback", "writeback session owner")
	concurrency := fl.Int("concurrency", 2, "post-writeback audit concurrency")
	budget := fl.Int("node-budget", 0, "post-writeback audit node budget")
	output := fl.String("output", "", "relative output path, or - for stdout")
	var subtrees stringList
	fl.Var(&subtrees, "subtree", "relative subtree (repeatable)")
	if err := fl.Parse(args); err != nil {
		return 1, err
	}
	if *workdir == "" {
		return 1, errors.New("--workdir is required")
	}
	if *crashPoint != "after-pending" && *crashPoint != "after-write" && *crashPoint != "after-commit" {
		return 1, errors.New("invalid --crash-point")
	}
	root, err := safepath.Root(*workdir)
	if err != nil {
		return 1, err
	}
	if err := validateOutputPath(root, *output); err != nil {
		return 1, err
	}
	_, db, err := commonFS(root)
	if err != nil {
		return 1, err
	}
	defer db.Close()
	var snap store.Snapshot
	if *auditVersion > 0 {
		snap, err = db.Replay(ctx, *auditVersion)
		if err != nil {
			return 3, err
		}
	} else {
		snap, err = db.Latest(ctx, "")
		if err != nil {
			return 1, errors.New("no audit snapshot available; run audit first")
		}
	}
	audited, err := decodeAuditResult(snap.Payload)
	if err != nil {
		return 1, err
	}
	requests := make([]audit.Request, 0, len(subtrees))
	for i, s := range subtrees {
		requests = append(requests, audit.Request{ID: fmt.Sprintf("audit-%d", i+1), Subtree: s})
	}
	res, runErr := writeback.Run(ctx, root, audited, writeback.Options{
		AuditVersion: auditedVersion(snap.Payload),
		Owner:        *owner, Subtree: func() string {
			if len(subtrees) == 0 {
				return "."
			}
			return strings.Join(subtrees, ",")
		}(),
		FailNode: *failNode, CrashAt: *crashAt, CrashPoint: *crashPoint,
		LockTimeout: 10 * time.Second, AuditConfig: audit.Config{Concurrency: *concurrency, NodeBudget: *budget}, Requests: requests,
	})
	if marshalErr := emitJSON(root, *output, &res); marshalErr != nil {
		return 1, marshalErr
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, mustJSON(map[string]any{"code": res.Code, "detail": runErr.Error(), "session_id": res.SessionID}))
		return 1, nil
	}
	return 0, nil
}

func auditedVersion(payload []byte) int64 {
	var rep report.Report
	if err := json.Unmarshal(payload, &rep); err == nil {
		return rep.Snapshot.Version
	}
	return 0
}

func decodeAuditResult(payload []byte) (audit.Result, error) {
	var rep report.Report
	if err := json.Unmarshal(payload, &rep); err != nil {
		return audit.Result{}, err
	}
	if rep.Schema != report.Schema {
		return audit.Result{}, fmt.Errorf("snapshot is not an audit report")
	}
	return audit.Result{
		FixtureID: rep.FixtureID, Sessions: rep.Sessions, Nodes: rep.Nodes,
		Conflicts: rep.Conflicts, Summary: rep.Summary.Conflicts, TreeHash: rep.TreeHash,
	}, nil
}

func cmdRecover(ctx context.Context, args []string) (int, error) {
	fl := flag.NewFlagSet("recover", flag.ContinueOnError)
	fl.SetOutput(os.Stderr)
	workdir := fl.String("workdir", "", "workspace root")
	output := fl.String("output", "", "relative output path, or - for stdout")
	if err := fl.Parse(args); err != nil {
		return 1, err
	}
	if *workdir == "" {
		return 1, errors.New("--workdir is required")
	}
	root, err := safepath.Root(*workdir)
	if err != nil {
		return 1, err
	}
	if err := validateOutputPath(root, *output); err != nil {
		return 1, err
	}
	res, err := writeback.Recover(ctx, root)
	if err != nil {
		return 1, err
	}
	if err := emitJSON(root, *output, &res); err != nil {
		return 1, err
	}
	if res.Code == writeback.CodeRecoveryIncomplete {
		return 4, nil
	}
	return 0, nil
}

type NodeComparison struct {
	RelPath          string `json:"rel_path"`
	Status           string `json:"status"`
	BeforeACL        string `json:"before_acl_hash,omitempty"`
	AfterACL         string `json:"after_acl_hash,omitempty"`
	DecisionsChanged bool   `json:"decisions_changed"`
}

type Comparison struct {
	Schema                string                   `json:"schema"`
	Code                  string                   `json:"code,omitempty"`
	BusinessCodes         []string                 `json:"business_codes"`
	SnapshotChain         []store.SnapshotInfo     `json:"snapshot_chain"`
	Before                report.SnapshotRef       `json:"before"`
	After                 report.SnapshotRef       `json:"after"`
	ChangeKind            string                   `json:"change_kind"`
	Nodes                 []NodeComparison         `json:"nodes"`
	BeforeNodes           []domain.NodeReport      `json:"before_nodes"`
	AfterNodes            []domain.NodeReport      `json:"after_nodes"`
	BeforeConflicts       []domain.Conflict        `json:"before_conflicts"`
	AfterConflicts        []domain.Conflict        `json:"after_conflicts"`
	BeforeConflictSummary []domain.ConflictSummary `json:"before_conflict_summary"`
	AfterConflictSummary  []domain.ConflictSummary `json:"after_conflict_summary"`
}

func cmdReport(ctx context.Context, args []string) (int, error) {
	fl := flag.NewFlagSet("report", flag.ContinueOnError)
	fl.SetOutput(os.Stderr)
	workdir := fl.String("workdir", "", "workspace root")
	before := fl.Int64("before", 0, "before snapshot version")
	after := fl.Int64("after", 0, "after snapshot version")
	output := fl.String("output", "", "relative output path, or - for stdout")
	if err := fl.Parse(args); err != nil {
		return 1, err
	}
	if *workdir == "" || *before <= 0 || *after <= 0 {
		return 1, errors.New("--workdir, --before and --after are required")
	}
	root, err := safepath.Root(*workdir)
	if err != nil {
		return 1, err
	}
	if err := validateOutputPath(root, *output); err != nil {
		return 1, err
	}
	_, db, err := commonFS(root)
	if err != nil {
		return 1, err
	}
	defer db.Close()
	bSnap, bErr := db.Replay(ctx, *before)
	aSnap, aErr := db.Replay(ctx, *after)
	code := ""
	if bErr != nil || aErr != nil {
		code = store.CodeSnapshotVersion
	}
	var br, ar report.Report
	if err := json.Unmarshal(bSnap.Payload, &br); err != nil {
		return 1, fmt.Errorf("before snapshot: %w", err)
	}
	if err := json.Unmarshal(aSnap.Payload, &ar); err != nil {
		return 1, fmt.Errorf("after snapshot: %w", err)
	}
	chain, _ := db.ListSnapshotInfo(ctx)
	changeKind := "incremental"
	for _, info := range chain {
		if info.Version == ar.Snapshot.Version {
			changeKind = info.ChangeKind
		}
	}
	cmp := Comparison{
		Schema: "ntfsacl/comparison/v1", Code: code, SnapshotChain: chain,
		Before: br.Snapshot, After: ar.Snapshot, ChangeKind: changeKind,
		BeforeNodes: br.Nodes, AfterNodes: ar.Nodes,
		BeforeConflicts: br.Conflicts, AfterConflicts: ar.Conflicts,
		BeforeConflictSummary: br.Summary.Conflicts, AfterConflictSummary: ar.Summary.Conflicts,
	}
	if cmp.BeforeNodes == nil {
		cmp.BeforeNodes = []domain.NodeReport{}
	}
	if cmp.AfterNodes == nil {
		cmp.AfterNodes = []domain.NodeReport{}
	}
	if cmp.BeforeConflicts == nil {
		cmp.BeforeConflicts = []domain.Conflict{}
	}
	if cmp.AfterConflicts == nil {
		cmp.AfterConflicts = []domain.Conflict{}
	}
	if cmp.BeforeConflictSummary == nil {
		cmp.BeforeConflictSummary = []domain.ConflictSummary{}
	}
	if cmp.AfterConflictSummary == nil {
		cmp.AfterConflictSummary = []domain.ConflictSummary{}
	}
	if cmp.SnapshotChain == nil {
		cmp.SnapshotChain = []store.SnapshotInfo{}
	}
	codes := map[string]bool{}
	if code != "" {
		codes[code] = true
	}
	for _, c := range br.Conflicts {
		codes[c.Code] = true
	}
	for _, c := range ar.Conflicts {
		codes[c.Code] = true
	}
	for c := range codes {
		cmp.BusinessCodes = append(cmp.BusinessCodes, c)
	}
	sort.Slice(cmp.BusinessCodes, func(i, j int) bool {
		return businessCodeRank(cmp.BusinessCodes[i]) < businessCodeRank(cmp.BusinessCodes[j]) ||
			(businessCodeRank(cmp.BusinessCodes[i]) == businessCodeRank(cmp.BusinessCodes[j]) && cmp.BusinessCodes[i] < cmp.BusinessCodes[j])
	})
	if cmp.BusinessCodes == nil {
		cmp.BusinessCodes = []string{}
	}
	bmap := make(map[string]domain.NodeReport, len(br.Nodes))
	amap := make(map[string]domain.NodeReport, len(ar.Nodes))
	for _, n := range br.Nodes {
		bmap[n.RelPath] = n
	}
	for _, n := range ar.Nodes {
		amap[n.RelPath] = n
	}
	paths := make(map[string]bool)
	for p := range bmap {
		paths[p] = true
	}
	for p := range amap {
		paths[p] = true
	}
	sorted := make([]string, 0, len(paths))
	for p := range paths {
		sorted = append(sorted, p)
	}
	sort.Strings(sorted)
	for _, p := range sorted {
		b, bok := bmap[p]
		a, aok := amap[p]
		nc := NodeComparison{RelPath: p}
		switch {
		case !bok:
			nc.Status = "ADDED"
		case !aok:
			nc.Status = "REMOVED"
		case b.ACLHash != a.ACLHash:
			nc.Status = "CHANGED"
		default:
			nc.Status = "UNCHANGED"
		}
		nc.BeforeACL = b.ACLHash
		nc.AfterACL = a.ACLHash
		nc.DecisionsChanged = decisionsHash(b.Decisions) != decisionsHash(a.Decisions)
		cmp.Nodes = append(cmp.Nodes, nc)
	}
	if cmp.Nodes == nil {
		cmp.Nodes = []NodeComparison{}
	}
	if err := emitJSON(root, *output, cmp); err != nil {
		return 1, err
	}
	if code != "" {
		return 3, nil
	}
	return 0, nil
}

func businessCodeRank(code string) int {
	switch code {
	case acl.CodeDenyOverride:
		return 0
	case acl.CodeInheritBroken:
		return 1
	case acl.CodeFlagIncompatible:
		return 2
	case acl.CodeOrphanSID:
		return 3
	case store.CodeSnapshotVersion:
		return 4
	default:
		return 5
	}
}

func decisionsHash(d []domain.Decision) string {
	b, _ := domain.CanonicalJSON(d)
	return domain.HashBytes(b)
}

func emitJSON(root, output string, v any) error {
	b, err := domain.CanonicalJSON(v)
	if err != nil {
		return err
	}
	return emitBytes(root, output, b)
}

func validateOutputPath(root, output string) error {
	if output == "" || output == "-" {
		return nil
	}
	rel, err := safepath.CleanRel(output)
	if err != nil {
		return err
	}
	lower := strings.ToLower(rel)
	treeDir := strings.ToLower(fixture.TreeDir)
	stateDir := strings.ToLower(fixture.StateDir)
	if lower == treeDir || strings.HasPrefix(lower, treeDir+"/") ||
		lower == stateDir || strings.HasPrefix(lower, stateDir+"/") {
		return fmt.Errorf("output path %q is reserved for fixture data", output)
	}
	target, err := safepath.Join(root, rel)
	if err != nil {
		return err
	}
	return safepath.EnsureNoSymlink(root, target)
}

func emitBytes(root, output string, b []byte) error {
	if output == "" || output == "-" {
		_, err := os.Stdout.Write(b)
		return err
	}
	if err := validateOutputPath(root, output); err != nil {
		return err
	}
	target, err := safepath.Join(root, output)
	if err != nil {
		return err
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := safepath.EnsureNoSymlink(root, target); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".ntfsacl-report-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}
	if err := f.Chmod(0o644); err != nil {
		cleanup()
		return err
	}
	if _, err := f.Write(b); err != nil {
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func emitError(root, output string, e error) error {
	payload := map[string]any{"schema": errorSchema, "detail": e.Error()}
	var me *acl.MalformedError
	if errors.As(e, &me) {
		payload = map[string]any{"schema": errorSchema, "code": me.Code, "node_path": me.NodePath, "ace_index": me.ACEIndex, "detail": me.Detail}
	}
	var sv *store.SnapshotVersionError
	if errors.As(e, &sv) {
		payload = map[string]any{"schema": errorSchema, "code": sv.Code, "requested_version": sv.Requested, "fallback_version": sv.Fallback, "detail": sv.Detail}
	}
	b, _ := domain.CanonicalJSON(payload)
	return emitBytes(root, output, b)
}

func writeError(err error) {
	payload := map[string]any{"schema": errorSchema, "code": "E_INTERNAL", "detail": err.Error()}
	var me *acl.MalformedError
	if errors.As(err, &me) {
		payload = map[string]any{"schema": errorSchema, "code": me.Code, "node_path": me.NodePath, "ace_index": me.ACEIndex, "detail": me.Detail}
	}
	var sv *store.SnapshotVersionError
	if errors.As(err, &sv) {
		payload = map[string]any{"schema": errorSchema, "code": sv.Code, "requested_version": sv.Requested, "fallback_version": sv.Fallback, "detail": sv.Detail}
	}
	b, _ := domain.CanonicalJSON(payload)
	_, _ = os.Stderr.Write(b)
}

func mustJSON(v any) string {
	b, _ := domain.CanonicalJSON(v)
	return strings.TrimSpace(string(b))
}
