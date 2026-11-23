package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"depverify/internal/closure"
	"depverify/internal/edges"
	"depverify/internal/engine"
	"depverify/internal/manifest"
	"depverify/internal/model"
	"depverify/internal/report"
	"depverify/internal/samples"
	"depverify/internal/session"
	"depverify/internal/snapshot"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "depverify:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("subcommand required")
	}
	switch args[0] {
	case "generate":
		return cmdGenerate(args[1:])
	case "verify":
		return cmdVerify(args[1:])
	case "inject":
		return cmdInject(args[1:])
	case "dualpath":
		return cmdDualPath(args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `depverify - dependency closure integrity verification engine

Usage:
  depverify generate [--out DIR]
  depverify verify [--db PATH] [--config FILE] [--threshold N] [--concurrency N] [--budget N] [--inject kind,...] [--out report.json]
  depverify inject --kind KIND [--db PATH] [--out DIR]
  depverify dualpath [--components N] [--threshold N] [--out report.json]
`)
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	outDir := fs.String("out", "samples", "output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	for _, b := range samples.All() {
		base := filepath.Join(*outDir, string(b.Kind))
		if err := os.WriteFile(base+".bin", b.Manifest, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(base+".edges.jsonl", []byte(b.Edges), 0o644); err != nil {
			return err
		}
		meta, _ := json.MarshalIndent(map[string]any{
			"kind": b.Kind, "roots": b.Roots,
			"expect_codes": b.ExpectCodes, "description": b.Description,
		}, "", "  ")
		if err := os.WriteFile(base+".meta.json", append(meta, '\n'), 0o644); err != nil {
			return err
		}
	}
	fmt.Println("samples written to", *outDir)
	return nil
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite snapshot path")
	cfgPath := fs.String("config", "", "JSON config file")
	threshold := fs.Int("threshold", 0, "dual-path switch threshold (0 = default/config)")
	concurrency := fs.Int("concurrency", 0, "max concurrent sessions")
	budget := fs.Int("budget", 0, "per-session component budget")
	inject := fs.String("inject", "", "comma-separated injection kinds")
	out := fs.String("out", "report.json", "report output path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	if *threshold > 0 {
		cfg.Threshold = *threshold
	}
	if *concurrency > 0 {
		cfg.MaxConcurrent = *concurrency
	}
	if *budget > 0 {
		cfg.ItemBudget = *budget
	}
	kinds := parseKinds(*inject, cfg.Inject)

	ctx := context.Background()
	store, err := snapshot.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer store.Close()

	mgr := session.NewManager(store, session.Config{
		MaxConcurrent: cfg.MaxConcurrent, ItemBudget: cfg.ItemBudget, Threshold: cfg.Threshold,
	})
	recovered, err := mgr.Recover(ctx)
	if err != nil {
		return err
	}

	bundles := selectBundles(kinds)
	results := make([]session.RunResult, len(bundles))
	var wg sync.WaitGroup
	for i, b := range bundles {
		wg.Add(1)
		go func(i int, b samples.Bundle) {
			defer wg.Done()
			results[i] = mgr.SubmitAndWait(ctx, session.RunRequest{
				TaskID:          "task-" + string(b.Kind),
				ManifestBuf:     b.Manifest,
				EdgesBuf:        []byte(b.Edges),
				Roots:           b.Roots,
				PersistSnapshot: b.Kind == samples.KindValid,
			})
		}(i, b)
	}
	wg.Wait()

	loadReports := make(map[string]snapshot.LoadReport)
	if rec, lr, lerr := store.Load(ctx, 0); lerr == nil {
		_ = rec
		loadReports["task-valid"] = lr
	} else {
		var ce *snapshot.ChainError
		if errors.As(lerr, &ce) {
			loadReports["task-valid"] = ce.Report
		}
	}

	rep := report.FromResults(results, loadReports)
	raw, err := rep.Marshal()
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, raw, 0o644); err != nil {
		return err
	}
	fmt.Printf("verify complete: %d sessions, %d recovered, report=%s\n",
		len(results), len(recovered), *out)
	return nil
}

func cmdInject(args []string) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	kind := fs.String("kind", "", "injection kind")
	dbPath := fs.String("db", "depverify.db", "SQLite snapshot path")
	outDir := fs.String("out", "samples", "sample output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *kind == "" {
		return errors.New("--kind is required")
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	ctx := context.Background()
	store, err := snapshot.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	bundle := findBundle(samples.Kind(*kind))
	if bundle == nil {
		return fmt.Errorf("unknown injection kind %q", *kind)
	}
	base := filepath.Join(*outDir, string(bundle.Kind))
	if err := os.WriteFile(base+".bin", bundle.Manifest, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(base+".edges.jsonl", []byte(bundle.Edges), 0o644); err != nil {
		return err
	}

	if samples.Kind(*kind) == samples.KindSnapshotGap {
		// 先连续提交三个健康快照，再制造版本 2 空洞，验证跳号回退。
		mgr := session.NewManager(store, session.Config{MaxConcurrent: 1, Threshold: 32})
		// 每轮使用差异化根集合，确保产生三条内容不同、版本连续的快照。
		seedRoots := [][]string{
			bundle.Roots,
			{"acme/core"},
			{"acme/net"},
		}
		for i, roots := range seedRoots {
			res := mgr.SubmitAndWait(ctx, session.RunRequest{
				TaskID: fmt.Sprintf("gap-seed-%d", i), ManifestBuf: bundle.Manifest,
				EdgesBuf: []byte(bundle.Edges), Roots: roots, PersistSnapshot: true,
			})
			if res.State != session.StateCompleted || res.Snapshot == nil {
				return fmt.Errorf("seed snapshot failed: %s", res.State)
			}
		}
		if err := store.InjectGap(ctx, 2); err != nil {
			return err
		}
		_, lr, lerr := store.Load(ctx, 3)
		fmt.Printf("injection %s: code=%s fallback=%d continuous=%v err=%v\n",
			*kind, lr.Code, lr.FallbackVersion, lr.Continuous, lerr)
		return nil
	}

	mgr := session.NewManager(store, session.Config{MaxConcurrent: 1, Threshold: 32})
	res := mgr.SubmitAndWait(ctx, session.RunRequest{
		TaskID: "inject-" + *kind, ManifestBuf: bundle.Manifest,
		EdgesBuf: []byte(bundle.Edges), Roots: bundle.Roots, PersistSnapshot: false,
	})
	raw, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(raw))
	fmt.Printf("injection %s: expected=%s snapshot_not_advanced=true\n", *kind, bundle.ExpectedCode())
	return nil
}

func cmdDualPath(args []string) error {
	fs := flag.NewFlagSet("dualpath", flag.ContinueOnError)
	n := fs.Int("components", 64, "number of chain components")
	threshold := fs.Int("threshold", 32, "switch threshold")
	out := fs.String("out", "dualpath.json", "report output path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bundle := samples.Large(*n)
	components, mErr := manifest.Parse(bytesReader(bundle.Manifest))
	if mErr != nil {
		return mErr
	}
	edgeList, edgeIssues := edges.Parse(bytesReader(bundle.Edges))
	known := make(map[string]struct{}, len(components))
	compMap := make(map[string]model.Component, len(components))
	for _, c := range components {
		known[c.ID] = struct{}{}
		compMap[c.ID] = c
	}
	graphIssues, unique := edges.CheckGraph(edgeList, known)
	allIssues := append(append([]model.ParseIssue{}, edgeIssues...), graphIssues...)
	uniqueEdges := make([]model.Edge, 0, len(unique))
	for _, e := range unique {
		uniqueEdges = append(uniqueEdges, e)
	}
	g := closure.NewGraph(uniqueEdges, compMap)

	// 相同输入分别强制走精算与批量路径，剔除路径元数据后比对业务结论。
	precise := engine.New(g, *threshold).VerifyWithPath(bundle.Roots, 0, allIssues, model.PathPrecise)
	batched := engine.New(g, *threshold).VerifyWithPath(bundle.Roots, 0, allIssues, model.PathBatched)
	autoSmall := engine.New(g, *n+1).Verify(bundle.Roots, 0, allIssues)
	autoLarge := engine.New(g, 1).Verify(bundle.Roots, 0, allIssues)
	strip := func(r model.ClosureResult) model.ClosureResult { r.Path = ""; r.Threshold = 0; return r }
	pb, _ := json.Marshal(strip(precise))
	bb, _ := json.Marshal(strip(batched))
	equal := string(pb) == string(bb)
	outDoc := map[string]any{
		"sample_marker":   model.FixedSampleMarker,
		"components":      *n,
		"threshold":       *threshold,
		"precise_path":    precise.Path,
		"batched_path":    batched.Path,
		"auto_small_path": autoSmall.Path,
		"auto_large_path": autoLarge.Path,
		"byte_identical":  equal,
		"precise_codes":   precise.CodeSequence,
		"batched_codes":   batched.CodeSequence,
		"precise_closure": precise.Closure,
		"batched_closure": batched.Closure,
	}
	raw, _ := json.MarshalIndent(outDoc, "", "  ")
	if err := os.WriteFile(*out, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("dualpath: precise=%s batched=%s byte_identical=%v report=%s\n",
		precise.Path, batched.Path, equal, *out)
	if !equal {
		return errors.New("dual path outputs diverge")
	}
	return nil
}

func selectBundles(kinds []string) []samples.Bundle {
	if len(kinds) == 0 {
		// 默认跑全量确定性样本。
		return samples.All()
	}
	out := make([]samples.Bundle, 0, len(kinds))
	for _, k := range kinds {
		if b := findBundle(samples.Kind(k)); b != nil {
			out = append(out, *b)
		}
	}
	return out
}

func findBundle(kind samples.Kind) *samples.Bundle {
	for _, b := range samples.All() {
		if b.Kind == kind {
			return &b
		}
	}
	return nil
}

func parseKinds(flagValue string, cfgKinds []string) []string {
	var out []string
	if flagValue != "" {
		start := 0
		for i := 0; i <= len(flagValue); i++ {
			if i == len(flagValue) || flagValue[i] == ',' {
				token := flagValue[start:i]
				if token != "" {
					out = append(out, token)
				}
				start = i + 1
			}
		}
	}
	return append(out, cfgKinds...)
}
