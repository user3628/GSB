package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"headprune/internal/appcli"
	"headprune/internal/model"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	ws := envOr("HEADPRUNE_WORKSPACE", "./workspace")
	if cmd == "demo" {
		if err := runDemo(args); err != nil {
			fatal(err)
		}
		return
	}
	if cmd == "serve" {
		if err := runServe(ws, args); err != nil {
			fatal(err)
		}
		return
	}
	svc, err := appcli.New(ws)
	if err != nil {
		fatal(err)
	}
	defer svc.Close()
	switch cmd {
	case "generate":
		_, err = svc.Generate()
	case "import-heads":
		err = requirePath(args, svc.ImportHeads)
	case "import-sentences":
		err = requirePath(args, svc.ImportSentences)
	case "set-policy":
		err = setPolicy(args, svc)
	case "run":
		var res any
		res, err = svc.RunAll()
		if err == nil {
			printJSON(res)
		}
	case "head-errors":
		var res any
		res, err = svc.HeadErrors()
		if err == nil {
			printJSON(res)
		}
	case "coupling":
		var res any
		res, err = svc.Coupling()
		if err == nil {
			printJSON(res)
		}
	case "quantization":
		var res any
		res, err = svc.Quantization()
		if err == nil {
			printJSON(res)
		}
	case "history":
		var res any
		res, err = svc.History()
		if err == nil {
			printJSON(res)
		}
	case "commit":
		var res any
		res, err = svc.Commit()
		if err == nil {
			printJSON(res)
		}
	case "version":
		err = versionCmd(args, svc)
	case "rollback":
		err = rollbackCmd(args, svc)
	case "stop":
		err = svc.Stop()
	case "resume":
		err = svc.Resume()
	case "status":
		printJSON(svc.Status())
	case "lineage":
		var res any
		res, err = svc.Lineage()
		if err == nil {
			printJSON(res)
		}
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

func runServe(ws string, args []string) error {
	addr := envOr("HEADPRUNE_ADDR", "127.0.0.1:8080")
	if len(args) > 0 {
		addr = args[0]
	}
	svc, err := appcli.New(ws)
	if err != nil {
		return err
	}
	defer svc.Close()
	srv := appcli.NewHTTPServer(svc, addr)
	fmt.Fprintf(os.Stderr, "headprune HTTP listening on %s (workspace=%s)\n", addr, ws)
	return srv.Start()
}

func runDemo(args []string) error {
	root := "./artifacts/demo"
	if len(args) > 0 {
		root = args[0]
	}
	summary1, err := demoRun(filepath.Join(root, "run1"))
	if err != nil {
		return err
	}
	summary2, err := demoRun(filepath.Join(root, "run2"))
	if err != nil {
		return err
	}
	b1, _ := json.MarshalIndent(summary1, "", "  ")
	b2, _ := json.MarshalIndent(summary2, "", "  ")
	if string(b1) != string(b2) {
		return fmt.Errorf("determinism check failed: demo summaries differ")
	}
	fmt.Println("== deterministic demo summary (identical across two runs) ==")
	fmt.Println(string(b1))
	return nil
}

func demoRun(dir string) (map[string]any, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	svc, err := appcli.New(dir)
	if err != nil {
		return nil, err
	}
	defer svc.Close()
	if _, err := svc.Generate(); err != nil {
		return nil, err
	}
	res, err := svc.RunAll()
	if err != nil {
		return nil, err
	}
	v1, err := svc.Commit()
	if err != nil {
		return nil, err
	}
	// A second decision with the same inputs yields the same digest.
	res2, err := svc.RunAll()
	if err != nil {
		return nil, err
	}
	v2, err := svc.Commit()
	if err != nil {
		return nil, err
	}
	rolled, err := svc.Rollback(v1.Version)
	if err != nil {
		return nil, err
	}
	restored, _, err := svc.RestoreVersion(rolled.Version)
	if err != nil {
		return nil, err
	}
	if restored.Fingerprint() != res.Decision.Fingerprint() {
		return nil, fmt.Errorf("rollback restore fingerprint mismatch")
	}
	coupledCount := len(res.Combos.Coupled)
	maxENOBGap := 0.0
	for _, q := range res.Quant.Heads {
		if gap := float64(q.Bits) - q.ENOB; gap > maxENOBGap {
			maxENOBGap = gap
		}
	}
	summary := map[string]any{
		"input_digest":        res.InputDigest,
		"decision_digest":     res.Decision.Fingerprint(),
		"selected_heads":      res.Decision.Selected,
		"empty_decision":      res.Decision.Empty,
		"coupled_combo_count": coupledCount,
		"max_enob_gap":        maxENOBGap,
		"versions_committed":  []int64{v1.Version, v2.Version, rolled.Version},
		"restored_head_fp":    restored.Selected.Fingerprint(),
		"quant_bits":          res.Quant.Bits,
		"probe_count":         len(res.Probe.Probes),
		"reused_rows":         res.ReusedRows,
	}
	if _, err := appcli.WriteArtifact(dir, "demo-summary.json", summary); err != nil {
		return nil, err
	}
	if _, err := appcli.WriteArtifact(dir, "stage-result.json", res2); err != nil {
		return nil, err
	}
	return summary, nil
}

func requirePath(args []string, fn func(string) error) error {
	if len(args) == 0 {
		return fmt.Errorf("a file path argument is required")
	}
	return fn(args[0])
}

func setPolicy(args []string, svc *appcli.Service) error {
	if len(args) == 0 {
		return fmt.Errorf("a policy JSON path is required")
	}
	var p model.Policy
	if err := model.ReadJSON(args[0], &p); err != nil {
		return err
	}
	return svc.SetPolicy(p)
}

func versionCmd(args []string, svc *appcli.Service) error {
	if len(args) == 0 {
		return fmt.Errorf("a version number is required")
	}
	v, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return err
	}
	d, vr, err := svc.RestoreVersion(v)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"version": vr, "decision": d})
	return nil
}

func rollbackCmd(args []string, svc *appcli.Service) error {
	if len(args) == 0 {
		return fmt.Errorf("a target version number is required")
	}
	v, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return err
	}
	vr, err := svc.Rollback(v)
	if err != nil {
		return err
	}
	printJSON(vr)
	return nil
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func fatal(err error) {
	if be, ok := model.AsBizError(err); ok {
		b, _ := json.Marshal(map[string]any{"error": be})
		fmt.Fprintln(os.Stderr, string(b))
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func usage() {
	fmt.Fprintln(os.Stderr, `headprune - deterministic attention-head pruning workbench

usage: headprune <command> [args]
  generate                 generate fixed-seed inputs in the workspace
  import-heads <path>      import attention-head shards JSON
  import-sentences <path>  import masked sentence pairs JSON
  set-policy <path>        import a pruning policy snapshot JSON
  run                      execute the four-stage pipeline
  head-errors              query per-head reconstruction errors
  coupling                 query combo replays and coupling amplification
  quantization             query per-head quant scales and error bounds
  commit                   commit the latest decision as a new version
  history                  list all committed versions
  version <n>              restore/inspect one version decision
  rollback <n>             roll back to an earlier active version
  stop / resume            graceful stop and recovery
  status                   workspace status
  serve [addr]             start the HTTP API (default 127.0.0.1:8080)
  demo [dir]               deterministic end-to-end demo (twice, compared)`)
}

var _ = context.Background
var _ = time.Second
