// Command ticketctl is the command-line driver for the deterministic ticket
// intent and priority pipeline. It also embeds the end-to-end demo used for
// acceptance verification.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/engine"
	"ticketpipeline/internal/gen"
	"ticketpipeline/internal/httpx"
	"ticketpipeline/internal/model"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	dataDir := envOr("TICKET_DATA_DIR", "data")
	outDir := envOr("TICKET_OUT_DIR", "out")
	if err := dispatch(cmd, args, dataDir, outDir); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		if be, ok := model.AsBizError(err); ok {
			fmt.Fprintf(os.Stderr, "code=%s stage=%s ticket=%s\n", be.Code, be.Stage, be.TicketID)
		}
		os.Exit(1)
	}
}

func dispatch(cmd string, args []string, dataDir, outDir string) error {
	switch cmd {
	case "generate":
		return generate(dataDir)
	case "run":
		return runPipeline(dataDir, outDir, "")
	case "crash":
		if len(args) < 1 {
			return fmt.Errorf("usage: crash stage:ticketID")
		}
		return runPipeline(dataDir, outDir, args[0])
	case "status":
		return status(dataDir, outDir)
	case "report":
		return report(dataDir, outDir)
	case "verify":
		return verify(outDir)
	case "change-threshold":
		return changeThreshold(dataDir, args)
	case "change-smoothing":
		return changeSmoothing(dataDir, args)
	case "change-hashdim":
		return changeHashDim(dataDir, args)
	case "inject":
		return inject(dataDir, args)
	case "replace-shard":
		return replaceShard(dataDir, args)
	case "serve":
		port := "18080"
		if len(args) > 0 {
			port = args[0]
		}
		return serve(dataDir, outDir, port)
	case "demo":
		return runDemo(dataDir, outDir)
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func generate(dataDir string) error {
	shards := gen.GenerateShards()
	for _, sh := range shards {
		if err := gen.WriteShard(dataDir, sh); err != nil {
			return err
		}
	}
	if err := gen.WritePolicy(dataDir, gen.GeneratePolicy()); err != nil {
		return err
	}
	fmt.Printf("generated %d shards, %d tickets into %s\n", len(shards), 3*20, dataDir)
	return nil
}

func runPipeline(dataDir, outDir, crashAt string) error {
	eng, err := engine.New(dataDir, outDir)
	if err != nil {
		return err
	}
	defer eng.Close()
	if crashAt != "" {
		eng.SetCrashAt(crashAt)
	}
	snap, err := eng.Run(context.Background())
	if err != nil {
		return err
	}
	printSnapshot(snap)
	return nil
}

func status(dataDir, outDir string) error {
	eng, err := engine.New(dataDir, outDir)
	if err != nil {
		return err
	}
	defer eng.Close()
	st, err := engine.Status(eng.Store())
	if err != nil {
		return err
	}
	for _, s := range st {
		fmt.Printf("%-9s gen=%d checkpoints=%d ok=%d errors=%d contiguous=%t fp=%s\n",
			s.Stage, s.LatestGen, s.Checkpoints, s.OK, s.BizErrors, s.Contiguous, short(s.ArtifactFP))
	}
	return nil
}

func report(dataDir, outDir string) error {
	eng, err := engine.New(dataDir, outDir)
	if err != nil {
		return err
	}
	defer eng.Close()
	snap, err := eng.Run(context.Background())
	if err != nil {
		return err
	}
	if snap.Report == nil {
		return fmt.Errorf("report not generated yet")
	}
	path := filepath.Join(outDir, "validate", "report.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fmt.Print(string(b))
	return nil
}

func verify(outDir string) error {
	eng, err := engine.New(envOr("TICKET_DATA_DIR", "data"), outDir)
	if err != nil {
		return err
	}
	defer eng.Close()
	return engine.VerifyCheckpoints(eng.Store())
}

func changeThreshold(dataDir string, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: change-threshold t0 t1 t2")
	}
	th, err := parseThreeFloats(args)
	if err != nil {
		return err
	}
	p, err := engine.BumpPolicy(dataDir, engine.PolicyPatch{PriorityThresh: th})
	if err != nil {
		return err
	}
	fmt.Printf("new policy version=%d thresholds=%v\n", p.Version, p.PriorityThresh)
	return nil
}

func changeSmoothing(dataDir string, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: change-smoothing value")
	}
	v, err := parseFloat(args[0])
	if err != nil {
		return err
	}
	p, err := engine.BumpPolicy(dataDir, engine.PolicyPatch{Smoothing: &v})
	if err != nil {
		return err
	}
	fmt.Printf("new policy version=%d smoothing=%g\n", p.Version, p.Smoothing)
	return nil
}

func changeHashDim(dataDir string, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: change-hashdim value")
	}
	v, err := parseInt(args[0])
	if err != nil {
		return err
	}
	p, err := engine.BumpPolicy(dataDir, engine.PolicyPatch{HashDim: &v})
	if err != nil {
		return err
	}
	fmt.Printf("new policy version=%d hash_dim=%d\n", p.Version, p.HashDim)
	return nil
}

func inject(dataDir string, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: inject empty-text|label-range|agreement-missing|zero-smoothing|zero-hashdim|shard-mismatch")
	}
	policy, err := engine.LoadPolicy(dataDir)
	if err != nil {
		return err
	}
	inj := policy.Injection
	switch args[0] {
	case "empty-text":
		inj.EmptyText = true
	case "label-range":
		inj.LabelOutOfRange = true
	case "agreement-missing":
		inj.AgreementMissing = true
	case "zero-smoothing":
		inj.ZeroSmoothing = true
		v := 0.0
		_, err = engine.BumpPolicy(dataDir, engine.PolicyPatch{Smoothing: &v, Injection: &inj})
	case "zero-hashdim":
		inj.ZeroHashDim = true
		v := 0
		_, err = engine.BumpPolicy(dataDir, engine.PolicyPatch{HashDim: &v, Injection: &inj})
	case "shard-mismatch":
		inj.ShardCountMismatch = true
		shards, lerr := engine.LoadShards(dataDir)
		if lerr != nil {
			return lerr
		}
		if err := engine.ReplaceShard(dataDir, shards[0].Meta.ShardID,
			shards[0].Tickets, true, policy.Version); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown injection %q", args[0])
	}
	if err != nil {
		return err
	}
	if args[0] != "zero-smoothing" && args[0] != "zero-hashdim" && args[0] != "shard-mismatch" {
		p, err := engine.BumpPolicy(dataDir, engine.PolicyPatch{Injection: &inj})
		if err != nil {
			return err
		}
		fmt.Printf("injection %s armed in policy version=%d\n", args[0], p.Version)
		return nil
	}
	fmt.Printf("injection %s armed\n", args[0])
	return nil
}

func replaceShard(dataDir string, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: replace-shard shard-id")
	}
	shards, err := engine.LoadShards(dataDir)
	if err != nil {
		return err
	}
	policy, err := engine.LoadPolicy(dataDir)
	if err != nil {
		return err
	}
	for _, sh := range shards {
		if sh.Meta.ShardID == args[0] {
			tickets := append([]model.RawTicket(nil), sh.Tickets...)
			return engine.ReplaceShard(dataDir, sh.Meta.ShardID, tickets, false, policy.Version)
		}
	}
	return fmt.Errorf("shard %q not found", args[0])
}

func serve(dataDir, outDir, port string) error {
	eng, err := engine.New(dataDir, outDir)
	if err != nil {
		return err
	}
	defer eng.Close()
	srv := httpx.NewServer(eng, dataDir, outDir)
	p, err := parseInt(port)
	if err != nil {
		return err
	}
	addr, err := srv.Start(p)
	if err != nil {
		return err
	}
	fmt.Printf("ticket pipeline listening on http://%s (loopback only)\n", addr)
	select {}
}

func printSnapshot(snap *engine.Snapshot) {
	fmt.Printf("policy v%d tickets=%d normalize_errors=%d intent_errors=%d priority_errors=%d\n",
		snap.Policy.Version, len(snap.Tickets), len(snap.NormErr), len(snap.IntentErr), len(snap.PrioErr))
	if snap.Report != nil {
		fmt.Printf("report digest=%s\n", snap.Report.Digest)
		fmt.Printf("intent accuracy=%.4f verdict_counts=%v\n", snap.Report.IntentAccuracy, snap.Report.VerdictCounts)
	}
}

func short(fp string) string {
	if len(fp) > 12 {
		return fp[:12]
	}
	return fp
}

func usage() {
	fmt.Fprint(os.Stderr, `ticketctl - deterministic ticket intent/priority pipeline

commands:
  generate                 create deterministic shards + policy v1
  run                      execute (or incrementally resume) the four stages
  crash stage:ticketID     hard-exit after that checkpoint commits
  status                   show per-stage checkpoint/fingerprint status
  report                   print the latest validation report
  verify                   replay-checkpoint invariants from SQLite
  change-threshold a b c   bump policy with new priority thresholds
  change-smoothing v       bump policy with new smoothing
  change-hashdim v         bump policy with new hash dimension
  inject <name>            arm an anomaly injection switch
  replace-shard id         rewrite one shard deterministically
  serve [port]             loopback HTTP API (default 18080)
  demo                     full acceptance demo incl. crash recovery
`)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var _ = determ.FingerprintBytes
