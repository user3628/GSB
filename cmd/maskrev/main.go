package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"maskrevision/internal/demo"
	"maskrevision/internal/generator"
	"maskrevision/internal/httpapi"
	"maskrevision/internal/imageio"
	"maskrevision/internal/model"
	"maskrevision/internal/replay"
	"maskrevision/internal/revlog"
	"maskrevision/internal/store"
	"maskrevision/internal/workspace"
)

const usage = `maskrev - deterministic panoptic mask revision workbench

Usage:
  maskrev demo        [--work DIR] [--seed N] [--workers N]
  maskrev init        [--work DIR] [--seed N] [--overlap POLICY] [--undo-policy POLICY]
  maskrev generate    [--data DIR] [--seed N]
  maskrev replay      [--work DIR] [--fresh] [--crash-after N] [--hard-kill]
  maskrev serve       [--work DIR] [--addr 127.0.0.1:8080]
  maskrev snapshot    [--work DIR] --shard ID --revision N [--png FILE]
  maskrev diff        [--work DIR] --shard ID --from N --to N
  maskrev upgrade     [--work DIR] --shard ID
  maskrev classify    [--work DIR] --shard ID --fixture RELATIVE_PATH
  maskrev publish     [--work DIR] --shard ID --expected N --revision N [--publisher NAME]
  maskrev verify      [--work DIR]

All data is generated locally from the fixed seed. The default work directory is .maskrev.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "demo":
		err = runDemo(os.Args[2:])
	case "init":
		err = runInit(os.Args[2:])
	case "generate":
		err = runGenerate(os.Args[2:])
	case "replay":
		err = runReplay(os.Args[2:])
	case "serve":
		err = runServe(os.Args[2:])
	case "snapshot":
		err = runSnapshot(os.Args[2:])
	case "diff":
		err = runDiff(os.Args[2:])
	case "upgrade":
		err = runUpgrade(os.Args[2:])
	case "classify":
		err = runClassify(os.Args[2:])
	case "publish":
		err = runPublish(os.Args[2:], false)
	case "publish-worker":
		err = runPublish(os.Args[2:], true)
	case "verify":
		err = runVerify(os.Args[2:])
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(1)
	}
	if err != nil {
		fail(err)
	}
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	seed := fs.Int64("seed", generator.DefaultSeed, "fixed generator seed")
	workers := fs.Int("workers", 4, "concurrent publisher processes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	report, path, err := demo.Run(demo.Options{Work: *work, Seed: *seed, Exe: exe, Workers: *workers})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "demo report: %s\n", path)
	return printJSON(report)
}

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	seed := fs.Int64("seed", generator.DefaultSeed, "fixed generator seed")
	overlap := fs.String("overlap", model.OverlapLastWrite, "last_write_wins or first_write_wins")
	undoPolicy := fs.String("undo-policy", model.UndoSegmentRecycle, "recycle or void")
	phraseMap := fs.String("phrase-map", "", "JSON phrase mapping file for v1 logs")
	neighborRadius := fs.Int("neighbor-radius", 0, "v1 instance target neighborhood radius")
	strictV1 := fs.Bool("strict-v1", false, "fail on unrecoverable v1 actions")
	if err := fs.Parse(args); err != nil {
		return err
	}
	layout, err := workspace.Prepare(*work, true)
	if err != nil {
		return err
	}
	if _, err := generator.Generate(layout.Data, *seed); err != nil {
		return err
	}
	upgrade, err := loadUpgradeOptions(*phraseMap, *neighborRadius, *strictV1)
	if err != nil {
		return err
	}
	ws, err := workspace.Open(layout.Root, workspace.Options{Replay: replay.Options{OverlapPolicy: *overlap, UndoSegmentPolicy: *undoPolicy}, Upgrade: upgrade})
	if err != nil {
		return err
	}
	defer ws.Close()
	summaries, err := ws.ShardsSummary()
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"workspace_digest": ws.Digest(), "database": ws.Store.Path(), "shards": summaries})
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	data := fs.String("data", filepath.Join(".maskrev", "data"), "output data directory")
	seed := fs.Int64("seed", generator.DefaultSeed, "fixed generator seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	manifest, err := generator.Generate(*data, *seed)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"data": *data, "manifest_digest": model.SHA256Hex([]byte(fmt.Sprintf("%+v", manifest))), "shards": len(manifest.Shards)})
}

func runReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	fresh := fs.Bool("fresh", false, "delete SQLite checkpoints before replay")
	crashAfter := fs.Int("crash-after", 0, "force process termination after N newly committed checkpoints")
	hardKill := fs.Bool("hard-kill", false, "use process kill instead of os.Exit for crash-after")
	overlap := fs.String("overlap", model.OverlapLastWrite, "last_write_wins or first_write_wins")
	undoPolicy := fs.String("undo-policy", model.UndoSegmentRecycle, "recycle or void")
	phraseMap := fs.String("phrase-map", "", "JSON phrase mapping file for v1 logs")
	neighborRadius := fs.Int("neighbor-radius", 0, "v1 instance target neighborhood radius")
	strictV1 := fs.Bool("strict-v1", false, "fail on unrecoverable v1 actions")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *fresh {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(filepath.Join(*work, "checkpoints.db") + suffix)
		}
	}
	upgrade, err := loadUpgradeOptions(*phraseMap, *neighborRadius, *strictV1)
	if err != nil {
		return err
	}
	opts := workspace.Options{Replay: replay.Options{OverlapPolicy: *overlap, UndoSegmentPolicy: *undoPolicy}, Upgrade: upgrade}
	if *crashAfter > 0 {
		opts.CrashAfter = *crashAfter
		opts.CrashFunc = func() error {
			if *hardKill {
				p, err := os.FindProcess(os.Getpid())
				if err != nil {
					return err
				}
				_ = p.Kill()
				time.Sleep(5 * time.Second)
				return fmt.Errorf("process kill did not terminate the process")
			}
			os.Exit(137)
			return nil
		}
	}
	ws, err := workspace.Open(*work, opts)
	if err != nil {
		return err
	}
	defer ws.Close()
	summaries, err := ws.ShardsSummary()
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"workspace_digest": ws.Digest(), "shards": summaries})
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	addr := fs.String("addr", "127.0.0.1:8080", "local listen address")
	overlap := fs.String("overlap", model.OverlapLastWrite, "last_write_wins or first_write_wins")
	undoPolicy := fs.String("undo-policy", model.UndoSegmentRecycle, "recycle or void")
	phraseMap := fs.String("phrase-map", "", "JSON phrase mapping file for v1 logs")
	neighborRadius := fs.Int("neighbor-radius", 0, "v1 instance target neighborhood radius")
	strictV1 := fs.Bool("strict-v1", false, "fail on unrecoverable v1 actions")
	if err := fs.Parse(args); err != nil {
		return err
	}
	upgrade, err := loadUpgradeOptions(*phraseMap, *neighborRadius, *strictV1)
	if err != nil {
		return err
	}
	ws, err := workspace.Open(*work, workspace.Options{Replay: replay.Options{OverlapPolicy: *overlap, UndoSegmentPolicy: *undoPolicy}, Upgrade: upgrade})
	if err != nil {
		return err
	}
	defer ws.Close()
	srv := &http.Server{Addr: *addr, Handler: httpapi.New(ws).Handler(), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	fmt.Printf("mask revision workbench: http://%s\n", *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func runSnapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	shard := fs.String("shard", "", "shard id")
	revision := fs.Int("revision", 0, "revision number")
	pngOut := fs.String("png", "", "optional PNG output path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ws, err := workspace.Open(*work, workspace.Options{})
	if err != nil {
		return err
	}
	defer ws.Close()
	snap, be := ws.Snapshot(*shard, *revision)
	if be != nil {
		return be
	}
	if *pngOut != "" {
		segments := map[int]*model.Segment{}
		for i := range snap.Segments {
			seg := snap.Segments[i]
			segments[seg.ID] = &seg
		}
		data, err := imageio.EncodeIndexed(snap.Width, snap.Height, snap.Pixels, segments)
		if err != nil {
			return err
		}
		if err := os.WriteFile(*pngOut, data, 0o644); err != nil {
			return err
		}
		return printJSON(map[string]any{"png": *pngOut, "snapshot_digest": snap.SnapshotDigest})
	}
	return printJSON(snap)
}

func runDiff(args []string) error {
	fs := flag.NewFlagSet("diff", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	shard := fs.String("shard", "", "shard id")
	from := fs.Int("from", 0, "from revision")
	to := fs.Int("to", 0, "to revision")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ws, err := workspace.Open(*work, workspace.Options{})
	if err != nil {
		return err
	}
	defer ws.Close()
	result, be := ws.Diff(*shard, *from, *to)
	if be != nil {
		return be
	}
	return printJSON(result)
}

func runUpgrade(args []string) error {
	fs := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	shard := fs.String("shard", "", "shard id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ws, err := workspace.Open(*work, workspace.Options{})
	if err != nil {
		return err
	}
	defer ws.Close()
	verification, be := ws.ExportAndVerifyV1(*shard)
	if be != nil {
		return be
	}
	return printJSON(verification)
}

func runClassify(args []string) error {
	fs := flag.NewFlagSet("classify", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	shard := fs.String("shard", "", "shard id")
	fixture := fs.String("fixture", "", "path relative to data directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ws, err := workspace.Open(*work, workspace.Options{})
	if err != nil {
		return err
	}
	defer ws.Close()
	result, be := ws.ClassifyFixture(*shard, *fixture)
	if be != nil {
		return be
	}
	return printJSON(result)
}

func runPublish(args []string, worker bool) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	shard := fs.String("shard", "", "shard id")
	expected := fs.Int("expected", -1, "expected current published revision")
	revision := fs.Int("revision", 0, "revision to publish")
	publisher := fs.String("publisher", "cli", "publisher identity")
	digest := fs.String("snapshot-digest", "", "optional expected snapshot digest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	layout, err := workspace.Prepare(*work, false)
	if err != nil {
		return err
	}
	st, err := store.Open(layout.Database)
	if err != nil {
		return err
	}
	defer st.Close()
	res, be := st.Publish(store.PublishLockPath(layout.Root, *shard), store.PublishRequest{
		ShardID: *shard, Revision: *revision, ExpectedRevision: *expected, Publisher: *publisher, SnapshotDigest: *digest,
	})
	if worker {
		if be != nil {
			out := map[string]any{"worker": *publisher, "accepted": false, "code": be.Code, "revision": *revision, "message": be.Message}
			data, _ := json.Marshal(out)
			fmt.Println(string(data))
			return nil
		}
		out := map[string]any{"worker": *publisher, "accepted": res.Accepted, "code": res.Code, "revision": *revision, "snapshot_digest": res.State.SnapshotDigest}
		data, _ := json.Marshal(out)
		fmt.Println(string(data))
		return nil
	}
	if be != nil {
		return be
	}
	return printJSON(res)
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	work := fs.String("work", ".maskrev", "workspace directory")
	phraseMap := fs.String("phrase-map", "", "JSON phrase mapping file for v1 logs")
	neighborRadius := fs.Int("neighbor-radius", 0, "v1 instance target neighborhood radius")
	strictV1 := fs.Bool("strict-v1", false, "fail on unrecoverable v1 actions")
	if err := fs.Parse(args); err != nil {
		return err
	}
	upgradeOpts, err := loadUpgradeOptions(*phraseMap, *neighborRadius, *strictV1)
	if err != nil {
		return err
	}
	ws, err := workspace.Open(*work, workspace.Options{Upgrade: upgradeOpts})
	if err != nil {
		return err
	}
	defer ws.Close()
	summaries, err := ws.ShardsSummary()
	if err != nil {
		return err
	}
	upgrade, be := ws.ExportAndVerifyV1("shard-01")
	if be != nil {
		return be
	}
	unrecoverable, be := ws.UnrecoverableFixture()
	if be != nil {
		return be
	}
	fixtures := map[string]workspace.FixtureClassification{}
	for name, path := range map[string]string{
		"fork": "fixtures/shard-00.fork.json", "edited": "fixtures/shard-00.edited.json", "rollback": "fixtures/shard-00.rollback.json",
	} {
		f, be := ws.ClassifyFixture("shard-00", path)
		if be != nil {
			return be
		}
		fixtures[name] = f
	}
	return printJSON(map[string]any{
		"workspace_digest": ws.Digest(), "shards": summaries, "v1_upgrade": upgrade,
		"unrecoverable_v1_fixture": unrecoverable, "fixtures": fixtures,
	})
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func loadUpgradeOptions(path string, radius int, strict bool) (revlog.UpgradeOptions, error) {
	opts := revlog.UpgradeOptions{NeighborRadius: radius, StrictUnrecoverable: strict}
	if path == "" {
		return opts, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return opts, err
	}
	var rules []revlog.PhraseRule
	if err := json.Unmarshal(data, &rules); err == nil && rules != nil {
		opts.PhraseRules = rules
		return opts, nil
	}
	var cfg struct {
		PhraseRules         []revlog.PhraseRule `json:"phrase_rules"`
		NeighborRadius      int                 `json:"neighbor_radius"`
		StrictUnrecoverable bool                `json:"strict_unrecoverable"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return opts, fmt.Errorf("decode phrase map %s: %w", path, err)
	}
	opts.PhraseRules = cfg.PhraseRules
	if radius == 0 {
		opts.NeighborRadius = cfg.NeighborRadius
	}
	opts.StrictUnrecoverable = strict || cfg.StrictUnrecoverable
	return opts, nil
}

func fail(err error) {
	be := model.AsBusinessError(err)
	_ = json.NewEncoder(os.Stderr).Encode(map[string]any{"error": be})
	os.Exit(2)
}
