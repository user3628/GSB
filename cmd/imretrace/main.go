package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"imretrace/internal/model"
	"imretrace/internal/pipeline"
	"imretrace/internal/store"
	"imretrace/internal/web"
)

type globalFlags struct {
	root string
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	root := envOr("IMRETRACE_ROOT", ".")
	cmdIndex := 1
	if os.Args[1] == "--root" {
		if len(os.Args) < 4 {
			fail(errors.New("--root requires a path and a command"))
		}
		root = os.Args[2]
		cmdIndex = 3
	} else if strings.HasPrefix(os.Args[1], "--root=") {
		root = strings.TrimPrefix(os.Args[1], "--root=")
		cmdIndex = 2
	}
	if cmdIndex >= len(os.Args) {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[cmdIndex]
	args := os.Args[cmdIndex+1:]
	switch cmd {
	case "-h", "--help", "help":
		usage()
	case "prepare":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			items, err := svc.EnsureDemo(ctx)
			if err != nil {
				return err
			}
			return printOK(map[string]any{"items": items, "count": len(items), "params": svc.Paths.ParamsPath, "manifest": svc.Paths.Manifest})
		})
	case "generate":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			items, err := svc.RegenerateDemo(ctx)
			if err != nil {
				return err
			}
			return printOK(map[string]any{"items": items, "count": len(items)})
		})
	case "import":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			path := fs.String("path", "", "JSONL file or shard directory")
			if err := fs.Parse(args); err != nil {
				return err
			}
			if *path == "" {
				return errors.New("--path is required")
			}
			items, err := svc.ImportPath(ctx, *path)
			if err != nil {
				return err
			}
			return printOK(map[string]any{"imported": len(items), "items": items})
		})
	case "build", "recover":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			crash := fs.Int("crash-after-stage", 0, "send SIGKILL immediately after first checkpoint of this stage commits (1-4)")
			if err := fs.Parse(args); err != nil {
				return err
			}
			opts := pipeline.BuildOptions{}
			if *crash > 0 {
				stage := *crash
				opts.Hooks.AfterPublish = func(seq int, _, _ string) error {
					if seq == stage {
						_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
						select {}
					}
					return nil
				}
			}
			rep, err := svc.Build(ctx, opts)
			if err != nil {
				if biz, ok := model.AsBiz(err); ok && biz.Partial == nil && rep != nil {
					biz.Partial = rep
				}
				return err
			}
			return printOK(rep)
		})
	case "status":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			if err := fs.Parse(args); err != nil {
				return err
			}
			st, err := svc.Status(ctx)
			if err != nil {
				return err
			}
			return printJSON(st)
		})
	case "publish-test":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			itemID := fs.String("item", "", "existing item_id")
			seq := fs.Int("seq", 1, "checkpoint sequence 1-4")
			if err := fs.Parse(args); err != nil {
				return err
			}
			if _, err := svc.EnsureDemo(ctx); err != nil {
				return err
			}
			item, inputHash, err := svc.Store.GetItem(ctx, *itemID)
			if err != nil {
				return err
			}
			policy, err := svc.Store.CurrentPolicy(ctx)
			if err != nil {
				return err
			}
			if policy == nil {
				p := model.DefaultPolicy()
				policy = &p
			}
			if *seq < 1 || *seq > len(model.StageNames) {
				return fmt.Errorf("seq must be 1..%d", len(model.StageNames))
			}
			payload := `{"publish_test":true}`
			cp, err := svc.Store.PublishCheckpoint(ctx, store.PublishInput{
				ShardID: item.ShardID, ItemID: item.ItemID, Stage: model.StageNames[*seq-1], Seq: *seq,
				PolicyVersion: policy.Version, PolicyHash: policy.ContentHash, InputHash: inputHash,
				ArtifactDigest: model.HashBytes("publish-test-v1:", []byte(payload)), PayloadJSON: payload,
			})
			if err != nil {
				return err
			}
			return printOK(map[string]any{"checkpoint": cp})
		})
	case "state-hash":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			if err := fs.Parse(args); err != nil {
				return err
			}
			hash, _, err := svc.StateHash(ctx)
			if err != nil {
				return err
			}
			return printOK(map[string]string{"state_hash": hash})
		})
	case "report":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			out := fs.String("out", "", "report output path")
			if err := fs.Parse(args); err != nil {
				return err
			}
			rep, err := svc.ExportReport(ctx, *out)
			if err != nil {
				return err
			}
			return printJSON(rep)
		})
	case "search-text":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			q := fs.String("q", "", "query text")
			k := fs.Int("k", 0, "candidate depth; 0 uses policy")
			if err := fs.Parse(args); err != nil {
				return err
			}
			res, err := svc.SearchText(ctx, *q, *k)
			if err != nil {
				return err
			}
			return printJSON(map[string]any{"ok": true, "result": res})
		})
	case "search-image":
		runWithService(root, args, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			item := fs.String("item", "", "query item_id")
			k := fs.Int("k", 0, "candidate depth; 0 uses policy")
			if err := fs.Parse(args); err != nil {
				return err
			}
			res, err := svc.SearchImage(ctx, *item, *k)
			if err != nil {
				return err
			}
			return printJSON(map[string]any{"ok": true, "result": res})
		})
	case "policy":
		policyCommand(root, args)
	case "serve":
		serve(root, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(2)
	}
}

func policyCommand(root string, args []string) {
	if len(args) == 0 {
		fail(errors.New("policy requires show|patch|replace"))
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "show":
		runWithService(root, rest, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			if err := fs.Parse(rest); err != nil {
				return err
			}
			p, err := svc.Store.CurrentPolicy(ctx)
			if err != nil {
				return err
			}
			return printJSON(p)
		})
	case "patch":
		runWithService(root, rest, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			file := fs.String("file", "", "patch JSON file or - for stdin")
			if err := fs.Parse(rest); err != nil {
				return err
			}
			b, err := readPolicyFile(*file)
			if err != nil {
				return err
			}
			patch, err := model.ParsePatchJSON(b)
			if err != nil {
				return err
			}
			p, _, err := svc.ApplyPatch(ctx, patch)
			if err != nil {
				return err
			}
			return printOK(map[string]any{"policy": p})
		})
	case "replace":
		runWithService(root, rest, func(ctx context.Context, svc *pipeline.Service, fs *flag.FlagSet) error {
			file := fs.String("file", "", "full policy JSON file or - for stdin")
			demo := fs.Bool("next-demo", false, "construct a deterministic full replacement from current policy")
			if err := fs.Parse(rest); err != nil {
				return err
			}
			var p model.Policy
			if *demo {
				current, err := svc.Store.CurrentPolicy(ctx)
				if err != nil {
					return err
				}
				if current == nil {
					def := model.DefaultPolicy()
					current = &def
				}
				p = *current
				p.Version = current.Version + 1
				p.Kind = "full"
				p.Grid = 4
				p.QuantBits = 8
				p.CandidateDepth = 5
				p.FusionWeights = model.Weights{Cosine: 0.70, Term: 0.30}
				p.ContentHash = ""
				p.Refresh()
			} else {
				b, err := readPolicyFile(*file)
				if err != nil {
					return err
				}
				p, err = model.ParsePolicyJSON(b)
				if err != nil {
					return err
				}
			}
			saved, _, err := svc.ReplacePolicy(ctx, p)
			if err != nil {
				return err
			}
			return printOK(map[string]any{"policy": saved})
		})
	default:
		fail(fmt.Errorf("unknown policy subcommand %s", sub))
	}
}

func readPolicyFile(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("--file is required")
	}
	if path == "-" {
		return readAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func readAll(f *os.File) ([]byte, error) {
	dec := json.NewDecoder(f)
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return model.CanonicalBytes(v)
}

func serve(root string, args []string) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8765", "HTTP listen address")
	rootFlag := fs.String("root", root, "workspace root containing data/")
	_ = fs.Parse(args)
	svc, err := pipeline.NewService(*rootFlag)
	if err != nil {
		fail(err)
	}
	defer svc.Close()
	if _, err := svc.EnsureDemo(context.Background()); err != nil {
		fail(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: *addr, Handler: web.NewHandler(svc, stop), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		fmt.Fprintf(os.Stderr, "imretrace workbench: http://%s (data=%s)\n", *addr, svc.Paths.DataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fail(err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

type serviceCommand func(context.Context, *pipeline.Service, *flag.FlagSet) error

func runWithService(root string, args []string, fn serviceCommand) {
	resolvedRoot := flagValue(args, "--root", root)
	svc, err := pipeline.NewService(resolvedRoot)
	if err != nil {
		fail(err)
	}
	defer svc.Close()
	fs := flag.NewFlagSet(os.Args[1], flag.ContinueOnError)
	fs.String("root", resolvedRoot, "workspace root containing data/")
	if err := fn(context.Background(), svc, fs); err != nil {
		fail(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func hasRootFlag(args []string) bool {
	for _, a := range args {
		if a == "--root" || len(a) > 7 && a[:7] == "--root=" {
			return true
		}
	}
	return false
}

func flagValue(args []string, name, def string) string {
	for i, a := range args {
		if a == name && i+1 < len(args) {
			return args[i+1]
		}
		if len(a) > len(name)+1 && a[:len(name)+1] == name+"=" {
			return a[len(name)+1:]
		}
	}
	return def
}

func printOK(v any) error {
	return printJSON(map[string]any{"ok": true, "data": v})
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func fail(err error) {
	code := 1
	resp := map[string]any{"ok": false}
	if biz, ok := model.AsBiz(err); ok {
		resp["error"] = biz
		switch biz.Code {
		case model.CodeQueryInvalid, model.CodeFeatureInvalid, model.CodeQuantInvalid, model.CodeNumericMismatch:
			code = 3
		case model.CodeStaleVersion, model.CodeStaleCheckpoint:
			code = 4
		default:
			code = 1
		}
	} else {
		resp["error"] = map[string]string{"code": model.CodeInternal, "message": err.Error()}
	}
	_ = printJSON(resp)
	os.Exit(code)
}

func usage() {
	fmt.Fprint(os.Stderr, `imretrace - deterministic cross-modal alignment demo

Usage:
  imretrace [--root .] prepare
  imretrace [--root .] generate
  imretrace [--root .] import --path <file.jsonl|shard-dir>
  imretrace [--root .] build [--crash-after-stage 1|2|3|4]
  imretrace [--root .] recover
  imretrace [--root .] status
  imretrace [--root .] publish-test --item item-shard-0001 --seq 1
  imretrace [--root .] state-hash
  imretrace [--root .] report [--out diagnostics.json]
  imretrace [--root .] search-text --q "red circle 红色" [--k 5]
  imretrace [--root .] search-image --item item-shard-0001 [--k 5]
  imretrace [--root .] policy show
  imretrace [--root .] policy patch --file patch.json
  imretrace [--root .] policy replace --file full.json
  imretrace [--root .] policy replace --next-demo
  imretrace [--root .] serve --addr 127.0.0.1:8765
`)
}
