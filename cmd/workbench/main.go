package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"regworkbench/internal/api"
	"regworkbench/internal/config"
	"regworkbench/internal/dataset"
	"regworkbench/internal/demo"
	"regworkbench/internal/experiment"
	"regworkbench/internal/verify"
	"regworkbench/internal/workerr"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(0)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = cmdGenerate(os.Args[2:])
	case "demo":
		err = cmdDemo(os.Args[2:])
	case "verify":
		err = cmdVerify(os.Args[2:])
	case "serve":
		err = cmdServe(os.Args[2:])
	case "train":
		err = cmdTrain(os.Args[2:])
	case "query":
		err = cmdQuery(os.Args[2:])
	case "stop":
		err = cmdStop(os.Args[2:])
	case "resume":
		err = cmdResume(os.Args[2:])
	case "export":
		err = cmdExport(os.Args[2:])
	case "incremental":
		err = cmdIncremental(os.Args[2:])
	case "version":
		fmt.Println("regworkbench 1.0.0 deterministic regularized linear model workbench")
	default:
		usage()
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		if we, ok := workerr.As(err); ok {
			b, _ := json.MarshalIndent(we, "", "  ")
			fmt.Fprintln(os.Stderr, string(b))
		} else {
			fmt.Fprintln(os.Stderr, `{"code":"E_CLI","message":`+strconv.Quote(err.Error())+`}`)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`regworkbench commands:
  generate   --out samples
  demo       --out artifacts/demo [--full]
  verify     --out artifacts/verification
  serve      --addr 127.0.0.1:8787 --root artifacts/http
  train      --manifest samples/manifest.json --key small --dir sessions/small
  query      --dir sessions/small --kind state|trajectory|metrics|current_coefficients|selection|final_model
  stop       --dir sessions/small
  resume     --dir sessions/small
  export     --dir sessions/small --kind coefficients|summary|trajectory --out file.json
  incremental --base sessions/a --dir sessions/b [--folds 4] [--metric auc] [--lambdas 0,.001] [--ratios 0,.5,1] [--encoding cat_01=target]
`)
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", "samples", "output directory")
	seed := fs.Int64("seed", dataset.DefaultSeed, "deterministic seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	m, err := dataset.GenerateDirectory(*out, *seed)
	if err != nil {
		return err
	}
	return printJSON(m)
}

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	out := fs.String("out", "artifacts/demo", "demo output directory")
	full := fs.Bool("full", false, "use full default path grid")
	if err := fs.Parse(args); err != nil {
		return err
	}
	summary, err := demo.Run(*out, *full)
	if err != nil {
		return err
	}
	return printJSON(summary)
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	out := fs.String("out", "artifacts/verification", "verification output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rep, err := verify.Run(*out)
	if err != nil {
		return err
	}
	if err := printJSON(rep); err != nil {
		return err
	}
	if !rep.OK {
		return fmt.Errorf("verification checks failed; see %s/verification.json", *out)
	}
	return nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8787", "listen address")
	root := fs.String("root", "artifacts/http", "server workspace root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	srv, err := api.NewServer(*root)
	if err != nil {
		return err
	}
	fmt.Printf("regworkbench listening on http://%s (root=%s)\n", *addr, *root)
	return http.ListenAndServe(*addr, srv.Handler())
}

type trainFlags struct {
	manifest, key, dir, metric, lambdas, ratios, encodings string
	folds, maxIter                                         int
	budget                                                 int64
}

func addTrainFlags(fs *flag.FlagSet) *trainFlags {
	f := &trainFlags{}
	fs.StringVar(&f.manifest, "manifest", "samples/manifest.json", "manifest path")
	fs.StringVar(&f.key, "key", "small", "dataset key")
	fs.StringVar(&f.dir, "dir", "sessions/small", "session directory")
	fs.IntVar(&f.folds, "folds", 3, "cross validation folds")
	fs.StringVar(&f.metric, "metric", "log_loss", "selection metric")
	fs.StringVar(&f.lambdas, "lambdas", "0,0.0003,0.001,0.003,0.01", "comma separated lambda path")
	fs.StringVar(&f.ratios, "ratios", "0,0.5,1", "comma separated l1 ratios")
	fs.IntVar(&f.maxIter, "max-iter", 160, "maximum solver iterations")
	fs.Int64Var(&f.budget, "budget-ms", 250, "query latency budget")
	fs.StringVar(&f.encodings, "encoding", "", "comma separated col=one_hot|target overrides")
	return f
}

func (f *trainFlags) config() (config.Config, error) {
	cfg := config.Default(f.key)
	cfg.KFolds = f.folds
	cfg.Metric = f.metric
	cfg.MaxIterations = f.maxIter
	cfg.LatencyBudgetMS = f.budget
	var err error
	if f.lambdas != "" {
		cfg.Lambdas, err = parseFloatList(f.lambdas)
		if err != nil {
			return cfg, err
		}
	}
	if f.ratios != "" {
		cfg.L1Ratios, err = parseFloatList(f.ratios)
		if err != nil {
			return cfg, err
		}
	}
	if f.encodings != "" {
		for _, item := range strings.Split(f.encodings, ",") {
			kv := strings.SplitN(item, "=", 2)
			if len(kv) != 2 {
				return cfg, fmt.Errorf("invalid --encoding %q", item)
			}
			cfg.Encoding[kv[0]] = kv[1]
		}
	}
	return cfg.Normalize()
}

func cmdTrain(args []string) error {
	fs := flag.NewFlagSet("train", flag.ContinueOnError)
	f := addTrainFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	ds, err := dataset.LoadDataset(f.manifest, f.key)
	if err != nil {
		return err
	}
	cfg, err := f.config()
	if err != nil {
		return err
	}
	sess, err := experiment.CreateSession(f.dir, cfg, ds, experiment.Hooks{})
	if err != nil {
		return err
	}
	defer sess.Close()
	installStopSignal(sess)
	rep, err := sess.Run(context.Background())
	if err != nil {
		return err
	}
	return printJSON(rep)
}

func cmdQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	dir := fs.String("dir", "", "session directory")
	kind := fs.String("kind", "state", "query kind")
	fold := fs.Int("fold", -1, "fold index")
	lambda := fs.Float64("lambda", 0, "lambda")
	ratio := fs.Float64("l1-ratio", 0, "l1 ratio")
	hasPoint := fs.Bool("point", false, "select lambda/l1-ratio point")
	budget := fs.Int64("budget-ms", -1, "budget override; 0 deterministically exceeds")
	qid := fs.String("query-id", "", "query id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("--dir is required")
	}
	sess, err := experiment.OpenSession(*dir, experiment.Hooks{})
	if err != nil {
		return err
	}
	defer sess.Close()
	req := experiment.QueryRequest{Kind: *kind, QueryID: *qid}
	if *fold >= 0 {
		req.Fold = fold
	}
	if *hasPoint {
		req.Lambda = lambda
		req.L1Ratio = ratio
	}
	if *budget >= 0 {
		req.BudgetMS = budget
	}
	resp, err := sess.Query(req)
	if err != nil {
		return err
	}
	return printJSON(resp)
}

func cmdStop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	dir := fs.String("dir", "", "session directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sess, err := experiment.OpenSession(*dir, experiment.Hooks{})
	if err != nil {
		return err
	}
	defer sess.Close()
	if err := sess.Store.RequestStop(); err != nil {
		return err
	}
	return printJSON(map[string]any{"stop_requested": true, "dir": *dir})
}

func cmdResume(args []string) error {
	fs := flag.NewFlagSet("resume", flag.ContinueOnError)
	dir := fs.String("dir", "", "session directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sess, err := experiment.OpenSession(*dir, experiment.Hooks{})
	if err != nil {
		return err
	}
	defer sess.Close()
	if err := sess.Store.ClearStop(); err != nil {
		return err
	}
	installStopSignal(sess)
	rep, err := sess.Run(context.Background())
	if err != nil {
		return err
	}
	return printJSON(rep)
}

func cmdExport(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	dir := fs.String("dir", "", "session directory")
	kind := fs.String("kind", "coefficients", "coefficients|summary|trajectory")
	out := fs.String("out", "", "output file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	sess, err := experiment.OpenSession(*dir, experiment.Hooks{})
	if err != nil {
		return err
	}
	defer sess.Close()
	if *out == "" {
		*out = *kind + ".json"
	}
	switch *kind {
	case "coefficients", "model":
		if err := sess.ExportCoefficients(*out); err != nil {
			return err
		}
	case "trajectory":
		if err := sess.ExportTrajectory(*out); err != nil {
			return err
		}
	case "summary", "validation":
		b, err := os.ReadFile(sess.Root + "/exports/validation_summary.json")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown export kind %q", *kind)
	}
	return printJSON(map[string]any{"exported": *kind, "path": *out})
}

func cmdIncremental(args []string) error {
	fs := flag.NewFlagSet("incremental", flag.ContinueOnError)
	baseDir := fs.String("base", "", "base session directory")
	newDir := fs.String("dir", "", "new session directory")
	folds := fs.Int("folds", 0, "new fold count")
	metric := fs.String("metric", "", "new metric")
	lambdas := fs.String("lambdas", "", "new lambda list")
	ratios := fs.String("ratios", "", "new l1 ratio list")
	encodings := fs.String("encoding", "", "comma separated col=encoding")
	if err := fs.Parse(args); err != nil {
		return err
	}
	base, err := experiment.OpenSession(*baseDir, experiment.Hooks{})
	if err != nil {
		return err
	}
	cfg := base.Cfg
	if *folds > 0 {
		cfg.KFolds = *folds
	}
	if *metric != "" {
		cfg.Metric = *metric
	}
	if *lambdas != "" {
		cfg.Lambdas, err = parseFloatList(*lambdas)
		if err != nil {
			return err
		}
	}
	if *ratios != "" {
		cfg.L1Ratios, err = parseFloatList(*ratios)
		if err != nil {
			return err
		}
	}
	if *encodings != "" {
		for _, item := range strings.Split(*encodings, ",") {
			kv := strings.SplitN(item, "=", 2)
			if len(kv) != 2 {
				return fmt.Errorf("invalid encoding %q", item)
			}
			cfg.Encoding[kv[0]] = kv[1]
		}
	}
	cfg, err = cfg.Normalize()
	if err != nil {
		return err
	}
	next, rep, plan, err := experiment.RunIncremental(context.Background(), base, cfg, *newDir, experiment.Hooks{})
	if err != nil {
		return err
	}
	defer next.Close()
	return printJSON(map[string]any{"change_plan": plan, "report": rep})
}

func installStopSignal(sess *experiment.Session) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		_ = sess.Store.RequestStop()
	}()
}

func parseFloatList(s string) ([]float64, error) {
	parts := strings.Split(s, ",")
	out := make([]float64, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
