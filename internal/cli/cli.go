package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"featureplatform/internal/configx"
	"featureplatform/internal/demo"
	"featureplatform/internal/engine"
	"featureplatform/internal/httpapi"
	"featureplatform/internal/model"
	"featureplatform/internal/samplegen"
	"featureplatform/internal/store"
)

func Main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = cmdGenerate(os.Args[2:])
	case "import":
		err = cmdImport(os.Args[2:])
	case "config":
		err = cmdConfig(os.Args[2:])
	case "increment":
		err = cmdIncrement(os.Args[2:])
	case "migrate", "recover":
		err = cmdMigrate(os.Args[2:])
	case "status":
		err = cmdStatus(os.Args[2:])
	case "read":
		err = cmdRead(os.Args[2:])
	case "summary":
		err = cmdSummary(os.Args[2:])
	case "rejections":
		err = cmdRejections(os.Args[2:])
	case "invalidations":
		err = cmdInvalidations(os.Args[2:])
	case "export":
		err = cmdExport(os.Args[2:])
	case "serve":
		err = cmdServe(os.Args[2:])
	case "stop":
		err = cmdStop(os.Args[2:])
	case "demo":
		err = cmdDemo(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		var coded *model.CodedError
		if errors.As(err, &coded) {
			printJSON(coded)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `featurectl - deterministic feature migration platform

Commands:
  generate --dir data
  import --db migration.db --shards shards.json [--batch id]
  config put --db migration.db --file config.json
  increment --db migration.db --file fragment.json
  migrate|recover --db migration.db [--budget N] [--hang-after-writes N --ready-file path]
  status --db migration.db [--units]
  read --db migration.db --sample ID --version N
  summary --db migration.db
  rejections --db migration.db
  invalidations --db migration.db
  export --db migration.db --out records.json
  serve --db migration.db [--addr 127.0.0.1:8080]
  stop [--url http://127.0.0.1:8080]
  demo [--dir .demo]
`)
}

func printJSON(v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(v)
}

func writeJSONFile(path string, v any) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	dir := fs.String("dir", "demo-data", "output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	shards := samplegen.GenerateShards()
	cfg := samplegen.DefaultConfig()
	fragment := samplegen.SentimentV2Fragment()
	if err := writeJSONFile(filepath.Join(*dir, "shards.json"), map[string]any{"shards": shards}); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(*dir, "config.json"), cfg); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(*dir, "increment.json"), fragment); err != nil {
		return err
	}
	fmt.Printf("generated %d task shards in %s\n", len(shards), *dir)
	return nil
}

func cmdImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "sqlite database path")
	shardsPath := fs.String("shards", "shards.json", "shards JSON")
	batch := fs.String("batch", "cli-import", "batch id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	shards, err := loadShards(*shardsPath)
	if err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	result, err := eng.Store.ImportShards(context.Background(), shards, *batch)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func loadShards(path string) ([]model.Shard, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var wrapped struct {
		Shards []model.Shard `json:"shards"`
	}
	if err := json.Unmarshal(b, &wrapped); err == nil && len(wrapped.Shards) > 0 {
		return wrapped.Shards, nil
	}
	var shards []model.Shard
	if err := json.Unmarshal(b, &shards); err != nil {
		return nil, fmt.Errorf("decode shards: %w", err)
	}
	return shards, nil
}

func cmdConfig(args []string) error {
	if len(args) == 0 || args[0] != "put" {
		return fmt.Errorf("usage: config put --db DB --file config.json")
	}
	fs := flag.NewFlagSet("config put", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	file := fs.String("file", "config.json", "config snapshot")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	var cfg configx.Config
	if err := readJSON(*file, &cfg); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	ctx := context.Background()
	current, _, err := eng.Store.CurrentVersion(ctx)
	if err != nil {
		return err
	}
	var fp string
	if current == 0 {
		fp, err = eng.CommitInitialConfig(ctx, cfg)
	} else {
		fp, err = eng.Store.CommitConfig(ctx, store.ConfigCommit{Config: cfg})
	}
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "config_version": cfg.ConfigVersion, "fingerprint": fp})
	return nil
}

func cmdIncrement(args []string) error {
	fs := flag.NewFlagSet("increment", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	file := fs.String("file", "increment.json", "fragment")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var fragment configx.Fragment
	if err := readJSON(*file, &fragment); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	next, plan, err := eng.SubmitFragment(context.Background(), fragment)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "config_version": next.ConfigVersion, "plan": plan})
	return nil
}

func cmdMigrate(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	budget := fs.Int("budget", 0, "sample budget for this round")
	stage := fs.String("stage", "record", "target stage: normalize, label, feature_map, coverage, record")
	hang := fs.Int("hang-after-writes", 0, "test hook: hang after N checkpoint writes")
	readyFile := fs.String("ready-file", "", "write ready file when hang point reached")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	report, err := eng.RunMigration(context.Background(), engine.RunOptions{
		Budget: *budget, TargetStage: *stage, HangAfterCheckpointWrites: *hang, ReadyFile: *readyFile,
	})
	if err != nil {
		return err
	}
	printJSON(report)
	return nil
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	units := fs.Bool("units", false, "include affected units")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	st, err := eng.Status(context.Background(), *units)
	if err != nil {
		return err
	}
	printJSON(st)
	return nil
}

func cmdRead(args []string) error {
	fs := flag.NewFlagSet("read", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	sample := fs.String("sample", "", "sample id")
	version := fs.Int64("version", -1, "observed config version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *sample == "" || *version < 0 {
		return fmt.Errorf("--sample and --version are required")
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	rec, coded := eng.Store.ReadAdjudicated(context.Background(), *sample, *version)
	if coded != nil {
		return coded
	}
	printJSON(rec)
	return nil
}

func cmdSummary(args []string) error {
	fs := flag.NewFlagSet("summary", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	summary, err := eng.Summary(context.Background())
	if err != nil {
		return err
	}
	printJSON(summary)
	return nil
}

func cmdRejections(args []string) error {
	fs := flag.NewFlagSet("rejections", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	items, err := eng.Rejections(context.Background())
	if err != nil {
		return err
	}
	printJSON(items)
	return nil
}

func cmdInvalidations(args []string) error {
	fs := flag.NewFlagSet("invalidations", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	items, err := eng.Store.ListInvalidations(context.Background())
	if err != nil {
		return err
	}
	printJSON(items)
	return nil
}

func cmdExport(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	out := fs.String("out", "", "output file; empty means stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	b, digest, n, err := eng.ExportCanonical(context.Background())
	if err != nil {
		return err
	}
	if *out != "" {
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "records=%d digest=%s out=%s\n", n, digest, *out)
		return nil
	}
	_, err = os.Stdout.Write(append(b, '\n'))
	return err
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	db := fs.String("db", "featureplatform.db", "db")
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	eng, err := engine.Open(*db)
	if err != nil {
		return err
	}
	defer eng.Close()
	apiServer := httpapi.New(eng)
	httpServer := &http.Server{Addr: *addr, Handler: apiServer.Handler()}
	apiServer.OnStop = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	fmt.Printf("SERVER_READY %s\n", ln.Addr().String())
	if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func cmdStop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	baseURL := fs.String("url", "http://127.0.0.1:8080", "server base url")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(*baseURL+"/api/v1/admin/stop", "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(os.Stdout, resp.Body)
	return nil
}

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	dir := fs.String("dir", ".demo", "demo working directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	report, err := demo.Run(*dir, exe)
	if err != nil {
		return err
	}
	printJSON(report)
	fmt.Fprintf(os.Stderr, "FIXED_SUMMARY=%s\n", report.FixedSummaryDigest)
	return nil
}
