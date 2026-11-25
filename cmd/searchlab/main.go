package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"searchlab/internal/apperr"
	"searchlab/internal/checkpoint"
	"searchlab/internal/config"
	"searchlab/internal/engine"
	"searchlab/internal/generator"
	"searchlab/internal/httpapi"
	"searchlab/internal/model"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = runGenerate(os.Args[2:])
	case "import":
		err = runImport(os.Args[2:])
	case "build":
		err = runBuild(os.Args[2:])
	case "resume":
		err = runBuild(os.Args[2:])
	case "query":
		err = runQuery(os.Args[2:])
	case "status":
		err = runStatus(os.Args[2:])
	case "diagnostics":
		err = runDiagnostics(os.Args[2:])
	case "checkpoint-publish":
		err = runCheckpointPublish(os.Args[2:])
	case "serve":
		err = runServe(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "version":
		fmt.Println("searchlab format=searchlab-demo/v1 go=1.22+")
		return
	case "help", "-h", "--help":
		usage()
		return
	default:
		err = apperr.E(apperr.CodeInput, "unknown command "+os.Args[1], nil)
	}
	if err != nil {
		if business := apperr.As(err); business != nil {
			fmt.Fprintln(os.Stderr, string(business.JSONBytes()))
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`searchlab - deterministic compressed inverted-index experiment

Commands:
  generate --out demo [--seed 42]
  import   --config demo/config.yaml --shards demo/shards
  build    --config demo/config.yaml --shards demo/shards --db demo/searchlab.db [--force]
  resume   --config demo/config.yaml --shards demo/shards --db demo/searchlab.db
  query    --db demo/searchlab.db --terms "Go,语言" --mode AND [--query-id demo-and]
  status   --db demo/searchlab.db
  diagnostics --db demo/searchlab.db
  serve    --db demo/searchlab.db --config demo/config.yaml --shards demo/shards --addr 127.0.0.1:8080
  demo     --out demo [--seed 42]
  checkpoint-publish --db DB --lock DB.lock --stage tokenize --seq 1 --pipeline ID --input-digest D --config-version V --payload FILE
`)
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", "demo", "output directory")
	seed := fs.Int64("seed", 42, "fixed generator seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	summary, err := generator.Generate(*out, *seed)
	if err != nil {
		return err
	}
	payload, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(payload))
	return nil
}

func runImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "YAML config path")
	shardsDir := fs.String("shards", "", "JSONL shard directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath == "" || *shardsDir == "" {
		return apperr.E(apperr.CodeInput, "--config and --shards are required", nil)
	}
	cfg, _, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	corpus, err := model.LoadShards(*shardsDir)
	if err != nil {
		return err
	}
	response := struct {
		Status        string   `json:"status"`
		Documents     int      `json:"documents"`
		ShardFiles    []string `json:"shard_files"`
		ShardDigest   string   `json:"shard_digest"`
		ConfigVersion string   `json:"config_version"`
		ConfigDigest  string   `json:"config_digest"`
	}{Status: "imported", Documents: len(corpus.Docs), ShardFiles: corpus.ShardFiles, ShardDigest: corpus.ShardDigest, ConfigVersion: cfg.Version, ConfigDigest: cfg.Digest()}
	payload, _ := json.MarshalIndent(response, "", "  ")
	fmt.Println(string(payload))
	return nil
}

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "YAML config path")
	shardsDir := fs.String("shards", "", "JSONL shard directory")
	dbPath := fs.String("db", "", "SQLite database path")
	lockPath := fs.String("lock", "", "checkpoint lock file (default DB.lock)")
	force := fs.Bool("force", false, "ignore reusable artifacts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath == "" || *shardsDir == "" || *dbPath == "" {
		return apperr.E(apperr.CodeInput, "--config, --shards and --db are required", nil)
	}
	cfg, _, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	corpus, err := model.LoadShards(*shardsDir)
	if err != nil {
		return err
	}
	store, err := checkpoint.Open(*dbPath, lockOrDefault(*lockPath, *dbPath))
	if err != nil {
		return err
	}
	defer store.Close()
	report, err := engine.Build(context.Background(), cfg, corpus, store, *force)
	if err != nil {
		return err
	}
	fmt.Println(engine.FormatReport(report))
	return nil
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	lockPath := fs.String("lock", "", "checkpoint lock file")
	terms := fs.String("terms", "", "comma-separated raw query terms")
	mode := fs.String("mode", "OR", "AND or OR")
	queryID := fs.String("query-id", "", "stable query identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return apperr.E(apperr.CodeInput, "--db is required", nil)
	}
	store, err := checkpoint.Open(*dbPath, lockOrDefault(*lockPath, *dbPath))
	if err != nil {
		return err
	}
	defer store.Close()
	runtime, err := engine.LoadRuntime(context.Background(), store)
	if err != nil {
		return err
	}
	query := engine.NewQueryEngine(runtime)
	var rawTerms []string
	if strings.TrimSpace(*terms) != "" {
		rawTerms = splitTerms(*terms)
	}
	result, payload, cacheHit, err := query.Execute(context.Background(), engine.QueryRequest{QueryID: *queryID, Terms: rawTerms, Mode: *mode})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "cache_hit=%v query_id=%s status=%s hits=%d\n", cacheHit, result.QueryID, result.Status, len(result.Hits))
	fmt.Println(string(payload))
	return nil
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	lockPath := fs.String("lock", "", "checkpoint lock file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return apperr.E(apperr.CodeInput, "--db is required", nil)
	}
	store, err := checkpoint.Open(*dbPath, lockOrDefault(*lockPath, *dbPath))
	if err != nil {
		return err
	}
	defer store.Close()
	records, err := store.Records(context.Background())
	if err != nil {
		return err
	}
	current, _ := store.CurrentSeq(context.Background())
	status := map[string]any{"current_seq": current, "records": records}
	for _, key := range []string{"config_digest", "config_version", "shard_digest", "pipeline_id"} {
		if value, err := store.GetMeta(context.Background(), key); err == nil {
			status[key] = string(value)
		}
	}
	if version, err := store.IndexVersion(context.Background()); err == nil {
		status["index_version"] = version
	}
	payload, _ := json.MarshalIndent(status, "", "  ")
	fmt.Println(string(payload))
	return nil
}

func runDiagnostics(args []string) error {
	fs := flag.NewFlagSet("diagnostics", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	lockPath := fs.String("lock", "", "checkpoint lock file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return apperr.E(apperr.CodeInput, "--db is required", nil)
	}
	store, err := checkpoint.Open(*dbPath, lockOrDefault(*lockPath, *dbPath))
	if err != nil {
		return err
	}
	defer store.Close()
	runtime, err := engine.LoadRuntime(context.Background(), store)
	if err != nil {
		return err
	}
	payload, err := json.MarshalIndent(runtime.Diagnostics, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(payload))
	return nil
}

func runCheckpointPublish(args []string) error {
	fs := flag.NewFlagSet("checkpoint-publish", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	lockPath := fs.String("lock", "", "checkpoint lock file")
	stage := fs.String("stage", "", "stage name")
	seq := fs.Int64("seq", 0, "sequence number")
	pipeline := fs.String("pipeline", "", "pipeline identifier")
	inputDigest := fs.String("input-digest", "", "stage input digest")
	configVersion := fs.String("config-version", "", "config digest/version")
	payloadPath := fs.String("payload", "", "artifact payload file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" || *stage == "" || *seq <= 0 || *payloadPath == "" {
		return apperr.E(apperr.CodeInput, "--db, --stage, --seq and --payload are required", nil)
	}
	payload, err := os.ReadFile(*payloadPath)
	if err != nil {
		return err
	}
	store, err := checkpoint.Open(*dbPath, lockOrDefault(*lockPath, *dbPath))
	if err != nil {
		return err
	}
	defer store.Close()
	record, err := store.Publish(context.Background(), checkpoint.PublishRequest{
		Seq:           *seq,
		Stage:         *stage,
		ShardDigest:   *inputDigest,
		InputDigest:   *inputDigest,
		ConfigVersion: *configVersion,
		PipelineID:    *pipeline,
		Payload:       payload,
	})
	if err != nil {
		return err
	}
	payloadOut, _ := json.Marshal(record)
	fmt.Println(string(payloadOut))
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dbPath := fs.String("db", "", "SQLite database path")
	lockPath := fs.String("lock", "", "checkpoint lock file")
	cfgPath := fs.String("config", "", "optional YAML config to import")
	shardsDir := fs.String("shards", "", "optional JSONL shard directory to import")
	addr := fs.String("addr", "127.0.0.1:8080", "loopback listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return apperr.E(apperr.CodeInput, "--db is required", nil)
	}
	app, err := httpapi.New(httpapi.Options{
		DBPath: *dbPath, LockPath: lockOrDefault(*lockPath, *dbPath),
		ConfigPath: *cfgPath, ShardsDir: *shardsDir, Addr: *addr,
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return app.Run(ctx)
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	out := fs.String("out", "demo", "demo output directory")
	seed := fs.Int64("seed", 42, "fixed generator seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return runDemoWorkflow(*out, *seed)
}

func splitTerms(raw string) []string {
	parts := strings.Split(raw, ",")
	terms := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			terms = append(terms, trimmed)
		}
	}
	return terms
}

func lockOrDefault(lockPath, dbPath string) string {
	if lockPath != "" {
		return lockPath
	}
	return dbPath + ".lock"
}
