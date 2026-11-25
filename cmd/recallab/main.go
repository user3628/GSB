package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"localsearch/multirecall/internal/api"
	"localsearch/multirecall/internal/engine"
	"localsearch/multirecall/internal/model"
	"localsearch/multirecall/internal/sample"
	"localsearch/multirecall/internal/store"
	"localsearch/multirecall/internal/worker"
)

const version = api.Version

func main() {
	if worker.IsWorkerProcess() {
		os.Exit(worker.Main())
	}
	if err := dispatch(os.Args[1:]); err != nil {
		appErr := model.AsAppError(err)
		data, _ := json.MarshalIndent(map[string]any{"ok": false, "error": appErr}, "", "  ")
		fmt.Fprintln(os.Stderr, string(data))
		os.Exit(1)
	}
}

func dispatch(args []string) error {
	if len(args) == 0 {
		return model.NewError(model.CodeInvalidConfig, "usage: recallab <generate|run|summary|checkpoints|serve|version>", "")
	}
	switch args[0] {
	case "generate":
		return commandGenerate(args[1:])
	case "run":
		return commandRun(args[1:])
	case "summary":
		return commandSummary(args[1:])
	case "checkpoints":
		return commandCheckpoints(args[1:])
	case "serve":
		return commandServe(args[1:])
	case "version":
		fmt.Println(version)
		return nil
	default:
		return model.NewError(model.CodeInvalidConfig, "unknown command: "+args[0], "")
	}
}

func commandGenerate(args []string) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := flags.String("out", "data", "output directory")
	seed := flags.Int64("seed", 20261125, "deterministic sample seed")
	items := flags.Int("items", 800, "number of items")
	queries := flags.Int("queries", 120, "number of queries")
	vectorDim := flags.Int("vector-dim", 24, "latent vector dimension")
	strategy := flags.String("strategy", "weighted", "fusion strategy: weighted or rrf")
	if err := flags.Parse(args); err != nil {
		return err
	}
	manifest, err := sample.Generate(sample.GenerateOptions{
		OutDir: *out, Seed: *seed, ItemCount: *items, QueryCount: *queries,
		VectorDim: *vectorDim, Strategy: *strategy,
	})
	if err != nil {
		return err
	}
	return writeJSON(*out+string(filepath.Separator)+"generate-result.json", manifest, true)
}

func commandRun(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	dataDir := flags.String("data", "data", "sample data directory")
	dbPath := flags.String("db", "", "SQLite database path (default: <data>/experiment.db)")
	outPath := flags.String("out", "", "result JSON path (default: stdout)")
	configPath := flags.String("config", "", "optional config override JSON")
	workers := flags.Int("workers", 0, "worker process count override")
	concurrency := flags.Int("concurrency", 0, "max concurrent queries override")
	mode := flags.String("mode", "process", "execution mode: process or inline")
	_ = flags.Bool("verify-incremental", true, "deprecated: incremental verification is enabled by default")
	skipVerify := flags.Bool("skip-incremental-verification", false, "skip the mandatory from-scratch comparison for reused checkpoints")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		*dbPath = filepath.Join(*dataDir, "experiment.db")
	}
	var configOverride *model.Config
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return model.NewError(model.CodeDataLoad, "read config override: "+err.Error(), "")
		}
		var config model.Config
		if err := json.Unmarshal(data, &config); err != nil {
			return model.NewError(model.CodeDataLoad, "parse config override: "+err.Error(), "")
		}
		configOverride = &config
	}
	stop := make(chan struct{})
	stopSignals(stop)
	output, err := (&engine.Engine{}).Run(context.Background(), engine.Request{
		DataDir: *dataDir, DBPath: *dbPath, Config: configOverride,
		Workers: *workers, Concurrency: *concurrency, Mode: *mode,
		SkipIncrementalVerification: *skipVerify, Stop: stop,
	})
	if err != nil {
		return err
	}
	if *outPath == "" {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	return writeJSON(*outPath, output, true)
}

func commandSummary(args []string) error {
	flags := flag.NewFlagSet("summary", flag.ContinueOnError)
	dbPath := flags.String("db", "data/experiment.db", "SQLite database path")
	runID := flags.String("run-id", "", "run id (default: latest run)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	var record store.RunRecord
	if *runID == "" {
		record, err = st.LatestRun()
	} else {
		record, err = st.GetRun(*runID)
	}
	if err != nil {
		return err
	}
	if record.SummaryJSON == "" {
		return model.NewError(model.CodeDataLoad, "run has no saved summary", record.RunID)
	}
	var summary any
	if err := json.Unmarshal([]byte(record.SummaryJSON), &summary); err != nil {
		return err
	}
	return writeJSON("", map[string]any{
		"run_id": record.RunID, "status": record.Status,
		"summary_digest": record.SummaryDigest, "summary": summary,
	}, true)
}

func commandCheckpoints(args []string) error {
	flags := flag.NewFlagSet("checkpoints", flag.ContinueOnError)
	dbPath := flags.String("db", "data/experiment.db", "SQLite database path")
	queryID := flags.String("query-id", "", "query id")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *queryID == "" {
		return model.NewError(model.CodeInvalidConfig, "--query-id is required", "")
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	checkpoints, err := st.ListCheckpoints(context.Background(), *queryID)
	if err != nil {
		return err
	}
	for i := range checkpoints {
		checkpoints[i].Payload = nil
	}
	return writeJSON("", checkpoints, true)
}

func commandServe(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := flags.String("addr", "127.0.0.1:18761", "local bind address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(*addr)
	if err != nil {
		return model.NewError(model.CodeInvalidConfig, "invalid listen address: "+err.Error(), "")
	}
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return model.NewError(model.CodeInvalidConfig, "HTTP server must bind to a loopback address", "")
	}
	server := &http.Server{
		Addr:              *addr,
		Handler:           api.NewServer(&engine.Engine{}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Fprintf(os.Stderr, "recallab HTTP listening on http://%s\n", *addr)
	return server.ListenAndServe()
}

func stopSignals(stop chan struct{}) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		close(stop)
		fmt.Fprintln(os.Stderr, "stop requested; finishing current query unit")
		<-signals
		os.Exit(130)
	}()
}

func writeJSON(path string, value any, indent bool) error {
	data, err := marshalJSON(value, indent)
	if err != nil {
		return err
	}
	if path == "" {
		_, err = os.Stdout.Write(append(data, '\n'))
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".recallab-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(path)
		if retryErr := os.Rename(tmpPath, path); retryErr != nil {
			_ = os.Remove(tmpPath)
			return retryErr
		}
	}
	return nil
}

func marshalJSON(value any, indent bool) ([]byte, error) {
	buffer := &strings.Builder{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	if indent {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(buffer.String(), "\n")), nil
}
