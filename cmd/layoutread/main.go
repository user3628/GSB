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
	"path/filepath"
	"syscall"
	"time"

	"layoutread/internal/demo"
	"layoutread/internal/httpapi"
	"layoutread/internal/model"
	"layoutread/internal/service"
	"layoutread/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		payload, _ := json.Marshal(model.AsError(err))
		fmt.Fprintln(os.Stderr, string(payload))
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}
	switch args[0] {
	case "generate":
		return runGenerate(args[1:])
	case "demo":
		return runDemo(args[1:])
	case "analyze", "resume":
		return runAnalyze(args[1:])
	case "serve":
		return runServe(args[1:])
	case "status":
		return runStatus(args[1:])
	case "query":
		return runQuery(args[1:])
	case "export":
		return runExport(args[1:])
	case "stop":
		return runStop(args[1:])
	case "check":
		return runCheck(args[1:])
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		return model.NewError(model.CodeInvalidRequest, "unknown command: "+args[0], "")
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `layoutread — deterministic document layout reading-order engine

Commands:
  generate  [-out file]                      emit the seeded demo bundle JSON
  demo      [-out file]                      run the full demo against a temporary SQLite database
  analyze   -db file (-demo | -input file)   run or incrementally resume all documents
              [-stop-file file] [-stage-delay-ms n] [-out file]
  resume    same flags as analyze
  serve     -db file [-addr :8080] [-demo]    start HTTP console and JSON API
  status    -db file                          show checkpoint status and version fingerprints
  query     -db file -doc id [-scope blocks|confidence|columns]
              [-budget-ms n] [-query-id id]
  export    -db file [-out file]              export deterministic results from committed facts
  stop      -db file [-stop-file file]         create a cooperative stop marker file
  check     -db file                          verify checkpoint snapshot integrity

All JSON output is canonical Go encoding/json output with HTML escaping disabled.`)
}

func runGenerate(args []string) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := flags.String("out", "", "output file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	bundle := demo.Generate()
	return writeCanonical(*out, bundle)
}

type demoOutput struct {
	Seed   int64                `json:"seed"`
	Run    service.RunReport    `json:"run"`
	Export service.ExportReport `json:"export"`
}

func runDemo(args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	out := flags.String("out", "", "output file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	tempDirectory, err := os.MkdirTemp("", "layoutread-demo-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDirectory)
	st, err := store.Open(filepath.Join(tempDirectory, "demo.sqlite"))
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st)
	ctx := context.Background()
	if err := svc.Import(ctx, demo.Generate()); err != nil {
		return err
	}
	report, err := svc.Run(ctx, service.RunOptions{})
	if err != nil {
		return err
	}
	export, err := svc.Export(ctx)
	if err != nil {
		return err
	}
	return writeCanonical(*out, demoOutput{Seed: demo.GenerationSeed, Run: report, Export: export})
}

func runAnalyze(args []string) error {
	flags := flag.NewFlagSet("analyze", flag.ContinueOnError)
	dbPath := flags.String("db", "layoutread.sqlite", "SQLite database path")
	inputPath := flags.String("input", "", "layout bundle JSON file")
	useDemo := flags.Bool("demo", false, "use the built-in deterministic bundle")
	stopFile := flags.String("stop-file", "", "optional cooperative stop marker file")
	stageDelayMS := flags.Int("stage-delay-ms", 0, "optional per-stage delay for testing stop semantics")
	out := flags.String("out", "", "output file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *stageDelayMS < 0 {
		return model.NewError(model.CodeInvalidRequest, "-stage-delay-ms must be non-negative", "")
	}
	bundle, err := loadBundle(*inputPath, *useDemo)
	if err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st)
	ctx := context.Background()
	if err := svc.Import(ctx, bundle); err != nil {
		return err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		<-signals
		svc.RequestStop()
	}()
	report, err := svc.Run(ctx, service.RunOptions{StopFilePath: *stopFile, StageDelay: time.Duration(*stageDelayMS) * time.Millisecond})
	if err != nil {
		return err
	}
	return writeCanonical(*out, report)
}

func runServe(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	dbPath := flags.String("db", "layoutread.sqlite", "SQLite database path")
	addr := flags.String("addr", "127.0.0.1:8080", "HTTP listen address")
	useDemo := flags.Bool("demo", false, "import the built-in demo bundle before serving")
	if err := flags.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st)
	ctx := context.Background()
	if err := svc.LoadPersistedStrategy(ctx); err != nil {
		return err
	}
	if *useDemo {
		if err := svc.Import(ctx, demo.Generate()); err != nil {
			return err
		}
	}
	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(svc).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		<-signals
		svc.RequestStop()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()
	fmt.Fprintf(os.Stderr, "layoutread listening on http://%s (db=%s)\n", *addr, *dbPath)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runStatus(args []string) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	dbPath := flags.String("db", "layoutread.sqlite", "SQLite database path")
	out := flags.String("out", "", "output file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st)
	ctx := context.Background()
	if err := svc.LoadPersistedStrategy(ctx); err != nil {
		return err
	}
	report, err := svc.Status(ctx)
	if err != nil {
		return err
	}
	return writeCanonical(*out, report)
}

func runQuery(args []string) error {
	flags := flag.NewFlagSet("query", flag.ContinueOnError)
	dbPath := flags.String("db", "layoutread.sqlite", "SQLite database path")
	docID := flags.String("doc", "", "document id")
	scope := flags.String("scope", "blocks", "blocks, confidence or columns")
	budgetMS := flags.Int("budget-ms", -1, "latency budget in milliseconds (default: strategy value)")
	queryID := flags.String("query-id", "", "caller-provided query id")
	out := flags.String("out", "", "output file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *docID == "" {
		return model.NewError(model.CodeInvalidRequest, "-doc is required", "")
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st)
	ctx := context.Background()
	if err := svc.LoadPersistedStrategy(ctx); err != nil {
		return err
	}
	request := service.QueryRequest{DocID: *docID, Scope: *scope, QueryID: *queryID}
	if *budgetMS >= 0 {
		request.BudgetMS = *budgetMS
		request.HasBudget = true
	}
	response, err := svc.Query(ctx, request)
	if err != nil {
		return err
	}
	return writeCanonical(*out, response)
}

func runExport(args []string) error {
	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	dbPath := flags.String("db", "layoutread.sqlite", "SQLite database path")
	out := flags.String("out", "", "output file (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st)
	ctx := context.Background()
	if err := svc.LoadPersistedStrategy(ctx); err != nil {
		return err
	}
	report, err := svc.Export(ctx)
	if err != nil {
		return err
	}
	return writeCanonical(*out, report)
}

func runStop(args []string) error {
	flags := flag.NewFlagSet("stop", flag.ContinueOnError)
	dbPath := flags.String("db", "layoutread.sqlite", "SQLite database path")
	stopFile := flags.String("stop-file", "", "stop marker path (default <db>.stop)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	path := *stopFile
	if path == "" {
		path = *dbPath + ".stop"
	}
	if err := os.WriteFile(path, []byte("stop\n"), 0o600); err != nil {
		return err
	}
	return writeCanonical("", map[string]string{"stop_file": path, "status": "requested"})
}

func runCheck(args []string) error {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	dbPath := flags.String("db", "layoutread.sqlite", "SQLite database path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.VerifySnapshot(context.Background()); err != nil {
		return err
	}
	return writeCanonical("", map[string]string{"status": "ok"})
}

func loadBundle(path string, useDemo bool) (model.Bundle, error) {
	if path == "" && !useDemo {
		return model.Bundle{}, model.NewError(model.CodeInvalidRequest, "provide -input or -demo", "")
	}
	if path == "" {
		return demo.Generate(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return model.Bundle{}, err
	}
	var bundle model.Bundle
	if err := json.Unmarshal(data, &bundle); err != nil {
		return model.Bundle{}, model.NewError(model.CodeInvalidRequest, "cannot decode bundle: "+err.Error(), "")
	}
	return bundle, nil
}

func writeCanonical(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if path == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
