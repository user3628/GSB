package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.com/prompt-prefix-cache"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	command := os.Args[1]
	args := os.Args[2:]
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var err error
	switch command {
	case "demo":
		err = runDemo(ctx, args)
	case "serve":
		err = runServe(ctx, args)
	case "init":
		err = runInit(ctx, args)
	case "status":
		err = runStatus(ctx, args)
	case "hit":
		err = runHit(ctx, args)
	case "patch":
		err = runPatch(ctx, args)
	case "policy":
		err = runPolicy(ctx, args)
	case "requests":
		err = runRequests(ctx, args)
	case "invalidations":
		err = runInvalidations(ctx, args)
	case "evictions":
		err = runEvictions(ctx, args)
	case "snapshot":
		err = runSnapshot(ctx, args)
	case "demo-data":
		err = runDemoData(args)
	case "stop":
		err = runStop(ctx, args)
	case "recover":
		err = runRecover(ctx, args)
	case "help", "--help", "-h":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		var typed *prefixcache.Error
		if errors.As(err, &typed) {
			printJSON(map[string]any{"ok": false, "error": typed})
		} else {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`prefixcache - deterministic prompt-prefix cache component demo

Commands:
  demo          Run the complete deterministic local scenario
  serve         Start the local HTTP API
  init          Initialize version 1 from generated data or JSON files
  status        Show current version, stage fingerprints, and occupancy
  hit           Submit a version-observed hit query
  patch         Apply exactly one incremental policy field
  policy        Submit a complete policy snapshot
  requests      Import or upsert request records
  invalidations Show invalidations for a version
  evictions     Show budget/limit/conflict eviction reasons
  snapshot      Show one immutable version snapshot
  demo-data     Print fixed-seed generated JSON
  stop          POST /v1/shutdown to a running local server
  recover       Reopen the database and verify the committed head

Run a command with --help for flags.
`)
}

func runDemo(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache-demo.db", "SQLite database path")
	seed := flags.Int64("seed", prefixcache.DefaultSeed, "fixed generator seed")
	fresh := flags.Bool("fresh", true, "remove an existing demo database before running")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *fresh {
		removeDatabase(*dbPath)
	}
	report, err := prefixcache.RunDemo(ctx, *dbPath, *seed)
	if err != nil {
		return err
	}
	printJSON(report)
	return nil
}

func runServe(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	address := flags.String("addr", "127.0.0.1:8080", "loopback listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	server := prefixcache.NewHTTPServer(engine)
	listener, err := server.Listen(*address)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "prefixcache listening on http://%s db=%s\n", listener.Addr().String(), engine.DatabasePath())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return err
		}
		select {
		case err := <-serveDone:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("HTTP server did not stop after graceful shutdown")
		}
	case err := <-serveDone:
		return err
	}
}

func runInit(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	seed := flags.Int64("seed", prefixcache.DefaultSeed, "fixed generator seed")
	policyFile := flags.String("policy", "", "optional policy JSON file")
	requestsFile := flags.String("requests", "", "optional requests JSON array file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	dataset := prefixcache.GenerateDataset(*seed)
	policy := dataset.Policy
	requests := dataset.Requests
	if *policyFile != "" {
		if err := decodeFile(*policyFile, &policy); err != nil {
			return err
		}
	}
	if *requestsFile != "" {
		if err := decodeFile(*requestsFile, &requests); err != nil {
			return err
		}
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.Initialize(ctx, policy, requests)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func runStatus(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	status, err := engine.Status(ctx)
	if err != nil {
		return err
	}
	printJSON(status)
	return nil
}

func runHit(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("hit", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	version := flags.Int64("version", 0, "observed effective version")
	requestID := flags.String("request-id", "", "imported request id")
	requestFile := flags.String("request", "", "optional inline request JSON file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	query := prefixcache.HitQuery{ObservedVersion: *version, RequestID: *requestID}
	if *requestFile != "" {
		var request prefixcache.Request
		if err := decodeFile(*requestFile, &request); err != nil {
			return err
		}
		query.Request = &request
	}
	if query.Request == nil && query.RequestID == "" {
		return errors.New("--request-id or --request is required")
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.Hit(ctx, query)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func runPatch(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("patch", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	expected := flags.Int64("expected", 0, "expected current version")
	field := flags.String("field", "", "granularity|min_prefix_len|merge_threshold|max_prefixes_per_request")
	value := flags.String("value", "", "new value")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var patch prefixcache.PolicyPatch
	switch *field {
	case "granularity":
		parsed, err := strconv.Atoi(*value)
		if err != nil {
			return err
		}
		patch.Granularity = &parsed
	case "min_prefix_len":
		parsed, err := strconv.Atoi(*value)
		if err != nil {
			return err
		}
		patch.MinPrefixLen = &parsed
	case "merge_threshold":
		parsed, err := strconv.ParseFloat(*value, 64)
		if err != nil {
			return err
		}
		patch.MergeThreshold = &parsed
	case "max_prefixes_per_request":
		parsed, err := strconv.Atoi(*value)
		if err != nil {
			return err
		}
		patch.MaxPrefixesPerRequest = &parsed
	default:
		return errors.New("--field and --value are required and field must be one of the four incremental fields")
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.ApplyPatch(ctx, *expected, patch)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func runPolicy(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("policy", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	file := flags.String("file", "", "PolicySubmission JSON file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("--file is required")
	}
	var submission prefixcache.PolicySubmission
	if err := decodeFile(*file, &submission); err != nil {
		return err
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.SubmitPolicy(ctx, submission)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

type requestsFile struct {
	Requests   []prefixcache.Request `json:"requests"`
	ReplaceAll bool                  `json:"replace_all"`
}

func runRequests(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("requests", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	expected := flags.Int64("expected", 0, "expected current version")
	file := flags.String("file", "", "requests JSON array or {requests,replace_all} file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("--file is required")
	}
	payload, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var requests []prefixcache.Request
	replaceAll := false
	if err := json.Unmarshal(payload, &requests); err != nil {
		var wrapper requestsFile
		if err := json.Unmarshal(payload, &wrapper); err != nil {
			return fmt.Errorf("decode requests file: %w", err)
		}
		requests = wrapper.Requests
		replaceAll = wrapper.ReplaceAll
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.ImportRequests(ctx, *expected, requests, replaceAll)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func runInvalidations(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("invalidations", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	version := flags.Int64("version", 0, "version; 0 means current")
	requestID := flags.String("request-id", "", "optional request filter")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.Invalidations(ctx, *version, *requestID)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func runEvictions(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("evictions", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	version := flags.Int64("version", 0, "version; 0 means current")
	requestID := flags.String("request-id", "", "optional request filter")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.Evictions(ctx, *version, *requestID)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func runSnapshot(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	version := flags.Int64("version", 0, "version; 0 means current")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	result, err := engine.Snapshot(ctx, *version)
	if err != nil {
		return err
	}
	printJSON(result)
	return nil
}

func runDemoData(args []string) error {
	flags := flag.NewFlagSet("demo-data", flag.ContinueOnError)
	seed := flags.Int64("seed", prefixcache.DefaultSeed, "fixed generator seed")
	if err := flags.Parse(args); err != nil {
		return err
	}
	printJSON(prefixcache.GenerateDataset(*seed))
	return nil
}

func runStop(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("stop", flag.ContinueOnError)
	url := flags.String("url", "http://127.0.0.1:8080", "running local HTTP API base URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(*url, "/")+"/v1/shutdown", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("shutdown failed with HTTP %d: %s", response.StatusCode, string(body))
	}
	fmt.Print(string(body))
	return nil
}

func runRecover(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := flags.String("db", "./prefixcache.db", "SQLite database path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := prefixcache.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer engine.Close()
	status, err := engine.Status(ctx)
	if err != nil {
		return err
	}
	if status.CurrentVersion == 0 {
		return errors.New("database has no committed version")
	}
	snapshot, err := engine.Snapshot(ctx, status.CurrentVersion)
	if err != nil {
		return err
	}
	consistent := snapshot.Version == status.CurrentVersion && snapshot.VersionFingerprint == status.VersionFingerprint && len(snapshot.Slots) == status.SlotOccupancy
	printJSON(map[string]any{
		"ok":                  consistent,
		"recovered_version":   status.CurrentVersion,
		"version_fingerprint": status.VersionFingerprint,
		"slot_occupancy":      status.SlotOccupancy,
		"request_state_count": len(snapshot.States),
		"allocation_sequence": status.AllocationSequence,
	})
	if !consistent {
		return errors.New("recovered head and immutable snapshot are inconsistent")
	}
	return nil
}

func decodeFile(path string, destination any) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func printJSON(value any) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func removeDatabase(path string) {
	for _, candidate := range []string{path, path + "-wal", path + "-shm", path + ".lock"} {
		_ = os.Remove(candidate)
	}
}
