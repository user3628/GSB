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
	"time"

	"edgeconfig/internal/verify"
	"edgeconfig/pkg/edgeconfig"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "hash-snapshot":
		hashSnapshot(os.Args[2:])
	case "verify":
		runVerify(os.Args[2:])
	case "serve":
		runServe(os.Args[2:])
	case "node-endpoint":
		runNodeEndpoint(os.Args[2:])
	case "__crash_child":
		runCrashChild(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: edge-dist <verify|serve|node-endpoint|hash-snapshot|__crash_child> [flags]")
}

func hashSnapshot(args []string) {
	fs := flag.NewFlagSet("hash-snapshot", flag.ExitOnError)
	file := fs.String("file", "", "snapshot JSON file")
	_ = fs.Parse(args)
	if *file == "" {
		fatal(errors.New("--file is required"))
	}
	raw, err := os.ReadFile(*file)
	fatal(err)
	var snapshot edgeconfig.Snapshot
	fatal(json.Unmarshal(raw, &snapshot))
	digest, err := edgeconfig.SnapshotDigest(snapshot)
	fatal(err)
	fmt.Println(digest)
}

func runVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	samples := fs.String("samples", "samples", "deterministic sample directory")
	dbPath := fs.String("db", ".runtime/edgeconfig-verify.db", "SQLite database path")
	output := fs.String("output", "artifacts/verification-report.json", "JSON report output path")
	freeEndpoints := fs.Bool("free-endpoints", false, "use ephemeral loopback ports instead of fixed sample ports")
	forceKill := fs.Bool("force-kill", true, "spawn and force-kill crash recovery children")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	report, err := verify.Run(ctx, verify.Options{
		SamplesDir:    *samples,
		DBPath:        *dbPath,
		OutputPath:    *output,
		ResetDB:       true,
		FreeEndpoints: *freeEndpoints,
		ForceKill:     *forceKill,
	})
	if report != nil {
		printSummary(report)
	}
	if err != nil {
		fatal(err)
	}
	if report == nil || !report.OK {
		os.Exit(1)
	}
}

func printSummary(report *verify.Report) {
	passed := 0
	for _, check := range report.Checks {
		if check.OK {
			passed++
		} else {
			fmt.Printf("FAIL %s: %s [%s]\n", check.Name, check.Error, check.Code)
		}
	}
	fmt.Printf("checks=%d passed=%d failed=%d report=%s\n", len(report.Checks), passed, len(report.Checks)-passed, "artifacts/verification-report.json")
	for _, force := range report.ForceKill {
		status := "PASS"
		if !force.OK {
			status = "FAIL " + force.Error
		}
		fmt.Printf("force-kill/%s=%s pending=%d\n", force.Scenario, status, force.PendingBefore)
	}
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:18080", "HTTP API listen address")
	dbPath := fs.String("db", ".runtime/edgeconfig.db", "SQLite database path")
	samplesDir := fs.String("samples", "", "optional samples directory to validate and load")
	_ = fs.Parse(args)

	store, err := edgeconfig.OpenStore(*dbPath)
	fatal(err)
	defer store.Close()
	service := edgeconfig.NewService(store)

	if *samplesDir != "" {
		samples, err := verify.LoadSampleSet(*samplesDir)
		fatal(err)
		_, err = service.SaveTopology(context.Background(), samples.Topology)
		fatal(err)
	}

	server := &http.Server{Addr: *addr, Handler: edgeconfig.NewHTTPHandler(service)}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		fmt.Printf("edge-dist API listening on http://%s\n", *addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func runNodeEndpoint(args []string) {
	fs := flag.NewFlagSet("node-endpoint", flag.ExitOnError)
	nodeID := fs.String("node-id", "mock-node", "node identifier")
	addr := fs.String("addr", "127.0.0.1:19091", "listen address")
	digest := fs.String("digest", "", "applied digest")
	version := fs.Int64("version", 1, "observed/applied version")
	hang := fs.Duration("hang", 0, "optional response hang duration")
	rejectStatus := fs.Int("reject-status", 0, "optional business rejection HTTP status")
	rejectCode := fs.String("reject-code", "E_NODE_AGENT_BUSY", "business rejection code")
	path := fs.String("path", "/applied", "endpoint path")
	_ = fs.Parse(args)

	mock := edgeconfig.NewMockNode(*nodeID, *digest, *version)
	mock.SetHang(*hang)
	mock.SetRejection(*rejectStatus, *rejectCode)
	mux := http.NewServeMux()
	mux.Handle(*path, mock)
	server := &http.Server{Addr: *addr, Handler: mux}
	fmt.Printf("mock node %s listening on http://%s%s\n", *nodeID, *addr, *path)
	fatal(server.ListenAndServe())
}

func runCrashChild(args []string) {
	fs := flag.NewFlagSet("__crash_child", flag.ExitOnError)
	samplesDir := fs.String("samples", "samples", "samples directory")
	dbPath := fs.String("db", "", "database path")
	scenario := fs.String("scenario", "affected_recompute", "crash scenario")
	_ = fs.Parse(args)
	if *dbPath == "" {
		fatal(errors.New("--db is required"))
	}
	fatal(verify.CrashChild(*samplesDir, *dbPath, *scenario))
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
