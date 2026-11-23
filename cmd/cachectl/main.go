package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"ciartifacts/internal/cache"
	"ciartifacts/internal/httpapi"
	"ciartifacts/internal/verify"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usageError()
	}
	switch args[0] {
	case "serve":
		return serve(args[1:])
	case "verify":
		return runVerify(args[1:])
	case "crash-helper":
		return crashHelper(args[1:])
	case "validate-samples":
		return validateSamples(args[1:])
	case "help", "--help", "-h":
		fmt.Print(usageText)
		return nil
	default:
		return usageError()
	}
}

const usageText = `cachectl - local CI artifact cache

Commands:
  serve             Start the loopback HTTP endpoint (CACHE_ADDR, CACHE_DIR, CACHE_CAPACITY_BYTES)
  verify            Run fixed-sample HTTP and forced-kill end-to-end checks
  validate-samples  Validate an all-or-nothing deterministic JSON sample file
  crash-helper      Internal helper used by verify (not a public API)
`

func usageError() error {
	fmt.Print(usageText)
	return errors.New("a command is required")
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	dirFlag := flags.String("dir", "", "cache data directory (overrides CACHE_DIR)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	addr := strings.TrimSpace(os.Getenv("CACHE_ADDR"))
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	dir := strings.TrimSpace(*dirFlag)
	if dir == "" {
		dir = strings.TrimSpace(os.Getenv("CACHE_DIR"))
	}
	if dir == "" {
		dir = filepath.Join(".", ".cache-data")
	}
	capacity := int64(1 << 30)
	if raw := strings.TrimSpace(os.Getenv("CACHE_CAPACITY_BYTES")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			return fmt.Errorf("invalid CACHE_CAPACITY_BYTES %q", raw)
		}
		capacity = value
	}
	ctx := context.Background()
	store, err := cache.Open(ctx, cache.Config{Dir: dir, CapacityBytes: capacity})
	if err != nil {
		return err
	}
	defer store.Close()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	endpoint := "http://" + listener.Addr().String()
	event, _ := json.Marshal(map[string]any{
		"event":          "listening",
		"endpoint":       endpoint,
		"dir":            store.Dir(),
		"capacity_bytes": store.CapacityBytes(),
	})
	fmt.Println(string(event))

	server := &http.Server{
		Handler:           httpapi.New(store),
		ReadHeaderTimeout: 10 * time.Second,
	}
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runVerify(args []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	samples := flags.String("samples", filepath.Join("samples", "entries.json"), "deterministic sample file")
	workDir := flags.String("work-dir", "", "optional persistent work directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	report, err := verify.Run(context.Background(), verify.Options{
		SamplesPath: *samples,
		WorkDir:     *workDir,
		Executable:  executable,
	})
	if err != nil {
		return err
	}
	for _, check := range report.Checks {
		status := "FAIL"
		if check.Passed {
			status = "PASS"
		}
		fmt.Printf("[%s] %-32s expected=%s | actual=%s\n", status, check.Name, check.Expected, check.Actual)
	}
	fmt.Printf("RESULT %d/%d PASS overall=%t\n", report.PassedCount, len(report.Checks), report.OverallPassed)
	if !report.OverallPassed {
		return errors.New("end-to-end verification failed")
	}
	return nil
}

func validateSamples(args []string) error {
	flags := flag.NewFlagSet("validate-samples", flag.ContinueOnError)
	path := flags.String("samples", filepath.Join("samples", "entries.json"), "deterministic sample file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	inputs, err := cache.LoadSampleFile(*path)
	if err != nil {
		return err
	}
	for _, input := range inputs {
		fmt.Printf("%s %s %s bytes=%d digest=%s\n", input.StageID, input.InputFingerprint, input.ToolchainVersion, input.ArtifactSize, input.ArtifactDigest)
	}
	fmt.Printf("validated %d entries; batch is ready for import\n", len(inputs))
	return nil
}

func crashHelper(args []string) error {
	flags := flag.NewFlagSet("crash-helper", flag.ContinueOnError)
	dir := flags.String("dir", "", "cache directory")
	scenario := flags.String("scenario", "write", "write or evict")
	samples := flags.String("samples", filepath.Join("samples", "entries.json"), "sample file")
	stage := flags.String("stage", "recovery", "sample stage to write")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("--dir is required")
	}
	marker := os.Getenv("CIARTIFACTS_CRASH_MARKER")
	if marker == "" {
		return errors.New("CIARTIFACTS_CRASH_MARKER is required")
	}
	crashPoint := cache.CrashAfterWriting
	capacity := int64(1024)
	if *scenario == "evict" {
		crashPoint = cache.CrashAfterEvictIntent
		capacity = 1
	} else if *scenario != "write" {
		return fmt.Errorf("unknown scenario %q", *scenario)
	}
	hook := func(point string) {
		_ = os.WriteFile(marker, []byte(point), 0o644)
		for {
			time.Sleep(time.Hour)
		}
	}
	ctx := context.Background()
	store, err := cache.Open(ctx, cache.Config{
		Dir:           *dir,
		CapacityBytes: capacity,
		CrashPoint:    crashPoint,
		CrashHook:     hook,
	})
	if err != nil {
		return err
	}
	defer store.Close()
	inputs, err := cache.LoadSampleFile(*samples)
	if err != nil {
		return err
	}
	input, err := cache.SampleByName(inputs, *stage)
	if err != nil {
		return err
	}
	if _, err := store.Put(ctx, input); err != nil {
		return err
	}
	if *scenario == "evict" {
		if _, err := store.Evict(ctx); err != nil {
			return err
		}
	}
	return errors.New("crash hook was not reached")
}
