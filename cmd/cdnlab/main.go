package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"cdn-edge-consistency/internal/engine"
	"cdn-edge-consistency/internal/httpapi"
	"cdn-edge-consistency/internal/model"
	"cdn-edge-consistency/internal/policy"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "checksum":
		err = checksum(os.Args[2:])
	case "publish":
		err = publish(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `cdnlab - local CDN edge cache consistency workbench

Commands:
  serve     Start origin, edge simulation and admin API
  checksum  Validate a policy snapshot and print its canonical checksum
  publish   Atomically replace a local origin content file

Examples:
  go run ./cmd/cdnlab serve --config samples/config.json
  go run ./cmd/cdnlab checksum --config samples/config.json --file samples/policies/policy-v1.json
  go run ./cmd/cdnlab publish --config samples/config.json --path /static/app.js --body "v2"`)
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "samples/config.json", "config JSON path")
	dbPath := fs.String("db", "", "override SQLite database path")
	listen := fs.String("listen", "", "override listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	if *listen != "" {
		cfg.ListenAddr = *listen
	}
	var initial *policy.Snapshot
	if cfg.InitialPolicy != "" {
		initial, err = loadPolicy(cfg.InitialPolicy)
		if err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	eng, err := engine.New(ctx, cfg, initial, model.RealClock{})
	if err != nil {
		return err
	}
	if cfg.RecoveryReport != "" {
		if err := eng.WriteRecoveryReport(cfg.RecoveryReport); err != nil {
			log.Printf("write recovery report: %v", err)
		}
	}
	source := &engine.OriginSource{Root: cfg.OriginRoot, TagsFile: cfg.TagsFile}
	logger := log.New(os.Stdout, "cdnlab ", log.LstdFlags|log.Lmicroseconds)
	api := httpapi.New(eng, source, cfg, logger)
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		_ = eng.Close(context.Background())
		return fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}
	actual := listener.Addr().String()
	originAddr := loopbackAddress(actual)
	eng.SetOriginBaseURL("http://" + originAddr + "/origin")
	server := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	logger.Printf("listening on http://%s (origin base http://%s/origin, db %s)", actual, originAddr, cfg.DBPath)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = eng.Close(context.Background())
			return err
		}
	case <-ctx.Done():
		logger.Printf("shutdown requested")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := eng.Close(closeCtx); err != nil {
		return err
	}
	if cfg.RecoveryReport != "" {
		_ = eng.WriteRecoveryReport(cfg.RecoveryReport)
	}
	return nil
}

func checksum(args []string) error {
	fs := flag.NewFlagSet("checksum", flag.ContinueOnError)
	configPath := fs.String("config", "", "config JSON path used to validate node references")
	file := fs.String("file", "", "policy snapshot JSON")
	write := fs.Bool("write", false, "rewrite the policy file with its canonical checksum")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("--file is required")
	}
	p, err := loadPolicy(*file)
	if err != nil {
		return err
	}
	if *configPath != "" {
		cfg, err := loadConfig(*configPath)
		if err != nil {
			return err
		}
		allowed := map[string]model.NodeConfig{}
		for _, n := range cfg.Nodes {
			allowed[n.ID] = n
		}
		if err := p.NormalizeAndValidate(allowed); err != nil {
			return err
		}
	} else {
		sort.Strings(p.Nodes)
		sort.Strings(p.Vary)
	}
	old := p.Checksum
	p.Checksum = ""
	sum, err := p.CanonicalChecksum()
	if err != nil {
		return err
	}
	if old != "" && !strings.EqualFold(old, sum) {
		return fmt.Errorf("checksum mismatch: file=%s canonical=%s", old, sum)
	}
	p.Checksum = sum
	if *write {
		raw, _ := json.MarshalIndent(p, "", "  ")
		raw = append(raw, '\n')
		if err := os.WriteFile(*file, raw, 0o644); err != nil {
			return err
		}
	}
	fmt.Println(sum)
	return nil
}

func publish(args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	configPath := fs.String("config", "samples/config.json", "config JSON path")
	urlPath := fs.String("path", "", "origin path, for example /static/app.js")
	body := fs.String("body", "", "literal body")
	input := fs.String("file", "", "read replacement body from this file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *urlPath == "" || (*body == "" && *input == "") {
		return errors.New("--path and one of --body/--file are required")
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	rel := strings.TrimPrefix(filepath.ToSlash(filepath.Clean("/"+*urlPath)), "/")
	if rel == "" || strings.HasPrefix(rel, "../") {
		return errors.New("invalid origin path")
	}
	var data []byte
	if *input != "" {
		data, err = os.ReadFile(*input)
		if err != nil {
			return err
		}
	} else {
		data = []byte(*body)
	}
	dest := filepath.Join(cfg.OriginRoot, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	fmt.Printf("published %s (%d bytes)\n", dest, len(data))
	return nil
}

func loadConfig(path string) (model.Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return model.Config{}, err
	}
	var cfg model.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return model.Config{}, err
	}
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return model.Config{}, err
	}
	cfg.DBPath = resolvePath(base, cfg.DBPath)
	cfg.OriginRoot = resolvePath(base, cfg.OriginRoot)
	if cfg.TagsFile != "" {
		cfg.TagsFile = resolvePath(base, cfg.TagsFile)
	}
	if cfg.InitialPolicy != "" {
		cfg.InitialPolicy = resolvePath(base, cfg.InitialPolicy)
	}
	if cfg.RecoveryReport != "" {
		cfg.RecoveryReport = resolvePath(base, cfg.RecoveryReport)
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:18080"
	}
	return cfg, nil
}

func loadPolicy(path string) (*policy.Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p policy.Snapshot
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func resolvePath(base, value string) string {
	if value == "" {
		return ""
	}
	if filepath.IsAbs(value) {
		return value
	}
	return filepath.Clean(filepath.Join(base, value))
}

func loopbackAddress(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}
