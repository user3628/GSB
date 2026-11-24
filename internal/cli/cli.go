package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"preprocesspipeline/internal/api"
	"preprocesspipeline/internal/app"
	"preprocesspipeline/internal/store"
	"preprocesspipeline/internal/verify"
)

var ErrVerificationFailed = errors.New("verification found mismatches")

func Server(args []string) error {
	if args == nil {
		args = os.Args[1:]
	}
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	dataDir := flags.String("data-dir", envOr("DATA_DIR", "data"), "directory for SQLite and shard artifacts")
	addr := flags.String("addr", envOr("ADDR", "127.0.0.1:18080"), "HTTP listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	st, err := store.Open(*dataDir)
	if err != nil {
		return fmt.Errorf("open sqlite store: %w", err)
	}
	defer st.Close()

	service := app.NewService(st)
	report, err := service.Recover(context.Background(), "startup")
	if err != nil {
		return fmt.Errorf("startup recovery scan: %w", err)
	}
	log.Printf("data_dir=%s startup_recovered_intents=%d", *dataDir, report.UnfinishedCount)

	server := &http.Server{
		Addr:              *addr,
		Handler:           api.NewServer(service).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("preprocess pipeline listening on http://%s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func Verify(args []string) error {
	if args == nil {
		args = os.Args[1:]
	}
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	dataDir := flags.String("data-dir", envOr("DATA_DIR", "data"), "directory containing preprocess.db and shard artifacts")
	batchID := flags.String("batch", "", "batch_id to verify")
	policyVersion := flags.Int64("policy-version", 0, "policy version; zero uses batch's current version")
	compact := flags.Bool("compact", false, "emit compact JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *batchID == "" {
		return errors.New("missing required --batch <batch_id>")
	}
	st, err := store.Open(*dataDir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	report, err := verify.VerifyBatch(context.Background(), st, *batchID, *policyVersion)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	if !*compact {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(report); err != nil {
		return err
	}
	if !report.OK {
		return ErrVerificationFailed
	}
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
