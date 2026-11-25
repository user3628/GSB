package serverapp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"mvccengine/internal/engine"
	"mvccengine/internal/httpapi"
)

type Config struct {
	Addr        string
	DBPath      string
	SnapshotDir string
}

func Run(ctx context.Context, cfg Config) error {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:18080"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join("data", "mvcc.db")
	}
	if cfg.SnapshotDir == "" {
		cfg.SnapshotDir = filepath.Join("data", "snapshots")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.SnapshotDir, 0o755); err != nil {
		return err
	}

	opts := engine.Options{}
	if crashRequestID := os.Getenv("MVCC_CRASH_COMMIT_REQUEST_ID"); crashRequestID != "" {
		opts.CommitIntentHook = func(intentID string, requestID string) {
			if requestID != crashRequestID {
				return
			}
			if readyFile := os.Getenv("MVCC_CRASH_READY_FILE"); readyFile != "" {
				_ = os.WriteFile(readyFile, []byte(intentID), 0o644)
			}
			time.Sleep(10 * time.Minute)
		}
	}

	eng, err := engine.OpenWithOptions(cfg.DBPath, opts)
	if err != nil {
		return err
	}
	defer eng.Close()

	if _, err := eng.RecoverPending(); err != nil {
		return fmt.Errorf("recover pending intents: %w", err)
	}

	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	fmt.Printf("MVCC_LISTEN_ADDR=%s\n", listener.Addr().String())

	server := &http.Server{
		Handler:           httpapi.NewServer(eng).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
