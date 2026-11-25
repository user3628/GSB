package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"

	"mvccengine/internal/serverapp"
)

func runServerSubcommand(args []string) error {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:0", "listen address")
	dbPath := fs.String("db", "demo-mvcc.db", "SQLite database path")
	snapshotDir := fs.String("snapshot-dir", "demo-snapshots", "snapshot directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return serverapp.Run(ctx, serverapp.Config{Addr: *addr, DBPath: *dbPath, SnapshotDir: *snapshotDir})
}
