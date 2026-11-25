package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"mvccengine/internal/serverapp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "listen address")
	dbPath := flag.String("db", "data/mvcc.db", "SQLite database path")
	snapshotDir := flag.String("snapshot-dir", "data/snapshots", "snapshot directory")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := serverapp.Run(ctx, serverapp.Config{Addr: *addr, DBPath: *dbPath, SnapshotDir: *snapshotDir}); err != nil {
		log.Fatal(err)
	}
}
