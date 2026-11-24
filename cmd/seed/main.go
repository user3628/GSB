package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"columnar-executor/internal/config"
	"columnar-executor/internal/model"
	"columnar-executor/internal/sample"
	"columnar-executor/internal/service"
	"columnar-executor/internal/store"
)

func main() {
	dbPath := flag.String("db", "data/columnar.db", "SQLite database file")
	rows := flag.Int("rows", config.DefaultRows, "deterministic snapshot rows")
	seed := flag.Uint64("seed", config.DefaultSeed, "deterministic generator seed")
	snapshotVersion := flag.Int64("snapshot-version", config.DefaultSnapshotVersion, "snapshot version")
	chainID := flag.String("chain-id", "demo-sales", "sample chain id")
	chainVersion := flag.Int64("chain-version", config.DefaultChainVersion, "sample chain version")
	flag.Parse()

	ctx := context.Background()
	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()
	svc, err := service.New(st, service.Options{})
	if err != nil {
		log.Fatalf("initialize service: %v", err)
	}

	current, err := svc.CurrentSnapshot(ctx)
	if err != nil {
		if biz, ok := model.AsBizError(err); !ok || biz.Code != model.CodeNotFound {
			log.Fatalf("read current snapshot: %v", err)
		}
		generated, err := svc.GenerateSnapshot(ctx, model.SnapshotGenerateRequest{
			Version: *snapshotVersion,
			Rows:    *rows,
			Seed:    *seed,
		})
		if err != nil {
			log.Fatalf("generate snapshot: %v", err)
		}
		current = generated
	}

	chain := sample.Chain(*chainID, *chainVersion, current.Version)
	if _, err := svc.SubmitChain(ctx, chain); err != nil {
		if biz, ok := model.AsBizError(err); ok && biz.Code == model.CodeStaleVersion {
			log.Printf("sample chain already exists at or after version %d; skipped", *chainVersion)
		} else {
			log.Fatalf("submit sample chain: %v", err)
		}
	}

	fmt.Printf("snapshot version=%d rows=%d seed=%d sha256=%s\n", current.Version, current.RowCount, current.Seed, current.SHA256Digest)
	fmt.Printf("chain id=%s version=%d base_snapshot_version=%d operators=%d\n", *chainID, *chainVersion, current.Version, len(chain.Operators))
	fmt.Printf("database=%s\n", st.DBPath())
}
