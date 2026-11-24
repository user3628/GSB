package sqlitepool_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	sqlitepool "local/sqlitepool"
)

func TestPublicFacadeAcquireQueryRelease(t *testing.T) {
	dir := t.TempDir()
	pool, err := sqlitepool.Open(context.Background(), sqlitepool.Options{
		DatabasePath: filepath.Join(dir, "data.sqlite"),
		MetadataPath: filepath.Join(dir, "meta.sqlite"),
		Config: sqlitepool.Config{
			MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 2000, MaxWaiters: 1, Version: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = pool.Shutdown(ctx)
	}()
	ctx := context.Background()
	lease, err := pool.Acquire(ctx, sqlitepool.AcquireOptions{RequestID: "public-acquire"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := pool.Query(ctx, lease.ConnectionID, "SELECT 1 AS value", sqlitepool.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("unexpected query result: %+v", result)
	}
	if _, err := pool.Release(ctx, lease.ConnectionID, "public-release"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.LatestRecoveryReport(ctx); err != nil {
		t.Fatal(err)
	}
}
