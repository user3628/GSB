package dbconn

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

func TestSQLiteOpenerConcurrentInitialConnect(t *testing.T) {
	opener := SQLiteOpener{Path: filepath.Join(t.TempDir(), "data.sqlite")}
	const connections = 8
	start := make(chan struct{})
	errs := make(chan error, connections)
	var wg sync.WaitGroup
	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			conn, err := opener.Open(context.Background())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			if err := conn.Ping(context.Background()); err != nil {
				errs <- err
				return
			}
			if _, err := conn.Query(context.Background(), "SELECT 1"); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SQLite connection failed: %v", err)
		}
	}
}
