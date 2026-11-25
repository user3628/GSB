package acceptance

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"voiceline/internal/checkpoint"
)

func TestPublisherProcessHelper(t *testing.T) {
	if os.Getenv("VL_PUBLISH_HELPER") != "1" {
		t.Skip("publisher helper only runs when VL_PUBLISH_HELPER=1")
	}
	block, _ := strconv.Atoi(os.Getenv("VL_BLOCK"))
	ctx := context.Background()
	store, err := checkpoint.Open(ctx, os.Getenv("VL_DB"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer store.Close()
	_, err = store.PublishBlock(ctx, checkpoint.BlockPublication{
		SegmentID:     os.Getenv("VL_SEGMENT"),
		BlockIndex:    block,
		WorkerID:      "process-" + strconv.Itoa(os.Getpid()),
		PolicyVersion: 1,
		PCM:           []byte{1, 2, 3, 4},
		Digest:        "cross-process-digest",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(10)
	}
	fmt.Println("published")
}

func TestCrossProcessPublicationAllowsOneContiguousWriter(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cross-process.db")
	segment := "seg-process"

	store, err := checkpoint.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureShard(ctx, segment); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	const processCount = 8
	var wg sync.WaitGroup
	results := make(chan error, processCount)
	for i := 0; i < processCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestPublisherProcessHelper$")
			cmd.Env = append(os.Environ(),
				"VL_PUBLISH_HELPER=1",
				"VL_DB="+dbPath,
				"VL_SEGMENT="+segment,
				"VL_BLOCK=0",
			)
			results <- cmd.Run()
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	staleOrFailure := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			staleOrFailure++
		}
	}
	if successes != 1 {
		t.Fatalf("successful publisher processes = %d, want 1 (failures=%d)", successes, staleOrFailure)
	}
	if staleOrFailure != processCount-1 {
		t.Fatalf("failed publisher processes = %d, want %d", staleOrFailure, processCount-1)
	}

	reopened, err := checkpoint.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next, err := reopened.NextBlockIndex(ctx, segment)
	if err != nil {
		t.Fatal(err)
	}
	if next != 1 {
		t.Fatalf("cursor after cross-process race = %d, want 1", next)
	}
}
