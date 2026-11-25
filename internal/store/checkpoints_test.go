package store

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"csrouter/internal/domain"
)

func TestConcurrentPublishersMaintainContiguousStageSequence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "csrouter.db")
	const workers = 16
	stores := make([]*Store, workers)
	for i := range workers {
		opened, err := Open(dbPath)
		if err != nil {
			t.Fatalf("open store %d: %v", i, err)
		}
		stores[i] = opened
		t.Cleanup(func() { _ = opened.Close() })
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ticketID := fmt.Sprintf("T-%02d", i)
			_, bizErr := stores[i].PublishArtifact(PublishInput{
				TicketID:          ticketID,
				Stage:             domain.StageNormalize,
				InputFingerprint:  "input-" + ticketID,
				ConfigVersion:     1,
				ConfigFingerprint: "policy-fingerprint",
				Artifact:          domain.NormalizeArtifact{TicketID: ticketID},
			})
			if bizErr != nil {
				errs <- bizErr
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent publish: %v", err)
		}
	}

	head, err := stores[0].StageHead(domain.StageNormalize)
	if err != nil {
		t.Fatal(err)
	}
	if head != workers {
		t.Fatalf("stage head=%d, want %d", head, workers)
	}

	rows, err := stores[0].db.Query(`SELECT stage_seq FROM artifacts WHERE stage=? ORDER BY stage_seq`, domain.StageNormalize)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sequences []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, seq)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(sequences) != workers {
		t.Fatalf("artifact rows=%d, want %d", len(sequences), workers)
	}
	for i, seq := range sequences {
		if seq != int64(i+1) {
			t.Fatalf("sequences=%v, expected contiguous 1..%d", sequences, workers)
		}
	}

	var ledgerRows int
	if err := stores[0].db.QueryRow(`SELECT COUNT(1) FROM artifact_ledger WHERE stage=?`, domain.StageNormalize).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if ledgerRows != workers {
		t.Fatalf("ledger rows=%d, want %d", ledgerRows, workers)
	}
}
