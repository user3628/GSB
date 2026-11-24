package service_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/sample"
	"mitmpipeline/internal/service"
	"mitmpipeline/internal/store"
)

func TestConcurrentReadersSeeCompleteConclusion(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := service.New(st)
	fx := sample.NewFixture()

	result, err := svc.ImportRecords(ctx, []domain.Record{fx.Accepted}, service.ImportOptions{BatchID: "seed"}, domain.Negotiation{Action: domain.ActionAccept}, domain.FormatBinary)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Conclusions) != 1 {
		t.Fatalf("expected one conclusion")
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				conclusion, err := st.GetConclusion(ctx, "ACCEPT-20251124-0001", result.CommitVersion)
				if err != nil {
					t.Errorf("concurrent read: %v", err)
					return
				}
				if conclusion.Conclusion != domain.ConclusionTrusted || len(conclusion.EvidenceChain) == 0 {
					t.Errorf("reader saw incomplete atomic conclusion: %+v", conclusion)
					return
				}
			}
		}()
	}
	wg.Wait()
}
