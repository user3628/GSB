package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"flashattention/internal/attn"
	"flashattention/internal/errs"
)

func checkpoint(shard string, seq int64, colEnd int) Checkpoint {
	return Checkpoint{
		ShardID: shard, BH: 0, RowBlock: 0, ColStart: 0, ColEnd: colEnd,
		Seq: seq, Strategy: "scaled", VersionFP: "fp-v1", InputFP: "input",
		Snapshots: []attn.Snapshot{{
			LocalRow: 0, Valid: true, M: 1.5, Sum: 2.5, LSE: 2.4,
			Acc: []float64{0.25, -0.75},
		}},
	}
}

func TestPublishRejectsDuplicateRollbackAndGap(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Publish(ctx, checkpoint("s", 1, 4)); err != nil {
		t.Fatal(err)
	}

	_, err = st.Publish(ctx, checkpoint("s", 1, 4))
	if errs.Code(err) != errs.EStaleCheckpoint {
		t.Fatalf("duplicate code=%s err=%v", errs.Code(err), err)
	}
	ce := errs.As(err)
	if ce.Fields["current_seq"] != int64(1) {
		t.Fatalf("missing current seq: %+v", ce.Fields)
	}

	_, err = st.Publish(ctx, checkpoint("s", 3, 12))
	if errs.Code(err) != errs.EStaleCheckpoint {
		t.Fatalf("gap code=%s", errs.Code(err))
	}

	if _, err := st.Publish(ctx, checkpoint("s", 2, 8)); err != nil {
		t.Fatal(err)
	}
	heads, err := st.HeadSequences(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if heads["0/0"] != 2 {
		t.Fatalf("head=%d", heads["0/0"])
	}
}

func TestConcurrentWorkersAdvanceOneSequence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.db")

	const workers = 4
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := Open(path)
			if err != nil {
				errCh <- err
				return
			}
			defer st.Close()
			for {
				heads, err := st.HeadSequences(ctx, "conc")
				if err != nil {
					errCh <- err
					return
				}
				cur := heads["0/0"]
				if cur >= 20 {
					return
				}
				_, err = st.Publish(ctx, checkpoint("conc", cur+1, int(cur+1)*4))
				if err == nil {
					continue
				}
				if errs.Code(err) == errs.EStaleCheckpoint {
					continue
				}
				errCh <- err
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	heads, err := st.HeadSequences(ctx, "conc")
	if err != nil {
		t.Fatal(err)
	}
	if heads["0/0"] != 20 {
		t.Fatalf("expected seq20, got %d", heads["0/0"])
	}
}

func TestRecoveryIgnoresDuplicateSequence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dup.db")
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for i := int64(1); i <= 3; i++ {
		if _, err := st.Publish(ctx, checkpoint("dup", i, int(i)*4)); err != nil {
			t.Fatal(err)
		}
	}
	_, err = st.RawDB().Exec(`
INSERT INTO checkpoints(shard_id,bh,row_block,col_start,col_end,rows_count,head_dim,seq,strategy,
 row_max_blob,exp_sum_blob,lse_blob,acc_blob,acc_hash,input_fp,version_fp,prev_hash,state_hash,active)
SELECT shard_id,bh,row_block,col_start,999,rows_count,head_dim,seq,strategy,
 row_max_blob,exp_sum_blob,lse_blob,acc_blob,acc_hash,input_fp,version_fp,prev_hash,'duplicate-test',1
FROM checkpoints WHERE shard_id='dup' AND seq=3 LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.LoadRecovery(ctx, "dup")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ig := range res.Ignored {
		if ig.Reason == "duplicate_sequence" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected duplicate ignored, got %+v", res.Ignored)
	}
	if res.States["0/0"].Seq != 3 {
		t.Fatalf("recovery seq=%d", res.States["0/0"].Seq)
	}
}
