package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	"dialogalign/internal/domain"
)

func payload(tag string) []byte {
	return []byte(`{"tag":"` + tag + `"}`)
}

func request(session string, turn int, stage, tag string) PublishRequest {
	body := payload(tag)
	output := domain.DigestBytes(body)
	req := PublishRequest{Checkpoint: Checkpoint{
		SessionID: session, TurnIndex: turn, Stage: stage,
		InputDigest: domain.DigestBytes([]byte("input-" + tag)), OutputDigest: output,
		MapVersion: "v1", Payload: body,
	}}
	if stage == StageEvolve {
		req.StateJSON = body
		req.StateDigest = output
	}
	if stage == StageAlign {
		req.AlignmentJSON = body
		req.AlignmentDigest = output
	}
	return req
}

func publishOK(t *testing.T, s *Store, session string, turn int, stage, tag string) PublishResult {
	t.Helper()
	head, err := s.HeadSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	req := request(session, turn, stage, tag)
	req.ExpectedSeq = head
	result, err := s.Publish(context.Background(), req)
	if err != nil {
		t.Fatalf("publish %s/%d/%s: %v", session, turn, stage, err)
	}
	return result
}

func publishTurn(t *testing.T, s *Store, session string, turn int) {
	t.Helper()
	for _, stage := range []string{StageParse, StageUpgrade, StageEvolve, StageAlign} {
		publishOK(t, s, session, turn, stage, stage)
	}
}

func TestPublishOrderAndDuplicate(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head, _ := s.HeadSeq(context.Background())
	bad := request("S1", 1, StageUpgrade, "bad")
	bad.ExpectedSeq = head
	if _, err := s.Publish(context.Background(), bad); err == nil || domain.AsBusinessError(err).Code != domain.CodeStageTransition {
		t.Fatalf("expected stage transition error, got %v", err)
	}
	publishOK(t, s, "S1", 1, StageParse, "parse")
	head, _ = s.HeadSeq(context.Background())
	duplicate := request("S1", 1, StageParse, "parse")
	duplicate.ExpectedSeq = head
	if _, err := s.Publish(context.Background(), duplicate); err == nil || domain.AsBusinessError(err).Code != domain.CodeStaleCheckpoint {
		t.Fatalf("expected stale duplicate, got %v", err)
	}
	skipped := request("S1", 1, StageEvolve, "evolve")
	skipped.ExpectedSeq = head
	if _, err := s.Publish(context.Background(), skipped); err == nil || domain.AsBusinessError(err).Code != domain.CodeStaleCheckpoint {
		t.Fatalf("expected stale skipped stage, got %v", err)
	}
	crossTurn := request("S1", 2, StageParse, "turn2")
	crossTurn.ExpectedSeq = head
	if _, err := s.Publish(context.Background(), crossTurn); err == nil || domain.AsBusinessError(err).Code != domain.CodeStaleCheckpoint {
		t.Fatalf("expected stale cross-turn publication, got %v", err)
	}
	head, _ = s.HeadSeq(context.Background())
	newSessionWrongTurn := request("S2", 2, StageParse, "s2t2")
	newSessionWrongTurn.ExpectedSeq = head
	if _, err := s.Publish(context.Background(), newSessionWrongTurn); err == nil || domain.AsBusinessError(err).Code != domain.CodeStaleCheckpoint {
		t.Fatalf("expected stale cross-session turn placement, got %v", err)
	}
}

func TestConcurrentPublishAdmitsExactlyOne(t *testing.T) {
	root := t.TempDir()
	stores := make([]*Store, 2)
	for i := range stores {
		s, err := Open(root)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		stores[i] = s
	}
	head, err := stores[0].HeadSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := request("S1", 1, StageParse, "same")
			req.ExpectedSeq = head
			_, results[i] = stores[i].Publish(context.Background(), req)
		}(i)
	}
	wg.Wait()
	success, stale := 0, 0
	for _, err := range results {
		if err == nil {
			success++
			continue
		}
		if domain.AsBusinessError(err).Code == domain.CodeStaleCheckpoint {
			stale++
			continue
		}
		t.Fatalf("unexpected concurrent error: %v", err)
	}
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d", success, stale)
	}
	if err := stores[0].VerifyIntegrity(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTamperAndRollbackAreClassified(t *testing.T) {
	t.Run("tamper", func(t *testing.T) {
		s, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		publishTurn(t, s, "S1", 1)
		if _, err := s.DB().Exec(`UPDATE checkpoints SET payload=? WHERE session_id='S1' AND turn_index=1 AND stage='align'`, []byte(`{"tag":"changed"}`)); err != nil {
			t.Fatal(err)
		}
		err = s.VerifyIntegrity(context.Background())
		if err == nil || domain.AsBusinessError(err).Code != domain.CodeCheckpointTampered {
			t.Fatalf("expected tamper classification, got %v", err)
		}
	})
	t.Run("rollback", func(t *testing.T) {
		s, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		publishTurn(t, s, "S1", 1)
		publishOK(t, s, "S1", 2, StageParse, "t2")
		cp, ok, err := s.ReadCheckpoint(context.Background(), "S1", 2, StageParse)
		if err != nil || !ok {
			t.Fatalf("read checkpoint: %v", err)
		}
		rolled := *cp
		rolled.Seq = 1
		if _, err := s.DB().Exec(`UPDATE checkpoints SET seq=?,record_digest=? WHERE session_id='S1' AND turn_index=2 AND stage='parse'`,
			rolled.Seq, recordDigest(rolled)); err != nil {
			t.Fatal(err)
		}
		err = s.VerifyIntegrity(context.Background())
		if err == nil || domain.AsBusinessError(err).Code != domain.CodeCheckpointRollback {
			t.Fatalf("expected rollback classification, got %v (is business=%v)", err, errors.As(err, new(*domain.BusinessError)))
		}
	})
}

func TestInvalidationPreservesHistoryAndAllowsRebuild(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	publishTurn(t, s, "S1", 1)
	head, _ := s.HeadSeq(context.Background())
	expected, ok, err := s.ReadCheckpoint(context.Background(), "S1", 1, StageAlign)
	if err != nil || !ok {
		t.Fatal(err)
	}
	result, err := s.InvalidateFrom(context.Background(), "S1", 1, StageAlign, "threshold changed", "new-input", "new-output", head)
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 1 {
		t.Fatalf("deleted=%d", result.Deleted)
	}
	historyRows := 0
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM checkpoint_history WHERE session_id='S1' AND turn_index=1 AND stage='align'`).Scan(&historyRows); err != nil {
		t.Fatal(err)
	}
	if historyRows != 1 {
		t.Fatalf("history row was lost, count=%d", historyRows)
	}
	if _, ok, err := s.ReadCheckpoint(context.Background(), "S1", 1, StageAlign); err != nil || ok {
		t.Fatalf("current invalid checkpoint still present: ok=%v err=%v", ok, err)
	}
	publishOK(t, s, "S1", 1, StageAlign, "rebuilt")
	records, err := s.ListInvalidations(context.Background(), "S1")
	if err != nil || len(records) != 1 {
		t.Fatalf("invalidation record missing: %+v %v", records, err)
	}
	if records[0].OldOutputDigest != expected.OutputDigest || records[0].NewOutputDigest != "new-output" {
		t.Fatalf("fingerprint comparison not retained: %+v", records[0])
	}
	if err := s.VerifyIntegrity(context.Background()); err != nil {
		t.Fatal(err)
	}
}
