package replay_test

import (
	"testing"

	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/normalize"
	"mitmpipeline/internal/replay"
	"mitmpipeline/internal/sample"
)

func TestInvalidTransitionReturnsCode(t *testing.T) {
	fx := sample.NewFixture()
	record := domain.Record{
		Format: domain.FormatBinary, FormatVersion: 2, ConnectionID: "bad-transition",
		SNI: "good.internal", ObservedStartMS: sample.FixedTime.UnixMilli(),
		CertChainPEM: fx.Good.ChainPEM, Policy: fx.Accepted.Policy,
		Events: []domain.Event{{Seq: 1, Type: domain.EventHandshakeComplete, OffsetNS: 1_000_000}},
	}
	events, _, err := normalize.NormalizeRecordEvents(&record, normalize.RoundTruncate)
	if err != nil {
		t.Fatal(err)
	}
	conclusion, perr := replay.Replay(record, events, replay.Options{})
	if perr == nil || perr.Code != domain.CodeInvalidTransition {
		t.Fatalf("expected E_INVALID_TRANSITION, got %v", perr)
	}
	if conclusion == nil || conclusion.Conclusion != domain.ConclusionInsufficient {
		t.Fatalf("expected partial insufficient conclusion, got %+v", conclusion)
	}
}

func TestAbortedConnectionIsNotTrusted(t *testing.T) {
	fx := sample.NewFixture()
	events, _, err := normalize.NormalizeRecordEvents(&fx.Aborted, normalize.RoundTruncate)
	if err != nil {
		t.Fatal(err)
	}
	conclusion, perr := replay.Replay(fx.Aborted, events, replay.Options{})
	if perr != nil {
		t.Fatal(perr)
	}
	if conclusion.Conclusion != domain.ConclusionInsufficient {
		t.Fatalf("aborted conclusion=%s want insufficient_evidence", conclusion.Conclusion)
	}
}

func TestHotUpdateBindsDifferentPolicyByEvent(t *testing.T) {
	fx := sample.NewFixture()
	events, _, err := normalize.NormalizeRecordEvents(&fx.HotUpdate, normalize.RoundTruncate)
	if err != nil {
		t.Fatal(err)
	}
	conclusion, perr := replay.Replay(fx.HotUpdate, events, replay.Options{})
	if perr != nil {
		t.Fatal(perr)
	}
	if conclusion.Conclusion != domain.ConclusionMITM {
		t.Fatalf("conclusion=%s want mitm", conclusion.Conclusion)
	}
	bySeq := map[int]domain.EventJudgment{}
	for _, judgment := range conclusion.EventJudgments {
		bySeq[judgment.EventSeq] = judgment
	}
	if bySeq[3].PolicyVersion != "pol-hot-v1" || bySeq[3].Result != domain.ConclusionTrusted {
		t.Fatalf("seq3 not bound to trusted v1: %+v", bySeq[3])
	}
	if bySeq[4].PolicyVersion != "pol-hot-v2" || bySeq[4].Result != domain.ConclusionMITM {
		t.Fatalf("seq4 not bound to MITM v2: %+v", bySeq[4])
	}
	if len(conclusion.PolicyVersions) != 2 || conclusion.PolicyVersions[0] != "pol-hot-v1" || conclusion.PolicyVersions[1] != "pol-hot-v2" {
		t.Fatalf("bad version sequence: %v", conclusion.PolicyVersions)
	}
}

func TestStateTransitions(t *testing.T) {
	state := domain.StateEmpty
	path := []string{
		domain.EventHandshakeStart, domain.EventCertificateReceived,
		domain.EventPinVerification, domain.EventPinVerification, domain.EventHandshakeComplete,
	}
	for _, event := range path {
		next, err := replay.Transition(state, event)
		if err != nil {
			t.Fatalf("event %s from %s: %v", event, state, err)
		}
		state = next
	}
	if state != domain.StateCompleted {
		t.Fatalf("state=%s", state)
	}
	if _, err := replay.Transition(domain.StateEmpty, domain.EventPinVerification); err == nil || err.Code != domain.CodeInvalidTransition {
		t.Fatalf("expected pin-before-cert invalid transition, got %v", err)
	}
}
