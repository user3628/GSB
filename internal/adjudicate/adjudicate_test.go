package adjudicate

import (
	"testing"

	"flowtrace/internal/codes"
	"flowtrace/internal/latency"
	"flowtrace/internal/model"
	"flowtrace/internal/policy"
)

func testPolicy() *policy.Snapshot {
	return &policy.Snapshot{
		Version:           1,
		SensitiveFields:   map[string][]string{"users.id_card": {"ID_CARD"}},
		AuthorizedTargets: map[string]bool{"users.id_card": true, "dw.copy": true},
		AggregateSafe:     map[string]bool{},
	}
}

func ev(seq uint64, src, dst string, op model.Op) model.Event {
	return model.Event{Seq: seq, Subject: "s", SrcField: src, DstField: dst, Op: op, LogicalTime: seq * 10}
}

func TestInOrderConsistentNoCorrections(t *testing.T) {
	w := NewWindow("W1", testPolicy(), latency.NewSampler())
	w.OnEvent(ev(1, "users.id_card", "dw.copy", model.OpCrossTableCopy))
	w.OnEvent(ev(2, "dw.copy", "ext.feed", model.OpCrossServiceExport))
	w.Settle()
	if len(w.Corrections()) != 0 {
		t.Fatalf("in-order arrival must not need corrections: %+v", w.Corrections())
	}
	if c := w.Consistency(); !c.Consistent {
		t.Fatalf("must be consistent: %+v", c.Mismatches)
	}
}

func TestOutOfOrderProducesForwardCorrections(t *testing.T) {
	w := NewWindow("W1", testPolicy(), latency.NewSampler())
	// Export arrives before the copy that carries the mark: interactive side
	// initially judges OK, settlement must correct it forward.
	w.OnEvent(ev(2, "dw.copy", "ext.feed", model.OpCrossServiceExport))
	w.OnEvent(ev(1, "users.id_card", "dw.copy", model.OpCrossTableCopy))
	w.Settle()

	corrections := w.Corrections()
	if len(corrections) != 1 {
		t.Fatalf("want 1 correction, got %+v", corrections)
	}
	c := corrections[0]
	if c.EventSeq != 2 || c.FromCode != codes.OK || c.ToCode != codes.EFlowEscaped {
		t.Fatalf("bad correction: %+v", c)
	}
	if c.Version <= 1 {
		t.Fatalf("version must move forward, got %d", c.Version)
	}
	// Settlement judgment carries the same version the display refers to.
	for _, j := range w.SettlementJudgments() {
		if j.EventSeq == 2 && j.Version != c.Version {
			t.Fatalf("judgment version %d != correction version %d", j.Version, c.Version)
		}
	}
	if cons := w.Consistency(); !cons.Consistent {
		t.Fatalf("post-correction must be consistent: %+v", cons.Mismatches)
	}
}

func TestJudgmentKeysBySeqAndDst(t *testing.T) {
	w := NewWindow("W1", testPolicy(), latency.NewSampler())
	w.OnEvent(ev(1, "users.id_card", "dw.copy", model.OpCrossTableCopy))
	w.OnEvent(ev(2, "dw.copy", "ext.feed", model.OpCrossServiceExport))
	w.Settle()
	settled := w.SettlementJudgments()
	if len(settled) != 2 {
		t.Fatalf("got %d judgments", len(settled))
	}
	if settled[0].EventSeq != 1 || settled[1].EventSeq != 2 {
		t.Fatalf("settlement must be canonical order: %+v", settled)
	}
}
