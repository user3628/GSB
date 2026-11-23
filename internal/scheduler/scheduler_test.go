package scheduler

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

func ev(seq uint64, dst string) model.Event {
	return model.Event{Seq: seq, Subject: "s", SrcField: "users.id_card", DstField: dst, Op: model.OpCrossTableCopy, LogicalTime: seq}
}

func TestEventBudgetTerminatesWindow(t *testing.T) {
	sessions := []Session{{ID: "S1", Policy: testPolicy(), Windows: []WindowInput{
		{ID: "W1", Events: []model.Event{ev(1, "dw.copy"), ev(2, "dw.copy2"), ev(3, "dw.copy3")}},
		{ID: "W2", Events: []model.Event{ev(1, "dw.copy")}},
	}}}
	results := Run(Config{MaxConcurrent: 2, MaxEventsPerWindow: 2}, sessions, latency.NewSampler())
	if len(results) != 2 {
		t.Fatalf("got %d results", len(results))
	}
	byID := map[string]WindowResult{}
	for _, r := range results {
		byID[r.WindowID] = r
	}
	if byID["W1"].Code != codes.EEventBudget || !byID["W1"].Incomplete {
		t.Fatalf("W1: %+v", byID["W1"])
	}
	if byID["W2"].Code != codes.OK || byID["W2"].Incomplete {
		t.Fatalf("W2: %+v", byID["W2"])
	}
}

func TestConcurrencyQueueAndIsolation(t *testing.T) {
	var wins []WindowInput
	for i := 0; i < 6; i++ {
		wins = append(wins, WindowInput{ID: "W" + string(rune('A'+i)), Events: []model.Event{ev(1, "dw.copy")}})
	}
	sessions := []Session{
		{ID: "S1", Policy: testPolicy(), Windows: wins[:3]},
		{ID: "S2", Policy: testPolicy(), Windows: wins[3:]},
	}
	// More windows than the limit: extras must wait, none rejected.
	results := Run(Config{MaxConcurrent: 2}, sessions, latency.NewSampler())
	if len(results) != 6 {
		t.Fatalf("all windows must complete via queue, got %d", len(results))
	}
	for _, r := range results {
		if r.Code != codes.OK {
			t.Fatalf("window %s rejected: %s", r.WindowID, r.Code)
		}
		// Session isolation: each window only sees its own marks.
		marks := r.Window.InteractiveMarks()
		if len(marks["dw.copy"]) != 1 {
			t.Fatalf("window %s mark state leaked: %v", r.WindowID, marks)
		}
	}
}
