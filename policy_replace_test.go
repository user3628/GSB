package feedbackstream

import (
	"context"
	"testing"
)

func TestFullPolicyReplaceMonotonic(t *testing.T) {
	service, store, _ := newTestService(t, nil)
	ctx := context.Background()

	next := DefaultPolicy()
	next.Version = 2
	next.RoundTimeBudgetMS = 42
	next.ChecksumSHA256 = ""
	sum, err := PolicyChecksum(next)
	if err != nil {
		t.Fatal(err)
	}
	next.ChecksumSHA256 = sum
	if _, err := service.ReplacePolicySnapshot(ctx, next); err != nil {
		t.Fatalf("replace v2: %v", err)
	}
	active, err := store.ActivePolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if active.Snapshot.Version != 2 || active.Snapshot.RoundTimeBudgetMS != 42 {
		t.Fatalf("unexpected active policy: %#v", active.Snapshot)
	}

	old := DefaultPolicy()
	old.Version = 1
	_, err = service.ReplacePolicySnapshot(ctx, old)
	requireCode(t, err, CodePolicyVersion)
}

func TestEventSnapshotWrapper(t *testing.T) {
	snapshot := EventSnapshot{
		Schema: SchemaEventStream, MetricScale: MetricScale,
		Seed: FixedSeed, Events: DefaultEvents(),
	}
	data, err := CanonicalJSON(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEventSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Events) != len(DefaultEvents()) {
		t.Fatalf("unexpected event count %d", len(parsed.Events))
	}
}
