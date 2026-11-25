package engine

import (
	"testing"

	"alarmengine/internal/errcode"
	"alarmengine/internal/sample"
)

func TestStaleBaseRejected(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	ingestSeed(t, eng, data) // committed version now 2
	_, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-stale", BaseVersion: 1, Observations: data.Observations[:1],
	})
	if errcode.CodeOf(err) != errcode.EStaleBase {
		t.Fatalf("want E_STALE_BASE, got %v", err)
	}
	if eng.Version() != 2 {
		t.Fatalf("committed state changed on stale base: %d", eng.Version())
	}
}

func TestIdempotentRequestReuseAndConflict(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	req := IngestRequest{RequestID: "req-idem", BaseVersion: 1, Observations: data.Observations[:5]}
	first, err := eng.IngestObservations(req)
	if err != nil {
		t.Fatal(err)
	}
	// Identical replay reuses the cached result and does not create a new version.
	second, err := eng.IngestObservations(req)
	if err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
	if second.CommitVersion != first.CommitVersion || second.Inserted != first.Inserted {
		t.Fatalf("replay result mismatch: %+v vs %+v", first, second)
	}
	// Same request id with a different payload is a conflict.
	conflict := req
	conflict.Observations = data.Observations[:6]
	_, err = eng.IngestObservations(conflict)
	if errcode.CodeOf(err) != errcode.EConflictingRequest {
		t.Fatalf("want E_CONFLICTING_REQUEST, got %v", err)
	}
}

func TestSnapshotMonotonicAndChecksum(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	ingestSeed(t, eng, data) // version 2
	current := eng.Version()

	// Version rollback must be rejected.
	old := data.Policy
	old.Version = current // not greater -> invalid
	old.Checksum = sample.ComputeChecksum(old)
	_, err := eng.ReplaceSnapshot(ReplaceSnapshotRequest{
		RequestID: "req-rollback", BaseVersion: current, Snapshot: old,
	})
	if errcode.CodeOf(err) != errcode.ESnapshotInvalid {
		t.Fatalf("want E_SNAPSHOT_INVALID for rollback, got %v", err)
	}

	// Tampered checksum rejected and old snapshot stays usable.
	tampered := clonePolicy(data.Policy)
	tampered.Version = current + 1
	tampered.Thresholds["cpu_pct"] = 1
	tampered.Checksum = "deadbeef"
	_, err = eng.ReplaceSnapshot(ReplaceSnapshotRequest{
		RequestID: "req-tamper", BaseVersion: current, Snapshot: tampered,
	})
	if errcode.CodeOf(err) != errcode.ESnapshotInvalid {
		t.Fatalf("want E_SNAPSHOT_INVALID for bad checksum, got %v", err)
	}
	if eng.Version() != current {
		t.Fatalf("old snapshot must remain active, version=%d", eng.Version())
	}
	alertsBefore, _ := eng.QueryAlerts(current)
	if len(alertsBefore) == 0 {
		t.Fatal("old derived facts missing after failed replacement")
	}

	// Valid monotonic replacement succeeds and atomically rebuilds.
	next := clonePolicy(data.Policy)
	next.Version = current + 1
	next.Thresholds["cpu_pct"] = 99 // change threshold -> cascade rebuild
	next.Checksum = sample.ComputeChecksum(next)
	res, err := eng.ReplaceSnapshot(ReplaceSnapshotRequest{
		RequestID: "req-next", BaseVersion: current, Snapshot: next,
	})
	if err != nil {
		t.Fatalf("valid replacement failed: %v", err)
	}
	if res.NewVersion != next.Version || eng.Version() != next.Version {
		t.Fatalf("version not advanced: %+v", res)
	}
}

func TestAtomicRebuildConsistency(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	ingestSeed(t, eng, data)
	v := eng.Version()
	refs, err := eng.QueryDerivedRefs(v)
	if err != nil {
		t.Fatal(err)
	}
	alerts, _ := eng.QueryAlerts(v)
	hits, _ := eng.QuerySuppressionHits(v)
	scores, _ := eng.QueryRootCauses(v)
	if len(refs) == 0 {
		t.Fatal("derived reference registry empty")
	}
	// Every suppression hit must reference an existing alert (no stale index).
	alertIDs := map[string]bool{}
	for _, a := range alerts {
		alertIDs[a.ID] = true
	}
	for _, h := range hits {
		if !alertIDs[h.AlertID] {
			t.Fatalf("suppression index references missing alert %s", h.AlertID)
		}
	}
	// Every score references a trigger alert that exists at this version.
	for _, sc := range scores {
		if !alertIDs[sc.TriggerAlertID] {
			t.Fatalf("root cause score references stale alert %s", sc.TriggerAlertID)
		}
	}
}
