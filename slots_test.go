package prefixcache

import (
	"reflect"
	"testing"
)

func collisionPolicy() Policy {
	return Policy{
		Version:               1,
		Granularity:           4,
		MinPrefixLen:          4,
		MergeThreshold:        0.5,
		MaxPrefixesPerRequest: 1,
		SlotCount:             1,
		OccupancyBudget:       2,
	}
}

func collisionRequests() []Request {
	return []Request{
		{ID: "req-a", TemplateID: "t", Messages: []Message{{Turn: 1, Role: "user", Language: "en-US", Text: "aaaaaaaa"}}},
		{ID: "req-b", TemplateID: "t", Messages: []Message{{Turn: 1, Role: "user", Language: "en-US", Text: "bbbbbbbb"}}},
	}
}

func TestSlotConflictDeterministicEvidence(t *testing.T) {
	var runs []struct {
		digest    string
		evictions []Eviction
	}
	for i := 0; i < 2; i++ {
		engine := openTestEngine(t, testDBPath(t))
		result, err := engine.Initialize(testContext(t), collisionPolicy(), collisionRequests())
		if err != nil {
			t.Fatalf("run %d initialize: %v", i, err)
		}
		if result.SlotCount != 1 {
			t.Fatalf("run %d slot count=%d", i, result.SlotCount)
		}
		var conflicts []Eviction
		for _, eviction := range result.Evictions {
			if eviction.Reason == "slot_conflict" {
				conflicts = append(conflicts, eviction)
			}
		}
		if len(conflicts) != 1 {
			t.Fatalf("run %d conflicts=%+v", i, result.Evictions)
		}
		conflict := conflicts[0]
		if conflict.SlotNo != 0 || conflict.PrefixKey == "" || conflict.Threshold != 0.5 || conflict.PrefixChars != 8 {
			t.Fatalf("run %d incomplete conflict evidence: %+v", i, conflict)
		}
		if conflict.WinnerPrefixKey == "" || conflict.WinnerRequestID == "" || conflict.Note == "" {
			t.Fatalf("run %d missing conflict winner/note: %+v", i, conflict)
		}
		runs = append(runs, struct {
			digest    string
			evictions []Eviction
		}{digest: result.SlotDigest, evictions: conflicts})
	}
	if runs[0].digest != runs[1].digest || !reflect.DeepEqual(runs[0].evictions, runs[1].evictions) {
		t.Fatalf("slot conflict decision is not deterministic:\nrun0=%+v\nrun1=%+v", runs[0], runs[1])
	}
}

func TestNoOpPatchRejected(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	initializeDataset(t, engine, DefaultSeed)
	threshold := DefaultPolicy().MergeThreshold
	_, err := engine.ApplyPatch(testContext(t), 1, PolicyPatch{MergeThreshold: &threshold})
	requireCode(t, err, CodeInvalidPolicy)
	status, err := engine.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentVersion != 1 {
		t.Fatalf("no-op patch advanced version to %d", status.CurrentVersion)
	}
}

func TestHistoricalSnapshotRemainsImmutable(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	initializeDataset(t, engine, DefaultSeed)
	before, err := engine.Snapshot(testContext(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	threshold := 0.8
	if _, err := engine.ApplyPatch(testContext(t), 1, PolicyPatch{MergeThreshold: &threshold}); err != nil {
		t.Fatal(err)
	}
	after, err := engine.Snapshot(testContext(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	if before.VersionFingerprint != after.VersionFingerprint || before.SlotDigest != after.SlotDigest || len(before.Slots) != len(after.Slots) {
		t.Fatalf("historical snapshot changed after a new commit: before=%+v after=%+v", before, after)
	}
	current, err := engine.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if current.CurrentVersion != 2 {
		t.Fatalf("current version=%d", current.CurrentVersion)
	}
}
