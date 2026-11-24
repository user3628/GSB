package model

import (
	"errors"
	"testing"

	"hostacl-workbench/internal/apperr"
)

func TestDeterministicSampleValidates(t *testing.T) {
	sample := DeterministicSample()
	policy, err := ValidateSnapshot(*sample.SnapshotV1)
	if err != nil {
		t.Fatalf("snapshot validation failed: %v", err)
	}
	if policy.Digest != sample.SnapshotV1.Digest {
		t.Fatalf("digest changed: got %s want %s", policy.Digest, sample.SnapshotV1.Digest)
	}
	current := policy
	for _, inc := range []*Increment{sample.IncrementV2, sample.IncrementV3, sample.IncrementV4} {
		if _, err := ValidateIncrementDigest(inc); err != nil {
			t.Fatalf("increment validation failed: %v", err)
		}
		current, err = ApplyIncrement(current, inc)
		if err != nil {
			t.Fatalf("increment apply failed: %v", err)
		}
	}
	if current.Version != 4 {
		t.Fatalf("version=%d, want 4", current.Version)
	}
}

func TestInvalidPathRejectedAtomically(t *testing.T) {
	sample := DeterministicSample()
	bad := ClonePolicy(sample.SnapshotV1)
	bad.Paths[0].Path = "../escape"
	bad.Digest = SnapshotDigest(bad)
	_, err := ValidateSnapshot(*bad)
	if err == nil {
		t.Fatal("expected validation failure")
	}
	var appErr *apperr.Error
	if !errors.As(err, &appErr) || appErr.Code != apperr.SpecInvalid {
		t.Fatalf("got %v, want E_SPEC_INVALID", err)
	}
}

func TestGroupMemberReferencesAndCycles(t *testing.T) {
	policy := &Policy{
		Version: 1, GeneratedAt: "2026-11-25T00:00:00Z",
		Subjects: []Subject{
			{ID: "user", Type: SubjectUser},
			{ID: "g1", Type: SubjectGroup, Members: []string{"g2"}},
			{ID: "g2", Type: SubjectGroup, Members: []string{"g1"}},
		},
		Paths: []ManagedPath{{Path: "x", Kind: "directory"}},
	}
	policy.Digest = SnapshotDigest(policy)
	_, err := ValidateSnapshot(*policy)
	var appErr *apperr.Error
	if !errors.As(err, &appErr) || appErr.Code != apperr.SpecInvalid {
		t.Fatalf("cycle not rejected: %v", err)
	}
	policy.Subjects[1].Members = []string{"missing"}
	policy.Subjects[2].Members = nil
	policy.Digest = SnapshotDigest(policy)
	_, err = ValidateSnapshot(*policy)
	if !errors.As(err, &appErr) || appErr.Code != apperr.SpecInvalid {
		t.Fatalf("missing member not rejected: %v", err)
	}
}

func TestEffectiveRightsCascade(t *testing.T) {
	sample := DeterministicSample()
	views1 := viewsByKey(ComputeViews(sample.SnapshotV1))
	if got := views1["u-api\x00data"].Rights; !got.Write || got.Read {
		t.Fatalf("v1 u-api/data=%+v, want write only", got)
	}
	if got := views1["g-platform\x00apps/api/deep/state.json"].Rights; !got.Read {
		t.Fatalf("v1 g-platform/deep read=false, want inherited read")
	}
	policy2, err := ApplyIncrement(sample.SnapshotV1, sample.IncrementV2)
	if err != nil {
		t.Fatal(err)
	}
	views2 := viewsByKey(ComputeViews(policy2))
	if got := views2["u-api\x00data"].Rights; !got.Read || !got.Write {
		t.Fatalf("v2 u-api/data=%+v, want read+write", got)
	}
	cascade := CascadeDiff(sample.SnapshotV1, policy2, mapValues(views1), mapValues(views2))
	if !contains(cascade.AffectedSubjects, "u-api") || !contains(cascade.AffectedPaths, "data") {
		t.Fatalf("unexpected cascade: %+v", cascade)
	}
	policy3, err := ApplyIncrement(policy2, sample.IncrementV3)
	if err != nil {
		t.Fatal(err)
	}
	views3 := viewsByKey(ComputeViews(policy3))
	if got := views3["g-platform\x00apps/api/deep/state.json"].Rights; got.Read {
		t.Fatalf("v3 g-platform/deep read=true after inheritance cut")
	}
}

func TestDenyPrecedence(t *testing.T) {
	policy := &Policy{
		Version: 1, GeneratedAt: "2026-11-25T00:00:00Z",
		Subjects: []Subject{{ID: "u", Type: SubjectUser}},
		Paths: []ManagedPath{{Path: "f", Kind: "file", ACL: []ACE{
			{SubjectID: "u", Effect: "allow", Rights: []string{"read", "write"}},
			{SubjectID: "u", Effect: "deny", Rights: []string{"write"}},
		}}},
	}
	policy.Digest = SnapshotDigest(policy)
	valid, err := ValidateSnapshot(*policy)
	if err != nil {
		t.Fatal(err)
	}
	view := ComputeViews(valid)[0]
	if !view.Rights.Read || view.Rights.Write {
		t.Fatalf("deny precedence failed: %+v", view.Rights)
	}
}

func TestWindowsSeparatorsAreCanonicalized(t *testing.T) {
	policy := &Policy{
		Version: 1, GeneratedAt: "2026-11-25T00:00:00Z",
		Subjects: []Subject{{ID: "u", Type: SubjectUser}},
		Paths:    []ManagedPath{{Path: `apps\api\file.txt`, Kind: "file"}},
	}
	policy.Digest = SnapshotDigest(policy)
	validated, err := ValidateSnapshot(*policy)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Paths[0].Path != "apps/api/file.txt" {
		t.Fatalf("path not normalized: %s", validated.Paths[0].Path)
	}
	bad := &Policy{
		Version: 1, GeneratedAt: "2026-11-25T00:00:00Z",
		Subjects: []Subject{{ID: "u", Type: SubjectUser}},
		Paths:    []ManagedPath{{Path: `..\escape`, Kind: "file"}},
	}
	bad.Digest = SnapshotDigest(bad)
	if _, err := ValidateSnapshot(*bad); err == nil {
		t.Fatal("Windows up-segment was not rejected")
	}
}

func viewsByKey(views []EffectiveView) map[string]EffectiveView {
	out := map[string]EffectiveView{}
	for _, view := range views {
		out[view.SubjectID+"\x00"+view.Path] = view
	}
	return out
}

func mapValues(m map[string]EffectiveView) []EffectiveView {
	out := make([]EffectiveView, 0, len(m))
	for _, view := range m {
		out = append(out, view)
	}
	return out
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
