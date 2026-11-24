package adjudicator

import (
	"bytes"
	"testing"

	"keylife/internal/model"
)

func testState() State {
	policies := map[string]model.SubjectPolicy{
		"alpha": {Subject: "alpha", MinStrengthBits: 256, RotationInterval: model.D(100), DefaultLifetime: model.D(1000), MaxPendingRequests: 8},
	}
	keys := make(map[string]model.Key)
	for i := 1; i <= 5; i++ {
		id := "key-" + string(rune('0'+i))
		keys[id] = model.Key{
			ID: id, Subject: "alpha", Algorithm: "aes-256-gcm", Family: "AES", StrengthBits: 256,
			CreatedAt: model.T(10), ExpiresAt: model.T(1000), RotationInterval: model.D(100), Phase: model.PhaseActive,
		}
	}
	return State{Now: model.T(50), Policies: policies, Keys: keys}
}

func requests(count int) []model.DecisionRequest {
	out := make([]model.DecisionRequest, 0, count)
	for i := 1; i <= count; i++ {
		id := "key-" + string(rune('0'+i))
		out = append(out, model.DecisionRequest{RequestID: "r-" + id, Subject: "alpha", KeyID: id, Actor: "alpha", Operation: "use"})
	}
	return out
}

func TestAdaptivePathSelection(t *testing.T) {
	state := testState()
	below, err := Adjudicate(requests(3), state, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(below.Paths) != 1 || below.Paths[0].Selected != PathAccurate {
		t.Fatalf("below threshold selected %+v", below.Paths)
	}
	above, err := Adjudicate(requests(5), state, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(above.Paths) != 1 || above.Paths[0].Selected != PathStreaming {
		t.Fatalf("above threshold selected %+v", above.Paths)
	}
	if above.PendingKeys != 5 || below.PendingKeys != 3 {
		t.Fatalf("unexpected pending counts: below=%d above=%d", below.PendingKeys, above.PendingKeys)
	}
}

func TestPathsAreByteIdentical(t *testing.T) {
	state := testState()
	input := requests(5)
	input[0].Algorithm = "aes-128-gcm"
	input[1].Actor = "outside"
	comparison, err := ComparePaths(input, state, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !comparison.ByteIdentical {
		t.Fatal("accurate and streaming paths differ")
	}
	if comparison.Accurate.Digest != comparison.Streaming.Digest {
		t.Fatalf("digests differ: %s %s", comparison.Accurate.Digest, comparison.Streaming.Digest)
	}
	accurateBytes, _ := model.CanonicalJSON(comparison.Accurate)
	streamingBytes, _ := model.CanonicalJSON(comparison.Streaming)
	if bytes.Equal(accurateBytes, streamingBytes) {
		t.Fatal("path metadata should differ even though decisions and audits match")
	}
	if len(comparison.Accurate.DecisionCodes) != 5 || comparison.Accurate.DecisionCodes[0] != model.CodeWeakAlgo || comparison.Accurate.DecisionCodes[1] != model.CodeSubjectScope {
		t.Fatalf("unexpected code sequence: %v", comparison.Accurate.DecisionCodes)
	}
}

func TestRepeatedAdjudicationIsStable(t *testing.T) {
	state := testState()
	first, err := Adjudicate(requests(5), state, 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Adjudicate(requests(5), state, 999999)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("decision digest changed with threshold: %s != %s", first.Digest, second.Digest)
	}
}

func TestEvaluationCodes(t *testing.T) {
	state := testState()
	key := state.Keys["key-1"]
	key.ExpiresAt = model.T(40)
	state.Keys[key.ID] = key
	cases := []struct {
		name string
		req  model.DecisionRequest
		want model.Code
	}{
		{"scope", model.DecisionRequest{Subject: "alpha", KeyID: "key-1", Actor: "beta", Operation: "use"}, model.CodeSubjectScope},
		{"expired", model.DecisionRequest{Subject: "alpha", KeyID: "key-1", Actor: "alpha", Operation: "use"}, model.CodeKeyExpired},
		{"weak", model.DecisionRequest{Subject: "alpha", KeyID: "key-2", Actor: "alpha", Operation: "use", Algorithm: "aes-128-gcm"}, model.CodeWeakAlgo},
		{"phase", model.DecisionRequest{Subject: "alpha", KeyID: "key-2", Actor: "alpha", Operation: "archive"}, model.CodePhaseTransition},
		{"missing", model.DecisionRequest{Subject: "alpha", KeyID: "missing", Actor: "alpha", Operation: "use"}, model.CodeKeyNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Evaluate(state, tc.req)
			if got.Code != tc.want {
				t.Fatalf("code=%s want=%s checks=%+v", got.Code, tc.want, got.Checks)
			}
		})
	}
}
