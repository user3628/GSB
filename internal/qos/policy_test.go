package qos

import (
	"testing"
)

func ptr(s string) *string { return &s }

func testQueue(id, parent string, cir, pir, cbs, pbs int64, priority int) Queue {
	q := Queue{
		ID: id, CIRBPS: cir, PIRBPS: pir, CBSBytes: cbs, PBSBytes: pbs, Priority: priority,
		Match: MatchRule{SrcPortMin: 0, SrcPortMax: 65535, DSCPValues: []int{0}},
	}
	if parent != "" {
		q.ParentID = ptr(parent)
	}
	return q
}

func TestNormalizeAndValidateFailures(t *testing.T) {
	valid := testQueue("a", "", 1000, 2000, 10, 20, 1)
	cases := []struct {
		name   string
		queues []Queue
		code   string
	}{
		{"empty", nil, CodePolicyInvalid},
		{"duplicate", []Queue{valid, valid}, CodePolicyInvalid},
		{"cbs_gt_pbs", []Queue{testQueue("a", "", 1000, 2000, 21, 20, 1)}, CodePolicyInvalid},
		{"zero_rate", []Queue{testQueue("a", "", 0, 2000, 10, 20, 1)}, CodePolicyInvalid},
		{"cir_gt_pir", []Queue{testQueue("a", "", 3000, 2000, 10, 20, 1)}, CodePolicyInvalid},
		{"missing_parent", []Queue{testQueue("a", "ghost", 1000, 2000, 10, 20, 1)}, CodePolicyInvalid},
		{"priority", []Queue{testQueue("a", "", 1000, 2000, 10, 20, 8)}, CodePolicyInvalid},
		{"bad_ports", []Queue{{ID: "a", CIRBPS: 1, PIRBPS: 1, CBSBytes: 1, PBSBytes: 1, Priority: 1,
			Match: MatchRule{SrcPortMin: 100, SrcPortMax: 10}}}, CodePolicyInvalid},
		{"bad_dscp", []Queue{{ID: "a", CIRBPS: 1, PIRBPS: 1, CBSBytes: 1, PBSBytes: 1, Priority: 1,
			Match: MatchRule{SrcPortMin: 0, SrcPortMax: 1, DSCPValues: []int{64}}}}, CodePolicyInvalid},
		{"cycle", []Queue{
			testQueue("a", "b", 1000, 2000, 10, 20, 1),
			testQueue("b", "a", 1000, 2000, 10, 20, 1),
		}, CodeDependencyCycle},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, biz := NormalizeAndValidate(tc.queues)
			if biz == nil {
				t.Fatal("expected business error")
			}
			if biz.Code != tc.code {
				t.Fatalf("code=%s want %s (%+v)", biz.Code, tc.code, biz.Context)
			}
		})
	}
}

func TestCycleReportsIDs(t *testing.T) {
	_, _, biz := NormalizeAndValidate([]Queue{
		testQueue("root", "", 1000, 2000, 10, 20, 1),
		testQueue("a", "c", 1000, 2000, 10, 20, 1),
		testQueue("b", "a", 1000, 2000, 10, 20, 1),
		testQueue("c", "b", 1000, 2000, 10, 20, 1),
	})
	if biz == nil || biz.Code != CodeDependencyCycle {
		t.Fatalf("got %+v, want cycle", biz)
	}
	ids, ok := biz.Context["queue_ids"].([]string)
	if !ok || len(ids) != 3 {
		t.Fatalf("cycle ids missing: %#v", biz.Context)
	}
	want := map[string]bool{"a": true, "b": true, "c": true}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected cycle id %s", id)
		}
	}
}

func TestEffectiveRateInheritanceAndSource(t *testing.T) {
	queues := []Queue{
		testQueue("root", "", 1000, 2000, 10, 20, 1),
		testQueue("mid", "root", 900, 1800, 10, 20, 2),
		testQueue("leaf", "mid", 800, 1600, 10, 20, 3),
	}
	_, policy, biz := NormalizeAndValidate(queues)
	if biz != nil {
		t.Fatal(biz)
	}
	eff := ComputeEffectiveRates(policy)
	if got := eff["root"].EffectiveCIRBPS; got != 1000 {
		t.Fatalf("root cir=%d", got)
	}
	if got := eff["mid"].EffectiveCIRBPS; got != 900 || eff["mid"].CIRSRC.Kind != "declared" {
		t.Fatalf("mid=%+v", eff["mid"])
	}
	if got := eff["leaf"].EffectiveCIRBPS; got != 800 || eff["leaf"].CIRSRC.Kind != "declared" {
		t.Fatalf("leaf=%+v", eff["leaf"])
	}

	queues[0].CIRBPS = 700
	_, policy, _ = NormalizeAndValidate(queues)
	eff = ComputeEffectiveRates(policy)
	if got := eff["mid"].EffectiveCIRBPS; got != 700 || eff["mid"].CIRSRC.Kind != "parent_limit" {
		t.Fatalf("mid after clamp=%+v", eff["mid"])
	}
	if got := eff["leaf"].EffectiveCIRBPS; got != 700 {
		t.Fatalf("leaf after clamp=%d", got)
	}
	if got := eff["mid"].ParentChain; len(got) != 1 || got[0] != "root" {
		t.Fatalf("chain=%v", got)
	}
}

func TestSamplesAreDeterministic(t *testing.T) {
	d1 := t.TempDir()
	d2 := t.TempDir()
	m1, err := EnsureSamples(d1, DefaultSampleSeed)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := EnsureSamples(d2, DefaultSampleSeed)
	if err != nil {
		t.Fatal(err)
	}
	if m1.PolicySHA256 != m2.PolicySHA256 || m1.TrafficSHA256 != m2.TrafficSHA256 {
		t.Fatalf("sample hashes differ: %+v %+v", m1, m2)
	}
	if m1.TrafficPackets != m2.TrafficPackets || m1.QueueCount != 7 {
		t.Fatalf("unexpected sample metadata: %+v", m1)
	}
}
