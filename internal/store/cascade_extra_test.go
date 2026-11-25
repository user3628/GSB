package store_test

import (
	"testing"

	"auditdrift/internal/model"
	"auditdrift/internal/store"
)

func TestExpectationAndDependencyEdgeChangesCascade(t *testing.T) {
	st := openTestStore(t)
	set := initializeSample(t, st, 100)

	pwd := findBaseline(set.Baselines, "ACCT_PWD_MINLEN")
	pwd.Expected.Interval.Min = 16

	ssh := findBaseline(set.Baselines, "SVC_SSH_START")
	ssh.DependsOn = []string{"REG_SAM_ACL"}

	result, err := st.CommitIncrement(store.Increment{
		SessionID: "s1", BaseVersion: 1, Reason: "expectation and edge change",
		BaselineChanges: []store.BaselineChange{
			{Op: "upsert", Baseline: &pwd},
			{Op: "upsert", Baseline: &ssh},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 2 {
		t.Fatalf("version=%d", result.Version)
	}

	conclusions, err := st.ListConclusions("s1", "host-a")
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]model.Conclusion)
	for _, c := range conclusions {
		byID[c.BaselineID] = c
	}
	if byID["ACCT_PWD_MINLEN"].DriftCategory != model.DriftValueOffset {
		t.Fatalf("tightened interval should make host-a value 14 drift: %+v", byID["ACCT_PWD_MINLEN"])
	}

	hostB, err := st.ListConclusions("s1", "host-b")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range hostB {
		byID[c.BaselineID] = c
	}
	sshConclusion := byID["SVC_SSH_START"]
	if len(sshConclusion.Propagation) == 0 || sshConclusion.Propagation[0].FromBaselineID != "REG_SAM_ACL" {
		t.Fatalf("new dependency edge did not propagate: %+v", sshConclusion.Propagation)
	}
	if sshConclusion.ViewVersion != 2 {
		t.Fatalf("view=%d want2", sshConclusion.ViewVersion)
	}
}
