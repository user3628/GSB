package acl

import (
	"errors"
	"strings"
	"testing"

	"example.com/ntfsacl/internal/domain"
)

const (
	testSID1 = "S-1-5-21-1000-1"
	testSID2 = "S-1-5-21-1000-2"
)

func desc(aces ...domain.ACE) domain.Descriptor {
	return domain.Descriptor{Schema: "ntfsacl/descriptor/v1", DACLPresent: true, ACEs: aces}
}

func testACE(i int, typ, sid string, mask uint32, f domain.InheritFlags) domain.ACE {
	return domain.ACE{Index: i, Type: typ, SID: sid, AccessMask: mask, Flags: f}
}

func TestValidateMalformedDescriptors(t *testing.T) {
	tests := []struct {
		name string
		d    domain.Descriptor
		kind string
		ace  int
	}{
		{"illegal IO without inheritance", desc(testACE(0, "ALLOW", testSID1, 1, domain.InheritFlags{IO: true})), "FILE", 0},
		{"illegal IO and NP", desc(testACE(0, "ALLOW", testSID1, 1, domain.InheritFlags{CI: true, IO: true, NP: true})), "DIRECTORY", 0},
		{"empty DACL", domain.Descriptor{Schema: "ntfsacl/descriptor/v1", DACLPresent: true}, "DIRECTORY", -1},
		{"absent DACL", domain.Descriptor{Schema: "ntfsacl/descriptor/v1"}, "DIRECTORY", -1},
		{"mask out of range", desc(testACE(0, "ALLOW", testSID1, 0x80000001, domain.InheritFlags{})), "FILE", 0},
		{"zero mask", desc(testACE(0, "ALLOW", testSID1, 0, domain.InheritFlags{})), "FILE", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDescriptor("some/node", tc.kind, tc.d)
			var me *MalformedError
			if !errors.As(err, &me) {
				t.Fatalf("got %v, want E_ACL_MALFORMED", err)
			}
			if me.Code != CodeACLMalformed || me.NodePath != "some/node" || me.ACEIndex != tc.ace {
				t.Fatalf("unexpected error: %+v", me)
			}
		})
	}
}

func TestInheritancePropagationAndEffectiveDecision(t *testing.T) {
	root := domain.Node{
		RelPath: ".", Kind: "DIRECTORY", Depth: 0,
		Descriptor: domain.Descriptor{Schema: "ntfsacl/descriptor/v1", DACLPresent: true, ACEs: []domain.ACE{
			testACE(0, "ALLOW", testSID1, domain.FileReadAccess, domain.InheritFlags{OI: true, CI: true}),
			testACE(1, "ALLOW", testSID2, domain.FileWriteAccess, domain.InheritFlags{CI: true, IO: true}),
			testACE(2, "ALLOW", testSID1, domain.FileExecute, domain.InheritFlags{OI: true, CI: true, NP: true}),
		}, Intents: []domain.Intent{{SID: testSID2, Access: "write", Expected: domain.DecisionNotGranted}}},
	}
	dir := domain.Node{
		RelPath: "dir", Kind: "DIRECTORY", Depth: 1,
		Descriptor: domain.Descriptor{Schema: "ntfsacl/descriptor/v1", DACLPresent: true, ACEs: []domain.ACE{
			testACE(0, "DENY", testSID1, domain.FileReadAccess, domain.InheritFlags{}),
		}, Intents: []domain.Intent{{SID: testSID2, Access: "write", Expected: domain.DecisionAllow}}},
	}
	file := domain.Node{
		RelPath: "dir/file.txt", Kind: "FILE", Depth: 2,
		Descriptor: domain.Descriptor{Schema: "ntfsacl/descriptor/v1", DACLPresent: true, ACEs: []domain.ACE{
			testACE(0, "ALLOW", testSID2, domain.FileReadAccess, domain.InheritFlags{}),
		}, Intents: []domain.Intent{{SID: testSID1, Access: "read", Expected: domain.DecisionAllow}}},
	}
	reb, err := RebuildTree([]domain.Node{file, dir, root}, domain.Policy{KnownSIDs: []string{testSID1, testSID2}})
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]domain.NodeReport{}
	for _, n := range reb.Nodes {
		byPath[n.RelPath] = n
	}
	rootNode := byPath["."]
	if len(rootNode.ACEs) != 3 {
		t.Fatalf("root ACE count=%d", len(rootNode.ACEs))
	}
	if rootNode.ACEs[0].Type != "ALLOW" || rootNode.ACEs[0].Sequence != 0 {
		t.Fatalf("canonical root order unexpected: %+v", rootNode.ACEs)
	}
	// INHERIT_ONLY does not apply to the root itself, while it still propagates.
	rootWrite := findDecision(rootNode.Decisions, testSID2, "write")
	if rootWrite == nil {
		t.Fatal("missing root decision")
	}
	if rootWrite.Effective != domain.DecisionNotGranted {
		t.Fatalf("IO ACE applied at its own node: %+v", *rootWrite)
	}
	dirNode := byPath["dir"]
	dirWrite := findDecision(dirNode.Decisions, testSID2, "write")
	if dirWrite == nil || dirWrite.Effective != domain.DecisionAllow {
		t.Fatalf("inherited IO ACE not applied to child: %+v", dirWrite)
	}
	// NP clears all propagation flags on the immediate child.
	for _, ace := range dirNode.ACEs {
		if ace.SourcePath == "." && ace.OriginalIndex == 2 {
			if !ace.Flags.Empty() {
				t.Fatalf("NP flags not cleared: %+v", ace.Flags)
			}
		}
	}
	fileNode := byPath["dir/file.txt"]
	for _, ace := range fileNode.ACEs {
		if ace.SourcePath == "." && ace.OriginalIndex == 2 {
			t.Fatalf("NP ACE propagated beyond one level: %+v", ace)
		}
	}
	fileRead := findDecision(fileNode.Decisions, testSID1, "read")
	if fileRead == nil || fileRead.Effective != domain.DecisionAllow || !fileRead.Matched {
		t.Fatalf("inherited file decision: %+v", fileRead)
	}
	if len(fileRead.Sources) == 0 || fileRead.Sources[0].ACENodePath != "." || fileRead.Sources[0].ACEIndex != 0 {
		t.Fatalf("decision source chain does not point to root ACE: %+v", fileRead.Sources)
	}
	if len(fileNode.ACEs) < 2 {
		t.Fatalf("file inherited ACEs missing: %+v", fileNode.ACEs)
	}
	foundChain := false
	for _, ace := range fileNode.ACEs {
		if ace.SourcePath == "." && ace.OriginalIndex == 0 && len(ace.SourceChain) >= 3 {
			foundChain = true
			if ace.SourceChain[0].ACEIndex != 0 || ace.SourceChain[len(ace.SourceChain)-1].NodePath != "dir/file.txt" {
				t.Fatalf("bad source chain: %+v", ace.SourceChain)
			}
		}
	}
	if !foundChain {
		t.Fatalf("could not find root-to-file source chain: %+v", fileNode.ACEs)
	}
}

func TestDenyDominatesAllow(t *testing.T) {
	n := domain.Node{RelPath: ".", Kind: "DIRECTORY", Descriptor: domain.Descriptor{
		Schema: "ntfsacl/descriptor/v1", DACLPresent: true,
		ACEs: []domain.ACE{
			testACE(0, "ALLOW", testSID1, domain.FileReadAccess, domain.InheritFlags{}),
			testACE(1, "DENY", testSID1, domain.FileReadAccess, domain.InheritFlags{}),
		},
		Intents: []domain.Intent{{SID: testSID1, Access: "read", Expected: domain.DecisionDeny}},
	}}
	reb, err := RebuildTree([]domain.Node{n}, domain.Policy{KnownSIDs: []string{testSID1}})
	if err != nil {
		t.Fatal(err)
	}
	d := reb.Nodes[0].Decisions[0]
	if d.Effective != domain.DecisionDeny || !d.Matched {
		t.Fatalf("deny did not dominate: %+v", d)
	}
	if len(reb.Conflicts) != 1 || reb.Conflicts[0].Code != CodeDenyOverride {
		t.Fatalf("raw allow-before-deny inversion not detected: %+v", reb.Conflicts)
	}
}

func findDecision(ds []domain.Decision, sid, access string) *domain.Decision {
	for i := range ds {
		if ds[i].SID == sid && strings.EqualFold(ds[i].Access, access) {
			return &ds[i]
		}
	}
	return nil
}

func TestMultipleAllowACEsCombineForRequestedMask(t *testing.T) {
	n := domain.Node{RelPath: ".", Kind: "DIRECTORY", Descriptor: domain.Descriptor{
		Schema: "ntfsacl/descriptor/v1", DACLPresent: true,
		ACEs: []domain.ACE{
			testACE(0, "ALLOW", testSID1, domain.FileReadAccess, domain.InheritFlags{}),
			testACE(1, "ALLOW", testSID1, domain.FileWriteAccess, domain.InheritFlags{}),
		},
		Intents: []domain.Intent{{SID: testSID1, Access: "full", Expected: domain.DecisionNotGranted}},
	}}
	reb, err := RebuildTree([]domain.Node{n}, domain.Policy{KnownSIDs: []string{testSID1}})
	if err != nil {
		t.Fatal(err)
	}
	d := reb.Nodes[0].Decisions[0]
	if d.AllowedMask != (domain.FileReadAccess | domain.FileWriteAccess) {
		t.Fatalf("allow masks did not combine: %+v", d)
	}
	// The advertised full mask still contains unrelated bits, so the
	// combination is not a full grant; importantly, both ACEs contributed.
	if len(d.Contributors) != 2 {
		t.Fatalf("partial allow ACEs were skipped: %+v", d)
	}
}
