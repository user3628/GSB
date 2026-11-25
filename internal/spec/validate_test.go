package spec

import "testing"

func TestValidateSnapshotAcceptsSeed(t *testing.T) {
	if err := ValidateSnapshot(seedSnapshot()); err != nil {
		t.Fatalf("valid snapshot rejected: %v", err)
	}
}

func TestValidateSnapshotRejectsInvalidBatch(t *testing.T) {
	s := seedSnapshot()
	s.Components = append(s.Components, Component{ID: "ghost", SubPrefixes: []string{"no.such.branch"}})
	s.Checksum = "bad"
	err := ValidateSnapshot(s)
	if err == nil {
		t.Fatal("expected E_SPEC_INVALID")
	}
	biz, ok := err.(*Error)
	if !ok || biz.Code != CodeSpecInvalid {
		t.Fatalf("want %s, got %v", CodeSpecInvalid, err)
	}
}

func TestKeyPathAndHops(t *testing.T) {
	if err := ValidateKeyPath("a.b."); err == nil {
		t.Fatal("empty segment should be invalid")
	}
	if HopDistance("a.b", "a.b.c.d") != 2 {
		t.Fatal("hop distance mismatch")
	}
	if HopDistance("a.x", "a.b.c") != -1 {
		t.Fatal("non-prefix should return -1")
	}
}

func seedSnapshot() *Snapshot {
	items := []ConfigItem{
		{KeyPath: "a.b.c", Type: "int", Value: []byte("1"), Order: 1},
		{KeyPath: "a.x", Type: "string", Value: []byte(`"v"`), Order: 1},
	}
	comps := []Component{
		{ID: "c1", SubPrefixes: []string{"a.b"}},
		{ID: "c2", SubPrefixes: []string{"a"}, DependsOn: []string{"c1"}},
	}
	return &Snapshot{
		Version: 1, Checksum: Checksum(items, comps),
		Items: items, Components: comps,
	}
}
