package smuggle

import (
	"bytes"
	"path/filepath"
	"testing"
)

func runReport(t *testing.T, workers int) []byte {
	t.Helper()
	dir := t.TempDir()
	if err := WriteSamples(dir); err != nil {
		t.Fatal(err)
	}
	conns, err := LoadSamples(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := DefaultConfig()
	cfg.Workers = workers
	eng := NewEngine(cfg, st)
	rep := BuildReport(cfg, eng.Run(conns))
	data, err := MarshalReport(rep)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReportDeterministicAcrossRunsAndConcurrency(t *testing.T) {
	a := runReport(t, 1)
	b := runReport(t, 8)
	if !bytes.Equal(a, b) {
		t.Fatalf("report differs across concurrency\n--- 1 ---\n%s\n--- 8 ---\n%s", a, b)
	}
}

func TestReportContents(t *testing.T) {
	data := runReport(t, 4)
	for _, want := range [][]byte{
		[]byte(`"marker": "SAMPLE-0001"`),
		[]byte(`"conn": "ambiguity/clte"`),
		[]byte(`"verdict": "ENCODING_AMBIGUITY"`),
		[]byte(`"code": "E_SMUGGLE_AMBIGUITY"`),
		[]byte(`"conn": "normal/consistent"`),
		[]byte(`"verdict": "CONSISTENT"`),
		[]byte(`"verdict": "COUNT_MISMATCH"`),
		[]byte(`"verdict": "BOUNDARY_MISMATCH"`),
		[]byte(`"path": "precise"`),
		[]byte(`"snapshot_version": 1`),
	} {
		if !bytes.Contains(data, want) {
			t.Fatalf("report missing %s", want)
		}
	}
}
