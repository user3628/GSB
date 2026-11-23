package capver

import (
	"bytes"
	"testing"
)

func buildTestReport() *Report {
	return &Report{
		Component:       ComponentID,
		PolicyName:      SampleSeed,
		PolicyHash:      GenerateSamplePolicy().Hash(),
		SnapshotVersion: 1,
		VersionChain:    []int64{1},
		Recovery:        RecoveryClean,
		Events: []Event{
			{SessionID: "sess-1", Kind: "started"},
			{SessionID: "sess-1", Kind: "finished"},
		},
		Sessions: []SessionRecord{{
			SessionID: "sess-1",
			Processes: []ProcessRecord{
				{ProcID: "proc-b", Declared: []Capability{CapFileRead}, Code: CodeOK,
					Probes: []ProbeResult{{Token: CapFileRead, Target: "<WORK:proc-b>/seed.txt", Allowed: true, Expected: true}}},
				{ProcID: "proc-a", Declared: []Capability{CapFileWrite}, Code: CodeCapDenied,
					Probes: []ProbeResult{{Token: CapFileWrite, Target: "<OUTSIDE:proc-a>/x", Allowed: false, Expected: false, DeniedBy: "component"}}},
			},
		}},
	}
}

func TestReportDeterministicBytes(t *testing.T) {
	r1 := buildTestReport()
	r2 := buildTestReport()
	// 打乱输入顺序，输出仍应一致。
	r2.Sessions[0].Processes[0], r2.Sessions[0].Processes[1] = r2.Sessions[0].Processes[1], r2.Sessions[0].Processes[0]
	r2.Events[0], r2.Events[1] = r2.Events[1], r2.Events[0]
	if !bytes.Equal(r1.Bytes(), r2.Bytes()) {
		t.Fatalf("report bytes differ:\n%s\n---\n%s", r1.Bytes(), r2.Bytes())
	}
	// 进程应按 ID 排序：proc-a 在前。
	if !bytes.Contains(r1.Bytes(), []byte(`"procId": "proc-a"`)) {
		t.Fatal("report missing proc-a")
	}
	idxA := bytes.Index(r1.Bytes(), []byte(`"procId": "proc-a"`))
	idxB := bytes.Index(r1.Bytes(), []byte(`"procId": "proc-b"`))
	if idxA > idxB {
		t.Fatal("processes not sorted by id")
	}
}

func TestPathNormalizer(t *testing.T) {
	n := NewPathNormalizer()
	n.Map("/tmp/capver-sess-1-123", "<SESSION>")
	n.Map("/tmp/capver-sess-1-123/proc-a", "<WORK:proc-a>")
	got := n.Normalize(`open /tmp/capver-sess-1-123/proc-a\out.txt: denied`)
	want := "open <WORK:proc-a>/out.txt: denied"
	if got != want {
		t.Fatalf("normalize: got %q want %q", got, want)
	}
}
