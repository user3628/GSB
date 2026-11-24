package sample

import (
	"strings"
	"testing"

	"privescworkbench/internal/domain"
)

func TestParseManifestValidAndOffsets(t *testing.T) {
	line := `{"pid":1,"parent_pid":0,"exe_path":"C:\\a.exe","account":"User","inherited_from":0,"inherited_mask":0,"service_bin":"","service_write":0}` + "\n"
	procs, err := ParseManifest([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if len(procs) != 1 || procs[0].PID != 1 || procs[0].SourceOffset != 0 {
		t.Fatalf("unexpected %+v", procs)
	}
	line2 := strings.Replace(line, `"pid":1`, `"pid":2`, 1)
	second := line + line2
	procs, err = ParseManifest([]byte(second))
	if err != nil {
		t.Fatal(err)
	}
	if procs[1].SourceOffset != int64(len(line)) {
		t.Fatalf("second line offset=%d want %d", procs[1].SourceOffset, len(line))
	}
}

func TestParseManifestRejectsBadInputs(t *testing.T) {
	base := `{"pid":1,"parent_pid":0,"exe_path":"C:\\a.exe","account":"%s","inherited_from":0,"inherited_mask":%d,"service_bin":"","service_write":%d}`
	cases := []struct {
		name  string
		acct  string
		mask  uint64
		write int
	}{
		{"bad account", "Ghost", 0, 0},
		{"mask range", "User", MaskMax + 1, 0},
		{"write grade", "User", 0, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := strings.NewReader("")
			_ = raw
			line := []byte(sprintf(base, tc.acct, tc.mask, tc.write) + "\n")
			_, err := ParseManifest(line)
			ce := wantCode(t, err, domain.CodeSampleParse)
			if ce.Offset < 0 {
				t.Fatal("source offset must be recorded")
			}
		})
	}
}

func TestDuplicatePIDRejected(t *testing.T) {
	line := `{"pid":1,"parent_pid":0,"exe_path":"C:\\a.exe","account":"User","inherited_from":0,"inherited_mask":0,"service_bin":"","service_write":0}` + "\n"
	_, err := ParseManifest([]byte(line + line))
	wantCode(t, err, domain.CodeSampleParse)
}

func TestEventStreamRoundTripAndTruncation(t *testing.T) {
	gen := Generate(1, ScenarioBaseline)
	events, err := ParseEventStream(gen.StreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(gen.Bundle.Events) {
		t.Fatalf("event count %d", len(events))
	}
	if events[0].SourceOffset == 0 || events[1].SourceOffset <= events[0].SourceOffset {
		t.Fatalf("offsets must advance: %+v", events[:2])
	}
	// truncated block must fail with offset
	bad := append(append([]byte{}, gen.StreamBytes[:8]...), 'x')
	_, err = ParseEventStream(bad)
	wantCode(t, err, domain.CodeSampleParse)
}

func TestMaskInjectionRejectedAtParse(t *testing.T) {
	gen := Generate(1, ScenarioMaskBad)
	_, err := ParseEventStream(gen.StreamBytes)
	ce := wantCode(t, err, domain.CodeSampleParse)
	if !strings.Contains(ce.Msg, "mask") {
		t.Fatalf("unexpected message %q", ce.Msg)
	}
}

func wantCode(t *testing.T, err error, code domain.BizCode) *domain.CodeError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s", code)
	}
	ce, ok := err.(*domain.CodeError)
	if !ok || ce.Code != code {
		t.Fatalf("got %v, want %s", err, code)
	}
	return ce
}

func sprintf(format string, args ...any) string {
	return stringsTrim(fmtSprintf(format, args...))
}
