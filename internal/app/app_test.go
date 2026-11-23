package app

import (
	"bytes"
	"path/filepath"
	"testing"

	"iotrust/internal/engine"
	"iotrust/internal/samples"
)

func run(t *testing.T, scenario string, workers int) *engine.Report {
	t.Helper()
	rep, _, err := RunScenario(t.TempDir(), scenario, workers)
	if err != nil {
		t.Fatalf("scenario %s: %v", scenario, err)
	}
	return rep
}

func deviceCodes(rep *engine.Report) map[string]engine.Code {
	out := map[string]engine.Code{}
	for _, d := range rep.Devices {
		out[d.TenantID+"/"+d.DeviceID] = d.Code
	}
	return out
}

func TestNormalScenario(t *testing.T) {
	rep := run(t, "normal", 0)
	for _, d := range rep.Devices {
		if d.Code != engine.CodeOK {
			t.Fatalf("device %s code=%s", d.DeviceID, d.Code)
		}
		if d.Cred == nil || d.Cred.VerifyCode != engine.CodeOK {
			t.Fatalf("device %s cred=%+v", d.DeviceID, d.Cred)
		}
	}
	if rep.SnapshotVersion != 2 {
		t.Fatalf("snapshot version=%d (two rounds expected)", rep.SnapshotVersion)
	}
	if len(rep.Snapshots) != 2 || !rep.Snapshots[0].Continuous || !rep.Snapshots[1].Continuous {
		t.Fatalf("chain=%+v", rep.Snapshots)
	}
	if rep.Fault != nil {
		t.Fatalf("fault=%+v", rep.Fault)
	}
}

func TestFingerprintDupInjection(t *testing.T) {
	rep := run(t, "fingerprint-dup", 0)
	codes := deviceCodes(rep)
	if codes["tenant-a/dev-x2"] != engine.CodeDeviceInvalid {
		t.Fatalf("codes=%v", codes)
	}
	if codes["tenant-a/dev-a1"] != engine.CodeOK || codes["tenant-a/dev-a3"] != engine.CodeOK {
		t.Fatalf("unaffected devices must stay OK: %v", codes)
	}
	if rep.SnapshotVersion != 0 {
		t.Fatalf("snapshot advanced on fault round: %d", rep.SnapshotVersion)
	}
}

func TestScoreOverflowInjection(t *testing.T) {
	rep := run(t, "score-overflow", 0)
	codes := deviceCodes(rep)
	if codes["tenant-a/dev-o1"] != engine.CodeScoreOverflow {
		t.Fatalf("codes=%v", codes)
	}
	if codes["tenant-a/dev-o2"] != engine.CodeOK {
		t.Fatalf("codes=%v", codes)
	}
	if rep.SnapshotVersion != 0 {
		t.Fatalf("snapshot advanced on fault round: %d", rep.SnapshotVersion)
	}
}

func TestCredExpiredInjection(t *testing.T) {
	rep := run(t, "cred-expired", 0)
	for _, d := range rep.Devices {
		if d.Code != engine.CodeOK {
			t.Fatalf("issuance failed: %s %s", d.DeviceID, d.Code)
		}
		if d.Cred == nil || d.Cred.VerifyCode != engine.CodeCredExpired {
			t.Fatalf("device %s verify=%+v", d.DeviceID, d.Cred)
		}
	}
	if rep.SnapshotVersion != 0 {
		t.Fatalf("snapshot advanced on fault round: %d", rep.SnapshotVersion)
	}
}

func TestSnapshotGapInjection(t *testing.T) {
	rep := run(t, "snapshot-gap", 0)
	if rep.Fault == nil || rep.Fault.Code != engine.CodeSnapshotVersion {
		t.Fatalf("fault=%+v", rep.Fault)
	}
	if rep.Fault.LatestContinuous != 1 {
		t.Fatalf("latest continuous=%d", rep.Fault.LatestContinuous)
	}
	// After fallback the next commit is version 2; the injected gap row
	// stays replayable in the chain.
	if rep.SnapshotVersion != 2 {
		t.Fatalf("snapshot version=%d", rep.SnapshotVersion)
	}
	var sawGap bool
	for _, rec := range rep.Snapshots {
		if rec.Version == 3 && !rec.Continuous {
			sawGap = true
		}
	}
	if !sawGap || len(rep.Snapshots) != 3 {
		t.Fatalf("chain=%+v", rep.Snapshots)
	}
}

func TestAuthContentionInjection(t *testing.T) {
	rep := run(t, "auth-contention", 0)
	if rep.Contention == nil {
		t.Fatal("no contention record")
	}
	if rep.Contention.Requests != 8 || rep.Contention.Succeeded != 1 || rep.Contention.Contended != 7 {
		t.Fatalf("contention=%+v", rep.Contention)
	}
	if rep.SnapshotVersion != 1 {
		t.Fatalf("contention must not advance snapshot: %d", rep.SnapshotVersion)
	}
}

func TestBudgetExhaustedInjection(t *testing.T) {
	rep := run(t, "budget-exhausted", 0)
	codes := deviceCodes(rep)
	want := map[string]engine.Code{
		"tenant-a/dev-g1": engine.CodeOK,
		"tenant-a/dev-g2": engine.CodeDeviceBudget,
		"tenant-a/dev-g3": engine.CodeOK,
		"tenant-a/dev-g4": engine.CodeGlobalBudget,
		"tenant-a/dev-g5": engine.CodeGlobalBudget,
		"tenant-a/dev-g6": engine.CodeGlobalBudget,
	}
	for k, v := range want {
		if codes[k] != v {
			t.Fatalf("codes=%v want=%v", codes, want)
		}
	}
	if rep.Totals.Devices != 6 {
		t.Fatalf("degraded devices missing from counts: %+v", rep.Totals)
	}
	if rep.Scheduling.Degraded != 4 || rep.Scheduling.Waiting != 4 {
		t.Fatalf("scheduling=%+v", rep.Scheduling)
	}
	if rep.SnapshotVersion != 0 {
		t.Fatalf("snapshot advanced on fault round: %d", rep.SnapshotVersion)
	}
}

func TestReportDeterministicAcrossWorkers(t *testing.T) {
	for _, scenario := range []string{"normal", "budget-exhausted", "auth-contention", "snapshot-gap"} {
		var ref []byte
		for _, workers := range []int{1, 2, 4, 8} {
			rep := run(t, scenario, workers)
			got := rep.Bytes()
			if ref == nil {
				ref = got
				continue
			}
			if !bytes.Equal(ref, got) {
				t.Fatalf("scenario %s: report differs for workers=%d", scenario, workers)
			}
		}
	}
}

func TestReportDeterministicAcrossRuns(t *testing.T) {
	for _, scenario := range samples.Scenarios {
		a := run(t, scenario, 3).Bytes()
		b := run(t, scenario, 3).Bytes()
		if !bytes.Equal(a, b) {
			t.Fatalf("scenario %s: repeated runs differ", scenario)
		}
	}
}

func TestInvalidDeviceLeavesNoState(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := RunScenario(dir, "fingerprint-dup", 0); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.NewEngine(filepath.Join(dir, "fingerprint-dup.db"), nil, engine.Params{MaxInFlight: 1}, samples.Key())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if st := eng.State("tenant-a", "dev-x2"); st != nil {
		t.Fatalf("invalid device persisted: %+v", st)
	}
	if eng.SnapshotVersion() != 0 {
		t.Fatalf("snapshot advanced: %d", eng.SnapshotVersion())
	}
}
