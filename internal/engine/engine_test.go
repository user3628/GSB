package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func testPolicies() []TenantPolicy {
	return []TenantPolicy{
		{TenantID: "tenant-a", PolicyVersion: 1, TrustThreshold: 600000, AllowedCaps: []string{"read", "write"}, CredTTL: 1000},
		{TenantID: "tenant-b", PolicyVersion: 3, TrustThreshold: 300000, AllowedCaps: []string{"read"}, CredTTL: 500},
	}
}

func testParams() Params {
	return Params{Concurrency: 2, GlobalCPU: 10000, GlobalMem: 10000, PerDeviceTime: 100, MaxInFlight: 2, WaitQueueMax: 4}
}

func fp(id string) string {
	sum := sha256.Sum256([]byte("fp:" + id))
	return hex.EncodeToString(sum[:])
}

func claim(id, tenant string, risk int64) DeviceClaim {
	return DeviceClaim{
		DeviceID:     id,
		TenantID:     tenant,
		Fingerprint:  fp(id),
		Capabilities: []string{"read"},
		Risk:         risk,
		NeedCPU:      100,
		NeedMem:      100,
		NeedTime:     10,
		Events:       []BehaviorEvent{{Seq: 1, Delta: 350000, Mult: 2}},
	}
}

func TestAccumulateOverflow(t *testing.T) {
	if _, _, err := Accumulate([]BehaviorEvent{{Seq: 1, Delta: math.MaxInt64, Mult: 2}}); err == nil || err.(*Error).Code != CodeScoreOverflow {
		t.Fatalf("multiply overflow: %v", err)
	}
	if _, _, err := Accumulate([]BehaviorEvent{{Seq: 1, Delta: math.MaxInt64, Mult: 1}, {Seq: 2, Delta: 1, Mult: 1}}); err == nil || err.(*Error).Code != CodeScoreOverflow {
		t.Fatalf("add overflow: %v", err)
	}
	score, steps, err := Accumulate([]BehaviorEvent{{Seq: 2, Delta: 250000, Mult: 2}, {Seq: 1, Delta: 150000, Mult: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if score != 800000 {
		t.Fatalf("score=%d", score)
	}
	if steps[0].Seq != 1 || steps[0].Running != 300000 || steps[1].Running != 800000 {
		t.Fatalf("chain=%+v", steps)
	}
}

func TestTruncate(t *testing.T) {
	q, entry := Truncate(1234567)
	if q != 1 || entry.Remainder != 234567 || entry.Divisor != Scale {
		t.Fatalf("q=%d entry=%+v", q, entry)
	}
}

func TestCredentialVerifyCodes(t *testing.T) {
	key := []byte("test-key")
	policy := &TenantPolicy{TenantID: "tenant-a", TrustThreshold: 100, AllowedCaps: []string{"read"}, CredTTL: 100}
	cred := IssueCredential(key, "d1", "tenant-a", []string{"read"}, 5, 1000, 100)
	if code := VerifyCredential(cred, key, policy, 5, 1050); code != CodeOK {
		t.Fatalf("verify=%s", code)
	}
	bad := *cred
	bad.Sig = cred.Sig[:2] + "ff" + cred.Sig[4:]
	if code := VerifyCredential(&bad, key, policy, 5, 1050); code != CodeCredSignature {
		t.Fatalf("sig=%s", code)
	}
	if code := VerifyCredential(cred, key, policy, 5, 1200); code != CodeCredExpired {
		t.Fatalf("expired=%s", code)
	}
	wide := IssueCredential(key, "d1", "tenant-a", []string{"read", "admin"}, 5, 1000, 100)
	if code := VerifyCredential(wide, key, policy, 5, 1050); code != CodeCapabilityDenied {
		t.Fatalf("caps=%s", code)
	}
	if code := VerifyCredential(cred, key, policy, 4, 1050); code != CodeScoreRegressed {
		t.Fatalf("regressed=%s", code)
	}
}

func TestPlanScheduleDegradation(t *testing.T) {
	claims := []*DeviceClaim{
		{DeviceID: "d1", Risk: 900, NeedCPU: 4000, NeedMem: 100, NeedTime: 10},
		{DeviceID: "d2", Risk: 800, NeedCPU: 4000, NeedMem: 100, NeedTime: 500},
		{DeviceID: "d3", Risk: 700, NeedCPU: 4000, NeedMem: 100, NeedTime: 10},
		{DeviceID: "d4", Risk: 600, NeedCPU: 4000, NeedMem: 100, NeedTime: 10},
	}
	plan := PlanSchedule(claims, testParams())
	codes := []Code{plan.Entries[0].Code, plan.Entries[1].Code, plan.Entries[2].Code, plan.Entries[3].Code}
	want := []Code{CodeOK, CodeDeviceBudget, CodeOK, CodeGlobalBudget}
	for i := range want {
		if codes[i] != want[i] {
			t.Fatalf("codes=%v want=%v", codes, want)
		}
	}
	if plan.Entries[2].Waited != true || plan.Entries[0].Waited != false {
		t.Fatalf("waiting flags wrong: %+v", plan.Entries)
	}
	if plan.CPUUsed != 8000 || plan.Runnable != 2 || plan.Degraded != 2 {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestValidateClaim(t *testing.T) {
	e, err := NewEngine(filepath.Join(t.TempDir(), "t.db"), testPolicies(), testParams(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	seenDev := map[string]bool{}
	seenFP := map[string]string{}
	good := claim("d1", "tenant-a", 100)
	if verr := e.validateClaim(&good, seenDev, seenFP); verr != nil {
		t.Fatal(verr)
	}
	badFP := claim("d2", "tenant-a", 100)
	badFP.Fingerprint = "zz"
	if verr := e.validateClaim(&badFP, seenDev, seenFP); verr == nil || verr.Code != CodeDeviceInvalid {
		t.Fatalf("bad fp: %v", verr)
	}
	badTenant := claim("d3", "tenant-zzz", 100)
	if verr := e.validateClaim(&badTenant, seenDev, seenFP); verr == nil || verr.Code != CodeDeviceInvalid {
		t.Fatalf("bad tenant: %v", verr)
	}
	dup := claim("d1", "tenant-a", 100)
	if verr := e.validateClaim(&dup, seenDev, seenFP); verr == nil || verr.Code != CodeDeviceInvalid {
		t.Fatalf("dup id: %v", verr)
	}
	dupFP := claim("d9", "tenant-a", 100)
	dupFP.Fingerprint = fp("d1")
	if verr := e.validateClaim(&dupFP, seenDev, seenFP); verr == nil || verr.Code != CodeDeviceInvalid {
		t.Fatalf("dup fp: %v", verr)
	}
	if len(e.states) != 0 {
		t.Fatalf("invalid claims polluted state: %+v", e.states)
	}
}

func TestSnapshotChainAndGap(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "snap.db")
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	s1 := &Snapshot{Version: 1, PrevHash: "", Devices: []DeviceState{{TenantID: "tenant-a", DeviceID: "d1", ScoreVersion: 1}}}
	if err := store.CommitSnapshot(s1); err != nil {
		t.Fatal(err)
	}
	s2 := &Snapshot{Version: 2, PrevHash: s1.Hash(), Devices: []DeviceState{{TenantID: "tenant-a", DeviceID: "d1", ScoreVersion: 2}}}
	if err := store.CommitSnapshot(s2); err != nil {
		t.Fatal(err)
	}
	records, latest, _, ferr := store.LoadChain()
	if ferr != nil || latest != 2 || len(records) != 2 {
		t.Fatalf("chain=%+v latest=%d err=%v", records, latest, ferr)
	}
	// Inject a version gap: v4 while v3 is missing.
	if err := store.InjectRawSnapshot(4, s2.Hash(), s2.Hash(), `{"version":4,"prev_hash":"x","devices":[]}`); err != nil {
		t.Fatal(err)
	}
	records, latest, _, ferr = store.LoadChain()
	if ferr == nil || ferr.Code != CodeSnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION, got %v", ferr)
	}
	if latest != 2 {
		t.Fatalf("latest continuous=%d", latest)
	}
	if records[len(records)-1].Continuous {
		t.Fatalf("gap record marked continuous: %+v", records)
	}
	store.Close()
}

func TestSnapshotWholeReplacementDetection(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "replace.db")
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	s1 := &Snapshot{Version: 1, PrevHash: "", Devices: []DeviceState{{TenantID: "tenant-a", DeviceID: "d1"}}}
	if err := store.CommitSnapshot(s1); err != nil {
		t.Fatal(err)
	}
	// Wholesale replacement of the stored payload under the same version.
	if _, err := store.DB().Exec(`UPDATE snapshots SET payload=? WHERE version=1`,
		`{"version":1,"prev_hash":"","devices":[{"tenant_id":"tenant-a","device_id":"evil"}]}`); err != nil {
		t.Fatal(err)
	}
	_, latest, _, ferr := store.LoadChain()
	if ferr == nil || ferr.Code != CodeSnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION, got %v", ferr)
	}
	if latest != 0 {
		t.Fatalf("latest continuous=%d", latest)
	}
	other := &Snapshot{Version: 1, PrevHash: "", Devices: []DeviceState{{TenantID: "tenant-a", DeviceID: "evil"}}}
	if SnapshotEqual(s1, other) {
		t.Fatal("whole-replacement not detected by content hash")
	}
	store.Close()
}

// TestCrashHelperProcess runs in a child process: it begins a transaction,
// writes an uncommitted snapshot v2 plus a ghost device, then exits without
// commit or rollback to emulate a forced kill.
func TestCrashHelperProcess(t *testing.T) {
	if os.Getenv("IOTRUST_CRASH_HELPER") != "1" {
		return
	}
	store, err := OpenStore(os.Getenv("IOTRUST_CRASH_DB"))
	if err != nil {
		os.Exit(1)
	}
	tx, err := store.DB().Begin()
	if err != nil {
		os.Exit(1)
	}
	if _, err := tx.Exec(`INSERT INTO snapshots(version, prev_hash, content_hash, payload) VALUES(2,'x','y','{}')`); err != nil {
		os.Exit(1)
	}
	if _, err := tx.Exec(`INSERT INTO device_state(tenant_id, device_id, state) VALUES('tenant-a','ghost','{}')`); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func TestCrashRecovery(t *testing.T) {
	if os.Getenv("IOTRUST_CRASH_HELPER") == "1" {
		return
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	eng, err := NewEngine(dbPath, testPolicies(), testParams(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	claims := []DeviceClaim{claim("d1", "tenant-a", 100)}
	rep := eng.RunRound(claims, 1000, 1500)
	if rep.SnapshotVersion != 1 {
		t.Fatalf("round1 snapshot=%d", rep.SnapshotVersion)
	}
	eng.Close()

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelperProcess")
	cmd.Env = append(os.Environ(), "IOTRUST_CRASH_HELPER=1", "IOTRUST_CRASH_DB="+dbPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, out)
	}

	eng2, err := NewEngine(dbPath, testPolicies(), testParams(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
	if eng2.Fault() != nil {
		t.Fatalf("unexpected fault after clean recovery: %+v", eng2.Fault())
	}
	if eng2.SnapshotVersion() != 1 {
		t.Fatalf("snapshot version after recovery=%d", eng2.SnapshotVersion())
	}
	if st := eng2.State("tenant-a", "d1"); st == nil || st.ScoreVersion != 1 {
		t.Fatalf("committed issuance rolled back: %+v", st)
	}
	if st := eng2.State("tenant-a", "ghost"); st != nil {
		t.Fatalf("uncommitted tx leaked: %+v", st)
	}
	// The device from the discarded transaction can be re-evaluated.
	rep2 := eng2.RunRound([]DeviceClaim{claim("ghost", "tenant-a", 100), claim("d1", "tenant-a", 100)}, 1000, 1500)
	if rep2.SnapshotVersion != 2 {
		t.Fatalf("re-evaluation snapshot=%d", rep2.SnapshotVersion)
	}
	if st := eng2.State("tenant-a", "ghost"); st == nil {
		t.Fatal("ghost device not re-evaluated after recovery")
	}
}

func TestContention(t *testing.T) {
	eng, err := NewEngine(filepath.Join(t.TempDir(), "c.db"), testPolicies(), testParams(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	rec := eng.RunContentionCheck("tenant-a/d1", 8)
	if rec.Succeeded != 1 || rec.Contended != 7 {
		t.Fatalf("contention=%+v", rec)
	}
}

func TestSessionIsolation(t *testing.T) {
	eng, err := NewEngine(filepath.Join(t.TempDir(), "s.db"), testPolicies(), testParams(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	sa := eng.NewSession("tenant-a")
	sb := eng.NewSession("tenant-b")
	sa.Record("dev-x", 100)
	sb.Record("dev-x", 200)
	if sa.Ledger()["dev-x"] != 100 || sb.Ledger()["dev-x"] != 200 {
		t.Fatalf("ledgers not isolated: a=%v b=%v", sa.Ledger(), sb.Ledger())
	}
	if _, ok := sa.Ledger()["dev-y"]; ok {
		t.Fatal("cross-session leakage")
	}
}

func TestAuthorizeContentionCode(t *testing.T) {
	eng, err := NewEngine(filepath.Join(t.TempDir(), "a.db"), testPolicies(), testParams(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	c := claim("d1", "tenant-a", 100)
	if !eng.tryLock("tenant-a/d1") {
		t.Fatal("lock failed")
	}
	if _, code := eng.Authorize(&c, 700000, 1, 1000); code != CodeAuthContention {
		t.Fatalf("contending authorize=%s", code)
	}
	eng.unlock("tenant-a/d1")
	if _, code := eng.Authorize(&c, 700000, 1, 1000); code != CodeOK {
		t.Fatalf("authorize=%s", code)
	}
	if _, code := eng.Authorize(&c, 100, 1, 1000); code != CodeTrustInsufficient {
		t.Fatalf("below threshold=%s", code)
	}
	wide := claim("d2", "tenant-a", 100)
	wide.Capabilities = []string{"admin"}
	if _, code := eng.Authorize(&wide, 700000, 1, 1000); code != CodeCapabilityDenied {
		t.Fatalf("caps=%s", code)
	}
}
