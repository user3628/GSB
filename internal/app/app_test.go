package app

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"fwadjudge/internal/codes"
	"fwadjudge/internal/ledger"
	"fwadjudge/internal/model"
	"fwadjudge/internal/samples"
	"fwadjudge/internal/store"
)

func genSamples(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "samples")
	if err := samples.Generate(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

func runOpts(db, samplesDir, report, scenario string) Options {
	return Options{
		DBPath:        db,
		SamplesDir:    samplesDir,
		ReportPath:    report,
		Scenario:      scenario,
		MaxConcurrent: 4,
		RuleBudget:    1000,
		WaitThreshold: 2 * time.Second,
	}
}

func hasCode(codesList []string, code string) bool {
	for _, c := range codesList {
		if c == code {
			return true
		}
	}
	return false
}

// TestFaultInjectionMatrix runs all six injection classes plus the
// baseline scenarios and asserts the business codes.
func TestFaultInjectionMatrix(t *testing.T) {
	dir := genSamples(t)
	db := filepath.Join(t.TempDir(), "fw.db")
	out, err := Run(runOpts(db, dir, "", "all"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"normal":      codes.OK,
		"diverge":     codes.ERuleDivergence,
		"conflict":    codes.ERuleDivergence,
		"bad-cidr":    codes.ERuleInvalid,
		"port-invert": codes.ERuleInvalid,
		"overflow":    codes.EFixedOverflow,
		"version-gap": codes.ESnapshotVersion,
		"replace":     codes.OK,
	}
	for name, code := range want {
		got := out.Scenario[name]
		if !hasCode(got, code) {
			t.Errorf("scenario %s: want code %s in %v", name, code, got)
		}
	}
}

// TestInjectionIsolation verifies that injected faults do not pollute
// committed snapshots, the error ledger, or other snapshots' verdicts.
func TestInjectionIsolation(t *testing.T) {
	dir := genSamples(t)
	dbPath := filepath.Join(t.TempDir(), "fw.db")
	if _, err := Run(runOpts(dbPath, dir, "", "all")); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Overflow: no partial ledger writes, no verdicts for "ovf".
	if bound, err := st.LedgerBound("ovf"); err != nil || bound != 0 {
		t.Errorf("ovf ledger bound %d, want 0 (no partial writes)", bound)
	}
	if n, err := st.VerdictCount("ovf"); err != nil || n != 0 {
		t.Errorf("ovf verdicts %d, want 0", n)
	}
	// Other snapshots are unaffected: hq verdicts and ledger exist.
	if n, err := st.VerdictCount("hq"); err != nil || n == 0 {
		t.Errorf("hq verdicts %d, want > 0", n)
	}
	// hq participates in three compare scenarios; each commits one
	// truncation record of 500000 ErrScale units from probe p2.
	if bound, err := st.LedgerBound("hq"); err != nil || bound != 1500000 {
		t.Errorf("hq ledger bound %d, want 1500000", bound)
	}
	// Invalid-rule snapshots kept their valid prefix.
	if n, err := st.VerdictCount("bad-cidr"); err != nil || n == 0 {
		t.Errorf("bad-cidr verdicts %d, want > 0", n)
	}
	// Whole replacement flagged on repl v2 only.
	metas, err := st.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	var replV1, replV2 bool
	for _, m := range metas {
		if m.SnapshotID == "repl" && m.Version == 1 {
			replV1 = m.Replaced
		}
		if m.SnapshotID == "repl" && m.Version == 2 {
			replV2 = m.Replaced
		}
	}
	if replV1 || !replV2 {
		t.Errorf("repl replaced flags v1=%v v2=%v, want false/true", replV1, replV2)
	}
	// Version gap: latest continuous is 1.
	if latest, cerr := st.LoadChain("gap"); cerr == nil || cerr.Code != codes.ESnapshotVersion || latest != 1 {
		t.Errorf("gap chain latest=%d err=%v", latest, cerr)
	}
}

// TestReportByteIdentical runs the full pipeline twice on identical
// input and requires byte-identical reports.
func TestReportByteIdentical(t *testing.T) {
	dir := genSamples(t)
	tmp := t.TempDir()
	rep1 := filepath.Join(tmp, "r1.json")
	rep2 := filepath.Join(tmp, "r2.json")
	if _, err := Run(runOpts(filepath.Join(tmp, "a.db"), dir, rep1, "all")); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(runOpts(filepath.Join(tmp, "b.db"), dir, rep2, "all")); err != nil {
		t.Fatal(err)
	}
	b1, err := os.ReadFile(rep1)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := os.ReadFile(rep2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatal("reports differ between identical runs")
	}
	if !bytes.Contains(b1, []byte(`"time_mark": "SAMPLE@FIXED-0"`)) {
		t.Error("fixed time mark missing")
	}
	for _, want := range []string{`"class": "deterministic"`, `"class": "uncertain"`, `"field": "action"`, `"field": "index"`} {
		if !bytes.Contains(b1, []byte(want)) {
			t.Errorf("report missing %s", want)
		}
	}
}

// TestKillRecovery simulates a hard kill after a committed snapshot:
// no report is produced by the killed run, and the recovered engine
// continues from committed state only.
func TestKillRecovery(t *testing.T) {
	if os.Getenv("APP_HELPER") == "1" {
		db := os.Getenv("APP_HELPER_DB")
		st, err := store.Open(db)
		if err != nil {
			os.Exit(3)
		}
		r := model.Rule{
			SnapshotID: "k", Index: 1, Direction: model.DirAny, Protocol: model.ProtoAny,
			SrcCIDR: "0.0.0.0/0", DstCIDR: "0.0.0.0/0", PortLo: 0, PortHi: 65535,
			Action: model.ActionDeny, Priority: 1,
		}
		if err := r.Normalize(); err != nil {
			os.Exit(4)
		}
		entries := []ledger.Entry{{SnapshotID: "k", RuleIndex: 1, Field: "weight", Amount: 250000}}
		if _, cerr := st.CommitSnapshot("k", []model.Rule{r}, entries, 0); cerr != nil {
			os.Exit(5)
		}
		// Killed before any report could be written.
		os.Exit(2)
	}
	tmp := t.TempDir()
	db := filepath.Join(tmp, "fw.db")
	reportPath := filepath.Join(tmp, "report.json")
	cmd := exec.Command(os.Args[0], "-test.run", "TestKillRecovery")
	cmd.Env = append(os.Environ(), "APP_HELPER=1", "APP_HELPER_DB="+db)
	if err := cmd.Run(); err == nil {
		t.Fatal("helper should have been 'killed'")
	}
	if _, err := os.Stat(reportPath); !os.IsNotExist(err) {
		t.Fatal("killed run must not leave a report behind")
	}
	// Recovery run on the same DB: committed state is reused.
	dir := genSamples(t)
	if _, err := Run(runOpts(db, dir, reportPath, "normal")); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if latest, cerr := st.LoadChain("k"); cerr != nil || latest != 1 {
		t.Errorf("recovered chain latest=%d err=%v", latest, cerr)
	}
	if bound, err := st.LedgerBound("k"); err != nil || bound != 250000 {
		t.Errorf("recovered ledger bound %d, want 250000", bound)
	}
	if _, err := os.Stat(reportPath); err != nil {
		t.Error("normal exit must produce the report")
	}
}
