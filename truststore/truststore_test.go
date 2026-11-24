package truststore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T, options StoreOptions) *Store {
	t.Helper()
	if options.DBPath == "" {
		options.DBPath = filepath.Join(t.TempDir(), "state.db")
	}
	if options.WaitTimeout == 0 {
		options.WaitTimeout = time.Second
	}
	store, err := Open(context.Background(), options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func bootstrapSample(t *testing.T, options StoreOptions) (*Store, *SampleSet, *Snapshot) {
	t.Helper()
	store := testStore(t, options)
	samples, err := GenerateSamples()
	if err != nil {
		t.Fatalf("GenerateSamples: %v", err)
	}
	content, err := samples.InitialContent()
	if err != nil {
		t.Fatalf("InitialContent: %v", err)
	}
	snapshot, err := store.Bootstrap(context.Background(), "test", content)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	return store, samples, snapshot
}

func TestSamplesAreDeterministic(t *testing.T) {
	first, err := GenerateSamples()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateSamples()
	if err != nil {
		t.Fatal(err)
	}
	if len(first.CertIDByName) != len(second.CertIDByName) {
		t.Fatalf("alias count changed")
	}
	for name, id := range first.CertIDByName {
		if second.CertIDByName[name] != id {
			t.Fatalf("alias %s changed ID: %s != %s", name, id, second.CertIDByName[name])
		}
		if first.PEMByName[name] != second.PEMByName[name] {
			t.Fatalf("alias %s changed PEM", name)
		}
	}
	firstContent, err := first.InitialContent()
	if err != nil {
		t.Fatal(err)
	}
	secondContent, err := second.InitialContent()
	if err != nil {
		t.Fatal(err)
	}
	firstHash, _ := firstContent.ContentHash()
	secondHash, _ := secondContent.ContentHash()
	if firstHash != secondHash {
		t.Fatalf("content hash changed: %s != %s", firstHash, secondHash)
	}
}

func TestValidationBusinessCodes(t *testing.T) {
	samples, err := GenerateSamples()
	if err != nil {
		t.Fatal(err)
	}
	content, err := samples.InitialContent()
	if err != nil {
		t.Fatal(err)
	}
	results := VerifyAll(content, VerificationOptions{CurrentTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	byID := make(map[string]Code)
	for _, result := range results {
		byID[result.ChainID] = result.Code
	}
	want := map[string]Code{
		"chain-svc-alpha":  CodeOK,
		"chain-svc-beta":   CodeOK,
		"chain-team":       CodeOK,
		"chain-dept":       CodeOK,
		"chain-cross":      CodeOK,
		"chain-broken":     CodeChainBroken,
		"chain-rogue":      CodeSelfSignedNonRoot,
		"chain-restricted": CodePathLenExceeded,
	}
	for chain, code := range want {
		if byID[chain] != code {
			t.Errorf("%s code = %s, want %s (result %+v)", chain, byID[chain], code, resultFor(chain, results))
		}
	}
}

func resultFor(chain string, results []ChainResult) ChainResult {
	for _, result := range results {
		if result.ChainID == chain {
			return result
		}
	}
	return ChainResult{}
}

func TestInvalidReferenceRollsBackVersion(t *testing.T) {
	store, _, initial := bootstrapSample(t, StoreOptions{})
	_, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "does-not-exist"}}}, ApplyOptions{RequestID: "bad-ref"})
	if !HasCode(err, CodeTrustInvalid) {
		t.Fatalf("Apply error = %v (%s), want E_TRUST_INVALID", err, CodeOf(err))
	}
	loaded, err := store.Load(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != initial.Version {
		t.Fatalf("version advanced to %d after rollback", loaded.Version)
	}
	versions, err := store.ListSnapshotVersions(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 {
		t.Fatalf("snapshot count = %d, want 1", len(versions))
	}
}

func TestRevokeIntermediateClosure(t *testing.T) {
	store, samples, _ := bootstrapSample(t, StoreOptions{})
	changeset := Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "dept-ca", Reason: "key compromise"}}}
	result, err := store.Apply(context.Background(), "test", changeset, ApplyOptions{RequestID: "revoke-dept"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.SnapshotAfter.Version != 2 {
		t.Fatalf("version = %d, want 2", result.SnapshotAfter.Version)
	}
	if result.Closure == nil {
		t.Fatal("closure is nil")
	}
	if !contains(result.Closure.DirectCertIDs, samples.ID("team-ca")) {
		t.Errorf("direct certs missing team-ca: %v", result.Closure.DirectCertIDs)
	}
	if !contains(result.Closure.TransitiveCertIDs, samples.ID("leaf-team")) {
		t.Errorf("transitive certs missing leaf-team: %v", result.Closure.TransitiveCertIDs)
	}
	if !contains(result.Closure.DirectChainIDs, "chain-dept") {
		t.Errorf("direct chains missing chain-dept: %v", result.Closure.DirectChainIDs)
	}
	if !contains(result.Closure.TransitiveChainIDs, "chain-team") {
		t.Errorf("transitive chains missing chain-team: %v", result.Closure.TransitiveChainIDs)
	}
	team := diffFor(result.Chains, "chain-team")
	if team.After.Code != CodeDependencyInvalidated {
		t.Errorf("chain-team after = %s, want %s", team.After.Code, CodeDependencyInvalidated)
	}
	beta := diffFor(result.Chains, "chain-svc-beta")
	if beta.After.Code != CodeOK {
		t.Errorf("chain-svc-beta after = %s, want OK", beta.After.Code)
	}
}

func diffFor(diffs []ChainDiff, chain string) ChainDiff {
	for _, diff := range diffs {
		if diff.ChainID == chain {
			return diff
		}
	}
	return ChainDiff{}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestWholeReplacementDetection(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{})
	changeset := Changeset{Operations: []Operation{
		{Type: OperationAddRoot, CertID: "root-gamma"},
		{Type: OperationRevoke, CertID: "root-alpha"},
		{Type: OperationRevoke, CertID: "root-beta"},
	}}
	result, err := store.Apply(context.Background(), "test", changeset, ApplyOptions{RequestID: "replace-all-roots"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !result.Replacement {
		t.Fatal("whole replacement was not detected")
	}
	loaded, err := store.Load(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Content.RootIDs) != 1 {
		t.Fatalf("roots = %v, want exactly gamma", loaded.Content.RootIDs)
	}
	if result.Closure == nil || result.Closure.Code != CodeDependencyInvalidated {
		t.Fatalf("closure = %+v, want E_DEP_INVALIDATED", result.Closure)
	}
	team := diffFor(result.Chains, "chain-team")
	if team.After.Code != CodeDependencyInvalidated {
		t.Errorf("chain-team after = %s, want %s", team.After.Code, CodeDependencyInvalidated)
	}
}

func TestRevokeRootKeepsIndependentNamespaceChain(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{})
	result, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "root-alpha"}}}, ApplyOptions{RequestID: "revoke-alpha"})
	if err != nil {
		t.Fatal(err)
	}
	beta := diffFor(result.Chains, "chain-svc-beta")
	team := diffFor(result.Chains, "chain-team")
	restricted := diffFor(result.Chains, "chain-restricted")
	if beta.After.Code != CodeOK {
		t.Errorf("chain-svc-beta = %s (%s), want OK", beta.After.Code, beta.After.Detail)
	}
	if team.After.Code != CodeDependencyInvalidated {
		t.Errorf("chain-team = %s, want %s", team.After.Code, CodeDependencyInvalidated)
	}
	if restricted.After.Code != CodeDependencyInvalidated {
		t.Errorf("chain-restricted = %s, want %s", restricted.After.Code, CodeDependencyInvalidated)
	}
}

func TestTightenScopeClosure(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{})
	changeset := Changeset{Operations: []Operation{{
		Type:          OperationTightenScope,
		ScopeID:       "scope-core",
		RemoveRootIDs: []string{"root-alpha"},
	}}}
	result, err := store.Apply(context.Background(), "test", changeset, ApplyOptions{RequestID: "tighten"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if result.Closure == nil {
		t.Fatal("closure is nil")
	}
	team := diffFor(result.Chains, "chain-team")
	if team.After.Code != CodeScopeDenied {
		t.Errorf("chain-team = %s, want %s", team.After.Code, CodeScopeDenied)
	}
	beta := diffFor(result.Chains, "chain-svc-beta")
	if beta.After.Code != CodeOK {
		t.Errorf("chain-svc-beta = %s, want OK", beta.After.Code)
	}
	if !contains(result.Closure.TransitiveChainIDs, "chain-team") {
		t.Errorf("scope closure missing chain-team: %+v", result.Closure)
	}
	for _, affected := range result.Closure.AffectedChains {
		if affected.ChainID == "chain-svc-beta" {
			t.Errorf("unrelated scope-beta chain was marked affected")
		}
	}
}

func TestClosureWithNoDownstreamIsValid(t *testing.T) {
	samples, err := GenerateSamples()
	if err != nil {
		t.Fatal(err)
	}
	content, err := samples.InitialContent()
	if err != nil {
		t.Fatal(err)
	}
	content.HostChains = nil
	content, err = content.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	closure, err := buildInvalidationClosure(content, []string{samples.ID("leaf-broken")}, VerificationOptions{})
	if err != nil {
		t.Fatalf("closure: %v", err)
	}
	if closure == nil || len(closure.OriginCertIDs) != 1 {
		t.Fatalf("unexpected closure: %+v", closure)
	}
	if len(closure.DirectCertIDs) != 0 || len(closure.TransitiveCertIDs) != 0 || len(closure.AffectedChains) != 0 {
		t.Fatalf("leaf should have no downstream closure: %+v", closure)
	}
}

func TestCrossSignatureReplacementKeepsAlternatePath(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{})
	changeset := Changeset{Operations: []Operation{
		{Type: OperationAddRoot, CertID: "root-gamma"},
		{Type: OperationReplaceCrossSignature, OldCertID: "intermediate-main-alpha", NewCertID: "intermediate-main-gamma"},
	}}
	result, err := store.Apply(context.Background(), "test", changeset, ApplyOptions{RequestID: "replace-cross"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cross := diffFor(result.Chains, "chain-cross")
	if cross.After.Code != CodeOK {
		t.Errorf("chain-cross after = %s (%s), want OK", cross.After.Code, cross.After.Detail)
	}
	if !strings.Contains(strings.Join(cross.After.PathNames, ","), "intermediate-main-gamma") {
		t.Errorf("cross chain did not move to gamma path: %v", cross.After.PathNames)
	}
}

func TestSnapshotGapFallbackAndServiceRejection(t *testing.T) {
	store, samples, _ := bootstrapSample(t, StoreOptions{})
	_, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationAddRoot, CertID: "root-gamma"}}}, ApplyOptions{RequestID: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InjectVersionGap(context.Background(), "test", 3); err != nil {
		t.Fatalf("InjectVersionGap: %v", err)
	}
	fallback, err := store.Load(context.Background(), "test")
	if !HasCode(err, CodeSnapshotVersion) {
		t.Fatalf("Load error = %v (%s), want E_SNAPSHOT_VERSION", err, CodeOf(err))
	}
	if fallback == nil || fallback.Version != 2 {
		t.Fatalf("fallback = %+v, want version 2", fallback)
	}
	_, err = store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "dept-ca"}}}, ApplyOptions{RequestID: "after-gap"})
	if !HasCode(err, CodeSnapshotVersion) {
		t.Fatalf("Apply after gap = %v (%s), want E_SNAPSHOT_VERSION", err, CodeOf(err))
	}
	report, err := store.ExportReport(context.Background(), "test")
	if !HasCode(err, CodeSnapshotVersion) {
		t.Fatalf("ExportReport error = %v, want E_SNAPSHOT_VERSION", err)
	}
	if report.SnapshotVersion != 2 || report.SnapshotChain.Code != CodeSnapshotVersion {
		t.Fatalf("report fallback = version %d code %s", report.SnapshotVersion, report.SnapshotChain.Code)
	}
	if len(report.SnapshotChain.Versions) != 3 {
		t.Fatalf("stored versions = %d, want 3 with a hole", len(report.SnapshotChain.Versions))
	}
	_ = samples
}

func TestOldSnapshotCanBeReplayed(t *testing.T) {
	store, _, initial := bootstrapSample(t, StoreOptions{})
	_, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "dept-ca"}}}, ApplyOptions{RequestID: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.LoadSnapshotVersion(context.Background(), "test", 1)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Version != 1 || replayed.ContentHash != initial.ContentHash {
		t.Fatalf("replay mismatch: version=%d hash=%s", replayed.Version, replayed.ContentHash)
	}
	team := ChainResult{}
	for _, chain := range replayed.Content.HostChains {
		if chain.ID == "chain-team" {
			team = ValidateChain(replayed.Content, chain, VerificationOptions{CurrentTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
		}
	}
	if team.Code != CodeOK {
		t.Fatalf("replayed chain-team = %s, want OK", team.Code)
	}
	if _, err := store.LoadSnapshotVersion(context.Background(), "test", 3); !HasCode(err, CodeNotFound) {
		t.Fatalf("missing version error = %v", err)
	}
}

func TestReportStableAndSorted(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{})
	_, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "dept-ca"}}}, ApplyOptions{RequestID: "report"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ExportReportJSON(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ExportReportJSON(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("report bytes changed between identical calls")
	}
	var report Report
	if err := json.Unmarshal(first, &report); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(report.Chains); i++ {
		if report.Chains[i-1].ChainID > report.Chains[i].ChainID {
			t.Fatalf("chains not sorted at %d", i)
		}
	}
	for i := 1; i < len(report.Codes); i++ {
		if report.Codes[i-1] > report.Codes[i] {
			t.Fatalf("codes not sorted at %d", i)
		}
	}
}

func TestReportsAreStableAcrossFreshStores(t *testing.T) {
	generate := func() []byte {
		store, _, _ := bootstrapSample(t, StoreOptions{})
		_, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "dept-ca"}}}, ApplyOptions{RequestID: "stable"})
		if err != nil {
			t.Fatal(err)
		}
		data, err := store.ExportReportJSON(context.Background(), "test")
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	first := generate()
	second := generate()
	if !bytes.Equal(first, second) {
		t.Fatal("fresh stores with identical inputs produced different report bytes")
	}
}

func TestContentionRejectedWithoutStateChange(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{MaxInflight: 1, WaitTimeout: 100 * time.Millisecond})
	marker := filepath.Join(t.TempDir(), "before-commit")
	holderDone := make(chan error, 1)
	go func() {
		_, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationAddRoot, CertID: "root-gamma"}}}, ApplyOptions{
			RequestID:   "holder",
			Diagnostics: &ApplyDiagnostics{MarkerBeforeCommit: marker, HoldBeforeCommit: 300 * time.Millisecond},
		})
		holderDone <- err
	}()
	waitForFile(t, marker, 2*time.Second)
	_, err := store.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "team-ca"}}}, ApplyOptions{RequestID: "contender", WaitTimeout: 30 * time.Millisecond})
	if !HasCode(err, CodeTrustContention) {
		t.Fatalf("contender error = %v (%s), want E_TRUST_CONTENTION", err, CodeOf(err))
	}
	if err := <-holderDone; err != nil {
		t.Fatalf("holder: %v", err)
	}
	loaded, err := store.Load(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != 2 {
		t.Fatalf("version = %d, want 2 (only holder committed)", loaded.Version)
	}
	team := mustChain(t, store, "chain-team")
	if team.Code != CodeOK {
		t.Fatalf("rejected contender changed team trust: %s", team.Code)
	}
	report, err := store.ExportReport(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range report.Contentions {
		if record.RequestID == "contender" && record.Code == CodeTrustContention && record.Namespace == "test" {
			found = true
		}
	}
	if !found {
		t.Fatalf("contention audit missing in report: %+v", report.Contentions)
	}
}

func TestDifferentNamespacesAreNotGatedTogether(t *testing.T) {
	store, samples, _ := bootstrapSample(t, StoreOptions{MaxInflight: 2, WaitTimeout: 2 * time.Second})
	content, err := samples.InitialContent()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Bootstrap(context.Background(), "other", content); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, namespace := range []string{"test", "other"} {
		wg.Add(1)
		go func(namespace string) {
			defer wg.Done()
			_, err := store.Apply(context.Background(), namespace, Changeset{Operations: []Operation{{Type: OperationAddRoot, CertID: "root-gamma"}}}, ApplyOptions{RequestID: "ns-" + namespace})
			errs <- err
		}(namespace)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("namespace apply: %v", err)
		}
	}
}

func TestCrossStoreDatabaseContentionIsBounded(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.db")
	first, _, _ := bootstrapSample(t, StoreOptions{DBPath: dbPath, MaxInflight: 1, WaitTimeout: 120 * time.Millisecond})
	second, err := Open(context.Background(), StoreOptions{DBPath: dbPath, MaxInflight: 1, WaitTimeout: 120 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	marker := filepath.Join(t.TempDir(), "first-before-commit")
	holderDone := make(chan error, 1)
	go func() {
		_, err := first.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationAddRoot, CertID: "root-gamma"}}}, ApplyOptions{
			RequestID:   "first-store",
			Diagnostics: &ApplyDiagnostics{MarkerBeforeCommit: marker, HoldBeforeCommit: 400 * time.Millisecond},
		})
		holderDone <- err
	}()
	waitForFile(t, marker, 2*time.Second)
	_, err = second.Apply(context.Background(), "test", Changeset{Operations: []Operation{{Type: OperationRevoke, CertID: "team-ca"}}}, ApplyOptions{RequestID: "second-store", WaitTimeout: 120 * time.Millisecond})
	if !HasCode(err, CodeTrustContention) {
		t.Fatalf("cross-store error = %v (%s), want E_TRUST_CONTENTION", err, CodeOf(err))
	}
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	loaded, err := first.Load(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != 2 {
		t.Fatalf("version = %d, want 2", loaded.Version)
	}
	report, err := second.ExportReport(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, record := range report.Contentions {
		if record.RequestID == "second-store" && record.Outcome == "rejected-store" {
			found = true
		}
	}
	if !found {
		t.Fatalf("cross-store contention audit missing: %+v", report.Contentions)
	}
}

func mustChain(t *testing.T, store *Store, chain string) ChainResult {
	t.Helper()
	result, err := store.Verify(context.Background(), "test", chain)
	if err != nil {
		t.Fatalf("Verify %s: %v", chain, err)
	}
	return result
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func TestForcedTerminationRecovery(t *testing.T) {
	if os.Getenv("PKITRUST_RECOVERY_CHILD") == "1" {
		t.Skip("helper process is driven by the parent test")
	}
	root := t.TempDir()
	for _, mode := range []string{"uncommitted", "committed"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(root, mode)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			dbPath := filepath.Join(dir, "state.db")
			store, err := Open(context.Background(), StoreOptions{DBPath: dbPath, WaitTimeout: 2 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			samples, err := GenerateSamples()
			if err != nil {
				t.Fatal(err)
			}
			content, err := samples.InitialContent()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Bootstrap(context.Background(), "recovery", content); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(dir, mode+".marker")
			cmd := exec.Command(os.Args[0], "-test.run=TestRecoveryChildProcess", "-test.v")
			cmd.Env = append(os.Environ(),
				"PKITRUST_RECOVERY_CHILD=1",
				"PKITRUST_DB="+dbPath,
				"PKITRUST_MODE="+mode,
				"PKITRUST_MARKER="+marker,
			)
			var output strings.Builder
			cmd.Stdout = &output
			cmd.Stderr = &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waitForFile(t, marker, 5*time.Second)
			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("kill: %v", err)
			}
			_ = cmd.Wait()

			reopened, err := Open(context.Background(), StoreOptions{DBPath: dbPath, WaitTimeout: 2 * time.Second})
			if err != nil {
				t.Fatalf("reopen: %v\nchild output:\n%s", err, output.String())
			}
			defer reopened.Close()
			snapshot, err := reopened.Load(context.Background(), "recovery")
			if err != nil {
				t.Fatalf("load after kill: %v\nchild output:\n%s", err, output.String())
			}
			if mode == "uncommitted" && snapshot.Version != 1 {
				t.Fatalf("uncommitted version = %d, want 1", snapshot.Version)
			}
			if mode == "committed" && snapshot.Version != 2 {
				t.Fatalf("committed version = %d, want 2", snapshot.Version)
			}
			wantRoot := contains(snapshot.Content.RootIDs, samples.ID("root-gamma"))
			if mode == "uncommitted" && wantRoot {
				t.Fatal("uncommitted root gamma survived")
			}
			if mode == "committed" && !wantRoot {
				t.Fatal("committed root gamma was lost")
			}
		})
	}
}

func TestRecoveryChildProcess(t *testing.T) {
	if os.Getenv("PKITRUST_RECOVERY_CHILD") != "1" {
		t.Skip("helper process")
	}
	dbPath := os.Getenv("PKITRUST_DB")
	mode := os.Getenv("PKITRUST_MODE")
	marker := os.Getenv("PKITRUST_MARKER")
	store, err := Open(context.Background(), StoreOptions{DBPath: dbPath, WaitTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("child open: %v", err)
	}
	defer store.Close()
	diagnostics := &ApplyDiagnostics{}
	if mode == "uncommitted" {
		diagnostics.MarkerBeforeCommit = marker
		diagnostics.HoldBeforeCommit = 10 * time.Second
	} else {
		diagnostics.MarkerAfterCommit = marker
		diagnostics.HoldAfterCommit = 10 * time.Second
	}
	_, err = store.Apply(context.Background(), "recovery", Changeset{Operations: []Operation{{Type: OperationAddRoot, CertID: "root-gamma"}}}, ApplyOptions{RequestID: "recovery-child", Diagnostics: diagnostics})
	if err != nil {
		t.Fatalf("child apply: %v", err)
	}
}

func TestMissingCAIsChainBroken(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{})
	result := mustChain(t, store, "chain-broken")
	if result.Code != CodeChainBroken {
		t.Fatalf("code = %s, want %s", result.Code, CodeChainBroken)
	}
	if !strings.Contains(result.Detail, "not present") {
		t.Fatalf("detail = %q", result.Detail)
	}
}

func TestLoadUnknownNamespace(t *testing.T) {
	store, _, _ := bootstrapSample(t, StoreOptions{})
	_, err := store.Load(context.Background(), "missing")
	if !HasCode(err, CodeNotFound) {
		t.Fatalf("err = %v, want E_NOT_FOUND", err)
	}
	var domainErr *Error
	if !errors.As(err, &domainErr) {
		t.Fatalf("not a domain error: %v", err)
	}
}
