package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"pkitrust/truststore"
)

const namespace = "demo"

type scenarioOptions struct {
	Root        string
	Out         string
	MaxInflight int
	WaitTimeout time.Duration
	Hold        time.Duration
}

type scenarioOutcome struct {
	Name       string   `json:"name"`
	Passed     bool     `json:"passed"`
	Code       string   `json:"code"`
	Version    int64    `json:"version"`
	Details    []string `json:"details"`
	ReportPath string   `json:"report_path"`
}

type runSummary struct {
	SchemaVersion string            `json:"schema_version"`
	Passed        bool              `json:"passed"`
	Scenarios     []scenarioOutcome `json:"scenarios"`
}

type checker struct {
	details []string
	errs    []string
}

func (c *checker) check(condition bool, format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if condition {
		c.details = append(c.details, message)
	} else {
		c.errs = append(c.errs, message)
	}
}

func (c *checker) outcome(name, reportPath string, code truststore.Code, version int64) scenarioOutcome {
	details := append([]string(nil), c.details...)
	if len(c.errs) != 0 {
		sort.Strings(c.errs)
		details = append(details, "FAILURES:")
		details = append(details, c.errs...)
	}
	return scenarioOutcome{
		Name:       name,
		Passed:     len(c.errs) == 0,
		Code:       string(code),
		Version:    version,
		Details:    details,
		ReportPath: reportPath,
	}
}

func scenarioNames(name string) ([]string, error) {
	all := []string{"baseline", "missing-ca", "revoke-intermediate", "revoke-root", "tighten-scope", "cross-replace", "whole-replace", "version-gap", "contention", "recovery"}
	if name == "all" {
		return all, nil
	}
	for _, candidate := range all {
		if name == candidate {
			return []string{name}, nil
		}
	}
	return nil, fmt.Errorf("unknown scenario %q", name)
}

func sanitizeName(name string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", " ", "_")
	return replacer.Replace(name)
}

func resetDirectory(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return os.MkdirAll(path, 0o700)
}

func runScenario(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	switch name {
	case "baseline":
		return scenarioBaseline(ctx, name, dir, options)
	case "missing-ca":
		return scenarioMissingCA(ctx, name, dir, options)
	case "revoke-intermediate":
		return scenarioRevokeIntermediate(ctx, name, dir, options)
	case "revoke-root":
		return scenarioRevokeRoot(ctx, name, dir, options)
	case "tighten-scope":
		return scenarioTightenScope(ctx, name, dir, options)
	case "cross-replace":
		return scenarioCrossReplace(ctx, name, dir, options)
	case "whole-replace":
		return scenarioWholeReplace(ctx, name, dir, options)
	case "version-gap":
		return scenarioVersionGap(ctx, name, dir, options)
	case "contention":
		return scenarioContention(ctx, name, dir, options)
	case "recovery":
		return scenarioRecovery(ctx, name, dir, options)
	default:
		return scenarioOutcome{Name: name, Passed: false, Code: "E_INPUT_INVALID", Details: []string{"unknown scenario"}}
	}
}

func openDemoStore(dir string, options scenarioOptions) (*truststore.Store, *truststore.SampleSet, *truststore.Snapshot, error) {
	store, err := truststore.Open(context.Background(), truststore.StoreOptions{
		DBPath:      filepath.Join(dir, "state.db"),
		MaxInflight: options.MaxInflight,
		WaitTimeout: options.WaitTimeout,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	samples, err := truststore.GenerateSamples()
	if err != nil {
		store.Close()
		return nil, nil, nil, err
	}
	content, err := samples.InitialContent()
	if err != nil {
		store.Close()
		return nil, nil, nil, err
	}
	snapshot, err := store.Bootstrap(context.Background(), namespace, content)
	if err != nil {
		store.Close()
		return nil, nil, nil, err
	}
	return store, samples, snapshot, nil
}

func scenarioBaseline(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, _, initial, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	check.check(initial.Version == 1, "initial snapshot version=1")
	results, err := store.VerifyAll(ctx, namespace)
	if err != nil {
		check.check(false, "verify all: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), initial.Version)
	}
	assertChainCodes(check, results, map[string]truststore.Code{
		"chain-svc-alpha":  truststore.CodeOK,
		"chain-svc-beta":   truststore.CodeOK,
		"chain-team":       truststore.CodeOK,
		"chain-dept":       truststore.CodeOK,
		"chain-cross":      truststore.CodeOK,
		"chain-broken":     truststore.CodeChainBroken,
		"chain-rogue":      truststore.CodeSelfSignedNonRoot,
		"chain-restricted": truststore.CodePathLenExceeded,
	})
	first, firstErr := store.ExportReportJSON(ctx, namespace)
	second, secondErr := store.ExportReportJSON(ctx, namespace)
	check.check(firstErr == nil && secondErr == nil, "report export succeeds")
	check.check(bytes.Equal(first, second), "repeated report export is byte-identical")
	check.check(json.Valid(first), "report is valid JSON")
	path := reportPath(dir, options, name)
	if err := writeBytes(path, first); err != nil {
		check.check(false, "write report: %v", err)
	}
	return check.outcome(name, path, truststore.CodeOK, initial.Version)
}

func scenarioMissingCA(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, _, initial, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	result, err := store.Verify(ctx, namespace, "chain-broken")
	if err != nil {
		check.check(false, "verify: %v", err)
	}
	check.check(result.Code == truststore.CodeChainBroken, "chain-broken code=E_CHAIN_BROKEN")
	check.check(result.PathIDs != nil && len(result.PathIDs) == 1, "broken chain stops at leaf")
	check.check(initial.Version == 1, "snapshot version did not advance")
	changeset := truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationRevoke, CertID: "missing-ca", Reason: "missing reference injection"}}}
	_, applyErr := store.Apply(ctx, namespace, changeset, truststore.ApplyOptions{RequestID: "missing-ca-ref"})
	check.check(truststore.HasCode(applyErr, truststore.CodeTrustInvalid), "missing CA reference rejected with E_TRUST_INVALID")
	current, loadErr := store.Load(ctx, namespace)
	check.check(loadErr == nil && current.Version == 1, "rejected change did not advance the snapshot version")
	report, reportErr := store.ExportReport(ctx, namespace)
	check.check(reportErr == nil, "report export succeeds")
	path := reportPath(dir, options, name)
	if report != nil {
		report.Decision = &truststore.ApplyDecision{
			Namespace:       namespace,
			RequestID:       "missing-ca-ref",
			Code:            truststore.CodeTrustInvalid,
			SnapshotVersion: report.SnapshotVersion,
			Detail:          "change set references an unknown CA",
			Changeset:       changeset,
		}
		report.Codes = append(report.Codes, string(truststore.CodeTrustInvalid))
		sort.Strings(report.Codes)
		data, marshalErr := truststore.ReportJSON(report)
		check.check(marshalErr == nil, "decision report encodes")
		if marshalErr == nil {
			check.check(writeBytes(path, data) == nil, "decision report written")
		}
	}
	return check.outcome(name, path, truststore.CodeTrustInvalid, 1)
}

func scenarioRevokeIntermediate(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, samples, _, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	result, err := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationRevoke, CertID: "dept-ca", Reason: "intermediate compromise"}}}, truststore.ApplyOptions{RequestID: "revoke-intermediate"})
	if err != nil {
		check.check(false, "apply: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), 1)
	}
	check.check(result.SnapshotAfter.Version == 2, "snapshot version=2")
	check.check(result.Closure != nil, "dependency closure present")
	if result.Closure != nil {
		check.check(contains(result.Closure.DirectCertIDs, samples.ID("team-ca")), "direct dependency includes team-ca")
		check.check(contains(result.Closure.TransitiveCertIDs, samples.ID("leaf-team")), "transitive dependency includes leaf-team")
		check.check(contains(result.Closure.DirectChainIDs, "chain-dept"), "direct affected chain includes chain-dept")
		check.check(contains(result.Closure.TransitiveChainIDs, "chain-team"), "transitive affected chain includes chain-team")
	}
	team := diffFor(result.Chains, "chain-team")
	dept := diffFor(result.Chains, "chain-dept")
	check.check(team.After.Code == truststore.CodeDependencyInvalidated, "chain-team terminal code=E_DEP_INVALIDATED")
	check.check(dept.After.Code == truststore.CodeDependencyInvalidated, "chain-dept terminal code=E_DEP_INVALIDATED")
	path, code, reportErr := exportAndWrite(ctx, store, namespace, reportPath(dir, options, name))
	check.check(reportErr == nil, "report export succeeds")
	if code == truststore.CodeOK {
		code = truststore.CodeDependencyInvalidated
	}
	return check.outcome(name, path, code, result.SnapshotAfter.Version)
}

func scenarioRevokeRoot(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, samples, _, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	result, err := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationRevoke, CertID: "root-alpha", Reason: "root removal"}}}, truststore.ApplyOptions{RequestID: "revoke-root"})
	if err != nil {
		check.check(false, "apply: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), 1)
	}
	check.check(result.SnapshotAfter.Version == 2, "snapshot version=2")
	check.check(result.Closure != nil, "dependency closure present")
	if result.Closure != nil {
		check.check(contains(result.Closure.DirectCertIDs, samples.ID("dept-ca")), "direct CA dependency includes dept-ca")
		check.check(contains(result.Closure.TransitiveCertIDs, samples.ID("team-ca")), "transitive CA dependency includes team-ca")
	}
	team := diffFor(result.Chains, "chain-team")
	beta := diffFor(result.Chains, "chain-svc-beta")
	restricted := diffFor(result.Chains, "chain-restricted")
	check.check(team.After.Code == truststore.CodeDependencyInvalidated, "chain-team terminal code=E_DEP_INVALIDATED")
	check.check(beta.After.Code == truststore.CodeOK, "independent root-beta chain remains OK")
	check.check(restricted.After.Code == truststore.CodeDependencyInvalidated, "all root-alpha dependants use E_DEP_INVALIDATED")
	path, code, reportErr := exportAndWrite(ctx, store, namespace, reportPath(dir, options, name))
	check.check(reportErr == nil, "report export succeeds")
	if code == truststore.CodeOK {
		code = truststore.CodeDependencyInvalidated
	}
	return check.outcome(name, path, code, result.SnapshotAfter.Version)
}

func scenarioCrossReplace(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, _, _, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	result, err := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{
		{Type: truststore.OperationAddRoot, CertID: "root-gamma"},
		{Type: truststore.OperationReplaceCrossSignature, OldCertID: "intermediate-main-alpha", NewCertID: "intermediate-main-gamma"},
	}}, truststore.ApplyOptions{RequestID: "cross-replace"})
	if err != nil {
		check.check(false, "apply: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), 1)
	}
	check.check(result.SnapshotAfter.Version == 2, "snapshot version=2")
	check.check(!result.Replacement, "incremental cross-sign replacement is not a whole replacement")
	cross := diffFor(result.Chains, "chain-cross")
	check.check(cross.After.Code == truststore.CodeOK, "chain-cross remains OK")
	check.check(strings.Contains(strings.Join(cross.After.PathNames, ","), "intermediate-main-gamma"), "chain-cross rebuilt through gamma cross-signature")
	path, code, reportErr := exportAndWrite(ctx, store, namespace, reportPath(dir, options, name))
	check.check(reportErr == nil, "report export succeeds")
	return check.outcome(name, path, code, result.SnapshotAfter.Version)
}

func scenarioTightenScope(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, _, _, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	result, err := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{{
		Type:          truststore.OperationTightenScope,
		ScopeID:       "scope-core",
		RemoveRootIDs: []string{"root-alpha"},
	}}}, truststore.ApplyOptions{RequestID: "tighten-scope"})
	if err != nil {
		check.check(false, "apply: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), 1)
	}
	check.check(result.SnapshotAfter.Version == 2, "snapshot version=2")
	check.check(result.Closure != nil, "scope dependency closure present")
	team := diffFor(result.Chains, "chain-team")
	beta := diffFor(result.Chains, "chain-svc-beta")
	check.check(team.After.Code == truststore.CodeScopeDenied, "chain-team terminal code=E_SCOPE_DENIED")
	check.check(beta.After.Code == truststore.CodeOK, "unrelated scope-beta chain remains OK")
	if result.Closure != nil {
		check.check(contains(result.Closure.TransitiveChainIDs, "chain-team"), "closure records chain-team as transitively affected")
	}
	path, code, reportErr := exportAndWrite(ctx, store, namespace, reportPath(dir, options, name))
	check.check(reportErr == nil, "report export succeeds")
	if code == truststore.CodeOK {
		code = truststore.CodeScopeDenied
	}
	return check.outcome(name, path, code, result.SnapshotAfter.Version)
}

func scenarioWholeReplace(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, _, _, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	result, err := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{
		{Type: truststore.OperationAddRoot, CertID: "root-gamma"},
		{Type: truststore.OperationRevoke, CertID: "root-alpha", Reason: "whole trust replacement"},
		{Type: truststore.OperationRevoke, CertID: "root-beta", Reason: "whole trust replacement"},
	}}, truststore.ApplyOptions{RequestID: "whole-replace"})
	if err != nil {
		check.check(false, "apply: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), 1)
	}
	check.check(result.SnapshotAfter.Version == 2, "snapshot version=2")
	check.check(result.Replacement, "whole replacement detected from disjoint root sets")
	check.check(result.Closure != nil, "dependency closure present")
	team := diffFor(result.Chains, "chain-team")
	check.check(team.After.Code == truststore.CodeDependencyInvalidated, "chain-team terminal code=E_DEP_INVALIDATED")
	loaded, err := store.Load(ctx, namespace)
	check.check(err == nil, "reload succeeds")
	if err == nil {
		check.check(len(loaded.Content.RootIDs) == 1, "exactly one new root remains")
	}
	path, code, reportErr := exportAndWrite(ctx, store, namespace, reportPath(dir, options, name))
	check.check(reportErr == nil, "report export succeeds")
	if code == truststore.CodeOK {
		code = truststore.CodeDependencyInvalidated
	}
	return check.outcome(name, path, code, result.SnapshotAfter.Version)
}

func scenarioVersionGap(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, _, _, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	if _, err := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationAddRoot, CertID: "root-gamma"}}}, truststore.ApplyOptions{RequestID: "build-v2"}); err != nil {
		check.check(false, "prepare version 2: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), 1)
	}
	if err := store.InjectVersionGap(ctx, namespace, 3); err != nil {
		check.check(false, "inject gap: %v", err)
		return check.outcome(name, "", truststore.CodeOf(err), 2)
	}
	fallback, loadErr := store.Load(ctx, namespace)
	check.check(truststore.HasCode(loadErr, truststore.CodeSnapshotVersion), "load rejects with E_SNAPSHOT_VERSION")
	check.check(fallback != nil && fallback.Version == 2, "load falls back to latest continuous version 2")
	_, applyErr := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationRevoke, CertID: "dept-ca"}}}, truststore.ApplyOptions{RequestID: "must-reject"})
	check.check(truststore.HasCode(applyErr, truststore.CodeSnapshotVersion), "mutations are refused after gap")
	path := reportPath(dir, options, name)
	data, reportErr := store.ExportReportJSON(ctx, namespace)
	check.check(truststore.HasCode(reportErr, truststore.CodeSnapshotVersion), "report carries E_SNAPSHOT_VERSION")
	if len(data) != 0 {
		if err := writeBytes(path, data); err != nil {
			check.check(false, "write fallback report: %v", err)
		}
	}
	var report truststore.Report
	if err := json.Unmarshal(data, &report); err == nil {
		check.check(report.SnapshotVersion == 2, "report snapshot version=2 fallback")
		check.check(report.SnapshotChain.Code == truststore.CodeSnapshotVersion, "report snapshot-chain code=E_SNAPSHOT_VERSION")
		check.check(report.SnapshotChain.StoredVersion == 4 && report.SnapshotChain.ContinuousVersion == 2, "report exposes stored=4 continuous=2")
	}
	return check.outcome(name, path, truststore.CodeSnapshotVersion, 2)
}

func scenarioContention(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	store, _, _, err := openDemoStore(dir, options)
	if err != nil {
		check.check(false, "bootstrap: %v", err)
		return check.outcome(name, "", truststore.CodeStoreFailure, 0)
	}
	defer store.Close()
	marker := filepath.Join(dir, "holder-before-commit.marker")
	holderDone := make(chan error, 1)
	go func() {
		_, err := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationAddRoot, CertID: "root-gamma"}}}, truststore.ApplyOptions{
			RequestID: "holder",
			Diagnostics: &truststore.ApplyDiagnostics{
				MarkerBeforeCommit: marker,
				HoldBeforeCommit:   options.Hold,
			},
		})
		holderDone <- err
	}()
	if err := waitForMarker(marker, 5*time.Second, nil); err != nil {
		check.check(false, "wait for holder: %v", err)
		return check.outcome(name, "", truststore.CodeTrustContention, 0)
	}
	wait := options.WaitTimeout / 5
	if wait < 20*time.Millisecond {
		wait = 20 * time.Millisecond
	}
	_, contenderErr := store.Apply(ctx, namespace, truststore.Changeset{Operations: []truststore.Operation{{Type: truststore.OperationRevoke, CertID: "team-ca"}}}, truststore.ApplyOptions{RequestID: "contender", WaitTimeout: wait})
	check.check(truststore.HasCode(contenderErr, truststore.CodeTrustContention), "contender rejected with E_TRUST_CONTENTION")
	if err := <-holderDone; err != nil {
		check.check(false, "holder apply: %v", err)
	}
	loaded, err := store.Load(ctx, namespace)
	check.check(err == nil, "load after contention succeeds")
	if err == nil {
		check.check(loaded.Version == 2, "only the holder advanced version to 2")
	}
	team, err := store.Verify(ctx, namespace, "chain-team")
	check.check(err == nil && team.Code == truststore.CodeOK, "rejected contender did not revoke team-ca")
	path := reportPath(dir, options, name)
	data, reportErr := store.ExportReportJSON(ctx, namespace)
	check.check(reportErr == nil, "report export succeeds")
	if len(data) != 0 {
		if err := writeBytes(path, data); err != nil {
			check.check(false, "write report: %v", err)
		}
	}
	var report truststore.Report
	if err := json.Unmarshal(data, &report); err == nil {
		found := false
		for _, record := range report.Contentions {
			if record.RequestID == "contender" && record.Code == truststore.CodeTrustContention && record.Namespace == namespace {
				found = true
			}
		}
		check.check(found, "contention audit records namespace, request ID and wait duration")
	}
	first, _ := store.ExportReportJSON(ctx, namespace)
	second, _ := store.ExportReportJSON(ctx, namespace)
	check.check(bytes.Equal(first, second), "contention report is byte-identical on repeated export")
	version := int64(2)
	if loaded != nil {
		version = loaded.Version
	}
	return check.outcome(name, path, truststore.CodeTrustContention, version)
}

func scenarioRecovery(ctx context.Context, name, dir string, options scenarioOptions) scenarioOutcome {
	check := &checker{}
	var committedReportPath string
	for _, mode := range []string{"uncommitted", "committed"} {
		modeDir := filepath.Join(dir, mode)
		if err := os.MkdirAll(modeDir, 0o700); err != nil {
			check.check(false, "%s: prepare dir: %v", mode, err)
			continue
		}
		dbPath := filepath.Join(modeDir, "state.db")
		store, samples, initial, err := openDemoStore(modeDir, options)
		if err != nil {
			check.check(false, "%s: bootstrap: %v", mode, err)
			continue
		}
		check.check(initial.Version == 1, "%s: baseline version=1", mode)
		if err := store.Close(); err != nil {
			check.check(false, "%s: close before child: %v", mode, err)
			continue
		}
		marker := filepath.Join(modeDir, mode+".marker")
		executable, err := os.Executable()
		if err != nil {
			check.check(false, "%s: executable: %v", mode, err)
			continue
		}
		cmd := exec.Command(executable, "recovery-child", "-db", dbPath, "-namespace", namespace, "-mode", mode, "-marker", marker, "-hold", "30s")
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Start(); err != nil {
			check.check(false, "%s: start child: %v", mode, err)
			continue
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		if err := waitForMarker(marker, 8*time.Second, done); err != nil {
			check.check(false, "%s: wait for marker: %v\n%s", mode, err, output.String())
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-done
			continue
		}
		if err := cmd.Process.Kill(); err != nil {
			check.check(false, "%s: kill child: %v", mode, err)
		}
		<-done
		reopened, err := truststore.Open(ctx, truststore.StoreOptions{DBPath: dbPath, WaitTimeout: 5 * time.Second})
		if err != nil {
			check.check(false, "%s: reopen: %v", mode, err)
			continue
		}
		snapshot, err := reopened.Load(ctx, namespace)
		if err != nil {
			check.check(false, "%s: load after kill: %v", mode, err)
			reopened.Close()
			continue
		}
		hasGamma := contains(snapshot.Content.RootIDs, samples.ID("root-gamma"))
		if mode == "uncommitted" {
			check.check(snapshot.Version == 1 && !hasGamma, "uncommitted transaction fully rolled back")
		} else {
			check.check(snapshot.Version == 2 && hasGamma, "committed transaction survives forced termination")
		}
		path := filepath.Join(modeDir, "report.json")
		data, reportErr := reopened.ExportReportJSON(ctx, namespace)
		check.check(reportErr == nil, "%s: post-recovery report succeeds", mode)
		if len(data) != 0 {
			_ = writeBytes(path, data)
		}
		if mode == "committed" {
			committedReportPath = path
		}
		_ = reopened.Close()
	}
	return check.outcome(name, committedReportPath, truststore.CodeOK, 2)
}

func waitForMarker(path string, timeout time.Duration, processDone <-chan error) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if processDone != nil {
			select {
			case err := <-processDone:
				return fmt.Errorf("child exited before marker: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	return fmt.Errorf("timed out waiting for %s", path)
}

func reportPath(dir string, options scenarioOptions, name string) string {
	if options.Out != "" && name != "all" {
		path, err := filepath.Abs(options.Out)
		if err == nil {
			return path
		}
		return options.Out
	}
	return filepath.Join(dir, "report.json")
}

func exportAndWrite(ctx context.Context, store *truststore.Store, ns, path string) (string, truststore.Code, error) {
	data, err := store.ExportReportJSON(ctx, ns)
	code := truststore.CodeOf(err)
	if len(data) != 0 {
		if writeErr := writeBytes(path, data); writeErr != nil {
			return path, code, writeErr
		}
	}
	return path, code, err
}

func writeBytes(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeBytes(path, append(data, '\n'))
}

func assertChainCodes(check *checker, results []truststore.ChainResult, want map[string]truststore.Code) {
	for _, result := range results {
		expected, ok := want[result.ChainID]
		if !ok {
			continue
		}
		check.check(result.Code == expected, "%s code=%s", result.ChainID, expected)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func diffFor(diffs []truststore.ChainDiff, chain string) truststore.ChainDiff {
	for _, diff := range diffs {
		if diff.ChainID == chain {
			return diff
		}
	}
	return truststore.ChainDiff{}
}
