package verify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"keylife/internal/adjudicator"
	"keylife/internal/model"
	"keylife/internal/report"
	"keylife/internal/sample"
	"keylife/internal/service"
	"keylife/internal/store"
)

var anomalyOrder = []string{
	"subject_scope",
	"expired_use",
	"weak_algo",
	"phase_transition",
	"snapshot_gap",
	"snapshot_replacement",
	"rotation_contention",
}

type Options struct {
	Root           string
	Threshold      int
	NowTick        int64
	Scale          int
	MaxConcurrency int
	SubjectLimit   int
	Anomalies      []string
	SelfExe        string
	SkipCrash      bool
}

type Suite struct {
	Schema          string           `json:"schema"`
	TimeSample      string           `json:"time_sample"`
	Passed          bool             `json:"passed"`
	Threshold       int              `json:"threshold"`
	NowTick         int64            `json:"now_tick"`
	ResourceControl ResourceControl  `json:"resource_control"`
	Checks          []Check          `json:"checks"`
	Determinism     DeterminismCheck `json:"determinism"`
	DualPaths       []DualPathCheck  `json:"dual_paths"`
	Anomalies       []AnomalyCheck   `json:"anomalies"`
	CrashRecovery   []CrashCheck     `json:"crash_recovery"`
	Chain           ChainCheck       `json:"chain"`
}

type ResourceControl struct {
	MaxConcurrency int `json:"max_concurrency"`
	SubjectLimit   int `json:"subject_limit"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Detail   string `json:"detail,omitempty"`
}

type DeterminismCheck struct {
	Passed          bool   `json:"passed"`
	ByteIdentical   bool   `json:"byte_identical"`
	ReportSHA256A   string `json:"report_sha256_a"`
	ReportSHA256B   string `json:"report_sha256_b"`
	ReportBytes     int    `json:"report_bytes"`
	SnapshotVersion int64  `json:"snapshot_version"`
}

type DualPathCheck struct {
	Case            string                      `json:"case"`
	InputCount      int                         `json:"input_count"`
	PendingKeys     int                         `json:"pending_keys"`
	Threshold       int                         `json:"threshold"`
	ExpectedPath    adjudicator.Path            `json:"expected_path"`
	SelectedPath    adjudicator.Path            `json:"selected_path"`
	ByteIdentical   bool                        `json:"byte_identical"`
	DigestEqual     bool                        `json:"digest_equal"`
	AccurateDigest  string                      `json:"accurate_digest"`
	StreamingDigest string                      `json:"streaming_digest"`
	DecisionCodes   []model.Code                `json:"decision_codes"`
	Passed          bool                        `json:"passed"`
	Paths           []adjudicator.PathSelection `json:"paths"`
}

type AnomalyCheck struct {
	Type             string      `json:"type"`
	Injection        string      `json:"injection"`
	ExpectedCode     model.Code  `json:"expected_code"`
	ActualCode       model.Code  `json:"actual_code"`
	Passed           bool        `json:"passed"`
	SnapshotBefore   int64       `json:"snapshot_before"`
	SnapshotAfter    int64       `json:"snapshot_after"`
	PhysicalMaxAfter int64       `json:"physical_max_after,omitempty"`
	RecoveredThrough int64       `json:"recovered_through,omitempty"`
	PhaseBefore      model.Phase `json:"phase_before,omitempty"`
	PhaseAfter       model.Phase `json:"phase_after,omitempty"`
	StatePreserved   bool        `json:"state_preserved"`
	Detail           string      `json:"detail,omitempty"`
}

type CrashCheck struct {
	Point             string      `json:"point"`
	ProcessExitCode   int         `json:"process_exit_code"`
	ExpectedExit      int         `json:"expected_exit"`
	SnapshotBefore    int64       `json:"snapshot_before"`
	SnapshotAtRestart int64       `json:"snapshot_at_restart"`
	SnapshotAfter     int64       `json:"snapshot_after"`
	ExpectedAfter     int64       `json:"expected_after"`
	PhaseAtRestart    model.Phase `json:"phase_at_restart,omitempty"`
	PhaseAfter        model.Phase `json:"phase_after,omitempty"`
	RetrySucceeded    bool        `json:"retry_succeeded"`
	ChainValid        bool        `json:"chain_valid"`
	Passed            bool        `json:"passed"`
	Detail            string      `json:"detail,omitempty"`
}

type ChainCheck struct {
	Valid            bool    `json:"valid"`
	RecoveredThrough int64   `json:"recovered_through"`
	PhysicalMax      int64   `json:"physical_max"`
	Versions         []int64 `json:"versions"`
	HeadChainHash    string  `json:"head_chain_hash"`
}

func Run(ctx context.Context, opts Options) (*Suite, error) {
	if opts.Threshold < 1 {
		opts.Threshold = 4
	}
	if opts.NowTick <= 0 {
		opts.NowTick = 420
	}
	if opts.Scale < 1 {
		opts.Scale = 3
	}
	if opts.MaxConcurrency < 1 {
		opts.MaxConcurrency = 8
	}
	if opts.SubjectLimit < 1 {
		opts.SubjectLimit = 64
	}
	root := opts.Root
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "keylife-verify-")
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	suite := &Suite{
		Schema: "keylife.verification/v1", TimeSample: model.TimeSample,
		Threshold: opts.Threshold, NowTick: opts.NowTick,
		ResourceControl: ResourceControl{MaxConcurrency: opts.MaxConcurrency, SubjectLimit: opts.SubjectLimit},
		Checks:          make([]Check, 0), DualPaths: make([]DualPathCheck, 0),
		Anomalies: make([]AnomalyCheck, 0), CrashRecovery: make([]CrashCheck, 0),
	}

	base, err := sample.Build(ctx, sampleOptions(opts, filepath.Join(root, "base"), opts.Scale))
	if err != nil {
		return nil, err
	}
	chain, chainErr := base.Store.LoadChain(ctx)
	if chainErr != nil {
		base.Close()
		return nil, chainErr
	}
	suite.Chain = chainCheck(chain)
	suite.Checks = append(suite.Checks, Check{
		ID: "lifecycle.snapshot_chain", Passed: chainErr == nil,
		Expected: "continuous version chain", Actual: fmt.Sprintf("recovered_through=%d", chain.RecoveredThrough),
	})

	below, above, dualChecks, err := runDualPaths(ctx, base, opts.Threshold)
	if err != nil {
		base.Close()
		return nil, err
	}
	_ = below
	_ = above
	suite.DualPaths = dualChecks
	for _, dual := range dualChecks {
		suite.Checks = append(suite.Checks, Check{
			ID: "dual_path." + dual.Case, Passed: dual.Passed,
			Expected: fmt.Sprintf("path=%s and byte-identical decisions/audit", dual.ExpectedPath),
			Actual:   fmt.Sprintf("path=%s byte_identical=%t digest_equal=%t", dual.SelectedPath, dual.ByteIdentical, dual.DigestEqual),
		})
	}

	det, err := runDeterminism(ctx, root, opts)
	if err != nil {
		base.Close()
		return nil, err
	}
	suite.Determinism = det
	suite.Checks = append(suite.Checks, Check{
		ID: "report.byte_determinism", Passed: det.Passed,
		Expected: "two identical runs produce same report bytes", Actual: fmt.Sprintf("byte_identical=%t bytes=%d", det.ByteIdentical, det.ReportBytes),
	})

	selectedAnomalies := normalizeAnomalies(opts.Anomalies)
	for _, anomaly := range selectedAnomalies {
		result, err := runAnomaly(ctx, root, opts, anomaly)
		if err != nil {
			base.Close()
			return nil, fmt.Errorf("anomaly %s: %w", anomaly, err)
		}
		suite.Anomalies = append(suite.Anomalies, result)
		suite.Checks = append(suite.Checks, Check{
			ID: "anomaly." + anomaly, Passed: result.Passed,
			Expected: string(result.ExpectedCode), Actual: string(result.ActualCode),
			Detail: fmt.Sprintf("state_preserved=%t snapshot_before=%d snapshot_after=%d", result.StatePreserved, result.SnapshotBefore, result.SnapshotAfter),
		})
	}

	if !opts.SkipCrash && opts.SelfExe != "" {
		checks, err := runCrashChecks(ctx, root, opts)
		if err != nil {
			base.Close()
			return nil, err
		}
		suite.CrashRecovery = checks
		for _, check := range checks {
			suite.Checks = append(suite.Checks, Check{
				ID: "crash_recovery." + check.Point, Passed: check.Passed,
				Expected: fmt.Sprintf("snapshot_after=%d", check.ExpectedAfter),
				Actual:   fmt.Sprintf("snapshot_after=%d phase=%s retry=%t", check.SnapshotAfter, check.PhaseAfter, check.RetrySucceeded),
			})
		}
	}
	if err := base.Close(); err != nil {
		return nil, err
	}
	suite.Passed = true
	for _, check := range suite.Checks {
		if !check.Passed {
			suite.Passed = false
			break
		}
	}
	return suite, nil
}

func runDualPaths(ctx context.Context, ws *sample.Workspace, threshold int) (adjudicator.BatchResult, adjudicator.BatchResult, []DualPathCheck, error) {
	requests, err := ws.Requests("alpha-payments", false)
	if err != nil {
		return adjudicator.BatchResult{}, adjudicator.BatchResult{}, nil, err
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].KeyID < requests[j].KeyID })
	if len(requests) < 4 {
		return adjudicator.BatchResult{}, adjudicator.BatchResult{}, nil, fmt.Errorf("sample produced %d active alpha keys, need at least 4", len(requests))
	}
	belowRequests := append([]model.DecisionRequest(nil), requests[:threshold-1]...)
	belowRequests[0].Algorithm = "aes-128-gcm"
	belowRequests[0].StrengthBits = 128
	aboveRequests := append([]model.DecisionRequest(nil), requests...)
	aboveRequests[0].Actor = "beta-lab"

	below, err := ws.Service.Adjudicate(ctx, belowRequests, threshold)
	if err != nil {
		return adjudicator.BatchResult{}, adjudicator.BatchResult{}, nil, err
	}
	above, err := ws.Service.Adjudicate(ctx, aboveRequests, threshold)
	if err != nil {
		return adjudicator.BatchResult{}, adjudicator.BatchResult{}, nil, err
	}
	state, err := ws.Service.LoadEvaluationState(ctx, model.T(ws.NowTick))
	if err != nil {
		return adjudicator.BatchResult{}, adjudicator.BatchResult{}, nil, err
	}
	belowCmp, err := adjudicator.ComparePaths(belowRequests, state, threshold)
	if err != nil {
		return adjudicator.BatchResult{}, adjudicator.BatchResult{}, nil, err
	}
	aboveCmp, err := adjudicator.ComparePaths(aboveRequests, state, threshold)
	if err != nil {
		return adjudicator.BatchResult{}, adjudicator.BatchResult{}, nil, err
	}
	checks := []DualPathCheck{
		dualCheck("below_threshold", below, belowCmp, adjudicator.PathAccurate),
		dualCheck("above_threshold", above, aboveCmp, adjudicator.PathStreaming),
	}
	return below, above, checks, nil
}

func dualCheck(name string, batch adjudicator.BatchResult, cmp adjudicator.Comparison, expected adjudicator.Path) DualPathCheck {
	selected := adjudicator.Path("")
	if len(batch.Paths) > 0 {
		selected = batch.Paths[0].Selected
	}
	return DualPathCheck{
		Case: name, InputCount: batch.InputCount, PendingKeys: batch.PendingKeys, Threshold: batch.Threshold,
		ExpectedPath: expected, SelectedPath: selected, ByteIdentical: cmp.ByteIdentical,
		DigestEqual:    cmp.Accurate.Digest == cmp.Streaming.Digest,
		AccurateDigest: cmp.Accurate.Digest, StreamingDigest: cmp.Streaming.Digest,
		DecisionCodes: batch.DecisionCodes,
		Passed:        selected == expected && cmp.ByteIdentical && cmp.Accurate.Digest == cmp.Streaming.Digest,
		Paths:         batch.Paths,
	}
}

func runDeterminism(ctx context.Context, root string, opts Options) (DeterminismCheck, error) {
	buildReport := func(dir string) ([]byte, int64, error) {
		ws, err := sample.Build(ctx, sampleOptions(opts, dir, opts.Scale))
		if err != nil {
			return nil, 0, err
		}
		defer ws.Close()
		rep, err := report.Build(ctx, ws.Store, report.Options{NowTick: ws.NowTick, Threshold: ws.Threshold})
		if err != nil {
			return nil, 0, err
		}
		data, err := report.Bytes(rep)
		return data, rep.SnapshotChain.RecoveredThrough, err
	}
	a, versionA, err := buildReport(filepath.Join(root, "determinism-a"))
	if err != nil {
		return DeterminismCheck{}, err
	}
	b, versionB, err := buildReport(filepath.Join(root, "determinism-b"))
	if err != nil {
		return DeterminismCheck{}, err
	}
	return DeterminismCheck{
		Passed: bytes.Equal(a, b) && versionA == versionB, ByteIdentical: bytes.Equal(a, b),
		ReportSHA256A: model.SHA256Hex(a), ReportSHA256B: model.SHA256Hex(b), ReportBytes: len(a),
		SnapshotVersion: versionA,
	}, nil
}

func normalizeAnomalies(requested []string) []string {
	if len(requested) == 0 {
		return append([]string(nil), anomalyOrder...)
	}
	allowed := make(map[string]struct{})
	for _, name := range anomalyOrder {
		allowed[name] = struct{}{}
	}
	seen := make(map[string]struct{})
	var result []string
	for _, name := range requested {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := allowed[name]; !ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	sort.Slice(result, func(i, j int) bool { return anomalyIndex(result[i]) < anomalyIndex(result[j]) })
	return result
}

func anomalyIndex(name string) int {
	for i, candidate := range anomalyOrder {
		if candidate == name {
			return i
		}
	}
	return len(anomalyOrder)
}

func runAnomaly(ctx context.Context, root string, opts Options, anomaly string) (AnomalyCheck, error) {
	dir := filepath.Join(root, "anomaly-"+anomaly)
	ws, err := sample.Build(ctx, sampleOptions(opts, dir, opts.Scale))
	if err != nil {
		return AnomalyCheck{}, err
	}
	defer ws.Close()
	switch anomaly {
	case "subject_scope":
		return anomalyDecision(ctx, ws, "subject_scope", "external request actor=beta-lab against alpha-payments key", whatIf{
			keyID: "key-alpha-002", actor: "beta-lab", subject: "alpha-payments", operation: "use",
		}, model.CodeSubjectScope)
	case "expired_use":
		ws.Clock.Set(10)
		if _, err := ws.Service.Generate(ctx, service.GenerateRequest{
			KeyID: "key-expired-sample", Actor: "alpha-payments", Subject: "alpha-payments",
			Algorithm: "aes-256-gcm", Lifetime: model.D(5), RotationInterval: model.D(5),
		}); err != nil {
			return AnomalyCheck{}, err
		}
		ws.Clock.Set(11)
		if _, err := ws.Service.Activate(ctx, service.TransitionRequest{Actor: "alpha-payments", KeyID: "key-expired-sample"}); err != nil {
			return AnomalyCheck{}, err
		}
		ws.Clock.Set(500)
		return anomalyDecision(ctx, ws, "expired_use", "clock advanced beyond expiry tick then use requested", whatIf{
			keyID: "key-expired-sample", actor: "alpha-payments", subject: "alpha-payments", operation: "use",
		}, model.CodeKeyExpired)
	case "weak_algo":
		beforeVersion, err := logicalVersion(ctx, ws.Store)
		if err != nil {
			return AnomalyCheck{}, err
		}
		keyBefore, _ := ws.Store.GetKey(ctx, "key-alpha-002")
		ws.Clock.Set(opts.NowTick)
		_, genErr := ws.Service.Generate(ctx, service.GenerateRequest{
			KeyID: "key-weak-sample", Actor: "alpha-payments", Subject: "alpha-payments",
			Algorithm: "aes-128-gcm", Lifetime: model.D(600),
		})
		afterVersion, err := logicalVersion(ctx, ws.Store)
		if err != nil {
			return AnomalyCheck{}, err
		}
		keyAfter, _ := ws.Store.GetKey(ctx, "key-alpha-002")
		_, missingErr := ws.Store.GetKey(ctx, "key-weak-sample")
		return AnomalyCheck{
			Type: "weak_algo", Injection: "external generate parameter algorithm=aes-128-gcm under min=256 policy",
			ExpectedCode: model.CodeWeakAlgo, ActualCode: model.CodeOf(genErr),
			Passed:         model.CodeOf(genErr) == model.CodeWeakAlgo && beforeVersion == afterVersion && model.CodeOf(missingErr) == model.CodeKeyNotFound && keysEqual(keyBefore, keyAfter),
			SnapshotBefore: beforeVersion, SnapshotAfter: afterVersion, PhaseBefore: keyBefore.Phase, PhaseAfter: keyAfter.Phase,
			StatePreserved: keysEqual(keyBefore, keyAfter) && model.CodeOf(missingErr) == model.CodeKeyNotFound,
			Detail:         "no weak key or snapshot was committed",
		}, nil
	case "phase_transition":
		return anomalyDecision(ctx, ws, "phase_transition", "archive requested for a generated key", whatIf{
			keyID: "key-gamma-002", actor: "gamma-ops", subject: "gamma-ops", operation: "archive",
		}, model.CodePhaseTransition)
	case "snapshot_gap":
		beforeVersion, err := logicalVersion(ctx, ws.Store)
		if err != nil {
			return AnomalyCheck{}, err
		}
		keyBefore, err := ws.Store.GetKey(ctx, "key-alpha-002")
		if err != nil {
			return AnomalyCheck{}, err
		}
		if err := ws.Store.InjectSnapshotGap(ctx, beforeVersion, beforeVersion+2, opts.NowTick); err != nil {
			return AnomalyCheck{}, err
		}
		chain, loadErr := ws.Store.LoadChain(ctx)
		keyAfter, _ := ws.Store.GetKey(ctx, "key-alpha-002")
		physical, _ := ws.Store.PhysicalMaxVersion(ctx)
		code := model.CodeOf(loadErr)
		return AnomalyCheck{
			Type: "snapshot_gap", Injection: "external snapshot version jump injected at +2",
			ExpectedCode: model.CodeSnapshotVersion, ActualCode: code,
			Passed:         code == model.CodeSnapshotVersion && chain.RecoveredThrough == beforeVersion && keysEqual(keyBefore, keyAfter),
			SnapshotBefore: beforeVersion, SnapshotAfter: chain.RecoveredThrough, PhysicalMaxAfter: physical,
			RecoveredThrough: chain.RecoveredThrough, PhaseBefore: keyBefore.Phase, PhaseAfter: keyAfter.Phase,
			StatePreserved: keysEqual(keyBefore, keyAfter),
			Detail:         "loader rejected the jump and fell back to the last contiguous snapshot",
		}, nil
	case "snapshot_replacement":
		beforeVersion, err := logicalVersion(ctx, ws.Store)
		if err != nil {
			return AnomalyCheck{}, err
		}
		keyBefore, err := ws.Store.GetKey(ctx, "key-alpha-002")
		if err != nil {
			return AnomalyCheck{}, err
		}
		entry, err := ws.Store.GetSnapshot(ctx, beforeVersion)
		if err != nil {
			return AnomalyCheck{}, err
		}
		replacement := *entry.State
		replacement.Keys = append([]model.Key(nil), replacement.Keys...)
		replacement.Schema = "keylife.snapshot/replaced"
		if len(replacement.Keys) > 0 {
			replacement.Keys[0].Phase = model.PhaseDestroyed
		}
		payload, err := json.Marshal(replacement)
		if err != nil {
			return AnomalyCheck{}, err
		}
		if err := ws.Store.InjectSnapshotReplacement(ctx, beforeVersion, payload); err != nil {
			return AnomalyCheck{}, err
		}
		chain, loadErr := ws.Store.LoadChain(ctx)
		code := model.CodeOf(loadErr)
		keyAfter, _ := ws.Store.GetKey(ctx, "key-alpha-002")
		return AnomalyCheck{
			Type: "snapshot_replacement", Injection: "same-version snapshot body replaced through external injector",
			ExpectedCode: model.CodeSnapshotReplaced, ActualCode: code,
			Passed:         code == model.CodeSnapshotReplaced && chain.RecoveredThrough == beforeVersion-1 && keysEqual(keyBefore, keyAfter),
			SnapshotBefore: beforeVersion, SnapshotAfter: chain.RecoveredThrough,
			PhysicalMaxAfter: chain.PhysicalMaxVersion, RecoveredThrough: chain.RecoveredThrough,
			PhaseBefore: keyBefore.Phase, PhaseAfter: keyAfter.Phase, StatePreserved: keysEqual(keyBefore, keyAfter),
			Detail: "content hash and chain hash detected replacement despite an unchanged version number",
		}, nil
	case "rotation_contention":
		return runRotationContention(ctx, ws, opts.NowTick)
	default:
		return AnomalyCheck{}, fmt.Errorf("unknown anomaly %q", anomaly)
	}
}

type whatIf struct {
	keyID     string
	actor     string
	subject   string
	operation string
}

func anomalyDecision(ctx context.Context, ws *sample.Workspace, name, injection string, req whatIf, expected model.Code) (AnomalyCheck, error) {
	beforeVersion, err := logicalVersion(ctx, ws.Store)
	if err != nil {
		return AnomalyCheck{}, err
	}
	keyBefore, err := ws.Store.GetKey(ctx, req.keyID)
	if err != nil {
		return AnomalyCheck{}, err
	}
	decision := ws.Service.Evaluate(ctx, model.DecisionRequest{
		RequestID: "anomaly-" + name, Subject: req.subject, KeyID: req.keyID,
		Actor: req.actor, Operation: req.operation,
	})
	afterVersion, err := logicalVersion(ctx, ws.Store)
	if err != nil {
		return AnomalyCheck{}, err
	}
	keyAfter, err := ws.Store.GetKey(ctx, req.keyID)
	if err != nil {
		return AnomalyCheck{}, err
	}
	return AnomalyCheck{
		Type: name, Injection: injection, ExpectedCode: expected, ActualCode: decision.Code,
		Passed:         decision.Code == expected && beforeVersion == afterVersion && keysEqual(keyBefore, keyAfter),
		SnapshotBefore: beforeVersion, SnapshotAfter: afterVersion,
		PhaseBefore: keyBefore.Phase, PhaseAfter: keyAfter.Phase, StatePreserved: keysEqual(keyBefore, keyAfter),
	}, nil
}

func runRotationContention(ctx context.Context, ws *sample.Workspace, nowTick int64) (AnomalyCheck, error) {
	beforeVersion, err := logicalVersion(ctx, ws.Store)
	if err != nil {
		return AnomalyCheck{}, err
	}
	keyBefore, err := ws.Store.GetKey(ctx, "key-alpha-002")
	if err != nil {
		return AnomalyCheck{}, err
	}
	allBefore, err := ws.Store.ListKeys(ctx, "")
	if err != nil {
		return AnomalyCheck{}, err
	}
	ws.Clock.Set(nowTick)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hooked := service.New(ws.Store, ws.Clock, service.Config{MaxConcurrency: ws.MaxConcurrency, DefaultSubjectLimit: ws.SubjectLimit}, service.Hooks{
		BeforeCommit: func(operation, keyID string) {
			if operation == "rotate" && keyID == "key-alpha-002" {
				once.Do(func() { close(entered) })
				<-release
			}
		},
	})
	workerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	type outcome struct {
		result service.MutationResult
		err    error
	}
	first := make(chan outcome, 1)
	go func() {
		result, err := hooked.Rotate(workerCtx, service.TransitionRequest{Actor: "alpha-payments", KeyID: "key-alpha-002"})
		first <- outcome{result, err}
	}()
	select {
	case <-entered:
	case <-workerCtx.Done():
		close(release)
		return AnomalyCheck{}, fmt.Errorf("rotation did not reach commit gate: %w", workerCtx.Err())
	}
	const contenders = 7
	contention := make(chan outcome, contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			result, err := hooked.Rotate(workerCtx, service.TransitionRequest{Actor: "alpha-payments", KeyID: "key-alpha-002"})
			contention <- outcome{result, err}
		}()
	}
	contentionErrors := 0
	for i := 0; i < contenders; i++ {
		result := <-contention
		if model.CodeOf(result.err) == model.CodeRotationContention {
			contentionErrors++
		}
	}
	close(release)
	firstResult := <-first
	afterVersion, err := logicalVersion(ctx, ws.Store)
	if err != nil {
		return AnomalyCheck{}, err
	}
	keyAfter, err := ws.Store.GetKey(ctx, "key-alpha-002")
	if err != nil {
		return AnomalyCheck{}, err
	}
	allAfter, err := ws.Store.ListKeys(ctx, "")
	if err != nil {
		return AnomalyCheck{}, err
	}
	successorID := ""
	if firstResult.result.Successor != nil {
		successorID = firstResult.result.Successor.ID
	}
	otherUnchanged := otherKeysUnchanged(allBefore, allAfter, "key-alpha-002", successorID)
	chain, chainErr := ws.Store.LoadChain(ctx)
	success := firstResult.err == nil && firstResult.result.SnapshotVersion == beforeVersion+1
	passed := success && contentionErrors == contenders && afterVersion == beforeVersion+1 &&
		keyBefore.Phase == model.PhaseActive && keyAfter.Phase == model.PhaseRotated && otherUnchanged && chainErr == nil
	detail := fmt.Sprintf("successes=%d contention=%d other_keys_unchanged=%t", boolInt(success), contentionErrors, otherUnchanged)
	return AnomalyCheck{
		Type: "rotation_contention", Injection: "8 concurrent rotate requests with a commit-gate barrier",
		ExpectedCode: model.CodeRotationContention, ActualCode: model.CodeRotationContention,
		Passed: passed, SnapshotBefore: beforeVersion, SnapshotAfter: afterVersion,
		PhysicalMaxAfter: chain.PhysicalMaxVersion, RecoveredThrough: chain.RecoveredThrough,
		PhaseBefore: keyBefore.Phase, PhaseAfter: keyAfter.Phase, StatePreserved: otherUnchanged,
		Detail: detail,
	}, nil
}

func otherKeysUnchanged(before, after []model.Key, changedID, successorID string) bool {
	beforeMap := make(map[string]model.Key)
	for _, key := range before {
		if key.ID != changedID {
			beforeMap[key.ID] = key
		}
	}
	for _, key := range after {
		if key.ID == changedID || key.ID == successorID {
			continue
		}
		prior, ok := beforeMap[key.ID]
		if !ok || model.SHA256Hex(mustJSON(prior)) != model.SHA256Hex(mustJSON(key)) {
			return false
		}
		delete(beforeMap, key.ID)
	}
	return len(beforeMap) == 0
}

func runCrashChecks(ctx context.Context, root string, opts Options) ([]CrashCheck, error) {
	checks := make([]CrashCheck, 0, 2)
	for _, point := range []string{"before_commit", "after_commit"} {
		check, err := runCrashCheck(ctx, root, opts, point)
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func runCrashCheck(ctx context.Context, root string, opts Options, point string) (CrashCheck, error) {
	dir := filepath.Join(root, "crash-"+point)
	ws, err := sample.Build(ctx, sampleOptions(opts, dir, 1))
	if err != nil {
		return CrashCheck{}, err
	}
	keyID := "key-alpha-002"
	beforeChain, err := ws.Store.LoadChain(ctx)
	if err != nil {
		ws.Close()
		return CrashCheck{}, err
	}
	beforeVersion := beforeChain.RecoveredThrough
	dbPath := ws.DBPath
	if err := ws.Close(); err != nil {
		return CrashCheck{}, err
	}
	cmd := exec.Command(opts.SelfExe, "_crash", "--db", dbPath, "--key", keyID, "--actor", "alpha-payments", "--crash-point", point, "--now-tick", fmt.Sprint(opts.NowTick))
	output, runErr := cmd.CombinedOutput()
	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return CrashCheck{}, fmt.Errorf("start crash child: %w", runErr)
		}
	}
	st, err := store.Open(dbPath)
	if err != nil {
		return CrashCheck{}, err
	}
	defer st.Close()
	chain, chainErr := st.LoadChain(ctx)
	key, keyErr := st.GetKey(ctx, keyID)
	if keyErr != nil {
		return CrashCheck{}, keyErr
	}
	check := CrashCheck{
		Point: point, ProcessExitCode: exitCode, ExpectedExit: 91,
		SnapshotBefore: beforeVersion, SnapshotAtRestart: chain.RecoveredThrough, SnapshotAfter: chain.RecoveredThrough,
		PhaseAtRestart: key.Phase, PhaseAfter: key.Phase, ChainValid: chainErr == nil,
	}
	if point == "before_commit" {
		check.ExpectedAfter = beforeVersion + 1
		if exitCode == 91 && chainErr == nil && chain.RecoveredThrough == beforeVersion && key.Phase == model.PhaseActive {
			clock := model.NewVirtualClock(opts.NowTick)
			svc := service.New(st, clock, service.Config{MaxConcurrency: 2, DefaultSubjectLimit: 8}, service.Hooks{})
			if _, err := svc.Rotate(ctx, service.TransitionRequest{Actor: "alpha-payments", KeyID: keyID}); err == nil {
				recovered, loadErr := st.LoadChain(ctx)
				check.RetrySucceeded = loadErr == nil && recovered.RecoveredThrough == beforeVersion+1
				if loadErr == nil {
					check.SnapshotAfter = recovered.RecoveredThrough
				}
				if rotated, err := st.GetKey(ctx, keyID); err == nil {
					check.PhaseAfter = rotated.Phase
				}
			}
		}
		check.Passed = exitCode == 91 && check.ChainValid && check.SnapshotAtRestart == beforeVersion && key.Phase == model.PhaseActive && check.RetrySucceeded && check.SnapshotAfter == beforeVersion+1
	} else {
		check.ExpectedAfter = beforeVersion + 1
		check.Passed = exitCode == 91 && chainErr == nil && chain.RecoveredThrough == beforeVersion+1 && key.Phase == model.PhaseRotated
		check.Detail = "committed rotation survived forced process exit"
	}
	if !check.Passed && len(output) > 0 {
		check.Detail = strings.TrimSpace(string(output))
	}
	return check, nil
}

func chainCheck(chain store.ChainResult) ChainCheck {
	check := ChainCheck{Valid: true, RecoveredThrough: chain.RecoveredThrough, PhysicalMax: chain.PhysicalMaxVersion}
	for _, entry := range chain.Entries {
		check.Versions = append(check.Versions, entry.Version)
		check.HeadChainHash = entry.ChainHash
	}
	return check
}

func logicalVersion(ctx context.Context, st *store.Store) (int64, error) {
	chain, err := st.LoadChain(ctx)
	if err != nil {
		return 0, err
	}
	return chain.RecoveredThrough, nil
}

func keysEqual(a, b model.Key) bool {
	return model.SHA256Hex(mustJSON(a)) == model.SHA256Hex(mustJSON(b))
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func sampleOptions(opts Options, dir string, scale int) sample.Options {
	return sample.Options{
		Dir: dir, Threshold: opts.Threshold, NowTick: opts.NowTick, Scale: scale,
		MaxConcurrency: opts.MaxConcurrency, SubjectLimit: opts.SubjectLimit,
	}
}
