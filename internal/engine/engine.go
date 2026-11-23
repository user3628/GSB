package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"secverify/internal/config"
	"secverify/internal/deps"
	"secverify/internal/model"
	"secverify/internal/probe"
	"secverify/internal/snapshot"
)

type Options struct {
	DBPath          string
	WorkDir         string
	MaxTransactions int
	MaxProbes       int
	WaitTimeout     time.Duration
	ProbeTimeout    time.Duration
	ProbeDelay      time.Duration
	WorkerPath      string
}

type Engine struct {
	opts      Options
	store     *snapshot.Store
	resources *resourceManager
	runner    *probe.Runner
	sandbox   string
}

type InitResult struct {
	Namespace string
	Version   int64
	Existed   bool
	Snapshot  SnapshotRef
}

type ApplyRequest struct {
	Namespace   string
	RoundName   string
	Changeset   []byte
	WaitTimeout time.Duration
	ProbeGate   <-chan struct{}
	ProbeReady  func()
	CrashPoint  string
	CrashNotify func(point string)
}

type Issue struct {
	Code            string       `json:"code"`
	ItemID          string       `json:"item_id,omitempty"`
	SourceOffset    int          `json:"source_offset"`
	Message         string       `json:"message,omitempty"`
	Items           []string     `json:"items,omitempty"`
	Edges           []model.Edge `json:"edges,omitempty"`
	FallbackVersion int64        `json:"fallback_version"`
}

type ProbeResult struct {
	ItemID       string `json:"item_id"`
	Kind         string `json:"kind"`
	Target       string `json:"target"`
	Method       string `json:"method"`
	SyncMarker   string `json:"sync_marker"`
	ExpectedMode string `json:"expected_mode,omitempty"`
	ObservedMode string `json:"observed_mode,omitempty"`
	Expected     string `json:"expected"`
	Observed     string `json:"observed"`
	Match        bool   `json:"match"`
	Code         string `json:"code"`
}

type SnapshotRef struct {
	Version     int64  `json:"version"`
	ContentHash string `json:"content_hash"`
	ParentHash  string `json:"parent_hash,omitempty"`
	StateHash   string `json:"state_hash"`
	ChangeHash  string `json:"change_hash"`
}

type RoundResult struct {
	Namespace        string             `json:"namespace"`
	RoundName        string             `json:"round_name"`
	BusinessCode     string             `json:"business_code"`
	VersionBefore    int64              `json:"version_before"`
	VersionAfter     int64              `json:"version_after"`
	SnapshotAdvanced bool               `json:"snapshot_advanced"`
	Diffs            []model.Diff       `json:"diffs"`
	Invalidation     model.Invalidation `json:"invalidation"`
	Probes           []ProbeResult      `json:"probes"`
	Issue            *Issue             `json:"issue,omitempty"`
	Concurrency      ConcurrencyStats   `json:"concurrency"`
	Snapshot         *SnapshotRef       `json:"snapshot,omitempty"`
}

func Open(opts Options) (*Engine, error) {
	if opts.DBPath == "" || opts.WorkDir == "" {
		return nil, fmt.Errorf("DBPath and WorkDir are required")
	}
	if opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 10 * time.Second
	}
	if err := os.MkdirAll(opts.WorkDir, 0o700); err != nil {
		return nil, err
	}
	store, err := snapshot.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}
	return &Engine{
		opts:      opts,
		store:     store,
		resources: newResourceManager(opts.MaxTransactions, opts.MaxProbes, opts.WaitTimeout),
		runner: &probe.Runner{
			WorkerPath: opts.WorkerPath,
			Timeout:    opts.ProbeTimeout,
			Delay:      opts.ProbeDelay,
		},
		sandbox: filepath.Join(opts.WorkDir, "sandbox"),
	}, nil
}

func (e *Engine) Close() error { return e.store.Close() }

func (e *Engine) Sandbox() string { return e.sandbox }

func (e *Engine) NamespaceSandbox(namespace string) string {
	sum := sha256.Sum256([]byte(namespace))
	return filepath.Join(e.sandbox, fmt.Sprintf("%x", sum[:8]))
}

func (e *Engine) Initialize(ctx context.Context, namespace string, items []model.Item) (*InitResult, *model.BizError) {
	releaseNS, _, bizErr := e.resources.acquireNamespace(ctx, namespace, 0)
	if bizErr != nil {
		return nil, bizErr
	}
	defer releaseNS()
	releaseTX, bizErr := e.resources.acquireTransaction(ctx, 0)
	if bizErr != nil {
		return nil, bizErr
	}
	defer releaseTX()

	chain, chainErr := e.store.LoadChain(ctx, namespace)
	if chainErr != nil {
		return nil, chainErr
	}
	if chain.Latest != nil {
		return &InitResult{Namespace: namespace, Version: chain.Latest.Version, Existed: true, Snapshot: refFromSnapshot(*chain.Latest)}, nil
	}
	normalized, bizErr := config.NormalizeItems(items)
	if bizErr != nil {
		return nil, bizErr
	}
	graph := deps.Build(normalized)
	if bizErr := graph.Validate(); bizErr != nil {
		return nil, bizErr
	}
	if _, bizErr := config.ResolveAccounts(normalized); bizErr != nil {
		return nil, bizErr
	}
	created, err := e.store.Initialize(ctx, namespace, normalized)
	if err != nil {
		return nil, model.NewError(model.CodeStore, "", -1, err.Error())
	}
	return &InitResult{Namespace: namespace, Version: 0, Snapshot: refFromSnapshot(*created)}, nil
}

// Apply executes parse -> graph validation -> closure -> real probe -> single
// snapshot/report transaction. Every pre-commit failure leaves the chain and
// the previous snapshot untouched.
func (e *Engine) Apply(ctx context.Context, req ApplyRequest) (RoundResult, error) {
	result, err := e.apply(ctx, req)
	if result.Issue != nil {
		result.BusinessCode = result.Issue.Code
	} else {
		result.BusinessCode = model.CodeOK
	}
	return result, err
}

func (e *Engine) apply(ctx context.Context, req ApplyRequest) (RoundResult, error) {
	result := RoundResult{Namespace: req.Namespace, RoundName: req.RoundName, BusinessCode: model.CodeOK, VersionBefore: -1, VersionAfter: -1, Diffs: []model.Diff{}, Probes: []ProbeResult{}}
	wait := req.WaitTimeout
	if wait <= 0 {
		wait = e.opts.WaitTimeout
	}
	releaseNS, _, bizErr := e.resources.acquireNamespace(ctx, req.Namespace, wait)
	if bizErr != nil {
		result.Concurrency = e.resources.stats(wait)
		result.Issue = issueFromError(bizErr)
		return result, bizErr
	}
	defer releaseNS()
	releaseTX, bizErr := e.resources.acquireTransaction(ctx, wait)
	if bizErr != nil {
		result.Concurrency = e.resources.stats(wait)
		result.Issue = issueFromError(bizErr)
		return result, bizErr
	}
	defer releaseTX()

	chain, chainErr := e.store.LoadChain(ctx, req.Namespace)
	if chainErr != nil {
		result.VersionBefore = chainErr.FallbackVersion
		result.VersionAfter = chainErr.FallbackVersion
		result.Issue = issueFromError(chainErr)
		result.Concurrency = e.resources.stats(wait)
		return result, chainErr
	}
	if chain.Latest == nil {
		bizErr = model.NewError(model.CodeNamespaceUnavailable, "", -1, "namespace has no baseline snapshot")
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	result.VersionBefore = chain.Latest.Version
	result.VersionAfter = chain.Latest.Version

	current, bizErr := config.NormalizeItems(chain.Latest.Payload.Items)
	if bizErr != nil {
		corrupt := model.NewError(model.CodeSnapshotCorrupt, bizErr.ItemID, -1, "stored items are invalid: "+bizErr.Message)
		corrupt.FallbackVersion = chain.Latest.Version
		result.Issue = issueFromError(corrupt)
		result.Concurrency = e.resources.stats(wait)
		return result, corrupt
	}
	oldGraph := deps.Build(current)
	if bizErr := oldGraph.Validate(); bizErr != nil {
		corrupt := model.NewError(model.CodeSnapshotCorrupt, bizErr.ItemID, -1, "stored dependency graph is invalid: "+bizErr.Message)
		corrupt.FallbackVersion = chain.Latest.Version
		result.Issue = issueFromError(corrupt)
		result.Concurrency = e.resources.stats(wait)
		return result, corrupt
	}
	resolvedCurrent, bizErr := config.ResolveAccounts(current)
	if bizErr != nil {
		corrupt := model.NewError(model.CodeSnapshotCorrupt, bizErr.ItemID, -1, "stored inheritance is invalid: "+bizErr.Message)
		corrupt.FallbackVersion = chain.Latest.Version
		result.Issue = issueFromError(corrupt)
		result.Concurrency = e.resources.stats(wait)
		return result, corrupt
	}
	changeSet, bizErr := config.ParseChangeSet(req.Changeset)
	if bizErr != nil {
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	candidate, bizErr := config.BuildCandidate(current, changeSet.Operations)
	if bizErr != nil {
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	candidateGraph := deps.Build(candidate)
	if bizErr := candidateGraph.Validate(); bizErr != nil {
		result.Issue = issueFromError(bizErr)
		result.Invalidation = invalidationFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	resolvedCandidate, bizErr := config.ResolveAccounts(candidate)
	if bizErr != nil {
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	result.Diffs = config.BuildDiffs(resolvedCurrent, resolvedCandidate, changeSet.Operations)
	triggers := config.TriggerIDs(result.Diffs)
	if len(triggers) > 0 {
		opByID := make(map[string]config.Operation, len(changeSet.Operations))
		for _, op := range changeSet.Operations {
			opByID[op.ID] = op
		}
		sources := make([]deps.TriggerSource, 0, len(triggers))
		for _, id := range triggers {
			graph := candidateGraph
			if opByID[id].Op == config.ActionDelete {
				graph = oldGraph
			}
			sources = append(sources, deps.TriggerSource{ID: id, Graph: graph})
		}
		result.Invalidation = deps.ComputeInvalidation(sources)
	}

	sandbox := e.NamespaceSandbox(req.Namespace)
	prepared, bizErr := probe.Materialize(sandbox, resolvedCandidate)
	if bizErr != nil {
		if bizErr.Code == model.CodeProbeEscape {
			escapeProbes, escapeErr := e.probeEscape(ctx, sandbox, resolvedCandidate, bizErr, wait, req)
			result.Probes = escapeProbes
			if escapeErr != nil {
				bizErr = escapeErr
			}
		}
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}

	probeRelease, bizErr := e.resources.acquireProbe(ctx, wait)
	if bizErr != nil {
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	if req.ProbeGate != nil {
		if req.ProbeReady != nil {
			req.ProbeReady()
		}
		select {
		case <-req.ProbeGate:
		case <-ctx.Done():
			probeRelease()
			bizErr = model.NewError(model.CodeChangeContention, "", -1, "probe gate cancelled")
			result.Issue = issueFromError(bizErr)
			result.Concurrency = e.resources.stats(wait)
			return result, bizErr
		}
	}
	if req.CrashPoint == "probe" {
		e.holdForCrash(req, "probe")
	}
	observations, bizErr := e.runner.Run(ctx, sandbox, prepared.Specs)
	probeRelease()
	if bizErr != nil {
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	result.Probes = compareObservations(resolvedCandidate, prepared, observations)
	if bizErr := probeFailure(result.Probes, resolvedCandidate); bizErr != nil {
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}

	nextVersion := chain.Latest.Version + 1
	payload, err := snapshot.NewRoundPayload(req.Namespace, nextVersion, chain.Latest.ContentHash, changeSet.Hash, candidate)
	if err != nil {
		bizErr = model.NewError(model.CodeStore, "", -1, err.Error())
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	content, err := snapshot.EncodePayload(payload)
	if err != nil {
		bizErr = model.NewError(model.CodeStore, "", -1, err.Error())
		result.Issue = issueFromError(bizErr)
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	contentHash := model.HashBytes(content)
	result.Snapshot = &SnapshotRef{Version: nextVersion, ContentHash: contentHash, ParentHash: chain.Latest.ContentHash, StateHash: payload.StateHash, ChangeHash: changeSet.Hash}
	result.Concurrency = e.resources.stats(wait)
	storedReport, _ := json.Marshal(result)
	beforeCommit := func() {
		if req.CrashPoint == "before-commit" {
			e.holdForCrash(req, "before-commit")
		}
	}
	committed, err := e.store.AppendRound(ctx, req.Namespace, chain.Latest.Version, chain.Latest.ContentHash, changeSet.Hash, candidate, storedReport, beforeCommit)
	if err != nil {
		bizErr = model.NewError(model.CodeStore, "", -1, err.Error())
		result.Issue = issueFromError(bizErr)
		result.Snapshot = nil
		result.Concurrency = e.resources.stats(wait)
		return result, bizErr
	}
	result.VersionAfter = committed.Version
	result.SnapshotAdvanced = true
	result.Snapshot = &SnapshotRef{Version: committed.Version, ContentHash: committed.ContentHash, ParentHash: committed.ParentHash, StateHash: committed.Payload.StateHash, ChangeHash: committed.Payload.ChangeHash}
	result.Concurrency = e.resources.stats(wait)
	return result, nil
}

func (e *Engine) holdForCrash(req ApplyRequest, point string) {
	if req.CrashNotify != nil {
		req.CrashNotify(point)
	}
	time.Sleep(10 * time.Minute)
}

func (e *Engine) probeEscape(ctx context.Context, sandbox string, items []model.Item, cause *model.BizError, wait time.Duration, req ApplyRequest) ([]ProbeResult, *model.BizError) {
	var target *model.Item
	for i := range items {
		if items[i].ID == cause.ItemID {
			target = &items[i]
			break
		}
	}
	if target == nil {
		return []ProbeResult{escapeResult(cause.ItemID, cause)}, nil
	}
	spec, specErr := probe.SpecForItem(*target)
	if specErr != nil {
		return []ProbeResult{escapeResult(cause.ItemID, cause)}, nil
	}
	probeRelease, bizErr := e.resources.acquireProbe(ctx, wait)
	if bizErr != nil {
		return nil, bizErr
	}
	defer probeRelease()
	if req.CrashPoint == "probe" {
		e.holdForCrash(req, "probe")
	}
	observations, bizErr := e.runner.Run(ctx, sandbox, []probe.Spec{spec})
	if bizErr != nil {
		return []ProbeResult{escapeResult(cause.ItemID, cause)}, nil
	}
	if len(observations) == 0 {
		return []ProbeResult{escapeResult(cause.ItemID, cause)}, nil
	}
	obs := observations[0]
	pr := ProbeResult{
		ItemID:     cause.ItemID,
		Kind:       target.Kind,
		Target:     spec.Path,
		Method:     obs.Method,
		SyncMarker: obs.SyncMarker,
		Observed:   strings.TrimSpace(obs.Message),
		Match:      false,
		Code:       model.CodeProbeEscape,
	}
	if pr.Method == "" {
		pr.Method = "parent_path_guard_v1"
	}
	if pr.SyncMarker == "" {
		pr.SyncMarker = probe.SyncMarker
	}
	if pr.Observed == "" {
		pr.Observed = "refused"
	}
	return []ProbeResult{pr}, nil
}

func escapeResult(itemID string, cause *model.BizError) ProbeResult {
	return ProbeResult{ItemID: itemID, Target: cause.ItemID, Method: "parent_path_guard_v1", SyncMarker: probe.SyncMarker, Observed: cause.Message, Match: false, Code: model.CodeProbeEscape}
}

func compareObservations(items []model.Item, prepared *probe.Prepared, observations []probe.Observation) []ProbeResult {
	byID := model.ItemMap(items)
	obsByID := make(map[string]probe.Observation, len(observations))
	for _, obs := range observations {
		obsByID[obs.ItemID] = obs
	}
	results := make([]ProbeResult, 0, len(items))
	model.SortItems(items)
	for _, item := range items {
		obs, ok := obsByID[item.ID]
		pr := ProbeResult{ItemID: item.ID, Kind: item.Kind, Target: prepared.Paths[item.ID], Code: model.CodeProbeMismatch}
		if !ok {
			pr.Method = "missing_observation"
			pr.Observed = "worker returned no observation"
			results = append(results, pr)
			continue
		}
		pr.Method = obs.Method
		pr.SyncMarker = obs.SyncMarker
		pr.ObservedMode = obs.Mode
		pr.Observed = strings.TrimSuffix(obs.Content, "\n")
		if obs.ErrorCode != "" {
			pr.Code = obs.ErrorCode
			if pr.Observed == "" {
				pr.Observed = strings.TrimSpace(obs.Message)
			}
			results = append(results, pr)
			continue
		}
		if !obs.Exists {
			pr.Observed = strings.TrimSpace(obs.Message)
			results = append(results, pr)
			continue
		}
		current, exists := byID[item.ID]
		if !exists {
			results = append(results, pr)
			continue
		}
		match := true
		switch current.Kind {
		case model.KindAccount:
			pr.Expected = strings.TrimSuffix(probe.ExpectedAccountContent(current), "\n")
			match = semanticJSONEqual(pr.Expected, pr.Observed)
		case model.KindAudit:
			pr.Expected = strings.TrimSuffix(probe.ExpectedAuditContent(current), "\n")
			pr.ExpectedMode = current.Audit.Mode
			match = pr.ExpectedMode == obs.Mode && semanticJSONEqual(pr.Expected, pr.Observed)
		case model.KindService:
			pr.Expected = strings.TrimSuffix(probe.ExpectedServiceContent(current), "\n")
			match = semanticJSONEqual(pr.Expected, pr.Observed)
		case model.KindRegistry:
			pr.Expected = strings.TrimSuffix(probe.ExpectedRegistryContent(current), "\n")
			pr.ExpectedMode = current.Registry.Mode
			match = pr.ExpectedMode == obs.Mode && semanticJSONEqual(pr.Expected, pr.Observed)
		default:
			match = false
		}
		if obs.SyncMarker != probe.SyncMarker {
			match = false
		}
		pr.Match = match
		if match {
			pr.Code = model.CodeOK
		}
		results = append(results, pr)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].ItemID != results[j].ItemID {
			return results[i].ItemID < results[j].ItemID
		}
		return results[i].Code < results[j].Code
	})
	return results
}

func semanticJSONEqual(left, right string) bool {
	var lv, rv any
	if json.Unmarshal([]byte(left), &lv) != nil || json.Unmarshal([]byte(right), &rv) != nil {
		return false
	}
	lb, _ := json.Marshal(lv)
	rb, _ := json.Marshal(rv)
	return bytes.Equal(lb, rb)
}

func probeFailure(results []ProbeResult, items []model.Item) *model.BizError {
	escapeIDs := make([]string, 0)
	mismatchIDs := make([]string, 0)
	firstOffset := -1
	byID := model.ItemMap(items)
	for _, result := range results {
		if result.Match || result.Code == model.CodeOK {
			continue
		}
		if result.Code == model.CodeProbeEscape {
			escapeIDs = append(escapeIDs, result.ItemID)
		} else {
			mismatchIDs = append(mismatchIDs, result.ItemID)
		}
		if firstOffset < 0 {
			if item, ok := byID[result.ItemID]; ok {
				firstOffset = item.SourceOffset
			}
		}
	}
	if len(escapeIDs) > 0 {
		sort.Strings(escapeIDs)
		return &model.BizError{Code: model.CodeProbeEscape, ItemID: escapeIDs[0], SourceOffset: firstOffset, Message: "probe path escape refused", Items: escapeIDs, FallbackVersion: -1}
	}
	if len(mismatchIDs) > 0 {
		sort.Strings(mismatchIDs)
		return &model.BizError{Code: model.CodeProbeMismatch, ItemID: mismatchIDs[0], SourceOffset: firstOffset, Message: "observed state differs from expected state", Items: mismatchIDs, FallbackVersion: -1}
	}
	return nil
}

func issueFromError(err *model.BizError) *Issue {
	if err == nil {
		return nil
	}
	fallback := err.FallbackVersion
	if fallback == 0 && err.Code != model.CodeSnapshotVersion {
		fallback = -1
	}
	return &Issue{
		Code:            err.Code,
		ItemID:          err.ItemID,
		SourceOffset:    err.SourceOffset,
		Message:         err.Message,
		Items:           append([]string(nil), err.Items...),
		Edges:           append([]model.Edge(nil), err.Edges...),
		FallbackVersion: fallback,
	}
}

func invalidationFromError(err *model.BizError) model.Invalidation {
	return model.Invalidation{
		Triggers: append([]string(nil), err.Items...),
		Items:    append([]string(nil), err.Items...),
		Edges:    append([]model.Edge(nil), err.Edges...),
		Traces:   []model.TraceEntry{},
	}
}

func refFromSnapshot(s snapshot.Snapshot) SnapshotRef {
	return SnapshotRef{Version: s.Version, ContentHash: s.ContentHash, ParentHash: s.ParentHash, StateHash: s.Payload.StateHash, ChangeHash: s.Payload.ChangeHash}
}

func (e *Engine) History(ctx context.Context, namespace string) ([]SnapshotRef, *model.BizError) {
	chain, bizErr := e.store.LoadChain(ctx, namespace)
	if bizErr != nil {
		return nil, bizErr
	}
	refs := make([]SnapshotRef, 0, len(chain.Snapshots))
	for _, snap := range chain.Snapshots {
		refs = append(refs, refFromSnapshot(snap))
	}
	return refs, nil
}

func (e *Engine) RawChain(ctx context.Context, namespace string) (*snapshot.Chain, *model.BizError) {
	return e.store.LoadChain(ctx, namespace)
}

func (e *Engine) InjectVersionGap(ctx context.Context, namespace string, version int64) error {
	return e.store.DeleteSnapshotForInjection(ctx, namespace, version)
}

func (e *Engine) ResourceStats(wait time.Duration) ConcurrencyStats {
	return e.resources.stats(wait)
}
