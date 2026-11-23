package replay

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"protocolfuzzlab/internal/codes"
	"protocolfuzzlab/internal/graph"
	"protocolfuzzlab/internal/model"
	"protocolfuzzlab/internal/protocol"
	"protocolfuzzlab/internal/scheduler"
	"protocolfuzzlab/internal/store"
	"protocolfuzzlab/internal/target"
)

const (
	Schema         = "protocol-fuzzlab.execution/v1"
	TargetIdentity = "loopback-deterministic-v1"
	SampleMarker   = store.SampleMarker
)

type Options struct {
	CaseSetVersion int
	BatchSize      int
	MaxConcurrency int
	CaseBudget     int
	NoCommit       bool
	TamperCaseID   string
	TamperStep     int
	TargetAddr     string
	OnStats        func(scheduler.Stats)
	AfterCommit    func(committed int)
}

type TraceStep struct {
	From protocol.State       `json:"from"`
	Type protocol.MessageType `json:"message_type"`
	To   protocol.State       `json:"to"`
	Code string               `json:"code"`
}

type AssertionResult struct {
	ID      string         `json:"id"`
	From    protocol.State `json:"from"`
	To      protocol.State `json:"to"`
	Covered bool           `json:"covered"`
	Passed  bool           `json:"passed"`
	Skipped bool           `json:"skipped"`
	Code    string         `json:"code"`
}

type CaseResult struct {
	CaseID     string                `json:"case_id"`
	Code       string                `json:"code"`
	FinalState protocol.State        `json:"final_state"`
	FramesSent int                   `json:"frames_sent"`
	Trace      []TraceStep           `json:"trace"`
	Coverage   []model.TransitionKey `json:"coverage"`
	Assertions []AssertionResult     `json:"assertions"`
	Diagnostic *model.Diagnostic     `json:"diagnostic,omitempty"`
}

type ExecutionBatch struct {
	Schema         string                `json:"schema"`
	SampledAt      string                `json:"sampled_at"`
	BatchID        string                `json:"batch_id"`
	LogicalBatch   string                `json:"logical_batch"`
	CaseSetVersion int                   `json:"caseset_version"`
	CaseSetDigest  string                `json:"caseset_digest"`
	Target         string                `json:"target"`
	Code           string                `json:"code"`
	CaseBudget     int                   `json:"case_budget"`
	BatchSize      int                   `json:"batch_size"`
	Cases          []CaseResult          `json:"cases"`
	Unprocessed    []string              `json:"unprocessed"`
	Coverage       []model.TransitionKey `json:"coverage"`
	LostCoverage   []model.TransitionKey `json:"lost_coverage,omitempty"`
}

type BatchRun struct {
	BatchID       string               `json:"batch_id"`
	LogicalBatch  string               `json:"logical_batch"`
	ReplayKey     string               `json:"replay_key"`
	Execution     ExecutionBatch       `json:"execution"`
	Snapshot      store.SnapshotRecord `json:"snapshot"`
	NewSnapshot   bool                 `json:"new_snapshot"`
	QueueWaitNote string               `json:"queue_wait_note,omitempty"`
}

type RunResult struct {
	CaseSetVersion int                    `json:"caseset_version"`
	CaseSetDigest  string                 `json:"caseset_digest"`
	Target         string                 `json:"target"`
	Batches        []BatchRun             `json:"batches"`
	Snapshots      []store.SnapshotRecord `json:"snapshots"`
	ChainError     *codes.Error           `json:"chain_error,omitempty"`
	Coverage       []model.TransitionKey  `json:"coverage"`
	Stats          scheduler.Stats        `json:"scheduler"`
	ContentDigest  string                 `json:"content_digest"`
	InvalidCaseIDs []string               `json:"invalid_case_ids"`
}

type batchPlan struct {
	index        int
	batchID      string
	logicalBatch string
	caseIDs      []string
	replayKey    string
}

type replayKeyMaterial struct {
	Version    int      `json:"version"`
	Digest     string   `json:"digest"`
	Logical    string   `json:"logical_batch"`
	BatchID    string   `json:"batch_id"`
	Cases      []string `json:"cases"`
	Budget     int      `json:"case_budget"`
	TamperCase string   `json:"tamper_case"`
	TamperStep int      `json:"tamper_step"`
	Target     string   `json:"target"`
	Schema     string   `json:"schema"`
}

func defaultOptions(opts Options) Options {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 16
	}
	if opts.MaxConcurrency <= 0 {
		opts.MaxConcurrency = 4
	}
	if opts.CaseBudget <= 0 {
		opts.CaseBudget = 1000
	}
	if opts.TamperStep < 0 {
		opts.TamperStep = 0
	}
	return opts
}

func Run(ctx context.Context, st *store.Store, opts Options) (RunResult, error) {
	opts = defaultOptions(opts)
	if opts.CaseSetVersion <= 0 {
		head, err := st.Head()
		if err != nil {
			return RunResult{}, err
		}
		opts.CaseSetVersion = head.Version
	}
	version, err := st.LoadCaseSet(opts.CaseSetVersion)
	if err != nil {
		return RunResult{}, err
	}
	cs := version.CaseSet
	cs.Normalize()

	addr := opts.TargetAddr
	var server *target.Server
	var cancel context.CancelFunc
	if addr == "" {
		serverCtx, c := context.WithCancel(ctx)
		cancel = c
		server, err = target.Start(serverCtx)
		if err != nil {
			return RunResult{}, codes.New(codes.ConnectionError, "start deterministic target: %v", err)
		}
		addr = server.Addr()
		defer func() {
			cancel()
			_ = server.Close()
		}()
	} else if !isLoopback(addr) {
		return RunResult{}, codes.New(codes.BadRequest, "target address %q is not loopback", addr)
	}

	invalidCases, invalidAssertions := invalidationMaps(cs, version.Version)
	plans := makeBatchPlans(cs, opts)
	sched := scheduler.New(opts.MaxConcurrency)
	runs := make([]BatchRun, len(plans))
	missing := make([]scheduler.Job, 0, len(plans))
	existing := make([]bool, len(plans))

	for i, plan := range plans {
		key := plan.replayKey
		if !opts.NoCommit {
			snap, found, lookupErr := st.SnapshotByReplayKey(key)
			if lookupErr != nil {
				return RunResult{}, lookupErr
			}
			if found {
				var exec ExecutionBatch
				if err := json.Unmarshal(snap.Content, &exec); err != nil {
					return RunResult{}, fmt.Errorf("decode committed snapshot %d: %w", snap.Version, err)
				}
				existing[i] = true
				runs[i] = BatchRun{BatchID: plan.batchID, LogicalBatch: plan.logicalBatch, ReplayKey: key, Execution: exec, Snapshot: snap, NewSnapshot: false}
				continue
			}
		}
		planCopy := plan
		missing = append(missing, scheduler.Job{
			Index: planCopy.index,
			ID:    planCopy.batchID,
			Run: func(jobCtx context.Context) error {
				exec, runErr := executeBatch(jobCtx, addr, cs, version, planCopy, opts, invalidCases, invalidAssertions)
				if runErr != nil {
					return runErr
				}
				runs[planCopy.index] = BatchRun{BatchID: planCopy.batchID, LogicalBatch: planCopy.logicalBatch, ReplayKey: planCopy.replayKey, Execution: exec, NewSnapshot: true}
				return nil
			},
		})
	}

	if len(missing) > 0 {
		errs := sched.Run(ctx, missing)
		for i, runErr := range errs {
			if runErr != nil {
				return RunResult{}, runErr
			}
			_ = i
		}
	}
	if opts.OnStats != nil {
		opts.OnStats(sched.Stats())
	}

	committed := 0
	for i := range runs {
		if existing[i] || opts.NoCommit {
			continue
		}
		content, err := canonicalJSON(runs[i].Execution)
		if err != nil {
			return RunResult{}, err
		}
		digest := model.DigestBytes(content)
		prev, found, err := st.LatestSnapshotForLogical(runs[i].LogicalBatch)
		if err != nil {
			return RunResult{}, err
		}
		whole := false
		changed := false
		prevVersion := 0
		if found {
			prevVersion = prev.Version
			changed = prev.Digest != digest
			var prevExec ExecutionBatch
			if err := json.Unmarshal(prev.Content, &prevExec); err == nil {
				whole = isWholeReplacement(prevExec.Cases, runs[i].Execution.Cases)
			}
		}
		snap, created, err := st.CommitSnapshot(store.SnapshotCommit{
			CaseSetVersion:   version.Version,
			BatchID:          runs[i].BatchID,
			LogicalBatch:     runs[i].LogicalBatch,
			ReplayKey:        runs[i].ReplayKey,
			Digest:           digest,
			Content:          content,
			WholeReplacement: whole,
			ContentChanged:   changed,
			PreviousVersion:  prevVersion,
		})
		if err != nil {
			return RunResult{}, err
		}
		runs[i].Snapshot = snap
		runs[i].NewSnapshot = created
		if created {
			committed++
			if opts.AfterCommit != nil {
				opts.AfterCommit(committed)
			}
		}
	}

	chain, chainErr := st.SnapshotChain()
	coverage := unionCoverage(runs)
	invalidIDs := make([]string, 0, len(invalidCases))
	for id := range invalidCases {
		invalidIDs = append(invalidIDs, id)
	}
	sort.Strings(invalidIDs)
	contentBatches := make([]ExecutionBatch, 0, len(runs))
	for _, run := range runs {
		contentBatches = append(contentBatches, run.Execution)
	}
	contentJSON, err := canonicalJSON(contentBatches)
	if err != nil {
		return RunResult{}, err
	}
	result := RunResult{
		CaseSetVersion: version.Version,
		CaseSetDigest:  version.Digest,
		Target:         TargetIdentity,
		Batches:        runs,
		Snapshots:      chain,
		Coverage:       coverage,
		Stats:          sched.Stats(),
		ContentDigest:  model.DigestBytes(contentJSON),
		InvalidCaseIDs: invalidIDs,
	}
	if chainErr != nil {
		if coded, ok := chainErr.(*codes.Error); ok {
			result.ChainError = coded
		} else {
			result.ChainError = codes.From(chainErr)
		}
	}
	return result, nil
}

func makeBatchPlans(cs model.CaseSet, opts Options) []batchPlan {
	ids := cs.CaseIDs()
	if len(ids) == 0 {
		return []batchPlan{{index: 0, batchID: "batch-001", logicalBatch: "slot-001", caseIDs: nil, replayKey: replayKey(opts, cs.Digest(), "slot-001", "batch-001", nil)}}
	}
	var plans []batchPlan
	for i := 0; i < len(ids); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(ids) {
			end = len(ids)
		}
		idx := len(plans)
		batchID := fmt.Sprintf("batch-%03d", idx+1)
		logical := fmt.Sprintf("slot-%03d", idx+1)
		batchCases := append([]string(nil), ids[i:end]...)
		plans = append(plans, batchPlan{
			index:        idx,
			batchID:      batchID,
			logicalBatch: logical,
			caseIDs:      batchCases,
			replayKey:    replayKey(opts, cs.Digest(), logical, batchID, batchCases),
		})
	}
	return plans
}

func replayKey(opts Options, digest, logical, batchID string, caseIDs []string) string {
	material := replayKeyMaterial{
		Version: opts.CaseSetVersion, Digest: digest, Logical: logical, BatchID: batchID,
		Cases: caseIDs, Budget: opts.CaseBudget, TamperCase: opts.TamperCaseID,
		TamperStep: opts.TamperStep, Target: TargetIdentity, Schema: Schema,
	}
	b, _ := json.Marshal(material)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func executeBatch(ctx context.Context, addr string, cs model.CaseSet, version store.CaseSetVersion, plan batchPlan, opts Options, invalidCases map[string]model.Diagnostic, invalidAssertions map[string]model.Diagnostic) (ExecutionBatch, error) {
	batch := ExecutionBatch{
		Schema: Schema, SampledAt: SampleMarker, BatchID: plan.batchID, LogicalBatch: plan.logicalBatch,
		CaseSetVersion: version.Version, CaseSetDigest: version.Digest, Target: TargetIdentity,
		Code: codes.OK, CaseBudget: opts.CaseBudget, BatchSize: opts.BatchSize,
	}
	limit := len(plan.caseIDs)
	if limit > opts.CaseBudget {
		limit = opts.CaseBudget
		batch.Unprocessed = append([]string(nil), plan.caseIDs[limit:]...)
		batch.Code = codes.CaseBudget
	}
	coverageSet := map[string]model.TransitionKey{}
	for _, caseID := range plan.caseIDs[:limit] {
		if ctx.Err() != nil {
			return ExecutionBatch{}, ctx.Err()
		}
		result := replayCase(ctx, addr, cs, caseID, opts, invalidCases, invalidAssertions)
		batch.Cases = append(batch.Cases, result)
		for _, key := range result.Coverage {
			coverageSet[key.String()] = key
		}
	}
	sort.Slice(batch.Cases, func(i, j int) bool { return batch.Cases[i].CaseID < batch.Cases[j].CaseID })
	batch.Coverage = sortedCoverage(coverageSet)
	if batch.Code == codes.OK {
		for _, result := range batch.Cases {
			if result.Code != codes.OK {
				batch.Code = result.Code
				break
			}
		}
	}
	return batch, nil
}

func replayCase(ctx context.Context, addr string, cs model.CaseSet, caseID string, opts Options, invalidCases, invalidAssertions map[string]model.Diagnostic) CaseResult {
	result := CaseResult{CaseID: caseID, Code: codes.OK, FinalState: protocol.StateHandshake}
	if diag, invalid := invalidCases[caseID]; invalid {
		result.Code = diag.Code
		result.Diagnostic = &diag
		result.Assertions = assertionResults(cs, caseID, nil, invalidAssertions)
		return result
	}
	tc, ok := cs.Case(caseID)
	if !ok {
		result.Code = codes.DepInvalidated
		result.Diagnostic = &model.Diagnostic{Entity: model.EntityCase, EntityID: caseID, Code: codes.DepInvalidated, RootChain: []string{"case:" + caseID}, Version: 0}
		return result
	}
	frames, err := materialize(cs, tc)
	if err != nil {
		coded := codes.From(err)
		result.Code = coded.Code
		if coded.Code == codes.DepInvalidated || coded.Code == codes.DepCycle {
			result.Diagnostic = &model.Diagnostic{Entity: model.EntityCase, EntityID: caseID, Code: coded.Code, RootChain: coded.Chain}
		}
		return result
	}
	networkResult := runFrames(ctx, addr, frames, caseID, opts)
	result.Code = networkResult.code
	result.FinalState = networkResult.finalState
	result.FramesSent = networkResult.framesSent
	result.Trace = networkResult.trace
	result.Coverage = networkResult.coverage
	result.Assertions = assertionResults(cs, caseID, networkResult.coverage, invalidAssertions)
	if result.Code == codes.OK {
		for _, assertion := range result.Assertions {
			if !assertion.Skipped && !assertion.Passed {
				result.Code = codes.AssertionMismatch
				break
			}
		}
	}
	return result
}

type frameRunResult struct {
	code       string
	finalState protocol.State
	framesSent int
	trace      []TraceStep
	coverage   []model.TransitionKey
}

func runFrames(ctx context.Context, addr string, frames []protocol.Frame, caseID string, opts Options) frameRunResult {
	out := frameRunResult{code: codes.OK, finalState: protocol.StateHandshake}
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		out.code = codes.ConnectionError
		return out
	}
	defer conn.Close()
	coverageSet := map[string]model.TransitionKey{}
	for i, frame := range frames {
		var wire []byte
		if opts.TamperCaseID == caseID && i == opts.TamperStep {
			wire, err = protocol.EncodeTampered(frame)
		} else {
			wire, err = protocol.Encode(frame)
		}
		if err != nil {
			if errors.Is(err, protocol.ErrBadWireLength) {
				out.code = codes.PayloadOverflow
			} else {
				out.code = codes.BadRequest
			}
			return out
		}
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write(wire); err != nil {
			out.code = codes.ConnectionError
			return out
		}
		out.framesSent++
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		response, err := protocol.Decode(conn)
		if err != nil {
			if errors.Is(err, protocol.ErrBadChecksum) {
				out.code = codes.BadChecksum
			} else if errors.Is(err, protocol.ErrPayloadOverflow) {
				out.code = codes.PayloadOverflow
			} else {
				out.code = codes.TargetResponse
			}
			return out
		}
		code, from, to, err := protocol.DecodeResponse(response)
		if err != nil {
			out.code = codes.TargetResponse
			return out
		}
		out.finalState = to
		out.trace = append(out.trace, TraceStep{From: from, Type: frame.Type, To: to, Code: code})
		if code == codes.OK {
			key := model.TransitionKey{From: from, To: to}
			coverageSet[key.String()] = key
		}
		if code != codes.OK {
			out.code = code
			break
		}
	}
	out.coverage = sortedCoverage(coverageSet)
	return out
}

func materialize(cs model.CaseSet, tc model.TestCase) ([]protocol.Frame, error) {
	frames := make([]protocol.Frame, 0, len(tc.Steps))
	for _, step := range tc.Steps {
		seed, ok := cs.Seed(step.SeedID)
		if !ok {
			return nil, &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("case %q references missing seed %q", tc.ID, step.SeedID), Chain: []string{"case:" + tc.ID, "seed:" + step.SeedID}}
		}
		payload := append([]byte(nil), seed.Payload...)
		for _, opID := range step.OperatorIDs {
			op, ok := cs.Operator(opID)
			if !ok {
				return nil, &codes.Error{Code: codes.DepInvalidated, Message: fmt.Sprintf("case %q references missing operator %q", tc.ID, opID), Chain: []string{"case:" + tc.ID, "operator:" + opID}}
			}
			mutated, err := applyOperator(payload, op)
			if err != nil {
				return nil, err
			}
			payload = mutated
		}
		frames = append(frames, protocol.Frame{Type: seed.Type, Payload: payload})
	}
	return frames, nil
}

func applyOperator(payload []byte, op model.MutationOperator) ([]byte, error) {
	switch op.Kind {
	case "append":
		return append(append([]byte(nil), payload...), []byte(op.Arg)...), nil
	case "prepend":
		return append([]byte(op.Arg), payload...), nil
	case "xor":
		key, err := hex.DecodeString(op.Arg)
		if err != nil || len(key) == 0 {
			return nil, codes.New(codes.BadRequest, "operator %q has invalid xor argument %q", op.ID, op.Arg)
		}
		out := append([]byte(nil), payload...)
		for i := range out {
			out[i] ^= key[i%len(key)]
		}
		return out, nil
	case "truncate":
		n, err := strconv.Atoi(op.Arg)
		if err != nil || n < 0 {
			return nil, codes.New(codes.BadRequest, "operator %q has invalid truncate argument %q", op.ID, op.Arg)
		}
		if n < len(payload) {
			return append([]byte(nil), payload[:n]...), nil
		}
		return payload, nil
	case "duplicate":
		return append(append([]byte(nil), payload...), payload...), nil
	default:
		return nil, codes.New(codes.BadRequest, "operator %q has unknown kind %q", op.ID, op.Kind)
	}
}

func assertionResults(cs model.CaseSet, caseID string, coverage []model.TransitionKey, invalidAssertions map[string]model.Diagnostic) []AssertionResult {
	covered := map[string]bool{}
	for _, key := range coverage {
		covered[key.String()] = true
	}
	var out []AssertionResult
	for _, as := range cs.AssertionsForCase(caseID) {
		r := AssertionResult{ID: as.ID, From: as.From, To: as.To, Code: codes.OK}
		if diag, invalid := invalidAssertions[as.ID]; invalid {
			r.Skipped = true
			r.Code = diag.Code
		} else {
			r.Covered = covered[model.TransitionKey{From: as.From, To: as.To}.String()]
			r.Passed = r.Covered
			if !r.Passed {
				r.Code = codes.AssertionMismatch
			}
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func invalidationMaps(cs model.CaseSet, version int) (map[string]model.Diagnostic, map[string]model.Diagnostic) {
	cases := map[string]model.Diagnostic{}
	assertions := map[string]model.Diagnostic{}
	putCase := func(d model.Diagnostic) {
		old, exists := cases[d.EntityID]
		if !exists || (old.Code != codes.DepCycle && d.Code == codes.DepCycle) {
			cases[d.EntityID] = d
		}
	}
	putAssertion := func(d model.Diagnostic) {
		old, exists := assertions[d.EntityID]
		if !exists || (old.Code != codes.DepCycle && d.Code == codes.DepCycle) {
			assertions[d.EntityID] = d
		}
	}
	for _, d := range cs.Invalidations {
		if d.Entity == model.EntityCase {
			putCase(d)
		}
		if d.Entity == model.EntityAssertion {
			putAssertion(d)
		}
	}
	analysis := graph.Analyze(cs, version, nil)
	for _, d := range analysis.Diagnostics {
		if d.Entity == model.EntityCase {
			putCase(d)
		}
		if d.Entity == model.EntityAssertion {
			putAssertion(d)
		}
	}
	return cases, assertions
}

func sortedCoverage(set map[string]model.TransitionKey) []model.TransitionKey {
	out := make([]model.TransitionKey, 0, len(set))
	for _, key := range set {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

func unionCoverage(runs []BatchRun) []model.TransitionKey {
	set := map[string]model.TransitionKey{}
	for _, run := range runs {
		for _, key := range run.Execution.Coverage {
			set[key.String()] = key
		}
	}
	return sortedCoverage(set)
}

func isWholeReplacement(previous, current []CaseResult) bool {
	if len(previous) == 0 || len(current) == 0 {
		return false
	}
	prevIDs := map[string]bool{}
	for _, c := range previous {
		prevIDs[c.CaseID] = true
	}
	for _, c := range current {
		if prevIDs[c.CaseID] {
			return false
		}
	}
	return true
}

func canonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var compact any
	if err := json.Unmarshal(b, &compact); err != nil {
		return nil, err
	}
	return json.Marshal(compact)
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}
