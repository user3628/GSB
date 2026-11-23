package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"meshmigrate/internal/codec"
	"meshmigrate/internal/compat"
	"meshmigrate/internal/model"
	"meshmigrate/internal/normalize"
	"meshmigrate/internal/store"
)

type Options struct {
	EnableTestHooks bool
	MarkerPath      string
	PauseDuration   time.Duration
	Now             func() time.Time
}

type Engine struct {
	store   *store.Store
	matrix  *compat.Matrix
	opt     Options
	writeMu sync.Mutex
}

func New(st *store.Store, matrix *compat.Matrix, opt Options) *Engine {
	if opt.Now == nil {
		opt.Now = func() time.Time { return time.Now().UTC() }
	}
	if opt.PauseDuration <= 0 {
		opt.PauseDuration = 60 * time.Second
	}
	return &Engine{store: st, matrix: matrix, opt: opt}
}

func (e *Engine) Matrix() *compat.Matrix { return e.matrix }

func (e *Engine) nowMs() int64 { return e.opt.Now().UnixMilli() }

func newID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

func requestHash(body []byte, hint, truncation string) string {
	h := sha256.New()
	h.Write([]byte("meshpolicy-import-v1\x00"))
	h.Write([]byte(strings.ToLower(hint)))
	h.Write([]byte{0})
	h.Write([]byte(truncation))
	h.Write([]byte{0})
	h.Write(body)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

type Analysis struct {
	Parsed     *model.ParsedSnapshot
	Normalized *model.Snapshot
	Decision   compat.Decision
	Truncation string
}

type NegotiationView struct {
	Decision          string                `json:"decision"`
	Format            string                `json:"format"`
	FormatVersion     int                   `json:"formatVersion"`
	MatrixVersion     string                `json:"matrixVersion"`
	Reason            string                `json:"reason"`
	ErrorCode         string                `json:"errorCode,omitempty"`
	UnknownFields     []string              `json:"unknownFields,omitempty"`
	RequiredFeatures  []string              `json:"requiredFeatures,omitempty"`
	SnapshotVersion   uint64                `json:"snapshotVersion"`
	SnapshotID        string                `json:"snapshotId"`
	PolicyCount       int                   `json:"policyCount"`
	SourceDigest      string                `json:"sourceDigest"`
	ContentDigest     string                `json:"contentDigest,omitempty"`
	TruncationRule    string                `json:"truncationRule"`
	DefaultsApplied   []model.DefaultDetail `json:"defaultsApplied,omitempty"`
	RoundingRecords   []RoundingView        `json:"roundingRecords,omitempty"`
	CumulativeErrorUs int64                 `json:"cumulativeRoundingErrorUs"`
}

type RoundingView struct {
	PolicyID             string `json:"policyId"`
	SourceTimestampUs    int64  `json:"sourceTimestampUs"`
	ConvertedTimestampMs int64  `json:"convertedTimestampMs"`
	RemainderUs          int64  `json:"remainderUs"`
	RoundingBit          int    `json:"roundingBit"`
	ErrorUs              int64  `json:"errorUs"`
	CumulativeErrorUs    int64  `json:"cumulativeErrorUs"`
}

func (e *Engine) Analyze(body []byte, hint, truncation string) (*Analysis, *NegotiationView, *APIError) {
	if truncation == "" {
		truncation = model.TruncationFloor
	}
	parsed, decErr := codec.Decode(body, hint)
	if decErr != nil {
		ae := badRequest(decErr.Code, decErr.Message)
		off := decErr.Offset
		ae.ByteOffset = &off
		ae.Line = decErr.Line
		ae.UnknownFields = decErr.UnknownFields
		return nil, nil, ae
	}
	decision := e.matrix.Decide(parsed)
	view := &NegotiationView{
		Decision:         decision.Decision,
		Format:           decision.Format,
		FormatVersion:    decision.FormatVersion,
		MatrixVersion:    decision.MatrixVersion,
		Reason:           decision.Reason,
		ErrorCode:        decision.ErrorCode,
		UnknownFields:    decision.UnknownFields,
		RequiredFeatures: decision.RequiredFeatures,
		SnapshotVersion:  parsed.SnapshotVersion,
		SnapshotID:       parsed.SnapshotID,
		PolicyCount:      parsed.PolicyCount,
		SourceDigest:     parsed.SourceDigest,
		TruncationRule:   truncation,
	}
	if decision.Decision == compat.DecisionReject {
		return &Analysis{Parsed: parsed, Decision: decision, Truncation: truncation}, view, nil
	}
	normalized, normErr := normalize.Normalize(parsed, e.matrix.DefaultsValue(), truncation)
	if normErr != nil {
		ae := newError(normalizeStatus(normErr.Code), normErr.Code, normErr.Message)
		off := normErr.Offset
		ae.ByteOffset = &off
		ae.Line = normErr.Line
		return nil, view, ae
	}
	view.ContentDigest = normalized.ContentDigest
	view.DefaultsApplied = normalized.DefaultDetails
	view.CumulativeErrorUs = normalized.CumulativeRoundingError
	for _, p := range normalized.Policies {
		view.RoundingRecords = append(view.RoundingRecords, RoundingView{
			PolicyID:             p.PolicyID,
			SourceTimestampUs:    p.Rounding.SourceTimestampUs,
			ConvertedTimestampMs: p.Rounding.ConvertedTimestampMs,
			RemainderUs:          p.Rounding.RemainderUs,
			RoundingBit:          p.Rounding.RoundingBit,
			ErrorUs:              p.Rounding.ErrorUs,
			CumulativeErrorUs:    p.Rounding.CumulativeErrorUs,
		})
	}
	return &Analysis{Parsed: parsed, Normalized: normalized, Decision: decision, Truncation: truncation}, view, nil
}

func normalizeStatus(code string) int {
	switch code {
	case normalize.CodeOverflow:
		return 422
	case normalize.CodeInvalidTrunc:
		return 400
	default:
		return 422
	}
}

func (e *Engine) Negotiate(body []byte, hint, truncation string) ([]byte, *APIError) {
	_, view, ae := e.Analyze(body, hint, truncation)
	if ae != nil {
		return ae.Envelope(), ae
	}
	b, err := json.Marshal(view)
	if err != nil {
		return newError(500, CodeInternal, "encode negotiation result").Envelope(), newError(500, CodeInternal, err.Error())
	}
	return b, nil
}

type ImportOptions struct {
	RequestID        string
	Body             []byte
	FormatHint       string
	Truncation       string
	PauseAfterIntent bool
	PauseMidRebuild  bool
}

type ImportResponse struct {
	Status   int
	Body     []byte
	Code     string
	ReportID string
}

func (e *Engine) Import(ctx context.Context, opt ImportOptions) ImportResponse {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	opt.RequestID = strings.TrimSpace(opt.RequestID)
	if opt.RequestID == "" || len(opt.RequestID) > 128 {
		ae := badRequest(CodeInvalidRequest, "X-Request-ID is required and must be at most 128 characters")
		return ImportResponse{Status: ae.Status, Body: ae.Envelope(), Code: ae.Code}
	}
	if opt.Truncation == "" {
		opt.Truncation = model.TruncationFloor
	}
	hash := requestHash(opt.Body, opt.FormatHint, opt.Truncation)
	if existing, err := e.store.GetRequest(ctx, opt.RequestID); err == nil {
		if existing.RequestHash != hash {
			ae := newError(409, CodeConflictingRequest, "request id was already used with different content")
			return ImportResponse{Status: ae.Status, Body: ae.Envelope(), Code: ae.Code, ReportID: existing.ReportID}
		}
		if existing.Status == "in_progress" {
			ae := newError(503, CodeImportAborted, "request is still in progress or was left pending by a previous process")
			return ImportResponse{Status: ae.Status, Body: ae.Envelope(), Code: ae.Code}
		}
		return ImportResponse{Status: existing.HTTPStatus, Body: []byte(existing.ResponseJSON), Code: codeFromResponse([]byte(existing.ResponseJSON)), ReportID: existing.ReportID}
	} else if err != store.ErrNotFound {
		ae := newError(500, CodeInternal, "read request registry: "+err.Error())
		return ImportResponse{Status: ae.Status, Body: ae.Envelope(), Code: ae.Code}
	}

	analysis, view, ae := e.Analyze(opt.Body, opt.FormatHint, opt.Truncation)
	if ae != nil {
		out := e.persistFailedAttempt(ctx, opt.RequestID, hash, opt, 0, ae, view, "rejected")
		return out
	}
	if analysis.Decision.Decision == compat.DecisionReject {
		msg := analysis.Decision.Reason
		if len(analysis.Decision.UnknownFields) > 0 {
			msg += "; unknown required fields: " + strings.Join(analysis.Decision.UnknownFields, ",")
		}
		ae = unsupported(msg, analysis.Decision.UnknownFields)
		ae.Details = map[string]any{"decision": analysis.Decision}
		snapshotVersion := uint64(0)
		if analysis.Parsed != nil {
			snapshotVersion = analysis.Parsed.SnapshotVersion
		}
		out := e.persistFailedAttempt(ctx, opt.RequestID, hash, opt, snapshotVersion, ae, view, "rejected")
		return out
	}
	if analysis.Normalized.SnapshotVersion > math.MaxInt64 {
		ae = badRequest(CodeInvalidPolicy, "snapshot version overflows sqlite integer")
		return e.persistFailedAttempt(ctx, opt.RequestID, hash, opt, 0, ae, view, "rejected")
	}

	now := e.nowMs()
	intentID := newID("intent")
	facts := map[string]any{
		"requestHash":    hash,
		"format":         analysis.Parsed.SourceFormat,
		"formatVersion":  analysis.Parsed.FormatVersion,
		"sourceDigest":   analysis.Parsed.SourceDigest,
		"contentDigest":  analysis.Normalized.ContentDigest,
		"policyCount":    len(analysis.Normalized.Policies),
		"truncationRule": opt.Truncation,
		"decision":       analysis.Decision.Decision,
		"matrixVersion":  e.matrix.MatrixVersion,
	}
	if err := e.store.CreateIntent(ctx, store.IntentRecord{
		IntentID: intentID, RequestID: opt.RequestID, Kind: "import", SnapshotVersion: analysis.Normalized.SnapshotVersion,
		Status: "pending", CreatedAtMs: now, Facts: facts,
	}); err != nil {
		return internalResponse("write intent: " + err.Error())
	}
	if err := e.store.CreateRequestPending(ctx, store.RequestRecord{RequestID: opt.RequestID, RequestHash: hash, CreatedAtMs: now}); err != nil {
		_ = e.store.MarkIntentAborted(ctx, intentID, map[string]any{"failure": "request_registry_insert", "error": err.Error()}, e.nowMs())
		return internalResponse("write request registry: " + err.Error())
	}

	if opt.PauseAfterIntent && e.opt.EnableTestHooks {
		e.pauseForKill("after_intent")
	}

	activeData, activeVersion, err := e.store.GetActiveSnapshot(ctx)
	if err != nil && err != store.ErrNotFound {
		return e.failPending(ctx, opt, hash, analysis, intentID, 500, CodeInternal, "read active snapshot: "+err.Error(), nil)
	}
	if activeData == nil {
		activeData = &store.SnapshotData{}
	}
	if activeVersion > 0 && analysis.Normalized.SnapshotVersion <= activeVersion {
		items := rejectAllItems(analysis.Normalized.Policies, []string{
			fmt.Sprintf("incoming snapshot version %d is not greater than active version %d", analysis.Normalized.SnapshotVersion, activeVersion),
			"snapshot versions must be strictly monotonic",
		})
		ae = newError(409, CodeVersionNotMonotonic, fmt.Sprintf("snapshot version %d must be greater than active version %d", analysis.Normalized.SnapshotVersion, activeVersion))
		ae.Details = map[string]any{"activeVersion": activeVersion}
		return e.persistBatchRejected(ctx, opt, hash, analysis, intentID, items, ae)
	}

	effective, items, affected, rejectReason := resolveConflicts(analysis.Normalized, activeData.Policies)
	if rejectReason != "" {
		ae = newError(409, CodeBatchRejected, rejectReason)
		return e.persistBatchRejected(ctx, opt, hash, analysis, intentID, items, ae)
	}
	if err := effective.Validate(); err != nil {
		items = rejectAllItems(effective.Policies, []string{"conflict-resolved snapshot is invalid: " + err.Error()})
		ae = newError(409, CodeBatchRejected, "conflict-resolved snapshot is invalid: "+err.Error())
		return e.persistBatchRejected(ctx, opt, hash, analysis, intentID, items, ae)
	}
	if err := effective.SetContentDigest(); err != nil {
		return e.failPending(ctx, opt, hash, analysis, intentID, 500, CodeInternal, "recompute content digest: "+err.Error(), nil)
	}
	reportID := newID("report")
	report := ImportReport{
		ReportID:         reportID,
		RequestID:        opt.RequestID,
		SnapshotVersion:  effective.SnapshotVersion,
		Status:           "committed",
		Decision:         analysis.Decision.Decision,
		Format:           effective.SourceFormat,
		FormatVersion:    effective.FormatVersion,
		SourceDigest:     effective.SourceDigest,
		ContentDigest:    effective.ContentDigest,
		CreatedAtMs:      e.nowMs(),
		DefaultsApplied:  effective.DefaultDetails,
		AffectedServices: affected,
		Items:            items,
		ActiveVersion:    effective.SnapshotVersion,
	}
	reportJSON, _ := json.Marshal(report)
	response := ImportSuccessResponse{
		RequestID:        opt.RequestID,
		ReportID:         reportID,
		Status:           "committed",
		SnapshotVersion:  effective.SnapshotVersion,
		ActiveVersion:    effective.SnapshotVersion,
		Decision:         analysis.Decision.Decision,
		Format:           effective.SourceFormat,
		FormatVersion:    effective.FormatVersion,
		SourceDigest:     effective.SourceDigest,
		ContentDigest:    effective.ContentDigest,
		AffectedServices: affected,
		DefaultsApplied:  effective.DefaultDetails,
		Items:            items,
	}
	responseJSON, _ := json.Marshal(response)
	commit := store.ImportCommit{
		Snapshot: effective, AffectedServices: affected,
		ReportID: reportID, ReportJSON: reportJSON, ReportItems: reportItemsToStore(items),
		RequestID: opt.RequestID, ResponseJSON: responseJSON, HTTPStatus: 201,
		IntentID: intentID, Decision: analysis.Decision.Decision, NowMs: e.nowMs(),
	}
	if opt.PauseMidRebuild && e.opt.EnableTestHooks {
		commit.MidCommitHook = e.midCommitHook
	}
	if err := e.store.CommitImport(ctx, commit); err != nil {
		return e.failPending(ctx, opt, hash, analysis, intentID, 500, CodeInternal, "commit import: "+err.Error(), nil)
	}
	return ImportResponse{Status: 201, Body: responseJSON, Code: "", ReportID: reportID}
}

func (e *Engine) pauseForKill(stage string) {
	if e.opt.MarkerPath != "" {
		_ = os.WriteFile(e.opt.MarkerPath, []byte(stage+"\n"), 0o644)
	}
	time.Sleep(e.opt.PauseDuration)
}

// midCommitHook is compiled into the server but is invoked only behind
// MESHPOLICY_ENABLE_TEST_HOOKS. The marker is intentionally not part of the
// recovery report; the report reads only committed SQLite facts.
func (e *Engine) midCommitHook(ctx context.Context, tx *sql.Tx) error {
	if e.opt.MarkerPath != "" {
		_ = os.WriteFile(e.opt.MarkerPath, []byte("mid_rebuild\n"), 0o644)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(e.opt.PauseDuration):
		// If an external kill test does not terminate the process, continue so
		// focused in-process transaction-visibility tests can finish normally.
		return nil
	}
}

func (e *Engine) failPending(ctx context.Context, opt ImportOptions, hash string, analysis *Analysis, intentID string, status int, code, message string, details any) ImportResponse {
	ae := newError(status, code, message)
	ae.Details = details
	facts := map[string]any{"failure": code, "message": message}
	if analysis != nil && analysis.Normalized != nil {
		facts["snapshotVersion"] = analysis.Normalized.SnapshotVersion
	}
	now := e.nowMs()
	body := ae.Envelope()
	if err := e.store.PersistTerminalOutcome(ctx, store.TerminalOutcome{
		RequestID: opt.RequestID, RequestHash: hash, HTTPStatus: status, Response: body,
		IntentID: intentID, IntentState: "aborted", Facts: facts, NowMs: now,
	}); err != nil {
		return internalResponse("record failed import: " + err.Error())
	}
	return ImportResponse{Status: status, Body: body, Code: code}
}

func (e *Engine) persistFailedAttempt(ctx context.Context, requestID, hash string, opt ImportOptions, snapshotVersion uint64, ae *APIError, view *NegotiationView, intentState string) ImportResponse {
	now := e.nowMs()
	intentID := newID("intent")
	facts := map[string]any{"failure": ae.Code, "message": ae.Message, "requestHash": hash}
	if view != nil {
		facts["format"] = view.Format
		facts["formatVersion"] = view.FormatVersion
		facts["sourceDigest"] = view.SourceDigest
		facts["decision"] = view.Decision
	}
	if err := e.store.CreateIntent(ctx, store.IntentRecord{
		IntentID: intentID, RequestID: requestID, Kind: "import_preflight", SnapshotVersion: snapshotVersion,
		Status: "pending", CreatedAtMs: now, Facts: facts,
	}); err != nil {
		return internalResponse("write intent: " + err.Error())
	}
	if err := e.store.CreateRequestPending(ctx, store.RequestRecord{RequestID: requestID, RequestHash: hash, CreatedAtMs: now}); err != nil {
		_ = e.store.MarkIntentAborted(ctx, intentID, map[string]any{"failure": "request_registry_insert", "error": err.Error()}, e.nowMs())
		return internalResponse("write request registry: " + err.Error())
	}
	if opt.PauseAfterIntent && e.opt.EnableTestHooks {
		e.pauseForKill("after_intent")
	}
	body := ae.Envelope()
	if err := e.store.PersistTerminalOutcome(ctx, store.TerminalOutcome{
		RequestID: requestID, RequestHash: hash, HTTPStatus: ae.Status, Response: body,
		IntentID: intentID, IntentState: intentState, Facts: facts, NowMs: e.nowMs(),
	}); err != nil {
		return internalResponse("record rejected attempt: " + err.Error())
	}
	return ImportResponse{Status: ae.Status, Body: body, Code: ae.Code}
}

func (e *Engine) persistBatchRejected(ctx context.Context, opt ImportOptions, hash string, analysis *Analysis, intentID string, items []ImportItem, ae *APIError) ImportResponse {
	reportID := newID("report")
	ae.ReportID = reportID
	ae.Details = map[string]any{"reason": ae.Message, "items": items}
	report := ImportReport{
		ReportID: reportID, RequestID: opt.RequestID, SnapshotVersion: analysis.Normalized.SnapshotVersion,
		Status: "rejected", Decision: analysis.Decision.Decision, Format: analysis.Normalized.SourceFormat,
		FormatVersion: analysis.Normalized.FormatVersion, SourceDigest: analysis.Normalized.SourceDigest,
		ContentDigest: analysis.Normalized.ContentDigest, CreatedAtMs: e.nowMs(),
		DefaultsApplied: analysis.Normalized.DefaultDetails, Items: items,
	}
	// The batch report itself is the only durable fact; no snapshot, policy or
	// derived row is written on this path.
	if av, err := e.store.ActiveVersion(ctx); err == nil {
		report.ActiveVersion = av
	}
	reportJSON, _ := json.Marshal(report)
	body := ae.Envelope()
	now := e.nowMs()
	if err := e.store.PersistTerminalOutcome(ctx, store.TerminalOutcome{
		ReportID: reportID, Report: &store.ReportRecord{
			ReportID: reportID, RequestID: opt.RequestID, SnapshotVersion: analysis.Normalized.SnapshotVersion,
			Status: "rejected", Decision: analysis.Decision.Decision, SourceFormat: analysis.Normalized.SourceFormat,
			FormatVersion: analysis.Normalized.FormatVersion, SourceDigest: analysis.Normalized.SourceDigest,
			ContentDigest: analysis.Normalized.ContentDigest, CreatedAtMs: now, ReportJSON: reportJSON,
			Items: reportItemsToStore(items),
		},
		RequestID: opt.RequestID, RequestHash: hash, HTTPStatus: ae.Status, Response: body,
		IntentID: intentID, IntentState: "rejected",
		Facts: map[string]any{"failure": ae.Code, "reportId": reportID, "snapshotVersion": analysis.Normalized.SnapshotVersion},
		NowMs: now,
	}); err != nil {
		return internalResponse("record rejected batch: " + err.Error())
	}
	return ImportResponse{Status: ae.Status, Body: body, Code: ae.Code, ReportID: reportID}
}

func internalResponse(message string) ImportResponse {
	ae := newError(500, CodeInternal, message)
	return ImportResponse{Status: ae.Status, Body: ae.Envelope(), Code: ae.Code}
}

func codeFromResponse(body []byte) string {
	var v struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &v)
	return v.Error.Code
}

func rejectAllItems(policies []model.Policy, basis []string) []ImportItem {
	out := make([]ImportItem, 0, len(policies))
	for _, p := range policies {
		out = append(out, ImportItem{PolicyID: p.PolicyID, Outcome: "reject", Basis: append([]string(nil), basis...),
			Detail: map[string]any{"incomingRevision": p.PolicyRevision, "lastWriteMs": p.LastWriteMs}})
	}
	return out
}

func resolveConflicts(incoming *model.Snapshot, existing []model.Policy) (*model.Snapshot, []ImportItem, []string, string) {
	oldByID := make(map[string]model.Policy, len(existing))
	for _, p := range existing {
		oldByID[p.PolicyID] = p
	}
	seen := make(map[string]int, len(incoming.Policies))
	items := make([]ImportItem, 0, len(incoming.Policies))
	winner := make(map[string]model.Policy, len(incoming.Policies))
	reject := ""
	for _, p := range incoming.Policies {
		seen[p.PolicyID]++
		item := ImportItem{PolicyID: p.PolicyID, Detail: map[string]any{
			"incomingRevision": p.PolicyRevision,
			"lastWriteMs":      p.LastWriteMs,
		}}
		if seen[p.PolicyID] > 1 {
			item.Outcome = "reject"
			item.Basis = []string{"duplicate policy id within import batch", "batch must be atomic"}
			items = append(items, item)
			reject = "duplicate policy id within import batch: " + p.PolicyID
			continue
		}
		old, ok := oldByID[p.PolicyID]
		if !ok {
			item.Outcome = "overwrite"
			item.Basis = []string{"no existing policy with this id", "incoming policy selected"}
			winner[p.PolicyID] = p
			items = append(items, item)
			continue
		}
		item.Detail["existingRevision"] = old.PolicyRevision
		item.Detail["existingLastWriteMs"] = old.LastWriteMs
		inDigest, oldDigest := model.ConflictDigest(p), model.ConflictDigest(old)
		item.Detail["incomingSemanticDigest"] = inDigest
		item.Detail["existingSemanticDigest"] = oldDigest
		switch {
		case p.PolicyRevision > old.PolicyRevision:
			item.Outcome = "overwrite"
			item.Basis = []string{fmt.Sprintf("higher policy revision wins: incoming %d > existing %d", p.PolicyRevision, old.PolicyRevision)}
			winner[p.PolicyID] = p
		case p.PolicyRevision < old.PolicyRevision:
			item.Outcome = "skip"
			item.Basis = []string{fmt.Sprintf("higher existing policy revision wins: existing %d > incoming %d", old.PolicyRevision, p.PolicyRevision)}
			winner[p.PolicyID] = old
		case p.LastWriteMs > old.LastWriteMs:
			item.Outcome = "overwrite"
			item.Basis = []string{fmt.Sprintf("equal revision; later lastWriteMs wins: incoming %d > existing %d", p.LastWriteMs, old.LastWriteMs)}
			winner[p.PolicyID] = p
		case p.LastWriteMs < old.LastWriteMs:
			item.Outcome = "skip"
			item.Basis = []string{fmt.Sprintf("equal revision; later existing lastWriteMs wins: existing %d > incoming %d", old.LastWriteMs, p.LastWriteMs)}
			winner[p.PolicyID] = old
		case inDigest == oldDigest:
			item.Outcome = "skip"
			item.Basis = []string{"equal revision and lastWriteMs; policy semantics are identical"}
			winner[p.PolicyID] = old
		case inDigest < oldDigest:
			item.Outcome = "overwrite"
			item.Basis = []string{"equal revision and lastWriteMs; stable dictionary order of semantic digests selects incoming"}
			winner[p.PolicyID] = p
		default:
			item.Outcome = "skip"
			item.Basis = []string{"equal revision and lastWriteMs; stable dictionary order of semantic digests selects existing"}
			winner[p.PolicyID] = old
		}
		items = append(items, item)
	}
	final := make([]model.Policy, 0, len(winner))
	for _, p := range winner {
		final = append(final, p)
	}
	sort.Slice(final, func(i, j int) bool { return final[i].PolicyID < final[j].PolicyID })
	effective := *incoming
	effective.Policies = final
	if effective.SetContentDigest() != nil {
		return &effective, items, nil, "failed to compute effective content digest"
	}
	affected := affectedServices(existing, final)
	return &effective, items, affected, reject
}

func affectedServices(old, current []model.Policy) []string {
	oldSvc := make(map[string]map[string]string)
	newSvc := make(map[string]map[string]string)
	for _, p := range old {
		if oldSvc[p.VirtualServiceID] == nil {
			oldSvc[p.VirtualServiceID] = map[string]string{}
		}
		oldSvc[p.VirtualServiceID][p.PolicyID] = model.ConflictDigest(p)
	}
	for _, p := range current {
		if newSvc[p.VirtualServiceID] == nil {
			newSvc[p.VirtualServiceID] = map[string]string{}
		}
		newSvc[p.VirtualServiceID][p.PolicyID] = model.ConflictDigest(p)
	}
	set := make(map[string]struct{})
	for svc := range oldSvc {
		set[svc] = struct{}{}
	}
	for svc := range newSvc {
		set[svc] = struct{}{}
	}
	out := make([]string, 0)
	for svc := range set {
		if !sameDigestMap(oldSvc[svc], newSvc[svc]) {
			out = append(out, svc)
		}
	}
	sort.Strings(out)
	return out
}

func sameDigestMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func reportItemsToStore(items []ImportItem) []store.ReportItem {
	out := make([]store.ReportItem, 0, len(items))
	for _, it := range items {
		out = append(out, store.ReportItem{PolicyID: it.PolicyID, Outcome: it.Outcome, Basis: it.Basis, Detail: it.Detail})
	}
	return out
}
