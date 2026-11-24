package adjudicator

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"keylife/internal/model"
)

type State struct {
	Now      model.MonoTime
	Policies map[string]model.SubjectPolicy
	Keys     map[string]model.Key
}

type Path string

const (
	PathAccurate  Path = "per-key-accrual"
	PathStreaming Path = "batch-streaming"
)

type PathSelection struct {
	Subject     string `json:"subject"`
	PendingKeys int    `json:"pending_keys"`
	Threshold   int    `json:"threshold"`
	Selected    Path   `json:"selected_path"`
	Rule        string `json:"rule"`
}

type BatchResult struct {
	Schema        string                `json:"schema"`
	TimeSample    string                `json:"time_sample"`
	Threshold     int                   `json:"threshold"`
	InputCount    int                   `json:"input_count"`
	PendingKeys   int                   `json:"pending_keys"`
	Paths         []PathSelection       `json:"paths"`
	Decisions     []model.Decision      `json:"decisions"`
	AuditTrail    []model.DecisionAudit `json:"audit_trail"`
	DecisionCodes []model.Code          `json:"decision_codes"`
	Digest        string                `json:"digest"`
}

type PathRun struct {
	Path          Path                  `json:"path"`
	Decisions     []model.Decision      `json:"decisions"`
	AuditTrail    []model.DecisionAudit `json:"audit_trail"`
	DecisionCodes []model.Code          `json:"decision_codes"`
	Digest        string                `json:"digest"`
}

type Comparison struct {
	Schema        string  `json:"schema"`
	Threshold     int     `json:"threshold"`
	InputCount    int     `json:"input_count"`
	Accurate      PathRun `json:"accurate"`
	Streaming     PathRun `json:"streaming"`
	ByteIdentical bool    `json:"byte_identical"`
}

type indexedRequest struct {
	sequence int
	request  model.DecisionRequest
}

func Adjudicate(requests []model.DecisionRequest, state State, threshold int) (BatchResult, error) {
	if threshold < 1 {
		return BatchResult{}, model.NewError(model.CodeConfig, "adjudication threshold must be at least 1")
	}
	sorted := normalizeAndSort(requests)
	groups := groupBySubject(sorted)
	selections := make([]PathSelection, 0, len(groups))
	var decisions []model.Decision
	for _, subject := range sortedSubjects(groups) {
		group := groups[subject]
		pending := distinctKeys(group)
		selection := PathSelection{Subject: subject, PendingKeys: pending, Threshold: threshold}
		var run []model.Decision
		if pending < threshold {
			selection.Selected = PathAccurate
			selection.Rule = "pending_keys < threshold"
			run = runAccurate(group, state)
		} else {
			selection.Selected = PathStreaming
			selection.Rule = "pending_keys >= threshold"
			run = runStreaming(group, state)
		}
		selections = append(selections, selection)
		decisions = append(decisions, run...)
	}
	return buildBatch(threshold, sorted, selections, decisions), nil
}

// ComparePaths executes both implementations over the same sorted input and
// compares the complete canonical decisions and audit trail bytes.
func ComparePaths(requests []model.DecisionRequest, state State, threshold int) (Comparison, error) {
	if threshold < 1 {
		return Comparison{}, model.NewError(model.CodeConfig, "adjudication threshold must be at least 1")
	}
	sorted := normalizeAndSort(requests)
	accurate := runAccurate(sorted, state)
	streaming := runStreaming(sorted, state)
	accurateRun := buildPathRun(PathAccurate, accurate)
	streamingRun := buildPathRun(PathStreaming, streaming)
	accuratePayload, err := model.CanonicalJSON(struct {
		Decisions  []model.Decision      `json:"decisions"`
		AuditTrail []model.DecisionAudit `json:"audit_trail"`
	}{accurate, audits(accurate)})
	if err != nil {
		return Comparison{}, err
	}
	streamingPayload, err := model.CanonicalJSON(struct {
		Decisions  []model.Decision      `json:"decisions"`
		AuditTrail []model.DecisionAudit `json:"audit_trail"`
	}{streaming, audits(streaming)})
	if err != nil {
		return Comparison{}, err
	}
	return Comparison{
		Schema: "keylife.adjudication-comparison/v1", Threshold: threshold, InputCount: len(sorted),
		Accurate: accurateRun, Streaming: streamingRun, ByteIdentical: bytes.Equal(accuratePayload, streamingPayload),
	}, nil
}

func Evaluate(state State, req model.DecisionRequest) model.Decision {
	decision := model.Decision{Request: req, At: state.Now, Checks: make([]model.Check, 0, 5)}
	key, exists := state.Keys[req.KeyID]
	if !exists {
		decision.Code = model.CodeKeyNotFound
		decision.Message = "key not found"
		decision.Checks = append(decision.Checks, model.Check{Name: "key_lookup", Code: model.CodeKeyNotFound, Detail: req.KeyID})
		return decision
	}
	decision.KeyPhase = key.Phase
	metrics := model.Metrics(key, state.Now)
	decision.Metrics = &metrics

	if (req.Subject != "" && req.Subject != key.Subject) || req.Actor != key.Subject {
		decision.Code = model.CodeSubjectScope
		decision.Message = "actor or requested subject is outside the key subject scope"
		decision.Checks = append(decision.Checks, model.Check{Name: "subject_scope", Code: model.CodeSubjectScope, Detail: fmt.Sprintf("key=%s actor=%s request=%s", key.Subject, req.Actor, req.Subject)})
		return decision
	}
	decision.Checks = append(decision.Checks, model.Check{Name: "subject_scope", Code: model.CodeOK})

	policy, hasPolicy := state.Policies[key.Subject]
	if !hasPolicy {
		decision.Code = model.CodePolicyNotFound
		decision.Message = "subject policy not found"
		decision.Checks = append(decision.Checks, model.Check{Name: "policy_lookup", Code: model.CodePolicyNotFound})
		return decision
	}
	decision.PolicyMinBits = policy.MinStrengthBits
	effectiveBits := key.StrengthBits
	if strings.TrimSpace(req.Algorithm) != "" {
		spec, ok := model.LookupAlgorithm(req.Algorithm)
		if !ok {
			decision.Code = model.CodeWeakAlgo
			decision.Message = "requested algorithm is unknown and treated as degraded"
			decision.Checks = append(decision.Checks, model.Check{Name: "algorithm_strength", Code: model.CodeWeakAlgo, Detail: req.Algorithm})
			return decision
		}
		bits, err := model.ResolveStrength(spec, req.StrengthBits)
		if err != nil {
			decision.Code = model.CodeWeakAlgo
			decision.Message = err.Error()
			decision.Checks = append(decision.Checks, model.Check{Name: "algorithm_strength", Code: model.CodeWeakAlgo, Detail: err.Error()})
			return decision
		}
		effectiveBits = bits
	}
	decision.EffectiveBits = effectiveBits
	if effectiveBits < policy.MinStrengthBits || key.StrengthBits < policy.MinStrengthBits {
		decision.Code = model.CodeWeakAlgo
		decision.Message = fmt.Sprintf("effective strength %d is below policy minimum %d", effectiveBits, policy.MinStrengthBits)
		decision.Checks = append(decision.Checks, model.Check{Name: "algorithm_strength", Code: model.CodeWeakAlgo, Detail: fmt.Sprintf("effective=%d key=%d min=%d", effectiveBits, key.StrengthBits, policy.MinStrengthBits)})
		return decision
	}
	decision.Checks = append(decision.Checks, model.Check{Name: "algorithm_strength", Code: model.CodeOK})

	operation := strings.ToLower(req.Operation)
	if operation == "" {
		operation = "use"
	}
	if operationUsesKey(operation) && state.Now.Tick >= key.ExpiresAt.Tick {
		decision.Code = model.CodeKeyExpired
		decision.Message = fmt.Sprintf("key expired at sample tick %d", key.ExpiresAt.Tick)
		decision.Checks = append(decision.Checks, model.Check{Name: "expiration", Code: model.CodeKeyExpired})
		return decision
	}
	decision.Checks = append(decision.Checks, model.Check{Name: "expiration", Code: model.CodeOK})

	expected, validOperation := model.ExpectedPhase(operation)
	if !validOperation || key.Phase != expected {
		decision.Code = model.CodePhaseTransition
		decision.Message = fmt.Sprintf("operation %s requires phase %s, current phase is %s", operation, expected, key.Phase)
		decision.Checks = append(decision.Checks, model.Check{Name: "phase_transition", Code: model.CodePhaseTransition})
		return decision
	}
	decision.Checks = append(decision.Checks, model.Check{Name: "phase_transition", Code: model.CodeOK})
	decision.Code = model.CodeOK
	decision.Message = "authorized"
	return decision
}

func operationUsesKey(operation string) bool {
	switch operation {
	case "use", "activate", "rotate":
		return true
	default:
		return false
	}
}

func normalizeAndSort(requests []model.DecisionRequest) []indexedRequest {
	indexed := make([]indexedRequest, len(requests))
	for i, req := range requests {
		indexed[i] = indexedRequest{sequence: i, request: req}
	}
	sort.SliceStable(indexed, func(i, j int) bool {
		a, b := indexed[i].request, indexed[j].request
		if a.Subject != b.Subject {
			return a.Subject < b.Subject
		}
		if a.KeyID != b.KeyID {
			return a.KeyID < b.KeyID
		}
		if a.Operation != b.Operation {
			return a.Operation < b.Operation
		}
		return a.RequestID < b.RequestID
	})
	for i := range indexed {
		indexed[i].sequence = i + 1
	}
	return indexed
}

func groupBySubject(requests []indexedRequest) map[string][]indexedRequest {
	groups := make(map[string][]indexedRequest)
	for _, req := range requests {
		groups[req.request.Subject] = append(groups[req.request.Subject], req)
	}
	return groups
}

func sortedSubjects(groups map[string][]indexedRequest) []string {
	subjects := make([]string, 0, len(groups))
	for subject := range groups {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	return subjects
}

func distinctKeys(requests []indexedRequest) int {
	seen := make(map[string]struct{})
	for _, req := range requests {
		seen[req.request.KeyID] = struct{}{}
	}
	return len(seen)
}

func runAccurate(requests []indexedRequest, state State) []model.Decision {
	decisions := make([]model.Decision, 0, len(requests))
	for _, indexed := range requests {
		decision := Evaluate(state, indexed.request)
		decision.Sequence = indexed.sequence
		decisions = append(decisions, decision)
	}
	return decisions
}

// runStreaming delivers requests through an in-process channel and consumes
// them one at a time. No network transport is involved.
func runStreaming(requests []indexedRequest, state State) []model.Decision {
	ch := make(chan indexedRequest)
	go func() {
		defer close(ch)
		for _, req := range requests {
			ch <- req
		}
	}()
	decisions := make([]model.Decision, 0, len(requests))
	for indexed := range ch {
		decision := Evaluate(state, indexed.request)
		decision.Sequence = indexed.sequence
		decisions = append(decisions, decision)
	}
	return decisions
}

func audits(decisions []model.Decision) []model.DecisionAudit {
	trail := make([]model.DecisionAudit, 0, len(decisions))
	for _, decision := range decisions {
		trail = append(trail, model.DecisionAudit{
			Sequence: decision.Sequence, RequestID: decision.Request.RequestID,
			Subject: decision.Request.Subject, KeyID: decision.Request.KeyID,
			Actor: decision.Request.Actor, Operation: decision.Request.Operation,
			Code: decision.Code, At: decision.At,
			Checks: decision.Checks,
		})
	}
	return trail
}

func buildPathRun(path Path, decisions []model.Decision) PathRun {
	trail := audits(decisions)
	codes := decisionCodes(decisions)
	payload, _ := model.CanonicalJSON(struct {
		Decisions  []model.Decision      `json:"decisions"`
		AuditTrail []model.DecisionAudit `json:"audit_trail"`
	}{decisions, trail})
	return PathRun{Path: path, Decisions: decisions, AuditTrail: trail, DecisionCodes: codes, Digest: model.SHA256Hex(payload)}
}

func buildBatch(threshold int, requests []indexedRequest, selections []PathSelection, decisions []model.Decision) BatchResult {
	trail := audits(decisions)
	codes := decisionCodes(decisions)
	payload, _ := model.CanonicalJSON(struct {
		Decisions  []model.Decision      `json:"decisions"`
		AuditTrail []model.DecisionAudit `json:"audit_trail"`
	}{decisions, trail})
	pending := 0
	for _, selection := range selections {
		pending += selection.PendingKeys
	}
	return BatchResult{
		Schema: "keylife.adjudication/v1", TimeSample: model.TimeSample, Threshold: threshold,
		InputCount: len(requests), PendingKeys: pending, Paths: selections,
		Decisions: decisions, AuditTrail: trail, DecisionCodes: codes,
		Digest: model.SHA256Hex(payload),
	}
}

func decisionCodes(decisions []model.Decision) []model.Code {
	codes := make([]model.Code, 0, len(decisions))
	for _, decision := range decisions {
		codes = append(codes, decision.Code)
	}
	return codes
}
