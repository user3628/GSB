package replay

import (
	"sort"
	"time"

	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/pki"
)

type Options struct {
	ExtraTimeline []domain.PolicyTimelineEntry
	CommitVersion int64
}

type policyInterval struct {
	domain.Policy
	FromMS int64
}

func Replay(record domain.Record, events []domain.NormalizedEvent, opts Options) (*domain.Conclusion, *domain.Error) {
	if len(events) == 0 {
		return nil, domain.NewError(domain.CodeRecordCorrupt, "no normalized events to replay").
			WithContext("connection_id", record.ConnectionID)
	}

	intervals, err := BuildIntervals(record, events, opts.ExtraTimeline)
	if err != nil {
		return nil, err
	}

	conclusion := &domain.Conclusion{
		ConnectionID:          record.ConnectionID,
		CanonicalConnectionID: record.ConnectionID,
		SNI:                   record.SNI,
		ProtocolVersion:       record.ProtocolVersion,
		State:                 domain.StateEmpty,
		CommitVersion:         opts.CommitVersion,
		Rounding:              []domain.RoundingInfo{},
		PolicyVersionSequence: []string{},
		PolicyVersions:        []string{},
	}
	for _, event := range events {
		conclusion.Rounding = append(conclusion.Rounding, event.Rounding)
	}

	uniqueVersions := map[string]bool{}
	conclusion.PolicyDocuments = map[string]domain.Policy{}
	for _, interval := range intervals {
		conclusion.PolicyDocuments[interval.Version] = interval.Policy
	}
	addVersion := func(version string) {
		conclusion.PolicyVersionSequence = append(conclusion.PolicyVersionSequence, version)
		if version != "" && !uniqueVersions[version] {
			uniqueVersions[version] = true
			conclusion.PolicyVersions = append(conclusion.PolicyVersions, version)
		}
	}

	ordered := append([]domain.NormalizedEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Seq < ordered[j].Seq })

	for _, event := range ordered {
		policy := PolicyAt(intervals, event.TimestampMS)
		event.PolicyVersion = policy.Version
		addVersion(policy.Version)

		judgment := domain.EventJudgment{
			EventSeq:      event.Seq,
			EventType:     event.Type,
			StateBefore:   conclusion.State,
			TimestampMS:   event.TimestampMS,
			PolicyVersion: policy.Version,
		}

		nextState, transitionErr := Transition(conclusion.State, event.Type)
		if transitionErr != nil {
			judgment.Error = transitionErr
			judgment.StateAfter = conclusion.State
			conclusion.EventJudgments = append(conclusion.EventJudgments, judgment)
			conclusion.Conclusion = domain.ConclusionInsufficient
			conclusion.ReasonCode = domain.CodeInvalidTransition
			conclusion.EvidenceChain = append(conclusion.EvidenceChain, domain.Evidence{
				Seq: event.Seq, EventSeq: event.Seq, PolicyVersion: policy.Version,
				Kind: "invalid_transition", Severity: "critical", Code: domain.CodeInvalidTransition,
				Message: transitionErr.Message, ObservedAtMS: event.TimestampMS,
				Details: transitionErr.Context,
			})
			return conclusion, transitionErr
		}
		judgment.StateAfter = nextState
		conclusion.State = nextState

		conclusion.EvidenceChain = append(conclusion.EvidenceChain, domain.Evidence{
			Seq: event.Seq, EventSeq: event.Seq, PolicyVersion: policy.Version,
			Kind: "state_transition", Severity: "info",
			Message:      event.Type + ": " + judgment.StateBefore + " -> " + nextState,
			ObservedAtMS: event.TimestampMS,
			Details:      map[string]any{"event_type": event.Type, "state_before": judgment.StateBefore, "state_after": nextState},
		})

		if event.Type == domain.EventCertificateReceived {
			serial := firstCertSerial(record.CertChainPEM)
			conclusion.EvidenceChain = append(conclusion.EvidenceChain, domain.Evidence{
				Seq: event.Seq + 1000, EventSeq: event.Seq, PolicyVersion: policy.Version,
				Kind: "certificate_received", Severity: "info", Message: "certificate chain received",
				ObservedAtMS: event.TimestampMS, Details: map[string]any{"leaf_serial": serial},
			})
		}

		if event.Type == domain.EventPinVerification {
			chainReport, chainErr := pki.EvaluateChain(record.CertChainPEM, pki.EvaluateOptions{
				SNI: record.SNI, AtMS: event.TimestampMS, Policy: policy,
			})
			judgment.Chain = chainReport
			if chainErr != nil {
				judgment.Error = chainErr
				judgment.Result = domain.ConclusionInsufficient
				conclusion.EvidenceChain = appendChainFindings(conclusion.EvidenceChain, event, policy.Version, chainReport)
				conclusion.EvidenceChain = append(conclusion.EvidenceChain, domain.Evidence{
					Seq: event.Seq + 2000, EventSeq: event.Seq, PolicyVersion: policy.Version,
					Kind: "chain_error", Severity: "critical", Code: chainErr.Code,
					Message: chainErr.Message, ObservedAtMS: event.TimestampMS, Details: chainErr.Context,
				})
			} else {
				judgment.Result = ClassifyChain(chainReport)
				conclusion.EvidenceChain = appendChainFindings(conclusion.EvidenceChain, event, policy.Version, chainReport)
			}
		}

		conclusion.EventJudgments = append(conclusion.EventJudgments, judgment)
	}

	conclusion.Conclusion, conclusion.ReasonCode = Aggregate(conclusion.State, conclusion.EventJudgments)
	for _, judgment := range conclusion.EventJudgments {
		if judgment.Error != nil && judgment.Error.Code == domain.CodeChainInvalid {
			conclusion.ReasonCode = domain.CodeChainInvalid
			break
		}
	}
	conclusion.EvidenceChain = append(conclusion.EvidenceChain, domain.Evidence{
		Seq: 9_999_999, Kind: "final_conclusion", Severity: conclusionSeverity(conclusion.Conclusion),
		Code: conclusion.ReasonCode, Message: "final conclusion: " + conclusion.Conclusion,
		Details: map[string]any{"state": conclusion.State, "reason_code": conclusion.ReasonCode},
	})
	return conclusion, nil
}

func Transition(state, eventType string) (string, *domain.Error) {
	invalid := func() *domain.Error {
		return domain.NewError(domain.CodeInvalidTransition, "illegal event transition").
			WithContext("state", state).WithContext("event_type", eventType)
	}
	switch eventType {
	case domain.EventHandshakeStart:
		if state == domain.StateEmpty {
			return domain.StateStarted, nil
		}
	case domain.EventCertificateReceived:
		if state == domain.StateStarted {
			return domain.StateCertReceived, nil
		}
	case domain.EventPinVerification:
		if state == domain.StateCertReceived || state == domain.StateChecked {
			return domain.StateChecked, nil
		}
	case domain.EventHandshakeComplete:
		if state == domain.StateChecked {
			return domain.StateCompleted, nil
		}
	case domain.EventAbnormalTermination:
		if state == domain.StateStarted || state == domain.StateCertReceived || state == domain.StateChecked {
			return domain.StateAborted, nil
		}
	}
	return state, invalid()
}

func ClassifyChain(report *domain.ChainReport) string {
	if report == nil || report.ErrorCode == domain.CodeChainInvalid {
		return domain.ConclusionInsufficient
	}
	if !report.PolicyMatched && !report.RootTrusted {
		return domain.ConclusionMITM
	}
	if !report.SNIMatched {
		if report.RootTrusted {
			return domain.ConclusionInsufficient
		}
		return domain.ConclusionMITM
	}
	if report.PolicyMatched && report.Valid {
		return domain.ConclusionTrusted
	}
	return domain.ConclusionInsufficient
}

func Aggregate(state string, judgments []domain.EventJudgment) (string, string) {
	hasMITM := false
	hasInsufficient := false
	hasTrustedCheck := false
	checkCount := 0

	for _, judgment := range judgments {
		if judgment.EventType != domain.EventPinVerification {
			continue
		}
		checkCount++
		switch judgment.Result {
		case domain.ConclusionMITM:
			hasMITM = true
		case domain.ConclusionTrusted:
			hasTrustedCheck = true
		default:
			hasInsufficient = true
		}
		if judgment.Error != nil && judgment.Error.Code == domain.CodeChainInvalid {
			hasInsufficient = true
		}
	}

	if hasMITM {
		return domain.ConclusionMITM, "policy_or_identity_indicates_mitm"
	}
	if state == domain.StateAborted {
		return domain.ConclusionInsufficient, "abnormal_termination_without_strong_trust"
	}
	if state != domain.StateCompleted {
		return domain.ConclusionInsufficient, "handshake_incomplete"
	}
	if checkCount == 0 {
		return domain.ConclusionInsufficient, "no_pin_verification_event"
	}
	if hasInsufficient || !hasTrustedCheck {
		return domain.ConclusionInsufficient, "chain_or_policy_evidence_incomplete"
	}
	return domain.ConclusionTrusted, "all_checks_trusted"
}

func BuildIntervals(record domain.Record, events []domain.NormalizedEvent, extra []domain.PolicyTimelineEntry) ([]policyInterval, *domain.Error) {
	entries := append([]domain.PolicyTimelineEntry(nil), record.PolicyTimeline...)
	entries = append(entries, extra...)
	intervals := []policyInterval{}

	initial := record.Policy
	initialFrom := int64(0)
	if record.ObservedStartMS > 0 {
		initialFrom = record.ObservedStartMS
	} else if len(events) > 0 {
		initialFrom = events[0].TimestampMS
	}
	if initial.Version != "" {
		intervals = append(intervals, policyInterval{Policy: initial, FromMS: initialFrom})
	}

	for index, entry := range entries {
		fromMS, err := effectiveAtMS(record, entry, events)
		if err != nil {
			return nil, domain.WrapError(domain.CodeRecordCorrupt, err, "invalid policy effective time").WithContext("policy_version", entry.Version)
		}
		if fromMS == 0 {
			fromMS = initialFrom + int64(index+1)
		}
		intervals = append(intervals, policyInterval{Policy: entry.Policy, FromMS: fromMS})
	}

	sort.SliceStable(intervals, func(i, j int) bool { return intervals[i].FromMS < intervals[j].FromMS })
	if len(intervals) == 0 {
		return nil, domain.NewError(domain.CodeRecordCorrupt, "record has no policy version")
	}
	// Same-millisecond replacements preserve declaration order by stable sort;
	// collapse only exact duplicate version/time pairs is unnecessary.
	return intervals, nil
}

func PolicyAt(intervals []policyInterval, atMS int64) domain.Policy {
	active := intervals[0].Policy
	for _, interval := range intervals {
		if interval.FromMS <= atMS {
			active = interval.Policy
		} else {
			break
		}
	}
	return active
}

func effectiveAtMS(record domain.Record, entry domain.PolicyTimelineEntry, events []domain.NormalizedEvent) (int64, error) {
	if entry.EffectiveAtMS > 0 {
		return entry.EffectiveAtMS, nil
	}
	if entry.EffectiveTime != "" {
		parsed, err := time.Parse(time.RFC3339Nano, entry.EffectiveTime)
		if err != nil {
			return 0, err
		}
		return parsed.UnixMilli(), nil
	}
	if entry.EffectiveOffsetNS != 0 && record.ObservedStartMS > 0 {
		return record.ObservedStartMS + entry.EffectiveOffsetNS/int64(time.Millisecond), nil
	}
	if entry.EffectiveOffsetNS != 0 && len(events) > 0 {
		return events[0].TimestampMS + entry.EffectiveOffsetNS/int64(time.Millisecond), nil
	}
	return 0, nil
}

func appendChainFindings(evidence []domain.Evidence, event domain.NormalizedEvent, version string, report *domain.ChainReport) []domain.Evidence {
	if report == nil {
		return evidence
	}
	for i, finding := range report.Findings {
		finding.EventSeq = event.Seq
		finding.PolicyVersion = version
		finding.ObservedAtMS = event.TimestampMS
		if finding.Seq == 0 {
			finding.Seq = event.Seq*100 + i
		}
		evidence = append(evidence, finding)
	}
	return evidence
}

func firstCertSerial(pemChain string) string {
	certs, err := pki.ParseCertificates(pemChain)
	if err != nil || len(certs) == 0 {
		return ""
	}
	return certs[0].SerialNumber.Text(16)
}

func conclusionSeverity(conclusion string) string {
	switch conclusion {
	case domain.ConclusionMITM:
		return "critical"
	case domain.ConclusionTrusted:
		return "info"
	default:
		return "warning"
	}
}
