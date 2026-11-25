package app

import (
	"math"

	"csrouter/internal/domain"
	"csrouter/internal/engine"
	"csrouter/internal/reference"
)

type RunOptions struct {
	ClearStop        bool   `json:"clear_stop"`
	StopAfterTickets int    `json:"stop_after_tickets,omitempty"`
	ThroughStage     string `json:"through_stage,omitempty"`
}

type RunResult struct {
	Code             string                  `json:"code"`
	Message          string                  `json:"message"`
	Tier             string                  `json:"tier"`
	CompletedTickets int                     `json:"completed_tickets"`
	TotalTickets     int                     `json:"total_tickets"`
	Strategy         domain.StrategyDecision `json:"strategy"`
	Report           *domain.Report          `json:"report,omitempty"`
}

func (a *App) RunTier(tier string, options RunOptions) (*RunResult, error) {
	a.runMu.Lock()
	defer a.runMu.Unlock()

	if options.ClearStop {
		if err := a.DB.ClearStop(); err != nil {
			return nil, err
		}
	}
	policy, err := a.loadPolicy(tier)
	if err != nil {
		return nil, err
	}
	rows, err := a.DB.ListTickets(tier)
	if err != nil {
		return nil, err
	}

	examples := make([]engine.TrainingExample, 0, len(rows))
	norms := make([]domain.NormalizeArtifact, len(rows))
	for i, row := range rows {
		norm, _ := nlpNormalize(row.Ticket, policy.EffectiveVersion())
		norms[i] = norm
		examples = append(examples, engine.TrainingExample{Ticket: row.Ticket, Normalize: norm})
	}

	model := engine.TrainModel(policy, examples)
	metrics := engine.EstimateStrategyMetrics(model, norms)
	scale := policy.Scale
	scale.ActualTickets = len(rows)
	scale.IntentCount = len(model.Intents)
	scale.ObservedTotalTerms = int(metrics.ExactOps / int64(maxInt(1, scale.IntentCount)))
	uniqueTerms := map[string]struct{}{}
	for _, norm := range norms {
		for _, token := range norm.Tokens {
			uniqueTerms[token.Text] = struct{}{}
		}
	}
	scale.ObservedUniqueTerms = len(uniqueTerms)
	decision := engine.PlanStrategy(scale, metrics)

	intents := make([]domain.IntentArtifact, 0, len(rows))
	priorities := make([]domain.PriorityArtifact, 0, len(rows))
	details := make([]domain.VerificationDetail, 0, len(rows))
	completed := 0
	throughIdx := len(domain.Stages) - 1
	if options.ThroughStage != "" {
		throughIdx = -1
		for i, stage := range domain.Stages {
			if stage == options.ThroughStage {
				throughIdx = i
			}
		}
		if throughIdx < 0 {
			return nil, domain.NewBizError(domain.ErrIllegalStage, "", "", policy.EffectiveVersion(),
				"unknown target stage %q", options.ThroughStage)
		}
	}

	for i, row := range rows {
		stopRequested, err := a.DB.StopRequested()
		if err != nil {
			return nil, err
		}
		if stopRequested {
			return &RunResult{
				Code: domain.CodeStopRequested, Message: "graceful stop before next indivisible ticket",
				Tier: tier, CompletedTickets: completed, TotalTickets: len(rows), Strategy: decision,
			}, nil
		}

		norm := norms[i]
		if bizErr := a.publishStage(row, domain.StageNormalize, policy, norm, statusForError(norm.ErrorCode), norm.ErrorCode); bizErr != nil {
			return nil, bizErr
		}
		if throughIdx == 0 {
			completed++
			if options.StopAfterTickets > 0 && completed == options.StopAfterTickets {
				_ = a.DB.RequestStop()
			}
			continue
		}

		intent, scoreErr := model.ScoreTicket(row.Ticket, norm, decision.SelectedStrategy)
		if scoreErr != nil {
			scoreErr.VersionFingerprint = policy.Fingerprint()
			_ = a.publishStage(row, domain.StageIntent, policy, intent, "failed", scoreErr.Code)
			return nil, scoreErr
		}
		if bizErr := a.publishStage(row, domain.StageIntent, policy, intent, "completed", ""); bizErr != nil {
			return nil, bizErr
		}
		intents = append(intents, intent)
		if throughIdx == 1 {
			completed++
			if options.StopAfterTickets > 0 && completed == options.StopAfterTickets {
				_ = a.DB.RequestStop()
			}
			continue
		}

		priority := engine.DecidePriority(row.Ticket, policy, intent)
		priorityStatus := "completed"
		if priority.ErrorCode != "" {
			priorityStatus = "error"
		} else if len(priority.DegradationSources) > 0 {
			priorityStatus = "degraded"
		}
		if bizErr := a.publishStage(row, domain.StagePriority, policy, priority, priorityStatus, priority.ErrorCode); bizErr != nil {
			return nil, bizErr
		}
		priorities = append(priorities, priority)
		if throughIdx == 2 {
			completed++
			if options.StopAfterTickets > 0 && completed == options.StopAfterTickets {
				_ = a.DB.RequestStop()
			}
			continue
		}

		shard := row.ShardPath
		detail := engine.VerifyTicket(row.Ticket, tier, shard, intent, priority)
		detail.NormalizationErrorCode = norm.ErrorCode
		detail.TopScore = intent.TopScore
		detail.TopPosterior = intent.TopPosterior
		detail.CombinedScore = priority.CombinedScore
		verifyStatus := "completed"
		if detail.ErrorCode != "" {
			verifyStatus = "error"
		}
		if bizErr := a.publishStage(row, domain.StageVerify, policy, detail, verifyStatus, detail.ErrorCode); bizErr != nil {
			return nil, bizErr
		}
		details = append(details, detail)

		completed++
		if options.StopAfterTickets > 0 && completed == options.StopAfterTickets {
			if err := a.DB.RequestStop(); err != nil {
				return nil, err
			}
		}
	}

	if throughIdx < len(domain.Stages)-1 {
		return &RunResult{
			Code: domain.CodeStageCompleted, Message: "requested stage completed for all available tickets",
			Tier: tier, CompletedTickets: completed, TotalTickets: len(rows), Strategy: decision,
		}, nil
	}

	refResult, bizErr := reference.Build(tier, policy, examples, model)
	if bizErr != nil {
		return nil, bizErr
	}
	report := a.buildReport(tier, policy, decision, details, refResult,
		reference.Compare(policy, intents, priorities, refResult))
	if err := writeJSONFile(a.ReferencePath(tier), refResult); err != nil {
		return nil, err
	}
	if err := a.persistReport(report); err != nil {
		return nil, err
	}
	if err := a.DB.ClearStop(); err != nil {
		return nil, err
	}
	return &RunResult{
		Code: domain.CodeRunCompleted, Message: "all tickets processed", Tier: tier,
		CompletedTickets: completed, TotalTickets: len(rows), Strategy: decision, Report: &report,
	}, nil
}

func (a *App) publishStage(row storeTicketRow, stage string, policy domain.Policy, artifact any, status, errorCode string) *domain.BizError {
	existing, err := a.DB.GetArtifact(row.Ticket.TicketID, stage)
	if err != nil {
		return domain.NewBizError("E_STORE", row.Ticket.TicketID, "", policy.EffectiveVersion(), "%v", err)
	}
	if existing != nil {
		return nil
	}
	_, bizErr := a.DB.PublishArtifact(storePublishInput(row, stage, policy, artifact, status, errorCode))
	return bizErr
}

func statusForError(code string) string {
	if code == "" {
		return "completed"
	}
	return "completed_with_error"
}

func (a *App) buildReport(
	tier string,
	policy domain.Policy,
	decision domain.StrategyDecision,
	details []domain.VerificationDetail,
	ref *reference.Result,
	comparison domain.ReferenceComparison,
) domain.Report {
	totals := map[string]int{
		"tickets":             len(details),
		"processed":           len(details),
		"empty_text":          0,
		"degraded":            0,
		"invalid_label":       0,
		"intent_label_hits":   0,
		"priority_comparable": 0,
	}
	comparisonCounts := map[string]int{}
	intentHits := 0
	priorityHits := 0
	priorityComparable := 0
	for _, detail := range details {
		comparisonCounts[detail.Comparison]++
		if detail.AnnotatedIntent != "" && detail.IntentHit {
			intentHits++
			totals["intent_label_hits"]++
		}
		if detail.NormalizationErrorCode == domain.ErrEmptyText {
			totals["empty_text"]++
		}
		if len(detail.DegradationSources) > 0 {
			totals["degraded"]++
		}
		if detail.ErrorCode == domain.ErrInvalidLabel || detail.Comparison == "INVALID_LABEL" {
			totals["invalid_label"]++
		}
		if detail.Comparison == "HIT" || detail.Comparison == "MISS" || detail.Comparison == "FALSE_POSITIVE" {
			priorityComparable++
			totals["priority_comparable"]++
			if detail.Comparison == "HIT" {
				priorityHits++
			}
		}
	}

	var intentAccuracy, priorityAccuracy float64
	if len(details) > 0 {
		intentAccuracy = round6(float64(intentHits) / float64(len(details)))
	}
	if priorityComparable > 0 {
		priorityAccuracy = round6(float64(priorityHits) / float64(priorityComparable))
	}

	manifestFP := ""
	if manifest, err := a.LoadManifest(); err == nil {
		manifestFP = domain.FingerprintHex("manifest/v1", manifest)
	}

	report := domain.Report{
		SchemaVersion:       "validation-report/v1",
		Product:             "csrouter",
		Tier:                tier,
		PolicyVersion:       policy.EffectiveVersion(),
		PolicyFingerprint:   policy.Fingerprint(),
		ManifestFingerprint: manifestFP,
		Strategy:            decision,
		Totals:              totals,
		ComparisonCounts:    comparisonCounts,
		IntentLabelAccuracy: intentAccuracy,
		PriorityAccuracy:    priorityAccuracy,
		DegradedCount:       totals["degraded"],
		InvalidCount:        totals["invalid_label"],
		Details:             details,
		Reference:           comparison,
	}
	report.ReportFingerprint = domain.FingerprintHex("validation-report/v1", report)
	return report
}

func (a *App) persistReport(report domain.Report) error {
	if err := writeJSONFile(a.ReportPath(report.Tier), report); err != nil {
		return err
	}
	if err := a.DB.SaveReport(report); err != nil {
		return err
	}
	return nil
}

func (a *App) ReportPath(tier string) string {
	return a.ReportDir + "/report-" + tier + ".json"
}

func (a *App) ReferencePath(tier string) string {
	return a.ReportDir + "/reference-" + tier + ".json"
}

func round6(v float64) float64 { return math.Round(v*1e6) / 1e6 }

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
