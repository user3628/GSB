// Package validate implements stage 4: it aggregates the first three stages
// into a canonical verification report. Every collection is key-sorted, so the
// report digest is identical between a fresh run, an incremental rebuild and
// a crash-recovery replay.
package validate

import (
	"sort"

	"ticketpipeline/internal/bayes"
	"ticketpipeline/internal/determ"
	"ticketpipeline/internal/model"
	"ticketpipeline/internal/normalize"
	"ticketpipeline/internal/priority"
)

// TicketRow is the per-ticket detail embedded in the report.
type TicketRow struct {
	TicketID        string `json:"ticket_id"`
	Channel         string `json:"channel"`
	IntentLabel     string `json:"intent_label"`
	TopIntent       string `json:"top_intent"`
	IntentCorrect   bool   `json:"intent_correct"`
	LabeledPriority int    `json:"labeled_priority"`
	FinalPriority   int    `json:"final_priority"`
	Verdict         string `json:"verdict"`
	DowngradeReason string `json:"downgrade_reason,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
}

// StageStats summarizes one stage, including its business-error breakdown.
type StageStats struct {
	OK     int            `json:"ok"`
	Errors map[string]int `json:"errors"`
	Digest string         `json:"digest"`
}

// Report is the full exported verification artifact.
type Report struct {
	PolicyVersion     int                   `json:"policy_version"`
	TotalTickets      int                   `json:"total_tickets"`
	Stages            map[string]StageStats `json:"stages"`
	IntentAccuracy    float64               `json:"intent_accuracy"`
	VerdictCounts     map[string]int        `json:"verdict_counts"`
	PriorityConfusion []int                 `json:"priority_confusion"` // histogram of final priority 0..3
	Rows              []TicketRow           `json:"rows"`
	Digest            string                `json:"digest"`
}

// Inputs bundles everything stage 4 needs. Error maps use ticket id -> error
// and are produced by the engine from checkpoint facts, keeping this package
// free of database concerns.
type Inputs struct {
	Tickets      []model.RawTicket
	Norm         map[string]*normalize.Result
	Intent       map[string]*bayes.Result
	Priority     map[string]*priority.Result
	NormErr      map[string]*model.BizError
	IntentErr    map[string]*model.BizError
	PrioErr      map[string]*model.BizError
	StageDigests map[string]string
}

// Build constructs the canonical report.
func Build(policyVersion int, in Inputs) (*Report, error) {
	report := &Report{
		PolicyVersion: policyVersion,
		TotalTickets:  len(in.Tickets),
		Stages: map[string]StageStats{
			model.StageNormalize: newStats(in.StageDigests[model.StageNormalize]),
			model.StageIntent:    newStats(in.StageDigests[model.StageIntent]),
			model.StagePriority:  newStats(in.StageDigests[model.StagePriority]),
			// validate self-digest is backfilled by the engine after the
			// report bytes are finalized, avoiding a self-referential hash.
			model.StageValidate: newStats(""),
		},
		VerdictCounts:     map[string]int{},
		PriorityConfusion: make([]int, 4),
		Rows:              make([]TicketRow, 0, len(in.Tickets)),
	}

	intentDenom, intentCorrect := 0, 0
	for _, t := range in.Tickets {
		row := TicketRow{
			TicketID: t.TicketID, Channel: t.Channel,
			IntentLabel: t.IntentLabel, LabeledPriority: t.Priority,
		}
		if be, ok := in.NormErr[t.TicketID]; ok {
			row.ErrorCode = be.Code
			report.Stages[model.StageNormalize] = bumpError(report.Stages[model.StageNormalize], be.Code)
		} else {
			report.Stages[model.StageNormalize] = bumpOK(report.Stages[model.StageNormalize])
		}
		if be, ok := in.IntentErr[t.TicketID]; ok {
			row.ErrorCode = coalesceCode(row.ErrorCode, be.Code)
			report.Stages[model.StageIntent] = bumpError(report.Stages[model.StageIntent], be.Code)
		} else if ir, ok := in.Intent[t.TicketID]; ok {
			report.Stages[model.StageIntent] = bumpOK(report.Stages[model.StageIntent])
			row.TopIntent = ir.TopIntent
			row.IntentCorrect = ir.TopIntent == t.IntentLabel
			intentDenom++
			if row.IntentCorrect {
				intentCorrect++
			}
		}
		if be, ok := in.PrioErr[t.TicketID]; ok {
			row.ErrorCode = coalesceCode(row.ErrorCode, be.Code)
			row.Verdict = priority.VerdictExcluded
			report.Stages[model.StagePriority] = bumpError(report.Stages[model.StagePriority], be.Code)
		} else if pr, ok := in.Priority[t.TicketID]; ok {
			report.Stages[model.StagePriority] = bumpOK(report.Stages[model.StagePriority])
			row.FinalPriority = pr.FinalPriority
			row.Verdict = pr.Verdict
			row.DowngradeReason = pr.DowngradeReason
			report.VerdictCounts[pr.Verdict]++
			if pr.FinalPriority >= 0 && pr.FinalPriority <= 3 {
				report.PriorityConfusion[pr.FinalPriority]++
			}
		}
		report.Rows = append(report.Rows, row)
	}
	sort.Slice(report.Rows, func(i, j int) bool { return report.Rows[i].TicketID < report.Rows[j].TicketID })

	if intentDenom > 0 {
		report.IntentAccuracy = float64(intentCorrect) / float64(intentDenom)
	}
	// Stage 4 self-statistic: every ticket reaching this point counts as
	// processed by validation. Must be finalized before the digest.
	report.Stages[model.StageValidate] = StageStats{
		OK: len(in.Tickets), Errors: map[string]int{}, Digest: "",
	}
	digest, err := determ.Fingerprint(report)
	if err != nil {
		return nil, err
	}
	report.Digest = digest
	return report, nil
}

func newStats(digest string) StageStats {
	return StageStats{OK: 0, Errors: map[string]int{}, Digest: digest}
}

func bumpOK(s StageStats) StageStats { s.OK++; return s }

func bumpError(s StageStats, code string) StageStats {
	s.Errors[code]++
	return s
}

func coalesceCode(existing, code string) string {
	if existing == "" {
		return code
	}
	return existing + "|" + code
}
