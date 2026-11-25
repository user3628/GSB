package priority

import (
	"testing"

	"ticketpipeline/internal/bayes"
	"ticketpipeline/internal/model"
)

func intentResult(id string, score float64) *bayes.Result {
	return &bayes.Result{
		TicketID: id, TopIntent: "billing",
		Scores: map[string]float64{"billing": score, "refund": score - 5},
	}
}

func policy() model.Policy {
	return model.Policy{
		Smoothing: 1, HashDim: 64,
		PriorityThresh: []float64{-30, -20, -10},
	}
}

func agreement(v float64) *float64 { return &v }

func TestVerdictClasses(t *testing.T) {
	cases := []struct {
		name     string
		score    float64
		label    int
		verdict  string
		priority int
	}{
		{"low-score-hit", -35, 0, VerdictHit, 0},
		{"mid-missed", -25, 3, VerdictMissed, 1},
		{"high-false", -5, 0, VerdictFalse, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk := model.RawTicket{TicketID: "T", Priority: tc.label, Agreement: agreement(80)}
			res, be := Decide(tk, intentResult("T", tc.score), policy())
			if be != nil {
				t.Fatalf("unexpected error: %v", be)
			}
			if res.Verdict != tc.verdict || res.FinalPriority != tc.priority {
				t.Fatalf("got verdict=%s priority=%d want %s/%d",
					res.Verdict, res.FinalPriority, tc.verdict, tc.priority)
			}
		})
	}
}

func TestLabelOutOfRange(t *testing.T) {
	tk := model.RawTicket{TicketID: "T5", Priority: 9, Agreement: agreement(80)}
	res, be := Decide(tk, intentResult("T5", -15), policy())
	if be == nil || be.Code != model.CodeLabelRange || be.TicketID != "T5" || be.Stage != model.StagePriority {
		t.Fatalf("want E_LABEL_RANGE for T5, got %v", be)
	}
	if res == nil || res.Verdict != VerdictExcluded || res.DowngradeReason != model.DowngradeLabelRange {
		t.Fatal("out-of-range ticket must be retained with explicit downgrade reason")
	}
}

func TestAgreementMissingDowngrade(t *testing.T) {
	tk := model.RawTicket{TicketID: "T6", Priority: 3, Agreement: nil}
	res, be := Decide(tk, intentResult("T6", -5), policy())
	if be != nil {
		t.Fatalf("missing agreement is a downgrade, not an error: %v", be)
	}
	if res.DowngradeReason != model.DowngradeAgreementMiss || res.FinalPriority > 1 {
		t.Fatalf("missing agreement must cap at 1, got %+v", res)
	}
	if res.AgreementSeen {
		t.Fatal("AgreementSeen must be false")
	}
}

func TestLowAgreementDowngrade(t *testing.T) {
	tk := model.RawTicket{TicketID: "T7", Priority: 3, Agreement: agreement(30)}
	res, _ := Decide(tk, intentResult("T7", -5), policy())
	if res.DowngradeReason != model.DowngradeAgreementLow || res.FinalPriority > 2 {
		t.Fatalf("low agreement must cap at 2, got %+v", res)
	}
}

func TestBadThresholdsConfig(t *testing.T) {
	p := policy()
	p.PriorityThresh = []float64{1, 1, 2}
	if err := ValidateConfig(p); err == nil || err.Code != model.CodeConfig {
		t.Fatalf("want E_CONFIG for non-monotonic thresholds, got %v", err)
	}
}
