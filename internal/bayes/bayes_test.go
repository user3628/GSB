package bayes

import (
	"math"
	"testing"

	"ticketpipeline/internal/model"
	"ticketpipeline/internal/normalize"
)

func policy() model.Policy {
	return model.Policy{
		Version: 1, Smoothing: 1, HashDim: 4096, SampleRatio: 1,
		PriorityThresh: []float64{-30, -20, -10},
		IntentWeights:  map[string]float64{"a": 1, "b": 1},
	}
}

func mustNorm(t *testing.T, id, text string) *normalize.Result {
	t.Helper()
	r, err := normalize.Process(model.RawTicket{TicketID: id, Text: text, Channel: "web", IntentLabel: "a"}, 4096)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	return r
}

func TestTrainScoreDeterministic(t *testing.T) {
	tickets := []model.RawTicket{
		{TicketID: "T1", Text: "账单 发票 invoice", Channel: "web", IntentLabel: "a"},
		{TicketID: "T2", Text: "退款 refund 退货", Channel: "web", IntentLabel: "b"},
		{TicketID: "T3", Text: "账单 invoice 核对", Channel: "app", IntentLabel: "a"},
		{TicketID: "T4", Text: "退款 refund 流程", Channel: "im", IntentLabel: "b"},
	}
	norms := map[string]*normalize.Result{}
	for _, tk := range tickets {
		norms[tk.TicketID] = mustNorm(t, tk.TicketID, tk.Text)
	}
	m1, err := Train(tickets, norms, policy())
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	m2, err := Train(tickets, norms, policy())
	if err != nil {
		t.Fatalf("train: %v", err)
	}
	if m1.Digest != m2.Digest {
		t.Fatal("model digest drift across identical training runs")
	}
	res1, be := m1.Score(norms["T1"], policy().IntentWeights)
	if be != nil {
		t.Fatalf("score: code=%s msg=%s ctx=%v", be.Code, be.Message, be.Context)
	}
	res2, _ := m2.Score(norms["T1"], policy().IntentWeights)
	if res1.Digest != res2.Digest {
		t.Fatal("score digest drift across identical runs")
	}
	if res1.TopIntent == "" {
		t.Fatal("empty top intent")
	}
}

func TestUnknownMarkerFallback(t *testing.T) {
	tickets := []model.RawTicket{
		{TicketID: "T1", Text: "账单 发票 invoice", Channel: "web", IntentLabel: "a"},
		{TicketID: "T2", Text: "退款 refund 退货", Channel: "web", IntentLabel: "b"},
	}
	norms := map[string]*normalize.Result{}
	for _, tk := range tickets {
		norms[tk.TicketID] = mustNorm(t, tk.TicketID, tk.Text)
	}
	m, err := Train(tickets, norms, policy())
	if err != nil {
		t.Fatal(err)
	}
	// A ticket whose feature buckets never appeared during training still
	// scores via the explicit unknown marker instead of producing zero logs.
	probe := mustNorm(t, "T9", "物流 logistics 快递 delivery")
	res, be := m.Score(probe, policy().IntentWeights)
	if be != nil {
		t.Fatalf("unexpected biz error: code=%s msg=%s", be.Code, be.Message)
	}
	if res.UnknownHits == 0 {
		t.Fatal("expected unknown marker hits for unseen features")
	}
	for class, score := range res.Scores {
		if math.IsNaN(score) || math.IsInf(score, 0) {
			t.Fatalf("non-finite score for class %s: %v", class, score)
		}
	}
}

func TestNumericGuard(t *testing.T) {
	// Hand-craft a model whose conditional log row contains a zero (the
	// forbidden log-domain underflow sentinel) and confirm scoring returns
	// E_NUMERIC with the ticket id and offending term in the context.
	m := &Model{
		Classes:   []string{"a"},
		LogPrior:  map[string]float64{"a": math.Log(0.5)},
		LogCond:   map[string][]float64{"a": make([]float64, 4097)},
		HashDim:   4096,
		Smoothing: 1,
	}
	nr := mustNorm(t, "T7", "账单 invoice")
	_, be := m.Score(nr, map[string]float64{"a": 1})
	if be == nil || be.Code != model.CodeNumeric || be.Stage != model.StageIntent || be.TicketID != "T7" {
		t.Fatalf("want E_NUMERIC for T7, got %v", be)
	}
	if be.Context["term"] == "" || be.Context["class"] != "a" {
		t.Fatalf("diagnostic context missing term/class: %+v", be.Context)
	}
}

func TestZeroSmoothingConfig(t *testing.T) {
	p := policy()
	p.Smoothing = 0
	if err := ValidateConfig(p); err == nil || err.Code != model.CodeConfig {
		t.Fatalf("want E_CONFIG for zero smoothing, got %v", err)
	}
}

func TestSplitIsDeterministic(t *testing.T) {
	first := InTrainingSet("TKT-0001", 0.7)
	for i := 0; i < 10; i++ {
		if InTrainingSet("TKT-0001", 0.7) != first {
			t.Fatal("train partition drifted across calls")
		}
	}
}
