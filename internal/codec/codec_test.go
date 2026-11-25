package codec_test

import (
	"testing"

	"dialogalign/internal/codec"
	"dialogalign/internal/domain"
	"dialogalign/internal/engine"
	"dialogalign/internal/generator"
	"dialogalign/internal/upgrade"
)

func loadCodecFixture(t *testing.T) (domain.Dataset, *upgrade.Resolver) {
	t.Helper()
	root := t.TempDir()
	if err := generator.Generate(root); err != nil {
		t.Fatal(err)
	}
	dataset, err := engine.LoadDataset(root)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := upgrade.NewResolver(dataset.ActiveMap, dataset.Maps, dataset.Catalog, dataset.Config.Normalization)
	if err != nil {
		t.Fatal(err)
	}
	return dataset, resolver
}

func stateFixture(dataset domain.Dataset) domain.SessionState {
	ranges := map[string]domain.ValueRange{}
	for _, role := range dataset.Catalog.Roles {
		ranges[role.Name] = role.Range
	}
	code := func(v int) *int { return &v }
	return domain.SessionState{
		SessionID:  "SX",
		MapVersion: "v1",
		Turns: []domain.TurnState{
			{
				SessionID: "SX", TurnIndex: 1, Intent: "order_status", MapVersion: "v1",
				Confirmed: []domain.ConfirmedSlot{
					{Role: "order.order_id", Range: ranges["order.order_id"], Value: domain.SlotValue{Kind: domain.KindInteger, Canonical: "12345", Raw: "12345"}, Source: domain.ValueSource{Kind: "legacy", Field: "order_id", CharOffset: 5, MapVersion: "v1", LegacyRoleCode: code(101), DeclaredRole: "order.order_id"}, SourceTurn: 1},
					{Role: "delivery.city", Range: ranges["delivery.city"], Value: domain.SlotValue{Kind: domain.KindEnum, Canonical: "shanghai", Raw: "上海"}, Source: domain.ValueSource{Kind: "new", Field: "city", CharOffset: 10}, SourceTurn: 1},
					{Role: "account.email", Range: ranges["account.email"], Value: domain.SlotValue{Kind: domain.KindEnum, Canonical: "primary", Raw: "primary"}, Source: domain.ValueSource{Kind: "legacy", Field: "email", CharOffset: 20, MapVersion: "v1", LegacyRoleCode: code(503)}, SourceTurn: 1},
					{Role: "profile.email", Range: ranges["profile.email"], Value: domain.SlotValue{Kind: domain.KindEnum, Canonical: "primary", Raw: "primary"}, Source: domain.ValueSource{Kind: "legacy", Field: "profile", CharOffset: 30, MapVersion: "v1", LegacyRoleCode: code(701)}, SourceTurn: 1},
				},
				Pending: []domain.PendingSlot{{
					Role: "delivery.date", Range: ranges["delivery.date"], Field: "delivery_date", Reason: "rule",
					Source: domain.ValueSource{Kind: "rule"}, RequestedTurn: 1,
				}},
			},
			{
				SessionID: "SX", TurnIndex: 2, Intent: "delivery_change", MapVersion: "v1",
				Confirmed: []domain.ConfirmedSlot{
					{Role: "order.channel", Range: ranges["order.channel"], Value: domain.SlotValue{Kind: domain.KindEnum, Canonical: "app", Raw: "app"}, Source: domain.ValueSource{Kind: "new", Field: "channel", CharOffset: 0}, SourceTurn: 2},
				},
			},
		},
	}
}

func TestNewCodecPreservesIntentSlotsRangesAndTurnOwnership(t *testing.T) {
	dataset, _ := loadCodecFixture(t)
	state := stateFixture(dataset)
	doc, err := codec.EncodeNewSession(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.DecodeNewSession(doc)
	if err != nil {
		t.Fatal(err)
	}
	assertStateEquivalent(t, state, decoded)
}

func TestLegacyCodecUsesUnambiguousCodes(t *testing.T) {
	dataset, resolver := loadCodecFixture(t)
	state := stateFixture(dataset)
	doc, err := codec.EncodeLegacySession(state, dataset.Maps["v1"])
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.DecodeLegacySession(doc, resolver)
	if err != nil {
		t.Fatal(err)
	}
	assertStateEquivalent(t, state, decoded)
	for _, turn := range doc.Turns {
		for _, slot := range turn.Confirmed {
			if slot.RoleCode == 0 || slot.MapVersion != "v1" || slot.RawValue == "" {
				t.Fatalf("legacy wire slot is incomplete: %+v", slot)
			}
		}
	}
}

func assertStateEquivalent(t *testing.T, want, got domain.SessionState) {
	t.Helper()
	if want.SessionID != got.SessionID || len(want.Turns) != len(got.Turns) {
		t.Fatalf("session/turn mismatch: want %s/%d got %s/%d", want.SessionID, len(want.Turns), got.SessionID, len(got.Turns))
	}
	for i := range want.Turns {
		w, g := want.Turns[i], got.Turns[i]
		if w.TurnIndex != g.TurnIndex || w.Intent != g.Intent {
			t.Fatalf("turn %d intent mismatch: %s != %s", w.TurnIndex, w.Intent, g.Intent)
		}
		if len(w.Confirmed) != len(g.Confirmed) {
			t.Fatalf("turn %d confirmed count mismatch: %d != %d", w.TurnIndex, len(w.Confirmed), len(g.Confirmed))
		}
		for j := range w.Confirmed {
			wc, gc := w.Confirmed[j], g.Confirmed[j]
			if wc.Role != gc.Role || wc.Value.Canonical != gc.Value.Canonical || wc.SourceTurn != gc.SourceTurn {
				t.Fatalf("turn %d slot mismatch: %+v != %+v", w.TurnIndex, wc, gc)
			}
			wRange, _ := domain.CanonicalJSON(wc.Range)
			gRange, _ := domain.CanonicalJSON(gc.Range)
			if string(wRange) != string(gRange) {
				t.Fatalf("turn %d role %s range boundary changed: %s != %s", w.TurnIndex, wc.Role, wRange, gRange)
			}
		}
		if len(w.Pending) != len(g.Pending) {
			t.Fatalf("turn %d pending count mismatch: %d != %d", w.TurnIndex, len(w.Pending), len(g.Pending))
		}
		for j := range w.Pending {
			if w.Pending[j].Role != g.Pending[j].Role {
				t.Fatalf("turn %d pending role mismatch: %s != %s", w.TurnIndex, w.Pending[j].Role, g.Pending[j].Role)
			}
		}
	}
}
