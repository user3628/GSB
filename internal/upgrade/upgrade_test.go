package upgrade_test

import (
	"testing"

	"dialogalign/internal/domain"
	"dialogalign/internal/engine"
	"dialogalign/internal/generator"
	"dialogalign/internal/upgrade"
)

func datasetAndResolver(t *testing.T, active string) (domain.Dataset, *upgrade.Resolver) {
	t.Helper()
	root := t.TempDir()
	if err := generator.Generate(root); err != nil {
		t.Fatal(err)
	}
	dataset, err := engine.LoadDataset(root)
	if err != nil {
		t.Fatal(err)
	}
	roleMap, ok := dataset.Maps[active]
	if !ok {
		t.Fatalf("missing map %s", active)
	}
	dataset.ActiveMap = roleMap
	resolver, err := upgrade.NewResolver(roleMap, dataset.Maps, dataset.Catalog, dataset.Config.Normalization)
	if err != nil {
		t.Fatal(err)
	}
	return dataset, resolver
}

func legacyTurn(code int, version, raw string, valueRange domain.ValueRange) domain.TurnInput {
	return domain.TurnInput{
		SessionID: "SX",
		TurnIndex: 2,
		UserText:  "raw " + raw,
		OldSlots: []domain.LegacySlotAnnotation{{
			RoleCode: code, MapVersion: version, RawValue: raw, Range: valueRange, Field: "test_field", CharOffset: 4,
		}},
	}
}

func TestLegacyResolutionAmbiguityPriorityAndDrift(t *testing.T) {
	dataset, resolver := datasetAndResolver(t, "v1")
	emailRange := domain.ValueRange{Kind: domain.KindEnum, Enum: []string{"primary", "secondary", "shared"}}

	ambiguous, err := resolver.UpgradeTurn(legacyTurn(501, "v1", "primary", emailRange))
	if err != nil {
		t.Fatal(err)
	}
	if len(ambiguous.Slots) != 0 || len(ambiguous.Ambiguities) != 1 {
		t.Fatalf("expected one unresolved ambiguity, got slots=%d ambiguities=%d", len(ambiguous.Slots), len(ambiguous.Ambiguities))
	}
	if len(ambiguous.Ambiguities[0].Candidates) != 2 {
		t.Fatalf("ambiguity must retain both candidates: %+v", ambiguous.Ambiguities[0])
	}

	priority, err := resolver.UpgradeTurn(legacyTurn(503, "v1", "primary", emailRange))
	if err != nil {
		t.Fatal(err)
	}
	if len(priority.Slots) != 1 || priority.Slots[0].Role != "account.email" {
		t.Fatalf("priority winner not selected: %+v", priority.Slots)
	}

	numeric, err := resolver.UpgradeTurn(legacyTurn(502, "v1", "42", domain.ValueRange{Kind: domain.KindInteger, Min: domain.Int64Ptr(1), Max: domain.Int64Ptr(9999)}))
	if err != nil {
		t.Fatal(err)
	}
	if len(numeric.Slots) != 1 || numeric.Slots[0].Role != "billing.amount" || numeric.Slots[0].Value.Canonical != "42" {
		t.Fatalf("range compatibility did not select numeric role: %+v", numeric.Slots)
	}

	dateRange := domain.ValueRange{Kind: domain.KindDateRange, DateMin: "2026-01-01", DateMax: "2026-12-31"}
	v1Date, err := resolver.UpgradeTurn(legacyTurn(301, "v1", "2026-04-01..2026-04-05", dateRange))
	if err != nil {
		t.Fatal(err)
	}
	if v1Date.Slots[0].Role != "delivery.date" {
		t.Fatalf("v1 role = %s", v1Date.Slots[0].Role)
	}

	v2Map := dataset.Maps["v2"]
	v2Resolver, err := upgrade.NewResolver(v2Map, dataset.Maps, dataset.Catalog, dataset.Config.Normalization)
	if err != nil {
		t.Fatal(err)
	}
	v2Date, err := v2Resolver.UpgradeTurn(legacyTurn(301, "v1", "2026-04-01..2026-04-05", dateRange))
	if err != nil {
		t.Fatal(err)
	}
	if v2Date.Slots[0].Role != "subscription.start_date" || v2Date.Slots[0].DeclaredRole != "delivery.date" {
		t.Fatalf("expected explicit role upgrade, got %+v", v2Date.Slots[0])
	}
	if v2Date.Slots[0].Source.MapVersion != "v1" {
		t.Fatalf("declared map version must be retained, got %s", v2Date.Slots[0].Source.MapVersion)
	}
	if len(v2Date.Diagnostics) != 1 || v2Date.Diagnostics[0].Code != "LEGACY_ROLE_UPGRADED" {
		t.Fatalf("missing upgrade diagnostic: %+v", v2Date.Diagnostics)
	}
}

func TestLegacyResolutionErrorsCarryContext(t *testing.T) {
	_, resolver := datasetAndResolver(t, "v1")
	integerRange := domain.ValueRange{Kind: domain.KindInteger, Min: domain.Int64Ptr(1), Max: domain.Int64Ptr(9999)}
	enumRange := domain.ValueRange{Kind: domain.KindEnum, Enum: []string{"damaged", "wrong", "late"}}
	cases := []struct {
		name     string
		turn     domain.TurnInput
		wantCode string
	}{
		{"map version", legacyTurn(101, "v0", "42", integerRange), domain.CodeMapVersionConflict},
		{"unknown code", legacyTurn(99999, "v1", "42", integerRange), domain.CodeUnknownRoleCode},
		{"normalize", legacyTurn(101, "v1", "not-a-number", integerRange), domain.CodeValueNormalize},
		{"range incompatible", legacyTurn(401, "v1", "surprise", enumRange), domain.CodeRoleRangeIncompatible},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolver.UpgradeTurn(tc.turn)
			if err == nil {
				t.Fatal("expected error")
			}
			be := domain.AsBusinessError(err)
			if be.Code != tc.wantCode {
				t.Fatalf("code = %s, want %s (%v)", be.Code, tc.wantCode, err)
			}
			if be.SessionID != "SX" || be.TurnIndex == nil || *be.TurnIndex != 2 || be.Field != "test_field" || be.Stage != "upgrade" {
				t.Fatalf("missing business context: %+v", be)
			}
		})
	}
}

func TestNormalizeValues(t *testing.T) {
	cfg := domain.NormalizationConfig{
		Version:          "n",
		IntegerPrecision: 0,
		EnumCaseFold:     true,
		EnumAliases:      map[string]string{"上海": "shanghai"},
		DateLayouts:      []string{"2006-01-02", "2006/01/02", "2006年1月2日"},
	}
	integer, err := upgrade.NormalizeValue(" 1,048 ", domain.KindInteger, cfg)
	if err != nil || integer.Canonical != "1048" {
		t.Fatalf("integer normalize: %+v %v", integer, err)
	}
	enum, err := upgrade.NormalizeValue("上海", domain.KindEnum, cfg)
	if err != nil || enum.Canonical != "shanghai" {
		t.Fatalf("enum normalize: %+v %v", enum, err)
	}
	dates, err := upgrade.NormalizeValue("2026年3月1日到2026年3月5日", domain.KindDateRange, cfg)
	if err != nil || dates.Canonical != "2026-03-01/2026-03-05" {
		t.Fatalf("date normalize: %+v %v", dates, err)
	}
	if _, err := upgrade.NormalizeValue("2026-03-05..2026-03-01", domain.KindDateRange, cfg); err == nil {
		t.Fatal("expected reversed date range to fail")
	}
}
