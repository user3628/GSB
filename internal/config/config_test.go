package config_test

import (
	"bytes"
	"fmt"
	"testing"

	"secverify/internal/config"
	"secverify/internal/deps"
	"secverify/internal/model"
	"secverify/internal/sample"
)

func TestParseBaselineNormalizesUnitsAndInheritance(t *testing.T) {
	baseline, bizErr := config.ParseBaseline(sample.BaselineJSON())
	if bizErr != nil {
		t.Fatalf("ParseBaseline: %v", bizErr)
	}
	graph := deps.Build(baseline.Items)
	if err := graph.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	resolved, bizErr := config.ResolveAccounts(baseline.Items)
	if bizErr != nil {
		t.Fatalf("ResolveAccounts: %v", bizErr)
	}
	byID := model.ItemMap(resolved)
	if got := byID["acct.base.min_length"].Account.NormalizedValue; got != 12 {
		t.Fatalf("base normalized value = %v, want 12", got)
	}
	if got := byID["acct.child.min_length"].Account.NormalizedValue; got != 12 {
		t.Fatalf("inherited normalized value = %v, want 12", got)
	}
	if got := byID["acct.base.min_length"].Account.NormalizedUnit; got != "count" {
		t.Fatalf("normalized unit = %q, want count", got)
	}
	if got := *byID["audit.logon"].Audit.RetentionValue; got != 30*86400 {
		t.Fatalf("audit retention seconds = %v, want %d", got, 30*86400)
	}
}

func TestParseChangeSetLengthPrefixAndOffsets(t *testing.T) {
	changeset, bizErr := config.ParseChangeSet(sample.RoundOne().Changeset)
	if bizErr != nil {
		t.Fatalf("ParseChangeSet: %v", bizErr)
	}
	if len(changeset.Operations) != 1 {
		t.Fatalf("operations = %d, want 1", len(changeset.Operations))
	}
	op := changeset.Operations[0]
	if op.Op != "modify" || op.ID != "acct.base.min_length" {
		t.Fatalf("unexpected operation: %+v", op)
	}
	if op.PayloadOffset <= len(config.ChangeHeader) {
		t.Fatalf("payload offset = %d, want after header and BLOCK line", op.PayloadOffset)
	}
	if op.Item.Account == nil || *op.Item.Account.Value != 14 {
		t.Fatalf("normalized modify payload missing value 14")
	}
}

func TestInvalidFieldDomainHasStableCodeItemAndOffset(t *testing.T) {
	raw := []byte(`{"namespace":"n","items":[{"id":"acct.bad","kind":"account_policy","account":{"name":"minimum_password_length","value":4,"unit":"characters","min":8,"max":64,"objective":"at_least"}}]}`)
	_, bizErr := config.ParseBaseline(raw)
	if bizErr == nil {
		t.Fatal("expected parse error")
	}
	if bizErr.Code != model.CodeConfigParse || bizErr.ItemID != "acct.bad" || bizErr.SourceOffset <= 0 {
		t.Fatalf("unexpected error: %+v", bizErr)
	}
}

func TestDuplicateItemIDRejected(t *testing.T) {
	item := `{"id":"acct.same","kind":"account_policy","account":{"name":"n","value":12,"unit":"characters","objective":"at_least"}}`
	raw := []byte(fmt.Sprintf(`{"namespace":"n","items":[%s,%s]}`, item, item))
	_, bizErr := config.ParseBaseline(raw)
	if bizErr == nil || bizErr.Code != model.CodeConfigParse || bizErr.ItemID != "acct.same" {
		t.Fatalf("unexpected duplicate result: %+v", bizErr)
	}
}

func TestTruncatedLengthPrefixedBlockRejected(t *testing.T) {
	raw := append([]byte(config.ChangeHeader), []byte("BLOCK 50\n{\"op\":\"delete\"}\n")...)
	_, bizErr := config.ParseChangeSet(raw)
	if bizErr == nil || bizErr.Code != model.CodeConfigParse || bizErr.SourceOffset <= 0 {
		t.Fatalf("unexpected truncated result: %+v", bizErr)
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	raw := []byte(`{"namespace":"n","items":[{"id":"acct.bad","kind":"account_policy","unknown":true,"account":{"name":"n","value":12,"unit":"characters","objective":"at_least"}}]}`)
	_, bizErr := config.ParseBaseline(raw)
	if bizErr == nil || bizErr.Code != model.CodeConfigParse || bizErr.ItemID != "acct.bad" {
		t.Fatalf("unexpected unknown field result: %+v", bizErr)
	}
}

func TestCandidateBuildDoesNotMutateBase(t *testing.T) {
	baseline, bizErr := config.ParseBaseline(sample.BaselineJSON())
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	before := make([]string, 0, len(baseline.Items))
	for _, item := range baseline.Items {
		before = append(before, model.ItemHash(item))
	}
	missingItem := model.CloneItem(baseline.Items[0])
	missingItem.ID = "missing.item"
	badFrame := config.EncodeFrames([][]byte{config.MustPayload(struct {
		Op   string     `json:"op"`
		ID   string     `json:"id"`
		Item model.Item `json:"item"`
	}{Op: "modify", ID: missingItem.ID, Item: missingItem})})
	changeset, bizErr := config.ParseChangeSet(badFrame)
	if bizErr != nil {
		t.Fatalf("frame should parse structurally: %v", bizErr)
	}
	_, bizErr = config.BuildCandidate(baseline.Items, changeset.Operations)
	if bizErr == nil || bizErr.Code != model.CodeConfigParse {
		t.Fatalf("unexpected candidate error: %+v", bizErr)
	}
	for i, item := range baseline.Items {
		if model.ItemHash(item) != before[i] {
			t.Fatalf("base item %s was mutated", item.ID)
		}
	}
}

func TestTimeUnitAccountConversion(t *testing.T) {
	raw := []byte(`{"namespace":"n","items":[{"id":"acct.lockout","kind":"account_policy","account":{"name":"lockout_duration","value":2,"unit":"hours","min":1,"max":4,"objective":"at_least"}}]}`)
	baseline, bizErr := config.ParseBaseline(raw)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	resolved, bizErr := config.ResolveAccounts(baseline.Items)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if resolved[0].Account.NormalizedValue != 7200 || resolved[0].Account.NormalizedUnit != "seconds" {
		t.Fatalf("normalized = %v %s", resolved[0].Account.NormalizedValue, resolved[0].Account.NormalizedUnit)
	}
}

func TestEncodeFramesIsDeterministic(t *testing.T) {
	a := sample.RoundOne().Changeset
	b := sample.RoundOne().Changeset
	if !bytes.Equal(a, b) {
		t.Fatal("sample generation is not deterministic")
	}
}
