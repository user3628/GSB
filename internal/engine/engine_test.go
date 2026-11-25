package engine_test

import (
	"encoding/json"
	"errors"
	"testing"

	"layoutread/internal/demo"
	"layoutread/internal/engine"
	"layoutread/internal/model"
)

func testStrategy() model.Strategy {
	strategy := model.DefaultStrategy()
	strategy.Version = 1
	strategy.MergeGapPX = 10
	strategy.MarginXPX = 50
	strategy.MarginYPX = 80
	strategy.ColumnHistogramBinPX = 10
	strategy.ColumnGapPX = 20
	strategy.SpanWidthRatio = 0.9
	return strategy
}

func TestNormalizeDeterministicMergeAndDiagnostics(t *testing.T) {
	doc := model.Document{
		DocID: "doc-normalize",
		Page:  model.Page{Width: 1000, Height: 1000},
		Boxes: []model.TextBox{
			{Index: 0, X: 0.10, Y: 0.20, W: 0.20, H: 0.04, FontTier: 2, Text: "alpha", Lang: "en", ColumnHint: 0},
			{Index: 1, X: 0.30, Y: 0.20, W: 0.20, H: 0.04, FontTier: 2, Text: "beta", Lang: "en", ColumnHint: 0},
			{Index: 2, X: 0.10, Y: 0.40, W: 0.20, H: 0.04, FontTier: 2, Text: "gamma", Lang: "en", ColumnHint: 0},
			{Index: 3, X: 0.10, Y: 0.01, W: 0.80, H: 0.02, FontTier: 1, Text: "header", Lang: "en", ColumnHint: 0},
			{Index: 4, X: 0.10, Y: 0.97, W: 0.80, H: 0.02, FontTier: 1, Text: "footer", Lang: "en", ColumnHint: 0},
		},
	}
	strategy := testStrategy()
	first, err := engine.Normalize(doc, strategy)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	second, err := engine.Normalize(doc, strategy)
	if err != nil {
		t.Fatalf("normalize second: %v", err)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("normalization is not byte deterministic")
	}
	bodyCount := 0
	mergedSeen := false
	kinds := map[string]bool{}
	for _, box := range first.Boxes {
		kinds[box.Kind] = true
		if box.Kind != model.KindBody {
			continue
		}
		bodyCount++
		if len(box.SourceIndices) == 2 && box.SourceIndices[0] == 0 && box.SourceIndices[1] == 1 {
			mergedSeen = true
			if box.X != 100 || box.Y != 200 || box.W != 400 || box.H != 40 {
				t.Fatalf("unexpected merged geometry: %+v", box)
			}
		}
	}
	if bodyCount != 2 || !mergedSeen {
		t.Fatalf("expected 2 body blocks including one merge, got %d merged=%v", bodyCount, mergedSeen)
	}
	if !kinds[model.KindHeader] || !kinds[model.KindFooter] {
		t.Fatalf("header/footer classification missing: %+v", kinds)
	}
}

func TestNormalizeInvalidBoxRetainsDiagnostic(t *testing.T) {
	doc := model.Document{
		DocID: "doc-invalid",
		Page:  model.Page{Width: 1000, Height: 1000},
		Boxes: []model.TextBox{
			{Index: 7, X: 0.1, Y: 0.1, W: 0, H: 0.1, FontTier: 2, Text: "bad", Lang: "en", ColumnHint: 0},
		},
	}
	artifact, err := engine.Normalize(doc, testStrategy())
	if err == nil {
		t.Fatal("expected invalid box error")
	}
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeBoxInvalid {
		t.Fatalf("unexpected error: %v", err)
	}
	if modelErr.DocID != "doc-invalid" || modelErr.BoxIndex == nil || *modelErr.BoxIndex != 7 {
		t.Fatalf("missing doc/box identity: %+v", modelErr)
	}
	if artifact == nil || len(artifact.Diagnostics) == 0 {
		t.Fatal("invalid box diagnostic was not retained")
	}
}

func TestColumnInferenceAndUnassigned(t *testing.T) {
	bundle := demo.Generate()
	for _, doc := range bundle.Documents {
		normalized, err := engine.Normalize(doc, bundle.Strategy)
		if err != nil {
			t.Fatalf("%s normalize: %v", doc.DocID, err)
		}
		columns, err := engine.InferColumns(normalized, bundle.Strategy)
		if err != nil {
			t.Fatalf("%s columns: %v", doc.DocID, err)
		}
		for i, column := range columns.Columns {
			if column.X1 <= column.X0 {
				t.Fatalf("%s empty column %+v", doc.DocID, column)
			}
			if i > 0 && columns.Columns[i-1].X1 != column.X0 {
				t.Fatalf("%s columns do not cover without gaps", doc.DocID)
			}
		}
		if columns.Columns[0].X0 != columns.ContentLeft || columns.Columns[len(columns.Columns)-1].X1 != columns.ContentRight {
			t.Fatalf("%s columns do not cover effective width", doc.DocID)
		}
		fullSpan := 0
		for _, box := range columns.Boxes {
			if box.FullSpan {
				fullSpan++
			}
		}
		switch doc.DocID {
		case "doc-double-en", "doc-dual-band-mixed":
			if len(columns.Columns) != 2 {
				t.Fatalf("%s expected 2 columns, got %d", doc.DocID, len(columns.Columns))
			}
		case "doc-span-title-zh-en":
			if len(columns.Columns) != 2 || fullSpan < 3 {
				t.Fatalf("%s expected 2 columns and header/title/footer spans, got %d/%d", doc.DocID, len(columns.Columns), fullSpan)
			}
		}
	}

	unassignedDoc := model.Document{
		DocID: "doc-cross",
		Page:  model.Page{Width: 1000, Height: 1000},
		Boxes: []model.TextBox{
			{Index: 0, X: 0.05, Y: 0.20, W: 0.10, H: 0.04, FontTier: 2, Text: "left", Lang: "en", ColumnHint: 0},
			{Index: 1, X: 0.55, Y: 0.20, W: 0.10, H: 0.04, FontTier: 2, Text: "right", Lang: "en", ColumnHint: 1},
			{Index: 2, X: 0.10, Y: 0.30, W: 0.50, H: 0.04, FontTier: 2, Text: "crossing", Lang: "en", ColumnHint: 0},
		},
	}
	normalized, err := engine.Normalize(unassignedDoc, testStrategy())
	if err != nil {
		t.Fatalf("normalize unassigned fixture: %v", err)
	}
	_, err = engine.InferColumns(normalized, testStrategy())
	if err == nil {
		t.Fatal("expected column unassigned error")
	}
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeColumnUnassigned {
		t.Fatalf("unexpected column error: %v", err)
	}
	if modelErr.DocID != "doc-cross" || modelErr.BoxIndex == nil {
		t.Fatalf("column error lacks doc/box identity: %+v", modelErr)
	}
	if len(modelErr.Diagnostics) == 0 {
		t.Fatal("column error diagnostic missing")
	}
}

func TestNoContentColumnIsUnassigned(t *testing.T) {
	doc := model.Document{
		DocID: "doc-only-margins",
		Page:  model.Page{Width: 1000, Height: 1000},
		Boxes: []model.TextBox{
			{Index: 0, X: 0.1, Y: 0.01, W: 0.8, H: 0.02, FontTier: 1, Text: "h", Lang: "en", ColumnHint: 0},
			{Index: 1, X: 0.1, Y: 0.97, W: 0.8, H: 0.02, FontTier: 1, Text: "f", Lang: "en", ColumnHint: 0},
		},
	}
	normalized, err := engine.Normalize(doc, testStrategy())
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	_, err = engine.InferColumns(normalized, testStrategy())
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeColumnUnassigned {
		t.Fatalf("expected E_COLUMN_UNASSIGNED, got %v", err)
	}
}

func TestReadingOrderIsUniqueAndRuleCompliant(t *testing.T) {
	bundle := demo.Generate()
	for _, doc := range bundle.Documents {
		_, result, err := engine.AnalyzeFull(doc, bundle.Strategy)
		if err != nil {
			t.Fatalf("%s: %v", doc.DocID, err)
		}
		sources := map[int]bool{}
		lastColumn := -2
		seenNonHeader := false
		for i, block := range result.Blocks {
			if block.Order != i {
				t.Fatalf("%s order index mismatch at %d", doc.DocID, i)
			}
			for _, source := range block.SourceIndices {
				if sources[source] {
					t.Fatalf("%s duplicate source box %d", doc.DocID, source)
				}
				sources[source] = true
			}
			if block.Kind == model.KindHeader {
				if seenNonHeader {
					t.Fatalf("%s header ordering violation", doc.DocID)
				}
			} else {
				seenNonHeader = true
			}
			if block.Kind == model.KindBody && block.FullSpan {
				lastColumn = -2
			} else if block.Kind == model.KindBody && block.Column >= 0 {
				if block.Column < lastColumn {
					t.Fatalf("%s column order regressed from %d to %d", doc.DocID, lastColumn, block.Column)
				}
				lastColumn = block.Column
			}
		}
		if len(result.Blocks) > 0 && result.Blocks[len(result.Blocks)-1].Kind != model.KindFooter {
			t.Fatalf("%s footer is not last", doc.DocID)
		}
		for _, confidence := range result.Confidences {
			if confidence.Confidence <= 0 {
				t.Fatalf("%s non-positive confidence: %+v", doc.DocID, confidence)
			}
		}
	}
}

func TestFullSpanBlockSeparatesBands(t *testing.T) {
	bundle := demo.Generate()
	var doc model.Document
	for _, candidate := range bundle.Documents {
		if candidate.DocID == "doc-dual-band-mixed" {
			doc = candidate
		}
	}
	_, result, err := engine.AnalyzeFull(doc, bundle.Strategy)
	if err != nil {
		t.Fatal(err)
	}
	spanOrder := -1
	for _, block := range result.Blocks {
		if block.Kind == model.KindBody && block.FullSpan {
			spanOrder = block.Order
		}
	}
	if spanOrder < 0 {
		t.Fatal("full-span block not marked")
	}
	for _, block := range result.Blocks {
		if block.Kind != model.KindBody || block.FullSpan {
			continue
		}
		if block.Y < 736 && block.Order > spanOrder {
			t.Fatalf("top-band block %s ordered after span", block.BlockID)
		}
		if block.Y >= 792 && block.Order < spanOrder {
			t.Fatalf("lower-band block %s ordered before span", block.BlockID)
		}
	}
}

func TestAllZeroConfidenceIsOrderConflict(t *testing.T) {
	bundle := demo.Generate()
	doc := bundle.Documents[1]
	bundle.Strategy.ConfidenceVerticalWeight = 0
	bundle.Strategy.ConfidenceColumnWeight = 0
	_, _, err := engine.AnalyzeFull(doc, bundle.Strategy)
	if err == nil {
		t.Fatal("expected order conflict")
	}
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeOrderConflict {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(modelErr.Conflict) == 0 || modelErr.Conflict[0].DocID != doc.DocID {
		t.Fatalf("missing conflict box pair: %+v", modelErr)
	}
}

func TestStageFingerprintsTrackEarliestPolicyStage(t *testing.T) {
	doc := demo.Generate().Documents[1]
	base := demo.Generate().Strategy
	normalized, err := engine.Normalize(doc, base)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := engine.InferColumns(normalized, base)
	if err != nil {
		t.Fatal(err)
	}
	changedMerge := base
	changedMerge.MergeGapPX++
	if engine.NormalizeInputFingerprint(doc, changedMerge) == engine.NormalizeInputFingerprint(doc, base) {
		t.Fatal("normalization fingerprint did not react to merge threshold")
	}
	changedColumn := base
	changedColumn.ColumnHistogramBinPX++
	if engine.NormalizeInputFingerprint(doc, changedColumn) != engine.NormalizeInputFingerprint(doc, base) {
		t.Fatal("column policy incorrectly changed normalization fingerprint")
	}
	if engine.ColumnInputFingerprint(normalized, changedColumn) == engine.ColumnInputFingerprint(normalized, base) {
		t.Fatal("column fingerprint did not react to column policy")
	}
	changedOrder := base
	changedOrder.ConfidenceColumnWeight += 0.1
	if engine.OrderInputFingerprint(columns, changedOrder) == engine.OrderInputFingerprint(columns, base) {
		t.Fatal("order fingerprint did not react to confidence weights")
	}
}
