package model

import (
	"encoding/json"
	"testing"
)

func TestDocumentUnmarshalAliasesAndDefaults(t *testing.T) {
	input := []byte(`{
		"doc_id":"alias-doc",
		"page_width":800,
		"page_height":1000,
		"boxes":[
			{"x":0.1,"y":0.1,"width":0.2,"height":0.1,"font_size":"title","text":"T","lang":"en","column_hint":0},
			{"x":0.1,"y":0.3,"w":0.2,"h":0.1,"font_size":2,"text":"B"}
		]
	}`)
	var doc Document
	if err := json.Unmarshal(input, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Page.Width != 800 || doc.Page.Height != 1000 {
		t.Fatalf("page alias not decoded: %+v", doc.Page)
	}
	if len(doc.Boxes) != 2 || doc.Boxes[0].Index != 0 || doc.Boxes[1].Index != 1 {
		t.Fatalf("missing box indexes not filled: %+v", doc.Boxes)
	}
	if doc.Boxes[0].W != 0.2 || doc.Boxes[0].H != 0.1 || doc.Boxes[0].FontTier != 3 {
		t.Fatalf("box aliases not decoded: %+v", doc.Boxes[0])
	}
	if doc.Boxes[1].Lang != "unknown" || doc.Boxes[1].ColumnHint != -2 {
		t.Fatalf("defaults not applied: %+v", doc.Boxes[1])
	}
}

func TestContentFingerprintIgnoresDeclarationOrder(t *testing.T) {
	docA := Document{DocID: "d", Page: Page{Width: 100, Height: 200}, Boxes: []TextBox{
		{Index: 0, X: 0.1, Y: 0.1, W: 0.2, H: 0.1, Text: "a"},
		{Index: 1, X: 0.1, Y: 0.2, W: 0.2, H: 0.1, Text: "b"},
	}}
	docB := docA
	docB.Boxes = []TextBox{docA.Boxes[1], docA.Boxes[0]}
	if docA.ContentFingerprint() != docB.ContentFingerprint() {
		t.Fatal("content fingerprint depends on declaration order")
	}
}

func TestDefaultStrategyIsValid(t *testing.T) {
	if err := DefaultStrategy().Validate(); err != nil {
		t.Fatalf("default strategy invalid: %v", err)
	}
}
