package edges

import (
	"strings"
	"testing"

	"depverify/internal/model"
)

func TestParseAndNormalize(t *testing.T) {
	in := strings.NewReader(`{"parent":"ACME/App","child":"acme/Core","constraint":[1,2,0]}

{"parent":"acme/app","child":"acme/util","constraint":[2]}
`)
	got, issues := Parse(in)
	if len(issues) != 0 {
		t.Fatalf("unexpected issues: %+v", issues)
	}
	if got[0].Parent != "acme/app" || got[0].Child != "acme/core" {
		t.Fatalf("key normalization failed: %+v", got[0])
	}
	if got[0].Line != 1 || got[1].Line != 3 {
		t.Fatalf("line numbers wrong: %+v", got)
	}
}

func TestMalformedLine(t *testing.T) {
	_, issues := Parse(strings.NewReader("{not json}\n"))
	if len(issues) != 1 || issues[0].Code != model.EEdgeMalformed || issues[0].Field != "line" {
		t.Fatalf("want E_EDGE_MALFORMED with field=line, got %+v", issues)
	}
}

func TestDupAndDangling(t *testing.T) {
	edgeList, pIssues := Parse(strings.NewReader(
		`{"parent":"acme/app","child":"acme/core","constraint":[1]}
{"parent":"acme/app","child":"acme/core","constraint":[2]}
{"parent":"acme/app","child":"acme/ghost","constraint":[1]}
`))
	if len(pIssues) != 0 {
		t.Fatalf("parse issues: %+v", pIssues)
	}
	known := map[string]struct{}{"acme/app": {}, "acme/core": {}}
	issues, unique := CheckGraph(edgeList, known)
	codes := map[string]int{}
	for _, is := range issues {
		codes[is.Code]++
	}
	if codes[model.EDupEdge] != 1 {
		t.Fatalf("want 1 dup edge, got %+v", issues)
	}
	if codes[model.EEdgeDangling] != 1 {
		t.Fatalf("want 1 dangling edge, got %+v", issues)
	}
	if issues[0].Line != 2 || issues[0].Field != "parent+child" {
		t.Fatalf("dup edge line/field wrong: %+v", issues[0])
	}
	if len(unique) != 2 {
		t.Fatalf("want 2 unique edges, got %d", len(unique))
	}
}
