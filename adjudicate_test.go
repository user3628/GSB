package dnsdetect

import (
	"sort"
	"testing"
)

func TestGeneratedDatasetFindingsAndPairing(t *testing.T) {
	dataset := testDataset(t)
	result := processGood(t, dataset, testConfig(dataset), nil)
	if len(result.Report.Findings) != 4 {
		t.Fatalf("findings = %d, want 4: %+v", len(result.Report.Findings), result.Report.Findings)
	}
	counts := map[string]int{}
	for _, finding := range result.Report.Findings {
		counts[finding.Code]++
	}
	for _, code := range []string{CodeUnsolicitedResponse, CodeIrrelevantAdditional, CodeTTLMismatch, CodeMultiAddress} {
		if counts[code] != 1 {
			t.Fatalf("%s count = %d", code, counts[code])
		}
	}
	if len(result.Report.Pairs) != 6 {
		t.Fatalf("pairs = %d, want 6", len(result.Report.Pairs))
	}
	if len(result.Report.Ledger) != 4 {
		t.Fatalf("ledger aggregates = %d, want 4", len(result.Report.Ledger))
	}
	var cross *Pair
	for index := range result.Report.Pairs {
		if result.Report.Pairs[index].TXID == 0x1001 {
			cross = &result.Report.Pairs[index]
			break
		}
	}
	if cross == nil {
		t.Fatal("missing cross-source pair 0x1001")
	}
	if cross.Primary.SourceKind != SourceBinary || len(cross.Evidence) != 5 {
		t.Fatalf("cross pair primary/evidence = %+v / %d", cross.Primary, len(cross.Evidence))
	}
	evidenceKinds := map[SourceKind]bool{}
	for _, ref := range cross.Evidence {
		evidenceKinds[ref.SourceKind] = true
	}
	for _, kind := range []SourceKind{SourceBinary, SourceJSONL, SourceText} {
		if !evidenceKinds[kind] {
			t.Fatalf("cross pair lacks %s evidence", kind)
		}
	}
	if len(result.Report.FormatVersions) != 2 || result.Report.FormatVersions[0] != 1 || result.Report.FormatVersions[1] != 2 {
		t.Fatalf("format versions = %v", result.Report.FormatVersions)
	}
	if !sort.SliceIsSorted(result.Report.Findings, func(i, j int) bool {
		a, b := result.Report.Findings[i], result.Report.Findings[j]
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		if a.TXID != b.TXID {
			return a.TXID < b.TXID
		}
		return a.Code < b.Code
	}) {
		t.Fatalf("findings not sorted by path/txid/code")
	}
}

func TestRelatedAdditionalIsNotFlagged(t *testing.T) {
	config := DefaultConfig()
	events := []Event{
		{SourcePath: "q", TXID: 9, Question: "example.com.", Role: RoleQuery, TimeTicks: 100},
		{SourcePath: "r", TXID: 9, Question: "example.com.", Role: RoleResponse, TimeTicks: 200, Records: []ResourceRecord{
			{Section: SectionAnswer, Name: "example.com", Type: TypeA, TTL: 30, Address: "192.0.2.1", Offset: 10},
			{Section: SectionAdditional, Name: "ns.example.com", Type: TypeA, TTL: 30, Address: "192.0.2.53", Offset: 20},
		}},
	}
	result, err := Adjudicate(events, config)
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range result.Findings {
		if finding.Code == CodeIrrelevantAdditional {
			t.Fatalf("in-bailiwick additional was flagged: %+v", finding)
		}
	}
}
