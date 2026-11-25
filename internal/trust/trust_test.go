package trust

import (
	"testing"

	"blockinspect/internal/manifest"
)

func TestComputeScoresPenalizesDivergence(t *testing.T) {
	rep := ComputeScores([]ReplicaInput{
		{ReplicaID: "a", DivergentPages: 0, FirstDivergeLevel: 0, InQuorum: true},
		{ReplicaID: "b", DivergentPages: 0, FirstDivergeLevel: 0, InQuorum: true},
		{ReplicaID: "c", DivergentPages: 3, FirstDivergeLevel: 2, InQuorum: false},
	}, 96, 6, 2)
	if len(rep.Scores) != 3 {
		t.Fatalf("want 3 scores, got %d", len(rep.Scores))
	}
	var a, c int64
	for _, s := range rep.Scores {
		switch s.ReplicaID {
		case "a":
			a = s.Score
		case "c":
			c = s.Score
		}
	}
	if a <= c {
		t.Fatalf("consistent replica must outrank divergent one: a=%d c=%d", a, c)
	}
	if len(rep.Steps) == 0 || rep.CumulativeErrorMicro == "" {
		t.Fatal("ledger steps and cumulative error must be reported")
	}
	if rep.Ranking[0] != "a" && rep.Ranking[0] != "b" {
		t.Fatalf("ranking must start with a quorum replica: %v", rep.Ranking)
	}
}

func TestCompareScoresToleranceAndTieBreak(t *testing.T) {
	// within 0.1% relative tolerance -> equal
	if CompareScores(1_000_000, 1_000_500) != 0 {
		t.Fatal("scores within tolerance must compare equal")
	}
	if CompareScores(1_000_000, 1_002_000) == 0 {
		t.Fatal("scores beyond tolerance must not compare equal")
	}
	ranking := Rank([]TrustScore{
		{ReplicaID: "zeta", Score: 1_000_000},
		{ReplicaID: "alpha", Score: 1_000_500}, // equal within tolerance
		{ReplicaID: "mid", Score: 500_000},
	})
	if ranking[0] != "alpha" || ranking[1] != "zeta" || ranking[2] != "mid" {
		t.Fatalf("tie must break by lexicographic replica id: %v", ranking)
	}
}

func TestDamageRange(t *testing.T) {
	m := &manifest.Manifest{PageCount: 10, ColumnsPerPage: 4}
	dr := ComputeDamageRange([]int{7, 2, 2, 5}, m)
	if dr.StartPage != 2 || dr.EndPage != 7 {
		t.Fatalf("bad range: %+v", dr)
	}
	if len(dr.AffectedPages) != 3 || len(dr.AffectedColumns) != 12 {
		t.Fatalf("bad affected sets: %+v", dr)
	}
	empty := ComputeDamageRange(nil, m)
	if empty.StartPage != -1 || empty.EndPage != -1 {
		t.Fatalf("empty damage must be -1/-1: %+v", empty)
	}
}

func TestDivRounding(t *testing.T) {
	l := NewLedger()
	// 1/3 in fixed point rounds half-up to 333333
	if got := l.Div("third", 1, 3); got != 333_333 {
		t.Fatalf("1/3 = %d, want 333333", got)
	}
	// 1/2 rounds to exactly 500000 with zero error
	l2 := NewLedger()
	if got := l2.Div("half", 1, 2); got != 500_000 {
		t.Fatalf("1/2 = %d, want 500000", got)
	}
	if l2.CumulativeErrorMicro() != "0.000000" {
		t.Fatalf("exact division must have zero error, got %s", l2.CumulativeErrorMicro())
	}
}
