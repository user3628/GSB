package engine

import (
	"math"
	"sort"
)

// ScoreStep is one entry of the score conversion chain.
type ScoreStep struct {
	Seq     int64 `json:"seq"`
	Delta   int64 `json:"delta"`
	Mult    int64 `json:"mult"`
	Product int64 `json:"product"`
	Running int64 `json:"running"`
}

// TruncationEntry records the fixed-point truncation error ledger when a
// score is converted to whole trust points.
type TruncationEntry struct {
	Dividend  int64 `json:"dividend"`
	Divisor   int64 `json:"divisor"`
	Quotient  int64 `json:"quotient"`
	Remainder int64 `json:"remainder"`
}

func mulOverflow(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, false
	}
	if (a == math.MinInt64 && b == -1) || (b == math.MinInt64 && a == -1) {
		return 0, true
	}
	c := a * b
	if c/b != a {
		return 0, true
	}
	return c, false
}

func addOverflow(a, b int64) (int64, bool) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, true
	}
	return c, false
}

// Accumulate folds behavior events, ordered by logical seq, into a
// fixed-point score. Any multiply/add overflow yields E_SCORE_OVERFLOW and
// the partial chain accumulated so far.
func Accumulate(events []BehaviorEvent) (int64, []ScoreStep, error) {
	sorted := make([]BehaviorEvent, len(events))
	copy(sorted, events)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })
	var score int64
	steps := make([]ScoreStep, 0, len(sorted))
	for _, ev := range sorted {
		prod, ov := mulOverflow(ev.Delta, ev.Mult)
		if ov {
			return 0, steps, errf(CodeScoreOverflow, "multiply overflow at seq %d", ev.Seq)
		}
		sum, ov := addOverflow(score, prod)
		if ov {
			return 0, steps, errf(CodeScoreOverflow, "add overflow at seq %d", ev.Seq)
		}
		score = sum
		steps = append(steps, ScoreStep{Seq: ev.Seq, Delta: ev.Delta, Mult: ev.Mult, Product: prod, Running: score})
	}
	return score, steps, nil
}

// Truncate converts a fixed-point score to whole trust points, recording
// the discarded remainder in the truncation error ledger.
func Truncate(score int64) (int64, TruncationEntry) {
	q := score / Scale
	r := score % Scale
	return q, TruncationEntry{Dividend: score, Divisor: Scale, Quotient: q, Remainder: r}
}
