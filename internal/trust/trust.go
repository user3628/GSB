// Package trust computes replica trust scores and block damage ranges using
// explicit fixed-point arithmetic. All scores are int64 values scaled by
// Scale (1e6). Every division records its rounding mode and the exact
// rounding residual (as a rational micro-error) so reports can state the
// cumulative rounding error without ever using floats.
package trust

import (
	"fmt"
	"math/big"
	"sort"

	"blockinspect/internal/manifest"
)

// Scale is the fixed-point denominator: 1.0 == 1_000_000.
const Scale int64 = 1_000_000

// Rounding modes used by fixed-point division.
const (
	RoundHalfUp = "ROUND_HALF_UP"
	RoundDown   = "ROUND_DOWN" // truncation toward zero
)

// Step records one fixed-point computation step for auditability.
type Step struct {
	Op           string `json:"op"`
	Inputs       string `json:"inputs"`
	RoundingMode string `json:"rounding_mode"`
	// RoundingErrorMicro is the exact residual of this step, expressed in
	// micro units (1e-6 of one Scale unit) as a decimal string.
	RoundingErrorMicro string `json:"rounding_error_micro"`
	Result             int64  `json:"result"`
}

// Ledger accumulates computation steps and the exact cumulative rounding
// error as a rational number of micro units.
type Ledger struct {
	Steps     []Step   `json:"steps"`
	cumErrRat *big.Rat // cumulative error in micro units
}

// NewLedger creates an empty ledger.
func NewLedger() *Ledger { return &Ledger{cumErrRat: new(big.Rat)} }

// Div computes a*Scale/b in fixed point with ROUND_HALF_UP, records the
// step and accumulates the exact rounding residual.
func (l *Ledger) Div(op string, a, b int64) int64 {
	if b == 0 {
		l.Steps = append(l.Steps, Step{Op: op, Inputs: fmt.Sprintf("%d/%d", a, b),
			RoundingMode: RoundDown, RoundingErrorMicro: "0", Result: 0})
		return 0
	}
	num := new(big.Int).Mul(big.NewInt(a), big.NewInt(Scale))
	q, r := new(big.Int).QuoRem(num, big.NewInt(b), new(big.Int))
	// half-up rounding on the remainder
	twice := new(big.Int).Mul(r, big.NewInt(2))
	if twice.Cmp(big.NewInt(b)) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	// Exact residual of the rounded fixed-point result, in micro units
	// (1e-6 of one Scale unit): (q - num/b) * 1e6 == (q*b - num) * 1e6 / b.
	resNum := new(big.Int).Mul(q, big.NewInt(b))
	resNum.Sub(resNum, num)
	errRat := new(big.Rat).SetInt(resNum)
	errRat.Mul(errRat, big.NewRat(1_000_000, 1))
	errRat.Quo(errRat, big.NewRat(b, 1))
	l.cumErrRat.Add(l.cumErrRat, errRat)
	res := q.Int64()
	l.Steps = append(l.Steps, Step{
		Op:                 op,
		Inputs:             fmt.Sprintf("%d/%d", a, b),
		RoundingMode:       RoundHalfUp,
		RoundingErrorMicro: errRat.RatString(),
		Result:             res,
	})
	return res
}

// MulDiv computes fixedA*b/c where fixedA is already fixed-point: the
// result stays in fixed point. Rounds half-up and records the residual.
func (l *Ledger) MulDiv(op string, fixedA, b, c int64) int64 {
	if c == 0 {
		l.Steps = append(l.Steps, Step{Op: op, Inputs: fmt.Sprintf("%d*%d/%d", fixedA, b, c),
			RoundingMode: RoundDown, RoundingErrorMicro: "0", Result: 0})
		return 0
	}
	num := new(big.Int).Mul(big.NewInt(fixedA), big.NewInt(b))
	q, r := new(big.Int).QuoRem(num, big.NewInt(c), new(big.Int))
	twice := new(big.Int).Mul(r, big.NewInt(2))
	if twice.Cmp(big.NewInt(c)) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	// exact residual in micro units: (q*c - num)/c * 1e6
	res := new(big.Int).Mul(q, big.NewInt(c))
	res.Sub(res, num)
	errRat := new(big.Rat).SetInt(res)
	errRat.Mul(errRat, big.NewRat(1_000_000, 1))
	errRat.Quo(errRat, big.NewRat(c, 1))
	l.cumErrRat.Add(l.cumErrRat, errRat)
	out := q.Int64()
	l.Steps = append(l.Steps, Step{
		Op:                 op,
		Inputs:             fmt.Sprintf("%d*%d/%d", fixedA, b, c),
		RoundingMode:       RoundHalfUp,
		RoundingErrorMicro: errRat.RatString(),
		Result:             out,
	})
	return out
}

// CumulativeErrorMicro returns the exact cumulative rounding error in micro
// units (1e-6 of one Scale unit), as a decimal string with 6 places.
func (l *Ledger) CumulativeErrorMicro() string {
	f := new(big.Rat).Set(l.cumErrRat)
	// print with 6 decimal places
	scaled := new(big.Rat).Mul(f, big.NewRat(1_000_000, 1))
	num := new(big.Int).Quo(scaled.Num(), scaled.Denom())
	neg := num.Sign() < 0
	if neg {
		num.Abs(num)
	}
	intPart := new(big.Int).Quo(num, big.NewInt(1_000_000))
	fracPart := new(big.Int).Rem(num, big.NewInt(1_000_000))
	sign := ""
	if neg {
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%06d", sign, intPart, fracPart)
}

// Weights for the trust formula (fixed-point, sum to Scale).
const (
	WeightPages = 700_000 // divergence-page-count weight
	WeightLevel = 300_000 // divergence-level-depth weight
)

// RelTolerance is the relative tolerance (fixed-point) used when comparing
// trust scores: scores within 0.1% are considered equal.
const RelTolerance int64 = 1_000

// ReplicaInput carries the per-replica divergence facts from verification.
type ReplicaInput struct {
	ReplicaID         string `json:"replica_id"`
	DivergentPages    int    `json:"divergent_pages"`
	FirstDivergeLevel int    `json:"first_diverge_level"` // 0 when fully consistent
	InQuorum          bool   `json:"in_quorum"`
}

// TrustScore is the fixed-point trust result for one replica.
type TrustScore struct {
	ReplicaID string `json:"replica_id"`
	Score     int64  `json:"score"` // fixed-point, Scale == 1.0
	ScoreText string `json:"score_text"`
}

// ScoreText renders a fixed-point score with 6 decimals.
func ScoreText(v int64) string {
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	return fmt.Sprintf("%s%d.%06d", sign, v/Scale, v%Scale)
}

// TrustReport bundles scores, ranking and the computation ledger.
type TrustReport struct {
	Scores               []TrustScore `json:"scores"`
	Ranking              []string     `json:"ranking"` // replica IDs, most trusted first
	Steps                []Step       `json:"steps"`
	CumulativeErrorMicro string       `json:"cumulative_error_micro"`
	QuorumParticipants   int          `json:"quorum_participants"`
	TotalReplicas        int          `json:"total_replicas"`
}

// ComputeScores derives per-replica trust scores from divergence facts.
//
//	per-replica penalty = WeightPages*divPages/pageCount + WeightLevel*divLevel/maxLevel
//	base              = Scale - penalty (floored at 0)
//	trust             = base * quorumParticipants / totalReplicas
func ComputeScores(inputs []ReplicaInput, pageCount, maxLevel, quorumParticipants int) *TrustReport {
	l := NewLedger()
	total := int64(len(inputs))
	rep := &TrustReport{QuorumParticipants: quorumParticipants, TotalReplicas: len(inputs)}
	for _, in := range inputs {
		pagePen := l.MulDiv(fmt.Sprintf("page_penalty[%s]", in.ReplicaID),
			WeightPages, int64(in.DivergentPages), int64(pageCount))
		levelPen := int64(0)
		if maxLevel > 0 {
			levelPen = l.MulDiv(fmt.Sprintf("level_penalty[%s]", in.ReplicaID),
				WeightLevel, int64(in.FirstDivergeLevel), int64(maxLevel))
		}
		base := Scale - pagePen - levelPen
		if base < 0 {
			base = 0
		}
		score := l.MulDiv(fmt.Sprintf("participation_scale[%s]", in.ReplicaID),
			base, int64(quorumParticipants), total)
		rep.Scores = append(rep.Scores, TrustScore{
			ReplicaID: in.ReplicaID, Score: score, ScoreText: ScoreText(score),
		})
	}
	rep.Steps = l.Steps
	rep.CumulativeErrorMicro = l.CumulativeErrorMicro()
	rep.Ranking = Rank(rep.Scores)
	return rep
}

// CompareScores compares two fixed-point scores with a relative tolerance.
// Returns 0 when equal within tolerance, -1/1 otherwise.
func CompareScores(a, b int64) int {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	m := a
	if b > m {
		m = b
	}
	if m < 0 {
		m = -m
	}
	// equal when diff*Scale <= RelTolerance*m  (avoid floats)
	if new(big.Int).Mul(big.NewInt(diff), big.NewInt(Scale)).
		Cmp(new(big.Int).Mul(big.NewInt(RelTolerance), big.NewInt(m))) <= 0 {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

// Rank orders replica IDs by trust score descending; scores equal within
// the relative tolerance are ordered by replica ID in stable lexicographic
// (dictionary) order.
func Rank(scores []TrustScore) []string {
	s := append([]TrustScore(nil), scores...)
	sort.SliceStable(s, func(i, j int) bool {
		if c := CompareScores(s[i].Score, s[j].Score); c != 0 {
			return c > 0
		}
		return s[i].ReplicaID < s[j].ReplicaID
	})
	out := make([]string, 0, len(s))
	for _, x := range s {
		out = append(out, x.ReplicaID)
	}
	return out
}

// DamageRange describes the damaged extent of a block.
type DamageRange struct {
	StartPage       int   `json:"start_page"` // -1 when no damage
	EndPage         int   `json:"end_page"`
	AffectedColumns []int `json:"affected_columns"`
	AffectedPages   []int `json:"affected_pages"`
}

// ComputeDamageRange derives the damage range from the set of pages that
// diverge from consensus and the manifest's column layout.
func ComputeDamageRange(divergentPages []int, m *manifest.Manifest) DamageRange {
	dr := DamageRange{StartPage: -1, EndPage: -1}
	if len(divergentPages) == 0 {
		return dr
	}
	pages := append([]int(nil), divergentPages...)
	sort.Ints(pages)
	seen := map[int]bool{}
	for _, p := range pages {
		if p < 0 || p >= m.PageCount || seen[p] {
			continue
		}
		seen[p] = true
		dr.AffectedPages = append(dr.AffectedPages, p)
		if dr.StartPage == -1 || p < dr.StartPage {
			dr.StartPage = p
		}
		if p > dr.EndPage {
			dr.EndPage = p
		}
		for c := 0; c < m.ColumnsPerPage; c++ {
			dr.AffectedColumns = append(dr.AffectedColumns, p*m.ColumnsPerPage+c)
		}
	}
	return dr
}
