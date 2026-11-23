package sum

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"

	"example.com/joinshuffle/internal/model"
)

const (
	DefaultFixedScaleBits = 80
	MinFixedScaleBits     = 32
	MaxFixedScaleBits     = 256
	unitRoundoff          = 1.0 / (1 << 53)
)

type Partial struct {
	Key          int64   `json:"key"`
	Partition    int     `json:"partition"`
	Count        int64   `json:"group_count"`
	ValueCount   int64   `json:"value_count"`
	AbsSum       float64 `json:"abs_sum"`
	Sum          float64 `json:"partial_sum"`
	Compensation float64 `json:"compensation"`
	FixedPoint   string  `json:"fixed_point,omitempty"`
	Digest       string  `json:"compensation_state_digest"`
}

type Accumulator interface {
	Add(value float64)
	Partial(key int64, partition int) Partial
}

func NewAccumulator(algorithm model.SumAlgorithm, fixedScaleBits int) (Accumulator, error) {
	switch algorithm {
	case model.SumNaive:
		return &naiveAccumulator{}, nil
	case model.SumKahan:
		return &kahanAccumulator{}, nil
	case model.SumDoubleDoubleFix:
		if fixedScaleBits == 0 {
			fixedScaleBits = DefaultFixedScaleBits
		}
		if fixedScaleBits < MinFixedScaleBits || fixedScaleBits > MaxFixedScaleBits {
			return nil, model.NewError(model.CodeInvalidRequest,
				"fixed_scale_bits is outside the supported range",
				"fixed_scale_bits", fixedScaleBits,
				"min", MinFixedScaleBits,
				"max", MaxFixedScaleBits)
		}
		return &ddFixedAccumulator{scaleBits: fixedScaleBits}, nil
	default:
		return nil, model.NewError(model.CodeInvalidRequest, "unknown sum algorithm", "algorithm", algorithm)
	}
}

type naiveAccumulator struct {
	sum        float64
	absSum     float64
	count      int64
	valueCount int64
}

func (a *naiveAccumulator) Add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	a.sum += value
	a.absSum += math.Abs(value)
	a.valueCount++
}

func (a *naiveAccumulator) Partial(key int64, partition int) Partial {
	p := Partial{
		Key:        key,
		Partition:  partition,
		ValueCount: a.valueCount,
		AbsSum:     a.absSum,
		Sum:        a.sum,
	}
	p.Digest = partialDigest(model.SumNaive, p, 0)
	return p
}

type kahanAccumulator struct {
	sum        float64
	comp       float64
	absSum     float64
	count      int64
	valueCount int64
}

func (a *kahanAccumulator) Add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	y := value - a.comp
	t := a.sum + y
	a.comp = (t - a.sum) - y
	a.sum = t
	a.absSum += math.Abs(value)
	a.valueCount++
}

func (a *kahanAccumulator) Partial(key int64, partition int) Partial {
	p := Partial{
		Key:          key,
		Partition:    partition,
		ValueCount:   a.valueCount,
		AbsSum:       a.absSum,
		Sum:          a.sum,
		Compensation: a.comp,
	}
	p.Digest = partialDigest(model.SumKahan, p, 0)
	return p
}

type ddFixedAccumulator struct {
	hi         float64
	lo         float64
	fixed      big.Int
	scaleBits  int
	absSum     float64
	count      int64
	valueCount int64
}

func (a *ddFixedAccumulator) Add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	s, err := twoSum(a.hi, value)
	a.hi = s
	a.lo += err
	a.hi, a.lo = quickTwoSum(a.hi, a.lo)
	q := quantize(value, a.scaleBits)
	a.fixed.Add(&a.fixed, q)
	a.absSum += math.Abs(value)
	a.valueCount++
}

func (a *ddFixedAccumulator) Partial(key int64, partition int) Partial {
	p := Partial{
		Key:          key,
		Partition:    partition,
		ValueCount:   a.valueCount,
		AbsSum:       a.absSum,
		Sum:          a.hi + a.lo,
		Compensation: a.lo,
		FixedPoint:   a.fixed.String(),
	}
	p.Digest = partialDigest(model.SumDoubleDoubleFix, p, a.scaleBits)
	return p
}

type MergeResult struct {
	Sum                  float64
	AbsSum               float64
	ValueCount           int64
	AbsErrorBound        float64
	RelErrorBound        float64
	AverageAbsErrorBound float64
	AverageRelErrorBound float64
	CompensationHash     string
	MergeOrderHash       string
}

func Merge(partials []Partial, algorithm model.SumAlgorithm, fixedScaleBits int, mergeRule string) (MergeResult, error) {
	if len(partials) == 0 {
		return MergeResult{CompensationHash: hashStrings(nil), MergeOrderHash: hashStrings(nil)}, nil
	}
	ordered, err := orderPartials(partials, mergeRule)
	if err != nil {
		return MergeResult{}, err
	}
	var absSum float64
	var valueCount int64
	for _, p := range ordered {
		absSum += p.AbsSum
		valueCount += p.ValueCount
	}
	var value float64
	switch algorithm {
	case model.SumNaive:
		for _, p := range ordered {
			value += p.Sum
		}
	case model.SumKahan:
		var acc kahanAccumulator
		for _, p := range ordered {
			acc.Add(p.Sum)
			if p.Compensation != 0 {
				acc.Add(-p.Compensation)
			}
		}
		value = acc.sum - acc.comp
	case model.SumDoubleDoubleFix:
		if fixedScaleBits == 0 {
			fixedScaleBits = DefaultFixedScaleBits
		}
		var total big.Int
		for _, p := range ordered {
			var n big.Int
			if p.FixedPoint != "" {
				if _, ok := n.SetString(p.FixedPoint, 10); !ok {
					return MergeResult{}, fmt.Errorf("invalid fixed-point partial for key %d", p.Key)
				}
			}
			total.Add(&total, &n)
		}
		value = fixedToFloat(&total, fixedScaleBits)
	default:
		return MergeResult{}, fmt.Errorf("unknown sum algorithm %q", algorithm)
	}
	absError := errorBound(algorithm, fixedScaleBits, valueCount, absSum, value)
	res := MergeResult{
		Sum:              value,
		AbsSum:           absSum,
		ValueCount:       valueCount,
		AbsErrorBound:    absError,
		RelErrorBound:    relativeBound(absError, value),
		CompensationHash: hashPartials(ordered),
		MergeOrderHash:   hashOrder(ordered),
	}
	if valueCount > 0 {
		average := value / float64(valueCount)
		averageBound := finite(absError/float64(valueCount) + unitRoundoff*math.Abs(average))
		res.AverageAbsErrorBound = averageBound
		res.AverageRelErrorBound = relativeBound(averageBound, average)
	}
	return res, nil
}

func orderPartials(partials []Partial, rule string) ([]Partial, error) {
	ordered := append([]Partial(nil), partials...)
	switch rule {
	case "", "partition_ascending":
		sort.SliceStable(ordered, func(i, j int) bool {
			if ordered[i].Partition != ordered[j].Partition {
				return ordered[i].Partition < ordered[j].Partition
			}
			return ordered[i].Key < ordered[j].Key
		})
	case "magnitude_bucket_partition_asc":
		sort.SliceStable(ordered, func(i, j int) bool {
			bi := magnitudeBucket(partialRepresentative(ordered[i]))
			bj := magnitudeBucket(partialRepresentative(ordered[j]))
			if bi != bj {
				return bi < bj
			}
			ai := math.Abs(partialRepresentative(ordered[i]))
			aj := math.Abs(partialRepresentative(ordered[j]))
			if ai != aj {
				return ai < aj
			}
			if ordered[i].Partition != ordered[j].Partition {
				return ordered[i].Partition < ordered[j].Partition
			}
			return ordered[i].Key < ordered[j].Key
		})
	default:
		return nil, model.NewError(model.CodeInvalidRequest, "unknown merge rule", "merge_rule", rule)
	}
	return ordered, nil
}

func partialRepresentative(p Partial) float64 {
	switch {
	case p.FixedPoint != "":
		return p.Sum
	case p.Compensation != 0:
		return p.Sum - p.Compensation
	default:
		return p.Sum
	}
}

func magnitudeBucket(value float64) int {
	value = math.Abs(value)
	if value == 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return math.MinInt32
	}
	return int(math.Floor(math.Log2(value)))
}

func errorBound(algorithm model.SumAlgorithm, scaleBits int, n int64, absSum, value float64) float64 {
	u := unitRoundoff
	nf := float64(n)
	switch algorithm {
	case model.SumNaive:
		denom := 1 - nf*u
		if denom <= 0 {
			return math.MaxFloat64
		}
		return finite((nf * u / denom) * absSum)
	case model.SumKahan:
		return finite((2*u + nf*u*u) * absSum)
	case model.SumDoubleDoubleFix:
		if scaleBits <= 0 {
			scaleBits = DefaultFixedScaleBits
		}
		quantization := nf * math.Ldexp(1, -(scaleBits+1))
		return finite(quantization + u*math.Abs(value))
	default:
		return math.MaxFloat64
	}
}

func relativeBound(absBound, value float64) float64 {
	if absBound == 0 {
		return 0
	}
	denom := math.Abs(value)
	if denom < math.SmallestNonzeroFloat64 {
		return math.MaxFloat64
	}
	return finite(absBound / denom)
}

func finite(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return math.MaxFloat64
	}
	return value
}

func quantize(value float64, scaleBits int) *big.Int {
	if value == 0 {
		return new(big.Int)
	}
	sign := 1
	bits := math.Float64bits(value)
	if value < 0 {
		sign = -1
		bits = math.Float64bits(-value)
	}
	expBits := int((bits >> 52) & 0x7ff)
	mant := int64(bits & ((1 << 52) - 1))
	var exp int
	if expBits == 0 {
		exp = -1022
	} else {
		mant |= 1 << 52
		exp = expBits - 1023
	}
	shift := exp - 52 + scaleBits
	n := new(big.Int).SetInt64(mant)
	if shift >= 0 {
		n.Lsh(n, uint(shift))
	} else {
		drop := uint(-shift)
		den := new(big.Int).Lsh(big.NewInt(1), drop)
		q := new(big.Int).Rsh(n, drop)
		rem := new(big.Int).And(n, new(big.Int).Sub(den, big.NewInt(1)))
		rem.Lsh(rem, 1)
		if rem.Cmp(den) >= 0 {
			q.Add(q, big.NewInt(1))
		}
		n = q
	}
	if sign < 0 {
		n.Neg(n)
	}
	return n
}

func fixedToFloat(n *big.Int, scaleBits int) float64 {
	f := new(big.Float).SetPrec(256).SetInt(n)
	f.SetMantExp(f, -scaleBits)
	value, _ := f.Float64()
	return value
}

func twoSum(a, b float64) (float64, float64) {
	s := a + b
	bb := s - a
	err := (a - (s - bb)) + (b - bb)
	return s, err
}

func quickTwoSum(a, b float64) (float64, float64) {
	s := a + b
	err := b - (s - a)
	return s, err
}

func partialDigest(algorithm model.SumAlgorithm, p Partial, scaleBits int) string {
	h := sha256.New()
	h.Write([]byte(algorithm))
	h.Write([]byte{0})
	writeUint64(h, uint64(p.Key))
	writeUint64(h, uint64(p.Partition))
	writeUint64(h, uint64(p.ValueCount))
	writeUint64(h, math.Float64bits(p.AbsSum))
	writeUint64(h, math.Float64bits(p.Sum))
	writeUint64(h, math.Float64bits(p.Compensation))
	if scaleBits != 0 {
		writeUint64(h, uint64(scaleBits))
	}
	h.Write([]byte(p.FixedPoint))
	return hex.EncodeToString(h.Sum(nil))
}

func hashPartials(partials []Partial) string {
	items := make([]string, len(partials))
	for i := range partials {
		items[i] = partials[i].Digest
	}
	return hashStrings(items)
}

func hashOrder(partials []Partial) string {
	items := make([]string, len(partials))
	for i := range partials {
		items[i] = strconv.Itoa(partials[i].Partition) + ":" + strconv.FormatInt(partials[i].Key, 10)
	}
	return hashStrings(items)
}

func hashStrings(items []string) string {
	h := sha256.New()
	for _, item := range items {
		binary.Write(h, binary.LittleEndian, uint32(len(item)))
		h.Write([]byte(item))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeUint64(h interface{ Write([]byte) (int, error) }, value uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], value)
	h.Write(b[:])
}

func JoinMeasure(left float64, leftValid bool, right float64, rightValid bool) (float64, bool) {
	switch {
	case leftValid && rightValid:
		return left + right, true
	case leftValid:
		return left, true
	case rightValid:
		return right, true
	default:
		return 0, false
	}
}
