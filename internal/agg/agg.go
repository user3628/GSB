// Package agg implements deterministic grouped floating-point aggregation.
//
// Every event is applied in production-sequence order. Batch boundaries only
// affect audit summaries, never the final accumulator state.
package agg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

type Algorithm string

const (
	Naive     Algorithm = "naive_sequential"
	Kahan     Algorithm = "kahan_compensated"
	Magnitude Algorithm = "magnitude_buckets"
)

// UnitRoundoff is 2^-53, the IEEE-754 binary64 unit roundoff for round-to-nearest.
const UnitRoundoff = 1.0 / (1 << 53)

func ParseAlgorithm(value string) (Algorithm, error) {
	switch Algorithm(value) {
	case Naive, Kahan, Magnitude:
		return Algorithm(value), nil
	default:
		return "", fmt.Errorf("%w: unsupported aggregation algorithm %q", ErrInvalidAlgorithm, value)
	}
}

var ErrInvalidAlgorithm = fmt.Errorf("invalid aggregation algorithm")

type Bucket struct {
	Sum          float64 `json:"sum"`
	Compensation float64 `json:"compensation"`
	Count        int64   `json:"count"`
	SumAbs       float64 `json:"sum_abs"`
}

// State is the durable, order-sensitive accumulator for one aggregation key.
type State struct {
	Count        int64             `json:"count"`
	Sum          float64           `json:"sum"`
	Compensation float64           `json:"compensation,omitempty"`
	SumAbs       float64           `json:"sum_abs"`
	Buckets      map[string]Bucket `json:"buckets,omitempty"`
}

func (s *State) Add(algorithm Algorithm, value float64) {
	if value == 0 && !math.Signbit(value) {
		value = 0
	}
	s.Count++
	s.SumAbs += math.Abs(value)

	switch algorithm {
	case Naive:
		s.Sum += value
	case Kahan:
		y := value - s.Compensation
		t := s.Sum + y
		s.Compensation = (t - s.Sum) - y
		s.Sum = t
	case Magnitude:
		if s.Buckets == nil {
			s.Buckets = make(map[string]Bucket)
		}
		key := bucketKey(value)
		b := s.Buckets[key]
		kahanAdd(&b.Sum, &b.Compensation, value)
		b.Count++
		b.SumAbs += math.Abs(value)
		s.Buckets[key] = b
	}
}

func (s *State) Value(algorithm Algorithm) float64 {
	if algorithm != Magnitude {
		return s.Sum
	}
	if len(s.Buckets) == 0 {
		return 0
	}
	keys := make([]string, 0, len(s.Buckets))
	for key := range s.Buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := strconv.Atoi(keys[i])
		b, _ := strconv.Atoi(keys[j])
		return a < b
	})

	var sum, compensation float64
	for _, key := range keys {
		b := s.Buckets[key]
		kahanAdd(&sum, &compensation, b.Sum)
	}
	return sum
}

// AbsErrorBound returns a conservative a-priori forward-error bound for this
// accumulator. It is expressed in units of sum_abs, which makes it meaningful
// even when catastrophic cancellation makes a result-relative bound huge.
func (s *State) AbsErrorBound(algorithm Algorithm) float64 {
	if s.Count == 0 || s.SumAbs == 0 {
		return 0
	}
	switch algorithm {
	case Naive:
		return gamma(s.Count) * l1UpperBound(s.Count, s.SumAbs)
	case Kahan:
		return kahanBound(s.Count) * l1UpperBound(s.Count, s.SumAbs)
	case Magnitude:
		if len(s.Buckets) == 0 {
			return 0
		}
		var bound float64
		keys := make([]string, 0, len(s.Buckets))
		for key := range s.Buckets {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			a, _ := strconv.Atoi(keys[i])
			b, _ := strconv.Atoi(keys[j])
			return a < b
		})
		for _, key := range keys {
			b := s.Buckets[key]
			bound += kahanBound(b.Count) * l1UpperBound(b.Count, b.SumAbs)
		}
		bound += kahanBound(int64(len(keys))) * l1UpperBound(s.Count, s.SumAbs)
		return bound
	default:
		return math.Inf(1)
	}
}

func (s *State) RelativeErrorBound(algorithm Algorithm) float64 {
	bound := s.AbsErrorBound(algorithm)
	if bound == 0 {
		return 0
	}
	denominator := math.Abs(s.Value(algorithm))
	if denominator == 0 {
		return math.MaxFloat64
	}
	value := bound / denominator
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return math.MaxFloat64
	}
	return value
}

// Finalized returns a copy with the algorithm's canonical merged value in Sum.
// It is used for snapshots and digests. Add still derives Magnitude from Buckets.
func (s State) Finalized(algorithm Algorithm) State {
	s.Sum = s.Value(algorithm)
	if algorithm == Magnitude {
		s.Compensation = 0
	}
	return s
}

type Group struct {
	Algorithm Algorithm          `json:"algorithm"`
	Keys      map[string]State   `json:"keys"`
	Bounds    map[string]float64 `json:"absolute_error_bounds"`
}

func NewGroup(algorithm Algorithm) Group {
	return Group{
		Algorithm: algorithm,
		Keys:      make(map[string]State),
		Bounds:    make(map[string]float64),
	}
}

func (g *Group) Add(key string, value float64) {
	state := g.Keys[key]
	state.Add(g.Algorithm, value)
	g.Keys[key] = state
}

func (g *Group) Finalize() {
	for key, state := range g.Keys {
		g.Keys[key] = state.Finalized(g.Algorithm)
		g.Bounds[key] = state.AbsErrorBound(g.Algorithm)
	}
}

func (g Group) EventCount() int64 {
	var count int64
	for _, key := range g.sortedKeys() {
		count += g.Keys[key].Count
	}
	return count
}

func (g Group) SumAbs() float64 {
	var total float64
	for _, key := range g.sortedKeys() {
		total += g.Keys[key].SumAbs
	}
	return total
}

func (g Group) TotalErrorBound() float64 {
	var total float64
	for _, key := range g.sortedKeys() {
		state := g.Keys[key]
		if bound, ok := g.Bounds[key]; ok {
			total += bound
		} else {
			total += state.AbsErrorBound(g.Algorithm)
		}
	}
	return total
}

func (g Group) sortedKeys() []string {
	keys := make([]string, 0, len(g.Keys))
	for key := range g.Keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (g Group) Digest() string {
	g.Finalize()
	data, _ := json.Marshal(struct {
		Algorithm Algorithm          `json:"algorithm"`
		Keys      map[string]State   `json:"keys"`
		Bounds    map[string]float64 `json:"absolute_error_bounds"`
	}{
		Algorithm: g.Algorithm,
		Keys:      g.Keys,
		Bounds:    g.Bounds,
	})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func EncodeState(state State) (string, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func DecodeState(data string) (State, error) {
	var state State
	if data == "" {
		return state, nil
	}
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return State{}, err
	}
	return state, nil
}

func kahanAdd(sum, compensation *float64, value float64) {
	y := value - *compensation
	t := *sum + y
	*compensation = (t - *sum) - y
	*sum = t
}

func bucketKey(value float64) string {
	abs := math.Abs(value)
	if abs == 0 {
		return "-32768"
	}
	exponent := math.Ilogb(abs)
	if exponent < -32768 {
		exponent = -32768
	}
	if exponent > 32767 {
		exponent = 32767
	}
	return strconv.Itoa(exponent)
}

func gamma(n int64) float64 {
	if n <= 0 {
		return 0
	}
	nu := float64(n) * UnitRoundoff
	if nu >= 1 {
		return math.MaxFloat64
	}
	return nu / (1 - nu)
}

// kahanBound is the standard Kahan forward-error form
// (2u + O(n*u^2))*sum_abs. The gamma(n)*u term makes the O-term explicit.
func kahanBound(n int64) float64 {
	bound := 2*UnitRoundoff + gamma(n)*UnitRoundoff
	if math.IsInf(bound, 0) || math.IsNaN(bound) {
		return math.MaxFloat64
	}
	return bound
}

// l1UpperBound accounts for the rounding error in the stored floating-point
// sum_abs, so error declarations remain upper bounds relative to the exact L1
// magnitude of the inputs.
func l1UpperBound(n int64, sumAbs float64) float64 {
	rounding := gamma(n)
	if rounding >= 1 {
		return math.MaxFloat64
	}
	value := sumAbs / (1 - rounding)
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return math.MaxFloat64
	}
	return value
}
