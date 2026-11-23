package sample

import (
	"math"
	"time"

	"example.com/joinshuffle/internal/snapshot"
)

var DefaultGeneratedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type rng struct {
	state uint64
}

func newRNG(seed uint64) *rng {
	return &rng{state: seed}
}

func (r *rng) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	x := r.state
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func GeneratePair(seed int64, leftRows, rightRows int, leftVersion, rightVersion uint64, generatedAt time.Time) (*snapshot.Snapshot, *snapshot.Snapshot, error) {
	left, err := generate("left", uint64(seed)^0x6c656674, leftRows, leftVersion, generatedAt)
	if err != nil {
		return nil, nil, err
	}
	right, err := generate("right", uint64(seed)^0x7269676874, rightRows, rightVersion, generatedAt)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}

func generate(side string, seed uint64, rows int, version uint64, generatedAt time.Time) (*snapshot.Snapshot, error) {
	if rows < 0 {
		rows = 0
	}
	r := newRNG(seed)
	keys := make([]int64, rows)
	measures := make([]float64, rows)
	nulls := make([]bool, rows)
	for i := 0; i < rows; i++ {
		r0 := r.next()
		if r0%100 < 34 {
			keys[i] = 17
		} else {
			keys[i] = int64(1 + (r0>>32)%20)
		}
		r1 := r.next()
		nulls[i] = r1%100 < 7
		if nulls[i] {
			measures[i] = 0
			continue
		}
		r2 := r.next()
		sign := 1.0
		if r2&1 == 1 {
			sign = -1
		}
		var magnitude float64
		switch {
		case i%131 == 0:
			magnitude = 1e15
		case i%17 == 0:
			magnitude = 1e-12
		default:
			frac := float64((r2>>11)&((1<<40)-1)) / float64(1<<40)
			exponent := int((r2>>53)%9) - 4
			magnitude = (0.5 + frac) * math.Pow10(exponent)
		}
		measures[i] = sign * magnitude
	}
	return snapshot.New(side, version, generatedAt.UTC(), keys, measures, nulls)
}
