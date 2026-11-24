package motion

// splitMix64 is deliberately implemented here instead of using math/rand so
// generated data remains stable across Go releases and platforms.
type splitMix64 struct {
	state uint64
}

func newPRNG(seed int64) *splitMix64 {
	return &splitMix64{state: uint64(seed) ^ 0x9e3779b97f4a7c15}
}

func (r *splitMix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *splitMix64) intn(n int) int {
	if n <= 1 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func (r *splitMix64) float64() float64 {
	return float64(r.next()>>11) / float64(1<<53)
}
