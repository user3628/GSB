package cockpitaudio

type DeterministicRNG struct {
	state uint64
}

func NewDeterministicRNG(seed int64) *DeterministicRNG {
	return &DeterministicRNG{state: uint64(seed) + 0x9E3779B97F4A7C15}
}

func (r *DeterministicRNG) Uint64() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *DeterministicRNG) Float64() float64 {
	return float64(r.Uint64()>>11) / float64(1<<53)
}

func (r *DeterministicRNG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}
