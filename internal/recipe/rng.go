package recipe

// RNG is a deterministic splitmix64 pseudo-random source. It is intentionally
// self-contained so generated samples are reproducible bit-for-bit on every
// platform and Go version.
type RNG struct {
	state uint64
}

// NewRNG seeds a splitmix64 stream.
func NewRNG(seed int64) *RNG {
	return &RNG{state: uint64(seed)}
}

// Uint64 returns the next pseudo-random uint64.
func (r *RNG) Uint64() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a deterministic value in [0, n).
func (r *RNG) Intn(n int) int {
	if n <= 0 {
		panic("recipe: Intn bound must be positive")
	}
	return int(r.Uint64() % uint64(n))
}

// Float64 returns a deterministic value in [0, 1).
func (r *RNG) Float64() float64 {
	return float64(r.Uint64()>>11) / float64(uint64(1)<<53)
}

// IntRange returns a deterministic int in [lo, hi].
func (r *RNG) IntRange(lo, hi int) int {
	return lo + r.Intn(hi-lo+1)
}
