// Package noise implements deterministic tileable value noise with per-octave
// synthesis, Kahan compensated accumulation, big.Float high-precision reference
// error bounds and perturbation replay attribution.
package noise

import (
	"math"
	"runtime"
	"sync"

	"matbench/internal/fp"
	"matbench/internal/recipe"
)

// FieldSize is the fixed internal synthesis resolution; target-resolution
// resampling happens in the palette stage.
const FieldSize = 64

// refGrid is the side length of the explicit high-precision reference grid.
const refGrid = 16

const bigPrec = 96 // bits; ~29 decimal digits, well above float64's ~16

// OctaveDiag records per-octave numerical diagnostics.
type OctaveDiag struct {
	Index           int     `json:"index"`
	Girth           int     `json:"girth"`
	RawAmplitude    float64 `json:"raw_amplitude"`
	NormAmplitude   float64 `json:"norm_amplitude"`
	LogContribution float64 `json:"log_contribution"`
	ErrorBound      float64 `json:"error_bound"`
	CumulativeBound float64 `json:"cumulative_bound"`
	RefMaxDelta     float64 `json:"ref_max_delta"`
	TheoryBound     float64 `json:"theory_bound"`
	PerturbL1       float64 `json:"perturb_l1"`
	ContribRank     int     `json:"contrib_rank"`
	PerturbRank     int     `json:"perturb_rank"`
	ReviewFlag      bool    `json:"review_flag"`
}

// PerturbInfo stores the two attribution orderings with explicit evidence.
type PerturbInfo struct {
	OctaveIndex int     `json:"octave_index"`
	LogBasis    float64 `json:"log_basis"`
	PerturbL1   float64 `json:"perturb_l1"`
}

// Result is the stage-2 artifact.
type Result struct {
	MaterialID     string        `json:"material_id"`
	Field          []float64     `json:"field"`
	Width          int           `json:"width"`
	Height         int           `json:"height"`
	Degenerate     bool          `json:"degenerate"`
	DegenerateNote string        `json:"degenerate_note,omitempty"`
	Octaves        []OctaveDiag  `json:"octaves"`
	PerturbReplay  []PerturbInfo `json:"perturb_replay"`
	ReviewOctaves  []int         `json:"review_octaves"`
	Fingerprint    string        `json:"fingerprint"`
}

type octaveLayer struct {
	index   int
	girth   int
	grid    []float64
	rawAmp  float64
	normAmp float64
}

func lattice(seed int64, octave, gx, gy, girth int) float64 {
	h := uint64(seed)
	mix := func(x uint64) uint64 {
		x += 0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		return x ^ (x >> 31)
	}
	h = mix(h ^ uint64(octave)*0x9e3779b97f4a7c15)
	h = mix(h ^ uint64(gx)*0xa24baed491ba200f)
	h = mix(h ^ uint64(gy)*0xc3a5c85c97cb3127)
	h = mix(h ^ uint64(girth)*0xb492b66fbe98f273)
	return float64(h>>11) / float64(uint64(1)<<53)
}

func smooth(t float64) float64 { return t * t * (3 - 2*t) }

func sampleLayer(l *octaveLayer, px, py int) float64 {
	g := l.girth
	fx := float64(px) * float64(g) / float64(FieldSize)
	fy := float64(py) * float64(g) / float64(FieldSize)
	x0 := int(math.Floor(fx))
	y0 := int(math.Floor(fy))
	tx := smooth(fx - float64(x0))
	ty := smooth(fy - float64(y0))
	at := func(gx, gy int) float64 {
		return l.grid[(gy%g)*g+(gx%g)]
	}
	top := at(x0, y0)*(1-tx) + at(x0+1, y0)*tx
	bot := at(x0, y0+1)*(1-tx) + at(x0+1, y0+1)*tx
	return top*(1-ty) + bot*ty
}

func buildLayer(r recipe.Recipe, idx int) *octaveLayer {
	p := r.Noise
	girth := int(math.Round(p.Frequency * math.Pow(2, float64(idx))))
	if girth < 1 {
		girth = 1
	}
	grid := make([]float64, girth*girth)
	for gy := 0; gy < girth; gy++ {
		for gx := 0; gx < girth; gx++ {
			grid[gy*girth+gx] = lattice(p.Seed, idx, gx, gy, girth)
		}
	}
	raw := 0.0
	if idx < len(p.Amplitudes) {
		raw = p.Amplitudes[idx]
	}
	return &octaveLayer{index: idx, girth: girth, grid: grid, rawAmp: raw}
}

// Synthesize runs octave synthesis. workers > 1 builds layers concurrently;
// accumulation order is fixed so both execution paths are byte identical.
func Synthesize(r recipe.Recipe, workers int) (*Result, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	p := r.Noise
	degenerate := false
	note := ""
	if p.Octaves <= 0 {
		degenerate = true
		note = "octaves is zero"
	}
	ampSum := 0.0
	for _, a := range p.Amplitudes {
		ampSum += math.Abs(a)
	}
	allZero := p.Octaves > 0 && ampSum == 0
	if allZero {
		degenerate = true
		note = "all octave amplitudes are zero"
	}

	layers := make([]*octaveLayer, p.Octaves)
	if workers > 1 && p.Octaves > 1 {
		var wg sync.WaitGroup
		sem := make(chan struct{}, workers)
		for i := 0; i < p.Octaves; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(idx int) {
				defer wg.Done()
				defer func() { <-sem }()
				layers[idx] = buildLayer(r, idx)
			}(i)
		}
		wg.Wait()
	} else {
		for i := 0; i < p.Octaves; i++ {
			layers[i] = buildLayer(r, i)
		}
	}

	norm := make([]float64, p.Octaves)
	total := 0.0
	for _, l := range layers {
		total += math.Abs(l.rawAmp)
	}
	if total > 0 {
		for i, l := range layers {
			norm[i] = math.Abs(l.rawAmp) / total
			l.normAmp = norm[i]
		}
	}

	n := FieldSize * FieldSize
	field := make([]float64, n)
	kahan := make([]float64, n)
	for _, l := range layers {
		if l.normAmp == 0 {
			continue
		}
		for py := 0; py < FieldSize; py++ {
			for px := 0; px < FieldSize; px++ {
				k := py*FieldSize + px
				v := l.normAmp * sampleLayer(l, px, py)
				y := v - kahan[k]
				t := field[k] + y
				kahan[k] = (t - field[k]) - y
				field[k] = t
			}
		}
	}

	diags, err := diagnose(r, layers, field)
	if err != nil {
		return nil, err
	}
	finger, err := fpFingerprint(r, field, diags)
	if err != nil {
		return nil, err
	}
	return &Result{
		MaterialID:     r.MaterialID,
		Field:          field,
		Width:          FieldSize,
		Height:         FieldSize,
		Degenerate:     degenerate,
		DegenerateNote: note,
		Octaves:        diags,
		PerturbReplay:  perturbList(diags),
		ReviewOctaves:  reviewList(diags),
		Fingerprint:    finger,
	}, nil
}

func reviewList(d []OctaveDiag) []int {
	var out []int
	for _, o := range d {
		if o.ReviewFlag {
			out = append(out, o.Index)
		}
	}
	return out
}

func perturbList(d []OctaveDiag) []PerturbInfo {
	out := make([]PerturbInfo, len(d))
	for i, o := range d {
		out[i] = PerturbInfo{OctaveIndex: o.Index, LogBasis: o.LogContribution, PerturbL1: o.PerturbL1}
	}
	return out
}

func fpFingerprint(r recipe.Recipe, field []float64, diags []OctaveDiag) (string, error) {
	return fp.Of("stage:noise", map[string]any{
		"material_id": r.MaterialID,
		"noise":       r.Noise,
		"field":       field,
		"octaves":     diags,
	})
}
