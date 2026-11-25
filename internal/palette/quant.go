package palette

import (
	"image"
	"image/color"
	"image/png"
	"math"
	"os"
	"path/filepath"

	"matbench/internal/control"
	"matbench/internal/errorsx"
	"matbench/internal/fp"
	"matbench/internal/noise"
	"matbench/internal/recipe"
)

// ParamError records the effective decimal digits and explicit error upper
// bound of one material parameter after the palette-to-quantization chain.
type ParamError struct {
	Name       string  `json:"name"`
	Value      float64 `json:"value"`
	ErrorBound float64 `json:"error_bound"`
	Digits     int     `json:"effective_digits"`
}

// Result is the stage-3 artifact.
type Result struct {
	MaterialID  string       `json:"material_id"`
	Width       int          `json:"width"`
	Height      int          `json:"height"`
	PaletteSize int          `json:"palette_size"`
	Params      []ParamError `json:"params"`
	TexturePath string       `json:"texture_path"`
	Fingerprint string       `json:"fingerprint"`
}

// Modulate applies the unit control vector to the noise field: each pixel is
// scaled by a deterministic scalar derived from a fixed component lookup.
func Modulate(field []float64, vec [control.VectorDim]float64) []float64 {
	out := make([]float64, len(field))
	for i, g := range field {
		d := i % control.VectorDim
		s := 0.5 + 0.5*vec[d] // deterministic scalar in (0, 1]
		v := g * s
		if v < 0 {
			v = 0
		}
		if v > 1 {
			v = 1
		}
		out[i] = v
	}
	return out
}

func resample(field []float64, srcW, srcH, dstW, dstH int) []float64 {
	out := make([]float64, dstW*dstH)
	for y := 0; y < dstH; y++ {
		sy := float64(y) * float64(srcH) / float64(dstH)
		y0 := int(math.Floor(sy))
		ty := sy - float64(y0)
		for x := 0; x < dstW; x++ {
			sx := float64(x) * float64(srcW) / float64(dstW)
			x0 := int(math.Floor(sx))
			tx := sx - float64(x0)
			v00 := field[(y0%srcH)*srcW+(x0%srcW)]
			v10 := field[(y0%srcH)*srcW+((x0+1)%srcW)]
			v01 := field[((y0+1)%srcH)*srcW+(x0%srcW)]
			v11 := field[((y0+1)%srcH)*srcW+((x0+1)%srcW)]
			top := v00*(1-tx) + v10*tx
			bot := v01*(1-tx) + v11*tx
			out[y*dstW+x] = top*(1-ty) + bot*ty
		}
	}
	return out
}

func nearest(r recipe.Recipe, pal []RGB, g float64) RGB {
	br := float64(r.BaseColor[0]) * g
	bg := float64(r.BaseColor[1]) * g
	bb := float64(r.BaseColor[2]) * g
	best := pal[0]
	bestD := math.Inf(1)
	for _, c := range pal {
		dr := float64(c.R) - br
		dg := float64(c.G) - bg
		db := float64(c.B) - bb
		d := dr*dr + dg*dg + db*db
		if d < bestD {
			bestD = d
			best = c
		}
	}
	return best
}

// Quantize runs palette matching, resampling and 8-bit quantization and writes
// the texture PNG plus the parameter diagnostics.
func Quantize(r recipe.Recipe, card *image.RGBA, cardSpec recipe.CardSpec, nr *noise.Result,
	cr *control.Result, outDir string) (*Result, error) {
	pal := ExtractPalette(card)
	if len(pal) == 0 {
		return nil, errorsx.New(errorsx.EPaletteEmpty, r.MaterialID,
			"stage palette: reference card yielded an empty palette")
	}
	mod := Modulate(nr.Field, cr.Vector)
	res := resample(mod, nr.Width, nr.Height, r.TargetResolution, r.TargetResolution)
	img := image.NewRGBA(image.Rect(0, 0, r.TargetResolution, r.TargetResolution))
	allZero := true
	for i, g := range res {
		c := nearest(r, pal, g)
		if c.R != 0 || c.G != 0 || c.B != 0 {
			allZero = false
		}
		img.SetRGBA(i%r.TargetResolution, i/r.TargetResolution, color.RGBA{
			R: c.R, G: c.G, B: c.B, A: 255,
		})
	}
	if allZero {
		return nil, errorsx.New(errorsx.EQuantAllZero, r.MaterialID,
			"stage palette: quantized texture is all-zero on every channel")
	}
	textureDir := filepath.Join(outDir, "artifacts", "textures")
	if err := os.MkdirAll(textureDir, 0o755); err != nil {
		return nil, err
	}
	texPath := filepath.Join(textureDir, r.MaterialID+"_texture.png")
	f, err := os.Create(texPath)
	if err != nil {
		return nil, err
	}
	if err := png.Encode(f, img); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	params := materialParams(r, nr)
	finger, err := fp.Of("stage:palette", map[string]any{
		"material_id":       r.MaterialID,
		"card":              cardSpec,
		"target_resolution": r.TargetResolution,
		"params":            params,
		"palette_size":      len(pal),
	})
	if err != nil {
		return nil, err
	}
	return &Result{
		MaterialID:  r.MaterialID,
		Width:       r.TargetResolution,
		Height:      r.TargetResolution,
		PaletteSize: len(pal),
		Params:      params,
		TexturePath: filepath.ToSlash(filepath.Join("artifacts", "textures",
			r.MaterialID+"_texture.png")),
		Fingerprint: finger,
	}, nil
}

// materialParams derives quantized material parameters with explicit bounds.
// Each parameter is rounded to 1/256 (the 8-bit texture granularity); the bound
// is the quantization half step plus the propagated noise error bound.
func materialParams(r recipe.Recipe, nr *noise.Result) []ParamError {
	noiseBound := 0.0
	if len(nr.Octaves) > 0 {
		noiseBound = nr.Octaves[len(nr.Octaves)-1].CumulativeBound
	}
	qStep := 1.0 / 256.0
	mk := func(name string, v float64) ParamError {
		q := math.Round(v*256) / 256
		bound := math.Abs(q-v) + qStep/2 + noiseBound
		digits := 0
		if bound > 0 && bound < 1 {
			digits = int(math.Floor(-math.Log10(bound)))
			if digits < 0 {
				digits = 0
			}
		}
		return ParamError{Name: name, Value: q, ErrorBound: bound, Digits: digits}
	}
	return []ParamError{
		mk("roughness", r.Roughness),
		mk("metallic", r.Metallic),
		mk("base_r", float64(r.BaseColor[0])/255),
		mk("base_g", float64(r.BaseColor[1])/255),
		mk("base_b", float64(r.BaseColor[2])/255),
	}
}
