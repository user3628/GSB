package quant

import (
	"math"
	"strings"

	"imretrace/internal/model"
)

// SemanticTarget is the transparent shared coordinate system used by the
// deterministic alignment matrix. Axes are colors, shapes and illumination.
func SemanticTarget(caption string, dim int) []float64 {
	y := make([]float64, dim)
	s := strings.ToLower(caption)
	set := func(i int, keys ...string) {
		if i >= dim {
			return
		}
		for _, key := range keys {
			if strings.Contains(s, strings.ToLower(key)) {
				y[i] = 1
				return
			}
		}
	}
	// Color axes 0..8.
	set(0, "red", "红")
	set(1, "blue", "蓝")
	set(2, "green", "绿")
	set(3, "yellow", "黄")
	set(4, "orange", "橙")
	set(5, "purple", "紫")
	set(6, "teal", "cyan", "青", "emerald")
	set(7, "gray", "grey", "灰白", "black", "黑", "白")
	set(8, "rose", "玫")
	// Shape axes 9..13.
	set(9, "circle", "圆", "disc", "disk")
	set(10, "rectangle", "rect", "矩形", "block", "方块")
	set(11, "triangle", "三角")
	set(12, "bar")
	set(13, "ring", "圆环")
	// Illumination axes 14..15.
	set(14, "bright", "light", "明", "亮")
	set(15, "dark", "deep", "暗", "深色")
	// Remaining declared projection axes carry a small shared bias so the
	// deterministic matrix has no dead output coordinate. Calibration still
	// records and rejects an actually zero scale if it occurs elsewhere.
	for i := 16; i < dim; i++ {
		y[i] = 0.125
	}
	return y
}

func FitAlignmentMatrix(p model.Policy, captions []string, visualX, textX [][]float64) (*Matrix, error) {
	n := len(captions)
	if n == 0 || len(visualX) != n || len(textX) != n {
		return nil, model.NewBiz(model.CodeQuantInvalid, model.StageProjectQuant, "corpus", "alignment fit requires equal non-empty sample and feature counts")
	}
	d := p.ProjectionDim
	y := make([][]float64, n)
	for i, caption := range captions {
		y[i] = SemanticTarget(caption, d)
	}
	wv, err := ridgeWeights(visualX, y)
	if err != nil {
		return nil, err
	}
	wt, err := ridgeWeights(textX, y)
	if err != nil {
		return nil, err
	}
	for r := range wv {
		for c := range wv[r] {
			wv[r][c] = round12(wv[r][c])
		}
	}
	for r := range wt {
		for c := range wt[r] {
			wt[r][c] = round12(wt[r][c])
		}
	}
	m := &Matrix{Seed: p.ProjectionSeed, OutputDim: d, VisualInputDim: model.VisualDim(p.Grid), TextInputDim: model.TextDim, Visual: wv, Text: wt}
	m.Digest = model.HashBytes("alignment-matrix-v1:", model.MustCanonical(struct {
		Seed int64       `json:"seed"`
		V    [][]float64 `json:"visual"`
		T    [][]float64 `json:"text"`
	}{m.Seed, m.Visual, m.Text}))
	return m, nil
}

// ridgeWeights returns W with shape outputs x inputs such that X W^T ~= Y.
// The kernel form keeps the deterministic demo small even when the feature
// dimension is larger than the sample count.
func ridgeWeights(x, y [][]float64) ([][]float64, error) {
	n := len(x)
	inputDim := len(x[0])
	outDim := len(y[0])
	k := make([][]float64, n)
	diagSum := 0.0
	for i := 0; i < n; i++ {
		if len(x[i]) != inputDim {
			return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, "corpus", i, "inconsistent input dimension")
		}
		k[i] = make([]float64, n)
		for j := 0; j <= i; j++ {
			dot := 0.0
			for d := 0; d < inputDim; d++ {
				dot += x[i][d] * x[j][d]
			}
			k[i][j], k[j][i] = dot, dot
			if i == j {
				diagSum += dot
			}
		}
	}
	lambda := math.Max(1e-6, 1e-3*diagSum/float64(n))
	for i := 0; i < n; i++ {
		k[i][i] += lambda
	}
	alpha, err := solveMany(k, y)
	if err != nil {
		return nil, err
	}
	w := make([][]float64, outDim)
	for o := 0; o < outDim; o++ {
		w[o] = make([]float64, inputDim)
		for j := 0; j < inputDim; j++ {
			sum := 0.0
			for i := 0; i < n; i++ {
				sum += alpha[i][o] * x[i][j]
			}
			w[o][j] = sum
		}
	}
	return w, nil
}

func solveMany(a [][]float64, b [][]float64) ([][]float64, error) {
	n := len(a)
	m := len(b[0])
	mat := make([][]float64, n)
	for i := range a {
		mat[i] = append(append([]float64(nil), a[i]...), b[i]...)
	}
	for col := 0; col < n; col++ {
		pivot := col
		for r := col + 1; r < n; r++ {
			if math.Abs(mat[r][col]) > math.Abs(mat[pivot][col]) {
				pivot = r
			}
		}
		if math.Abs(mat[pivot][col]) < 1e-14 {
			return nil, model.DimError(model.CodeQuantInvalid, model.StageProjectQuant, "corpus", col, "singular alignment kernel")
		}
		mat[col], mat[pivot] = mat[pivot], mat[col]
		pv := mat[col][col]
		for j := col; j < n+m; j++ {
			mat[col][j] /= pv
		}
		for r := 0; r < n; r++ {
			if r == col {
				continue
			}
			factor := mat[r][col]
			if factor == 0 {
				continue
			}
			for j := col; j < n+m; j++ {
				mat[r][j] -= factor * mat[col][j]
			}
		}
	}
	out := make([][]float64, n)
	for i := 0; i < n; i++ {
		out[i] = append([]float64(nil), mat[i][n:]...)
	}
	return out, nil
}

// HandcraftedMatrix remains available for dimension checks and offline tests.
func HandcraftedMatrix(p model.Policy) *Matrix { return DeriveMatrix(p) }
