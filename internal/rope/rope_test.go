package rope_test

import (
	"math"
	"reflect"
	"testing"

	"ropechunk/internal/model"
	"ropechunk/internal/rope"
)

func testConfig(mode model.ScalingMode) model.Config {
	return model.Config{
		SnapshotVersion:    1,
		BaseFrequency:      10_000,
		ScalingMode:        mode,
		ScaleFactor:        2,
		AngleBlockSize:     4,
		CausalMask:         true,
		RowBlockSize:       2,
		KeyBlockSize:       2,
		MaxConcurrentBlock: 2,
		WorkerBlockQuota:   4,
		RoundTimeBudgetMS:  1000,
		PeakMemoryBytes:    8 << 20,
	}
}

func TestReferenceRotationMatchesCore(t *testing.T) {
	data := make([]int64, 4*9)
	for i := range data {
		data[i] = int64(i*271_828-3_000_000) + int64((i%7)-3)*111_111
	}
	data[7] = 8_000_000
	data[8] = -9_000_000
	shape := model.Shape{1, 1, 4, 9}
	src := model.QuantTensor{Shape: shape, Scale: 1_000_000, Data: data}
	for _, mode := range []model.ScalingMode{model.ScaleNone, model.ScaleLinear, model.ScaleNTKPart} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := testConfig(mode)
			got, blocks, err := rope.RotateTensor(src, cfg)
			if err != nil {
				t.Fatal(err)
			}
			want, err := rope.ReferenceRotate(src, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				for i := range got.Data {
					if got.Data[i] != want.Data[i] {
						t.Fatalf("element %d: got %d want %d", i, got.Data[i], want.Data[i])
					}
				}
			}
			gotAgain, _, err := rope.RotateTensor(src, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if model.TensorDigest(got) != model.TensorDigest(gotAgain) {
				t.Fatal("rotation is not deterministic")
			}
			if len(blocks) == 0 {
				t.Fatal("missing angle summaries")
			}
			if len(blocks[0].UnrotatedDim) != 1 || blocks[0].UnrotatedDim[0] != 8 {
				t.Fatalf("odd dimension not recorded: %v", blocks[0].UnrotatedDim)
			}
			for b := 0; b < shape.Batch(); b++ {
				for h := 0; h < shape.Heads(); h++ {
					for row := 0; row < shape.Seq(); row++ {
						idx := src.Index(b, h, row, 8)
						if got.Data[idx] != src.Data[idx] {
							t.Fatal("last odd dimension was rotated")
						}
					}
				}
			}
		})
	}
}

func TestInvFrequencySegments(t *testing.T) {
	cfg := testConfig(model.ScaleNTKPart)
	base := cfg.BaseFrequency
	split := 8 / 8
	if got, want := rope.InvFrequency(0, 8, cfg), 1.0; math.Abs(got-want) > 1e-15 {
		t.Fatalf("high-frequency segment: got %v want %v", got, want)
	}
	adjusted := base * math.Pow(cfg.ScaleFactor, 8.0/6.0)
	want := math.Pow(adjusted, -2.0*1.0/8.0)
	if got := rope.InvFrequency(split, 8, cfg); math.Abs(got-want) > 1e-15 {
		t.Fatalf("ntk segment: got %v want %v", got, want)
	}
}

func TestOnlineSoftmaxMatchesNaive(t *testing.T) {
	cfg := testConfig(model.ScaleNone)
	src := model.QuantTensor{Shape: model.Shape{1, 1, 5, 4}, Scale: 1_000_000, Data: []int64{
		100_000, 200_000, -300_000, 400_000,
		-200_000, 500_000, 100_000, -100_000,
		300_000, -100_000, 250_000, 150_000,
		50_000, 70_000, -90_000, 110_000,
		-400_000, 220_000, 330_000, -120_000,
	}}
	rot, _, err := rope.RotateTensor(src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	headDim := src.Shape.HeadDim()
	q := make([]float64, headDim)
	for d := range q {
		q[d] = rot.FloatAt(rot.Index(0, 0, 2, d))
	}
	k := make([]float64, src.Shape.Seq()*headDim)
	for row := 0; row < src.Shape.Seq(); row++ {
		for d := 0; d < headDim; d++ {
			k[row*headDim+d] = rot.FloatAt(rot.Index(0, 0, row, d))
		}
	}
	state := rope.NewSoftmaxState(headDim)
	for start := 0; start < src.Shape.Seq(); start += cfg.KeyBlockSize {
		end := min(start+cfg.KeyBlockSize, src.Shape.Seq())
		if err := rope.ConsumeKeyRange(q, k, src.Shape.Seq(), headDim, 2, true, start, end, &state, 0, start/cfg.KeyBlockSize, "t"); err != nil {
			t.Fatal(err)
		}
	}
	got, err := rope.FinalizeAttentionRow(q, k, src.Shape.Seq(), headDim, 2, true, state, "t", 0)
	if err != nil {
		t.Fatal(err)
	}
	wantMax := math.Inf(-1)
	var dots []float64
	for key := 0; key <= 2; key++ {
		dot := 0.0
		for d := 0; d < headDim; d++ {
			dot += q[d] * k[key*headDim+d]
		}
		dot /= math.Sqrt(float64(headDim))
		dots = append(dots, dot)
		wantMax = math.Max(wantMax, dot)
	}
	wantSum := 0.0
	for _, dot := range dots {
		wantSum += math.Exp(dot - wantMax)
	}
	if math.Abs(got.MaxScore-wantMax) > 1e-14 || math.Abs(got.ExpSum-wantSum) > 1e-13 {
		t.Fatalf("got max/sum %v/%v want %v/%v", got.MaxScore, got.ExpSum, wantMax, wantSum)
	}
	if got.AllowedKeyStart != 0 || got.AllowedKeyEnd != 3 {
		t.Fatalf("causal range: got [%d,%d)", got.AllowedKeyStart, got.AllowedKeyEnd)
	}
}

func TestNumericErrorIncludesCoordinates(t *testing.T) {
	state := rope.NewSoftmaxState(2)
	q := []float64{math.Inf(1), 0}
	k := []float64{1, 1}
	err := rope.ConsumeKeyRange(q, k, 1, 2, 0, false, 0, 1, &state, 7, 3, "bad-shard")
	app, ok := model.AsAppError(err)
	if !ok || app.Code != model.CodeNumeric || app.ShardID != "bad-shard" || app.RowBlock != 7 || app.ColBlock != 3 {
		t.Fatalf("unexpected error: %#v", err)
	}
}

func TestZeroNormalizationErrorIncludesCoordinates(t *testing.T) {
	state := rope.NewSoftmaxState(2)
	state.Max = 0
	state.SumExp = 0
	state.Weighted = []float64{0, 0}
	_, err := rope.FinalizeAttentionRow([]float64{1, 0}, []float64{1, 0}, 1, 2, 0, false, state, "zero-shard", 4)
	app, ok := model.AsAppError(err)
	if !ok || app.Code != model.CodeNumeric || app.RowBlock != 4 || app.ShardID != "zero-shard" {
		t.Fatalf("unexpected zero normalization error: %#v", err)
	}
}
