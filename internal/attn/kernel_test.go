package attn_test

import (
	"context"
	"math"
	"testing"

	"flashattention/internal/attn"
	"flashattention/internal/reference"
	"flashattention/internal/tensor"
)

func TestKernelMatchesReferenceAndDeterministic(t *testing.T) {
	cases := []struct {
		name     string
		sh       tensor.Shape
		rb, cb   int
		causal   bool
		strategy attn.Strategy
	}{
		{"short", tensor.Shape{B: 2, H: 2, S: 16, D: 8}, 4, 4, false, attn.Scaled},
		{"odd_dim", tensor.Shape{B: 1, H: 1, S: 37, D: 13}, 8, 7, false, attn.Scaled},
		{"causal_logsum", tensor.Shape{B: 2, H: 2, S: 31, D: 17}, 5, 6, true, attn.LogSum},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := tensor.Deterministic(tc.sh, 123, -1, 1)
			k := tensor.Deterministic(tc.sh, 456, -1, 1)
			v := tensor.Deterministic(tc.sh, 789, -1, 1)
			cfg := attn.Config{
				ShardID: tc.name, RowBlock: tc.rb, ColBlock: tc.cb, GroupBlocks: 1,
				Causal: tc.causal, Scale: 1 / math.Sqrt(float64(tc.sh.D)), Strategy: tc.strategy,
			}
			kn, err := attn.NewKernel(cfg, q, k, v)
			if err != nil {
				t.Fatal(err)
			}
			r1, err := kn.Run(context.Background(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			r2, err := kn.Run(context.Background(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if h1, h2 := attn.HashFloat32(r1.Output.Data), attn.HashFloat32(r2.Output.Data); h1 != h2 {
				t.Fatalf("nondeterministic output hashes %s != %s", h1, h2)
			}
			if len(r1.Summaries) != len(r2.Summaries) {
				t.Fatalf("summary length changed %d != %d", len(r1.Summaries), len(r2.Summaries))
			}
			for i := range r1.Summaries {
				if r1.Summaries[i].StateHash != r2.Summaries[i].StateHash || r1.Summaries[i].OutputHash != r2.Summaries[i].OutputHash {
					t.Fatalf("intermediate summary %d is not byte deterministic", i)
				}
			}
			ref := reference.Compute(q, k, v, tc.causal, cfg.Scale, 32, tc.sh.S)
			want := ref.Dequantized()
			var maxAbs float64
			for i := range r1.Output.Data {
				maxAbs = math.Max(maxAbs, math.Abs(float64(r1.Output.Data[i])-want[i]))
			}
			if maxAbs > 2e-6 {
				t.Fatalf("max abs error %g exceeds float32 expectation", maxAbs)
			}
		})
	}
}

func TestNaNReturnsNumericError(t *testing.T) {
	sh := tensor.Shape{B: 1, H: 1, S: 4, D: 2}
	q := tensor.Deterministic(sh, 1, -1, 1)
	k := tensor.Deterministic(sh, 2, -1, 1)
	v := tensor.Deterministic(sh, 3, -1, 1)
	q.Data[0] = float32(math.NaN())
	_, err := attn.NewKernel(attn.Config{RowBlock: 2, ColBlock: 2, GroupBlocks: 1, Scale: 1}, q, k, v)
	if err == nil || err.Error() == "" {
		t.Fatal("expected E_NUMERIC error")
	}
}
