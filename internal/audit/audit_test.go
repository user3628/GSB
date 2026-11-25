package audit_test

import (
	"math"
	"testing"

	"flashattention/internal/attn"
	"flashattention/internal/audit"
	"flashattention/internal/errs"
	"flashattention/internal/reference"
	"flashattention/internal/tensor"
)

func TestAuditPassesAndDetectsTampering(t *testing.T) {
	sh := tensor.Shape{B: 1, H: 1, S: 12, D: 5}
	q := tensor.Deterministic(sh, 1, -1, 1)
	k := tensor.Deterministic(sh, 2, -1, 1)
	v := tensor.Deterministic(sh, 3, -1, 1)
	cfg := attn.Config{ShardID: "a", RowBlock: 4, ColBlock: 3, GroupBlocks: 1, Scale: 1 / math.Sqrt(5), Strategy: attn.Scaled}
	kn, err := attn.NewKernel(cfg, q, k, v)
	if err != nil {
		t.Fatal(err)
	}
	res, err := kn.Run(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref := reference.Compute(q, k, v, false, cfg.Scale, 24, sh.S)
	env := audit.Envelope{QMax: 1, KMax: 1, VMax: 1}
	set, err := audit.Audit("a", cfg, sh, res.Output, ref, env)
	if err != nil || !set.OK {
		t.Fatalf("audit should pass: %v", err)
	}

	tampered := res.Output.Clone()
	tampered.Data[tampered.Offset(0, 0, 2, 1)] += 0.5
	_, err = audit.Audit("a", cfg, sh, tampered, ref, env)
	if errs.Code(err) != errs.EErrorBound {
		t.Fatalf("expected E_ERROR_BOUND, got %v", err)
	}
	ce := errs.As(err)
	if ce.Fields["shard"] != "a" || ce.Fields["row_block"] != 0 {
		t.Fatalf("missing fields %+v", ce.Fields)
	}
	if _, ok := ce.Fields["max_abs_err"]; !ok {
		t.Fatal("missing max_abs_err")
	}
}
