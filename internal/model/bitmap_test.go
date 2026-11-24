package model_test

import (
	"testing"

	"columnar-executor/internal/model"
)

func TestBitmapAndDoesNotMutateInputs(t *testing.T) {
	left := model.NewAll(70)
	right, err := model.BitmapFromRows(70, []int{0, 2, 64, 69})
	if err != nil {
		t.Fatal(err)
	}
	leftBefore := left.Clone()
	rightBefore := right.Clone()
	out, err := left.And(right)
	if err != nil {
		t.Fatal(err)
	}
	if !left.Equal(leftBefore) || !right.Equal(rightBefore) {
		t.Fatal("AND mutated an input bitmap")
	}
	if out.Count() != 4 {
		t.Fatalf("AND count = %d", out.Count())
	}
}

func TestBitmapMismatch(t *testing.T) {
	a := model.NewAll(10)
	b := model.NewAll(11)
	if _, err := a.And(b); err == nil {
		t.Fatal("expected bitmap mismatch")
	} else if biz, ok := model.AsBizError(err); !ok || biz.Code != model.CodeBitmapMismatch {
		t.Fatalf("wrong error: %v", err)
	}
	if model.NewNone(10).Count() != 0 {
		t.Fatal("empty selection should be valid")
	}
}

func TestAllBitmapForms(t *testing.T) {
	forms := []string{model.FormAll, model.FormNone, model.FormSparse, model.FormDense}
	for _, form := range forms {
		if !model.ValidBitmapForm(form) {
			t.Fatalf("form %s not recognized", form)
		}
	}
	all := model.NewAll(129)
	none := model.NewNone(129)
	if all.Count() != 129 || none.Count() != 0 {
		t.Fatal("all/none constructors failed")
	}
	if err := all.Validate(); err != nil {
		t.Fatal(err)
	}
}
