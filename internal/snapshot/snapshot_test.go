package snapshot_test

import (
	"reflect"
	"testing"

	"columnar-executor/internal/model"
	"columnar-executor/internal/snapshot"
)

func TestGenerateIsDeterministicAndValid(t *testing.T) {
	first, err := snapshot.Generate(7, 257, 20240517)
	if err != nil {
		t.Fatal(err)
	}
	second, err := snapshot.Generate(7, 257, 20240517)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("generator output is not deterministic")
	}
	if err := snapshot.Validate(first); err != nil {
		t.Fatal(err)
	}
	if first.Selections[model.FormAll].Count() != 257 {
		t.Fatal("all bitmap count mismatch")
	}
	if first.Selections[model.FormNone].Count() != 0 {
		t.Fatal("none bitmap count mismatch")
	}
	if first.Selections[model.FormSparse].Count() >= first.Selections[model.FormDense].Count() {
		t.Fatal("sparse selection should be smaller than dense selection")
	}
}

func TestValidateRejectsCorruption(t *testing.T) {
	base, err := snapshot.Generate(1, 64, 99)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("null bitmap length", func(t *testing.T) {
		bad := base.Clone()
		bad.Columns[1].Nulls = bad.Columns[1].Nulls[:len(bad.Columns[1].Nulls)-1]
		assertCode(t, snapshot.Validate(bad), model.CodeSnapshotInvalid)
	})

	t.Run("dictionary index", func(t *testing.T) {
		bad := base.Clone()
		bad.Columns[4].DictIndices[0] = 999
		bad.Columns[4].Nulls[0] &= ^byte(1)
		assertCode(t, snapshot.Validate(bad), model.CodeSnapshotInvalid)
	})

	t.Run("digest", func(t *testing.T) {
		bad := base.Clone()
		bad.Columns[1].IntValues[0]++
		assertCode(t, snapshot.Validate(bad), model.CodeSnapshotInvalid)
	})
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s", code)
	}
	biz, ok := model.AsBizError(err)
	if !ok || biz.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}
