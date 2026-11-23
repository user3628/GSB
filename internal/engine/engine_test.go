package engine_test

import (
	"math"
	"testing"
	"time"

	"example.com/joinshuffle/internal/engine"
	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/sample"
	"example.com/joinshuffle/internal/snapshot"
)

func TestInnerAndLeftJoinSemantics(t *testing.T) {
	left, err := snapshot.New("left", 1, time.Unix(0, 0),
		[]int64{1, 1, 2, 3},
		[]float64{10, -4, 5, 0},
		[]bool{false, false, false, true})
	if err != nil {
		t.Fatal(err)
	}
	right, err := snapshot.New("right", 1, time.Unix(0, 0),
		[]int64{1, 2, 2, 4},
		[]float64{2, 3, 7, 9},
		[]bool{false, true, false, false})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := engine.Run(left, right, engine.Options{
		JoinType: model.JoinInner, PartitionCount: 2, SkewThreshold: 0.8,
		Algorithm: model.SumDoubleDoubleFix, FixedScaleBits: 80, MergeRule: engine.MergePartitionAscending,
	})
	if err != nil {
		t.Fatal(err)
	}
	innerGroups := engine.GroupMap(inner)
	if group := innerGroups[1]; group == nil || group.GroupCount != 2 || group.ValueCount != 2 || group.Sum == nil || *group.Sum != 10 {
		t.Fatalf("unexpected inner group key 1: %+v", group)
	}
	if group := innerGroups[2]; group == nil || group.GroupCount != 2 || group.ValueCount != 2 || group.Sum == nil || *group.Sum != 17 {
		t.Fatalf("unexpected inner group key 2: %+v", group)
	}
	if innerGroups[3] != nil || innerGroups[4] != nil {
		t.Fatal("inner join emitted unmatched keys")
	}

	leftJoin, err := engine.Run(left, right, engine.Options{
		JoinType: model.JoinLeft, PartitionCount: 2, SkewThreshold: 0.8,
		Algorithm: model.SumDoubleDoubleFix, FixedScaleBits: 80, MergeRule: engine.MergePartitionAscending,
	})
	if err != nil {
		t.Fatal(err)
	}
	leftGroups := engine.GroupMap(leftJoin)
	if group := leftGroups[3]; group == nil || group.GroupCount != 1 || group.ValueCount != 0 || group.Sum != nil || group.Average != nil {
		t.Fatalf("unexpected unmatched left-outer group: %+v", group)
	}
}

func TestPartitionCountIndependenceAndSkew(t *testing.T) {
	left, right, err := sample.GeneratePair(7, 200, 150, 1, 1, sample.DefaultGeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := engine.Run(left, right, engine.Options{
		JoinType: model.JoinLeft, PartitionCount: 1, SkewThreshold: 0.35,
		Algorithm: model.SumDoubleDoubleFix, FixedScaleBits: 80, MergeRule: engine.MergeMagnitudeBucket,
	})
	if err != nil {
		t.Fatal(err)
	}
	referenceGroups := engine.GroupMap(reference)
	for _, partitions := range []int{2, 4, 7} {
		result, err := engine.Run(left, right, engine.Options{
			JoinType: model.JoinLeft, PartitionCount: partitions, SkewThreshold: 0.35,
			Algorithm: model.SumDoubleDoubleFix, FixedScaleBits: 80, MergeRule: engine.MergeMagnitudeBucket,
		})
		if err != nil {
			t.Fatal(err)
		}
		for key, group := range engine.GroupMap(result) {
			ref := referenceGroups[key]
			if ref == nil || ref.Sum == nil || group.Sum == nil {
				continue
			}
			if math.Float64bits(*group.Sum) != math.Float64bits(*ref.Sum) {
				t.Fatalf("partition count %d changed key %d: got %v want %v", partitions, key, *group.Sum, *ref.Sum)
			}
		}
	}
	dist := engine.Analyze(left, right, 4, 0.35, model.JoinLeft)
	if len(dist.SkewedPartitions) == 0 {
		t.Fatalf("expected deterministic sample to expose skew: %+v", dist)
	}
}

func TestPartitionInvarianceRejectsExceedingTolerance(t *testing.T) {
	makeGroup := func(value float64) *engine.Result {
		return &engine.Result{Groups: []model.GroupResult{{Key: 9, Sum: &value, Average: &value}}}
	}
	groups := map[int]*engine.Result{1: makeGroup(100), 2: makeGroup(101)}
	_, _, businessErr := engine.ValidatePartitionInvariance(groups, 0.001)
	if businessErr == nil || businessErr.Code != model.CodePrecisionToleranceExceeded {
		t.Fatalf("expected precision error, got %+v", businessErr)
	}
	if businessErr.Context["key"] != int64(9) {
		t.Fatalf("precision error lacks key context: %+v", businessErr.Context)
	}
}

func TestPartitionInvarianceReportsSortedFirstKeyAndCountMismatch(t *testing.T) {
	makeResult := func(sumValue float64, count int64) *engine.Result {
		return &engine.Result{Groups: []model.GroupResult{
			{Key: 9, Sum: &sumValue, Average: &sumValue, GroupCount: count, ValueCount: count},
			{Key: 2, Sum: &sumValue, Average: &sumValue, GroupCount: count, ValueCount: count},
		}}
	}
	_, _, businessErr := engine.ValidatePartitionInvariance(map[int]*engine.Result{
		1: makeResult(100, 5),
		2: makeResult(101, 5),
	}, 0.001)
	if businessErr == nil || businessErr.Context["key"] != int64(2) {
		t.Fatalf("expected deterministic lowest failing key 2, got %+v", businessErr)
	}
	_, _, businessErr = engine.ValidatePartitionInvariance(map[int]*engine.Result{
		1: makeResult(100, 5),
		2: makeResult(100, 6),
	}, 1.0)
	if businessErr == nil || businessErr.Code != model.CodePrecisionToleranceExceeded {
		t.Fatalf("expected count mismatch to fail precision contract, got %+v", businessErr)
	}
}
