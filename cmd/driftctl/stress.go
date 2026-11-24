package main

import (
	"flag"
	"fmt"
	"strconv"

	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/model"
	"driftconverge/internal/plan"
)

func runStress(args []string) error {
	fs := flag.NewFlagSet("stress", flag.ContinueOnError)
	count := fs.Int("count", 20000, "number of deterministic objects")
	fanout := fs.Int("fanout", 4, "dependencies per object (dense graph factor)")
	exactLimit := fs.Int("exact-limit", 5000, "exact path threshold")
	jsonOut := fs.Bool("json", false, "print summary JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *count < 1 || *count > 200000 {
		return fmt.Errorf("count must be in [1,200000]")
	}
	if *fanout < 1 || *fanout > 64 {
		return fmt.Errorf("fanout must be in [1,64]")
	}
	cluster, policy, err := generateStressCluster(*count, *fanout)
	if err != nil {
		return err
	}
	policy.ExactObjectLimit = *exactLimit
	policy.Checksum = policy.ComputeChecksum()
	values := &model.DriftValuesSample{Version: 1, Cases: []model.FieldValueCase{}}
	drift, err := detect.Analyze(cluster, policy, values)
	if err != nil {
		return err
	}
	batches, batchErr := batch.Organize(cluster, drift.Results, policy)
	if batches == nil {
		return batchErr
	}
	cost, planErr := plan.Build(cluster, drift, batches, policy)
	if planErr != nil {
		return planErr
	}
	exactBatches, _ := batch.Organize(cluster, drift.ExactResults, policy)
	bucketedBatches, _ := batch.Organize(cluster, drift.BucketedResults, policy)
	batchPathsReconciled := sameStressBatchPartition(exactBatches, bucketedBatches) && sameStressBatchPartition(batches, exactBatches)
	driftCount := 0
	for _, result := range drift.Results {
		if result.HasDrift {
			driftCount++
		}
	}
	summary := map[string]any{
		"objectCount": *count, "fanout": *fanout, "exactObjectLimit": *exactLimit,
		"selectedPath": drift.SelectedPath, "reconciled": drift.Reconciled, "objectsCompared": drift.ObjectsCompared,
		"exactDurationMicros": drift.ExactDurationUs, "bucketDurationMicros": drift.BucketDurationUs,
		"driftObjects": driftCount, "cleanObjects": *count - driftCount,
		"batches": len(batches.Batches), "batchSignature": batches.StableSignature,
		"exactBatchSignature": stressBatchSignature(exactBatches), "bucketedBatchSignature": stressBatchSignature(bucketedBatches),
		"batchPathReconciled": batchPathsReconciled,
		"estimatedCost":       cost.TotalEstimatedCost, "budgetOccupancy": cost.TotalBudgetOccupancy,
		"roundingError": cost.TotalRoundingError, "cycleError": batchErr != nil,
	}
	if *jsonOut {
		return printJSON(summary)
	}
	fmt.Printf("stress objects=%d fanout=%d selected=%s reconciled=%v compared=%d\n", *count, *fanout, drift.SelectedPath, drift.Reconciled, drift.ObjectsCompared)
	fmt.Printf("timings exact=%dus bucket=%dus drift=%d clean=%d\n", drift.ExactDurationUs, drift.BucketDurationUs, driftCount, *count-driftCount)
	for _, bucket := range drift.Buckets {
		fmt.Printf("  bucket %-16s objects=%d\n", bucket.Category, bucket.ObjectCnt)
	}
	fmt.Printf("batches=%d signature=%s exactSignature=%s bucketSignature=%s reconciled=%v\n", len(batches.Batches), batches.StableSignature, stressBatchSignature(exactBatches), stressBatchSignature(bucketedBatches), batchPathsReconciled)
	fmt.Printf("cost=%s occupancy=%s rounding=%s\n", cost.TotalEstimatedCost, cost.TotalBudgetOccupancy, cost.TotalRoundingError)
	return nil
}

func sameStressBatchPartition(left, right *batch.Report) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.StableSignature == right.StableSignature && left.ErrorCode == right.ErrorCode
}

func stressBatchSignature(report *batch.Report) string {
	if report == nil {
		return ""
	}
	return report.StableSignature
}

func generateStressCluster(count, fanout int) (*model.ClusterSpec, *model.Policy, error) {
	objects := make([]model.Object, 0, count)
	block := count/16 + 1
	if block < 32 {
		block = 32
	}
	for i := 0; i < count; i++ {
		namespace := fmt.Sprintf("bulk-%02d", i%16)
		id := fmt.Sprintf("%s/object-%06d", namespace, i)
		expected := map[string]model.FieldValue{
			"v":     {Type: model.TypeInteger, Value: strconv.Itoa(i % 97)},
			"ratio": {Type: model.TypeFloat, Value: "0.5"},
			"tag":   {Type: model.TypeString, Value: fmt.Sprintf("t-%d", i%19)},
		}
		actual := map[string]model.FieldValue{
			"v":     expected["v"],
			"ratio": expected["ratio"],
			"tag":   expected["tag"],
		}
		if i%5 == 0 {
			actual["v"] = model.FieldValue{Type: model.TypeInteger, Value: strconv.Itoa(i%97 + 1)}
		}
		if i%7 == 0 {
			delete(actual, "tag")
		}
		if i%11 == 0 {
			actual["extra"] = model.FieldValue{Type: model.TypeString, Value: "unexpected"}
		}
		sources := map[string]model.FieldSource{
			"v":     {Expected: "gitops", Actual: "gitops"},
			"ratio": {Expected: "gitops", Actual: "gitops"},
			"tag":   {Expected: "gitops", Actual: "gitops"},
		}
		if i%13 == 0 {
			sources["tag"] = model.FieldSource{Expected: "gitops", Actual: "manual"}
		}
		deps := []string{}
		for j := 1; j <= fanout; j++ {
			dep := i - j*block
			if dep >= 0 {
				deps = append(deps, fmt.Sprintf("%s/object-%06d", fmt.Sprintf("bulk-%02d", dep%16), dep))
			}
		}
		if half := i / 2; half >= 0 && half < i {
			deps = append(deps, fmt.Sprintf("%s/object-%06d", fmt.Sprintf("bulk-%02d", half%16), half))
		}
		objects = append(objects, model.Object{
			ID: id, Kind: []string{"ConfigMap", "Deployment", "Service", "Job"}[i%4], Namespace: namespace,
			Expected: expected, Actual: actual, Sources: sources, DependsOn: deps,
			ResourceBudget: int64(1_000_000 + (i%101)*1000 + i%17),
		})
	}
	cluster := &model.ClusterSpec{SnapshotVersion: 901, Description: "deterministic in-memory scale fixture", Objects: objects}
	if err := cluster.NormalizeAndValidate(); err != nil {
		return nil, nil, err
	}
	policy := &model.Policy{
		Version: 901, GeneratedAt: "2026-11-20T08:00:00Z", ExactObjectLimit: 5000, BatchSizeLimit: 1000,
		DependencyOrderRule: "dependencies_first", RoundTimeBudgetMs: 60000, TimeBudgetRelativeTolerancePPM: 5000,
		FloatTolerance:     model.FloatTolerance{RelativePPM: 10000, Mode: "relative_decimal_ppm"},
		MaxParallelBatches: 8, WriteQueueCapacity: 128, RetryLimit: 0, WriteTimeoutMs: 1000, PreviewSampleSize: 10,
	}
	policy.Checksum = policy.ComputeChecksum()
	return cluster, policy, nil
}

func organizeStress(cluster *model.ClusterSpec, report *detect.Report, policy *model.Policy) (int, bool, error) {
	organized, err := batch.Organize(cluster, report.Results, policy)
	if organized == nil {
		return 0, false, err
	}
	exactBatches, _ := batch.Organize(cluster, report.ExactResults, policy)
	bucketedBatches, _ := batch.Organize(cluster, report.BucketedResults, policy)
	reconciled := sameStressBatchPartition(exactBatches, bucketedBatches) && sameStressBatchPartition(organized, exactBatches)
	return len(organized.Batches), reconciled, err
}
