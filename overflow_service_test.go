package feedbackstream

import (
	"context"
	"math"
	"testing"
)

func TestSubmitOverflowPropagatesWindowAndGroup(t *testing.T) {
	threshold := int64(-1)
	service, store, policy := newTestService(t, func(p *PolicySnapshot) {
		p.Pipeline.Operators[1].Config.MetricGTE = &threshold
	})
	ctx := context.Background()
	events := []Event{
		{TimestampMS: policy.GeneratedAtMS + 10, PartitionID: 4, GroupKey: "hot", MetricFixed: math.MaxInt64, Sequence: 1},
		{TimestampMS: policy.GeneratedAtMS + 20, PartitionID: 4, GroupKey: "hot", MetricFixed: 1, Sequence: 2},
	}
	_, err := service.SubmitBatch(ctx, SubmitBatchRequest{BatchID: "overflow-batch", Events: events})
	coded := requireCode(t, err, CodeNumericOverflow)
	if coded.Details["group_key"] != "hot" {
		t.Fatalf("missing group key: %#v", coded.Details)
	}
	rows, err := store.ListWindowResults(ctx, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("overflow batch must not commit aggregates: %#v", rows)
	}
}
