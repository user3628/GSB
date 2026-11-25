package qos

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTokenBucketIntegerRoundingAndColors(t *testing.T) {
	policy := map[string]Queue{
		"q": testQueue("q", "", 1001, 2003, 10, 20, 4),
	}
	eff := map[string]EffectiveRates{
		"q": {QueueID: "q", EffectiveCIRBPS: 1001, EffectivePIRBPS: 2003},
	}
	input := SimulationInput{
		PolicyVersion: 9, Policy: policy, Effective: eff,
		QueueVersions: map[string]QueueVersionStamp{"q": {CalculationVersion: 9, ValidThroughVersion: 9}},
		Samples: []TrafficSample{
			{TimestampMS: 100, QueueID: "q", Bytes: 5, DSCP: 0},
			{TimestampMS: 101, QueueID: "q", Bytes: 10, DSCP: 0},
			{TimestampMS: 102, QueueID: "q", Bytes: 10, DSCP: 0},
		},
	}
	results, packets, biz := RunSimulation(input)
	if biz != nil {
		t.Fatal(biz)
	}
	if packets != 3 || len(results) != 1 {
		t.Fatalf("packets=%d results=%d", packets, len(results))
	}
	got := results[0]
	if got.Summary.GreenPackets != 1 || got.Summary.YellowPackets != 1 || got.Summary.RedPackets != 1 {
		t.Fatalf("colors: %+v", got.Summary)
	}
	if got.Summary.SentBytes != 15 || got.Summary.DroppedBytes != 10 {
		t.Fatalf("bytes: %+v", got.Summary)
	}
	if got.Packets[0].Color != "green" || got.Packets[1].Color != "yellow" || got.Packets[2].Color != "red" {
		t.Fatalf("packet colors: %s %s %s", got.Packets[0].Color, got.Packets[1].Color, got.Packets[2].Color)
	}
	// t=101 committed: 1001 millibits -> 1 whole bit, carry 1.
	// t=102 committed: (1001+1) -> 1 whole bit, carry 2.
	if refill := got.Packets[1].Refill[0]; refill.RawMilliBits != 1001 || refill.WholeBits != 1 || refill.CarryAfterMilliBits != 1 {
		t.Fatalf("first rounding=%+v", refill)
	}
	if refill := got.Packets[2].Refill[0]; refill.CarryBeforeMilliBits != 1 || refill.WholeBits != 1 || refill.CarryAfterMilliBits != 2 {
		t.Fatalf("second rounding=%+v", refill)
	}
	if got.Summary.CumulativeErrorMilliBits != 2 {
		t.Fatalf("committed carry=%d", got.Summary.CumulativeErrorMilliBits)
	}
	if got.Summary.CumulativePeakErrorMilliBits != 6 {
		t.Fatalf("peak carry=%d", got.Summary.CumulativePeakErrorMilliBits)
	}
	if got.Summary.Conclusion != "NON_CONFORMANT" {
		t.Fatalf("conclusion=%s", got.Summary.Conclusion)
	}
	results2, _, biz := RunSimulation(input)
	if biz != nil {
		t.Fatal(biz)
	}
	if !reflect.DeepEqual(results, results2) {
		t.Fatal("same input did not produce bit-identical simulation")
	}
}

func TestSimulationStoreDeterministicAndStale(t *testing.T) {
	store, dir := newStore(t)
	if _, err := EnsureSamples(dir+"/data", DefaultSampleSeed); err != nil {
		t.Fatal(err)
	}
	_, policy, biz := NormalizeAndValidate(samplePolicy())
	if biz != nil {
		t.Fatal(biz)
	}
	snapshotPolicy(t, store, 1, queueSlice(policy))
	first, be := store.SubmitSimulation(context.Background(), SimulationRequest{RequestID: "sim-a", PolicyVersion: 1})
	if be != nil {
		t.Fatal(be)
	}
	second, be := store.SubmitSimulation(context.Background(), SimulationRequest{RequestID: "sim-b", PolicyVersion: 1})
	if be != nil {
		t.Fatal(be)
	}
	if first.Packets != 432 || second.Packets != first.Packets {
		t.Fatalf("packet counts %d %d", first.Packets, second.Packets)
	}
	if !reflect.DeepEqual(first.Queues, second.Queues) {
		t.Fatal("repeated store simulation is not deterministic")
	}
	diag1, be := store.GetQueueDiagnostic(context.Background(), "sim-a", "video", 1)
	if be != nil {
		t.Fatal(be)
	}
	diag2, be := store.GetQueueDiagnostic(context.Background(), "sim-b", "video", 1)
	if be != nil {
		t.Fatal(be)
	}
	if !reflect.DeepEqual(diag1.Packets, diag2.Packets) {
		t.Fatal("packet-level simulation output differs")
	}
	videoCIR := int64(1_234_567)
	if _, be := store.SubmitIncrement(context.Background(), IncrementRequest{
		RequestID: "sim-increment", BaseVersion: 1,
		QueuePatches: []QueuePatch{{QueueID: "video", CIRBPS: &videoCIR}},
	}); be != nil {
		t.Fatal(be)
	}
	if _, be := store.GetQueueDiagnostic(context.Background(), "sim-a", "video", 1); be == nil || be.Code != CodeStaleRead {
		t.Fatalf("old simulation diagnostic should be stale: %+v", be)
	}
}

func TestSimulationStoreRedPacketsAndDrops(t *testing.T) {
	store, dir := newStore(t)
	if _, err := EnsureSamples(filepath.Join(dir, "data"), DefaultSampleSeed); err != nil {
		t.Fatal(err)
	}
	queues := []Queue{testQueue("q", "", 1, 1, 1, 1, 4)}
	snapshotPolicy(t, store, 1, queues)
	traffic := []TrafficSample{
		{TimestampMS: 1000, QueueID: "q", Bytes: 1, DSCP: 0},
		{TimestampMS: 1001, QueueID: "q", Bytes: 1, DSCP: 0},
		{TimestampMS: 1002, QueueID: "q", Bytes: 1, DSCP: 0},
		{TimestampMS: 1003, QueueID: "q", Bytes: 1, DSCP: 0},
		{TimestampMS: 1004, QueueID: "q", Bytes: 1, DSCP: 0},
	}
	var file []byte
	for _, p := range traffic {
		b, _ := json.Marshal(p)
		file = append(file, b...)
		file = append(file, '\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "data", "traffic_sample.jsonl"), file, 0o644); err != nil {
		t.Fatal(err)
	}
	resp, be := store.SubmitSimulation(context.Background(), SimulationRequest{RequestID: "tiny", PolicyVersion: 1, QueueIDs: []string{"q"}})
	if be != nil {
		t.Fatal(be)
	}
	if len(resp.Queues) != 1 || resp.Queues[0].RedPackets == 0 {
		t.Fatalf("expected red packets in constrained simulation: %+v", resp.Queues)
	}
	if resp.Queues[0].DroppedBytes == 0 {
		t.Fatalf("expected dropped bytes: %+v", resp.Queues[0])
	}
}
