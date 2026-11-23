package engine

import (
	"testing"

	"h2diag/internal/model"
)

func TestExactAllocateUsesLargestRemainderAndStreamIDTieBreak(t *testing.T) {
	entries := []allocEntry{
		{StreamID: 2, Weight: 1, Demand: 100},
		{StreamID: 1, Weight: 1, Demand: 100},
		{StreamID: 3, Weight: 1, Demand: 100},
	}
	allocations := exactAllocate(2, entries)
	if allocations[0] != 1 || allocations[1] != 1 || allocations[2] != 0 {
		t.Fatalf("unexpected allocation: %#v", allocations)
	}
	bits, residual := normalizeWeights(entries, model.FixedScalePPM)
	var sum int64
	for _, bit := range bits {
		sum += bit.NormalizedSharePPM
	}
	if sum != model.FixedScalePPM {
		t.Fatalf("normalized shares sum %d, want %d", sum, model.FixedScalePPM)
	}
	if residual != 1 {
		t.Fatalf("rounding adjustments %d, want 1", residual)
	}
}

func TestAuditOrderUsesTheSameTieBreakAsSendOrder(t *testing.T) {
	entries := []allocEntry{
		{StreamID: 1, Weight: 1, Demand: 10},
		{StreamID: 2, Weight: 3, Demand: 10},
		{StreamID: 3, Weight: 2, Demand: 10},
	}
	order := orderForAllocation(entries, []int64{1, 1, 1})
	want := []int64{2, 3, 1}
	if !equalOrders(order, want) {
		t.Fatalf("audit order=%v, want %v", order, want)
	}
}

func TestWeightedScheduleIsDeterministic(t *testing.T) {
	sample := twoStreamSample("weighted-v1")
	planA, biz := Compile(sample)
	if biz != nil {
		t.Fatalf("compile: %v", biz)
	}
	sampleB := twoStreamSample("weighted-v1")
	sampleB.Frames[0], sampleB.Frames[len(sampleB.Frames)-1] = sampleB.Frames[len(sampleB.Frames)-1], sampleB.Frames[0]
	planB, biz := Compile(sampleB)
	if biz != nil {
		t.Fatalf("compile reversed: %v", biz)
	}
	if planA.Digest != planB.Digest {
		t.Fatalf("final digest differs across input array order: %s vs %s", planA.Digest, planB.Digest)
	}
	if len(planA.Commits) != len(planB.Commits) {
		t.Fatalf("commit count differs: %d vs %d", len(planA.Commits), len(planB.Commits))
	}
	var firstSelection *model.Selection
	for _, commit := range planA.Commits {
		if commit.Round == nil || len(commit.Round.Selections) == 0 {
			continue
		}
		firstSelection = &commit.Round.Selections[0]
		break
	}
	if firstSelection == nil || firstSelection.StreamID != 2 || firstSelection.Bytes != 3 {
		t.Fatalf("first weighted selection = %#v, want stream 2 with 3 bytes", firstSelection)
	}
	for _, stream := range planA.Final.Streams {
		if stream.TotalSent != 10 || stream.WindowRemaining != 990 || stream.PendingBytes != 0 {
			t.Fatalf("unexpected final stream state: %#v", stream)
		}
	}
	for _, connection := range planA.Final.Connections {
		if connection.WindowRemaining != 980 {
			t.Fatalf("connection window = %d, want 980", connection.WindowRemaining)
		}
	}
}

func TestBlockedSampleReturnsContextAndNoPlan(t *testing.T) {
	sample := baseSample("blocked-v1")
	sample.Config.RoundBytes = 4
	sample.Connections[0].InitialConnectionWindow = 3
	sample.Frames = []model.Frame{
		frame(1, 0, 0, model.FrameSettings, nil, 0, 0),
		frame(2, 1, 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0),
		frame(3, 2, 1, model.FrameData, nil, 8, 0),
	}
	plan, biz := Compile(sample)
	if plan != nil {
		t.Fatal("blocked sample unexpectedly produced a plan")
	}
	if biz == nil || biz.Code != model.CodeFlowControlBlocked {
		t.Fatalf("error = %#v, want %s", biz, model.CodeFlowControlBlocked)
	}
	if biz.Context["connection_window_remaining"] != int64(0) || biz.Context["pending_bytes"] != int64(5) {
		t.Fatalf("unexpected blocked context: %#v", biz.Context)
	}
}

func TestWindowOverflowDoesNotProducePlan(t *testing.T) {
	sample := baseSample("overflow-v1")
	sample.Connections[0].InitialConnectionWindow = 10
	sample.Connections[0].MaxConnectionWindow = 12
	sample.Frames = []model.Frame{
		frame(1, 0, 0, model.FrameSettings, nil, 0, 0),
		frame(2, 0, 0, model.FrameWindowUpdate, nil, 0, 5),
	}
	plan, biz := Compile(sample)
	if plan != nil || biz == nil || biz.Code != model.CodeWindowOverflow {
		t.Fatalf("plan=%#v err=%#v, want %s", plan, biz, model.CodeWindowOverflow)
	}
}

func TestStreamWindowOverflowAndIdleWindowUpdateAreRejected(t *testing.T) {
	sample := baseSample("stream-overflow-v1")
	sample.Streams[0].InitialWindow = 1000
	sample.Streams[0].MaxWindow = 1000
	sample.Frames = []model.Frame{
		frame(1, 0, 0, model.FrameSettings, nil, 0, 0),
		frame(2, 1, 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0),
		frame(3, 2, 1, model.FrameWindowUpdate, nil, 0, 1),
	}
	plan, biz := Compile(sample)
	if plan != nil || biz == nil || biz.Code != model.CodeWindowOverflow || biz.Context["scope"] != "stream" {
		t.Fatalf("plan=%#v err=%#v, want stream %s", plan, biz, model.CodeWindowOverflow)
	}

	sample = baseSample("idle-window-update-v1")
	sample.Frames = []model.Frame{
		frame(1, 0, 0, model.FrameSettings, nil, 0, 0),
		frame(2, 1, 1, model.FrameWindowUpdate, nil, 0, 1),
	}
	plan, biz = Compile(sample)
	if plan != nil || biz == nil || biz.Code != model.CodeInvalidTransition {
		t.Fatalf("plan=%#v err=%#v, want %s", plan, biz, model.CodeInvalidTransition)
	}
}

func TestResetClearsPendingAndSuppressesScheduling(t *testing.T) {
	sample := baseSample("reset-test-v1")
	sample.Frames = []model.Frame{
		frame(1, 0, 0, model.FrameSettings, nil, 0, 0),
		frame(2, 1, 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0),
		frame(3, 2, 1, model.FrameData, nil, 512, 0),
		frame(4, 2, 1, model.FrameResetStream, nil, 0, 0),
	}
	plan, biz := Compile(sample)
	if biz != nil {
		t.Fatalf("compile: %v", biz)
	}
	if plan.Summary.DataBytesSent != 0 || plan.Summary.SchedulingRounds != 0 {
		t.Fatalf("reset stream was scheduled: %#v", plan.Summary)
	}
	stream := plan.Final.Streams[0]
	if stream.State != model.StateReset || stream.PendingBytes != 0 || stream.TotalSent != 0 {
		t.Fatalf("unexpected reset state: %#v", stream)
	}
}

func TestBucketedEstimatePathReportsSamplingAndDiffReason(t *testing.T) {
	sample := samplingSample("sampling-test-v1")
	plan, biz := Compile(sample)
	if biz != nil {
		t.Fatalf("compile: %v", biz)
	}
	if plan.Summary.SamplingRounds == 0 {
		t.Fatalf("sampling path was not used: %#v", plan.Summary)
	}
	found := false
	for _, commit := range plan.Commits {
		if commit.Round == nil {
			continue
		}
		actualOrder := make([]int64, 0, len(commit.Round.Selections))
		for _, selection := range commit.Round.Selections {
			actualOrder = append(actualOrder, selection.StreamID)
		}
		if commit.Round.Sampling.Active {
			if !equalOrders(commit.Round.Sampling.EstimatedOrder, actualOrder) {
				t.Fatalf("estimated audit order=%v differs from actual send order=%v in round %d",
					commit.Round.Sampling.EstimatedOrder, actualOrder, commit.Round.RoundNo)
			}
		} else if !equalOrders(commit.Round.Sampling.ExactOrder, actualOrder) {
			t.Fatalf("exact audit order=%v differs from actual send order=%v in round %d",
				commit.Round.Sampling.ExactOrder, actualOrder, commit.Round.RoundNo)
		}
		if !commit.Round.Sampling.Active {
			continue
		}
		found = true
		if commit.Round.Sampling.SamplingRatePPM >= model.FixedScalePPM {
			t.Fatalf("sampling rate should be below 100%%: %#v", commit.Round.Sampling)
		}
		if commit.Round.Sampling.BucketMethod == "" || commit.Round.Sampling.Confidence == "" ||
			commit.Round.Sampling.DifferenceReason == "" {
			t.Fatalf("sampling diagnostics are incomplete: %#v", commit.Round.Sampling)
		}
	}
	if !found {
		t.Fatal("no active sampling round found")
	}
	for _, stream := range plan.Final.Streams {
		if stream.PendingBytes != 0 {
			t.Fatalf("sampling path left pending bytes: %#v", stream)
		}
	}
}

func twoStreamSample(sampleID string) *model.Sample {
	sample := baseSample(sampleID)
	sample.Config.RoundBytes = 4
	sample.Streams = []model.StreamSpec{
		{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0},
		{ConnectionID: "loopback", StreamID: 2, Weight: 3, Priority: 0},
	}
	sample.Frames = []model.Frame{
		frame(1, 0, 0, model.FrameSettings, nil, 0, 0),
		frame(2, 1, 1, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0),
		frame(3, 1, 2, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0),
		frame(4, 2, 1, model.FrameData, []string{"END_STREAM"}, 10, 0),
		frame(5, 2, 2, model.FrameData, []string{"END_STREAM"}, 10, 0),
	}
	return sample
}

func samplingSample(sampleID string) *model.Sample {
	sample := baseSample(sampleID)
	sample.Config.RoundBytes = 64
	sample.Config.SchedulingThreshold = 4
	sample.Config.SamplingFallbackBelow = 2
	sample.Config.Approximation = "bucketed"
	var streams []model.StreamSpec
	var frames []model.Frame
	sequence := int64(1)
	frames = append(frames, frame(sequence, 0, 0, model.FrameSettings, nil, 0, 0))
	for index := 0; index < 20; index++ {
		streamID := int64(index + 1)
		streams = append(streams, model.StreamSpec{
			ConnectionID: "loopback", StreamID: streamID, Weight: int64(index%7 + 1), Priority: 0,
		})
		sequence++
		frames = append(frames, frame(sequence, 1, streamID, model.FrameHeaders, []string{"END_HEADERS"}, 0, 0))
	}
	for index := 0; index < 20; index++ {
		sequence++
		frames = append(frames, frame(sequence, 2, int64(index+1), model.FrameData, []string{"END_STREAM"}, int64(13+index), 0))
	}
	sample.Streams = streams
	sample.Frames = frames
	return sample
}

func baseSample(sampleID string) *model.Sample {
	config := model.DefaultSampleConfig()
	config.RoundBytes = 16
	return &model.Sample{
		SampleID: sampleID,
		Config:   config,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 1000,
			MaxConnectionWindow:     model.MaxHTTP2StreamID,
			InitialStreamWindow:     1000,
			MaxStreamWindow:         model.MaxHTTP2StreamID,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames:  []model.Frame{frame(1, 0, 0, model.FrameSettings, nil, 0, 0)},
	}
}

func frame(sequence, timestamp int64, streamID int64, frameType model.FrameType, flags []string, payload, increment int64) model.Frame {
	if flags == nil {
		flags = []string{}
	}
	return model.Frame{
		Sequence:        sequence,
		Timestamp:       timestamp,
		ConnectionID:    "loopback",
		StreamID:        streamID,
		Type:            frameType,
		Flags:           flags,
		PayloadBytes:    payload,
		WindowIncrement: increment,
	}
}
