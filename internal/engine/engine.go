package engine

import (
	"fmt"
	"sort"

	"h2diag/internal/model"
)

type connectionState struct {
	Spec               model.ConnectionSpec
	Window             int64
	WindowUpdatedBytes int64
}

type streamState struct {
	Spec        model.StreamSpec
	State       model.StreamState
	Window      int64
	SentUnacked int64
	TotalSent   int64
	Pending     []model.PendingData
}

type compileState struct {
	sample                  *model.Sample
	config                  model.SampleConfig
	conns                   map[string]*connectionState
	streams                 map[streamKey]*streamState
	connOrder               []string
	streamOrder             []streamKey
	commits                 []model.PlanCommit
	summary                 model.RunSummary
	roundNo                 int64
	cumulativeRoundingError int64
	blockedSeen             map[streamKey]bool
}

func Compile(sample *model.Sample) (*model.Plan, *model.BizError) {
	if biz := ValidateAndNormalize(sample); biz != nil {
		return nil, biz
	}
	canonical, hash, err := model.CanonicalSample(sample)
	if err != nil {
		return nil, model.Biz(model.CodeInternal, "cannot canonicalize sample", map[string]any{"detail": err.Error()})
	}
	state := newCompileState(sample)
	initial := state.snapshot(0)

	for framePos := 0; framePos < len(sample.Frames); {
		timestamp := sample.Frames[framePos].Timestamp
		outcomes := make([]model.FrameOutcome, 0, 4)
		for framePos < len(sample.Frames) && sample.Frames[framePos].Timestamp == timestamp {
			frame := &sample.Frames[framePos]
			outcome, biz := state.processFrame(frame)
			if biz != nil {
				return nil, biz
			}
			outcomes = append(outcomes, outcome)
			framePos++
		}
		state.settleZeroLengthData()
		state.addCommit("EVENT", timestamp, "", outcomes, nil)
		for _, connectionID := range state.connOrder {
			if biz := state.scheduleConnection(timestamp, connectionID); biz != nil {
				return nil, biz
			}
		}
	}

	for _, connectionID := range state.connOrder {
		connection := state.conns[connectionID]
		for _, key := range state.streamOrder {
			if key.ConnectionID != connectionID {
				continue
			}
			stream := state.streams[key]
			pending := pendingBytes(stream)
			if pending == 0 && len(stream.Pending) == 0 {
				continue
			}
			if pending == 0 {
				state.settleZeroStream(stream)
				continue
			}
			requested := int64(0)
			for _, segment := range stream.Pending {
				requested += segment.Requested
			}
			return nil, model.Biz(model.CodeFlowControlBlocked, "DATA demand remains blocked by flow-control windows", map[string]any{
				"connection_id":               connectionID,
				"stream_id":                   key.StreamID,
				"connection_window_remaining": connection.Window,
				"stream_window_remaining":     stream.Window,
				"pending_bytes":               pending,
				"requested_bytes":             requested,
			})
		}
	}

	final := state.snapshot(int64(len(state.commits)))
	summary := state.summary
	summary.CumulativeRoundingErrorPPM = state.cumulativeRoundingError
	plan := &model.Plan{
		Sample:    sample,
		Canonical: canonical,
		Hash:      hash,
		Initial:   initial,
		Commits:   state.commits,
		Final:     final,
		Summary:   summary,
		Digest:    model.SnapshotDigest(final),
	}
	return plan, nil
}

func newCompileState(sample *model.Sample) *compileState {
	state := &compileState{
		sample:      sample,
		config:      sample.Config,
		conns:       make(map[string]*connectionState, len(sample.Connections)),
		streams:     make(map[streamKey]*streamState, len(sample.Streams)),
		blockedSeen: make(map[streamKey]bool),
	}
	state.summary.Frames = len(sample.Frames)
	for _, spec := range sample.Connections {
		state.conns[spec.ID] = &connectionState{Spec: spec, Window: spec.InitialConnectionWindow}
		state.connOrder = append(state.connOrder, spec.ID)
	}
	for _, spec := range sample.Streams {
		key := streamKey{ConnectionID: spec.ConnectionID, StreamID: spec.StreamID}
		state.streams[key] = &streamState{
			Spec:   spec,
			State:  model.StateIdle,
			Window: spec.InitialWindow,
		}
		state.streamOrder = append(state.streamOrder, key)
	}
	sort.Strings(state.connOrder)
	sort.Slice(state.streamOrder, func(i, j int) bool {
		if state.streamOrder[i].ConnectionID == state.streamOrder[j].ConnectionID {
			return state.streamOrder[i].StreamID < state.streamOrder[j].StreamID
		}
		return state.streamOrder[i].ConnectionID < state.streamOrder[j].ConnectionID
	})
	return state
}

func (e *compileState) processFrame(frame *model.Frame) (model.FrameOutcome, *model.BizError) {
	key := streamKey{ConnectionID: frame.ConnectionID, StreamID: frame.StreamID}
	outcome := model.FrameOutcome{
		Sequence:     frame.Sequence,
		Type:         frame.Type,
		ConnectionID: frame.ConnectionID,
		StreamID:     frame.StreamID,
	}
	connection := e.conns[frame.ConnectionID]
	var stream *streamState
	if frame.StreamID > 0 {
		stream = e.streams[key]
		outcome.StateBefore = stream.State
	}

	switch frame.Type {
	case model.FrameSettings:
		if frame.HasFlag("ACK") && frame.Settings != nil && frame.Settings.RoundBytes != nil {
			return outcome, frameInvalid(frame, "SETTINGS ACK cannot carry round_bytes", nil)
		}
		if frame.Settings != nil && frame.Settings.RoundBytes != nil {
			if *frame.Settings.RoundBytes <= 0 {
				return outcome, frameInvalid(frame, "SETTINGS round_bytes must be positive", nil)
			}
			e.config.RoundBytes = *frame.Settings.RoundBytes
			outcome.Result = "APPLIED"
			outcome.Detail = fmt.Sprintf("round_bytes=%d", e.config.RoundBytes)
		} else {
			outcome.Result = "ACCEPTED"
			outcome.Detail = "settings accepted; no diagnostic knob changed"
		}
		return outcome, nil

	case model.FrameHeaders:
		switch stream.State {
		case model.StateIdle, model.StateOpen:
			if frame.HasFlag("END_STREAM") {
				stream.State = model.StateHalfClosedRem
				outcome.Result = "OPENED_REMOTE_CLOSED"
			} else {
				stream.State = model.StateOpen
				outcome.Result = "OPENED"
			}
			outcome.StateAfter = stream.State
			outcome.Detail = "HEADERS does not consume flow-control credit"
			return outcome, nil
		default:
			return outcome, invalidTransition(frame, stream.State, "HEADERS", "HEADERS is legal only in IDLE or OPEN")
		}

	case model.FrameData:
		if stream.State != model.StateOpen && stream.State != model.StateHalfClosedRem {
			return outcome, invalidTransition(frame, stream.State, "DATA", "DATA is legal only when local side is open")
		}
		if frame.PayloadBytes > 0 || frame.HasFlag("END_STREAM") {
			stream.Pending = append(stream.Pending, model.PendingData{
				Sequence:  frame.Sequence,
				Remaining: frame.PayloadBytes,
				EndStream: frame.HasFlag("END_STREAM"),
				Requested: frame.PayloadBytes,
			})
		}
		e.summary.DATAIntentFrames++
		outcome.Result = "QUEUED"
		outcome.Detail = fmt.Sprintf("payload_bytes=%d, end_stream=%t", frame.PayloadBytes, frame.HasFlag("END_STREAM"))
		return outcome, nil

	case model.FrameWindowUpdate:
		if frame.StreamID == 0 {
			if frame.WindowIncrement > connection.Spec.MaxConnectionWindow-connection.Window {
				return outcome, windowOverflow(frame, "connection", connection.Window, frame.WindowIncrement, connection.Spec.MaxConnectionWindow)
			}
			connection.Window += frame.WindowIncrement
			connection.WindowUpdatedBytes += frame.WindowIncrement
			outcome.Result = "CONNECTION_WINDOW_UPDATED"
			outcome.WindowIncrement = frame.WindowIncrement
			outcome.Detail = fmt.Sprintf("window_remaining=%d", connection.Window)
			return outcome, nil
		}
		switch stream.State {
		case model.StateOpen, model.StateHalfClosedLoc, model.StateHalfClosedRem:
			if frame.WindowIncrement > stream.Spec.MaxWindow-stream.Window {
				return outcome, windowOverflow(frame, "stream", stream.Window, frame.WindowIncrement, stream.Spec.MaxWindow)
			}
			stream.Window += frame.WindowIncrement
			stream.SentUnacked -= minInt64(frame.WindowIncrement, stream.SentUnacked)
			outcome.Result = "STREAM_WINDOW_UPDATED"
			outcome.WindowIncrement = frame.WindowIncrement
			outcome.StateAfter = stream.State
			outcome.Detail = fmt.Sprintf("window_remaining=%d, sent_unacked=%d", stream.Window, stream.SentUnacked)
			return outcome, nil
		default:
			return outcome, invalidTransition(frame, stream.State, "WINDOW_UPDATE", "stream WINDOW_UPDATE is illegal in IDLE, CLOSED, or RESET")
		}

	case model.FrameResetStream:
		switch stream.State {
		case model.StateOpen, model.StateHalfClosedLoc, model.StateHalfClosedRem:
			discarded := pendingBytes(stream)
			stream.Pending = nil
			stream.State = model.StateReset
			delete(e.blockedSeen, key)
			outcome.Result = "RESET"
			outcome.StateAfter = model.StateReset
			outcome.Detail = fmt.Sprintf("discarded_pending_bytes=%d", discarded)
			return outcome, nil
		default:
			return outcome, invalidTransition(frame, stream.State, "RST_STREAM", "RST_STREAM is legal only on an active non-closed stream")
		}
	default:
		return outcome, frameInvalid(frame, "unsupported frame type", nil)
	}
}

func (e *compileState) scheduleConnection(timestamp int64, connectionID string) *model.BizError {
	connection := e.conns[connectionID]
	for {
		e.settleZeroLengthDataForConnection(connectionID)
		hasPending := false
		for _, key := range e.streamOrder {
			if key.ConnectionID != connectionID {
				continue
			}
			stream := e.streams[key]
			if len(stream.Pending) > 0 && stream.State != model.StateReset && stream.State != model.StateClosed {
				hasPending = true
				break
			}
		}
		if !hasPending {
			return nil
		}
		if connection.Window <= 0 {
			e.markBlocked(connectionID)
			return nil
		}
		budget := minInt64(e.config.RoundBytes, connection.Window)
		progressed := false
		for priority := 0; priority < connection.Spec.PriorityLevels; priority++ {
			if budget <= 0 {
				break
			}
			entries := e.priorityEntries(connectionID, priority)
			if len(entries) == 0 {
				continue
			}
			result := allocateRound(entries, budget, e.config)
			total := sumInt64(result.Allocations)
			if total <= 0 {
				continue
			}
			if e.roundNo >= int64(e.config.MaxSchedulingRounds) {
				return model.Biz(model.CodeSchedulerLimit, "scheduling round limit reached", map[string]any{
					"max_scheduling_rounds": e.config.MaxSchedulingRounds,
					"connection_id":         connectionID,
				})
			}
			e.roundNo++
			e.summary.SchedulingRounds++
			if result.Exact {
				e.summary.ExactRounds++
			} else {
				e.summary.SamplingRounds++
			}
			weightBits, roundRoundingError := normalizeWeights(entries, e.config.WeightScalePPM)
			e.cumulativeRoundingError += roundRoundingError
			normalizedByStream := make(map[int64]int64, len(weightBits))
			for _, bit := range weightBits {
				normalizedByStream[bit.StreamID] = bit.NormalizedSharePPM
			}
			type candidate struct {
				index  int
				amount int64
			}
			candidates := make([]candidate, 0, len(entries))
			for index, amount := range result.Allocations {
				if amount > 0 {
					candidates = append(candidates, candidate{index: index, amount: amount})
				}
			}
			sort.Slice(candidates, func(i, j int) bool {
				left := entries[candidates[i].index]
				right := entries[candidates[j].index]
				if candidates[i].amount != candidates[j].amount {
					return candidates[i].amount > candidates[j].amount
				}
				if left.Weight != right.Weight {
					return left.Weight > right.Weight
				}
				return left.StreamID < right.StreamID
			})
			round := model.RoundTrace{
				RoundNo:                    e.roundNo,
				Timestamp:                  timestamp,
				ConnectionID:               connectionID,
				Priority:                   priority,
				BudgetBytes:                budget,
				ConnectionWindowBefore:     connection.Window,
				EligibleStreams:            len(entries),
				Sampling:                   result.Sampling,
				WeightNormalization:        weightBits,
				RoundRoundingErrorPPM:      roundRoundingError,
				CumulativeRoundingErrorPPM: e.cumulativeRoundingError,
			}
			for _, selected := range candidates {
				key := streamKey{ConnectionID: connectionID, StreamID: entries[selected.index].StreamID}
				stream := e.streams[key]
				amount := selected.amount
				if connection.Window < amount || stream.Window < amount {
					return model.Biz(model.CodeFlowControlBlocked, "DATA send failed flow-control precheck", map[string]any{
						"connection_id":               connectionID,
						"stream_id":                   key.StreamID,
						"connection_window_remaining": connection.Window,
						"stream_window_remaining":     stream.Window,
						"pending_bytes":               amount,
						"requested_bytes":             pendingBytes(stream),
					})
				}
				requestedPending := pendingBytes(stream)
				streamBefore := stream.Window
				connBefore := connection.Window
				connection.Window -= amount
				stream.Window -= amount
				stream.SentUnacked += amount
				stream.TotalSent += amount
				e.consumePendingData(stream, amount)
				e.summary.Selections++
				e.summary.DataBytesSent += amount
				sortOrder := "exact_weighted_largest_remainder"
				if !result.Exact {
					sortOrder = "bucketed_estimate"
				}
				round.Selections = append(round.Selections, model.Selection{
					Priority:              priority,
					StreamID:              key.StreamID,
					Bytes:                 amount,
					RequestedPendingBytes: requestedPending,
					Weight:                stream.Spec.Weight,
					NormalizedSharePPM:    normalizedByStream[key.StreamID],
					Basis: fmt.Sprintf("strict_priority; %s; weight=%d; normalized_share_ppm=%d; send_order=bytes_desc,weight_desc,stream_id_asc",
						sortOrder, stream.Spec.Weight, normalizedByStream[key.StreamID]),
					ConnWindowBefore:   connBefore,
					ConnWindowAfter:    connection.Window,
					StreamWindowBefore: streamBefore,
					StreamWindowAfter:  stream.Window,
				})
				if stream.Pending == nil {
					if e.blockedSeen[key] {
						e.summary.ResolvedBlockingEvents++
						delete(e.blockedSeen, key)
					}
				}
			}
			round.ConnectionWindowAfter = connection.Window
			e.addCommit("ROUND", timestamp, connectionID, nil, &round)
			budget -= total
			progressed = true
		}
		if !progressed {
			e.markBlocked(connectionID)
			return nil
		}
	}
}

func (e *compileState) priorityEntries(connectionID string, priority int) []allocEntry {
	entries := make([]allocEntry, 0)
	for _, key := range e.streamOrder {
		if key.ConnectionID != connectionID {
			continue
		}
		stream := e.streams[key]
		if stream.Spec.Priority != priority {
			continue
		}
		if stream.State != model.StateOpen && stream.State != model.StateHalfClosedRem {
			continue
		}
		pending := pendingBytes(stream)
		if pending <= 0 {
			continue
		}
		demand := minInt64(pending, stream.Window)
		if demand <= 0 {
			continue
		}
		entries = append(entries, allocEntry{StreamID: key.StreamID, Weight: stream.Spec.Weight, Demand: demand})
	}
	return entries
}

func (e *compileState) consumePendingData(stream *streamState, amount int64) {
	for amount > 0 && len(stream.Pending) > 0 {
		segment := &stream.Pending[0]
		take := minInt64(amount, segment.Remaining)
		segment.Remaining -= take
		amount -= take
		if segment.Remaining == 0 {
			endStream := segment.EndStream
			stream.Pending = stream.Pending[1:]
			if endStream {
				e.finishLocal(stream)
			}
		}
	}
}

func (e *compileState) finishLocal(stream *streamState) {
	switch stream.State {
	case model.StateOpen:
		stream.State = model.StateHalfClosedLoc
	case model.StateHalfClosedRem:
		stream.State = model.StateClosed
	}
}

func (e *compileState) settleZeroLengthData() {
	for _, key := range e.streamOrder {
		e.settleZeroStream(e.streams[key])
	}
}

func (e *compileState) settleZeroLengthDataForConnection(connectionID string) {
	for _, key := range e.streamOrder {
		if key.ConnectionID == connectionID {
			e.settleZeroStream(e.streams[key])
		}
	}
}

func (e *compileState) settleZeroStream(stream *streamState) {
	for len(stream.Pending) > 0 && stream.Pending[0].Remaining == 0 {
		endStream := stream.Pending[0].EndStream
		stream.Pending = stream.Pending[1:]
		if endStream {
			e.finishLocal(stream)
		}
	}
}

func (e *compileState) markBlocked(connectionID string) {
	for _, key := range e.streamOrder {
		if key.ConnectionID != connectionID {
			continue
		}
		stream := e.streams[key]
		if pendingBytes(stream) > 0 && stream.State != model.StateReset && stream.State != model.StateClosed {
			e.blockedSeen[key] = true
			return
		}
	}
}

func (e *compileState) addCommit(kind string, timestamp int64, connectionID string, outcomes []model.FrameOutcome, round *model.RoundTrace) {
	version := int64(len(e.commits)) + 1
	snapshot := e.snapshot(version)
	commit := model.PlanCommit{
		Version:    version,
		Kind:       kind,
		Timestamp:  timestamp,
		Connection: connectionID,
		Outcomes:   append([]model.FrameOutcome(nil), outcomes...),
		Round:      round,
		Snapshot:   snapshot,
		Digest:     model.SnapshotDigest(snapshot),
	}
	if round != nil {
		commit.RoundNo = round.RoundNo
	}
	e.commits = append(e.commits, commit)
	if kind == "EVENT" {
		e.summary.EventCommits++
	}
}

func (e *compileState) snapshot(version int64) model.Snapshot {
	snapshot := model.Snapshot{
		SampleID:                   e.sample.SampleID,
		Version:                    version,
		CumulativeRoundingErrorPPM: e.cumulativeRoundingError,
		Connections:                make([]model.ConnectionSnapshot, 0, len(e.conns)),
		Streams:                    make([]model.StreamSnapshot, 0, len(e.streams)),
	}
	for _, connectionID := range e.connOrder {
		connection := e.conns[connectionID]
		snapshot.Connections = append(snapshot.Connections, model.ConnectionSnapshot{
			ID:                 connection.Spec.ID,
			InitialWindow:      connection.Spec.InitialConnectionWindow,
			MaxWindow:          connection.Spec.MaxConnectionWindow,
			WindowRemaining:    connection.Window,
			PriorityLevels:     connection.Spec.PriorityLevels,
			WindowUpdatedBytes: connection.WindowUpdatedBytes,
		})
	}
	for _, key := range e.streamOrder {
		stream := e.streams[key]
		snapshot.Streams = append(snapshot.Streams, model.StreamSnapshot{
			ConnectionID:    key.ConnectionID,
			StreamID:        key.StreamID,
			State:           stream.State,
			InitialWindow:   stream.Spec.InitialWindow,
			MaxWindow:       stream.Spec.MaxWindow,
			WindowRemaining: stream.Window,
			SentUnacked:     stream.SentUnacked,
			TotalSent:       stream.TotalSent,
			Weight:          stream.Spec.Weight,
			Priority:        stream.Spec.Priority,
			PendingBytes:    pendingBytes(stream),
			Pending:         append([]model.PendingData(nil), stream.Pending...),
		})
	}
	return snapshot
}

func pendingBytes(stream *streamState) int64 {
	var total int64
	for _, segment := range stream.Pending {
		total += segment.Remaining
	}
	return total
}

func invalidTransition(frame *model.Frame, state model.StreamState, operation, detail string) *model.BizError {
	return model.Biz(model.CodeInvalidTransition, detail, map[string]any{
		"sequence":      frame.Sequence,
		"timestamp":     frame.Timestamp,
		"connection_id": frame.ConnectionID,
		"stream_id":     frame.StreamID,
		"frame_type":    frame.Type,
		"state":         state,
		"operation":     operation,
	})
}

func windowOverflow(frame *model.Frame, scope string, current, increment, maximum int64) *model.BizError {
	return model.Biz(model.CodeWindowOverflow, "WINDOW_UPDATE would exceed declared maximum", map[string]any{
		"sequence":         frame.Sequence,
		"timestamp":        frame.Timestamp,
		"connection_id":    frame.ConnectionID,
		"stream_id":        frame.StreamID,
		"scope":            scope,
		"window_remaining": current,
		"window_increment": increment,
		"window_maximum":   maximum,
	})
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}

func sumInt64(values []int64) int64 {
	var total int64
	for _, value := range values {
		total += value
	}
	return total
}
