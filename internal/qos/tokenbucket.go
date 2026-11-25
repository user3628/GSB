package qos

import (
	"fmt"
	"math"
	"sort"
)

const RoundingContract = "Tokens are bits. For each refill: exact_millibits = rate_bps * delta_ms; whole = floor((carry_millibits + exact_millibits) / 1000); carry = remainder mod 1000. Carry is retained across packets, so the cumulative truncation error is always in [0,1) bit and is reported per packet. Timestamps are integer milliseconds; sub-millisecond time is truncated before refill. Same policy version + same sample always yields bit-identical output."

type SimulationInput struct {
	PolicyVersion    int64
	Policy           map[string]Queue
	Effective        map[string]EffectiveRates
	QueueVersions    map[string]QueueVersionStamp
	Samples          []TrafficSample
	SelectedQueueIDs []string
}

type QueueVersionStamp struct {
	CalculationVersion  int64
	ValidThroughVersion int64
}

type QueueSimulationResult struct {
	Summary QueueSimulationSummary `json:"summary"`
	Packets []PacketResult         `json:"packets"`
}

func RunSimulation(input SimulationInput) ([]QueueSimulationResult, int, *BusinessError) {
	selected := map[string]bool{}
	ids := append([]string(nil), input.SelectedQueueIDs...)
	if len(ids) == 0 {
		ids = SortedQueueIDs(input.Policy)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, ok := input.Policy[id]; !ok {
			return nil, 0, NewError(CodeNotFound, "simulation queue does not exist", 404, map[string]any{"queue_id": id})
		}
		selected[id] = true
	}
	type event struct {
		sample   TrafficSample
		priority int
		index    int
	}
	events := make([]event, 0, len(input.Samples))
	for i, s := range input.Samples {
		if !selected[s.QueueID] {
			continue
		}
		q := input.Policy[s.QueueID]
		events = append(events, event{sample: s, priority: q.Priority, index: i})
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].sample.TimestampMS != events[j].sample.TimestampMS {
			return events[i].sample.TimestampMS < events[j].sample.TimestampMS
		}
		if events[i].priority != events[j].priority {
			return events[i].priority > events[j].priority
		}
		if events[i].sample.QueueID != events[j].sample.QueueID {
			return events[i].sample.QueueID < events[j].sample.QueueID
		}
		return events[i].index < events[j].index
	})

	type state struct {
		initialized bool
		lastTS      int64
		tokens      int64
		peak        int64
		carry       int64
		peakCarry   int64
		packets     []PacketResult
		summary     QueueSimulationSummary
	}
	states := make(map[string]*state, len(ids))
	for _, id := range ids {
		q := input.Policy[id]
		eff := input.Effective[id]
		stamp := input.QueueVersions[id]
		states[id] = &state{summary: QueueSimulationSummary{
			QueueID: id, Priority: q.Priority,
			EffectiveCIRBPS: eff.EffectiveCIRBPS, EffectivePIRBPS: eff.EffectivePIRBPS,
			PolicyVersion:      input.PolicyVersion,
			CalculationVersion: stamp.CalculationVersion, ValidThroughVersion: stamp.ValidThroughVersion,
		}}
	}
	for seq, ev := range events {
		s := ev.sample
		q := input.Policy[s.QueueID]
		eff := input.Effective[s.QueueID]
		st := states[s.QueueID]
		capacity := q.CBSBytes * 8
		peakCapacity := q.PBSBytes * 8
		var refills []RefillRounding
		if !st.initialized {
			st.initialized = true
			st.lastTS = s.TimestampMS
			st.tokens = capacity
			st.peak = peakCapacity
			refills = append(refills,
				RefillRounding{Bucket: "committed", DeltaMS: 0, Explanation: "initial fill to CBS (no elapsed milliseconds)"},
				RefillRounding{Bucket: "peak", DeltaMS: 0, Explanation: "initial fill to PBS (no elapsed milliseconds)"},
			)
		} else {
			delta := s.TimestampMS - st.lastTS
			if delta < 0 {
				return nil, 0, NewError(CodeSimulationFailed, "traffic sample is not chronological within a queue", 422, map[string]any{
					"queue_id": s.QueueID, "timestamp_ms": s.TimestampMS, "previous_timestamp_ms": st.lastTS,
				})
			}
			var rCommitted RefillRounding
			var biz *BusinessError
			st.tokens, st.carry, rCommitted, biz = refillBucket("committed", st.tokens, capacity, st.carry, eff.EffectiveCIRBPS, delta)
			if biz != nil {
				return nil, 0, biz
			}
			var rPeak RefillRounding
			st.peak, st.peakCarry, rPeak, biz = refillBucket("peak", st.peak, peakCapacity, st.peakCarry, eff.EffectivePIRBPS, delta)
			if biz != nil {
				return nil, 0, biz
			}
			refills = []RefillRounding{rCommitted, rPeak}
			st.lastTS = s.TimestampMS
		}
		bits := s.Bytes * 8
		if s.Bytes <= 0 || s.Bytes > math.MaxInt64/8 {
			return nil, 0, NewError(CodeSimulationFailed, "packet size is outside the supported integer range", 422, map[string]any{"queue_id": s.QueueID})
		}
		before, peakBefore := st.tokens, st.peak
		color := "red"
		sent := int64(0)
		dropped := s.Bytes
		if st.tokens >= bits && st.peak >= bits {
			color = "green"
			st.tokens -= bits
			st.peak -= bits
			sent = s.Bytes
			dropped = 0
			st.summary.GreenPackets++
		} else if st.peak >= bits {
			color = "yellow"
			st.peak -= bits
			sent = s.Bytes
			dropped = 0
			st.summary.YellowPackets++
		} else {
			st.summary.RedPackets++
		}
		st.summary.OfferedBytes += s.Bytes
		st.summary.SentBytes += sent
		st.summary.DroppedBytes += dropped
		p := PacketResult{
			Sequence: seq + 1, TimestampMS: s.TimestampMS, QueueID: s.QueueID, Bytes: s.Bytes, DSCP: s.DSCP,
			Priority: q.Priority, TokensBeforeBits: before, PeakTokensBeforeBits: peakBefore,
			Refill: refills, TokensAfterBits: st.tokens, PeakTokensAfterBits: st.peak,
			Color: color, SentBytes: sent, DroppedBytes: dropped,
			TokenBalanceBits: st.tokens, PeakBalanceBits: st.peak, MsTruncationMS: 0,
		}
		st.packets = append(st.packets, p)
	}
	results := make([]QueueSimulationResult, 0, len(ids))
	for _, id := range ids {
		st := states[id]
		st.summary.FinalTokenBalanceBits = st.tokens
		st.summary.FinalPeakBalanceBits = st.peak
		st.summary.CumulativeErrorMilliBits = st.carry
		st.summary.CumulativePeakErrorMilliBits = st.peakCarry
		switch {
		case st.summary.RedPackets > 0:
			st.summary.Conclusion = "NON_CONFORMANT"
		case st.summary.YellowPackets > 0:
			st.summary.Conclusion = "EXCEEDS_COMMITTED_RATE"
		default:
			st.summary.Conclusion = "CONFORMANT"
		}
		results = append(results, QueueSimulationResult{Summary: st.summary, Packets: st.packets})
	}
	return results, len(events), nil
}

func refillBucket(name string, tokens, capacity, carry, rate, deltaMS int64) (int64, int64, RefillRounding, *BusinessError) {
	if rate <= 0 || deltaMS < 0 {
		return tokens, carry, RefillRounding{}, NewError(CodeSimulationFailed, "invalid refill rate or timestamp delta", 422, map[string]any{
			"bucket": name, "rate_bps": rate, "delta_ms": deltaMS,
		})
	}
	if deltaMS != 0 && rate > math.MaxInt64/deltaMS {
		return tokens, carry, RefillRounding{}, NewError(CodeSimulationFailed, "refill arithmetic overflow", 422, map[string]any{
			"bucket": name, "rate_bps": rate, "delta_ms": deltaMS,
		})
	}
	raw := rate * deltaMS
	if raw > math.MaxInt64-999 {
		return tokens, carry, RefillRounding{}, NewError(CodeSimulationFailed, "refill carry arithmetic overflow", 422, map[string]any{
			"bucket": name,
		})
	}
	total := carry + raw
	whole := total / 1000
	nextCarry := total % 1000
	room := capacity - tokens
	if room < 0 {
		room = 0
	}
	added := whole
	if added > room {
		added = room
	}
	overflow := whole - added
	tokens += added
	ev := RefillRounding{
		Bucket: name, RateBPS: rate, DeltaMS: deltaMS, RawMilliBits: raw,
		CarryBeforeMilliBits: carry, WholeBits: whole, CarryAfterMilliBits: nextCarry,
		CumulativeErrorMilliBits: nextCarry, CapacityOverflowBits: overflow,
		Explanation: fmt.Sprintf("%d bps * %d ms = %d millibits; (%d carry + %d raw)/1000 = %d whole bits, carry %d millibits; added %d bits, overflow %d bits",
			rate, deltaMS, raw, carry, raw, whole, nextCarry, added, overflow),
	}
	return tokens, nextCarry, ev, nil
}
