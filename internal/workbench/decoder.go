package workbench

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"unsafe"
)

type DecodeLimits struct {
	MaxDepth     int
	NestedBudget int64
}

type memoryMeter struct {
	live        int64
	peak        int64
	allocations int64
}

func (m *memoryMeter) alloc(n int64) {
	if n <= 0 {
		return
	}
	m.live += n
	m.allocations++
	if m.live > m.peak {
		m.peak = m.live
	}
}

func (m *memoryMeter) free(n int64) {
	if n <= 0 {
		return
	}
	m.live -= n
	if m.live < 0 {
		m.live = 0
	}
}

type traceLog struct {
	events []TraceEvent
	task   *memoryMeter
	record *memoryMeter
}

func newTraceLog(task *memoryMeter) *traceLog {
	return &traceLog{events: []TraceEvent{}, task: task}
}

func (t *traceLog) setRecord(record *memoryMeter) {
	t.record = record
}

func (t *traceLog) push(event TraceEvent) {
	event.Seq = int64(len(t.events) + 1)
	if len(t.events) == cap(t.events) {
		oldCap := cap(t.events)
		newCap := oldCap * 2
		if newCap == 0 {
			newCap = 8
		}
		size := int64(unsafe.Sizeof(TraceEvent{}))
		t.task.alloc(int64(newCap) * size)
		if t.record != nil {
			t.record.alloc(int64(newCap) * size)
		}
		if oldCap > 0 {
			t.task.free(int64(oldCap) * size)
			if t.record != nil {
				t.record.free(int64(oldCap) * size)
			}
		}
		next := make([]TraceEvent, len(t.events), newCap)
		copy(next, t.events)
		t.events = next
	}
	t.events = append(t.events, event)
}

type issueLog struct {
	items  []Issue
	task   *memoryMeter
	record *memoryMeter
}

func newIssueLog(task *memoryMeter) *issueLog {
	return &issueLog{items: []Issue{}, task: task}
}

func (l *issueLog) setRecord(record *memoryMeter) {
	l.record = record
}

func (l *issueLog) push(issue Issue) {
	if len(l.items) == cap(l.items) {
		oldCap := cap(l.items)
		newCap := oldCap * 2
		if newCap == 0 {
			newCap = 4
		}
		size := int64(unsafe.Sizeof(Issue{}))
		l.task.alloc(int64(newCap) * size)
		if l.record != nil {
			l.record.alloc(int64(newCap) * size)
		}
		if oldCap > 0 {
			l.task.free(int64(oldCap) * size)
			if l.record != nil {
				l.record.free(int64(oldCap) * size)
			}
		}
		next := make([]Issue, len(l.items), newCap)
		copy(next, l.items)
		l.items = next
	}
	l.items = append(l.items, issue)
}

type decodeFatal struct {
	code        string
	detail      string
	recordIndex int
	recordID    uint16
	offset      int64
	parentEnd   int64
	declared    uint64
	peakBytes   int64
	allocations int64
}

type decoder struct {
	data     []byte
	limits   DecodeLimits
	task     *memoryMeter
	record   *memoryMeter
	trace    *traceLog
	issues   *issueLog
	records  []RecordSummary
	code     string
	topIndex int
}

func DecodeContainer(data []byte, limits DecodeLimits) DecodeResult {
	digest := sha256.Sum256(data)
	result := DecodeResult{
		Code:            CodeOK,
		ContainerDigest: hex.EncodeToString(digest[:]),
		ContainerBytes:  int64(len(data)),
		Records:         []RecordSummary{},
		Issues:          []Issue{},
		Trajectory:      []TraceEvent{},
	}
	taskMeter := &memoryMeter{}
	d := &decoder{
		data:    data,
		limits:  limits,
		task:    taskMeter,
		trace:   newTraceLog(taskMeter),
		issues:  newIssueLog(taskMeter),
		records: []RecordSummary{},
		code:    CodeOK,
	}
	if len(data) < containerHeaderSize {
		result.Code = CodeTruncated
		result.Failure = &FailureSummary{Code: CodeTruncated, Detail: "container header is truncated", ParentEnd: int64(len(data))}
		result.PeakBytes = taskMeter.peak
		result.Allocations = taskMeter.allocations
		return result
	}
	if [4]byte(data[0:4]) != containerMagic || data[4] != 1 {
		result.Code = CodeTruncated
		result.Failure = &FailureSummary{Code: CodeTruncated, Detail: "container magic or version is invalid", ParentEnd: int64(len(data))}
		result.PeakBytes = taskMeter.peak
		result.Allocations = taskMeter.allocations
		return result
	}
	declaredRecords := int(binary.BigEndian.Uint16(data[5:7]))
	declaredBody := binary.BigEndian.Uint64(data[7:15])
	result.DeclaredRecords = declaredRecords
	bodyEnd, code := checkedEnd(containerHeaderSize, declaredBody, int64(len(data)))
	if code != CodeOK {
		result.Code = code
		result.Failure = &FailureSummary{Code: code, Offset: containerHeaderSize, ParentEnd: int64(len(data)), DeclaredLength: declaredBody, Detail: endErrorDetail(code, "container body")}
		result.PeakBytes = taskMeter.peak
		result.Allocations = taskMeter.allocations
		return result
	}
	if bodyEnd != int64(len(data)) {
		result.Code = CodeTruncated
		result.Failure = &FailureSummary{Code: CodeTruncated, Offset: bodyEnd, ParentEnd: int64(len(data)), DeclaredLength: declaredBody, Detail: "container declared length does not consume the complete input"}
		result.PeakBytes = taskMeter.peak
		result.Allocations = taskMeter.allocations
		return result
	}
	maxPossible := (bodyEnd - containerHeaderSize) / recordHeaderSize
	if int64(declaredRecords) > maxPossible {
		result.Code = CodeTruncated
		result.Failure = &FailureSummary{Code: CodeTruncated, Offset: containerHeaderSize, ParentEnd: bodyEnd, Detail: "declared record count cannot fit in container body"}
		result.PeakBytes = taskMeter.peak
		result.Allocations = taskMeter.allocations
		return result
	}
	recordSize := int64(unsafe.Sizeof(RecordSummary{}))
	taskMeter.alloc(int64(declaredRecords) * recordSize)
	d.records = make([]RecordSummary, 0, declaredRecords)

	cursor := int64(containerHeaderSize)
	for index := 0; index < declaredRecords; index++ {
		if cursor >= bodyEnd {
			d.fail(CodeTruncated, index, 0, cursor, bodyEnd, 0, "record stream ended before declared record count")
			result.Failure = &FailureSummary{
				RecordIndex: index,
				Code:        CodeTruncated,
				Offset:      cursor,
				ParentEnd:   bodyEnd,
				Detail:      "record stream ended before declared record count",
				PeakBytes:   taskMeter.peak,
				Allocations: taskMeter.allocations,
			}
			break
		}
		d.topIndex = index
		d.record = &memoryMeter{}
		d.trace.setRecord(d.record)
		d.issues.setRecord(d.record)
		summary, next, fatal := d.parseRecord(cursor, bodyEnd, 1, index)
		if fatal != nil {
			d.trace.push(TraceEvent{Action: "record_fail", RecordIndex: index, RecordID: fatal.recordID, Depth: 1, Offset: fatal.offset, EndOffset: fatal.parentEnd, Code: fatal.code, Detail: fatal.detail})
			d.code = WorseCode(d.code, fatal.code)
			result.Failure = &FailureSummary{
				RecordIndex:    fatal.recordIndex,
				RecordID:       fatal.recordID,
				Code:           fatal.code,
				Offset:         fatal.offset,
				ParentEnd:      fatal.parentEnd,
				DeclaredLength: fatal.declared,
				Detail:         fatal.detail,
				PeakBytes:      d.record.peak,
				Allocations:    d.record.allocations,
			}
			cursor = next
			break
		}
		d.records = append(d.records, summary)
		d.code = WorseCode(d.code, summary.Code)
		d.trace.push(TraceEvent{Action: "record_complete", RecordIndex: index, RecordID: summary.RecordID, Depth: 1, Offset: cursor, EndOffset: next, Code: summary.Code, Detail: "record committed to safe prefix"})
		cursor = next
	}
	if result.Failure == nil && cursor != bodyEnd {
		d.fail(CodeTruncated, len(d.records), 0, cursor, bodyEnd, 0, "record bytes remain after declared record count")
		result.Failure = &FailureSummary{
			RecordIndex: len(d.records),
			Code:        CodeTruncated,
			Offset:      cursor,
			ParentEnd:   bodyEnd,
			Detail:      "record bytes remain after declared record count",
			PeakBytes:   taskMeter.peak,
			Allocations: taskMeter.allocations,
		}
		d.code = WorseCode(d.code, CodeTruncated)
	}
	result.Code = d.code
	result.SafePrefix = len(d.records)
	result.Records = d.records
	result.Issues = d.issues.items
	result.Trajectory = d.trace.events
	result.PeakBytes = taskMeter.peak
	result.Allocations = taskMeter.allocations
	d.record = nil
	d.trace.setRecord(nil)
	d.issues.setRecord(nil)
	return result
}

func (d *decoder) parseRecord(start, bound int64, depth, topIndex int) (RecordSummary, int64, *decodeFatal) {
	if !hasBytes(start, recordHeaderSize, bound) {
		return RecordSummary{}, start, d.fatal(CodeTruncated, topIndex, 0, start, bound, 0, "record header is truncated")
	}
	id := binary.BigEndian.Uint16(d.data[start : start+2])
	declared := binary.BigEndian.Uint64(d.data[start+2 : start+10])
	bodyStart := start + recordHeaderSize
	bodyEnd, code := checkedEnd(bodyStart, declared, bound)
	if code != CodeOK {
		return RecordSummary{}, bound, d.fatal(code, topIndex, id, bodyStart, bound, declared, endErrorDetail(code, "record body"))
	}
	d.trace.push(TraceEvent{Action: "record_start", RecordIndex: topIndex, RecordID: id, Depth: depth, Offset: start, EndOffset: bodyEnd, Detail: "length-prefixed record"})
	if !hasBytes(bodyStart, 2, bodyEnd) {
		return RecordSummary{}, bodyEnd, d.fatal(CodeTruncated, topIndex, id, bodyStart, bodyEnd, declared, "record field count is truncated")
	}
	fieldCount := int(binary.BigEndian.Uint16(d.data[bodyStart : bodyStart+2]))
	cursor := bodyStart + 2
	summary := RecordSummary{
		Index:          topIndex,
		RecordID:       id,
		DeclaredLength: declared,
		BodyBytes:      bodyEnd - bodyStart,
		FieldCount:     fieldCount,
		Code:           CodeOK,
		MaxDepth:       depth,
	}
	recordCode := CodeOK
	var nestedCumulative uint64
	for fieldIndex := 0; fieldIndex < fieldCount; fieldIndex++ {
		fieldStart := cursor
		if !hasBytes(fieldStart, fieldHeaderSize, bodyEnd) {
			return RecordSummary{}, bodyEnd, d.fatal(CodeTruncated, topIndex, id, fieldStart, bodyEnd, 0, "field header is truncated")
		}
		kind := d.data[fieldStart]
		declaredField := binary.BigEndian.Uint64(d.data[fieldStart+1 : fieldStart+9])
		payloadStart := fieldStart + fieldHeaderSize
		d.trace.push(TraceEvent{Action: "field_start", RecordIndex: topIndex, RecordID: id, Depth: depth, Offset: fieldStart, EndOffset: payloadStart, Detail: fieldKindName(kind)})
		switch kind {
		case 1:
			payloadEnd, fieldCode := checkedEnd(payloadStart, declaredField, bodyEnd)
			if fieldCode != CodeOK {
				return RecordSummary{}, bodyEnd, d.fatal(fieldCode, topIndex, id, payloadStart, bodyEnd, declaredField, endErrorDetail(fieldCode, "variable field"))
			}
			size := payloadEnd - payloadStart
			if size > 0 {
				d.record.alloc(size)
				d.task.alloc(size)
				buf := make([]byte, size)
				copy(buf, d.data[payloadStart:payloadEnd])
				d.record.free(size)
				d.task.free(size)
			}
			summary.PayloadBytes += size
			cursor = payloadEnd
		case 2:
			summary.NestedFields++
			if depth+1 > d.limits.MaxDepth {
				termination := displayEnd(payloadStart, declaredField)
				d.issue(CodeDepthExceeded, topIndex, id, depth+1, fieldStart, bodyEnd, declaredField, 0, uint64(d.limits.MaxDepth), "nested child depth exceeds configured maximum")
				d.trace.push(TraceEvent{Action: "subtree_abort", RecordIndex: topIndex, RecordID: id, Depth: depth + 1, Offset: payloadStart, EndOffset: termination, Code: CodeDepthExceeded, Detail: "depth limit reached; subtree skipped"})
				d.trace.push(TraceEvent{Action: "parent_recover", RecordIndex: topIndex, RecordID: id, Depth: depth, Offset: bodyEnd, EndOffset: bodyEnd, Code: CodeDepthExceeded, Detail: "resume at parent record boundary"})
				recordCode = WorseCode(recordCode, CodeDepthExceeded)
				cursor = bodyEnd
				fieldIndex = fieldCount
				continue
			}
			nextCumulative, addOK := addU64(nestedCumulative, declaredField)
			if !addOK {
				return RecordSummary{}, bodyEnd, d.fatal(CodeLengthOverflow, topIndex, id, fieldStart, bodyEnd, declaredField, "nested declaration accumulation overflows uint64")
			}
			nestedCumulative = nextCumulative
			parentRemaining := uint64(bodyEnd - fieldStart)
			limit := parentRemaining
			if d.limits.NestedBudget >= 0 && uint64(d.limits.NestedBudget) < limit {
				limit = uint64(d.limits.NestedBudget)
			}
			if nestedCumulative > limit {
				d.issue(CodeNestedBudget, topIndex, id, depth, fieldStart, bodyEnd, declaredField, nestedCumulative, limit, "nested declarations exceed parent remainder or configured cumulative budget")
				d.trace.push(TraceEvent{Action: "subtree_abort", RecordIndex: topIndex, RecordID: id, Depth: depth, Offset: payloadStart, EndOffset: bodyEnd, Code: CodeNestedBudget, Detail: "nested cumulative budget reached; subtree skipped"})
				d.trace.push(TraceEvent{Action: "parent_recover", RecordIndex: topIndex, RecordID: id, Depth: depth, Offset: bodyEnd, EndOffset: bodyEnd, Code: CodeNestedBudget, Detail: "resume at parent record boundary"})
				recordCode = WorseCode(recordCode, CodeNestedBudget)
				cursor = bodyEnd
				fieldIndex = fieldCount
				continue
			}
			childEnd, fieldCode := checkedEnd(payloadStart, declaredField, bodyEnd)
			if fieldCode != CodeOK {
				return RecordSummary{}, bodyEnd, d.fatal(fieldCode, topIndex, id, payloadStart, bodyEnd, declaredField, endErrorDetail(fieldCode, "nested field"))
			}
			if !hasBytes(payloadStart, nestedHeaderSize, childEnd) {
				return RecordSummary{}, bodyEnd, d.fatal(CodeTruncated, topIndex, id, payloadStart, childEnd, declaredField, "nested child count is truncated")
			}
			childCount := int(binary.BigEndian.Uint16(d.data[payloadStart : payloadStart+2]))
			childCursor := payloadStart + nestedHeaderSize
			childRegionStart := childCursor
			var childCumulative uint64
			decodedChildren := 0
			for childIndex := 0; childIndex < childCount; childIndex++ {
				if !hasBytes(childCursor, recordHeaderSize, childEnd) {
					return RecordSummary{}, bodyEnd, d.fatal(CodeTruncated, topIndex, id, childCursor, childEnd, 0, "nested child record header is truncated")
				}
				childID := binary.BigEndian.Uint16(d.data[childCursor : childCursor+2])
				childDeclared := binary.BigEndian.Uint64(d.data[childCursor+2 : childCursor+10])
				nextChildCumulative, addOK := addU64(childCumulative, childDeclared)
				if !addOK {
					return RecordSummary{}, bodyEnd, d.fatal(CodeLengthOverflow, topIndex, childID, childCursor, childEnd, childDeclared, "child record length accumulation overflows uint64")
				}
				childCumulative = nextChildCumulative
				childRemaining := uint64(childEnd - childRegionStart)
				childLimit := childRemaining
				if d.limits.NestedBudget >= 0 && uint64(d.limits.NestedBudget) < childLimit {
					childLimit = uint64(d.limits.NestedBudget)
				}
				if childCumulative > childLimit {
					d.issue(CodeNestedBudget, topIndex, childID, depth+1, childCursor, childEnd, childDeclared, childCumulative, childLimit, "child record declarations exceed nested region remainder")
					d.trace.push(TraceEvent{Action: "subtree_abort", RecordIndex: topIndex, RecordID: childID, Depth: depth + 1, Offset: childCursor, EndOffset: childEnd, Code: CodeNestedBudget, Detail: "nested region skipped after cumulative budget"})
					d.trace.push(TraceEvent{Action: "parent_recover", RecordIndex: topIndex, RecordID: id, Depth: depth, Offset: childEnd, EndOffset: childEnd, Code: CodeNestedBudget, Detail: "resume after nested region"})
					recordCode = WorseCode(recordCode, CodeNestedBudget)
					childCursor = childEnd
					break
				}
				childSummary, childNext, childFatal := d.parseRecord(childCursor, childEnd, depth+1, topIndex)
				if childFatal != nil {
					return RecordSummary{}, bodyEnd, childFatal
				}
				recordCode = WorseCode(recordCode, childSummary.Code)
				summary.PayloadBytes += childSummary.PayloadBytes
				decodedChildren++
				if childSummary.MaxDepth > summary.MaxDepth {
					summary.MaxDepth = childSummary.MaxDepth
				}
				childCursor = childNext
			}
			if childCursor != childEnd {
				return RecordSummary{}, bodyEnd, d.fatal(CodeTruncated, topIndex, id, childCursor, childEnd, declaredField, "nested field declared length was not fully consumed")
			}
			summary.ChildRecords += decodedChildren
			if depth+1 > summary.MaxDepth {
				summary.MaxDepth = depth + 1
			}
			cursor = childEnd
		default:
			return RecordSummary{}, bodyEnd, d.fatal(CodeTruncated, topIndex, id, fieldStart, bodyEnd, declaredField, "unknown field tag")
		}
	}
	if cursor != bodyEnd {
		return RecordSummary{}, bodyEnd, d.fatal(CodeTruncated, topIndex, id, cursor, bodyEnd, declared, "record declared length was not fully consumed")
	}
	digest := sha256.Sum256(d.data[start:bodyEnd])
	summary.Digest = hex.EncodeToString(digest[:])
	d.record.alloc(64)
	d.task.alloc(64)
	summary.Code = recordCode
	summary.PeakBytes = d.record.peak
	summary.Allocations = d.record.allocations
	return summary, bodyEnd, nil
}

func (d *decoder) fatal(code string, topIndex int, id uint16, offset, parentEnd int64, declared uint64, detail string) *decodeFatal {
	return &decodeFatal{
		code:        code,
		detail:      detail,
		recordIndex: topIndex,
		recordID:    id,
		offset:      offset,
		parentEnd:   parentEnd,
		declared:    declared,
		peakBytes:   d.record.peak,
		allocations: d.record.allocations,
	}
}

func (d *decoder) issue(code string, topIndex int, id uint16, depth int, offset, parentEnd int64, declared, cumulative, limit uint64, detail string) {
	d.issues.push(Issue{
		Code:        code,
		RecordIndex: topIndex,
		RecordID:    id,
		Depth:       depth,
		Offset:      offset,
		ParentEnd:   parentEnd,
		Declared:    declared,
		Cumulative:  cumulative,
		Limit:       limit,
		Detail:      detail,
	})
}

func (d *decoder) fail(code string, topIndex int, id uint16, offset, parentEnd int64, declared uint64, detail string) {
	d.code = WorseCode(d.code, code)
	d.trace.push(TraceEvent{Action: "record_fail", RecordIndex: topIndex, RecordID: id, Depth: 1, Offset: offset, EndOffset: parentEnd, Code: code, Detail: detail})
}

func checkedEnd(offset int64, length uint64, bound int64) (int64, string) {
	if offset < 0 || bound < offset {
		return bound, CodeTruncated
	}
	uoffset := uint64(offset)
	if length > math.MaxUint64-uoffset {
		return bound, CodeLengthWrap
	}
	sum := uoffset + length
	if sum > math.MaxInt64 {
		return bound, CodeLengthOverflow
	}
	end := int64(sum)
	if end > bound {
		return bound, CodeTruncated
	}
	return end, CodeOK
}

func addU64(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return math.MaxUint64, false
	}
	return a + b, true
}

func displayEnd(offset int64, length uint64) int64 {
	if offset < 0 {
		return 0
	}
	if length > uint64(math.MaxInt64-offset) {
		return math.MaxInt64
	}
	return offset + int64(length)
}

func hasBytes(offset int64, length, bound int64) bool {
	return offset >= 0 && length >= 0 && offset <= bound && length <= bound-offset
}

func endErrorDetail(code, scope string) string {
	switch code {
	case CodeLengthWrap:
		return scope + " length addition wraps the unsigned offset"
	case CodeLengthOverflow:
		return scope + " length sum exceeds the signed addressable range"
	default:
		return scope + " length exceeds the available parent bytes"
	}
}

func fieldKindName(kind byte) string {
	switch kind {
	case 1:
		return "variable field"
	case 2:
		return "nested record block"
	default:
		return "unknown field"
	}
}
