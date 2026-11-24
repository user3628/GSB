package workbench

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	containerHeaderSize = 15
	recordHeaderSize    = 10
	fieldHeaderSize     = 9
	nestedHeaderSize    = 2
)

var containerMagic = [4]byte{'R', 'X', 'C', '1'}

func GenerateTasks(cfg Config) ([]TaskSpec, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var tasks []TaskSpec
	switch cfg.Scenario {
	case BaselineScenario:
		tasks = []TaskSpec{
			{TaskID: "t01-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "valid length-prefixed records"},
			{TaskID: "t02-truncated", SampleKind: SampleTruncated, DeclaredBudget: 32768, Description: "truncated variable field"},
			{TaskID: "t03-length-wrap", SampleKind: SampleLengthWrap, DeclaredBudget: 32768, Description: "unsigned length addition wraps"},
			{TaskID: "t04-length-overflow", SampleKind: SampleLengthOverflow, DeclaredBudget: 32768, Description: "length sum exceeds signed addressable range"},
			{TaskID: "t05-depth", SampleKind: SampleDepth, DeclaredBudget: 32768, Description: "nested records exceed depth limit"},
			{TaskID: "t06-nested-budget", SampleKind: SampleNestedBudget, DeclaredBudget: 32768, Description: "nested declarations exceed parent remainder"},
			{TaskID: "t07-task-budget", SampleKind: SampleValid, DeclaredBudget: 8, Description: "valid sample with deliberately tiny task budget"},
			{TaskID: "t08-global-budget", SampleKind: SampleValid, DeclaredBudget: 120000, Description: "declared budget cannot fit remaining global budget"},
			{TaskID: "t09-valid-after", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "valid task after admitted and rejected work"},
		}
	case "inject-length-wrap":
		tasks = injectionTasks(cfg, SampleLengthWrap, 32768)
	case "inject-deep-nesting":
		tasks = injectionTasks(cfg, SampleDepth, 32768)
	case "inject-task-budget":
		tasks = injectionTasks(cfg, SampleValid, 8)
	case "inject-global-budget":
		tasks = injectionTasks(cfg, SampleValid, 60000)
	case "inject-snapshot-gap":
		tasks = []TaskSpec{
			{TaskID: "gap-a-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "snapshot recovery control A"},
			{TaskID: "gap-b-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "snapshot recovery control B"},
			{TaskID: "gap-c-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "snapshot recovery control C"},
			{TaskID: "gap-d-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "snapshot recovery control D"},
			{TaskID: "gap-e-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "snapshot recovery control E"},
		}
	default:
		return nil, fmt.Errorf("unsupported scenario %q", cfg.Scenario)
	}
	return tasks, nil
}

func injectionTasks(cfg Config, injectedKind string, injectedBudget int64) []TaskSpec {
	kind := SampleValid
	budget := int64(32768)
	if cfg.Injection {
		kind = injectedKind
		budget = injectedBudget
	}
	return []TaskSpec{
		{TaskID: "ctrl-a-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "control task before injection"},
		{TaskID: "inject-target", SampleKind: kind, DeclaredBudget: budget, Description: "external injection target"},
		{TaskID: "ctrl-z-valid", SampleKind: SampleValid, DeclaredBudget: 32768, Description: "control task after injection"},
	}
}

func BuildSample(spec TaskSpec, cfg Config) []byte {
	switch spec.SampleKind {
	case SampleTruncated:
		return buildTruncatedSample(cfg)
	case SampleLengthWrap:
		return buildWrapSample(cfg)
	case SampleLengthOverflow:
		return buildOverflowSample(cfg)
	case SampleDepth:
		return buildDepthSample(cfg)
	case SampleNestedBudget:
		return buildNestedBudgetSample(cfg)
	default:
		return buildValidSample(cfg, spec.TaskID)
	}
}

func buildValidSample(cfg Config, taskID string) []byte {
	return encodeContainer(buildValidRecords(cfg, taskID))
}

func buildValidRecords(cfg Config, taskID string) [][]byte {
	seed := cfg.Seed ^ taskSeed(taskID)
	rng := newSplitMix(seed)
	records := make([][]byte, 0, cfg.RecordCount)
	for i := 0; i < cfg.RecordCount; i++ {
		id := uint16(100 + i)
		payload := rng.bytes(cfg.PayloadSize)
		childPayload := rng.bytes(maxInt(4, cfg.PayloadSize/3))
		child := encodeRecord(uint16(300+i), [][]byte{encodeVarField(childPayload)})
		nested := encodeNestedField([][]byte{child})
		tail := rng.bytes(maxInt(2, cfg.PayloadSize/8))
		record := encodeRecord(id, [][]byte{encodeVarField(payload), nested, encodeVarField(tail)})
		records = append(records, record)
	}
	return records
}

func buildTruncatedSample(cfg Config) []byte {
	prefix := buildValidRecords(cfg, "prefix")
	payload := []byte{0x41, 0x42, 0x43, 0x44}
	badField := encodeFieldWithDeclared(1, uint64(len(payload)+32), payload)
	badRecord := encodeRecord(901, [][]byte{badField})
	return encodeContainer(append(append(prefix, badRecord), buildValidRecords(cfg, "suffix")...))
}

func buildWrapSample(cfg Config) []byte {
	prefix := buildValidRecords(cfg, "prefix")
	badField := encodeFieldWithDeclared(1, math.MaxUint64, nil)
	badRecord := encodeRecord(902, [][]byte{badField})
	return encodeContainer(append(prefix, badRecord))
}

func buildOverflowSample(cfg Config) []byte {
	prefix := buildValidRecords(cfg, "prefix")
	badField := encodeFieldWithDeclared(1, uint64(math.MaxInt64)+1, nil)
	badRecord := encodeRecord(903, [][]byte{badField})
	return encodeContainer(append(prefix, badRecord))
}

func buildDepthSample(cfg Config) []byte {
	prefix := buildValidRecords(cfg, "prefix")
	deepDepth := cfg.MaxDepth + 3
	deep := encodeDepthRecord(910, deepDepth, maxInt(4, cfg.PayloadSize/4))
	suffix := buildValidRecords(cfg, "suffix")
	return encodeContainer(append(append(prefix, deep), suffix...))
}

func buildNestedBudgetSample(cfg Config) []byte {
	prefix := buildValidRecords(cfg, "prefix")
	childPayload := splitMixBytes(cfg.Seed^0x9090, maxInt(8, cfg.PayloadSize/2))
	child := encodeRecord(921, [][]byte{encodeVarField(childPayload)})
	firstNested := encodeNestedField([][]byte{child})
	declaredSecond := uint64(cfg.NestedBudget)
	if declaredSecond < 256 {
		declaredSecond = 256
	}
	declaredSecond += 256
	secondNestedHeader := encodeFieldWithDeclared(2, declaredSecond, nil)
	badBody := encodeRecordBody([][]byte{firstNested, secondNestedHeader})
	badRecord := wrapRecord(922, badBody)
	suffix := buildValidRecords(cfg, "suffix")
	return encodeContainer(append(append(prefix, badRecord), suffix...))
}

func encodeDepthRecord(id uint16, depth int, payloadSize int) []byte {
	if depth <= 1 {
		return encodeRecord(id, [][]byte{encodeVarField(splitMixBytes(uint64(id), payloadSize))})
	}
	child := encodeDepthRecord(id+1, depth-1, payloadSize)
	nested := encodeNestedField([][]byte{child})
	return encodeRecord(id, [][]byte{encodeVarField(splitMixBytes(uint64(id), payloadSize)), nested})
}

func encodeContainer(records [][]byte) []byte {
	bodyLen := 0
	for _, record := range records {
		bodyLen += len(record)
	}
	out := make([]byte, containerHeaderSize+bodyLen)
	copy(out[0:4], containerMagic[:])
	out[4] = 1
	binary.BigEndian.PutUint16(out[5:7], uint16(len(records)))
	binary.BigEndian.PutUint64(out[7:15], uint64(bodyLen))
	offset := containerHeaderSize
	for _, record := range records {
		offset += copy(out[offset:], record)
	}
	return out
}

func encodeRecordBody(fields [][]byte) []byte {
	fieldBytes := 0
	for _, field := range fields {
		fieldBytes += len(field)
	}
	body := make([]byte, 2+fieldBytes)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(fields)))
	offset := 2
	for _, field := range fields {
		offset += copy(body[offset:], field)
	}
	return body
}

func encodeRecord(id uint16, fields [][]byte) []byte {
	return wrapRecord(id, encodeRecordBody(fields))
}

func wrapRecord(id uint16, body []byte) []byte {
	out := make([]byte, recordHeaderSize+len(body))
	binary.BigEndian.PutUint16(out[0:2], id)
	binary.BigEndian.PutUint64(out[2:10], uint64(len(body)))
	copy(out[recordHeaderSize:], body)
	return out
}

func encodeVarField(payload []byte) []byte {
	return encodeFieldWithDeclared(1, uint64(len(payload)), payload)
}

func encodeNestedField(children [][]byte) []byte {
	childBytes := 0
	for _, child := range children {
		childBytes += len(child)
	}
	payload := make([]byte, nestedHeaderSize+childBytes)
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(children)))
	offset := nestedHeaderSize
	for _, child := range children {
		offset += copy(payload[offset:], child)
	}
	return encodeFieldWithDeclared(2, uint64(len(payload)), payload)
}

func encodeFieldWithDeclared(kind byte, declared uint64, payload []byte) []byte {
	out := make([]byte, fieldHeaderSize+len(payload))
	out[0] = kind
	binary.BigEndian.PutUint64(out[1:9], declared)
	copy(out[fieldHeaderSize:], payload)
	return out
}

type splitMix struct {
	state uint64
}

func newSplitMix(seed uint64) *splitMix {
	return &splitMix{state: seed}
}

func (s *splitMix) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	z := s.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (s *splitMix) bytes(n int) []byte {
	out := make([]byte, n)
	for i := 0; i < n; i += 8 {
		v := s.next()
		for j := 0; j < 8 && i+j < n; j++ {
			out[i+j] = byte(v >> (8 * j))
		}
	}
	return out
}

func splitMixBytes(seed uint64, n int) []byte {
	return newSplitMix(seed).bytes(n)
}

func taskSeed(taskID string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(taskID); i++ {
		h ^= uint64(taskID[i])
		h *= 1099511628211
	}
	return h
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
