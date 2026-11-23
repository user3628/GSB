// Package model defines the unified internal event model and tracing types.
package model

import (
	"encoding/json"
	"fmt"
)

// Op is the operation type of an event.
type Op uint8

const (
	OpCrossTableCopy     Op = iota // 跨表复制
	OpCrossServiceExport           // 跨服务导出
	OpAggregateWrite               // 聚合写
)

func (o Op) String() string {
	switch o {
	case OpCrossTableCopy:
		return "COPY_XTABLE"
	case OpCrossServiceExport:
		return "EXPORT_XSVC"
	case OpAggregateWrite:
		return "AGG_WRITE"
	}
	return fmt.Sprintf("OP_UNKNOWN(%d)", uint8(o))
}

// OpFromString parses an operation name.
func OpFromString(s string) (Op, bool) {
	switch s {
	case "COPY_XTABLE":
		return OpCrossTableCopy, true
	case "EXPORT_XSVC":
		return OpCrossServiceExport, true
	case "AGG_WRITE":
		return OpAggregateWrite, true
	}
	return 0, false
}

// MarshalJSON renders the op as its stable string name in reports.
func (o Op) MarshalJSON() ([]byte, error) { return json.Marshal(o.String()) }

// UnmarshalJSON accepts the string op name.
func (o *Op) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return err
	}
	op, ok := OpFromString(name)
	if !ok {
		return fmt.Errorf("unknown op %q", name)
	}
	*o = op
	return nil
}

// Event is the unified internal event model produced by all parsers.
type Event struct {
	Seq         uint64 `json:"seq"`
	Subject     string `json:"subject"`
	SrcField    string `json:"src"`
	DstField    string `json:"dst"`
	Op          Op     `json:"op"`
	LogicalTime uint64 `json:"ts"`
}

// EventJSON mirrors Event for JSONL input with string op names.
type EventJSON struct {
	Seq         *uint64 `json:"seq"`
	Subject     *string `json:"subject"`
	SrcField    *string `json:"src"`
	DstField    *string `json:"dst"`
	Op          *string `json:"op"`
	LogicalTime *uint64 `json:"ts"`
}

// Judgment is a per-event adjudication result.
type Judgment struct {
	EventSeq uint64   `json:"event_seq"`
	DstField string   `json:"dst_field"`
	Code     string   `json:"code"`
	Marks    []string `json:"marks,omitempty"`
	Path     []string `json:"path,omitempty"` // minimal escape path (field chain)
	Version  int      `json:"version"`
}

// Correction records a forward-only judgment revision.
type Correction struct {
	EventSeq uint64 `json:"event_seq"`
	DstField string `json:"dst_field"`
	FromCode string `json:"from_code"`
	ToCode   string `json:"to_code"`
	Version  int    `json:"version"`
}

// Key identifies a judgment by event sequence and target field.
func Key(seq uint64, dst string) string {
	return fmt.Sprintf("%d|%s", seq, dst)
}
