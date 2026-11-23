package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"secverify/internal/model"
)

const ChangeHeader = "SECCHANGE/1\n"
const maxBlockBytes = 4 << 20

type Baseline struct {
	Namespace string       `json:"namespace"`
	Items     []model.Item `json:"items"`
}

type rawBaseline struct {
	Namespace string            `json:"namespace"`
	Items     []json.RawMessage `json:"items"`
}

// ParseBaseline parses and normalizes all items. Dependency existence and
// cycle checks are intentionally a separate graph phase because they need the
// complete candidate state after a change set is applied.
func ParseBaseline(data []byte) (*Baseline, *model.BizError) {
	var env rawBaseline
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, jsonError(err, "", -1)
	}
	if !validName(env.Namespace) || strings.TrimSpace(env.Namespace) != env.Namespace {
		return nil, parseErr("", 0, "baseline namespace is required")
	}
	if len(env.Items) == 0 {
		return nil, parseErr("", 0, "baseline must contain at least one item")
	}

	items := make([]model.Item, 0, len(env.Items))
	seen := make(map[string]struct{}, len(env.Items))
	searchFrom := 0
	for _, raw := range env.Items {
		offset := findRawOffset(data, raw, searchFrom)
		if offset >= 0 {
			searchFrom = offset + len(raw)
		}
		var idProbe struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(raw, &idProbe)
		var item model.Item
		if err := strictUnmarshal(raw, &item); err != nil {
			return nil, jsonError(err, idProbe.ID, offset)
		}
		item.SourceOffset = offset
		norm, bizErr := NormalizeItem(item)
		if bizErr != nil {
			return nil, bizErr
		}
		if _, ok := seen[norm.ID]; ok {
			return nil, parseErr(norm.ID, offset, "duplicate item id")
		}
		seen[norm.ID] = struct{}{}
		items = append(items, norm)
	}
	model.SortItems(items)
	return &Baseline{Namespace: env.Namespace, Items: items}, nil
}

func findRawOffset(data, raw []byte, from int) int {
	if from < 0 || from > len(data) {
		from = 0
	}
	idx := bytes.Index(data[from:], raw)
	if idx >= 0 {
		return from + idx
	}
	if idx = bytes.Index(data, raw); idx >= 0 {
		return idx
	}
	return 0
}

func jsonError(err error, id string, fallback int) *model.BizError {
	offset := fallback
	switch e := err.(type) {
	case *json.SyntaxError:
		offset = int(e.Offset) - 1
	case *json.UnmarshalTypeError:
		offset = int(e.Offset) - 1
	}
	if offset < 0 {
		offset = 0
	}
	return parseErr(id, offset, err.Error())
}

type Operation struct {
	Op            string
	ID            string
	Item          *model.Item
	SourceOffset  int
	PayloadOffset int
}

type ChangeSet struct {
	Raw        []byte
	Operations []Operation
	Hash       string
}

type rawOperation struct {
	Op   string      `json:"op"`
	ID   string      `json:"id,omitempty"`
	Item *model.Item `json:"item,omitempty"`
}

// ParseChangeSet parses frames of the form:
//
//	SECCHANGE/1\n
//	BLOCK <decimal-byte-length>\n<exact payload bytes>\n
//
// Offsets in errors refer to the zero-based byte offset of the payload in the
// original input. Parsing is all-or-nothing.
func ParseChangeSet(data []byte) (*ChangeSet, *model.BizError) {
	if !bytes.HasPrefix(data, []byte(ChangeHeader)) {
		return nil, parseErr("", 0, "missing SECCHANGE/1 header")
	}
	pos := len(ChangeHeader)
	ops := make([]Operation, 0)
	for pos < len(data) {
		if pos == len(data)-1 && data[pos] == '\n' {
			break
		}
		frameStart := pos
		lineEnd := bytes.IndexByte(data[pos:], '\n')
		if lineEnd < 0 {
			return nil, parseErr("", pos, "unterminated BLOCK length line")
		}
		lineEnd += pos
		line := string(data[pos:lineEnd])
		if !strings.HasPrefix(line, "BLOCK ") {
			return nil, parseErr("", pos, "expected BLOCK length prefix")
		}
		lengthText := strings.TrimSpace(strings.TrimPrefix(line, "BLOCK "))
		length, err := strconv.Atoi(lengthText)
		if err != nil || length < 0 || length > maxBlockBytes {
			return nil, parseErr("", pos, "invalid BLOCK length")
		}
		payloadStart := lineEnd + 1
		payloadEnd := payloadStart + length
		if payloadEnd > len(data) {
			return nil, parseErr("", payloadStart, "truncated BLOCK payload")
		}
		if payloadEnd == len(data) || data[payloadEnd] != '\n' {
			return nil, parseErr("", payloadEnd, "BLOCK payload must end with newline")
		}
		payload := data[payloadStart:payloadEnd]
		var raw rawOperation
		if err := strictUnmarshal(payload, &raw); err != nil {
			return nil, jsonError(err, raw.ID, payloadStart)
		}
		raw.Op = strings.ToLower(strings.TrimSpace(raw.Op))
		switch raw.Op {
		case "add", "modify":
			if raw.Item == nil {
				return nil, parseErr(raw.ID, payloadStart, "add/modify requires item payload")
			}
			if raw.ID != "" && raw.Item.ID != "" && raw.ID != raw.Item.ID {
				return nil, parseErr(raw.ID, payloadStart, "operation id does not match item id")
			}
			if raw.Item.ID == "" {
				raw.Item.ID = raw.ID
			}
			if raw.ID == "" {
				raw.ID = raw.Item.ID
			}
			raw.Item.SourceOffset = payloadStart
			norm, bizErr := NormalizeItem(*raw.Item)
			if bizErr != nil {
				return nil, bizErr
			}
			raw.Item = &norm
		case "delete":
			if raw.ID == "" || raw.Item != nil {
				return nil, parseErr(raw.ID, payloadStart, "delete requires id and no item payload")
			}
			if !validID(raw.ID) {
				return nil, parseErr(raw.ID, payloadStart, "invalid delete id")
			}
		default:
			return nil, parseErr(raw.ID, payloadStart, "operation must be add, modify, or delete")
		}
		ops = append(ops, Operation{
			Op:            raw.Op,
			ID:            raw.ID,
			Item:          raw.Item,
			SourceOffset:  frameStart,
			PayloadOffset: payloadStart,
		})
		pos = payloadEnd + 1
	}
	if len(ops) == 0 {
		return nil, parseErr("", len(ChangeHeader), "change set contains no blocks")
	}
	return &ChangeSet{Raw: append([]byte(nil), data...), Operations: ops, Hash: model.HashBytes(data)}, nil
}

func strictUnmarshal(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// EncodeFrames builds a valid length-prefixed change set from already encoded
// JSON payloads. It is used by the deterministic sample generator and tests.
func EncodeFrames(payloads [][]byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(ChangeHeader)
	for _, payload := range payloads {
		fmt.Fprintf(&buf, "BLOCK %d\n", len(payload))
		buf.Write(payload)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func MustPayload(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
