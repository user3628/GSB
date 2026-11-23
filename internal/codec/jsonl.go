package codec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"time"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/model"
)

type JSONLOptions struct {
	// StatsSeparate writes one operator line and one statistics line per bound
	// operator, matching the sharded JSON-lines interchange shape.
	StatsSeparate bool
}

type jsonlMeta struct {
	Record           string   `json:"record"`
	Format           string   `json:"format"`
	FormatVersion    int      `json:"format_version"`
	SnapshotSeq      uint64   `json:"snapshot_seq"`
	GeneratedAt      string   `json:"generated_at"`
	RequiredFeatures []string `json:"required_features,omitempty"`
}

type jsonlOperator struct {
	Record    string              `json:"record"`
	ID        string              `json:"id"`
	Type      model.OperatorType  `json:"type"`
	Parent    string              `json:"parent"`
	Children  []string            `json:"children,omitempty"`
	Columns   []model.ColumnRef   `json:"columns,omitempty"`
	Predicate *model.Predicate    `json:"predicate,omitempty"`
	Stats     *model.StatsBinding `json:"stats,omitempty"`
}

type jsonlStats struct {
	Record     string           `json:"record"`
	OperatorID string           `json:"operator_id"`
	RowCount   int64            `json:"row_count"`
	Distinct   map[string]int64 `json:"distinct,omitempty"`
}

type rawJSONLOperator struct {
	lineNo   int
	lineCRC  uint32
	record   jsonlOperator
	hasCols  bool
	hasStats bool
	hasChild bool
}

func EncodeJSONL(w io.Writer, snapshot *model.Snapshot, opts JSONLOptions) error {
	if snapshot == nil {
		return bizerr.New(bizerr.CodeBadRequest, "cannot encode a nil snapshot", nil)
	}
	parents := make(map[string]string, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node == nil {
			return bizerr.New(bizerr.CodePlanInvalid, "cannot encode nil operator", nil)
		}
		for _, child := range node.Children {
			parents[child] = node.ID
		}
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	meta := jsonlMeta{
		Record: "snapshot", Format: "jsonl", FormatVersion: snapshot.FormatVersion,
		SnapshotSeq: snapshot.SnapshotSeq, GeneratedAt: snapshot.GeneratedAt.UTC().Format(time.RFC3339Nano),
	}
	if err := enc.Encode(meta); err != nil {
		return err
	}
	for _, node := range snapshot.Nodes {
		line := jsonlOperator{
			Record: "operator", ID: node.ID, Type: node.Type, Parent: parents[node.ID],
			Columns: node.Columns, Predicate: node.Predicate,
		}
		if len(node.Children) > 0 {
			line.Children = append([]string(nil), node.Children...)
		}
		if snapshot.FormatVersion == 1 {
			line.Columns = nil
			line.Stats = nil
		} else if !opts.StatsSeparate {
			line.Stats = node.Stats
		}
		if err := enc.Encode(line); err != nil {
			return err
		}
		if opts.StatsSeparate && snapshot.FormatVersion != 1 && node.Stats != nil {
			if err := enc.Encode(jsonlStats{
				Record: "stats", OperatorID: node.Stats.OperatorID, RowCount: node.Stats.RowCount,
				Distinct: node.Stats.Distinct,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func inspectJSONLHeader(data []byte) (*Header, error) {
	line, lineNo, lineBytes, err := firstNonEmptyLine(data)
	if err != nil {
		return nil, corruptJSONLine(lineNo, lineBytes, err)
	}
	var meta jsonlMeta
	if err := json.Unmarshal(line, &meta); err != nil {
		return nil, corruptJSONLine(lineNo, lineBytes, fmt.Errorf("metadata line is not valid JSON: %w", err))
	}
	if meta.Record != "snapshot" {
		return nil, corruptJSONLine(lineNo, lineBytes, fmt.Errorf("first JSONL record must be snapshot metadata, got %q", meta.Record))
	}
	if meta.Format != "" && meta.Format != "jsonl" {
		return nil, corruptJSONLine(lineNo, lineBytes, fmt.Errorf("metadata format must be jsonl"))
	}
	if !sequenceInRange(meta.SnapshotSeq) {
		return nil, corruptJSONLine(lineNo, lineBytes, fmt.Errorf("snapshot_seq must be in 1..MaxInt64"))
	}
	generated, err := time.Parse(time.RFC3339Nano, meta.GeneratedAt)
	if err != nil {
		return nil, corruptJSONLine(lineNo, lineBytes, fmt.Errorf("invalid generated_at: %w", err))
	}
	return &Header{
		Format: FormatJSONL, FormatVersion: meta.FormatVersion, SnapshotSeq: meta.SnapshotSeq,
		GeneratedAt: generated, RequiredFeatures: append([]string(nil), meta.RequiredFeatures...),
	}, nil
}

func DecodeJSONL(data []byte) (*model.Snapshot, error) {
	header, err := inspectJSONLHeader(data)
	if err != nil {
		return nil, err
	}
	legacy := header.FormatVersion == 1
	raws := make(map[string]*rawJSONLOperator)
	order := make([]string, 0)
	statsLines := make(map[string]*jsonlStats)
	statsCRC := make(map[string]uint32)
	statsLineNo := make(map[string]int)
	degradations := make([]model.Degradation, 0)
	seenMeta := false
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		lineCRC := crc32.ChecksumIEEE(line)
		if !seenMeta {
			seenMeta = true
			continue
		}
		var envelope struct {
			Record string `json:"record"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			return nil, corruptJSONLine(lineNo, line, fmt.Errorf("invalid JSON: %w", err))
		}
		switch envelope.Record {
		case "operator":
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(line, &fields); err != nil {
				return nil, corruptJSONLine(lineNo, line, err)
			}
			var op jsonlOperator
			if err := json.Unmarshal(line, &op); err != nil {
				return nil, corruptJSONLine(lineNo, line, fmt.Errorf("invalid operator record: %w", err))
			}
			if op.ID == "" {
				return nil, bizerr.New(bizerr.CodePlanInvalid, "operator record has an empty id", map[string]any{"record": lineNo, "crc32": fmt.Sprintf("%08x", lineCRC)})
			}
			if _, dup := raws[op.ID]; dup {
				return nil, bizerr.New(bizerr.CodePlanInvalid, "duplicate operator id in JSONL", map[string]any{"record": lineNo, "crc32": fmt.Sprintf("%08x", lineCRC), "operator_id": op.ID})
			}
			_, hasCols := fields["columns"]
			_, hasStats := fields["stats"]
			_, hasChildren := fields["children"]
			raws[op.ID] = &rawJSONLOperator{
				lineNo: lineNo, lineCRC: lineCRC, record: op,
				hasCols: hasCols, hasStats: hasStats, hasChild: hasChildren,
			}
			order = append(order, op.ID)
		case "stats":
			var stats jsonlStats
			if err := json.Unmarshal(line, &stats); err != nil {
				return nil, corruptJSONLine(lineNo, line, fmt.Errorf("invalid stats record: %w", err))
			}
			if stats.OperatorID == "" {
				return nil, bizerr.New(bizerr.CodePlanInvalid, "stats record has an empty operator_id", map[string]any{"record": lineNo, "crc32": fmt.Sprintf("%08x", lineCRC)})
			}
			if _, dup := statsLines[stats.OperatorID]; dup {
				return nil, bizerr.New(bizerr.CodePlanInvalid, "duplicate stats binding for operator", map[string]any{"record": lineNo, "crc32": fmt.Sprintf("%08x", lineCRC), "operator_id": stats.OperatorID})
			}
			cp := stats
			statsLines[stats.OperatorID] = &cp
			statsCRC[stats.OperatorID] = lineCRC
			statsLineNo[stats.OperatorID] = lineNo
		default:
			return nil, corruptJSONLine(lineNo, line, fmt.Errorf("unknown JSONL record kind %q", envelope.Record))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, corruptJSONLine(lineNo+1, nil, err)
	}
	if !seenMeta {
		return nil, corruptJSONLine(1, nil, fmt.Errorf("empty JSONL input"))
	}
	if len(raws) == 0 {
		return nil, bizerr.New(bizerr.CodePlanInvalid, "snapshot has no operators", map[string]any{"record": 1})
	}

	inferredChildren := make(map[string][]string)
	for _, id := range order {
		raw := raws[id]
		if raw.record.Parent != "" {
			if _, ok := raws[raw.record.Parent]; !ok {
				return nil, bizerr.New(bizerr.CodePlanInvalid, "operator names a missing parent",
					map[string]any{"record": raw.lineNo, "crc32": fmt.Sprintf("%08x", raw.lineCRC), "operator_id": id, "parent": raw.record.Parent})
			}
			inferredChildren[raw.record.Parent] = append(inferredChildren[raw.record.Parent], id)
		}
	}

	nodes := make([]*model.Node, 0, len(order))
	for _, id := range order {
		raw := raws[id]
		op := raw.record
		node := &model.Node{ID: op.ID, Type: op.Type}
		if raw.hasChild {
			node.Children = append([]string(nil), op.Children...)
		} else {
			node.Children = append([]string(nil), inferredChildren[id]...)
		}
		if raw.hasCols {
			node.Columns = append([]model.ColumnRef(nil), op.Columns...)
		} else {
			node.Columns = []model.ColumnRef{}
			if legacy {
				degradations = append(degradations, degradation(raw.lineNo, id, "columns", "default_applied", []any{},
					"legacy JSONL v1 omitted column references"))
			}
		}
		if op.Predicate != nil {
			node.Predicate = op.Predicate.Clone()
		}
		if raw.hasStats {
			node.Stats = op.Stats.Clone()
			if node.Stats != nil && node.Stats.OperatorID == "" {
				node.Stats.OperatorID = id
			}
		}
		if stats, ok := statsLines[id]; ok {
			node.Stats = &model.StatsBinding{OperatorID: id, RowCount: stats.RowCount, Distinct: cloneInt64Map(stats.Distinct)}
		} else if legacy && node.Stats == nil {
			node.Stats = &model.StatsBinding{OperatorID: id, RowCount: 0, Distinct: map[string]int64{}}
			degradations = append(degradations, degradation(raw.lineNo, id, "stats", "default_applied",
				map[string]any{"row_count": 0, "distinct": map[string]int64{}},
				"legacy JSONL v1 omitted statistics bindings"))
		}
		nodes = append(nodes, node)
	}
	// Parent identifiers are part of the interchange contract. Explicit children
	// lists remain authoritative, but a non-empty parent link must agree with the
	// final reconstructed tree.
	for _, id := range order {
		raw := raws[id]
		if raw.record.Parent == "" {
			continue
		}
		parent := raws[raw.record.Parent]
		parentChildren := parent.record.Children
		if !parent.hasChild {
			parentChildren = inferredChildren[parent.record.ID]
		}
		if !containsString(parentChildren, id) {
			return nil, bizerr.New(bizerr.CodePlanInvalid, "parent/child records disagree",
				map[string]any{"record": raw.lineNo, "crc32": fmt.Sprintf("%08x", raw.lineCRC), "operator_id": id, "parent": raw.record.Parent})
		}
	}
	for opID, stats := range statsLines {
		if _, ok := raws[opID]; !ok {
			return nil, bizerr.New(bizerr.CodePlanInvalid, "stats binding references unknown operator",
				map[string]any{"record": statsLineNo[opID], "crc32": fmt.Sprintf("%08x", statsCRC[opID]), "operator_id": opID})
		}
		_ = stats
	}
	sort.SliceStable(degradations, func(i, j int) bool {
		if degradations[i].Record == degradations[j].Record {
			return degradations[i].Field < degradations[j].Field
		}
		return degradations[i].Record < degradations[j].Record
	})
	return &model.Snapshot{
		FormatVersion: header.FormatVersion, SnapshotSeq: header.SnapshotSeq,
		GeneratedAt: header.GeneratedAt, Nodes: nodes, Degradations: degradations,
	}, nil
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func firstNonEmptyLine(data []byte) ([]byte, int, []byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) > 0 {
			return line, lineNo, line, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, lineNo + 1, nil, err
	}
	return nil, 1, nil, fmt.Errorf("empty JSONL input")
}

func corruptJSONLine(record int, line []byte, cause error) *bizerr.Error {
	checksum := crc32.ChecksumIEEE(line)
	return corruptRecord(record, checksum, cause)
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	if in == nil {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ = io.Discard
