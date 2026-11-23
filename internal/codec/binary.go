package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"time"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/model"
)

const (
	binaryHeaderSize = 4 + 1 + 1 + 8 + 8 + 4
	bitColumns       = 1 << 0
	bitPredicate     = 1 << 1
	bitStats         = 1 << 2
	knownPresence    = bitColumns | bitPredicate | bitStats
)

type BinaryOptions struct {
	ByteOrder binary.ByteOrder
	// Version overrides snapshot.FormatVersion when non-zero.
	Version int
	// OmitColumns and OmitStats produce legacy-shaped records regardless of the
	// snapshot content. They are intended for deterministic sample generation.
	OmitColumns bool
	OmitStats   bool
}

func EncodeBinary(w io.Writer, snapshot *model.Snapshot, opts BinaryOptions) error {
	if snapshot == nil {
		return bizerr.New(bizerr.CodeBadRequest, "cannot encode a nil snapshot", nil)
	}
	version := snapshot.FormatVersion
	if opts.Version != 0 {
		version = opts.Version
	}
	if version < 0 || version > 255 {
		return bizerr.New(bizerr.CodeBadRequest, "format version must fit in one byte", map[string]any{"format_version": version})
	}
	order := opts.ByteOrder
	if order == nil {
		order = binary.LittleEndian
	}
	endianMark := byte(1)
	if order == binary.BigEndian {
		endianMark = 2
	}

	var out bytes.Buffer
	out.WriteString(BinaryMagic)
	out.WriteByte(byte(version))
	out.WriteByte(endianMark)
	writeU64(&out, order, snapshot.SnapshotSeq)
	writeI64(&out, order, snapshot.GeneratedAt.UTC().Unix())
	writeU32(&out, order, uint32(len(snapshot.Nodes)))

	for index, node := range snapshot.Nodes {
		if node == nil {
			return bizerr.New(bizerr.CodePlanInvalid, "cannot encode nil operator", map[string]any{"record": index + 1})
		}
		var payload bytes.Buffer
		payload.WriteByte(1) // operator record kind
		mask := byte(0)
		includeColumns := !opts.OmitColumns && version != 1
		includeStats := !opts.OmitStats && version != 1
		if includeColumns {
			mask |= bitColumns
		}
		if node.Predicate != nil {
			mask |= bitPredicate
		}
		if includeStats && node.Stats != nil {
			mask |= bitStats
		}
		payload.WriteByte(mask)
		writeString(&payload, order, node.ID)
		writeString(&payload, order, string(node.Type))
		if len(node.Children) > 65535 {
			return bizerr.New(bizerr.CodePlanInvalid, "operator has too many children", map[string]any{"record": index + 1, "operator_id": node.ID})
		}
		writeU16(&payload, order, uint16(len(node.Children)))
		for _, child := range node.Children {
			writeString(&payload, order, child)
		}
		if mask&bitColumns != 0 {
			writeColumns(&payload, order, node.Columns)
		}
		if mask&bitPredicate != 0 {
			writeString(&payload, order, node.Predicate.Expr)
			writeColumns(&payload, order, node.Predicate.Columns)
		}
		if mask&bitStats != 0 {
			writeStats(&payload, order, node.Stats)
		}

		recordLen := payload.Len() + 4
		writeU32(&out, order, uint32(recordLen))
		out.Write(payload.Bytes())
		writeU32(&out, order, crc32.ChecksumIEEE(payload.Bytes()))
	}
	_, err := w.Write(out.Bytes())
	return err
}

func inspectBinaryHeader(data []byte) (*Header, error) {
	if len(data) < binaryHeaderSize {
		return nil, corruptRecord(0, crc32.ChecksumIEEE(data), fmt.Errorf("truncated binary header: got %d bytes, need %d", len(data), binaryHeaderSize))
	}
	version := int(data[4])
	var order binary.ByteOrder
	switch data[5] {
	case 1:
		order = binary.LittleEndian
	case 2:
		order = binary.BigEndian
	default:
		return nil, corruptRecord(0, crc32.ChecksumIEEE(data[:6]), fmt.Errorf("unknown endian marker 0x%02x", data[5]))
	}
	seq := order.Uint64(data[6:14])
	generated := time.Unix(int64(order.Uint64(data[14:22])), 0).UTC()
	count := int(order.Uint32(data[22:26]))
	if !sequenceInRange(seq) {
		return nil, corruptRecord(0, crc32.ChecksumIEEE(data[:binaryHeaderSize]), fmt.Errorf("snapshot sequence must be in 1..MaxInt64"))
	}
	return &Header{
		Format: FormatBinary, FormatVersion: version, SnapshotSeq: seq,
		GeneratedAt: generated, RecordCount: count,
		ByteOrder: map[byte]string{1: "little", 2: "big"}[data[5]],
	}, nil
}

func DecodeBinary(data []byte) (*model.Snapshot, error) {
	header, endianMark, err := binaryHeader(data)
	if err != nil {
		return nil, err
	}
	var order binary.ByteOrder = binary.LittleEndian
	if endianMark == 2 {
		order = binary.BigEndian
	}
	offset := binaryHeaderSize
	nodes := make([]*model.Node, 0, header.RecordCount)
	degradations := make([]model.Degradation, 0)
	legacy := header.FormatVersion == 1
	for recordIndex := 1; recordIndex <= header.RecordCount; recordIndex++ {
		if offset+4 > len(data) {
			return nil, corruptRecord(recordIndex, crc32.ChecksumIEEE(data[offset:]), fmt.Errorf("missing record length prefix"))
		}
		recordLen := int(order.Uint32(data[offset : offset+4]))
		offset += 4
		if recordLen < 5 || recordLen > 64*1024*1024 {
			return nil, corruptRecord(recordIndex, 0, fmt.Errorf("invalid record length %d", recordLen))
		}
		if offset+recordLen > len(data) {
			return nil, corruptRecord(recordIndex, crc32.ChecksumIEEE(data[offset:]), fmt.Errorf("record declares %d bytes but only %d remain", recordLen, len(data)-offset))
		}
		payload := data[offset : offset+recordLen-4]
		storedCRC := order.Uint32(data[offset+recordLen-4 : offset+recordLen])
		actualCRC := crc32.ChecksumIEEE(payload)
		if storedCRC != actualCRC {
			err := corruptRecord(recordIndex, actualCRC, fmt.Errorf("crc32 mismatch: stored=%08x computed=%08x", storedCRC, actualCRC))
			err.Context["stored_crc32"] = fmt.Sprintf("%08x", storedCRC)
			err.Context["stored_crc32_numeric"] = storedCRC
			return nil, err
		}
		offset += recordLen

		reader := &binaryReader{data: payload, order: order, record: recordIndex, crc: actualCRC}
		kind, err := reader.u8()
		if err != nil {
			return nil, reader.corrupt(err)
		}
		if kind != 1 {
			return nil, reader.corrupt(fmt.Errorf("unknown binary record kind %d", kind))
		}
		mask, err := reader.u8()
		if err != nil {
			return nil, reader.corrupt(err)
		}
		if mask & ^byte(knownPresence) != 0 {
			return nil, reader.corrupt(fmt.Errorf("record has unknown optional-field bits 0x%02x", mask&^byte(knownPresence)))
		}
		id, err := reader.str()
		if err != nil {
			return nil, reader.corrupt(err)
		}
		opType, err := reader.str()
		if err != nil {
			return nil, reader.corrupt(err)
		}
		childCount, err := reader.u16()
		if err != nil {
			return nil, reader.corrupt(err)
		}
		children := make([]string, 0, childCount)
		for j := 0; j < int(childCount); j++ {
			child, err := reader.str()
			if err != nil {
				return nil, reader.corrupt(err)
			}
			children = append(children, child)
		}
		node := &model.Node{ID: id, Type: model.OperatorType(opType), Children: children}
		if mask&bitColumns != 0 {
			cols, err := reader.columns()
			if err != nil {
				return nil, reader.corrupt(err)
			}
			node.Columns = cols
		} else {
			node.Columns = []model.ColumnRef{}
			if legacy {
				degradations = append(degradations, degradation(recordIndex, id, "columns", "default_applied", []any{},
					"legacy format v1 omitted column references"))
			}
		}
		if mask&bitPredicate != 0 {
			expr, err := reader.str()
			if err != nil {
				return nil, reader.corrupt(err)
			}
			cols, err := reader.columns()
			if err != nil {
				return nil, reader.corrupt(err)
			}
			node.Predicate = &model.Predicate{Expr: expr, Columns: cols}
		}
		if mask&bitStats != 0 {
			stats, err := reader.stats()
			if err != nil {
				return nil, reader.corrupt(err)
			}
			if stats.OperatorID == "" {
				stats.OperatorID = id
			}
			node.Stats = stats
		} else if legacy {
			node.Stats = &model.StatsBinding{OperatorID: id, RowCount: 0, Distinct: map[string]int64{}}
			degradations = append(degradations, degradation(recordIndex, id, "stats", "default_applied",
				map[string]any{"row_count": 0, "distinct": map[string]int64{}},
				"legacy format v1 omitted statistics bindings"))
		}
		if reader.remaining() != 0 {
			return nil, reader.corrupt(fmt.Errorf("record has %d trailing bytes", reader.remaining()))
		}
		nodes = append(nodes, node)
	}
	if offset != len(data) {
		return nil, corruptRecord(header.RecordCount+1, crc32.ChecksumIEEE(data[offset:]), fmt.Errorf("file has %d trailing bytes", len(data)-offset))
	}
	return &model.Snapshot{
		FormatVersion: header.FormatVersion, SnapshotSeq: header.SnapshotSeq,
		GeneratedAt: header.GeneratedAt, Nodes: nodes, Degradations: degradations,
	}, nil
}

func binaryHeader(data []byte) (*Header, byte, error) {
	if len(data) < binaryHeaderSize || !bytes.Equal(data[:4], []byte(BinaryMagic)) {
		return nil, 0, corruptRecord(0, crc32.ChecksumIEEE(data), fmt.Errorf("missing %q magic", BinaryMagic))
	}
	h, err := inspectBinaryHeader(data)
	if err != nil {
		return nil, 0, err
	}
	return h, data[5], nil
}

type binaryReader struct {
	data   []byte
	off    int
	order  binary.ByteOrder
	record int
	crc    uint32
}

func (r *binaryReader) remaining() int { return len(r.data) - r.off }

func (r *binaryReader) take(n int) ([]byte, error) {
	if n < 0 || r.off+n > len(r.data) {
		return nil, fmt.Errorf("truncated record field at offset %d", r.off)
	}
	b := r.data[r.off : r.off+n]
	r.off += n
	return b, nil
}

func (r *binaryReader) u8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *binaryReader) u16() (uint16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return r.order.Uint16(b), nil
}

func (r *binaryReader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return r.order.Uint32(b), nil
}

func (r *binaryReader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return r.order.Uint64(b), nil
}

func (r *binaryReader) str() (string, error) {
	n, err := r.u32()
	if err != nil {
		return "", err
	}
	if n > 16*1024*1024 {
		return "", fmt.Errorf("string length %d exceeds limit", n)
	}
	b, err := r.take(int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *binaryReader) columns() ([]model.ColumnRef, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n > 1_000_000 {
		return nil, fmt.Errorf("column count %d exceeds limit", n)
	}
	cols := make([]model.ColumnRef, 0, n)
	for i := 0; i < int(n); i++ {
		qualifier, err := r.str()
		if err != nil {
			return nil, err
		}
		name, err := r.str()
		if err != nil {
			return nil, err
		}
		cols = append(cols, model.ColumnRef{Qualifier: qualifier, Name: name})
	}
	return cols, nil
}

func (r *binaryReader) stats() (*model.StatsBinding, error) {
	opID, err := r.str()
	if err != nil {
		return nil, err
	}
	rowsRaw, err := r.u64()
	if err != nil {
		return nil, err
	}
	distinct, err := r.stringInt64Map()
	if err != nil {
		return nil, err
	}
	return &model.StatsBinding{OperatorID: opID, RowCount: int64(rowsRaw), Distinct: distinct}, nil
}

func (r *binaryReader) stringInt64Map() (map[string]int64, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n > 1_000_000 {
		return nil, fmt.Errorf("map size %d exceeds limit", n)
	}
	out := make(map[string]int64, n)
	for i := 0; i < int(n); i++ {
		key, err := r.str()
		if err != nil {
			return nil, err
		}
		value, err := r.u64()
		if err != nil {
			return nil, err
		}
		out[key] = int64(value)
	}
	return out, nil
}

func (r *binaryReader) corrupt(cause error) error {
	return corruptRecord(r.record, r.crc, cause)
}

func writeU16(w io.Writer, order binary.ByteOrder, v uint16) {
	var b [2]byte
	order.PutUint16(b[:], v)
	_, _ = w.Write(b[:])
}

func writeU32(w io.Writer, order binary.ByteOrder, v uint32) {
	var b [4]byte
	order.PutUint32(b[:], v)
	_, _ = w.Write(b[:])
}

func writeU64(w io.Writer, order binary.ByteOrder, v uint64) {
	var b [8]byte
	order.PutUint64(b[:], v)
	_, _ = w.Write(b[:])
}

func writeI64(w io.Writer, order binary.ByteOrder, v int64) { writeU64(w, order, uint64(v)) }

func writeString(w io.Writer, order binary.ByteOrder, s string) {
	writeU32(w, order, uint32(len(s)))
	_, _ = io.WriteString(w, s)
}

func writeColumns(w io.Writer, order binary.ByteOrder, cols []model.ColumnRef) {
	writeU32(w, order, uint32(len(cols)))
	for _, col := range cols {
		writeString(w, order, col.Qualifier)
		writeString(w, order, col.Name)
	}
}

func writeStats(w io.Writer, order binary.ByteOrder, stats *model.StatsBinding) {
	writeString(w, order, stats.OperatorID)
	writeI64(w, order, stats.RowCount)
	writeU32(w, order, uint32(len(stats.Distinct)))
	keys := make([]string, 0, len(stats.Distinct))
	for key := range stats.Distinct {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeString(w, order, key)
		writeI64(w, order, stats.Distinct[key])
	}
}

func degradation(record int, operatorID, field, action string, def any, reason string) model.Degradation {
	return model.Degradation{
		Record: record, OperatorID: operatorID, Field: field, Action: action,
		Default: def, Reason: reason,
	}
}

func corruptRecord(record int, checksum uint32, cause error) *bizerr.Error {
	return bizerr.New(bizerr.CodeFormatCorrupt, cause.Error(), map[string]any{
		"record": record, "record_seq": record, "crc32": fmt.Sprintf("%08x", checksum),
		"crc32_numeric": checksum,
	})
}
