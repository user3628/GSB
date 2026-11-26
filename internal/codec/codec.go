package codec

import (
	"encoding/binary"
	"math"
	"sort"

	"logagg/internal/errs"
	"logagg/internal/model"
)

const (
	ColumnGroup  = "group"
	ColumnMetric = "metric"
)

func DecodeColumn(block *model.Block, column string, startRow, maxRows int) ([]int64, error) {
	if block == nil {
		return nil, errs.New(errs.EInvalidRequest, "block is nil")
	}
	if err := verifyBlock(block); err != nil {
		return nil, err
	}
	payload, err := payloadFor(block, column)
	if err != nil {
		return nil, err
	}
	switch block.Meta.Encoding {
	case model.EncodingFixed:
		return DecodeFixedValues(block.Meta.BlockNo, block.Meta.RowCount, payload, startRow, maxRows)
	case model.EncodingRLE:
		return DecodeRLEValues(block.Meta.BlockNo, block.Meta.RowCount, payload, startRow, maxRows)
	case model.EncodingDictionary:
		return DecodeDictionaryValues(block.Meta.BlockNo, block.Meta.RowCount, payload, startRow, maxRows)
	default:
		return nil, errs.BlockCorrupt(block.Meta.BlockNo, 0, "unsupported encoding %q", block.Meta.Encoding)
	}
}

func DecodeFixed(block *model.Block, column string, startRow, maxRows int) ([]int64, error) {
	if err := verifyBlock(block); err != nil {
		return nil, err
	}
	payload, err := payloadFor(block, column)
	if err != nil {
		return nil, err
	}
	if block.Meta.Encoding != model.EncodingFixed {
		return nil, encodingMismatch(block, model.EncodingFixed)
	}
	return DecodeFixedValues(block.Meta.BlockNo, block.Meta.RowCount, payload, startRow, maxRows)
}

func DecodeRLE(block *model.Block, column string, startRow, maxRows int) ([]int64, error) {
	if err := verifyBlock(block); err != nil {
		return nil, err
	}
	payload, err := payloadFor(block, column)
	if err != nil {
		return nil, err
	}
	if block.Meta.Encoding != model.EncodingRLE {
		return nil, encodingMismatch(block, model.EncodingRLE)
	}
	return DecodeRLEValues(block.Meta.BlockNo, block.Meta.RowCount, payload, startRow, maxRows)
}

func DecodeDictionary(block *model.Block, column string, startRow, maxRows int) ([]int64, error) {
	if err := verifyBlock(block); err != nil {
		return nil, err
	}
	payload, err := payloadFor(block, column)
	if err != nil {
		return nil, err
	}
	if block.Meta.Encoding != model.EncodingDictionary {
		return nil, encodingMismatch(block, model.EncodingDictionary)
	}
	return DecodeDictionaryValues(block.Meta.BlockNo, block.Meta.RowCount, payload, startRow, maxRows)
}

func verifyBlock(block *model.Block) error {
	if block.Meta.BlockNo < 0 {
		return errs.New(errs.EShardInvalid, "block number must be non-negative")
	}
	if block.Meta.RowCount < 0 {
		return errs.BlockCorrupt(block.Meta.BlockNo, 0, "negative declared row count %d", block.Meta.RowCount)
	}
	if !block.Meta.Encoding.Valid() {
		return errs.BlockCorrupt(block.Meta.BlockNo, 0, "invalid encoding %q", block.Meta.Encoding)
	}
	if err := block.VerifyDigest(); err != nil {
		coded := errs.BlockCorrupt(block.Meta.BlockNo, 0, "header/digest validation failed: %v", err)
		coded.Details["reason"] = "digest_mismatch"
		return coded
	}
	return nil
}

func encodingMismatch(block *model.Block, want model.Encoding) error {
	return errs.BlockCorrupt(block.Meta.BlockNo, 0, "encoding type mismatch: decoder=%s header=%s", want, block.Meta.Encoding).
		WithDetail("expected_encoding", string(want)).
		WithDetail("actual_encoding", string(block.Meta.Encoding))
}

func payloadFor(block *model.Block, column string) ([]byte, error) {
	switch column {
	case ColumnGroup:
		return block.GroupPayload, nil
	case ColumnMetric:
		return block.MetricPayload, nil
	default:
		return nil, errs.New(errs.EInvalidRequest, "unknown column %q", column)
	}
}

func rangeEnd(rowCount, start, maxRows int) (int, error) {
	if start < 0 || start > rowCount {
		return 0, errs.New(errs.EInvalidRequest, "start row %d outside [0,%d]", start, rowCount)
	}
	end := rowCount
	if maxRows > 0 && start+maxRows < end {
		end = start + maxRows
	}
	return end, nil
}

func EncodeFixed(values []int64) []byte {
	out := make([]byte, 8*len(values))
	for i, value := range values {
		binary.LittleEndian.PutUint64(out[i*8:], uint64(value))
	}
	return out
}

func DecodeFixedValues(blockNo, rowCount int, payload []byte, startRow, maxRows int) ([]int64, error) {
	expectedLen := 8 * rowCount
	if len(payload) != expectedLen {
		offset := int64(len(payload))
		if len(payload) > expectedLen {
			offset = int64(expectedLen)
		}
		return nil, errs.BlockCorrupt(blockNo, offset, "fixed payload length %d does not match row_count*8=%d", len(payload), expectedLen)
	}
	end, err := rangeEnd(rowCount, startRow, maxRows)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, end-startRow)
	for row := startRow; row < end; row++ {
		out = append(out, int64(binary.LittleEndian.Uint64(payload[row*8:])))
	}
	return out, nil
}

type rleRun struct {
	value  int64
	count  int
	offset int
}

func EncodeRLE(values []int64) []byte {
	out := make([]byte, 0, 12*len(values))
	for i := 0; i < len(values); {
		value := values[i]
		j := i + 1
		for j < len(values) && values[j] == value {
			j++
		}
		var head [12]byte
		binary.LittleEndian.PutUint64(head[0:8], uint64(value))
		binary.LittleEndian.PutUint32(head[8:12], uint32(j-i))
		out = append(out, head[:]...)
		i = j
	}
	return out
}

func parseRLE(blockNo, rowCount int, payload []byte) ([]rleRun, error) {
	runs := make([]rleRun, 0)
	rows := 0
	for pos := 0; pos < len(payload); {
		runOffset := pos
		if len(payload)-pos < 12 {
			return nil, errs.BlockCorrupt(blockNo, int64(pos), "truncated RLE run header at byte %d", pos)
		}
		value := int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
		count := int(binary.LittleEndian.Uint32(payload[pos+8 : pos+12]))
		if count == 0 {
			return nil, errs.BlockCorrupt(blockNo, int64(pos+8), "zero-length RLE run at byte %d", pos)
		}
		if uint64(rows)+uint64(count) > math.MaxInt || rows+count > rowCount {
			return nil, errs.BlockCorrupt(blockNo, int64(pos), "RLE run exceeds declared row count %d", rowCount)
		}
		runs = append(runs, rleRun{value: value, count: count, offset: runOffset})
		rows += count
		pos += 12
	}
	if rows != rowCount {
		return nil, errs.BlockCorrupt(blockNo, int64(len(payload)), "RLE run total %d does not match declared row count %d", rows, rowCount)
	}
	return runs, nil
}

func DecodeRLEValues(blockNo, rowCount int, payload []byte, startRow, maxRows int) ([]int64, error) {
	runs, err := parseRLE(blockNo, rowCount, payload)
	if err != nil {
		return nil, err
	}
	end, err := rangeEnd(rowCount, startRow, maxRows)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, end-startRow)
	row := 0
	for _, run := range runs {
		runStart := row
		runEnd := row + run.count
		from := maxInt(startRow, runStart)
		to := minInt(end, runEnd)
		if from < to {
			for i := from; i < to; i++ {
				out = append(out, run.value)
			}
		}
		row = runEnd
		if row >= end {
			break
		}
	}
	return out, nil
}

func EncodeDictionary(values []int64) []byte {
	unique := make(map[int64]struct{}, len(values))
	for _, value := range values {
		unique[value] = struct{}{}
	}
	dict := make([]int64, 0, len(unique))
	for value := range unique {
		dict = append(dict, value)
	}
	sort.Slice(dict, func(i, j int) bool { return dict[i] < dict[j] })
	indexByValue := make(map[int64]uint32, len(dict))
	for i, value := range dict {
		if i > math.MaxUint16 {
			panic("dictionary contains more than 65535 values")
		}
		indexByValue[value] = uint32(i)
	}
	out := make([]byte, 2+8*len(dict)+4*len(values))
	binary.LittleEndian.PutUint16(out[0:2], uint16(len(dict)))
	pos := 2
	for _, value := range dict {
		binary.LittleEndian.PutUint64(out[pos:pos+8], uint64(value))
		pos += 8
	}
	for i, value := range values {
		binary.LittleEndian.PutUint32(out[pos+i*4:pos+i*4+4], indexByValue[value])
	}
	return out
}

func DecodeDictionaryValues(blockNo, rowCount int, payload []byte, startRow, maxRows int) ([]int64, error) {
	if len(payload) < 2 {
		return nil, errs.BlockCorrupt(blockNo, int64(len(payload)), "dictionary header is truncated")
	}
	dictCount := int(binary.LittleEndian.Uint16(payload[0:2]))
	if dictCount == 0 {
		return nil, errs.BlockCorrupt(blockNo, 0, "dictionary has zero entries")
	}
	dictBytes := 8 * dictCount
	indexStart := 2 + dictBytes
	if indexStart < 2 || indexStart > len(payload) {
		offset := int64(2 + 8*(len(payload)-2)/8)
		return nil, errs.BlockCorrupt(blockNo, offset, "dictionary entries are truncated: need %d bytes", dictBytes)
	}
	dict := make([]int64, dictCount)
	for i := 0; i < dictCount; i++ {
		off := 2 + i*8
		dict[i] = int64(binary.LittleEndian.Uint64(payload[off : off+8]))
	}
	expectedLen := indexStart + 4*rowCount
	if expectedLen < indexStart || len(payload) != expectedLen {
		offset := int64(len(payload))
		if len(payload) > expectedLen && expectedLen >= indexStart {
			offset = int64(expectedLen)
		}
		return nil, errs.BlockCorrupt(blockNo, offset, "dictionary index length %d does not match expected %d", len(payload)-indexStart, 4*rowCount)
	}
	for row := 0; row < rowCount; row++ {
		off := indexStart + row*4
		index := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		if index >= dictCount {
			return nil, errs.BlockCorrupt(blockNo, int64(off), "dictionary index %d at row %d exceeds dictionary size %d", index, row, dictCount)
		}
	}
	end, err := rangeEnd(rowCount, startRow, maxRows)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, end-startRow)
	for row := startRow; row < end; row++ {
		off := indexStart + row*4
		index := binary.LittleEndian.Uint32(payload[off : off+4])
		out = append(out, dict[index])
	}
	return out, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
