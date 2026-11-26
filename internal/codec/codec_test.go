package codec

import (
	"errors"
	"testing"

	"logagg/internal/errs"
	"logagg/internal/model"
)

func makeBlock(t *testing.T, encoding model.Encoding, groups, metrics []int64) *model.Block {
	t.Helper()
	var groupPayload, metricPayload []byte
	switch encoding {
	case model.EncodingFixed:
		groupPayload = EncodeFixed(groups)
		metricPayload = EncodeFixed(metrics)
	case model.EncodingRLE:
		groupPayload = EncodeRLE(groups)
		metricPayload = EncodeRLE(metrics)
	case model.EncodingDictionary:
		groupPayload = EncodeDictionary(groups)
		metricPayload = EncodeDictionary(metrics)
	default:
		t.Fatalf("unsupported encoding %s", encoding)
	}
	block := &model.Block{
		Meta:         BlockMetaForTest(encoding, len(groups), groupPayload, metricPayload),
		GroupPayload: groupPayload, MetricPayload: metricPayload,
	}
	return block
}

func BlockMetaForTest(encoding model.Encoding, rows int, groupPayload, metricPayload []byte) model.BlockMeta {
	meta := model.BlockMeta{BlockNo: 7, Partition: 2, RowCount: rows, Encoding: encoding, UncompressedBytes: int64(16 * rows)}
	meta.Digest = model.DigestPayload(meta, groupPayload, metricPayload)
	return meta
}

func TestAllCodecsRangeDecodePreservesOrder(t *testing.T) {
	groups := []int64{3, 3, 1, 1, 1, 4, 4, 0, 2, 2, 2, 9}
	metrics := []int64{10, 10, 11, 11, 11, 12, 12, 13, 14, 14, 14, 15}
	encodings := []model.Encoding{model.EncodingFixed, model.EncodingRLE, model.EncodingDictionary}
	for _, encoding := range encodings {
		t.Run(string(encoding), func(t *testing.T) {
			block := makeBlock(t, encoding, groups, metrics)
			fullGroups, err := DecodeColumn(block, ColumnGroup, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			fullMetrics, err := DecodeColumn(block, ColumnMetric, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			assertInt64Slices(t, groups, fullGroups)
			assertInt64Slices(t, metrics, fullMetrics)

			start, maxRows := 3, 4
			partGroups, err := DecodeColumn(block, ColumnGroup, start, maxRows)
			if err != nil {
				t.Fatal(err)
			}
			partMetrics, err := DecodeColumn(block, ColumnMetric, start, maxRows)
			if err != nil {
				t.Fatal(err)
			}
			assertInt64Slices(t, groups[start:start+maxRows], partGroups)
			assertInt64Slices(t, metrics[start:start+maxRows], partMetrics)

			againGroups, err := DecodeColumn(block, ColumnGroup, start, maxRows)
			if err != nil {
				t.Fatal(err)
			}
			assertInt64Slices(t, partGroups, againGroups)
		})
	}
}

func TestFixedCorruptionReportsBlockAndOffset(t *testing.T) {
	values := []int64{1, 2, 3, 4}
	payload := EncodeFixed(values)
	payload = payload[:len(payload)-1]
	_, err := DecodeFixedValues(11, len(values), payload, 0, 0)
	requireCoded(t, err, errs.EBlockCorrupt)
	var coded *errs.Error
	errors.As(err, &coded)
	if coded.BlockNo != 11 || coded.Offset != int64(len(payload)) {
		t.Fatalf("got block=%d offset=%d, want 11/%d", coded.BlockNo, coded.Offset, len(payload))
	}
}

func TestRLEZeroRunReportsExactOffset(t *testing.T) {
	payload := EncodeRLE([]int64{5, 5, 6, 6})
	for i := 8; i < 12; i++ {
		payload[i] = 0
	}
	_, err := DecodeRLEValues(12, 4, payload, 0, 0)
	requireCoded(t, err, errs.EBlockCorrupt)
	var coded *errs.Error
	errors.As(err, &coded)
	if coded.BlockNo != 12 || coded.Offset != 8 {
		t.Fatalf("got block=%d offset=%d, want 12/8", coded.BlockNo, coded.Offset)
	}
}

func TestDictionaryBadIndexReportsExactOffset(t *testing.T) {
	values := []int64{10, 20, 10, 30}
	payload := EncodeDictionary(values)
	dictCount := int(payload[0]) | int(payload[1])<<8
	indexStart := 2 + 8*dictCount
	lastIndexOffset := indexStart + (len(values)-1)*4
	payload[lastIndexOffset] = 99
	payload[lastIndexOffset+1] = 0
	payload[lastIndexOffset+2] = 0
	payload[lastIndexOffset+3] = 0

	meta := model.BlockMeta{BlockNo: 13, RowCount: len(values), Encoding: model.EncodingDictionary}
	meta.Digest = model.DigestPayload(meta, nil, payload)
	block := &model.Block{Meta: meta, MetricPayload: payload}
	_, err := DecodeDictionary(block, ColumnMetric, 0, 0)
	requireCoded(t, err, errs.EBlockCorrupt)
	var coded *errs.Error
	errors.As(err, &coded)
	if coded.BlockNo != 13 || coded.Offset != int64(lastIndexOffset) {
		t.Fatalf("got block=%d offset=%d, want 13/%d", coded.BlockNo, coded.Offset, lastIndexOffset)
	}
}

func TestDigestMismatchIsBlockCorrupt(t *testing.T) {
	block := makeBlock(t, model.EncodingFixed, []int64{1, 2}, []int64{3, 4})
	block.MetricPayload[0] ^= 0xff
	_, err := DecodeColumn(block, ColumnMetric, 0, 0)
	requireCoded(t, err, errs.EBlockCorrupt)
	var coded *errs.Error
	errors.As(err, &coded)
	if coded.BlockNo != 7 {
		t.Fatalf("block no=%d want 7", coded.BlockNo)
	}
}

func requireCoded(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s error", code)
	}
	if errs.Code(err) != code {
		t.Fatalf("error=%v code=%q want %s", err, errs.Code(err), code)
	}
}

func assertInt64Slices(t *testing.T, want, got []int64) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("length mismatch: want %d got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("value mismatch at %d: want %d got %d", i, want[i], got[i])
		}
	}
}
