package walgroup

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
)

const (
	segmentMagic      = "WALSG001"
	segmentVersion    = uint32(1)
	segmentHeaderSize = uint32(128)
	recordMagic       = uint32(0x574c5231) // WLR1
	recordFixedSize   = 76
	segmentFilePrefix = "wal-"
	segmentFileSuffix = ".seg"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

func putU32(b []byte, v uint32) { binary.BigEndian.PutUint32(b, v) }
func putU64(b []byte, v uint64) { binary.BigEndian.PutUint64(b, v) }
func getU32(b []byte) uint32    { return binary.BigEndian.Uint32(b) }
func getU64(b []byte) uint64    { return binary.BigEndian.Uint64(b) }

func encodeHeader(h SegmentHeader) []byte {
	b := make([]byte, segmentHeaderSize)
	copy(b[0:8], []byte(segmentMagic))
	putU32(b[8:12], h.Version)
	putU32(b[12:16], h.HeaderSize)
	putU64(b[16:24], h.SegmentNo)
	putU64(b[24:32], h.StartLSN)
	prev, _ := hex.DecodeString(h.PrevDigest)
	copy(b[32:64], prev)
	putU64(b[64:72], h.RecordCount)
	putU64(b[72:80], h.TxnCount)
	putU64(b[80:88], h.LastLSN)
	putU32(b[88:92], h.Flags)
	// bytes 92..123 are reserved and zero.
	putU32(b[124:128], crc32c(b[:124]))
	return b
}

func decodeHeader(b []byte, expectedSegmentNo, expectedStartLSN uint64) (SegmentHeader, error) {
	if len(b) < int(segmentHeaderSize) {
		return SegmentHeader{}, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "short segment header")
	}
	h := SegmentHeader{
		Magic:          string(b[0:8]),
		Version:        getU32(b[8:12]),
		HeaderSize:     getU32(b[12:16]),
		SegmentNo:      getU64(b[16:24]),
		StartLSN:       getU64(b[24:32]),
		PrevDigest:     hex.EncodeToString(b[32:64]),
		RecordCount:    getU64(b[64:72]),
		TxnCount:       getU64(b[72:80]),
		LastLSN:        getU64(b[80:88]),
		Flags:          getU32(b[88:92]),
		HeaderChecksum: getU32(b[124:128]),
	}
	if h.Magic != segmentMagic {
		return h, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "bad segment magic")
	}
	if h.Version != segmentVersion {
		return h, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "unsupported segment version %d", h.Version)
	}
	if h.HeaderSize != segmentHeaderSize {
		return h, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "bad header size %d", h.HeaderSize)
	}
	if got, want := crc32c(b[:124]), h.HeaderChecksum; got != want {
		return h, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "header checksum mismatch: got %08x want %08x", got, want)
	}
	if expectedSegmentNo != 0 && h.SegmentNo != expectedSegmentNo {
		return h, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "segment number mismatch: got %d", h.SegmentNo)
	}
	if expectedStartLSN != 0 && h.StartLSN != expectedStartLSN {
		return h, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "start LSN mismatch: got %d want %d", h.StartLSN, expectedStartLSN)
	}
	if h.StartLSN == 0 {
		return h, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "start LSN must be positive")
	}
	return h, nil
}

func encodeRecord(r Record) []byte {
	txn := []byte(r.TxnID)
	digest, _ := hex.DecodeString(r.Digest)
	total := recordFixedSize + len(txn) + len(r.Data)
	b := make([]byte, total)
	putU32(b[0:4], recordMagic)
	putU32(b[4:8], uint32(total))
	putU64(b[8:16], r.LSN)
	putU64(b[16:24], uint64(r.PageNo))
	putU64(b[24:32], r.Version)
	copy(b[32:64], digest)
	putU32(b[64:68], uint32(len(txn)))
	putU32(b[68:72], uint32(len(r.Data)))
	putU32(b[72:76], 0)
	copy(b[recordFixedSize:recordFixedSize+len(txn)], txn)
	copy(b[recordFixedSize+len(txn):], r.Data)
	crc := crc32c(b)
	putU32(b[72:76], crc)
	return b
}

func decodeRecordAt(b []byte, off int, segmentNo uint64, ordinal int) (Record, int, error) {
	if off < 0 || len(b)-off < recordFixedSize {
		return Record{}, 0, segmentCorrupt(segmentNo, 0, "short record at offset %d", off)
	}
	buf := b[off:]
	if getU32(buf[0:4]) != recordMagic {
		return Record{}, 0, segmentCorrupt(segmentNo, 0, "bad record magic at offset %d", off)
	}
	total := int(getU32(buf[4:8]))
	if total < recordFixedSize || total > len(buf) {
		return Record{}, 0, segmentCorrupt(segmentNo, 0, "bad record length %d at offset %d", total, off)
	}
	rec := Record{
		LSN:       getU64(buf[8:16]),
		PageNo:    int64(getU64(buf[16:24])),
		Version:   getU64(buf[24:32]),
		Digest:    hex.EncodeToString(buf[32:64]),
		SegmentNo: segmentNo,
		Offset:    int64(off),
		Length:    total,
		Checksum:  getU32(buf[72:76]),
		Ordinal:   ordinal,
	}
	txnLen := int(getU32(buf[64:68]))
	dataLen := int(getU32(buf[68:72]))
	if txnLen < 0 || dataLen < 0 || recordFixedSize+txnLen+dataLen != total {
		return Record{}, 0, segmentCorrupt(segmentNo, 0, "inconsistent record lengths at offset %d", off)
	}
	check := append([]byte(nil), buf[:total]...)
	putU32(check[72:76], 0)
	if got, want := crc32c(check), rec.Checksum; got != want {
		return Record{}, 0, segmentCorrupt(segmentNo, 0, "record checksum mismatch at offset %d", off)
	}
	rec.TxnID = string(buf[recordFixedSize : recordFixedSize+txnLen])
	rec.Data = append([]byte(nil), buf[recordFixedSize+txnLen:total]...)
	digest := sha256.Sum256(rec.Data)
	if rec.Digest != hex.EncodeToString(digest[:]) {
		return Record{}, 0, segmentCorrupt(segmentNo, 0, "page digest mismatch at offset %d", off)
	}
	return rec, total, nil
}

func recordSize(txnID string, data []byte) int64 {
	return int64(recordFixedSize + len([]byte(txnID)) + len(data))
}

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func equalPageRefs(a, b []PageVersionRef) bool {
	aa := canonicalPageRefs(a)
	bb := canonicalPageRefs(b)
	if len(aa) != len(bb) {
		return false
	}
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
