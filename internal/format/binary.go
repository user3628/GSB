package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"gsb/internal/gwerr"
)

// 紧凑二进制格式布局：
//
//	头部: magic "GRTB"(4B) | formatVersion uint8 | endianFlag uint8(0x01=LE,0x00=BE) | recordCount uint32
//	记录: lengthPrefix uint32(=len(payload)+4) | payload | crc32.IEEE(payload) uint32
//	payload(v2): bitmap uint16 | model_id(u16+bytes) | endpoint(u16+bytes) |
//	             vram_bytes u64 | concurrency u32 | [toolchain] | [base_time_ms i64] |
//	             [retention_ms i64] | [last_write_ms i64]
//	payload(v1): bitmap uint16 | model_id | endpoint | vram_gib float32(低精度) |
//	             concurrency u32 | [base_time_ms] | [retention_ms] | [last_write_ms]
var binMagic = []byte{'G', 'R', 'T', 'B'}

const (
	endianBig    = 0x00
	endianLittle = 0x01
)

// 可选字段存在位图位定义
const (
	bitToolchain   uint16 = 1 << 0
	bitRetention   uint16 = 1 << 1
	bitBaseTime    uint16 = 1 << 2
	bitLastWrite   uint16 = 1 << 3
	bitConcurrency uint16 = 1 << 4
)

// BinaryHeader 是解析后的二进制头部。
type BinaryHeader struct {
	Version      int
	LittleEndian bool
	RecordCount  int
}

// DetectBinary 检测数据是否为二进制格式，是则返回头部。
func DetectBinary(data []byte) (*BinaryHeader, bool) {
	if len(data) < 10 || !bytes.Equal(data[:4], binMagic) {
		return nil, false
	}
	h := &BinaryHeader{Version: int(data[4])}
	switch data[5] {
	case endianLittle:
		h.LittleEndian = true
	case endianBig:
		h.LittleEndian = false
	default:
		return nil, false
	}
	order := binaryOrder(h.LittleEndian)
	h.RecordCount = int(order.Uint32(data[6:10]))
	return h, true
}

func binaryOrder(little bool) binary.ByteOrder {
	if little {
		return binary.LittleEndian
	}
	return binary.BigEndian
}

// binRaw 是二进制解码出的原始记录（尚未归一化）。
type binRaw struct {
	modelID        string
	endpoint       string
	hasVRAMBytes   bool
	vramBytes      uint64
	hasVRAMGiB     bool
	vramGiB        float64
	hasConcurrency bool
	concurrency    uint32
	hasToolchain   bool
	toolchain      string
	hasBaseTime    bool
	baseTimeMs     int64
	hasRetention   bool
	retentionMs    int64
	hasLastWrite   bool
	lastWriteMs    int64
}

// decodeBinaryRecords 逐条解码，损坏时返回 E_RECORD_CORRUPT 并指明偏移。
func decodeBinaryRecords(data []byte, h *BinaryHeader) ([]binRaw, error) {
	order := binaryOrder(h.LittleEndian)
	off := 10
	recs := make([]binRaw, 0, h.RecordCount)
	for i := 0; i < h.RecordCount; i++ {
		recOff := off
		if off+4 > len(data) {
			return nil, gwerr.Corrupt(int64(recOff), fmt.Sprintf("记录 #%d 长度前缀越界", i))
		}
		recLen := int(order.Uint32(data[off : off+4]))
		if recLen < 4 {
			return nil, gwerr.Corrupt(int64(recOff), fmt.Sprintf("记录 #%d 长度前缀非法: %d", i, recLen))
		}
		if off+4+recLen > len(data) {
			return nil, gwerr.Corrupt(int64(recOff),
				fmt.Sprintf("记录 #%d 长度前缀 %d 与实际剩余 %d 不符", i, recLen, len(data)-off-4))
		}
		payload := data[off+4 : off+4+recLen-4]
		crcGot := order.Uint32(data[off+4+recLen-4 : off+4+recLen])
		crcWant := crc32.ChecksumIEEE(payload)
		if crcGot != crcWant {
			return nil, gwerr.Corrupt(int64(recOff),
				fmt.Sprintf("记录 #%d CRC32 校验失败: got=%08x want=%08x", i, crcGot, crcWant))
		}
		raw, err := parseBinPayload(payload, h.Version, order, int64(recOff))
		if err != nil {
			return nil, err
		}
		recs = append(recs, *raw)
		off += 4 + recLen
	}
	if off != len(data) {
		return nil, gwerr.Corrupt(int64(off), "尾部存在未消费字节")
	}
	return recs, nil
}

func parseBinPayload(p []byte, version int, order binary.ByteOrder, recOff int64) (*binRaw, error) {
	r := &binRaw{}
	if len(p) < 2 {
		return nil, gwerr.Corrupt(recOff, "payload 过短，缺少位图")
	}
	bitmap := order.Uint16(p[:2])
	pos := 2
	readStr := func() (string, error) {
		if pos+2 > len(p) {
			return "", gwerr.Corrupt(recOff, "字符串长度前缀越界")
		}
		n := int(order.Uint16(p[pos : pos+2]))
		pos += 2
		if pos+n > len(p) {
			return "", gwerr.Corrupt(recOff, "字符串内容越界")
		}
		s := string(p[pos : pos+n])
		pos += n
		return s, nil
	}
	readU64 := func() (uint64, error) {
		if pos+8 > len(p) {
			return 0, gwerr.Corrupt(recOff, "u64 字段越界")
		}
		v := order.Uint64(p[pos : pos+8])
		pos += 8
		return v, nil
	}
	readU32 := func() (uint32, error) {
		if pos+4 > len(p) {
			return 0, gwerr.Corrupt(recOff, "u32 字段越界")
		}
		v := order.Uint32(p[pos : pos+4])
		pos += 4
		return v, nil
	}
	readI64 := func() (int64, error) {
		v, err := readU64()
		return int64(v), err
	}
	var err error
	if r.modelID, err = readStr(); err != nil {
		return nil, err
	}
	if r.endpoint, err = readStr(); err != nil {
		return nil, err
	}
	switch version {
	case 1:
		var v uint32
		if v, err = readU32(); err != nil {
			return nil, err
		}
		r.hasVRAMGiB = true
		r.vramGiB = float64(math.Float32frombits(v)) // 旧版低精度 float32 GiB
	case 2:
		if r.vramBytes, err = readU64(); err != nil {
			return nil, err
		}
		r.hasVRAMBytes = true
	default:
		return nil, gwerr.New(gwerr.EFormatUnsupported,
			fmt.Sprintf("二进制格式版本 %d 含未知必填字段布局", version),
			map[string]any{"format": FormatBinary, "version": version})
	}
	if bitmap&bitConcurrency != 0 {
		if r.concurrency, err = readU32(); err != nil {
			return nil, err
		}
		r.hasConcurrency = true
	}
	if bitmap&bitToolchain != 0 {
		if r.toolchain, err = readStr(); err != nil {
			return nil, err
		}
		r.hasToolchain = true
	}
	if bitmap&bitBaseTime != 0 {
		if r.baseTimeMs, err = readI64(); err != nil {
			return nil, err
		}
		r.hasBaseTime = true
	}
	if bitmap&bitRetention != 0 {
		if r.retentionMs, err = readI64(); err != nil {
			return nil, err
		}
		r.hasRetention = true
	}
	if bitmap&bitLastWrite != 0 {
		if r.lastWriteMs, err = readI64(); err != nil {
			return nil, err
		}
		r.hasLastWrite = true
	}
	if pos != len(p) {
		return nil, gwerr.Corrupt(recOff, "payload 存在未消费尾部字节")
	}
	return r, nil
}

// EncodeBinary 以原生 v2 版本编码路由表（默认小端）。
func EncodeBinary(recs []RouteRecord, littleEndian bool) []byte {
	order := binaryOrder(littleEndian)
	var buf bytes.Buffer
	buf.Write(binMagic)
	buf.WriteByte(NativeBinaryVersion)
	if littleEndian {
		buf.WriteByte(endianLittle)
	} else {
		buf.WriteByte(endianBig)
	}
	var tmp [8]byte
	order.PutUint32(tmp[:4], uint32(len(recs)))
	buf.Write(tmp[:4])
	for _, rec := range recs {
		var payload bytes.Buffer
		bitmap := bitToolchain | bitRetention | bitBaseTime | bitLastWrite | bitConcurrency
		order.PutUint16(tmp[:2], bitmap)
		payload.Write(tmp[:2])
		writeBinStr(&payload, order, rec.ModelID)
		writeBinStr(&payload, order, rec.Endpoint)
		order.PutUint64(tmp[:8], rec.VRAMBytes)
		payload.Write(tmp[:8])
		order.PutUint32(tmp[:4], rec.Concurrency)
		payload.Write(tmp[:4])
		writeBinStr(&payload, order, rec.Toolchain)
		order.PutUint64(tmp[:8], uint64(rec.BaseTimeMs))
		payload.Write(tmp[:8])
		order.PutUint64(tmp[:8], uint64(rec.RetentionMs))
		payload.Write(tmp[:8])
		order.PutUint64(tmp[:8], uint64(rec.LastWriteMs))
		payload.Write(tmp[:8])
		body := payload.Bytes()
		order.PutUint32(tmp[:4], uint32(len(body)+4))
		buf.Write(tmp[:4])
		buf.Write(body)
		order.PutUint32(tmp[:4], crc32.ChecksumIEEE(body))
		buf.Write(tmp[:4])
	}
	return buf.Bytes()
}

func writeBinStr(buf *bytes.Buffer, order binary.ByteOrder, s string) {
	var tmp [2]byte
	order.PutUint16(tmp[:2], uint16(len(s)))
	buf.Write(tmp[:2])
	buf.WriteString(s)
}

// EncodeBinaryV1 以旧版 v1 布局编码（用于生成确定性样本）。
// vramGiB 低精度 float32；不写工具链字段。
func EncodeBinaryV1(recs []RouteRecord, vramGiB []float32, dropToolchain bool) []byte {
	order := binary.LittleEndian
	var buf bytes.Buffer
	buf.Write(binMagic)
	buf.WriteByte(1)
	buf.WriteByte(endianLittle)
	var tmp [8]byte
	order.PutUint32(tmp[:4], uint32(len(recs)))
	buf.Write(tmp[:4])
	for i, rec := range recs {
		var payload bytes.Buffer
		bitmap := bitRetention | bitBaseTime | bitLastWrite | bitConcurrency
		if !dropToolchain {
			bitmap |= bitToolchain
		}
		order.PutUint16(tmp[:2], bitmap)
		payload.Write(tmp[:2])
		writeBinStr(&payload, order, rec.ModelID)
		writeBinStr(&payload, order, rec.Endpoint)
		order.PutUint32(tmp[:4], math.Float32bits(vramGiB[i]))
		payload.Write(tmp[:4])
		order.PutUint32(tmp[:4], rec.Concurrency)
		payload.Write(tmp[:4])
		if !dropToolchain {
			writeBinStr(&payload, order, rec.Toolchain)
		}
		order.PutUint64(tmp[:8], uint64(rec.BaseTimeMs))
		payload.Write(tmp[:8])
		order.PutUint64(tmp[:8], uint64(rec.RetentionMs))
		payload.Write(tmp[:8])
		order.PutUint64(tmp[:8], uint64(rec.LastWriteMs))
		payload.Write(tmp[:8])
		body := payload.Bytes()
		order.PutUint32(tmp[:4], uint32(len(body)+4))
		buf.Write(tmp[:4])
		buf.Write(body)
		order.PutUint32(tmp[:4], crc32.ChecksumIEEE(body))
		buf.Write(tmp[:4])
	}
	return buf.Bytes()
}
