package format

import (
	"strings"
	"testing"

	"gsb/internal/gwerr"
)

func sampleRecs() []RouteRecord {
	base := int64(1735689600000)
	mk := func(id string, day int64) RouteRecord {
		b := base + day*86400000
		return RouteRecord{
			ModelID: id, Endpoint: "http://127.0.0.1:9001",
			VRAMBytes: 8192 << 20, Concurrency: 8, Toolchain: "tc-1.4.2",
			BaseTimeMs: b, RetentionMs: 259200000,
			ExpiresAtMs: b + 259200000, LastWriteMs: b + 60000,
		}
	}
	return []RouteRecord{mk("m1", 0), mk("m2", 1)}
}

func TestBinaryRoundtripBothEndian(t *testing.T) {
	recs := sampleRecs()
	for _, little := range []bool{true, false} {
		data := EncodeBinary(recs, little)
		rep, err := Import(data, TruncTruncate)
		if err != nil {
			t.Fatalf("little=%v: %v", little, err)
		}
		if rep.Verdict != VerdictAccept || rep.FormatVersion != 2 {
			t.Fatalf("裁决异常: %+v", rep)
		}
		if err := RoundtripEqual(recs, rep.Records); err != nil {
			t.Fatalf("little=%v 往返失败: %v", little, err)
		}
	}
}

func TestCrossFormatRoundtrip(t *testing.T) {
	recs := sampleRecs()
	binRep, err := Import(EncodeBinary(recs, true), TruncTruncate)
	if err != nil {
		t.Fatal(err)
	}
	jsonlRep, err := Import(EncodeJSONL(recs), TruncTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if err := RoundtripEqual(binRep.Records, jsonlRep.Records); err != nil {
		t.Fatalf("二进制→JSONL 往返失败: %v", err)
	}
	// 反向：JSONL 记录再编码为二进制
	rebin, err := Import(EncodeBinary(jsonlRep.Records, false), TruncTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if err := RoundtripEqual(jsonlRep.Records, rebin.Records); err != nil {
		t.Fatalf("JSONL→二进制往返失败: %v", err)
	}
}

func TestCorruptCRCOffset(t *testing.T) {
	data := EncodeBinary(sampleRecs(), true)
	data[len(data)-1] ^= 0xFF // 破坏最后一条记录 CRC
	_, err := Import(data, TruncTruncate)
	ge, ok := err.(*gwerr.Error)
	if !ok || ge.Code != gwerr.ERecordCorrupt {
		t.Fatalf("期望 E_RECORD_CORRUPT, 得到 %v", err)
	}
	if _, ok := ge.Context["offset"]; !ok {
		t.Fatal("错误未指明偏移")
	}
}

func TestCorruptLengthPrefix(t *testing.T) {
	data := EncodeBinary(sampleRecs(), true)
	data[10] = 0xFF // 第一条记录长度前缀
	_, err := Import(data, TruncTruncate)
	ge := err.(*gwerr.Error)
	if ge.Code != gwerr.ERecordCorrupt || !strings.Contains(ge.Message, "不符") {
		t.Fatalf("期望长度前缀不符错误, 得到 %v", ge)
	}
}

func TestLegacyV1Downgrade(t *testing.T) {
	recs := sampleRecs()
	gib := []float32{8, 8}
	rep, err := Import(EncodeBinaryV1(recs, gib, true), TruncTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Verdict != VerdictDowngrade {
		t.Fatalf("期望降级接受, 得到 %s", rep.Verdict)
	}
	if len(rep.Downgrades) == 0 {
		t.Fatal("缺少降级明细")
	}
	for _, r := range rep.Records {
		if r.Toolchain != DefaultToolchain {
			t.Fatalf("缺省工具链未补齐: %s", r.Toolchain)
		}
		if r.VRAMBytes != 8<<30 {
			t.Fatalf("GiB 归一化错误: %d", r.VRAMBytes)
		}
	}
}

func TestFutureVersionRejected(t *testing.T) {
	future := `{"_meta":{"format":"grt-jsonl","version":3,"shard":0,"shards":1}}` + "\n" +
		`{"model_id":"m","endpoint":"e","vram_budget":"1Mi","unknown_required_field":{}}` + "\n"
	_, err := Import([]byte(future), TruncTruncate)
	ge, ok := err.(*gwerr.Error)
	if !ok || ge.Code != gwerr.EFormatUnsupported {
		t.Fatalf("期望 E_FORMAT_UNSUPPORTED, 得到 %v", err)
	}
}

func TestTimeNormalizationRounding(t *testing.T) {
	// 微秒 1234567890123456 → 毫秒，truncate 丢弃 456us
	ms, dropped, err := NormalizeTimeUs(1234567890123456, TruncTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if ms != 1234567890123 || dropped != 456 {
		t.Fatalf("truncate: ms=%d dropped=%d", ms, dropped)
	}
	// round-half: 600us → 进位
	ms, dropped, _ = NormalizeTimeUs(1000600, TruncRound)
	if ms != 1001 || dropped != -400 {
		t.Fatalf("round: ms=%d dropped=%d", ms, dropped)
	}
	// JSONL 导入累计舍入误差
	jsonl := `{"_meta":{"format":"grt-jsonl","version":2,"shard":0,"shards":1}}` + "\n" +
		`{"model_id":"m","endpoint":"e","vram_budget":"8Mi","concurrency_cap":1,"toolchain_version":"tc-1.0.0","base_time_us":1000123,"retention_us":2000456,"last_write_us":3000789}` + "\n"
	rep, err := Import([]byte(jsonl), TruncTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if rep.CumulativeRoundUs != 123+456+789 {
		t.Fatalf("累计舍入误差错误: %d", rep.CumulativeRoundUs)
	}
	if len(rep.Rounding) != 3 {
		t.Fatalf("舍入明细条数错误: %d", len(rep.Rounding))
	}
	rec := rep.Records[0]
	if rec.ExpiresAtMs != rec.BaseTimeMs+rec.RetentionMs {
		t.Fatal("绝对过期时刻换算错误")
	}
}

func TestVRAMOverflow(t *testing.T) {
	if _, err := ParseVRAM("18014398509481984Gi"); err == nil {
		t.Fatal("期望溢出错误")
	} else if ge := err.(*gwerr.Error); ge.Code != gwerr.EUnitOverflow {
		t.Fatalf("期望 E_UNIT_OVERFLOW, 得到 %v", ge)
	}
	v, err := ParseVRAM("8192Mi")
	if err != nil || v != 8192<<20 {
		t.Fatalf("Mi 归一化错误: %d %v", v, err)
	}
	v, err = ParseVRAM("8Gi")
	if err != nil || v != 8<<30 {
		t.Fatalf("Gi 归一化错误: %d %v", v, err)
	}
}
