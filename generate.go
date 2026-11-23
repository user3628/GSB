package dnsdetect

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const sampleObservationRFC3339 = "2024-01-01T00:00:00Z"

type MalformedSample struct {
	Name         string
	Path         string
	Kind         SourceKind
	ExpectedCode string
	Description  string
}

type Dataset struct {
	Dir                   string
	ObservationStartTicks int64
	Sources               []SourceSpec
	Malformed             []MalformedSample
	SnapshotGapDB         string
}

type generatedRecord struct {
	section RecordSection
	rrType  uint16
	ttl     uint32
	owner   string
	address string
	data    []byte
}

type generatedEvent struct {
	relativeNS int64
	txid       uint32
	flags      uint16
	question   string
	qtype      uint16
	role       RecordRole
	records    []generatedRecord
}

// GenerateDataset writes a deterministic local dataset. It never contacts the
// network and overwrites only the known sample files under dir.
func GenerateDataset(dir string) (*Dataset, error) {
	if dir == "" {
		return nil, newBusinessError(CodeIO, dir, -1, "sample directory is empty", nil)
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, newBusinessError(CodeIO, dir, -1, "cannot resolve sample directory", err)
	}
	if err := os.MkdirAll(filepath.Join(absolute, "malformed"), 0o755); err != nil {
		return nil, newBusinessError(CodeIO, absolute, -1, "cannot create sample directory", err)
	}
	startTicks, _, err := TicksFromRFC3339(sampleObservationRFC3339)
	if err != nil {
		return nil, err
	}
	dataset := &Dataset{Dir: absolute, ObservationStartTicks: startTicks}
	v1Events := []generatedEvent{
		{relativeNS: relNS(100, 0), txid: 0x1002, flags: 0x0100, question: "V1.Example.", qtype: TypeA, role: RoleQuery},
		{relativeNS: relNS(200, 50), txid: 0x1002, flags: 0x8180, question: "v1.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 60, owner: "v1.example.", address: "192.0.2.2"},
		}},
	}
	v2Events := []generatedEvent{
		{relativeNS: relNS(300, 50), txid: 0x1001, flags: 0x0100, question: "Cross.Example.", qtype: TypeA, role: RoleQuery},
		{relativeNS: relNS(400, 0), txid: 0x1001, flags: 0x8180, question: "cross.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 120, owner: "cross.example.", address: "192.0.2.1"},
		}},
		{relativeNS: relNS(1000, 0), txid: 0x2002, flags: 0x8180, question: "unsolicited.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 999, owner: "unsolicited.example.", address: "192.0.2.66"},
		}},
		{relativeNS: relNS(1100, 50), txid: 0x2003, flags: 0x0100, question: "additional.example.", qtype: TypeA, role: RoleQuery},
		{relativeNS: relNS(1150, 0), txid: 0x2003, flags: 0x8180, question: "additional.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 300, owner: "additional.example.", address: "192.0.2.3"},
			{section: SectionAdditional, rrType: TypeA, ttl: 300, owner: "evil.example.", address: "203.0.113.66"},
		}},
	}
	jsonEvents := []generatedEvent{
		{relativeNS: relNS(300, 50), txid: 0x1001, flags: 0x0100, question: "CROSS.EXAMPLE.", qtype: TypeA, role: RoleQuery},
		{relativeNS: relNS(400, 50), txid: 0x1001, flags: 0x8180, question: "cross.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 120, owner: "cross.example.", address: "192.0.2.1"},
		}},
		{relativeNS: relNS(200, 0), txid: 0x1002, flags: 0x8180, question: "v1.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 60, owner: "v1.example.", address: "192.0.2.2"},
		}},
		{relativeNS: relNS(1300, 50), txid: 0x2004, flags: 0x0100, question: "ttl.example.", qtype: TypeA, role: RoleQuery},
		{relativeNS: relNS(1400, 0), txid: 0x2004, flags: 0x8180, question: "ttl.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 300, owner: "ttl.example.", address: "192.0.2.20"},
		}},
		{relativeNS: relNS(1500, 0), txid: 0x2005, flags: 0x0100, question: "multi.example.", qtype: TypeA, role: RoleQuery},
	}
	textEvents := []generatedEvent{
		{relativeNS: relNS(300, 0), txid: 0x1001, flags: 0x0100, question: "cross.example.", qtype: TypeA, role: RoleQuery},
		{relativeNS: relNS(400, 0), txid: 0x1001, flags: 0x8180, question: "cross.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 120, owner: "cross.example.", address: "192.0.2.1"},
		}},
		{relativeNS: relNS(1450, 0), txid: 0x2004, flags: 0x8180, question: "ttl.example.", qtype: TypeA, role: RoleCache, records: []generatedRecord{
			{section: SectionCache, rrType: TypeA, ttl: 30, owner: "ttl.example.", address: "192.0.2.20"},
		}},
		{relativeNS: relNS(1600, 0), txid: 0x2005, flags: 0x8180, question: "multi.example.", qtype: TypeA, role: RoleResponse, records: []generatedRecord{
			{section: SectionAnswer, rrType: TypeA, ttl: 300, owner: "multi.example.", address: "192.0.2.10"},
			{section: SectionAnswer, rrType: TypeA, ttl: 300, owner: "multi.example.", address: "192.0.2.11"},
		}},
	}

	v1Path := filepath.Join(absolute, "binary-v1.dnb")
	if err := writeDeterministicFile(v1Path, encodeBinaryDataset(1, "", "", v1Events)); err != nil {
		return nil, err
	}
	v2Path := filepath.Join(absolute, "binary-v2.dnb")
	if err := writeDeterministicFile(v2Path, encodeBinaryDataset(2, "192.0.2.53", "resolver-lab-1", v2Events)); err != nil {
		return nil, err
	}
	jsonPath := filepath.Join(absolute, "probe.jsonl")
	if err := writeDeterministicFile(jsonPath, encodeJSONDataset(startTicks, jsonEvents)); err != nil {
		return nil, err
	}
	textPath := filepath.Join(absolute, "legacy.txt")
	if err := writeDeterministicFile(textPath, encodeTextDataset(startTicks, textEvents)); err != nil {
		return nil, err
	}
	dataset.Sources = []SourceSpec{
		{Path: v2Path, Kind: SourceBinary, ObservationStart: startTicks},
		{Path: jsonPath, Kind: SourceJSONL},
		{Path: textPath, Kind: SourceText},
		{Path: v1Path, Kind: SourceBinary, ObservationStart: startTicks},
	}
	sort.Slice(dataset.Sources, func(i, j int) bool { return dataset.Sources[i].Path < dataset.Sources[j].Path })

	malformed, snapshotGapDB, err := generateMalformed(absolute, startTicks)
	if err != nil {
		return nil, err
	}
	dataset.Malformed = malformed
	dataset.SnapshotGapDB = snapshotGapDB
	return dataset, nil
}

func relNS(ticks, remainderNS int64) int64 {
	return ticks*NanosecondsPerTick + remainderNS
}

func writeDeterministicFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return newBusinessError(CodeIO, path, -1, "cannot create sample subdirectory", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return newBusinessError(CodeIO, path, -1, "cannot write sample file", err)
	}
	return nil
}

func encodeBinaryDataset(version uint16, sourceAddress, resolverID string, events []generatedEvent) []byte {
	var buffer bytes.Buffer
	buffer.WriteString(binaryMagic)
	writeLE16(&buffer, version)
	writeLE32(&buffer, uint32(len(events)))
	if version == 2 {
		writeLE16(&buffer, uint16(len(sourceAddress)))
		buffer.WriteString(sourceAddress)
		writeLE16(&buffer, uint16(len(resolverID)))
		buffer.WriteString(resolverID)
	}
	for _, event := range events {
		writeLE64(&buffer, uint64(event.relativeNS))
		writeLE32(&buffer, event.txid)
		writeLE16(&buffer, event.flags)
		writeLE16(&buffer, event.qtype)
		buffer.WriteByte(byte(roleToByte(event.role)))
		writeLE16(&buffer, uint16(len(event.question)))
		buffer.WriteString(event.question)
		writeLE16(&buffer, uint16(len(event.records)))
		for _, record := range event.records {
			buffer.WriteByte(byte(sectionToByte(record.section)))
			writeLE16(&buffer, record.rrType)
			writeLE32(&buffer, record.ttl)
			writeLE16(&buffer, uint16(len(record.owner)))
			buffer.WriteString(record.owner)
			rdata := record.data
			if record.rrType == TypeA {
				rdata = sampleIPBytes(record.address, true)
			} else if record.rrType == TypeAAAA {
				rdata = sampleIPBytes(record.address, false)
			}
			writeLE16(&buffer, uint16(len(rdata)))
			buffer.Write(rdata)
		}
	}
	return buffer.Bytes()
}

func encodeJSONDataset(startTicks int64, events []generatedEvent) []byte {
	type jsonRR struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		TTL     uint32 `json:"ttl"`
		Address string `json:"address,omitempty"`
	}
	type jsonEvent struct {
		Time        string   `json:"time"`
		TXID        string   `json:"txid"`
		Flags       string   `json:"flags"`
		Role        string   `json:"role"`
		Question    string   `json:"qname"`
		QType       uint16   `json:"qtype"`
		Answers     []jsonRR `json:"answers,omitempty"`
		Additionals []jsonRR `json:"additionals,omitempty"`
		Cache       []jsonRR `json:"cache,omitempty"`
	}
	var buffer bytes.Buffer
	for _, event := range events {
		ticks := startTicks + event.relativeNS/NanosecondsPerTick
		remainder := event.relativeNS % NanosecondsPerTick
		wire := jsonEvent{
			Time:     TicksToTime(ticks).Add(time.Duration(remainder) * time.Nanosecond).Format(time.RFC3339Nano),
			TXID:     fmt.Sprintf("0x%08x", event.txid),
			Flags:    fmt.Sprintf("0x%04x", event.flags),
			Role:     string(event.role),
			Question: event.question,
			QType:    event.qtype,
		}
		for _, record := range event.records {
			rr := jsonRR{Name: record.owner, Type: rrTypeName(record.rrType), TTL: record.ttl, Address: record.address}
			switch record.section {
			case SectionAnswer:
				wire.Answers = append(wire.Answers, rr)
			case SectionAdditional:
				wire.Additionals = append(wire.Additionals, rr)
			case SectionCache:
				wire.Cache = append(wire.Cache, rr)
			}
		}
		line, _ := json.Marshal(wire)
		buffer.Write(line)
		buffer.WriteByte('\n')
	}
	return buffer.Bytes()
}

func encodeTextDataset(startTicks int64, events []generatedEvent) []byte {
	var output bytes.Buffer
	for _, event := range events {
		ticks := startTicks + event.relativeNS/NanosecondsPerTick
		filetime, _ := FILETIMEFromTicks(ticks)
		var block bytes.Buffer
		fmt.Fprintf(&block, "txid=0x%08x\n", event.txid)
		fmt.Fprintf(&block, "flags=0x%04x\n", event.flags)
		fmt.Fprintf(&block, "role=%s\n", event.role)
		fmt.Fprintf(&block, "qname=%s\n", event.question)
		fmt.Fprintf(&block, "qtype=%d\n", event.qtype)
		fmt.Fprintf(&block, "filetime=%d\n", filetime)
		for _, record := range event.records {
			key := string(record.section)
			fmt.Fprintf(&block, "%s=%s|%s|%d|%s\n", key, record.owner, rrTypeName(record.rrType), record.ttl, record.address)
		}
		payload := block.Bytes()
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
		output.Write(prefix[:])
		output.Write(payload)
	}
	return output.Bytes()
}

func generateMalformed(dir string, startTicks int64) ([]MalformedSample, string, error) {
	malformedDir := filepath.Join(dir, "malformed")
	samples := make([]MalformedSample, 0, 6)
	add := func(name string, kind SourceKind, code, description string, data []byte) error {
		path := filepath.Join(malformedDir, name)
		if err := writeDeterministicFile(path, data); err != nil {
			return err
		}
		samples = append(samples, MalformedSample{Name: name, Path: path, Kind: kind, ExpectedCode: code, Description: description})
		return nil
	}
	truncated := encodeBinaryDataset(1, "", "", []generatedEvent{{
		relativeNS: 100, txid: 0x9001, flags: 0x0100, question: "truncated.example.", qtype: TypeA, role: RoleQuery,
	}})
	if err := add("truncated.dnb", SourceBinary, CodeBinaryTruncated, "declared binary record ends inside its question name", truncated[:len(truncated)-3]); err != nil {
		return nil, "", err
	}
	unknown := append([]byte(binaryMagic), 99, 0, 1, 0, 0, 0)
	if err := add("unknown-version.dnb", SourceBinary, CodeUnknownVersion, "header advertises an unsupported format version", unknown); err != nil {
		return nil, "", err
	}
	var badLength bytes.Buffer
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], 1024)
	badLength.Write(prefix[:])
	badLength.WriteString("txid=1\n")
	if err := add("length-overrun.txt", SourceText, CodeTextLength, "length prefix exceeds the remaining text bytes", badLength.Bytes()); err != nil {
		return nil, "", err
	}
	conflictEvents := []generatedEvent{
		{relativeNS: relNS(100, 0), txid: 0x7777, flags: 0x0100, question: "first.example.", qtype: TypeA, role: RoleQuery},
		{relativeNS: relNS(200, 0), txid: 0x7777, flags: 0x0100, question: "second.example.", qtype: TypeA, role: RoleQuery},
	}
	if err := add("txid-qname-conflict.jsonl", SourceJSONL, CodeTXIDQNameConflict, "one txid maps to two normalized questions in one fixed window", encodeJSONDataset(startTicks, conflictEvents)); err != nil {
		return nil, "", err
	}
	var overflowBlock bytes.Buffer
	fmt.Fprintf(&overflowBlock, "txid=0x9005\nrole=response\nqname=overflow.example.\nfiletime=18446744073709551615\n")
	var overflowPrefix [4]byte
	binary.BigEndian.PutUint32(overflowPrefix[:], uint32(overflowBlock.Len()))
	var overflowBytes bytes.Buffer
	overflowBytes.Write(overflowPrefix[:])
	overflowBytes.Write(overflowBlock.Bytes())
	if err := add("filetime-overflow.txt", SourceText, CodeTimeOverflow, "FILETIME conversion exceeds int64 Unix tick range", overflowBytes.Bytes()); err != nil {
		return nil, "", err
	}

	gapPath := filepath.Join(malformedDir, "snapshot-gap.db")
	_ = os.Remove(gapPath)
	_ = os.Remove(gapPath + "-wal")
	_ = os.Remove(gapPath + "-shm")
	store, err := OpenSnapshotStore(gapPath)
	if err != nil {
		return nil, "", err
	}
	ctx := context.Background()
	for version := 1; version <= 3; version++ {
		_, commitErr := store.Commit(ctx, SnapshotDraft{
			CreatedTicks:   startTicks + int64(version),
			FormatVersions: []int{1, 2},
			SourcePriorities: []SourcePriority{
				{Kind: SourceBinary, Priority: 30}, {Kind: SourceJSONL, Priority: 20}, {Kind: SourceText, Priority: 10},
			},
			LedgerDigest:   zeroDigest,
			WindowProgress: []WindowProgress{{Index: int64(version), StartTicks: startTicks, EndTicks: startTicks + 1}},
			Content:        []byte(fmt.Sprintf("snapshot-gap-version-%d", version)),
		})
		if commitErr != nil {
			store.Close()
			return nil, "", commitErr
		}
	}
	if _, err := store.db.ExecContext(ctx, "DELETE FROM snapshots WHERE version = 2"); err != nil {
		store.Close()
		return nil, "", newBusinessError(CodeIO, gapPath, -1, "cannot inject snapshot gap", err)
	}
	if err := store.Close(); err != nil {
		return nil, "", newBusinessError(CodeIO, gapPath, -1, "cannot close snapshot gap sample", err)
	}
	samples = append(samples, MalformedSample{
		Name:         "snapshot-gap.db",
		Path:         gapPath,
		ExpectedCode: CodeSnapshotVersion,
		Description:  "snapshot versions 1 and 3 remain after deleting contiguous version 2",
	})
	return samples, gapPath, nil
}

func writeLE16(buffer *bytes.Buffer, value uint16) {
	var bytesValue [2]byte
	binary.LittleEndian.PutUint16(bytesValue[:], value)
	buffer.Write(bytesValue[:])
}

func writeLE32(buffer *bytes.Buffer, value uint32) {
	var bytesValue [4]byte
	binary.LittleEndian.PutUint32(bytesValue[:], value)
	buffer.Write(bytesValue[:])
}

func writeLE64(buffer *bytes.Buffer, value uint64) {
	var bytesValue [8]byte
	binary.LittleEndian.PutUint64(bytesValue[:], value)
	buffer.Write(bytesValue[:])
}

func roleToByte(role RecordRole) byte {
	switch role {
	case RoleQuery:
		return 0
	case RoleResponse:
		return 1
	default:
		return 2
	}
}

func sectionToByte(section RecordSection) byte {
	switch section {
	case SectionAnswer:
		return 0
	case SectionAuthority:
		return 1
	case SectionAdditional:
		return 2
	default:
		return 3
	}
}

func rrTypeName(rrType uint16) string {
	switch rrType {
	case TypeA:
		return "A"
	case TypeAAAA:
		return "AAAA"
	default:
		return fmt.Sprintf("%d", rrType)
	}
}

func sampleIPBytes(value string, ipv4 bool) []byte {
	ip := net.ParseIP(value)
	if ip == nil {
		if ipv4 {
			return make([]byte, net.IPv4len)
		}
		return make([]byte, net.IPv6len)
	}
	if ipv4 {
		return append([]byte(nil), ip.To4()...)
	}
	return append([]byte(nil), ip.To16()...)
}
