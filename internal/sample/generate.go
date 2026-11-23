package sample

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"dns-cache-workbench/internal/workbench"
)

const (
	InjectionTruncatedBinary = "truncated-binary"
	InjectionUnknownVersion  = "unknown-version"
	InjectionTextOverflow    = "text-overflow"
	InjectionNameConflict    = "name-conflict"
	InjectionSnapshotJump    = "snapshot-jump"
)

var InjectionKinds = []string{
	InjectionTruncatedBinary,
	InjectionUnknownVersion,
	InjectionTextOverflow,
	InjectionNameConflict,
	InjectionSnapshotJump,
}

type sampleRecord struct {
	Name    string
	Type    string
	Class   uint16
	TTL     uint32
	Address string
}

type sampleEvent struct {
	Kind        string
	TxID        uint32
	Flags       uint16
	Tick        uint64
	Name        string
	Answers     []sampleRecord
	Additionals []sampleRecord
}

func cleanEvents() (binaryV1, binaryV2 []sampleEvent, jsonEvents []sampleEvent, textEvents []sampleEvent) {
	binaryV1 = []sampleEvent{
		{Kind: workbench.KindQuery, TxID: 0x3003, Flags: 0x0100, Tick: 1, Name: "flip.example.test"},
		{Kind: workbench.KindQuery, TxID: 0x4004, Flags: 0x0100, Tick: 1, Name: "steady.example.test"},
	}
	binaryV2 = []sampleEvent{
		{Kind: workbench.KindResponse, TxID: 0x4004, Flags: 0x8180, Tick: 2, Name: "steady.example.test", Answers: []sampleRecord{
			{Name: "steady.example.test", Type: "A", Class: 1, TTL: 300, Address: "192.0.2.20"},
		}},
		{Kind: workbench.KindResponse, TxID: 0x3003, Flags: 0x8180, Tick: 2, Name: "flip.example.test", Answers: []sampleRecord{
			{Name: "flip.example.test", Type: "A", Class: 1, TTL: 120, Address: "198.18.0.1"},
		}},
		{Kind: workbench.KindResponse, TxID: 0x2002, Flags: 0x8180, Tick: 4, Name: "unsolicited.example.test", Answers: []sampleRecord{
			{Name: "unsolicited.example.test", Type: "A", Class: 1, TTL: 300, Address: "198.18.1.1"},
		}},
	}
	jsonEvents = []sampleEvent{
		{Kind: workbench.KindQuery, TxID: 0x1001, Flags: 0x0100, Tick: 1, Name: "Cache.Example.Test."},
		{Kind: workbench.KindResponse, TxID: 0x1001, Flags: 0x8180, Tick: 2, Name: "cache.example.test", Answers: []sampleRecord{
			{Name: "cache.example.test", Type: "A", Class: 1, TTL: 60, Address: "192.0.2.10"},
		}, Additionals: []sampleRecord{
			{Name: "evil.additional.test", Type: "A", Class: 1, TTL: 999, Address: "203.0.113.66"},
		}},
	}
	textEvents = []sampleEvent{
		{Kind: workbench.KindCache, TxID: 0x1001, Flags: 0x0100, Tick: 3, Name: "cache.example.test", Answers: []sampleRecord{
			{Name: "cache.example.test", Type: "A", Class: 1, TTL: 12, Address: "192.0.2.10"},
		}},
		{Kind: workbench.KindCache, TxID: 0x3003, Flags: 0x0100, Tick: 3, Name: "flip.example.test", Answers: []sampleRecord{
			{Name: "flip.example.test", Type: "A", Class: 1, TTL: 120, Address: "198.18.0.2"},
		}},
		{Kind: workbench.KindCache, TxID: 0x4004, Flags: 0x0100, Tick: 3, Name: "steady.example.test", Answers: []sampleRecord{
			{Name: "steady.example.test", Type: "A", Class: 1, TTL: 300, Address: "192.0.2.20"},
		}},
	}
	return binaryV1, binaryV2, jsonEvents, textEvents
}

func Generate(outDir string) error {
	cleanDir := filepath.Join(outDir, "clean")
	injectionDir := filepath.Join(outDir, "injections")
	if err := os.RemoveAll(cleanDir); err != nil {
		return err
	}
	if err := os.RemoveAll(injectionDir); err != nil {
		return err
	}
	if err := WriteClean(cleanDir); err != nil {
		return err
	}
	for _, kind := range InjectionKinds {
		if err := WriteInjection(injectionDir, kind, cleanDir); err != nil {
			return err
		}
	}
	return nil
}

func WriteClean(dir string) error {
	binaryV1, binaryV2, jsonEvents, textEvents := cleanEvents()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := writeBinary(filepath.Join(dir, "binary-v1.dnsbin"), 1, binaryV1); err != nil {
		return err
	}
	if err := writeBinary(filepath.Join(dir, "binary-v2.dnsbin"), 2, binaryV2); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "json-summary.jsonl"), jsonEvents); err != nil {
		return err
	}
	if err := writeText(filepath.Join(dir, "text-blocks.dnslog"), textEvents); err != nil {
		return err
	}
	return nil
}

func WriteInjection(root, kind, cleanDir string) error {
	dir := filepath.Join(root, kind)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, name := range []string{"binary-v1.dnsbin", "binary-v2.dnsbin", "json-summary.jsonl", "text-blocks.dnslog"} {
		if err := copyFile(filepath.Join(cleanDir, name), filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	switch kind {
	case InjectionTruncatedBinary:
		binaryV2Path := filepath.Join(dir, "binary-v2.dnsbin")
		malformed := []byte{'D', 'N', 'S', 'L', 2, 0, 0, 0, 20, 0, 1}
		file, err := os.OpenFile(binaryV2Path, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer file.Close()
		if _, err := file.Write(malformed); err != nil {
			return err
		}
	case InjectionUnknownVersion:
		binaryV2Path := filepath.Join(dir, "binary-v2.dnsbin")
		unknown := []byte{'D', 'N', 'S', 'L', 9, 0, 0, 0, 12, 1, 2, 3}
		file, err := os.OpenFile(binaryV2Path, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer file.Close()
		if _, err := file.Write(unknown); err != nil {
			return err
		}
	case InjectionTextOverflow:
		textPath := filepath.Join(dir, "text-blocks.dnslog")
		file, err := os.OpenFile(textPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer file.Close()
		if _, err := file.WriteString("999999:short\n"); err != nil {
			return err
		}
	case InjectionNameConflict:
		if err := writeJSON(filepath.Join(dir, "txid-conflict.jsonl"), []sampleEvent{
			{Kind: workbench.KindQuery, TxID: 0x1001, Flags: 0x0100, Tick: 4, Name: "conflict.example.test"},
		}); err != nil {
			return err
		}
	case InjectionSnapshotJump:
		// The jump is an external runtime parameter; the input remains clean.
	default:
		return fmt.Errorf("unknown injection kind %q", kind)
	}
	return nil
}

func writeBinary(path string, version byte, events []sampleEvent) error {
	var b strings.Builder
	for _, event := range events {
		entry, err := encodeBinaryEntry(version, event)
		if err != nil {
			return err
		}
		b.Write(entry)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func encodeBinaryEntry(version byte, event sampleEvent) ([]byte, error) {
	name, nameWire, err := workbench.NormalizeName(event.Name)
	if err != nil {
		return nil, err
	}
	_ = name
	kindByte := byte(0)
	switch event.Kind {
	case workbench.KindQuery:
		kindByte = 0
	case workbench.KindResponse:
		kindByte = 1
	case workbench.KindCache:
		kindByte = 2
	default:
		return nil, fmt.Errorf("unknown event kind %q", event.Kind)
	}
	body := append([]byte(nil), nameWire...)
	for _, record := range event.Answers {
		encoded, err := encodeBinaryRecord(version, record)
		if err != nil {
			return nil, err
		}
		body = append(body, encoded...)
	}
	for _, record := range event.Additionals {
		encoded, err := encodeBinaryRecord(version, record)
		if err != nil {
			return nil, err
		}
		body = append(body, encoded...)
	}
	specificHeaderSize := 17
	layoutHeaderSize := 26
	if version == 2 {
		specificHeaderSize = 23
		layoutHeaderSize = 32
	}
	total := layoutHeaderSize + len(body)
	entry := make([]byte, 9, total)
	copy(entry[0:4], []byte("DNSL"))
	entry[4] = version
	binary.BigEndian.PutUint32(entry[5:9], uint32(total))
	if version == 1 {
		header := make([]byte, specificHeaderSize)
		header[0] = kindByte
		binary.BigEndian.PutUint16(header[1:3], event.Flags)
		binary.BigEndian.PutUint32(header[3:7], event.TxID)
		binary.BigEndian.PutUint32(header[7:11], uint32(event.Tick))
		binary.BigEndian.PutUint16(header[11:13], uint16(len(nameWire)))
		binary.BigEndian.PutUint16(header[13:15], uint16(len(event.Answers)))
		binary.BigEndian.PutUint16(header[15:17], uint16(len(event.Additionals)))
		entry = append(entry, header...)
	} else {
		header := make([]byte, specificHeaderSize)
		binary.LittleEndian.PutUint32(header[0:4], event.TxID)
		header[4] = kindByte
		binary.LittleEndian.PutUint16(header[5:7], event.Flags)
		binary.LittleEndian.PutUint64(header[7:15], event.Tick)
		binary.LittleEndian.PutUint16(header[15:17], uint16(len(nameWire)))
		binary.LittleEndian.PutUint16(header[17:19], uint16(len(event.Answers)))
		binary.LittleEndian.PutUint16(header[19:21], uint16(len(event.Additionals)))
		binary.LittleEndian.PutUint16(header[21:23], 2)
		entry = append(entry, header...)
	}
	entry = append(entry, body...)
	return entry, nil
}

func encodeBinaryRecord(version byte, input sampleRecord) ([]byte, error) {
	_, nameWire, err := workbench.NormalizeName(input.Name)
	if err != nil {
		return nil, err
	}
	typeCode, err := workbench.TypeCode(input.Type)
	if err != nil {
		return nil, err
	}
	rdata, err := addressRData(input.Type, input.Address)
	if err != nil {
		return nil, err
	}
	encoded := append([]byte(nil), nameWire...)
	fields := make([]byte, 10)
	if version == 1 {
		binary.BigEndian.PutUint16(fields[0:2], typeCode)
		binary.BigEndian.PutUint16(fields[2:4], input.Class)
		binary.BigEndian.PutUint32(fields[4:8], input.TTL)
		binary.BigEndian.PutUint16(fields[8:10], uint16(len(rdata)))
	} else {
		binary.LittleEndian.PutUint16(fields[0:2], typeCode)
		binary.LittleEndian.PutUint16(fields[2:4], input.Class)
		binary.LittleEndian.PutUint32(fields[4:8], input.TTL)
		binary.LittleEndian.PutUint16(fields[8:10], uint16(len(rdata)))
	}
	encoded = append(encoded, fields...)
	encoded = append(encoded, rdata...)
	return encoded, nil
}

func addressRData(typeText, address string) ([]byte, error) {
	typeCode, err := workbench.TypeCode(typeText)
	if err != nil {
		return nil, err
	}
	if typeCode != 1 && typeCode != 28 {
		return []byte{}, nil
	}
	addr, err := netip.ParseAddr(address)
	if err != nil {
		return nil, err
	}
	if typeCode == 1 {
		if !addr.Is4() {
			return nil, fmt.Errorf("A record requires IPv4 address: %s", address)
		}
		a4 := addr.As4()
		return a4[:], nil
	}
	if !addr.Is6() || addr.Is4() {
		return nil, fmt.Errorf("AAAA record requires IPv6 address: %s", address)
	}
	a16 := addr.As16()
	return a16[:], nil
}

func writeJSON(path string, events []sampleEvent) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, event := range events {
		name, _, err := workbench.NormalizeName(event.Name)
		if err != nil {
			return err
		}
		kind := event.Kind
		answers := recordsJSON(event.Answers)
		additionals := recordsJSON(event.Additionals)
		line := fmt.Sprintf(`{"event":%q,"txid":"0x%08x","flags":"0x%04x","parser_version":"v2","sample":"S%06d","name":%q,"answers":[%s],"additionals":[%s]}`,
			kind, event.TxID, event.Flags, event.Tick, name, answers, additionals)
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func recordsJSON(records []sampleRecord) string {
	parts := make([]string, 0, len(records))
	for _, record := range records {
		parts = append(parts, fmt.Sprintf(`{"name":%q,"type":%q,"class":%d,"ttl":%d,"address":%q}`,
			record.Name, record.Type, record.Class, record.TTL, record.Address))
	}
	return strings.Join(parts, ",")
}

func writeText(path string, events []sampleEvent) error {
	var b strings.Builder
	for _, event := range events {
		name, _, err := workbench.NormalizeName(event.Name)
		if err != nil {
			return err
		}
		txid := "0x" + strconv.FormatUint(uint64(event.TxID), 16)
		if event.TxID == 0x1001 {
			txid = strconv.FormatUint(uint64(event.TxID), 10)
		}
		payload := fmt.Sprintf("txid=%s|kind=%s|flags=0x%04x|sample=S%06d|version=v1|name=%s|answers=%s",
			txid, event.Kind, event.Flags, event.Tick, name, textRecords(event.Answers))
		if len(event.Additionals) > 0 {
			payload += "|additionals=" + textRecords(event.Additionals)
		}
		fmt.Fprintf(&b, "%08d:%s\n", len([]byte(payload)), payload)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func textRecords(records []sampleRecord) string {
	parts := make([]string, 0, len(records))
	for _, record := range records {
		parts = append(parts, fmt.Sprintf("%s,%s,%d,%d,%s", record.Name, record.Type, record.Class, record.TTL, record.Address))
	}
	return strings.Join(parts, ";")
}

func copyFile(source, destination string) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(destination, data, 0o644)
}

// Files returns the sorted relative files in the clean deterministic sample.
func Files(cleanDir string) ([]string, error) {
	var list []string
	err := filepath.WalkDir(cleanDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		list = append(list, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(list)
	return list, nil
}
