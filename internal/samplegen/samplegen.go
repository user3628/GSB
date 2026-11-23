// Package samplegen deterministically materializes a multi-format audit corpus
// into a local working directory. It needs no network, accounts or clock: all
// timestamps and payloads are fixed literals so repeated runs are identical.
package samplegen

import (
	"os"
	"path/filepath"
	"strconv"

	"forensicworkbench/internal/bincode"
	"forensicworkbench/internal/codes"
)

// Fixed deterministic anchor: 2024-01-02T03:04:05Z.
const baseEpochMS int64 = 1704164645000

// File names emitted in the target directory.
const (
	BinV1File   = "audit_v1.bin"
	BinV2File   = "audit_v2.bin"
	JSONLFile   = "events.jsonl"
	TextFile    = "legacy.txt"
	MalTrunc    = "malformed_truncated.bin"
	MalJSONType = "malformed_json_type.jsonl"
	MalTextLen  = "malformed_text_len.txt"
	MalFT       = "malformed_filetime.bin"
	MalVersion  = "malformed_unknown_version.bin"
)

// Bundle lists every generated file with its logical format.
type Bundle struct {
	Dir   string
	Files []Generated
}

// Generated describes one materialized source.
type Generated struct {
	Path   string
	Format string
}

// Generate writes the full corpus under dir and returns its manifest.
func Generate(dir string) (*Bundle, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	b := &Bundle{Dir: dir}
	writers := []struct {
		name string
		data []byte
		fmt  string
	}{
		{BinV1File, must(bincode.Marshal(bincode.V1, baseEpochMS, v1Records())), codes.SourceBinary},
		{BinV2File, must(bincode.Marshal(bincode.V2, baseEpochMS, v2Records())), codes.SourceBinary},
		{JSONLFile, jsonlBytes(), codes.SourceJSONL},
		{TextFile, textBytes(), codes.SourceText},
		{MalTrunc, truncatedBinary(), codes.SourceBinary},
		{MalJSONType, malformedJSONL(), codes.SourceJSONL},
		{MalTextLen, malformedText(), codes.SourceText},
		{MalFT, malformedFiletime(), codes.SourceBinary},
		{MalVersion, malformedVersion(), codes.SourceBinary},
	}
	for _, w := range writers {
		p := filepath.Join(dir, w.name)
		if err := os.WriteFile(p, w.data, 0o644); err != nil {
			return nil, err
		}
		// Keep the manifest path relative to the corpus directory so that
		// snapshots and reports never embed machine-specific absolute prefixes.
		b.Files = append(b.Files, Generated{Path: filepath.Join("samples", w.name), Format: w.fmt})
	}
	return b, nil
}

func must(data []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return data
}

// v1Records uses epoch-ms / RFC3339 timestamps and the v1 (no PPID) layout.
func v1Records() []bincode.Record {
	return []bincode.Record{
		{TypeCode: bincode.TypeProcessCreate, TimeEnc: bincode.EncEpochMS, RawTime: strconv.FormatInt(baseEpochMS, 10),
			SessionID: "S-A", PID: 100, Subject: "alice", Object: "C:\\win\\explorer.exe"},
		{TypeCode: bincode.TypeProcessCreate, TimeEnc: bincode.EncEpochMS, RawTime: strconv.FormatInt(baseEpochMS+1000, 10),
			SessionID: "S-A", PID: 101, Subject: "alice", Object: "C:\\tools\\cmd.exe"},
		{TypeCode: bincode.TypeFileAccess, TimeEnc: bincode.EncRFC3339, RawTime: "2024-01-02T03:04:07Z",
			SessionID: "S-A", PID: 101, Subject: "alice", Object: "C:\\data\\secret.docx"},
		{TypeCode: bincode.TypeRegistryWrite, TimeEnc: bincode.EncEpochMS, RawTime: strconv.FormatInt(baseEpochMS+3000, 10),
			SessionID: "S-A", PID: 101, Subject: "alice", Object: "HKCU\\Software\\Evil"},
	}
}

// v2Records uses FILETIME and RFC3339, exercises PPID and module load, and
// includes an orphan child (pid 300, ppid 999 never created) plus a second
// session to prove sessions never cross-link.
func v2Records() []bincode.Record {
	// FILETIME for baseEpochMS (2024-01-02T03:04:05Z).
	ftBase := uint64(133490702450000000)
	return []bincode.Record{
		{TypeCode: bincode.TypeProcessCreate, TimeEnc: bincode.EncFiletime, RawTime: strconv.FormatUint(ftBase, 10),
			SessionID: "S-B", PID: 200, PPID: 0, Subject: "system", Object: "C:\\win\\services.exe"},
		{TypeCode: bincode.TypeProcessCreate, TimeEnc: bincode.EncFiletime, RawTime: strconv.FormatUint(ftBase+10000, 10),
			SessionID: "S-B", PID: 201, PPID: 200, Subject: "svc", Object: "C:\\app\\worker.exe"},
		{TypeCode: bincode.TypeModuleLoad, TimeEnc: bincode.EncFiletime, RawTime: strconv.FormatUint(ftBase+20000, 10),
			SessionID: "S-B", PID: 201, PPID: 200, Subject: "svc", Object: "C:\\app\\helper.dll"},
		{TypeCode: bincode.TypeNetworkConn, TimeEnc: bincode.EncRFC3339, RawTime: "2024-01-02T03:04:09Z",
			SessionID: "S-B", PID: 201, PPID: 200, Subject: "svc", Object: "10.0.0.9:443"},
		// Orphan: parent 999 has no creation event.
		{TypeCode: bincode.TypeProcessCreate, TimeEnc: bincode.EncFiletime, RawTime: strconv.FormatUint(ftBase+30000, 10),
			SessionID: "S-B", PID: 300, PPID: 999, Subject: "svc", Object: "C:\\tmp\\mystery.exe"},
		// Separate session; pid 201 here must not link to S-B's 201.
		{TypeCode: bincode.TypeFileAccess, TimeEnc: bincode.EncRFC3339, RawTime: "2024-01-02T03:04:10Z",
			SessionID: "S-C", PID: 201, Subject: "bob", Object: "D:\\share\\readme.txt"},
	}
}
