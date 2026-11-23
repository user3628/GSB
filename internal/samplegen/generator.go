package samplegen

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/source"
)

const (
	FaultNone                = "none"
	FaultUnknownVersion      = "unknown-version"
	FaultTextPrefixOOB       = "text-prefix-oob"
	FaultUnterminatedQuote   = "unterminated-quote"
	FaultUnknownAccount      = "unknown-account"
	FaultDeclarationConflict = "declaration-conflict"
	FaultSnapshotGap         = "snapshot-gap"
)

var ValidFaults = []string{
	FaultNone,
	FaultUnknownVersion,
	FaultTextPrefixOOB,
	FaultUnterminatedQuote,
	FaultUnknownAccount,
	FaultDeclarationConflict,
	FaultSnapshotGap,
}

type Options struct {
	TargetFormat string
	Fault        string
}

type serviceSpec struct {
	Name              string
	DisplayName       string
	StartType         domain.StartType
	BinaryPath        string
	JSONPath          string
	TextPath          string
	AccountRaw        string
	AccountType       string
	AccountEnum       uint8
	Logons            []domain.LogonCapability
	WritableHint      bool
	TextStartOverride domain.StartType
}

func Generate(dir string, options Options) ([]source.Input, error) {
	target := strings.ToLower(strings.TrimSpace(options.TargetFormat))
	if target == "" {
		target = "v2"
	}
	if target != "v1" && target != "v2" {
		return nil, fmt.Errorf("target format must be v1 or v2")
	}
	fault := strings.ToLower(strings.TrimSpace(options.Fault))
	if fault == "" {
		fault = FaultNone
	}
	if !validFault(fault) {
		return nil, fmt.Errorf("unknown fault %q", options.Fault)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	specs := baseSpecs()
	if fault == FaultUnterminatedQuote {
		for index := range specs {
			if specs[index].Name == "Quoted Tool Service" {
				specs[index].JSONPath = "\"C:\\Program Files\\Quoted Tool\\quoted.exe -mode quiet"
			}
		}
	}
	if fault == FaultUnknownAccount {
		for index := range specs {
			if specs[index].Name == "Custom Worker" {
				specs[index].AccountRaw = "NT AUTHORITY\\Ghost"
				specs[index].AccountType = "Unknown"
				specs[index].AccountEnum = 0
			}
		}
	}
	if fault == FaultDeclarationConflict {
		for index := range specs {
			if specs[index].Name == "Vendor Updater" {
				specs[index].TextStartOverride = domain.StartDisabled
			}
		}
	}
	binaryPath := filepath.Join(dir, "policy.bin")
	jsonPath := filepath.Join(dir, "services.jsonl")
	textPath := filepath.Join(dir, "legacy.txt")
	version := 2
	if target == "v1" {
		version = 1
	}
	if err := writeBinary(binaryPath, version, specs); err != nil {
		return nil, err
	}
	if err := writeJSONLines(jsonPath, specs); err != nil {
		return nil, err
	}
	if err := writeTextBlocks(textPath, specs); err != nil {
		return nil, err
	}
	if fault == FaultUnknownVersion {
		if err := corruptBinaryVersion(binaryPath, 99); err != nil {
			return nil, err
		}
	}
	if fault == FaultTextPrefixOOB {
		if err := corruptLastTextLength(textPath); err != nil {
			return nil, err
		}
	}
	return []source.Input{
		{ID: "binary-policy", Kind: domain.SourceBinaryPolicy, Path: binaryPath},
		{ID: "json-manifest", Kind: domain.SourceJSONManifest, Path: jsonPath},
		{ID: "legacy-text", Kind: domain.SourceLegacyText, Path: textPath},
	}, nil
}

func FormatVersions(targetFormat string) map[string]string {
	binaryVersion := "policy-v2"
	if strings.EqualFold(targetFormat, "v1") {
		binaryVersion = "policy-v1"
	}
	return map[string]string{
		string(domain.SourceBinaryPolicy): binaryVersion,
		string(domain.SourceJSONManifest): "jsonl-v1",
		string(domain.SourceLegacyText):   "legacy-text-v1",
	}
}

func baseSpecs() []serviceSpec {
	return []serviceSpec{
		{
			Name:        "Spooler",
			DisplayName: "Print Spooler",
			StartType:   domain.StartAutomatic,
			BinaryPath:  "C:\\Windows\\System32\\spoolsv.exe",
			JSONPath:    "C:\\Windows\\System32\\spoolsv.exe",
			TextPath:    "c:/WINDOWS//System32/spoolsv.exe",
			AccountRaw:  "LocalSystem",
			AccountType: "LocalSystem",
			AccountEnum: 1,
			Logons:      []domain.LogonCapability{domain.LogonService, domain.LogonInteractive},
		},
		{
			Name:        "Vendor Updater",
			DisplayName: "Vendor Update Agent",
			StartType:   domain.StartAutomatic,
			BinaryPath:  "C:\\Program Files\\Vendor\\updater.exe -k run",
			JSONPath:    "\"C:\\Program Files\\Vendor\\updater.exe\" -k run",
			TextPath:    "C:/Program Files/Vendor//updater.exe -k run",
			AccountRaw:  "LocalSystem",
			AccountType: "LocalSystem",
			AccountEnum: 1,
			Logons:      []domain.LogonCapability{domain.LogonService, domain.LogonInteractive},
		},
		{
			Name:         "Writable Agent",
			DisplayName:  "Writable Directory Agent",
			StartType:    domain.StartAutomatic,
			BinaryPath:   "C:\\ProgramData\\Vendor Tools\\agent.exe --service",
			JSONPath:     "\"C:\\ProgramData\\Vendor Tools\\agent.exe\" --service",
			TextPath:     "C:/PROGRAMDATA/Vendor Tools//agent.exe --service",
			AccountRaw:   "LocalSystem",
			AccountType:  "LocalSystem",
			AccountEnum:  1,
			Logons:       []domain.LogonCapability{domain.LogonService, domain.LogonInteractive},
			WritableHint: true,
		},
		{
			Name:        "Network Relay",
			DisplayName: "Network Relay Service",
			StartType:   domain.StartManual,
			BinaryPath:  "C:\\Windows\\System32\\networkrelay.exe",
			JSONPath:    "C:\\Windows\\System32\\networkrelay.exe",
			TextPath:    "C:/Windows/System32/networkrelay.exe",
			AccountRaw:  "NetworkService",
			AccountType: "NetworkService",
			AccountEnum: 2,
			Logons:      []domain.LogonCapability{domain.LogonService, domain.LogonNetwork},
		},
		{
			Name:        "Custom Worker",
			DisplayName: "Domain Custom Worker",
			StartType:   domain.StartAutomatic,
			BinaryPath:  "C:\\Program Files\\Worker\\worker.exe",
			JSONPath:    "C:\\Program Files\\Worker\\worker.exe",
			TextPath:    "C:/Program Files/Worker//worker.exe",
			AccountRaw:  "DOMAIN\\worker",
			AccountType: "Custom",
			AccountEnum: 4,
			Logons:      []domain.LogonCapability{domain.LogonBatch, domain.LogonService},
		},
		{
			Name:        "Local Helper",
			DisplayName: "Local Helper Service",
			StartType:   domain.StartAutomatic,
			BinaryPath:  "C:\\Windows\\System32\\localhelper.exe",
			JSONPath:    "C:\\Windows\\System32\\localhelper.exe",
			TextPath:    "C:/Windows//System32/localhelper.exe",
			AccountRaw:  "LocalService",
			AccountType: "LocalService",
			AccountEnum: 3,
			Logons:      []domain.LogonCapability{domain.LogonService},
		},
		{
			Name:        "Quoted Tool Service",
			DisplayName: "Quoted Tool Service",
			StartType:   domain.StartManual,
			BinaryPath:  "\"C:\\Program Files\\Quoted Tool\\quoted.exe\" -mode quiet",
			JSONPath:    "\"C:\\Program Files\\Quoted Tool\\quoted.exe\" -mode quiet",
			TextPath:    "\"C:/Program Files/Quoted Tool//quoted.exe\" -mode quiet",
			AccountRaw:  "LocalSystem",
			AccountType: "LocalSystem",
			AccountEnum: 1,
			Logons:      []domain.LogonCapability{domain.LogonService, domain.LogonInteractive},
		},
	}
}

func writeBinary(path string, version int, specs []serviceSpec) error {
	buffer := bytes.NewBuffer(nil)
	buffer.Write([]byte{'G', 'S', 'B', 'S', 'P', 'O', 'L', 0})
	writeU16(buffer, uint16(version))
	writeU32(buffer, uint32(len(specs)))
	for _, spec := range specs {
		writeString16(buffer, spec.Name)
		writeString16(buffer, spec.DisplayName)
		buffer.WriteByte(startCode(spec.StartType))
		if version == 2 {
			buffer.WriteByte(spec.AccountEnum)
			writeString16(buffer, spec.AccountRaw)
			writeU16(buffer, logonMask(spec.Logons))
			var flags byte
			if spec.WritableHint {
				flags |= 1
			}
			buffer.WriteByte(flags)
		}
		writeString16(buffer, spec.BinaryPath)
	}
	return os.WriteFile(path, buffer.Bytes(), 0o644)
}

func writeJSONLines(path string, specs []serviceSpec) error {
	buffer := bytes.NewBuffer(nil)
	for _, spec := range specs {
		record := struct {
			ServiceName       string   `json:"service_name"`
			DisplayName       string   `json:"display_name"`
			StartType         string   `json:"start_type"`
			ImagePath         string   `json:"image_path"`
			AccountType       string   `json:"account_type"`
			Account           string   `json:"account"`
			LogonCapabilities []string `json:"logon_capabilities"`
			PathWritable      bool     `json:"path_writable"`
		}{
			ServiceName:       spec.Name,
			DisplayName:       spec.DisplayName,
			StartType:         jsonStartType(spec.StartType),
			ImagePath:         spec.JSONPath,
			AccountType:       spec.AccountType,
			Account:           spec.AccountRaw,
			LogonCapabilities: capabilityStrings(spec.Logons),
			PathWritable:      spec.WritableHint,
		}
		line, err := json.Marshal(record)
		if err != nil {
			return err
		}
		buffer.Write(line)
		buffer.WriteByte('\n')
	}
	return os.WriteFile(path, buffer.Bytes(), 0o644)
}

func writeTextBlocks(path string, specs []serviceSpec) error {
	buffer := bytes.NewBuffer(nil)
	for _, spec := range specs {
		writable := "0"
		if spec.WritableHint {
			writable = "1"
		}
		textStart := spec.StartType
		if spec.TextStartOverride != "" {
			textStart = spec.TextStartOverride
		}
		payload := strings.Join([]string{
			spec.Name,
			spec.DisplayName,
			textStartType(textStart),
			spec.TextPath,
			spec.AccountRaw,
			strings.Join(capabilityStrings(spec.Logons), ","),
			writable,
		}, "|")
		writeU32(buffer, uint32(len(payload)))
		buffer.WriteString(payload)
	}
	return os.WriteFile(path, buffer.Bytes(), 0o644)
}

func corruptBinaryVersion(path string, version uint16) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < 10 {
		return fmt.Errorf("binary sample too short")
	}
	binary.BigEndian.PutUint16(data[8:10], version)
	return os.WriteFile(path, data, 0o644)
}

func corruptLastTextLength(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	offset := 0
	lastPrefix := -1
	for offset+4 <= len(data) {
		length := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		next := offset + 4 + length
		if next > len(data) || length <= 0 {
			return fmt.Errorf("sample text block is malformed before injection")
		}
		lastPrefix = offset
		offset = next
	}
	if lastPrefix < 0 {
		return fmt.Errorf("sample text file has no records")
	}
	binary.BigEndian.PutUint32(data[lastPrefix:lastPrefix+4], uint32(len(data)-lastPrefix+64))
	return os.WriteFile(path, data, 0o644)
}

func writeU16(buffer *bytes.Buffer, value uint16) {
	var data [2]byte
	binary.BigEndian.PutUint16(data[:], value)
	buffer.Write(data[:])
}

func writeU32(buffer *bytes.Buffer, value uint32) {
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], value)
	buffer.Write(data[:])
}

func writeString16(buffer *bytes.Buffer, value string) {
	writeU16(buffer, uint16(len(value)))
	buffer.WriteString(value)
}

func startCode(start domain.StartType) byte {
	switch start {
	case domain.StartBoot:
		return 1
	case domain.StartSystem:
		return 2
	case domain.StartAutomatic:
		return 3
	case domain.StartManual:
		return 4
	case domain.StartDisabled:
		return 5
	default:
		return 0
	}
}

func jsonStartType(start domain.StartType) string {
	switch start {
	case domain.StartAutomatic:
		return "auto"
	case domain.StartManual:
		return "demand"
	case domain.StartDisabled:
		return "disabled"
	case domain.StartBoot:
		return "boot"
	case domain.StartSystem:
		return "system"
	default:
		return "unknown"
	}
}

func textStartType(start domain.StartType) string {
	return strings.ToUpper(string(start))
}

func logonMask(capabilities []domain.LogonCapability) uint16 {
	var mask uint16
	for _, capability := range capabilities {
		switch capability {
		case domain.LogonInteractive:
			mask |= 1
		case domain.LogonNetwork:
			mask |= 2
		case domain.LogonService:
			mask |= 4
		case domain.LogonBatch:
			mask |= 8
		case domain.LogonDenied:
			mask |= 16
		}
	}
	return mask
}

func capabilityStrings(capabilities []domain.LogonCapability) []string {
	sorted := domain.SortLogonCapabilities(capabilities)
	out := make([]string, 0, len(sorted))
	for _, capability := range sorted {
		out = append(out, string(capability))
	}
	return out
}

func validFault(value string) bool {
	for _, candidate := range ValidFaults {
		if value == candidate {
			return true
		}
	}
	return false
}
