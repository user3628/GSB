package source

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/privilege"
	"svcworkbench/internal/winpath"
)

var binaryMagic = []byte{'G', 'S', 'B', 'S', 'P', 'O', 'L', 0}

type Input struct {
	ID   string
	Kind domain.SourceKind
	Path string
}

type Options struct {
	ServiceBudget int
}

type recordFields struct {
	ServiceName       string
	DisplayName       string
	StartType         domain.StartType
	StartTypeValid    bool
	ImagePath         string
	AccountRaw        string
	AccountKind       domain.AccountKind
	AccountPresent    bool
	LogonCapabilities []domain.LogonCapability
	LogonPresent      bool
	DeclaredWritable  bool
}

func ParseFile(ctx context.Context, input Input, options Options) domain.SourceResult {
	result := domain.SourceResult{SourceID: input.ID, Kind: input.Kind, FormatVersion: "unknown"}
	data, err := os.ReadFile(input.Path)
	if err != nil {
		detail := "source read failed"
		if os.IsNotExist(err) {
			detail = "source file not found"
		}
		result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, 0, 0, "", detail))
		return result
	}
	sum := sha256.Sum256(data)
	result.Fingerprint = hex.EncodeToString(sum[:])
	result.Bytes = int64(len(data))
	if err := ctx.Err(); err != nil {
		result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, 0, int64(len(data)), "", err.Error()))
		return result
	}
	switch input.Kind {
	case domain.SourceBinaryPolicy:
		parseBinary(input, data, options.ServiceBudget, &result)
	case domain.SourceJSONManifest:
		parseJSONLines(input, data, options.ServiceBudget, &result)
	case domain.SourceLegacyText:
		parseLengthPrefixedText(input, data, options.ServiceBudget, &result)
	default:
		result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, 0, int64(len(data)), "", "unknown source kind"))
	}
	result.UsableDeclarations = 0
	for _, declaration := range result.Declarations {
		if declaration.Usable {
			result.UsableDeclarations++
		}
	}
	return result
}

func parseBinary(input Input, data []byte, budget int, result *domain.SourceResult) {
	minimumHeader := len(binaryMagic) + 2 + 4
	if len(data) < minimumHeader {
		result.Issues = append(result.Issues, sourceIssue(domain.CodeFormatVersion, input, 0, int64(len(data)), "", "truncated binary policy header"))
		result.SkippableRanges = append(result.SkippableRanges, domain.ByteRange{Offset: 0, End: int64(len(data))})
		return
	}
	if !bytes.Equal(data[:len(binaryMagic)], binaryMagic) {
		result.Issues = append(result.Issues, sourceIssue(domain.CodeFormatVersion, input, 0, int64(len(data)), "", "invalid binary policy magic"))
		result.SkippableRanges = append(result.SkippableRanges, domain.ByteRange{Offset: 0, End: int64(len(data))})
		return
	}
	version := binary.BigEndian.Uint16(data[8:10])
	result.FormatVersion = fmt.Sprintf("policy-v%d", version)
	if version != 1 && version != 2 {
		result.Issues = append(result.Issues, sourceIssue(domain.CodeFormatVersion, input, 0, int64(len(data)), "", fmt.Sprintf("unknown policy version %d", version)))
		result.SkippableRanges = append(result.SkippableRanges, domain.ByteRange{Offset: 0, End: int64(len(data))})
		return
	}
	count := binary.BigEndian.Uint32(data[10:14])
	offset := 14
	for index := uint32(0); index < count; index++ {
		if offset >= len(data) {
			result.Partial = true
			result.Issues = append(result.Issues, sourceIssue(domain.CodeFormatVersion, input, int64(offset), int64(len(data)), "", "missing declared binary record"))
			result.SkippableRanges = append(result.SkippableRanges, domain.ByteRange{Offset: int64(offset), End: int64(len(data))})
			return
		}
		start := offset
		result.RecordsSeen++
		if budget > 0 && result.RecordsSeen > budget {
			result.Partial = true
			result.Issues = append(result.Issues, sourceIssue(domain.CodeServiceBudget, input, int64(start), int64(len(data)), "", fmt.Sprintf("service budget %d exceeded", budget)))
			return
		}
		fields, next, ok := parseBinaryRecord(data, offset, version)
		if !ok {
			result.Partial = true
			result.Issues = append(result.Issues, sourceIssue(domain.CodeFormatVersion, input, int64(start), int64(len(data)), "", "truncated or invalid binary record"))
			result.SkippableRanges = append(result.SkippableRanges, domain.ByteRange{Offset: int64(start), End: int64(len(data))})
			return
		}
		finalizeRecord(input, result, fields, int(start), next, int(index))
		offset = next
	}
}

func parseBinaryRecord(data []byte, offset int, version uint16) (recordFields, int, bool) {
	fields := recordFields{StartTypeValid: true}
	position := offset
	readU16 := func() (uint16, bool) {
		if position+2 > len(data) {
			return 0, false
		}
		value := binary.BigEndian.Uint16(data[position : position+2])
		position += 2
		return value, true
	}
	readBytes := func(length int) (string, bool) {
		if length < 0 || position+length > len(data) {
			return "", false
		}
		value := string(data[position : position+length])
		position += length
		return value, true
	}

	nameLength, ok := readU16()
	if !ok {
		return fields, offset, false
	}
	name, ok := readBytes(int(nameLength))
	if !ok {
		return fields, offset, false
	}
	displayLength, ok := readU16()
	if !ok {
		return fields, offset, false
	}
	display, ok := readBytes(int(displayLength))
	if !ok {
		return fields, offset, false
	}
	if position >= len(data) {
		return fields, offset, false
	}
	startCode := data[position]
	position++
	start, startOK := startTypeFromCode(startCode)
	fields.StartType = start
	fields.StartTypeValid = startOK
	if version == 2 {
		if position >= len(data) {
			return fields, offset, false
		}
		accountType := data[position]
		position++
		accountLength, ok := readU16()
		if !ok {
			return fields, offset, false
		}
		account, ok := readBytes(int(accountLength))
		if !ok {
			return fields, offset, false
		}
		if position+2 > len(data) {
			return fields, offset, false
		}
		logonMask := binary.BigEndian.Uint16(data[position : position+2])
		position += 2
		capabilities, validMask := domain.LogonCapabilitiesFromMask(logonMask)
		if !validMask {
			return fields, offset, false
		}
		if position >= len(data) {
			return fields, offset, false
		}
		pathFlags := data[position]
		position++
		pathLength, ok := readU16()
		if !ok {
			return fields, offset, false
		}
		imagePath, ok := readBytes(int(pathLength))
		if !ok {
			return fields, offset, false
		}
		fields.ServiceName = name
		fields.DisplayName = display
		fields.AccountRaw = account
		fields.AccountKind = domain.AccountKindFromEnum(accountType)
		fields.AccountPresent = true
		fields.LogonCapabilities = capabilities
		fields.LogonPresent = true
		fields.DeclaredWritable = pathFlags&1 != 0
		fields.ImagePath = imagePath
		return fields, position, true
	}
	pathLength, ok := readU16()
	if !ok {
		return fields, offset, false
	}
	imagePath, ok := readBytes(int(pathLength))
	if !ok {
		return fields, offset, false
	}
	fields.ServiceName = name
	fields.DisplayName = display
	fields.ImagePath = imagePath
	fields.AccountRaw = "LocalSystem"
	fields.AccountKind = domain.AccountLocalSystem
	fields.AccountPresent = false
	fields.LogonCapabilities = []domain.LogonCapability{domain.LogonService}
	fields.LogonPresent = false
	return fields, position, true
}

func startTypeFromCode(value byte) (domain.StartType, bool) {
	switch value {
	case 1:
		return domain.StartBoot, true
	case 2:
		return domain.StartSystem, true
	case 3:
		return domain.StartAutomatic, true
	case 4:
		return domain.StartManual, true
	case 5:
		return domain.StartDisabled, true
	default:
		return domain.StartUnknown, false
	}
}

type jsonRecord struct {
	ServiceName       string          `json:"service_name"`
	DisplayName       string          `json:"display_name"`
	StartType         string          `json:"start_type"`
	ImagePath         string          `json:"image_path"`
	AccountType       *string         `json:"account_type"`
	Account           string          `json:"account"`
	LogonCapabilities json.RawMessage `json:"logon_capabilities"`
	PathWritable      bool            `json:"path_writable"`
}

func parseJSONLines(input Input, data []byte, budget int, result *domain.SourceResult) {
	result.FormatVersion = "jsonl-v1"
	offset := 0
	recordIndex := 0
	for offset < len(data) {
		lineStart := offset
		lineEnd := bytes.IndexByte(data[offset:], '\n')
		if lineEnd < 0 {
			lineEnd = len(data)
		} else {
			lineEnd += offset
		}
		next := lineEnd
		if next < len(data) && data[next] == '\n' {
			next++
		}
		line := bytes.TrimSpace(data[lineStart:lineEnd])
		if len(line) == 0 {
			offset = next
			continue
		}
		result.RecordsSeen++
		if budget > 0 && result.RecordsSeen > budget {
			result.Partial = true
			result.Issues = append(result.Issues, sourceIssue(domain.CodeServiceBudget, input, int64(lineStart), int64(next), "", fmt.Sprintf("service budget %d exceeded", budget)))
			return
		}
		var raw jsonRecord
		decoder := json.NewDecoder(bytes.NewReader(line))
		if err := decoder.Decode(&raw); err != nil {
			result.RecordsSkipped++
			result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, int64(lineStart), int64(lineEnd), "", "invalid JSON record: "+err.Error()))
			offset = next
			continue
		}
		start, startOK := domain.NormalizeStartType(raw.StartType)
		accountKind := domain.AccountUnknown
		if raw.AccountType != nil {
			accountKind = accountKindFromText(*raw.AccountType)
		} else {
			accountKind = domain.ClassifyAccount(raw.Account)
		}
		capabilities, capsOK := parseJSONCapabilities(raw.LogonCapabilities)
		fields := recordFields{
			ServiceName:       raw.ServiceName,
			DisplayName:       raw.DisplayName,
			StartType:         start,
			StartTypeValid:    startOK,
			ImagePath:         raw.ImagePath,
			AccountRaw:        raw.Account,
			AccountKind:       accountKind,
			AccountPresent:    true,
			LogonCapabilities: capabilities,
			LogonPresent:      len(raw.LogonCapabilities) > 0 && capsOK,
			DeclaredWritable:  raw.PathWritable,
		}
		if len(raw.LogonCapabilities) > 0 && !capsOK {
			result.RecordsSkipped++
			result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, int64(lineStart), int64(lineEnd), raw.ServiceName, "invalid logon capability"))
			offset = next
			continue
		}
		finalizeRecord(input, result, fields, lineStart, lineEnd, recordIndex)
		recordIndex++
		offset = next
	}
}

func parseJSONCapabilities(raw json.RawMessage) ([]domain.LogonCapability, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, true
	}
	var values []string
	if raw[0] == '[' {
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, false
		}
	} else if raw[0] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, false
		}
		values = strings.Split(value, ",")
	} else {
		return nil, false
	}
	capabilities := make([]domain.LogonCapability, 0, len(values))
	for _, value := range values {
		capability, ok := domain.NormalizeLogonCapability(value)
		if !ok {
			return nil, false
		}
		capabilities = append(capabilities, capability)
	}
	return domain.SortLogonCapabilities(capabilities), true
}

func accountKindFromText(value string) domain.AccountKind {
	switch domain.NormalizeWindowsToken(value) {
	case "localsystem", "local system", "system":
		return domain.AccountLocalSystem
	case "networkservice", "network service":
		return domain.AccountNetworkService
	case "localservice", "local service":
		return domain.AccountLocalService
	case "custom":
		return domain.AccountCustom
	default:
		return domain.AccountUnknown
	}
}

func parseLengthPrefixedText(input Input, data []byte, budget int, result *domain.SourceResult) {
	result.FormatVersion = "legacy-text-v1"
	offset := 0
	recordIndex := 0
	for offset < len(data) {
		start := offset
		if len(data)-offset < 4 {
			result.Partial = true
			result.Issues = append(result.Issues, sourceIssue(domain.CodeFormatVersion, input, int64(start), int64(len(data)), "", "truncated text block length prefix"))
			result.SkippableRanges = append(result.SkippableRanges, domain.ByteRange{Offset: int64(start), End: int64(len(data))})
			return
		}
		length := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		contentStart := offset + 4
		end := contentStart + length
		if length == 0 || end > len(data) {
			result.Partial = true
			result.Issues = append(result.Issues, sourceIssue(domain.CodeFormatVersion, input, int64(start), int64(len(data)), "", fmt.Sprintf("text block length %d exceeds remaining bytes %d", length, len(data)-contentStart)))
			result.SkippableRanges = append(result.SkippableRanges, domain.ByteRange{Offset: int64(start), End: int64(len(data))})
			return
		}
		result.RecordsSeen++
		if budget > 0 && result.RecordsSeen > budget {
			result.Partial = true
			result.Issues = append(result.Issues, sourceIssue(domain.CodeServiceBudget, input, int64(start), int64(len(data)), "", fmt.Sprintf("service budget %d exceeded", budget)))
			return
		}
		payload := string(data[contentStart:end])
		parts := strings.Split(payload, "|")
		if len(parts) < 6 {
			result.RecordsSkipped++
			result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, int64(start), int64(end), "", "text record has too few fields"))
			offset = end
			continue
		}
		startType, startOK := domain.NormalizeStartType(parts[2])
		accountKind := domain.ClassifyAccount(parts[4])
		capabilities := make([]domain.LogonCapability, 0, 4)
		capabilitiesOK := true
		if strings.TrimSpace(parts[5]) != "" {
			for _, value := range strings.Split(parts[5], ",") {
				capability, ok := domain.NormalizeLogonCapability(value)
				if !ok {
					capabilitiesOK = false
					break
				}
				capabilities = append(capabilities, capability)
			}
		}
		if !capabilitiesOK {
			result.RecordsSkipped++
			result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, int64(start), int64(end), parts[0], "invalid logon capability in text record"))
			offset = end
			continue
		}
		declaredWritable := false
		if len(parts) >= 7 {
			declaredWritable = strings.TrimSpace(parts[6]) == "1" || strings.EqualFold(strings.TrimSpace(parts[6]), "true")
		}
		fields := recordFields{
			ServiceName:       parts[0],
			DisplayName:       parts[1],
			StartType:         startType,
			StartTypeValid:    startOK,
			ImagePath:         parts[3],
			AccountRaw:        parts[4],
			AccountKind:       accountKind,
			AccountPresent:    true,
			LogonCapabilities: domain.SortLogonCapabilities(capabilities),
			LogonPresent:      true,
			DeclaredWritable:  declaredWritable,
		}
		finalizeRecord(input, result, fields, start, end, recordIndex)
		recordIndex++
		offset = end
	}
}

func finalizeRecord(input Input, result *domain.SourceResult, fields recordFields, start, end, recordIndex int) {
	serviceName := strings.TrimSpace(fields.ServiceName)
	if serviceName == "" {
		result.RecordsSkipped++
		result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, int64(start), int64(end), "", "empty service name"))
		return
	}
	ref := domain.SourceRef{
		SourceID:      input.ID,
		Kind:          input.Kind,
		FormatVersion: result.FormatVersion,
		Offset:        int64(start),
		End:           int64(end),
		RecordIndex:   recordIndex,
		Fingerprint:   result.Fingerprint,
	}
	declaration := domain.Declaration{
		ServiceName:           serviceName,
		NormalizedServiceName: domain.NormalizeServiceName(serviceName),
		DisplayName:           fields.DisplayName,
		StartType:             fields.StartType,
		ImagePath:             fields.ImagePath,
		AccountRaw:            fields.AccountRaw,
		AccountKind:           fields.AccountKind,
		AccountPresent:        fields.AccountPresent,
		LogonCapabilities:     domain.SortLogonCapabilities(fields.LogonCapabilities),
		LogonPresent:          fields.LogonPresent,
		Path:                  winpath.Assess(fields.ImagePath, fields.DeclaredWritable),
		DeclaredWritable:      fields.DeclaredWritable,
		Usable:                true,
		Source:                ref,
	}
	if declaration.AccountKind == domain.AccountCustom && strings.TrimSpace(declaration.AccountRaw) == "" {
		declaration.AccountKind = domain.AccountUnknown
	}
	codes := make([]domain.Code, 0, 3)
	if !fields.StartTypeValid {
		declaration.Usable = false
		codes = append(codes, domain.CodeSourceMalformed)
		result.Issues = append(result.Issues, sourceIssue(domain.CodeSourceMalformed, input, int64(start), int64(end), serviceName, "unknown service start type"))
	}
	if declaration.Path.Malformed {
		declaration.Usable = false
		codes = append(codes, domain.CodePathMalformed)
		result.Issues = append(result.Issues, sourceIssue(domain.CodePathMalformed, input, int64(start), int64(end), serviceName, declaration.Path.MalformedReason))
	}
	if fields.AccountPresent && fields.AccountKind == domain.AccountUnknown {
		declaration.Usable = false
		codes = append(codes, domain.CodeAccountUnknown)
		result.Issues = append(result.Issues, sourceIssue(domain.CodeAccountUnknown, input, int64(start), int64(end), serviceName, "unrecognized service account: "+fields.AccountRaw))
	}
	declaration.Privilege = privilege.Build(declaration.AccountKind, declaration.StartType, declaration.LogonCapabilities)
	codeStrings := make([]string, 0, len(codes))
	for _, code := range codes {
		codeStrings = append(codeStrings, string(code))
	}
	declaration.Codes = domain.SortedStrings(codeStrings)
	result.Declarations = append(result.Declarations, declaration)
	result.RecordsAccepted++
}

func sourceIssue(code domain.Code, input Input, start, end int64, service, detail string) domain.Issue {
	return domain.Issue{
		Code:        code,
		SourceID:    input.ID,
		SourceKind:  input.Kind,
		Offset:      start,
		End:         end,
		ServiceName: service,
		Detail:      detail,
	}
}
