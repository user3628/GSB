package formats

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"supplyaudit/internal/model"
	"supplyaudit/internal/normalize"
)

// ParseOptions controls the binary adapter compatibility ceiling. v2 accepts
// both v1 and v2 collector manifests so historical inputs remain readable.
type ParseOptions struct {
	BinaryFormatVersion string
	MaxComponents       int
}

func (o ParseOptions) MaxBinaryVersion() int {
	if strings.EqualFold(strings.TrimSpace(o.BinaryFormatVersion), "v1") || o.BinaryFormatVersion == "1" {
		return 1
	}
	return 2
}

func ParseSource(spec model.SourceSpec, options ParseOptions) *model.SourceResult {
	result := &model.SourceResult{Spec: spec}
	data, err := os.ReadFile(spec.Path)
	if err != nil {
		result.AddIssue(model.CodeSourceIO, err.Error(), 0, 0, 0)
		result.Finalize()
		return result
	}
	result.BytesRead = int64(len(data))

	switch spec.Format {
	case model.FormatBinary:
		parseBinary(data, result, options)
	case model.FormatJSONL:
		parseJSONL(data, result, options)
	case model.FormatTextBlock:
		parseTextBlock(data, result, options)
	default:
		result.AddIssue(model.CodeFormatVersion, "unknown source format "+string(spec.Format), 0, result.BytesRead, 0)
	}
	result.Finalize()
	return result
}

// Detect identifies a sample format from its header, then falls back to the
// extension and JSON object shape.
func Detect(path string, data []byte) model.SourceFormat {
	if len(data) >= len(binaryMagic) && string(data[:len(binaryMagic)]) == binaryMagic {
		return model.FormatBinary
	}
	if len(data) >= len(textMagic) && string(data[:len(textMagic)]) == textMagic {
		return model.FormatTextBlock
	}
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".jsonl") || strings.HasSuffix(lower, ".ndjson") {
		return model.FormatJSONL
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		return model.FormatJSONL
	}
	return ""
}

func buildDeclaration(spec model.SourceSpec, offset int64, recordIndex int, rawName, rawVersion, rawDigest, origin, signature string) (model.Declaration, error) {
	decl := model.Declaration{
		SourceID:     spec.ID,
		SourcePath:   spec.ReportPath,
		SourceFormat: spec.Format,
		Origin:       origin,
		Offset:       offset,
		RecordIndex:  recordIndex,
		RawName:      rawName,
		RawDigest:    rawDigest,
		Signature:    signature,
		Codes:        []string{},
	}
	if decl.Origin == "" {
		decl.Origin = spec.ID
	}
	key, err := normalize.ComponentKey(rawName)
	if err != nil {
		return decl, err
	}
	decl.ComponentKey = key
	version, err := normalize.ParseVersion(rawVersion)
	if err != nil {
		return decl, err
	}
	decl.Version = version
	digest, err := normalize.ParseDigest(rawDigest)
	if err != nil {
		return decl, err
	}
	decl.Digest = digest
	return decl, nil
}

// acceptDeclaration enforces the per-session component budget before the next
// record contributes any result state. A budget hit terminates that source.
func acceptDeclaration(result *model.SourceResult, declaration model.Declaration, options ParseOptions) bool {
	if options.MaxComponents > 0 && len(result.Declarations) >= options.MaxComponents {
		result.BudgetStopped = true
		result.AddIssue(
			model.CodeComponentBudget,
			fmt.Sprintf("source exceeded component budget %d; retained %d parsed declarations", options.MaxComponents, len(result.Declarations)),
			declaration.Offset,
			result.BytesRead,
			declaration.RecordIndex,
		)
		return false
	}
	result.Declarations = append(result.Declarations, declaration)
	return true
}

func issueFromError(result *model.SourceResult, err error, offset, bytesRead int64, record int) bool {
	if codeErr, ok := err.(*model.CodeError); ok {
		result.AddIssue(codeErr.Code, codeErr.Message, offset, bytesRead, record)
		return codeErr.Code == model.CodeTruncated
	}
	result.AddIssue(model.CodeRecordInvalid, err.Error(), offset, bytesRead, record)
	return false
}
