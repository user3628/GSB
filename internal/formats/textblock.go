package formats

import (
	"encoding/binary"
	"fmt"
	"strings"

	"supplyaudit/internal/model"
)

const textMagic = "SADTXT01"

func parseTextBlock(data []byte, result *model.SourceResult, options ParseOptions) {
	if len(data) < len(textMagic) {
		result.AddIssue(model.CodeTruncated, "textblock magic is incomplete", 0, int64(len(data)), 0)
		return
	}
	if string(data[:len(textMagic)]) != textMagic {
		result.AddIssue(model.CodeFormatVersion, "unknown textblock magic", 0, int64(len(data)), 0)
		return
	}
	result.FormatVersion = "1"

	position := len(textMagic)
	recordIndex := 0
	for position < len(data) {
		blockOffset := int64(position)
		if len(data)-position < 4 {
			result.AddIssue(model.CodeTruncated, "textblock ended while reading 4-byte length prefix", blockOffset, int64(len(data)), recordIndex)
			return
		}
		length := int(binary.BigEndian.Uint32(data[position : position+4]))
		position += 4
		if length > len(data)-position {
			result.AddIssue(model.CodeTruncated, fmt.Sprintf("textblock prefix requests %d bytes but only %d remain", length, len(data)-position), blockOffset, int64(len(data)), recordIndex)
			return
		}
		payload := string(data[position : position+length])
		position += length
		if length == 0 {
			result.AddIssue(model.CodeRecordInvalid, "textblock record is empty", blockOffset, int64(len(data)), recordIndex)
			recordIndex++
			continue
		}
		fields := strings.Split(payload, "|")
		if len(fields) != 4 {
			result.AddIssue(model.CodeRecordInvalid, "textblock record must contain name|version|digest|repository", blockOffset, int64(len(data)), recordIndex)
			recordIndex++
			continue
		}
		decl, err := buildDeclaration(result.Spec, blockOffset, recordIndex, fields[0], fields[1], fields[2], fields[3], "")
		if err != nil {
			issueFromError(result, err, blockOffset, int64(len(data)), recordIndex)
			recordIndex++
			continue
		}
		if !acceptDeclaration(result, decl, options) {
			return
		}
		recordIndex++
	}
}
