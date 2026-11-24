package formats

import (
	"encoding/binary"
	"fmt"

	"supplyaudit/internal/model"
)

const binaryMagic = "SADBIN01"

type byteCursor struct {
	data []byte
	pos  int
}

func (c *byteCursor) remaining() int { return len(c.data) - c.pos }

func (c *byteCursor) readUint16(offsetBase int64) (uint16, error) {
	if c.remaining() < 2 {
		return 0, model.NewCodeError(model.CodeTruncated, "binary manifest ended while reading uint16", offsetBase+int64(c.pos), int64(len(c.data)))
	}
	value := binary.LittleEndian.Uint16(c.data[c.pos : c.pos+2])
	c.pos += 2
	return value, nil
}

func (c *byteCursor) readUint32(offsetBase int64) (uint32, error) {
	if c.remaining() < 4 {
		return 0, model.NewCodeError(model.CodeTruncated, "binary manifest ended while reading uint32", offsetBase+int64(c.pos), int64(len(c.data)))
	}
	value := binary.LittleEndian.Uint32(c.data[c.pos : c.pos+4])
	c.pos += 4
	return value, nil
}

func (c *byteCursor) readBytes(length int, field string, offsetBase int64) ([]byte, error) {
	if length < 0 || c.remaining() < length {
		return nil, model.NewCodeError(model.CodeTruncated, fmt.Sprintf("binary manifest ended while reading %s (%d bytes requested)", field, length), offsetBase+int64(c.pos), int64(len(c.data)))
	}
	value := c.data[c.pos : c.pos+length]
	c.pos += length
	return value, nil
}

func parseBinary(data []byte, result *model.SourceResult, options ParseOptions) {
	if len(data) < len(binaryMagic) {
		result.AddIssue(model.CodeTruncated, "binary magic is incomplete", 0, int64(len(data)), 0)
		return
	}
	if string(data[:len(binaryMagic)]) != binaryMagic {
		result.AddIssue(model.CodeFormatVersion, "unknown binary magic", 0, int64(len(data)), 0)
		return
	}

	cursor := &byteCursor{data: data, pos: len(binaryMagic)}
	versionValue, err := cursor.readUint16(0)
	if err != nil {
		issueFromError(result, err, 0, int64(len(data)), 0)
		return
	}
	version := int(versionValue)
	if version != 1 && version != 2 {
		result.AddIssue(model.CodeFormatVersion, fmt.Sprintf("unsupported binary format version %d", version), int64(len(binaryMagic)), int64(len(data)), 0)
		return
	}
	if version > options.MaxBinaryVersion() {
		result.AddIssue(model.CodeFormatVersion, fmt.Sprintf("binary format v%d is newer than configured target v%d", version, options.MaxBinaryVersion()), int64(len(binaryMagic)), int64(len(data)), 0)
		return
	}
	result.FormatVersion = fmt.Sprintf("%d", version)

	count, err := cursor.readUint32(0)
	if err != nil {
		issueFromError(result, err, 0, int64(len(data)), 0)
		return
	}

	globalOrigin := result.Spec.ID
	if version == 1 {
		sourceLength, err := cursor.readUint16(0)
		if err != nil {
			issueFromError(result, err, 0, int64(len(data)), 0)
			return
		}
		sourceBytes, err := cursor.readBytes(int(sourceLength), "v1 source repository", 0)
		if err != nil {
			issueFromError(result, err, 0, int64(len(data)), 0)
			return
		}
		if len(sourceBytes) > 0 {
			globalOrigin = string(sourceBytes)
		}
	}

	for index := 0; index < int(count); index++ {
		recordOffset := int64(cursor.pos)
		nameLength, err := cursor.readUint16(recordOffset)
		if err != nil {
			issueFromError(result, err, recordOffset, int64(len(data)), index)
			return
		}
		versionLength, err := cursor.readUint16(recordOffset)
		if err != nil {
			issueFromError(result, err, recordOffset, int64(len(data)), index)
			return
		}
		digestLength, err := cursor.readUint16(recordOffset)
		if err != nil {
			issueFromError(result, err, recordOffset, int64(len(data)), index)
			return
		}
		var repositoryLength, signatureLength uint16
		if version == 2 {
			repositoryLength, err = cursor.readUint16(recordOffset)
			if err != nil {
				issueFromError(result, err, recordOffset, int64(len(data)), index)
				return
			}
			signatureLength, err = cursor.readUint16(recordOffset)
			if err != nil {
				issueFromError(result, err, recordOffset, int64(len(data)), index)
				return
			}
		}

		nameBytes, err := cursor.readBytes(int(nameLength), "component name", recordOffset)
		if err != nil {
			issueFromError(result, err, recordOffset, int64(len(data)), index)
			return
		}
		versionBytes, err := cursor.readBytes(int(versionLength), "component version", recordOffset)
		if err != nil {
			issueFromError(result, err, recordOffset, int64(len(data)), index)
			return
		}
		digestBytes, err := cursor.readBytes(int(digestLength), "integrity digest", recordOffset)
		if err != nil {
			issueFromError(result, err, recordOffset, int64(len(data)), index)
			return
		}
		origin := globalOrigin
		signature := ""
		if version == 2 {
			repositoryBytes, err := cursor.readBytes(int(repositoryLength), "source repository", recordOffset)
			if err != nil {
				issueFromError(result, err, recordOffset, int64(len(data)), index)
				return
			}
			signatureBytes, err := cursor.readBytes(int(signatureLength), "signature digest", recordOffset)
			if err != nil {
				issueFromError(result, err, recordOffset, int64(len(data)), index)
				return
			}
			if len(repositoryBytes) > 0 {
				origin = string(repositoryBytes)
			}
			signature = string(signatureBytes)
		}

		decl, err := buildDeclaration(result.Spec, recordOffset, index, string(nameBytes), string(versionBytes), string(digestBytes), origin, signature)
		if err != nil {
			issueFromError(result, err, recordOffset, int64(len(data)), index)
			continue
		}
		if !acceptDeclaration(result, decl, options) {
			return
		}
	}
}
