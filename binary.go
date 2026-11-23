package dnsdetect

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
)

const binaryMagic = "DNSB"

type binaryCursor struct {
	data   []byte
	offset int64
	path   string
}

func (c *binaryCursor) take(n int) ([]byte, error) {
	if n < 0 || c.offset+int64(n) > int64(len(c.data)) {
		return nil, newBusinessError(CodeBinaryTruncated, c.path, c.offset, fmt.Sprintf("need %d bytes, have %d", n, int64(len(c.data))-c.offset), nil)
	}
	value := c.data[c.offset : c.offset+int64(n)]
	c.offset += int64(n)
	return value, nil
}

func (c *binaryCursor) u8() (byte, error) {
	v, err := c.take(1)
	if err != nil {
		return 0, err
	}
	return v[0], nil
}

func (c *binaryCursor) u16() (uint16, error) {
	v, err := c.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(v), nil
}

func (c *binaryCursor) u32() (uint32, error) {
	v, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(v), nil
}

func (c *binaryCursor) i64() (int64, error) {
	v, err := c.take(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(v)), nil
}

func (c *binaryCursor) lengthBytes() ([]byte, error) {
	n, err := c.u16()
	if err != nil {
		return nil, err
	}
	return c.take(int(n))
}

// parseBinary implements the probe's compact DNSB v1/v2 layout documented in
// FORMATS.md. All integer fields are little-endian; TTL is little-endian too.
func parseBinary(ctx context.Context, data []byte, builder *sessionBuilder) error {
	cursor := &binaryCursor{data: data, path: builder.session.Path}
	magic, err := cursor.take(4)
	if err != nil {
		return err
	}
	if string(magic) != binaryMagic {
		return newBusinessError(CodeBinaryMagic, cursor.path, 0, "expected DNSB magic", nil)
	}
	version, err := cursor.u16()
	if err != nil {
		return err
	}
	if version != 1 && version != 2 {
		return newBusinessError(CodeUnknownVersion, cursor.path, 4, fmt.Sprintf("unsupported binary format version %d", version), nil)
	}
	count, err := cursor.u32()
	if err != nil {
		return err
	}
	builder.setFormatVersion(int(version))
	if int64(count) > int64(builder.options.MaxEvents) {
		return newBusinessError(CodeEventBudget, cursor.path, cursor.offset, fmt.Sprintf("header declares %d events, budget is %d", count, builder.options.MaxEvents), nil)
	}
	var sourceAddress, resolverID string
	if version == 2 {
		value, readErr := cursor.lengthBytes()
		if readErr != nil {
			return readErr
		}
		sourceAddress = string(value)
		value, readErr = cursor.lengthBytes()
		if readErr != nil {
			return readErr
		}
		resolverID = string(value)
	}
	for index := uint32(0); index < count; index++ {
		recordOffset := cursor.offset
		relativeNS, readErr := cursor.i64()
		if readErr != nil {
			return readErr
		}
		txid, readErr := cursor.u32()
		if readErr != nil {
			return readErr
		}
		flags, readErr := cursor.u16()
		if readErr != nil {
			return readErr
		}
		qtype, readErr := cursor.u16()
		if readErr != nil {
			return readErr
		}
		roleByte, readErr := cursor.u8()
		if readErr != nil {
			return readErr
		}
		role, roleErr := roleFromByte(roleByte)
		if roleErr != nil {
			return newBusinessError(CodeBinaryField, cursor.path, recordOffset, roleErr.Error(), roleErr)
		}
		qnameBytes, readErr := cursor.lengthBytes()
		if readErr != nil {
			return readErr
		}
		question, questionKey, nameErr := NormalizeName(string(qnameBytes))
		if nameErr != nil {
			return wrapOffsetError(nameErr, cursor.path, recordOffset)
		}
		ticks, truncation, timeErr := TicksFromRelativeNS(builder.session.ObservationStartTicks, relativeNS)
		if timeErr != nil {
			return wrapOffsetError(timeErr, cursor.path, recordOffset)
		}
		rrCount, readErr := cursor.u16()
		if readErr != nil {
			return readErr
		}
		event := Event{
			Offset:        recordOffset,
			TXID:          txid,
			Flags:         flags,
			Question:      question,
			QuestionKey:   questionKey,
			QType:         qtype,
			Role:          role,
			TimeTicks:     ticks,
			TimeErrorNS:   truncation,
			SourceAddress: sourceAddress,
			ResolverID:    resolverID,
		}
		for rrIndex := uint16(0); rrIndex < rrCount; rrIndex++ {
			rrOffset := cursor.offset
			sectionByte, sectionErr := cursor.u8()
			if sectionErr != nil {
				return sectionErr
			}
			section, sectionErr := sectionFromByte(sectionByte)
			if sectionErr != nil {
				return newBusinessError(CodeBinaryField, cursor.path, rrOffset, sectionErr.Error(), sectionErr)
			}
			rrType, typeErr := cursor.u16()
			if typeErr != nil {
				return typeErr
			}
			ttl, ttlErr := cursor.u32()
			if ttlErr != nil {
				return ttlErr
			}
			ownerBytes, ownerErr := cursor.lengthBytes()
			if ownerErr != nil {
				return ownerErr
			}
			owner, _, ownerNameErr := NormalizeName(string(ownerBytes))
			if ownerNameErr != nil {
				return wrapOffsetError(ownerNameErr, cursor.path, rrOffset)
			}
			rdata, dataErr := cursor.lengthBytes()
			if dataErr != nil {
				return dataErr
			}
			record := ResourceRecord{Section: section, Name: owner, Type: rrType, TTL: ttl, Offset: rrOffset}
			switch rrType {
			case TypeA:
				if len(rdata) != net.IPv4len {
					return newBusinessError(CodeBinaryField, cursor.path, rrOffset, "A record rdata must be 4 bytes", nil)
				}
				record.Address = net.IP(rdata).String()
			case TypeAAAA:
				if len(rdata) != net.IPv6len {
					return newBusinessError(CodeBinaryField, cursor.path, rrOffset, "AAAA record rdata must be 16 bytes", nil)
				}
				record.Address = net.IP(rdata).String()
			default:
				record.DataHex = hex.EncodeToString(rdata)
			}
			event.Records = append(event.Records, record)
		}
		if err := builder.addEvent(ctx, event, "relative-ns"); err != nil {
			return err
		}
	}
	return nil
}
