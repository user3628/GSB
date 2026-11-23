package dnsdetect

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func parseTextBlocks(ctx context.Context, data []byte, builder *sessionBuilder) error {
	cursor := 0
	for cursor < len(data) {
		if err := ctx.Err(); err != nil {
			return newBusinessError(CodeIO, builder.session.Path, int64(cursor), "source parse canceled", err)
		}
		blockOffset := cursor
		if len(data)-cursor < 4 {
			return newBusinessError(CodeTextLength, builder.session.Path, int64(blockOffset), "truncated length prefix", nil)
		}
		length := int(binary.BigEndian.Uint32(data[cursor : cursor+4]))
		cursor += 4
		if length < 0 || length > builder.options.MaxBlockBytes {
			return newBusinessError(CodeTextLength, builder.session.Path, int64(blockOffset), fmt.Sprintf("block length %d exceeds configured limit %d", length, builder.options.MaxBlockBytes), nil)
		}
		if length > len(data)-cursor {
			return newBusinessError(CodeTextLength, builder.session.Path, int64(blockOffset), fmt.Sprintf("length prefix %d exceeds remaining %d bytes", length, len(data)-cursor), nil)
		}
		payload := data[cursor : cursor+length]
		payloadOffset := cursor
		cursor += length
		event, conversion, err := decodeTextBlock(payload, builder.session.Path, blockOffset, payloadOffset)
		if err != nil {
			return err
		}
		if err := builder.addEvent(ctx, event, conversion); err != nil {
			return err
		}
	}
	return nil
}

func decodeTextBlock(payload []byte, sourcePath string, blockOffset, payloadOffset int) (Event, string, error) {
	values := make(map[string][]string)
	lineOffset := payloadOffset
	for _, rawLine := range strings.Split(string(payload), "\n") {
		currentOffset := lineOffset
		lineOffset += len(rawLine) + 1
		line := strings.TrimSpace(strings.TrimSuffix(rawLine, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		separator := strings.IndexByte(line, '=')
		if separator <= 0 {
			return Event{}, "", newBusinessError(CodeTextField, sourcePath, int64(currentOffset), "text block line must be key=value", nil)
		}
		key := strings.ToLower(strings.TrimSpace(line[:separator]))
		value := strings.TrimSpace(line[separator+1:])
		values[key] = append(values[key], value)
	}
	one := func(keys ...string) string {
		for _, key := range keys {
			if entries := values[key]; len(entries) > 0 {
				return entries[len(entries)-1]
			}
		}
		return ""
	}
	txidValue := one("txid")
	if txidValue == "" {
		return Event{}, "", newBusinessError(CodeTextField, sourcePath, int64(blockOffset), "missing txid", nil)
	}
	txid, err := ParseTXID(txidValue)
	if err != nil {
		return Event{}, "", wrapOffsetError(err, sourcePath, int64(blockOffset))
	}
	question, questionKey, err := NormalizeName(one("qname", "question"))
	if err != nil {
		return Event{}, "", wrapOffsetError(err, sourcePath, int64(blockOffset))
	}
	role, err := parseRoleWithCode(one("role"), CodeTextField)
	if err != nil {
		return Event{}, "", wrapOffsetError(err, sourcePath, int64(blockOffset))
	}
	flags := uint16(0)
	if rawFlags := one("flags"); rawFlags != "" {
		base := 10
		if strings.HasPrefix(rawFlags, "0x") || strings.HasPrefix(rawFlags, "0X") {
			base = 16
			rawFlags = rawFlags[2:]
		}
		parsed, parseErr := strconv.ParseUint(rawFlags, base, 16)
		if parseErr != nil {
			return Event{}, "", newBusinessError(CodeTextField, sourcePath, int64(blockOffset), "invalid flags", parseErr)
		}
		flags = uint16(parsed)
	}
	qtype := TypeA
	if rawQType := one("qtype"); rawQType != "" {
		parsed, parseErr := strconv.ParseUint(rawQType, 10, 16)
		if parseErr != nil {
			return Event{}, "", newBusinessError(CodeTextField, sourcePath, int64(blockOffset), "invalid qtype", parseErr)
		}
		qtype = uint16(parsed)
	}
	filetimeValue := one("filetime", "time_filetime")
	if filetimeValue == "" {
		return Event{}, "", newBusinessError(CodeTextField, sourcePath, int64(blockOffset), "missing FILETIME", nil)
	}
	filetime, parseErr := strconv.ParseUint(filetimeValue, 10, 64)
	if parseErr != nil {
		if numErr, ok := parseErr.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
			return Event{}, "", newBusinessError(CodeTimeOverflow, sourcePath, int64(blockOffset), "FILETIME exceeds uint64", parseErr)
		}
		return Event{}, "", newBusinessError(CodeTextField, sourcePath, int64(blockOffset), "invalid FILETIME", parseErr)
	}
	ticks, truncation, err := TicksFromFILETIME(filetime)
	if err != nil {
		return Event{}, "", wrapOffsetError(err, sourcePath, int64(blockOffset))
	}
	event := Event{
		Offset:        int64(blockOffset),
		TXID:          txid,
		Flags:         flags,
		Question:      question,
		QuestionKey:   questionKey,
		QType:         qtype,
		Role:          role,
		TimeTicks:     ticks,
		TimeErrorNS:   truncation,
		SourceAddress: one("source_address"),
		ResolverID:    one("resolver_id"),
	}
	type recordGroup struct {
		keys    []string
		section RecordSection
	}
	groups := []recordGroup{
		{[]string{"answer", "answers"}, SectionAnswer},
		{[]string{"authority", "authorities"}, SectionAuthority},
		{[]string{"additional", "additionals"}, SectionAdditional},
		{[]string{"cache"}, SectionCache},
	}
	for _, group := range groups {
		for _, key := range group.keys {
			for _, rawRecord := range values[key] {
				record, recordErr := decodeTextRecord(rawRecord, group.section, question, int64(blockOffset))
				if recordErr != nil {
					return Event{}, "", wrapOffsetError(recordErr, sourcePath, int64(blockOffset))
				}
				event.Records = append(event.Records, record)
			}
		}
	}
	return event, "filetime", nil
}

func decodeTextRecord(value string, section RecordSection, question string, offset int64) (ResourceRecord, error) {
	parts := strings.Split(value, "|")
	if len(parts) < 4 {
		return ResourceRecord{}, newBusinessError(CodeTextField, "", -1, "record must be owner|type|ttl|address", nil)
	}
	ownerValue := strings.TrimSpace(parts[0])
	if ownerValue == "" || ownerValue == "@" {
		ownerValue = question
	}
	owner, _, err := NormalizeName(ownerValue)
	if err != nil {
		return ResourceRecord{}, err
	}
	rrType, err := parseTextRRType(strings.TrimSpace(parts[1]))
	if err != nil {
		return ResourceRecord{}, err
	}
	ttl, err := strconv.ParseUint(strings.TrimSpace(parts[2]), 10, 32)
	if err != nil {
		return ResourceRecord{}, newBusinessError(CodeTextField, "", -1, "TTL must be an unsigned 32-bit decimal", err)
	}
	record := ResourceRecord{Section: section, Name: owner, Type: rrType, TTL: uint32(ttl), Offset: offset}
	switch rrType {
	case TypeA, TypeAAAA:
		address := strings.TrimSpace(parts[3])
		ip := net.ParseIP(address)
		if ip == nil || rrType == TypeA && ip.To4() == nil {
			return ResourceRecord{}, newBusinessError(CodeTextField, "", -1, "invalid A/AAAA address", nil)
		}
		if rrType == TypeA {
			record.Address = ip.To4().String()
		} else {
			record.Address = ip.To16().String()
		}
	default:
		record.DataHex = strings.TrimSpace(parts[3])
	}
	return record, nil
}

func parseTextRRType(value string) (uint16, error) {
	switch strings.ToUpper(value) {
	case "A":
		return TypeA, nil
	case "AAAA":
		return TypeAAAA, nil
	default:
		parsed, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return 0, newBusinessError(CodeTextField, "", -1, "unsupported record type", err)
		}
		return uint16(parsed), nil
	}
}
