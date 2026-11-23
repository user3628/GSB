package dnsdetect

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

type jsonEventWire struct {
	Time          string       `json:"time"`
	TXID          any          `json:"txid"`
	Flags         any          `json:"flags"`
	Role          string       `json:"role"`
	Question      string       `json:"qname"`
	QType         any          `json:"qtype"`
	Answers       []jsonRRWire `json:"answers"`
	Authorities   []jsonRRWire `json:"authorities"`
	Additionals   []jsonRRWire `json:"additionals"`
	Cache         []jsonRRWire `json:"cache"`
	SourceAddress string       `json:"source_address"`
	ResolverID    string       `json:"resolver_id"`
}

type jsonRRWire struct {
	Name    string `json:"name"`
	Type    any    `json:"type"`
	TTL     any    `json:"ttl"`
	Address string `json:"address"`
	Data    string `json:"data"`
}

func parseJSONLines(ctx context.Context, data []byte, builder *sessionBuilder) error {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), builder.options.MaxBlockBytes)
	offset := int64(0)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := append([]byte(nil), scanner.Bytes()...)
		lineOffset := offset
		offset += int64(len(line)) + 1
		if err := ctx.Err(); err != nil {
			return newBusinessError(CodeIO, builder.session.Path, lineOffset, "source parse canceled", err)
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		event, err := decodeJSONEvent(line, builder.session.Path, lineOffset, lineNumber)
		if err != nil {
			return err
		}
		if err := builder.addEvent(ctx, event, "rfc3339"); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return newBusinessError(CodeJSONSyntax, builder.session.Path, offset, "JSON line exceeds configured limit or cannot be read", err)
	}
	return nil
}

func decodeJSONEvent(line []byte, sourcePath string, offset int64, lineNumber int) (Event, error) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	var wire jsonEventWire
	if err := decoder.Decode(&wire); err != nil {
		return Event{}, newBusinessError(CodeJSONSyntax, sourcePath, offset, fmt.Sprintf("line %d is not a JSON object", lineNumber), err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Event{}, newBusinessError(CodeJSONSyntax, sourcePath, offset, "trailing JSON content", err)
	}
	txid, err := parseJSONTXID(wire.TXID)
	if err != nil {
		return Event{}, wrapOffsetError(err, sourcePath, offset)
	}
	flags := uint16(0)
	if wire.Flags != nil {
		flags, err = parseJSONUint16(wire.Flags, "flags")
		if err != nil {
			return Event{}, wrapOffsetError(err, sourcePath, offset)
		}
	}
	qtype := TypeA
	if wire.QType != nil {
		qtype, err = parseJSONUint16(wire.QType, "qtype")
		if err != nil {
			return Event{}, wrapOffsetError(err, sourcePath, offset)
		}
	}
	role, err := parseRole(wire.Role)
	if err != nil {
		return Event{}, wrapOffsetError(err, sourcePath, offset)
	}
	question, questionKey, err := NormalizeName(wire.Question)
	if err != nil {
		return Event{}, wrapOffsetError(err, sourcePath, offset)
	}
	ticks, truncation, err := TicksFromRFC3339(wire.Time)
	if err != nil {
		return Event{}, wrapOffsetError(err, sourcePath, offset)
	}
	event := Event{
		Offset:        offset,
		TXID:          txid,
		Flags:         flags,
		Question:      question,
		QuestionKey:   questionKey,
		QType:         qtype,
		Role:          role,
		TimeTicks:     ticks,
		TimeErrorNS:   truncation,
		SourceAddress: wire.SourceAddress,
		ResolverID:    wire.ResolverID,
	}
	type rrGroup struct {
		section RecordSection
		values  []jsonRRWire
	}
	groups := []rrGroup{
		{SectionAnswer, wire.Answers},
		{SectionAuthority, wire.Authorities},
		{SectionAdditional, wire.Additionals},
		{SectionCache, wire.Cache},
	}
	for _, group := range groups {
		for _, rrWire := range group.values {
			record, rrErr := decodeJSONRR(rrWire, group.section, question, offset)
			if rrErr != nil {
				return Event{}, wrapOffsetError(rrErr, sourcePath, offset)
			}
			event.Records = append(event.Records, record)
		}
	}
	return event, nil
}

func decodeJSONRR(wire jsonRRWire, section RecordSection, question string, offset int64) (ResourceRecord, error) {
	name := wire.Name
	if name == "" || name == "@" {
		name = question
	}
	owner, _, err := NormalizeName(name)
	if err != nil {
		return ResourceRecord{}, err
	}
	rrType, err := parseRRType(wire.Type)
	if err != nil {
		return ResourceRecord{}, err
	}
	ttl, err := parseJSONUint32(wire.TTL, "ttl")
	if err != nil {
		return ResourceRecord{}, err
	}
	record := ResourceRecord{Section: section, Name: owner, Type: rrType, TTL: ttl, Offset: offset}
	switch rrType {
	case TypeA, TypeAAAA:
		record.Address = strings.TrimSpace(wire.Address)
		if record.Address == "" {
			record.Address = strings.TrimSpace(wire.Data)
		}
		ip := net.ParseIP(record.Address)
		if ip == nil || rrType == TypeA && ip.To4() == nil {
			return ResourceRecord{}, newBusinessError(CodeJSONField, "", -1, "invalid A/AAAA address", nil)
		}
		if rrType == TypeAAAA {
			record.Address = ip.To16().String()
		} else {
			record.Address = ip.To4().String()
		}
	default:
		record.DataHex = strings.TrimSpace(wire.Data)
	}
	return record, nil
}

func parseRole(value string) (RecordRole, error) {
	return parseRoleWithCode(value, CodeJSONField)
}

func parseRoleWithCode(value, code string) (RecordRole, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "query", "q":
		return RoleQuery, nil
	case "response", "answer", "reply", "r":
		return RoleResponse, nil
	case "cache", "cache_observation", "cached":
		return RoleCache, nil
	default:
		return "", newBusinessError(code, "", -1, "unknown record role", nil)
	}
}

func parseJSONTXID(value any) (uint32, error) {
	switch typed := value.(type) {
	case nil:
		return 0, newBusinessError(CodeJSONField, "", -1, "missing txid", nil)
	case json.Number:
		parsed, err := strconv.ParseUint(string(typed), 10, 32)
		if err != nil {
			return 0, newBusinessError(CodeTXID, "", -1, "txid must be an unsigned 32-bit integer", err)
		}
		return uint32(parsed), nil
	case string:
		return ParseTXID(typed)
	default:
		return 0, newBusinessError(CodeTXID, "", -1, "txid must be a number or decimal/hex string", nil)
	}
}

func parseJSONUint16(value any, field string) (uint16, error) {
	u, err := parseJSONUint(value, 16, field)
	return uint16(u), err
}

func parseJSONUint32(value any, field string) (uint32, error) {
	u, err := parseJSONUint(value, 32, field)
	return uint32(u), err
}

func parseJSONUint(value any, bits int, field string) (uint64, error) {
	switch typed := value.(type) {
	case nil:
		return 0, newBusinessError(CodeJSONField, "", -1, "missing "+field, nil)
	case json.Number:
		u, err := strconv.ParseUint(string(typed), 10, bits)
		if err != nil {
			return 0, newBusinessError(CodeJSONField, "", -1, field+" must be an unsigned integer", err)
		}
		return u, nil
	case string:
		v := strings.TrimSpace(typed)
		base := 10
		if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
			base = 16
			v = v[2:]
		}
		u, err := strconv.ParseUint(v, base, bits)
		if err != nil {
			return 0, newBusinessError(CodeJSONField, "", -1, field+" must be an unsigned integer", err)
		}
		return u, nil
	default:
		return 0, newBusinessError(CodeJSONField, "", -1, field+" must be numeric", nil)
	}
}

func parseRRType(value any) (uint16, error) {
	switch typed := value.(type) {
	case nil:
		return 0, newBusinessError(CodeJSONField, "", -1, "missing record type", nil)
	case string:
		switch strings.ToUpper(strings.TrimSpace(typed)) {
		case "A":
			return TypeA, nil
		case "AAAA":
			return TypeAAAA, nil
		default:
			u, err := strconv.ParseUint(strings.TrimSpace(typed), 10, 16)
			if err != nil {
				return 0, newBusinessError(CodeJSONField, "", -1, "unsupported record type", err)
			}
			return uint16(u), nil
		}
	default:
		u, err := parseJSONUint(typed, 16, "record type")
		return uint16(u), err
	}
}
