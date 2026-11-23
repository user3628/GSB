package requests

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"unicode"

	"vpnbench/internal/model"
)

// Parse parses a concatenation of length-prefixed UTF-8 request blocks.
//
// Format:
//
//	<decimal byte length>\n
//	stream_id=flow-a
//	request_seq=1
//	src=10.0.0.10
//	dst=10.0.1.20
//	packet_bytes=1200
//
// The length is the exact byte length of the block body. A CRLF line ending is
// accepted. The parser preserves arrival order; settlement sorts later.
func Parse(data []byte) ([]model.Request, error) {
	var parsed []model.Request
	var cursor int64
	for {
		cursor = skipWhitespace(data, cursor)
		if cursor >= int64(len(data)) {
			break
		}
		prefixOffset := cursor
		lineEnd := bytes.IndexByte(data[cursor:], '\n')
		if lineEnd < 0 {
			return nil, requestError("length prefix must end with newline", prefixOffset)
		}
		lineEnd += int(cursor)
		lengthText := strings.TrimSpace(string(data[cursor:lineEnd]))
		if lengthText == "" {
			return nil, requestError("empty block length prefix", prefixOffset)
		}
		length, err := strconv.ParseInt(lengthText, 10, 64)
		if err != nil || length <= 0 {
			return nil, requestError("block length must be a positive decimal byte count", prefixOffset)
		}
		bodyStart := int64(lineEnd + 1)
		if length > int64(len(data))-bodyStart {
			return nil, requestError("declared block length exceeds remaining input", prefixOffset)
		}
		bodyEnd := bodyStart + length
		request, err := parseBlock(data[bodyStart:bodyEnd], bodyStart, prefixOffset)
		if err != nil {
			return nil, err
		}
		request.ArrivalOrdinal = int64(len(parsed) + 1)
		parsed = append(parsed, request)
		cursor = bodyEnd
	}
	if len(parsed) == 0 {
		return nil, requestError("request stream contains no blocks", 0)
	}
	return parsed, nil
}

func parseBlock(body []byte, bodyOffset, itemOffset int64) (model.Request, error) {
	request := model.Request{InputOffset: itemOffset}
	seen := make(map[string]bool, 5)
	var lineOffset int64
	for lineOffset <= int64(len(body)) {
		end := bytes.IndexByte(body[lineOffset:], '\n')
		var line []byte
		var next int64
		if end < 0 {
			line = body[lineOffset:]
			next = int64(len(body)) + 1
		} else {
			end += int(lineOffset)
			line = body[lineOffset:end]
			next = int64(end) + 1
		}
		line = bytes.TrimSuffix(line, []byte{'\r'})
		text := strings.TrimSpace(string(line))
		offset := bodyOffset + lineOffset
		if text != "" && !strings.HasPrefix(text, "#") {
			separator := strings.IndexByte(text, '=')
			if separator < 0 {
				return model.Request{}, requestError("request line must be key=value", offset)
			}
			key := strings.TrimSpace(text[:separator])
			value := strings.TrimSpace(text[separator+1:])
			canonical, ok := canonicalField(key)
			if !ok {
				return model.Request{}, requestError("unknown request field "+key, offset)
			}
			if seen[canonical] {
				return model.Request{}, requestError("duplicate request field "+canonical, offset)
			}
			seen[canonical] = true
			switch canonical {
			case "stream_id":
				if err := validateStreamID(value, offset); err != nil {
					return model.Request{}, err
				}
				request.StreamID = value
			case "request_seq":
				number, err := parsePositiveInt(value, "request_seq", offset)
				if err != nil {
					return model.Request{}, err
				}
				request.RequestSeq = number
			case "source":
				ip := net.ParseIP(value)
				if ip == nil {
					return model.Request{}, requestError("src must be a valid IP address", offset)
				}
				request.Source = ip.String()
			case "destination":
				ip := net.ParseIP(value)
				if ip == nil {
					return model.Request{}, requestError("dst must be a valid IP address", offset)
				}
				request.Destination = ip.String()
			case "packet_bytes":
				number, err := parseNonNegativeInt(value, "packet_bytes", offset)
				if err != nil {
					return model.Request{}, err
				}
				request.PacketBytes = number
			}
		}
		if next > int64(len(body)) {
			break
		}
		lineOffset = next
	}

	required := []struct {
		name string
		ok   bool
	}{
		{"stream_id", request.StreamID != ""},
		{"request_seq", request.RequestSeq > 0},
		{"src", request.Source != ""},
		{"dst", request.Destination != ""},
		{"packet_bytes", seen["packet_bytes"]},
	}
	for _, field := range required {
		if !field.ok {
			return model.Request{}, requestError("missing required request field "+field.name, itemOffset)
		}
	}
	return request, nil
}

func canonicalField(field string) (string, bool) {
	switch field {
	case "stream_id", "flow_id":
		return "stream_id", true
	case "request_seq", "seq":
		return "request_seq", true
	case "src", "source":
		return "source", true
	case "dst", "destination":
		return "destination", true
	case "packet_bytes", "packet_length":
		return "packet_bytes", true
	default:
		return "", false
	}
}

func validateStreamID(value string, offset int64) error {
	if value == "" {
		return requestError("stream_id must not be empty", offset)
	}
	if strings.TrimSpace(value) != value {
		return requestError("stream_id must not have surrounding whitespace", offset)
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return requestError("stream_id contains whitespace or control characters", offset)
		}
	}
	return nil
}

func parsePositiveInt(text, field string, offset int64) (int64, error) {
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 {
		return 0, requestError(field+" must be a positive integer", offset)
	}
	return value, nil
}

func parseNonNegativeInt(text, field string, offset int64) (int64, error) {
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value < 0 {
		return 0, requestError(field+" must be a non-negative integer", offset)
	}
	return value, nil
}

func requestError(message string, offset int64) *model.CodedError {
	return model.NewError(model.CodeRequestInvalid, message, offset)
}

func skipWhitespace(data []byte, offset int64) int64 {
	for offset < int64(len(data)) {
		switch data[offset] {
		case ' ', '\t', '\r', '\n':
			offset++
		default:
			return offset
		}
	}
	return offset
}

func FormatBlock(request model.Request) string {
	body := fmt.Sprintf(
		"stream_id=%s\nrequest_seq=%d\nsrc=%s\ndst=%s\npacket_bytes=%d\n",
		request.StreamID,
		request.RequestSeq,
		request.Source,
		request.Destination,
		request.PacketBytes,
	)
	return fmt.Sprintf("%d\n%s", len(body), body)
}
