// Package parse reads JSON context declarations and length-prefixed text
// block payload sets and normalizes them into the internal sample model.
package parse

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"injectctl/internal/model"
)

// ParseError records an illegal declaration together with its source offset.
type ParseError struct {
	Code         string `json:"code"`
	SourceOffset int    `json:"source_offset"`
	SampleID     string `json:"sample_id,omitempty"`
	Message      string `json:"message"`
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("%s at offset %d: %s", e.Code, e.SourceOffset, e.Message)
}

// Output is the fault-tolerant parse result.
type Output struct {
	ContextVersion string
	Samples        []model.Sample
	Errors         []*ParseError
}

// ParseContextFile decodes the JSON declaration document and records the
// stream offset of every declaration for traceability.
func ParseContextFile(r io.Reader) (model.ContextFile, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return model.ContextFile{}, &ParseError{Code: model.CodeContextInvalid, Message: err.Error()}
	}

	var doc model.ContextFile
	if err := json.Unmarshal(raw, &doc); err != nil {
		return model.ContextFile{}, &ParseError{
			Code:    model.CodeContextInvalid,
			Message: "malformed context JSON: " + err.Error(),
		}
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	delim, ok := tok.(json.Delim)
	if err != nil || !ok || delim != '{' {
		return model.ContextFile{}, &ParseError{
			Code:    model.CodeContextInvalid,
			Message: "context document must be a JSON object",
		}
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			break
		}
		if keyTok.(string) == "declarations" {
			arrTok, err := dec.Token()
			if err != nil {
				break
			}
			if d, ok := arrTok.(json.Delim); !ok || d != '[' {
				return model.ContextFile{}, &ParseError{Code: model.CodeContextInvalid, Message: "declarations must be an array"}
			}
			idx := 0
			for dec.More() {
				start := dec.InputOffset()
				var decl model.ContextDecl
				if err := dec.Decode(&decl); err != nil {
					return model.ContextFile{}, &ParseError{
						Code:         model.CodeContextInvalid,
						SourceOffset: int(start),
						Message:      "malformed declaration: " + err.Error(),
					}
				}
				decl.SourceOffset = int(start)
				doc.Declarations[idx] = decl
				idx++
			}
			break
		}
		// skip unknown top-level value
		var skip any
		if err := dec.Decode(&skip); err != nil {
			break
		}
	}
	return doc, nil
}

// ReadPayloadBlocks reads the length-prefixed text block stream.
//
// Wire format (deterministic, binary-safe):
//
//	uint32 big-endian block length N (the "length prefix")
//	N bytes of UTF-8 text:
//	  id=<sample id>\n
//	  surface=<sql|command|path|template>\n
//	  <remaining bytes are the raw payload, may contain any byte>
func ReadPayloadBlocks(r io.Reader) ([]model.PayloadBlock, error) {
	var blocks []model.PayloadBlock
	streamOffset := 0
	var header [4]byte
	for {
		n, err := io.ReadFull(r, header[:])
		if err == io.EOF && n == 0 {
			return blocks, nil
		}
		if err == io.ErrUnexpectedEOF {
			return nil, &ParseError{Code: model.CodeContextInvalid, SourceOffset: streamOffset, Message: "truncated block length prefix"}
		}
		if err != nil {
			return nil, &ParseError{Code: model.CodeContextInvalid, SourceOffset: streamOffset, Message: err.Error()}
		}
		length := binary.BigEndian.Uint32(header[:])
		blockStart := streamOffset
		streamOffset += 4

		body := make([]byte, length)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, &ParseError{Code: model.CodeContextInvalid, SourceOffset: blockStart, Message: "truncated block body"}
		}
		streamOffset += int(length)

		block, perr := parseBlock(body, blockStart)
		if perr != nil {
			return nil, perr
		}
		blocks = append(blocks, block)
	}
}

func parseBlock(body []byte, offset int) (model.PayloadBlock, error) {
	idEnd := bytes.IndexByte(body, '\n')
	if idEnd < 0 {
		return model.PayloadBlock{}, &ParseError{Code: model.CodeContextInvalid, SourceOffset: offset, Message: "block missing id line"}
	}
	id := string(bytes.TrimPrefix(body[:idEnd], []byte("id=")))
	rest := body[idEnd+1:]
	surfaceEnd := bytes.IndexByte(rest, '\n')
	if surfaceEnd < 0 {
		return model.PayloadBlock{}, &ParseError{Code: model.CodeContextInvalid, SourceOffset: offset, Message: "block missing surface line"}
	}
	surface := string(bytes.TrimPrefix(rest[:surfaceEnd], []byte("surface=")))
	raw := string(rest[surfaceEnd+1:])
	return model.PayloadBlock{ID: id, Surface: surface, Raw: raw, SourceOffset: offset}, nil
}

// BuildSamples joins payload blocks with context declarations, validates the
// declarations and returns valid samples plus non-fatal per-item errors.
func BuildSamples(doc model.ContextFile, blocks []model.PayloadBlock) Output {
	out := Output{ContextVersion: doc.ContextVersion}
	addErr := func(code string, offset int, id, msg string) {
		out.Errors = append(out.Errors, &ParseError{
			Code: code, SourceOffset: offset, SampleID: id, Message: msg,
		})
	}

	seenDecl := map[string]bool{}
	declByID := map[string]model.ContextDecl{}
	for _, decl := range doc.Declarations {
		if seenDecl[decl.ID] {
			addErr(model.CodeContextInvalid, decl.SourceOffset, decl.ID, "duplicate declaration id")
			continue
		}
		seenDecl[decl.ID] = true

		if _, ok := model.SupportedLandings[decl.Landing]; !ok {
			addErr(model.CodeContextInvalid, decl.SourceOffset, decl.ID, "unsupported landing: "+decl.Landing)
			continue
		}
		if len(decl.Encodings) == 0 {
			addErr(model.CodeContextInvalid, decl.SourceOffset, decl.ID, "encoding requirement set must not be empty")
			continue
		}
		bad := false
		for _, enc := range decl.Encodings {
			if !model.SupportedEncodings[enc] {
				addErr(model.CodeContextInvalid, decl.SourceOffset, decl.ID, "unsupported encoding: "+enc)
				bad = true
			}
		}
		if bad {
			continue
		}
		declByID[decl.ID] = decl
	}

	seenBlock := map[string]bool{}
	type joined struct {
		block model.PayloadBlock
		seq   int
	}
	var okBlocks []joined
	seq := 0
	for _, b := range blocks {
		if seenBlock[b.ID] {
			addErr(model.CodeContextInvalid, b.SourceOffset, b.ID, "duplicate payload block id")
			continue
		}
		seenBlock[b.ID] = true
		seq++
		okBlocks = append(okBlocks, joined{block: b, seq: seq})
	}

	for _, j := range okBlocks {
		decl, ok := declByID[j.block.ID]
		if !ok {
			addErr(model.CodeContextInvalid, j.block.SourceOffset, j.block.ID, "payload has no valid context declaration")
			continue
		}
		if decl.Surface != "" && decl.Surface != j.block.Surface {
			addErr(model.CodeContextOutOfBounds, j.block.SourceOffset, j.block.ID,
				fmt.Sprintf("payload surface %q outside declared context surface %q", j.block.Surface, decl.Surface))
			continue
		}
		out.Samples = append(out.Samples, model.Sample{
			ID:           j.block.ID,
			Seq:          j.seq,
			Surface:      j.block.Surface,
			Raw:          j.block.Raw,
			Landing:      decl.Landing,
			Encodings:    append([]string(nil), decl.Encodings...),
			SourceOffset: j.block.SourceOffset,
		})
	}

	sort.SliceStable(out.Samples, func(i, j int) bool { return out.Samples[i].Seq < out.Samples[j].Seq })
	return out
}
