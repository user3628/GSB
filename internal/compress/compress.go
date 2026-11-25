package compress

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"searchlab/internal/apperr"
	"searchlab/internal/config"
	"searchlab/internal/index"
)

const EncodingVersion = "docid_delta+base128/v2"

type Posting struct {
	DocID int64 `json:"doc_id"`
	Quant int64 `json:"quantized"`
}

type Block struct {
	Ordinal        int    `json:"ordinal"`
	FirstDocID     int64  `json:"first_doc_id"`
	LastDocID      int64  `json:"last_doc_id"`
	Count          int    `json:"count"`
	DeltaEncoding  string `json:"delta_encoding"`
	WeightEncoding string `json:"weight_encoding"`
	Data           []byte `json:"data"`
	Checksum       string `json:"checksum"`
}

type Skip struct {
	DocID   int64 `json:"doc_id"`
	Block   int   `json:"block"`
	Index   int   `json:"index"`
	Posting int   `json:"posting"`
}

type CompressedTerm struct {
	TermID int32   `json:"term_id"`
	Term   string  `json:"term"`
	DF     int     `json:"df"`
	Blocks []Block `json:"blocks"`
	Skips  []Skip  `json:"skips"`
}

type Artifact struct {
	ConfigVersion  string           `json:"config_version"`
	PostingsDigest string           `json:"postings_digest"`
	Encoding       string           `json:"encoding"`
	DeltaEncoding  string           `json:"delta_encoding"`
	VarintEncoding string           `json:"varint_encoding"`
	BlockSize      int              `json:"block_size"`
	SkipStep       int              `json:"skip_step"`
	Terms          []CompressedTerm `json:"terms"`
}

func Build(cfg config.Config, stats index.StatsArtifact) ([]byte, *Artifact, error) {
	artifact := Artifact{
		ConfigVersion:  cfg.Version,
		PostingsDigest: cfg.PostingsDigest(),
		Encoding:       EncodingVersion,
		DeltaEncoding:  cfg.Index.DeltaEncoding,
		VarintEncoding: cfg.Index.VarintEncoding,
		BlockSize:      cfg.Index.BlockSize,
		SkipStep:       cfg.Index.SkipStep,
		Terms:          make([]CompressedTerm, 0, len(stats.Terms)),
	}
	for _, term := range stats.Terms {
		compressed := CompressedTerm{TermID: term.TermID, Term: term.Term, DF: len(term.Postings), Blocks: make([]Block, 0), Skips: make([]Skip, 0)}
		source := make([]Posting, 0, len(term.Postings))
		for _, posting := range term.Postings {
			source = append(source, Posting{DocID: posting.DocID, Quant: posting.Quantized})
		}
		for start := 0; start < len(source); start += cfg.Index.BlockSize {
			end := start + cfg.Index.BlockSize
			if end > len(source) {
				end = len(source)
			}
			data, err := encodeBlock(source[start:end], cfg.Index.DeltaEncoding, cfg.Index.VarintEncoding)
			if err != nil {
				return nil, nil, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "encode posting block", Term: term.Term, FirstDiffBlock: start/cfg.Index.BlockSize + 1, Err: err}
			}
			compressed.Blocks = append(compressed.Blocks, Block{
				Ordinal:        start / cfg.Index.BlockSize,
				FirstDocID:     source[start].DocID,
				LastDocID:      source[end-1].DocID,
				Count:          end - start,
				DeltaEncoding:  cfg.Index.DeltaEncoding,
				WeightEncoding: cfg.Index.VarintEncoding,
				Data:           data,
				Checksum:       digest(data),
			})
		}
		postingIndex := 0
		for _, block := range compressed.Blocks {
			for i := 0; i < block.Count; i++ {
				if postingIndex%cfg.Index.SkipStep == 0 && postingIndex > 0 {
					compressed.Skips = append(compressed.Skips, Skip{
						DocID:   source[postingIndex].DocID,
						Block:   block.Ordinal,
						Index:   i,
						Posting: postingIndex,
					})
				}
				postingIndex++
			}
		}
		decodedAll := make([]Posting, 0, len(source))
		for _, block := range compressed.Blocks {
			decoded, err := DecodeBlock(block)
			if err != nil {
				return nil, nil, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "compressed block does not decode", Term: term.Term, FirstDiffBlock: block.Ordinal + 1, Err: err}
			}
			if len(decoded) != block.Count {
				return nil, nil, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: fmt.Sprintf("decoded count %d differs from expected %d", len(decoded), block.Count), Term: term.Term, FirstDiffBlock: block.Ordinal + 1}
			}
			decodedAll = append(decodedAll, decoded...)
		}
		for i := range source {
			if i >= len(decodedAll) || source[i] != decodedAll[i] {
				block := 1
				if i >= 0 && cfg.Index.BlockSize > 0 {
					block = i/cfg.Index.BlockSize + 1
				}
				return nil, nil, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "decoded posting differs from uncompressed source", Term: term.Term, FirstDiffBlock: block}
			}
		}
		if len(decodedAll) != len(source) {
			return nil, nil, &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "decoded posting count differs from uncompressed source", Term: term.Term, FirstDiffBlock: len(source)/cfg.Index.BlockSize + 1}
		}
		artifact.Terms = append(artifact.Terms, compressed)
	}
	payload, err := json.Marshal(artifact)
	if err != nil {
		return nil, nil, apperr.E(apperr.CodeInput, "marshal postings artifact", err)
	}
	return payload, &artifact, nil
}

func encodeBlock(postings []Posting, deltaEncoding, weightEncoding string) ([]byte, error) {
	if len(postings) == 0 {
		return nil, fmt.Errorf("empty posting block")
	}
	var buf bytes.Buffer
	writeUvarint(&buf, uint64(postings[0].DocID))
	previous := postings[0].DocID
	for _, posting := range postings[1:] {
		if posting.DocID <= previous {
			return nil, fmt.Errorf("doc_id sequence is not strictly increasing: %d then %d", previous, posting.DocID)
		}
		switch deltaEncoding {
		case "docid_delta":
			writeUvarint(&buf, uint64(posting.DocID-previous))
		case "docid_delta_first":
			writeUvarint(&buf, uint64(posting.DocID-postings[0].DocID))
		default:
			return nil, fmt.Errorf("unsupported doc_id delta encoding %q", deltaEncoding)
		}
		previous = posting.DocID
	}
	for _, posting := range postings {
		switch weightEncoding {
		case "base128_zigzag":
			writeUvarint(&buf, zigzag(posting.Quant))
		case "base128":
			if posting.Quant < 0 {
				return nil, fmt.Errorf("base128 weight encoding requires non-negative quantized values")
			}
			writeUvarint(&buf, uint64(posting.Quant))
		default:
			return nil, fmt.Errorf("unsupported weight varint encoding %q", weightEncoding)
		}
	}
	return buf.Bytes(), nil
}

func DecodeBlock(block Block) ([]Posting, error) {
	if !checksumValid(block.Data, block.Checksum) {
		return nil, decodeError(block, "block checksum mismatch")
	}
	if block.Count <= 0 {
		return nil, decodeError(block, "invalid block count %d", block.Count)
	}
	postings := make([]Posting, block.Count)
	offset := 0
	first, n := binary.Uvarint(block.Data[offset:])
	if n <= 0 {
		return nil, decodeError(block, "invalid first doc_id varint")
	}
	offset += n
	postings[0].DocID = int64(first)
	if postings[0].DocID != block.FirstDocID {
		return nil, decodeError(block, "first doc_id %d differs from metadata %d", postings[0].DocID, block.FirstDocID)
	}
	for i := 1; i < block.Count; i++ {
		delta, n := binary.Uvarint(block.Data[offset:])
		if n <= 0 || delta == 0 {
			return nil, decodeError(block, "invalid doc delta at posting %d", i)
		}
		offset += n
		switch block.DeltaEncoding {
		case "docid_delta":
			postings[i].DocID = postings[i-1].DocID + int64(delta)
		case "docid_delta_first":
			postings[i].DocID = postings[0].DocID + int64(delta)
		default:
			return nil, decodeError(block, "unsupported doc_id delta encoding %q", block.DeltaEncoding)
		}
	}
	for i := 0; i < block.Count; i++ {
		encoded, n := binary.Uvarint(block.Data[offset:])
		if n <= 0 {
			return nil, decodeError(block, "invalid weight varint at posting %d", i)
		}
		offset += n
		switch block.WeightEncoding {
		case "base128_zigzag":
			postings[i].Quant = unzigzag(encoded)
		case "base128":
			if encoded > uint64(^uint64(0)>>1) {
				return nil, decodeError(block, "base128 weight overflows int64")
			}
			postings[i].Quant = int64(encoded)
		default:
			return nil, decodeError(block, "unsupported weight varint encoding %q", block.WeightEncoding)
		}
	}
	if offset != len(block.Data) {
		return nil, decodeError(block, "trailing bytes in block: consumed %d of %d", offset, len(block.Data))
	}
	if postings[len(postings)-1].DocID != block.LastDocID {
		return nil, decodeError(block, "last doc_id %d differs from metadata %d", postings[len(postings)-1].DocID, block.LastDocID)
	}
	return postings, nil
}

func VerifyArtifact(artifact Artifact) error {
	for _, term := range artifact.Terms {
		var previous int64 = -1
		postingIndex := 0
		for _, block := range term.Blocks {
			decoded, err := DecodeBlock(block)
			if err != nil {
				return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "compressed block does not decode", Term: term.Term, FirstDiffBlock: block.Ordinal + 1, Err: err}
			}
			for _, posting := range decoded {
				if posting.DocID <= previous {
					return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: "decoded doc_id order is not strictly increasing", Term: term.Term, FirstDiffBlock: block.Ordinal + 1}
				}
				previous = posting.DocID
				postingIndex++
			}
		}
		if postingIndex != term.DF {
			return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: fmt.Sprintf("decoded postings %d differ from df %d", postingIndex, term.DF), Term: term.Term}
		}
	}
	return nil
}

func decodeError(block Block, format string, args ...any) error {
	return &apperr.Error{Code: apperr.CodeDecodeMismatch, Message: fmt.Sprintf(format, args...), FirstDiffBlock: block.Ordinal + 1}
}

func writeUvarint(buf *bytes.Buffer, value uint64) {
	var scratch [10]byte
	n := binary.PutUvarint(scratch[:], value)
	buf.Write(scratch[:n])
}

func zigzag(value int64) uint64 {
	return uint64((value << 1) ^ (value >> 63))
}

func unzigzag(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func checksumValid(data []byte, expected string) bool {
	if expected == "" {
		return false
	}
	actual := digest(data)
	return actual == expected
}
