package codec

import (
	"bytes"
	"math"
	"time"
)

const BinaryMagic = "LPSB"

type Header struct {
	Format           Format    `json:"format"`
	FormatVersion    int       `json:"format_version"`
	SnapshotSeq      uint64    `json:"snapshot_seq"`
	GeneratedAt      time.Time `json:"generated_at"`
	RecordCount      int       `json:"record_count"`
	ByteOrder        string    `json:"byte_order,omitempty"`
	RequiredFeatures []string  `json:"required_features,omitempty"`
}

func Inspect(data []byte) (*Header, error) {
	if len(data) >= len(BinaryMagic) && bytes.Equal(data[:len(BinaryMagic)], []byte(BinaryMagic)) {
		return inspectBinaryHeader(data)
	}
	return inspectJSONLHeader(data)
}

func sequenceInRange(seq uint64) bool { return seq > 0 && seq <= math.MaxInt64 }
