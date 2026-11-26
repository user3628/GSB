package model

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
)

type Encoding string

const (
	EncodingFixed      Encoding = "fixed"
	EncodingRLE        Encoding = "rle"
	EncodingDictionary Encoding = "dictionary"
)

func (e Encoding) Valid() bool {
	switch e {
	case EncodingFixed, EncodingRLE, EncodingDictionary:
		return true
	default:
		return false
	}
}

type ColumnDef struct {
	Name     string   `json:"name"`
	Role     string   `json:"role"`
	Encoding Encoding `json:"encoding"`
	Scale    int      `json:"scale,omitempty"`
	Width    int      `json:"width_bytes"`
}

type BlockMeta struct {
	BlockNo           int      `json:"block_no"`
	Partition         int      `json:"partition"`
	RowCount          int      `json:"row_count"`
	Encoding          Encoding `json:"encoding"`
	UncompressedBytes int64    `json:"uncompressed_bytes"`
	Digest            string   `json:"digest"`
}

type Shard struct {
	ShardID     string      `json:"shard_id"`
	Version     int64       `json:"version"`
	GeneratedAt time.Time   `json:"generated_at"`
	Columns     []ColumnDef `json:"columns"`
	Blocks      []BlockMeta `json:"blocks"`
}

type Block struct {
	Meta          BlockMeta `json:"meta"`
	GroupPayload  []byte    `json:"group_payload_base64,omitempty"`
	MetricPayload []byte    `json:"metric_payload_base64,omitempty"`
}

type ShardPackage struct {
	Manifest Shard    `json:"manifest"`
	Blocks   []*Block `json:"-"`
}

func (p *ShardPackage) BlockByNo(blockNo int) *Block {
	for _, block := range p.Blocks {
		if block != nil && block.Meta.BlockNo == blockNo {
			return block
		}
	}
	return nil
}

func DigestPayload(meta BlockMeta, groupPayload, metricPayload []byte) string {
	h := sha256.New()
	var le [8]byte
	binary.LittleEndian.PutUint64(le[:], uint64(meta.RowCount))
	h.Write(le[:])
	binary.LittleEndian.PutUint64(le[:], uint64(meta.Partition))
	h.Write(le[:])
	binary.LittleEndian.PutUint64(le[:], uint64(meta.UncompressedBytes))
	h.Write(le[:])
	h.Write([]byte(meta.Encoding))
	h.Write([]byte{0})
	binary.LittleEndian.PutUint32(le[:4], uint32(len(groupPayload)))
	h.Write(le[:4])
	h.Write(groupPayload)
	binary.LittleEndian.PutUint32(le[:4], uint32(len(metricPayload)))
	h.Write(le[:4])
	h.Write(metricPayload)
	return hex.EncodeToString(h.Sum(nil))
}

func (b *Block) VerifyDigest() error {
	if b == nil {
		return fmt.Errorf("nil block")
	}
	want := DigestPayload(b.Meta, b.GroupPayload, b.MetricPayload)
	if b.Meta.Digest != want {
		return fmt.Errorf("digest mismatch: want %s, got %s", want, b.Meta.Digest)
	}
	return nil
}

type OfflineEvent struct {
	Seq       int64     `json:"seq"`
	EventID   string    `json:"event_id"`
	BatchID   string    `json:"batch_id"`
	SessionID string    `json:"session_id"`
	ShardID   string    `json:"shard_id"`
	Version   int64     `json:"version"`
	Partition int       `json:"partition"`
	BlockNo   int       `json:"block_no"`
	Type      string    `json:"type"`
	At        time.Time `json:"at"`
}
