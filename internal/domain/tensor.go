package domain

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
)

const TensorMagic = "PPT1"

type TensorHeader struct {
	Format       string   `json:"format"`
	Version      int      `json:"version"`
	DType        string   `json:"dtype"`
	Shape        []int    `json:"shape"`
	SampleCount  int      `json:"sample_count"`
	FeatureDim   int      `json:"feature_dim"`
	FeatureNames []string `json:"feature_names"`
	NullMask     []bool   `json:"null_mask"`
	NullCount    int      `json:"null_count"`
	BodyDigest   string   `json:"body_digest"`
}

type TensorArtifact struct {
	ShardIndex int          `json:"shard_index"`
	Header     TensorHeader `json:"header"`
	Body       []byte       `json:"-"`
	Bytes      []byte       `json:"-"`
	Digest     string       `json:"digest"`
}

func BuildTensorArtifact(shardIndex int, records []SampledRecord, policy Policy) (*TensorArtifact, error) {
	selected := make([]SampledRecord, 0, len(records))
	for _, item := range records {
		if item.Selected && item.Assignment.ShardIndex == shardIndex {
			selected = append(selected, item)
		}
	}
	featureDim := policy.Normalize.FeatureDim
	if featureDim != 4 {
		return nil, NewError(CodeRecordInvalid, "only feature_dim=4 is supported")
	}
	names := []string{"text_length_norm", "label_index_norm", "shard_key_hash", "timestamp_hour_norm"}
	nullMask := make([]bool, len(selected)*featureDim)
	body := make([]byte, len(selected)*featureDim*4)
	nullCount := 0

	for row, item := range selected {
		record := item.Assignment.Record
		values := make([]float32, featureDim)
		for col := 0; col < featureDim; col++ {
			maskIndex := row*featureDim + col
			switch col {
			case 0:
				if record.TextNull {
					nullMask[maskIndex] = true
					nullCount++
					values[col] = 0
				} else {
					values[col] = float32(NormalizeFloat(float64(record.EffectiveLength),
						policy.Normalize.TextLengthMean, policy.Normalize.TextLengthStd))
				}
			case 1:
				labelIndex := indexOf(policy.Cleaning.AllowedLabels, record.Label)
				denominator := float64(maxInt(len(policy.Cleaning.AllowedLabels)-1, 1))
				values[col] = float32(float64(labelIndex) / denominator)
			case 2:
				h := fnv.New64a()
				_, _ = h.Write([]byte(record.ShardKey))
				values[col] = float32(h.Sum64()>>11) / float32(uint64(1)<<53)
			case 3:
				values[col] = float32(float64(record.TimestampMS-policy.Normalize.EpochMS) / float64(3_600_000))
			}
		}
		for col, value := range values {
			offset := (row*featureDim + col) * 4
			binary.LittleEndian.PutUint32(body[offset:offset+4], math.Float32bits(value))
		}
	}

	header := TensorHeader{
		Format:       "ppt-tensor-v1",
		Version:      1,
		DType:        "float32",
		Shape:        []int{len(selected), featureDim},
		SampleCount:  len(selected),
		FeatureDim:   featureDim,
		FeatureNames: names,
		NullMask:     nullMask,
		NullCount:    nullCount,
		BodyDigest:   DigestBytes(body),
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return nil, NewError(CodeInternal, "tensor header encoding failed: "+err.Error())
	}
	var buffer bytes.Buffer
	buffer.WriteString(TensorMagic)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(headerJSON)))
	buffer.Write(length[:])
	buffer.Write(headerJSON)
	buffer.Write(body)
	fullBytes := buffer.Bytes()
	return &TensorArtifact{
		ShardIndex: shardIndex,
		Header:     header,
		Body:       body,
		Bytes:      fullBytes,
		Digest:     DigestBytes(fullBytes),
	}, nil
}

func (a TensorArtifact) Verify() error {
	if got := DigestBytes(a.Bytes); got != a.Digest {
		return NewError(CodeShardDigestMismatch, "artifact digest does not match canonical bytes",
			map[string]any{"shard_index": a.ShardIndex, "expected_digest": a.Digest, "actual_digest": got})
	}
	if got := DigestBytes(a.Body); got != a.Header.BodyDigest {
		return NewError(CodeShardDigestMismatch, "tensor body digest does not match header",
			map[string]any{"shard_index": a.ShardIndex, "expected_digest": a.Header.BodyDigest, "actual_digest": got})
	}
	return nil
}

func ParseTensorArtifact(data []byte) (*TensorHeader, []float32, error) {
	if len(data) < 8 || string(data[:4]) != TensorMagic {
		return nil, nil, fmt.Errorf("invalid tensor magic")
	}
	headerLength := binary.BigEndian.Uint32(data[4:8])
	if int(headerLength)+8 > len(data) {
		return nil, nil, fmt.Errorf("truncated tensor header")
	}
	var header TensorHeader
	if err := json.Unmarshal(data[8:8+headerLength], &header); err != nil {
		return nil, nil, err
	}
	body := data[8+headerLength:]
	if DigestBytes(body) != header.BodyDigest {
		return nil, nil, fmt.Errorf("body digest mismatch")
	}
	values := make([]float32, header.SampleCount*header.FeatureDim)
	for i := range values {
		bits := binary.LittleEndian.Uint32(body[i*4 : (i+1)*4])
		values[i] = math.Float32frombits(bits)
	}
	return &header, values, nil
}

func indexOf(values []string, target string) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
