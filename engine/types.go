package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

const FormatVersion = "sampleengine.checkpoint.v1"

// Config is the deterministic feature, quantization and training configuration.
type Config struct {
	Dim           int      `json:"dim"`
	Units         int      `json:"units"`
	Epochs        int      `json:"epochs"`
	LRNum         int64    `json:"lr_num"`
	LRDen         int64    `json:"lr_den"`
	BaseScale     int64    `json:"base_scale"`
	ScoreScale    int64    `json:"score_scale"`
	MissingFields []string `json:"missing_fields"`
	Seed          int64    `json:"seed"`
}

func DefaultConfig() Config {
	return Config{
		Dim:           128,
		Units:         8,
		Epochs:        3,
		LRNum:         1,
		LRDen:         20,
		BaseScale:     1 << 20,
		ScoreScale:    1 << 30,
		MissingFields: []string{"text", "source"},
		Seed:          0x51A7E2,
	}
}

func (c Config) Normalize() Config {
	out := c
	if out.MissingFields == nil {
		out.MissingFields = []string{}
	}
	out.MissingFields = append([]string(nil), out.MissingFields...)
	sort.Strings(out.MissingFields)
	return out
}

func (c Config) Validate() error {
	if c.Dim == 0 {
		return newError(CodeHashDimZero, "hash dimension must not be zero")
	}
	if c.Dim < 2 {
		return newError(CodeHashDimInvalid, "hash dimension must be at least 2, got %d", c.Dim)
	}
	if c.Units <= 0 {
		return newError(CodeConfigInvalid, "specialization units must be positive")
	}
	if c.Units > c.Dim {
		return newError(CodeConfigInvalid, "units %d exceeds hash dimension %d", c.Units, c.Dim)
	}
	if c.Epochs <= 0 {
		return newError(CodeConfigInvalid, "epochs must be positive")
	}
	if c.LRNum <= 0 || c.LRDen <= 0 {
		return newError(CodeLearningRateInvalid, "learning rate must be positive: %d/%d", c.LRNum, c.LRDen)
	}
	if c.BaseScale <= 0 || c.ScoreScale <= 0 {
		return newError(CodeConfigInvalid, "fixed-point scales must be positive")
	}
	return nil
}

func (c Config) Digest() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	digest, _, err := DigestCanonical(c.Normalize())
	if err != nil {
		return "", err
	}
	return digest, nil
}

// SampleInput is one current sample value. Fields contains optional supplemental features.
type SampleInput struct {
	ID     string            `json:"id"`
	Text   string            `json:"text"`
	Label  int               `json:"label"`
	Fields map[string]string `json:"fields,omitempty"`
}

// StoredSample adds arrival order and revision metadata.
type StoredSample struct {
	SampleInput
	Seq           int64  `json:"seq"`
	BatchID       string `json:"batch_id"`
	BatchIndex    int    `json:"batch_index"`
	Revision      int64  `json:"revision"`
	ContentDigest string `json:"content_digest"`
	VectorDigest  string `json:"vector_digest"`
}

type Feature struct {
	Dim   int `json:"dim"`
	Value int `json:"value"`
}

type FeatureVector struct {
	SampleID       string    `json:"sample_id"`
	Dim            int       `json:"dim"`
	MissingBucket  int       `json:"missing_bucket"`
	Features       []Feature `json:"features"`
	MissingCount   int       `json:"missing_count"`
	CollisionCount int       `json:"collision_count"`
	Digest         string    `json:"digest"`
}

type DimState struct {
	Dim         int    `json:"dim"`
	UnitID      int    `json:"unit_id"`
	Scale       int64  `json:"scale"`
	Weight      int64  `json:"weight"`
	RefWeight   string `json:"ref_weight"`
	LastDelta   int64  `json:"last_delta"`
	UpdateCount int64  `json:"update_count"`
}

type UnitCheckpoint struct {
	UnitID      int        `json:"unit_id"`
	UnitVersion int64      `json:"unit_version"`
	StateDigest string     `json:"state_digest"`
	Dims        []DimState `json:"dims"`
}

type Checkpoint struct {
	Format        string           `json:"format"`
	Config        Config           `json:"config"`
	ConfigDigest  string           `json:"config_digest"`
	DatasetDigest string           `json:"dataset_digest"`
	ModelVersion  int64            `json:"model_version"`
	EventSequence int64            `json:"event_sequence"`
	Generation    int64            `json:"generation"`
	Units         []UnitCheckpoint `json:"units"`
	Fingerprint   string           `json:"fingerprint"`
}

type checkpointFingerprint struct {
	Format        string           `json:"format"`
	Config        Config           `json:"config"`
	ConfigDigest  string           `json:"config_digest"`
	DatasetDigest string           `json:"dataset_digest"`
	ModelVersion  int64            `json:"model_version"`
	EventSequence int64            `json:"event_sequence"`
	Generation    int64            `json:"generation"`
	Units         []UnitCheckpoint `json:"units"`
}

func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func DigestCanonical(v any) (string, []byte, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", nil, err
	}
	return DigestBytes(b), b, nil
}

func SampleContentDigest(in SampleInput) (string, error) {
	type payload struct {
		ID     string            `json:"id"`
		Text   string            `json:"text"`
		Label  int               `json:"label"`
		Fields map[string]string `json:"fields"`
	}
	fields := in.Fields
	if fields == nil {
		fields = map[string]string{}
	}
	digest, _, err := DigestCanonical(payload{ID: in.ID, Text: in.Text, Label: in.Label, Fields: fields})
	return digest, err
}

func DatasetDigest(samples []StoredSample) string {
	type item struct {
		Seq           int64  `json:"seq"`
		ID            string `json:"id"`
		ContentDigest string `json:"content_digest"`
	}
	items := make([]item, 0, len(samples))
	for _, s := range samples {
		items = append(items, item{Seq: s.Seq, ID: s.ID, ContentDigest: s.ContentDigest})
	}
	h, _, _ := DigestCanonical(items)
	return h
}

func (cp *Checkpoint) canonicalBody() ([]byte, error) {
	return CanonicalJSON(checkpointFingerprint{
		Format:        cp.Format,
		Config:        cp.Config,
		ConfigDigest:  cp.ConfigDigest,
		DatasetDigest: cp.DatasetDigest,
		ModelVersion:  cp.ModelVersion,
		EventSequence: cp.EventSequence,
		Generation:    cp.Generation,
		Units:         cp.Units,
	})
}

// RefreshFingerprint recomputes all redundant digests deterministically.
func (cp *Checkpoint) RefreshFingerprint() error {
	for i := range cp.Units {
		body, err := CanonicalJSON(struct {
			UnitID int        `json:"unit_id"`
			Dims   []DimState `json:"dims"`
		}{UnitID: cp.Units[i].UnitID, Dims: cp.Units[i].Dims})
		if err != nil {
			return err
		}
		cp.Units[i].StateDigest = DigestBytes(body)
	}
	body, err := cp.canonicalBody()
	if err != nil {
		return err
	}
	cp.Fingerprint = DigestBytes(body)
	return nil
}

func (cp *Checkpoint) Bytes() ([]byte, error) {
	if err := cp.RefreshFingerprint(); err != nil {
		return nil, err
	}
	return CanonicalJSON(cp)
}

func FirstByteDifference(a, b []byte) int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return int64(i)
		}
	}
	if len(a) != len(b) {
		return int64(n)
	}
	return -1
}
