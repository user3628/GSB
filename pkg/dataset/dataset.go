// Package dataset defines the local, deterministic input format used by the
// quantized operator runner.
package dataset

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"local/quantops/pkg/canon"
	"local/quantops/pkg/quant"
)

const (
	SchemaVersion = "quantops.dataset.v1"
	DefaultSeed   = int64(20261125)
)

type Manifest struct {
	SchemaVersion string  `json:"schema_version"`
	Name          string  `json:"name"`
	Seed          int64   `json:"seed"`
	Shards        []Shard `json:"shards"`
}

type Shard struct {
	ID                  string           `json:"id"`
	Ordinal             int              `json:"ordinal"`
	Description         string           `json:"description,omitempty"`
	Activation          TensorRef        `json:"activation"`
	Weights             []TensorRef      `json:"weights"`
	Chain               []Layer          `json:"chain"`
	ExpectedOutputShape []int            `json:"expected_output_shape"`
	Verification        VerificationSpec `json:"verification"`
}

type Layer struct {
	Name       string       `json:"name"`
	Activation quant.Config `json:"activation"`
	Weight     quant.Config `json:"weight"`
}

type TensorRef struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Shape []int  `json:"shape"`
}

type VerificationSpec struct {
	AbsoluteTolerance float64       `json:"absolute_tolerance"`
	RelativeTolerance float64       `json:"relative_tolerance"`
	SamplePoints      []SamplePoint `json:"sample_points"`
}

type SamplePoint struct {
	Layer int `json:"layer"`
	Row   int `json:"row"`
	Col   int `json:"col"`
}

// TensorFile is the on-disk representation. JSON cannot represent NaN, so
// non-finite positions carry explicit side-band metadata and a zero placeholder.
type TensorFile struct {
	SchemaVersion string    `json:"schema_version"`
	Name          string    `json:"name"`
	Shape         []int     `json:"shape"`
	Layout        string    `json:"layout"`
	Values        []float64 `json:"values"`
	NaNPositions  []int     `json:"nan_positions,omitempty"`
	ChannelScales []float64 `json:"channel_scales"`
}

type Loaded struct {
	Manifest     Manifest
	ManifestPath string
	Tensors      map[string]quant.Tensor
	TensorFiles  map[string]TensorFile
	Fingerprint  string
}

func Load(manifestPath string) (*Loaded, error) {
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	if m.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("unsupported dataset schema %q", m.SchemaVersion)
	}
	base := filepath.Dir(manifestPath)
	loaded := &Loaded{
		Manifest:     m,
		ManifestPath: manifestPath,
		Tensors:      make(map[string]quant.Tensor),
		TensorFiles:  make(map[string]TensorFile),
	}
	seen := make(map[string]string)
	refs := make([]TensorRef, 0)
	for _, sh := range m.Shards {
		refs = append(refs, sh.Activation)
		refs = append(refs, sh.Weights...)
	}
	for _, ref := range refs {
		path := filepath.Join(base, filepath.FromSlash(ref.Path))
		if prior, ok := seen[ref.Name]; ok && prior != path {
			return nil, fmt.Errorf("tensor name %q refers to two paths", ref.Name)
		}
		seen[ref.Name] = path
		if _, ok := loaded.Tensors[ref.Name]; ok {
			continue
		}
		tf, err := readTensor(path)
		if err != nil {
			return nil, fmt.Errorf("tensor %q: %w", ref.Name, err)
		}
		if tf.Name != "" && tf.Name != ref.Name {
			return nil, fmt.Errorf("tensor %q file names itself %q", ref.Name, tf.Name)
		}
		if !equalShape(tf.Shape, ref.Shape) {
			return nil, fmt.Errorf("tensor %q shape %v does not match reference %v", ref.Name, tf.Shape, ref.Shape)
		}
		values := append([]float64(nil), tf.Values...)
		for _, pos := range tf.NaNPositions {
			if pos < 0 || pos >= len(values) {
				return nil, fmt.Errorf("tensor %q NaN position %d out of range", ref.Name, pos)
			}
			values[pos] = math.NaN()
		}
		loaded.Tensors[ref.Name] = quant.NewTensor(tf.Shape, values)
		loaded.TensorFiles[ref.Name] = tf
	}
	if err := Validate(&m, loaded.Tensors); err != nil {
		return nil, err
	}
	fp, err := Fingerprint(&m, loaded.Tensors, loaded.TensorFiles)
	if err != nil {
		return nil, err
	}
	loaded.Fingerprint = fp
	return loaded, nil
}

func readTensor(path string) (TensorFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return TensorFile{}, err
	}
	var tf TensorFile
	if err := json.Unmarshal(b, &tf); err != nil {
		return TensorFile{}, err
	}
	if tf.SchemaVersion != SchemaVersion {
		return TensorFile{}, fmt.Errorf("unsupported tensor schema %q", tf.SchemaVersion)
	}
	if len(tf.Shape) != 2 || len(tf.Values) != tf.Shape[0]*tf.Shape[1] {
		return TensorFile{}, fmt.Errorf("invalid tensor shape %v / value count %d", tf.Shape, len(tf.Values))
	}
	if len(tf.ChannelScales) != tf.Shape[0] {
		return TensorFile{}, fmt.Errorf("channel_scales length %d does not match shape %v", len(tf.ChannelScales), tf.Shape)
	}
	return tf, nil
}

func Validate(m *Manifest, tensors map[string]quant.Tensor) error {
	if len(m.Shards) == 0 {
		return fmt.Errorf("manifest has no shards")
	}
	seenOrdinal := make(map[int]bool)
	seenID := make(map[string]bool)
	for _, sh := range m.Shards {
		if sh.ID == "" || sh.Ordinal <= 0 {
			return fmt.Errorf("shard must have a non-empty id and positive ordinal")
		}
		if seenID[sh.ID] {
			return fmt.Errorf("duplicate shard id %q", sh.ID)
		}
		if seenOrdinal[sh.Ordinal] {
			return fmt.Errorf("duplicate shard ordinal %d", sh.Ordinal)
		}
		seenID[sh.ID], seenOrdinal[sh.Ordinal] = true, true
		if len(sh.Chain) == 0 || len(sh.Weights) != len(sh.Chain) {
			return fmt.Errorf("shard %s must declare one weight per chain layer", sh.ID)
		}
		if sh.Verification.AbsoluteTolerance < 0 || sh.Verification.RelativeTolerance < 0 {
			return fmt.Errorf("shard %s has a negative tolerance", sh.ID)
		}
		act := tensors[sh.Activation.Name]
		curK := act.Shape[1]
		lastN := 0
		for i, layer := range sh.Chain {
			if err := layer.Activation.Validate(); err != nil {
				return fmt.Errorf("shard %s layer %d activation: %w", sh.ID, i, err)
			}
			if err := layer.Weight.Validate(); err != nil {
				return fmt.Errorf("shard %s layer %d weight: %w", sh.ID, i, err)
			}
			w := tensors[sh.Weights[i].Name]
			if w.Shape[1] != curK {
				return fmt.Errorf("shard %s layer %d inner dimension %d != %d", sh.ID, i, w.Shape[1], curK)
			}
			curK = w.Shape[0]
			lastN = w.Shape[0]
		}
		want := []int{act.Shape[0], lastN}
		if !equalShape(want, sh.ExpectedOutputShape) {
			return fmt.Errorf("shard %s expected output %v, chain yields %v", sh.ID, sh.ExpectedOutputShape, want)
		}
		for _, p := range sh.Verification.SamplePoints {
			if p.Layer < 0 || p.Layer >= len(sh.Chain) {
				return fmt.Errorf("shard %s sample layer %d out of range", sh.ID, p.Layer)
			}
			rows := act.Shape[0]
			cols := 0
			if p.Layer == 0 {
				cols = tensors[sh.Weights[0].Name].Shape[0]
			} else {
				cols = tensors[sh.Weights[p.Layer].Name].Shape[0]
			}
			if p.Row < 0 || p.Row >= rows || p.Col < 0 || p.Col >= cols {
				return fmt.Errorf("shard %s sample (%d,%d,%d) out of bounds", sh.ID, p.Layer, p.Row, p.Col)
			}
		}
	}
	return nil
}

type fingerprintView struct {
	SchemaVersion string                  `json:"schema_version"`
	Name          string                  `json:"name"`
	Seed          int64                   `json:"seed"`
	Shards        []Shard                 `json:"shards"`
	Tensors       []tensorFingerprintView `json:"tensors"`
}

type tensorFingerprintView struct {
	Name          string    `json:"name"`
	Shape         []int     `json:"shape"`
	Values        []float64 `json:"values"`
	NaNPositions  []int     `json:"nan_positions,omitempty"`
	ChannelScales []float64 `json:"channel_scales"`
}

// Fingerprint hashes logical content, excluding filesystem paths and JSON
// whitespace, so moving a workspace does not invalidate checkpoints.
func Fingerprint(m *Manifest, tensors map[string]quant.Tensor, files map[string]TensorFile) (string, error) {
	names := make([]string, 0, len(tensors))
	for name := range tensors {
		names = append(names, name)
	}
	sort.Strings(names)
	view := fingerprintView{SchemaVersion: m.SchemaVersion, Name: m.Name, Seed: m.Seed, Shards: m.Shards}
	for _, name := range names {
		t := tensors[name]
		f := files[name]
		vals := append([]float64(nil), t.Values...)
		var nans []int
		for i, v := range vals {
			if math.IsNaN(v) {
				nans = append(nans, i)
				vals[i] = 0
			}
		}
		view.Tensors = append(view.Tensors, tensorFingerprintView{
			Name: name, Shape: append([]int(nil), t.Shape...), Values: vals,
			NaNPositions: nans, ChannelScales: append([]float64(nil), f.ChannelScales...),
		})
	}
	b, err := canon.JSON(view)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (l *Loaded) Tensor(ref TensorRef) quant.Tensor {
	t := l.Tensors[ref.Name]
	return t.Clone()
}

func (l *Loaded) Shard(id string) (Shard, bool) {
	for _, sh := range l.Manifest.Shards {
		if sh.ID == id {
			return sh, true
		}
	}
	return Shard{}, false
}

func equalShape(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// LoadTensorFile reads one on-disk tensor and reconstructs its NaN positions.
func LoadTensorFile(path string) (TensorFile, quant.Tensor, error) {
	tf, err := readTensor(path)
	if err != nil {
		return TensorFile{}, quant.Tensor{}, err
	}
	values := append([]float64(nil), tf.Values...)
	for _, pos := range tf.NaNPositions {
		if pos < 0 || pos >= len(values) {
			return TensorFile{}, quant.Tensor{}, fmt.Errorf("NaN position %d out of range", pos)
		}
		values[pos] = math.NaN()
	}
	return tf, quant.NewTensor(tf.Shape, values), nil
}
