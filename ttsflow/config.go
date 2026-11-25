package ttsflow

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// LayerSpec declares one fully connected layer. Its deterministic weights are
// derived from AcousticConfig.WeightSeed and the layer index.
type LayerSpec struct {
	In  int `json:"in" yaml:"in"`
	Out int `json:"out" yaml:"out"`
}

type AcousticConfig struct {
	EmbeddingDim      int         `json:"embedding_dim" yaml:"embedding_dim"`
	FrameShiftSamples int         `json:"frame_shift_samples" yaml:"frame_shift_samples"`
	Bands             int         `json:"bands" yaml:"bands"`
	Layers            []LayerSpec `json:"layers" yaml:"layers"`
	WeightSeed        int64       `json:"weight_seed" yaml:"weight_seed"`
	MinFramesPerUnit  int         `json:"min_frames_per_unit" yaml:"min_frames_per_unit"`
	MaxFramesPerUnit  int         `json:"max_frames_per_unit" yaml:"max_frames_per_unit"`
}

type WaveConfig struct {
	SampleRate int `json:"sample_rate" yaml:"sample_rate"`
	BitDepth   int `json:"bit_depth" yaml:"bit_depth"`
	Channels   int `json:"channels" yaml:"channels"`
}

type SplitConfig struct {
	FirstMinChars            int  `json:"first_min_chars" yaml:"first_min_chars"`
	FollowingChars           int  `json:"following_chars" yaml:"following_chars"`
	ForceAtPunctuation       bool `json:"force_at_punctuation" yaml:"force_at_punctuation"`
	CheckpointIntervalFrames int  `json:"checkpoint_interval_frames" yaml:"checkpoint_interval_frames"`
}

// FrontendConfig contains both the grapheme-to-phoneme table and the required
// phoneme-to-pronunciation-unit table. "common" is consulted after the sample
// language table and is intended for punctuation and symbols.
type FrontendConfig struct {
	Graphemes    map[string]map[string][]string `json:"graphemes" yaml:"graphemes"`
	PhonemeUnits map[string]string              `json:"phoneme_units" yaml:"phoneme_units"`
}

type Config struct {
	SchemaVersion int            `json:"schema_version" yaml:"schema_version"`
	Revision      int64          `json:"revision" yaml:"revision"`
	Seed          int64          `json:"seed" yaml:"seed"`
	Acoustic      AcousticConfig `json:"acoustic" yaml:"acoustic"`
	Wave          WaveConfig     `json:"wave" yaml:"wave"`
	Splitting     SplitConfig    `json:"splitting" yaml:"splitting"`
	Frontend      FrontendConfig `json:"frontend" yaml:"frontend"`
}

func LoadConfigFile(path string) (Config, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, nil, err
	}
	cfg, err := ParseConfig(raw)
	return cfg, raw, err
}

func ParseConfig(raw []byte) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, biz(CodeConfigInvalid, "cannot decode YAML configuration").withCause(err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.SchemaVersion != 1 {
		return biz(CodeConfigInvalid, fmt.Sprintf("schema_version must be 1, got %d", c.SchemaVersion))
	}
	if c.Revision < 1 {
		return biz(CodeConfigInvalid, "revision must be at least 1")
	}
	a := c.Acoustic
	if a.EmbeddingDim <= 0 || a.FrameShiftSamples <= 0 || a.Bands <= 0 {
		return biz(CodeConfigInvalid, "acoustic embedding_dim, frame_shift_samples and bands must be positive")
	}
	if a.MinFramesPerUnit <= 0 || a.MaxFramesPerUnit < a.MinFramesPerUnit {
		return biz(CodeConfigInvalid, "invalid min/max frames per unit")
	}
	if len(a.Layers) == 0 {
		return biz(CodeConfigInvalid, "acoustic layers must not be empty")
	}
	in := a.EmbeddingDim
	for i, layer := range a.Layers {
		if layer.In != in || layer.Out <= 0 {
			return biz(CodeConfigInvalid, fmt.Sprintf("acoustic layer %d must consume %d and have positive output", i, in))
		}
		in = layer.Out
	}
	if in != a.Bands+1 {
		return biz(CodeConfigInvalid, fmt.Sprintf("final acoustic layer output must be bands+1 (%d), got %d", a.Bands+1, in))
	}
	if c.Wave.SampleRate < 8000 || c.Wave.SampleRate > 192000 {
		return biz(CodeConfigInvalid, "wave sample_rate is outside the supported range")
	}
	if c.Wave.BitDepth != 16 {
		return biz(CodeConfigInvalid, "wave bit_depth must be 16")
	}
	if c.Wave.Channels != 1 && c.Wave.Channels != 2 {
		return biz(CodeConfigInvalid, "wave channels must be 1 or 2")
	}
	if c.Splitting.FirstMinChars < 1 || c.Splitting.FollowingChars < 1 {
		return biz(CodeConfigInvalid, "split character counts must be positive")
	}
	if c.Splitting.CheckpointIntervalFrames < 1 {
		return biz(CodeConfigInvalid, "checkpoint_interval_frames must be positive")
	}
	if len(c.Frontend.Graphemes) == 0 || len(c.Frontend.PhonemeUnits) == 0 {
		return biz(CodeConfigInvalid, "frontend graphemes and phoneme_units must not be empty")
	}
	for lang, table := range c.Frontend.Graphemes {
		if lang == "" {
			return biz(CodeConfigInvalid, "grapheme language key must not be empty")
		}
		for g, ps := range table {
			if g == "" || len(ps) == 0 {
				return biz(CodeConfigInvalid, fmt.Sprintf("empty grapheme mapping in language %s", lang))
			}
			for _, p := range ps {
				if strings.TrimSpace(p) == "" {
					return biz(CodeConfigInvalid, fmt.Sprintf("empty phoneme for grapheme %q", g))
				}
			}
		}
	}
	for p, u := range c.Frontend.PhonemeUnits {
		if strings.TrimSpace(p) == "" || strings.TrimSpace(u) == "" {
			return biz(CodeConfigInvalid, "phoneme_units contains an empty key or value")
		}
	}
	return nil
}

func (c Config) Digest() string { return digestJSON(c) }

func (c Config) AcousticDigest() string { return digestJSON(c.Acoustic) }

func (c Config) WaveDigest() string { return digestJSON(c.Wave) }

func (c Config) SplitDigest() string { return digestJSON(c.Splitting) }

func (c Config) GraphemeDigest() string { return digestJSON(c.Frontend.Graphemes) }

func (c Config) PhonemeUnitDigest() string { return digestJSON(c.Frontend.PhonemeUnits) }

// EncodeYAML returns the canonical YAML representation written to a workspace.
func (c Config) EncodeYAML() ([]byte, error) { return yaml.Marshal(c) }

func (c Config) PhonemesFor(grapheme, language string) ([]string, bool) {
	if table, ok := c.Frontend.Graphemes[language]; ok {
		if p, found := table[grapheme]; found {
			return append([]string(nil), p...), true
		}
	}
	if table, ok := c.Frontend.Graphemes["common"]; ok {
		if p, found := table[grapheme]; found {
			return append([]string(nil), p...), true
		}
	}
	return nil, false
}

func (c Config) UnitFor(phoneme string) (string, bool) {
	u, ok := c.Frontend.PhonemeUnits[phoneme]
	return u, ok
}

func (c Config) GraphemeLanguage(grapheme, sampleLanguage string) string {
	if _, ok := c.PhonemesFor(grapheme, sampleLanguage); ok {
		return sampleLanguage
	}
	if _, ok := c.PhonemesFor(grapheme, "common"); ok {
		return "common"
	}
	if sampleLanguage != "zh" {
		if _, ok := c.PhonemesFor(grapheme, "zh"); ok {
			return "zh"
		}
	}
	if sampleLanguage != "en" {
		if _, ok := c.PhonemesFor(grapheme, "en"); ok {
			return "en"
		}
	}
	return sampleLanguage
}

// TextSetDigest hashes a set independently of JSONL line ordering.
func TextSetDigest(samples []TextSample) string {
	cp := append([]TextSample(nil), samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].UTTID < cp[j].UTTID })
	return digestJSON(cp)
}

func (t TextSample) Digest() string { return digestJSON(t) }

func (t TextSample) ContentDigest() string {
	return digestParts(t.UTTID, t.Text, t.Language)
}

func (t TextSample) ControlDigest() string {
	return digestParts(t.SpeakerID, t.Prosody)
}

func (e *BizError) withCause(cause error) *BizError {
	e.Cause = cause
	return e
}
