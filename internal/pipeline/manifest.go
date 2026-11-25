package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"

	"voiceline/internal/canonical"
	"voiceline/internal/model"
	"voiceline/internal/wav"
)

type SegmentInput struct {
	Manifest     *model.Manifest
	Segment      model.SegmentManifest
	SegmentIndex int
	WAVBytes     []byte
	PCM          wav.PCM16
	Fingerprint  string
	ManifestDir  string
}

func LoadManifest(path string) (*model.Manifest, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	var manifest model.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, "", err
	}
	return &manifest, filepath.Dir(abs), nil
}

func ReadSegmentInput(manifestPath string, segmentIndex int) (*SegmentInput, error) {
	manifest, dir, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if segmentIndex < 0 || segmentIndex >= len(manifest.Segments) {
		return nil, model.NewError(model.CodeNotFound, "", "segment index %d not found", segmentIndex)
	}
	segment := manifest.Segments[segmentIndex]
	wavPath := segment.WAVPath
	if !filepath.IsAbs(wavPath) {
		wavPath = filepath.Join(dir, wavPath)
	}
	pcm, wavBytes, err := wav.ReadFile(wavPath)
	if err != nil {
		return nil, err
	}
	return &SegmentInput{
		Manifest:     manifest,
		Segment:      segment,
		SegmentIndex: segmentIndex,
		WAVBytes:     wavBytes,
		PCM:          pcm,
		Fingerprint:  canonical.Bytes(wavBytes),
		ManifestDir:  dir,
	}, nil
}
