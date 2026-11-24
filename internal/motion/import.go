package motion

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"motionlab/internal/model"
)

type ImportBundle struct {
	Spec       model.SequenceSpec
	Initial    *Frame
	Background *Frame
	Truth      []*Frame
}

// ImportSequence validates and commits an externally supplied sequence into
// the same deterministic on-disk format used by the built-in generator.
func ImportSequence(dir string, in ImportBundle) (*Catalog, error) {
	spec := in.Spec
	if spec.SequenceID == "" || strings.ContainsAny(spec.SequenceID, `/\\`) || strings.Contains(spec.SequenceID, "..") {
		return nil, model.NewError(model.CodeValidation, "sequence_id must be a safe, non-empty path segment")
	}
	if spec.Width < 8 || spec.Height < 8 || spec.Width > 512 || spec.Height > 512 {
		return nil, model.NewError(model.CodeValidation, "imported frame dimensions must be between 8 and 512")
	}
	if spec.TotalFrames < 2 || len(in.Truth) != spec.TotalFrames {
		return nil, model.NewError(model.CodeValidation, "truth frame count must equal total_frames (>= 2)")
	}
	if spec.PredictSteps < 1 || spec.PredictSteps > spec.TotalFrames-1 {
		return nil, model.NewError(model.CodeValidation, "predict_steps must be within the truth timeline")
	}
	if spec.Branches < 1 || spec.Branches > 32 {
		return nil, model.NewError(model.CodeValidation, "branches must be between 1 and 32")
	}
	if spec.Scene.SquareRect.W < 1 || spec.Scene.BlockRect.W < 1 ||
		spec.Scene.SquareRect.X+spec.Scene.SquareRect.W > spec.Width ||
		spec.Scene.BlockRect.X+spec.Scene.BlockRect.W > spec.Width ||
		spec.Scene.SquareRect.Y+spec.Scene.SquareRect.H > spec.Height ||
		spec.Scene.BlockRect.Y+spec.Scene.BlockRect.H > spec.Height {
		return nil, model.NewError(model.CodeValidation, "scene layer rectangles must lie inside the canvas")
	}
	initial := in.Initial
	if initial == nil {
		initial = in.Truth[0]
	}
	background := in.Background
	if background == nil {
		background = in.Truth[0]
	}
	for i, f := range in.Truth {
		if f == nil || f.Width != spec.Width || f.Height != spec.Height {
			return nil, model.NewError(model.CodeValidation, fmt.Sprintf("truth frame %d has the wrong dimensions", i))
		}
	}
	if initial.Width != spec.Width || initial.Height != spec.Height || background.Width != spec.Width || background.Height != spec.Height {
		return nil, model.NewError(model.CodeValidation, "initial/background frame dimensions do not match the manifest")
	}
	var manifest model.Manifest
	if err := readJSON(filepath.Join(dir, ManifestFile), &manifest); err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	for _, existing := range manifest.Sequences {
		if existing.SequenceID == spec.SequenceID {
			e := model.NewError(model.CodePolicyConflict, "sequence already exists")
			e.SequenceID = spec.SequenceID
			return nil, e
		}
	}
	var index FramesIndex
	if err := readJSON(filepath.Join(dir, FramesFile), &index); err != nil {
		return nil, fmt.Errorf("read frame index: %w", err)
	}
	seqDir := filepath.Join(dir, SequencesDir, spec.SequenceID)
	truthDir := filepath.Join(seqDir, "truth")
	if err := os.MkdirAll(truthDir, 0o755); err != nil {
		return nil, err
	}
	if err := writePNG(filepath.Join(seqDir, "initial.png"), initial); err != nil {
		return nil, err
	}
	if err := writePNG(filepath.Join(seqDir, "background.png"), background); err != nil {
		return nil, err
	}
	records := make([]FrameRecord, 0, len(in.Truth))
	for i, frame := range in.Truth {
		rel := filepath.ToSlash(filepath.Join(SequencesDir, spec.SequenceID, "truth", fmt.Sprintf("%04d.png", i)))
		if err := writePNG(filepath.Join(dir, filepath.FromSlash(rel)), frame); err != nil {
			return nil, err
		}
		records = append(records, FrameRecord{Path: rel, Width: frame.Width, Height: frame.Height, SHA256: frame.SHA256})
	}
	manifest.Sequences = append(manifest.Sequences, spec)
	sort.Slice(manifest.Sequences, func(i, j int) bool { return manifest.Sequences[i].SequenceID < manifest.Sequences[j].SequenceID })
	index.Sequences = append(index.Sequences, SequenceFrames{
		SequenceID: spec.SequenceID, InitialSHA256: initial.SHA256,
		BackgroundSHA256: background.SHA256, Frames: records,
	})
	sort.Slice(index.Sequences, func(i, j int) bool { return index.Sequences[i].SequenceID < index.Sequences[j].SequenceID })
	if err := writeJSON(filepath.Join(dir, FramesFile), index); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(dir, ManifestFile), manifest); err != nil {
		return nil, err
	}
	return LoadCatalog(dir)
}

func SavePolicy(dir string, policy model.PolicySnapshot) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, PolicyFile), policy)
}
