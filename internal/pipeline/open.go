package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/config"
	"ocrlab/internal/generator"
	"ocrlab/internal/model"
	"ocrlab/internal/store"
	"ocrlab/internal/workspace"
)

type Pipeline struct {
	paths          workspace.Paths
	cfg            config.Config
	store          *store.Store
	manifest       model.Manifest
	manifestDigest string
}

func Open(ctx context.Context, root string) (*Pipeline, error) {
	paths, err := workspace.New(root)
	if err != nil {
		return nil, apperr.Wrap(apperr.CodeStorage, "prepare workspace", err)
	}
	if _, err := os.Stat(paths.ConfigPath); os.IsNotExist(err) {
		if err := config.Save(paths.ConfigPath, config.Default()); err != nil {
			return nil, apperr.Wrap(apperr.CodeStorage, "write default config", err)
		}
	}
	cfg, err := config.Load(paths.ConfigPath)
	if err != nil {
		return nil, apperr.Wrap(apperr.CodeInvalidArgument, "load config", err)
	}
	if _, err := generator.Ensure(paths, false); err != nil {
		return nil, apperr.Wrap(apperr.CodeStorage, "prepare deterministic samples", err)
	}
	manifestBytes, err := os.ReadFile(paths.ManifestPath)
	if err != nil {
		return nil, apperr.Wrap(apperr.CodeStorage, "read manifest", err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, apperr.Wrap(apperr.CodeInvalidArgument, "parse manifest", err)
	}
	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	manifestDigest, err := canonical.Digest(manifest)
	if err != nil {
		return nil, err
	}
	stateStore, err := store.Open(paths)
	if err != nil {
		return nil, apperr.Wrap(apperr.CodeStorage, "open checkpoint store", err)
	}
	if _, err := stateStore.ImportManifest(ctx, manifest, manifestDigest); err != nil {
		stateStore.Close()
		return nil, apperr.Wrap(apperr.CodeStorage, "import manifest", err)
	}
	return &Pipeline{paths: paths, cfg: cfg, store: stateStore, manifest: manifest, manifestDigest: manifestDigest}, nil
}

func (p *Pipeline) Close() error { return p.store.Close() }

func (p *Pipeline) Config() config.Config { return p.cfg }

func (p *Pipeline) Manifest() model.Manifest { return p.manifest }

func (p *Pipeline) ManifestDigest() string { return p.manifestDigest }

func (p *Pipeline) Paths() workspace.Paths { return p.paths }

func (p *Pipeline) Store() *store.Store { return p.store }

func (p *Pipeline) Reimport(ctx context.Context) (store.ImportSummary, error) {
	if _, err := generator.Ensure(p.paths, false); err != nil {
		return store.ImportSummary{}, err
	}
	manifestBytes, err := os.ReadFile(p.paths.ManifestPath)
	if err != nil {
		return store.ImportSummary{}, err
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return store.ImportSummary{}, apperr.Wrap(apperr.CodeInvalidArgument, "parse manifest", err)
	}
	if err := validateManifest(manifest); err != nil {
		return store.ImportSummary{}, err
	}
	manifestDigest, err := canonical.Digest(manifest)
	if err != nil {
		return store.ImportSummary{}, err
	}
	summary, err := p.store.ImportManifest(ctx, manifest, manifestDigest)
	if err != nil {
		return store.ImportSummary{}, err
	}
	p.manifest = manifest
	p.manifestDigest = manifestDigest
	return summary, nil
}

func validateManifest(manifest model.Manifest) error {
	if manifest.SchemaVersion != model.SchemaVersion {
		return apperr.New(apperr.CodeInvalidArgument, "unsupported manifest schema: "+manifest.SchemaVersion)
	}
	groups := make(map[string]model.GroupSpec, len(manifest.Groups))
	groupShards := make(map[string]int)
	groupChars := make(map[string]int)
	for _, group := range manifest.Groups {
		if group.Name == "" {
			return apperr.New(apperr.CodeInvalidArgument, "manifest contains an empty group name")
		}
		if _, exists := groups[group.Name]; exists {
			return apperr.New(apperr.CodeInvalidArgument, "duplicate group: "+group.Name)
		}
		groups[group.Name] = group
	}
	ids := make(map[string]struct{}, len(manifest.Images))
	for _, imageSpec := range manifest.Images {
		if imageSpec.ImageID == "" {
			return apperr.New(apperr.CodeInvalidArgument, "manifest contains an empty image_id")
		}
		if _, exists := ids[imageSpec.ImageID]; exists {
			return apperr.WithImage(apperr.CodeInvalidArgument, "duplicate image_id", imageSpec.ImageID)
		}
		ids[imageSpec.ImageID] = struct{}{}
		if _, exists := groups[imageSpec.Group]; !exists {
			return apperr.WithImage(apperr.CodeInvalidArgument, "image references an undeclared group", imageSpec.ImageID)
		}
		if imageSpec.Width <= 0 || imageSpec.Height <= 0 {
			return apperr.WithImage(apperr.CodeInvalidArgument, "image dimensions must be positive", imageSpec.ImageID)
		}
		if imageSpec.File == "" {
			return apperr.WithImage(apperr.CodeInvalidArgument, "image file is empty", imageSpec.ImageID)
		}
		groupShards[imageSpec.Group]++
		seenTruth := make(map[int]struct{}, len(imageSpec.Truth))
		for _, truth := range imageSpec.Truth {
			if truth.Index <= 0 {
				return apperr.WithImage(apperr.CodeInvalidArgument, "truth box index must be positive", imageSpec.ImageID)
			}
			if _, exists := seenTruth[truth.Index]; exists {
				return apperr.WithImage(apperr.CodeInvalidArgument, "duplicate truth box index", imageSpec.ImageID)
			}
			seenTruth[truth.Index] = struct{}{}
			if truth.Text == "" {
				return apperr.WithImage(apperr.CodeInvalidArgument, "truth text is empty", imageSpec.ImageID)
			}
			if reason := normalizedBoxReason(truth.Box); reason != "" {
				return apperr.WithImage(apperr.CodeInvalidArgument, "invalid truth box: "+reason, imageSpec.ImageID)
			}
			groupChars[imageSpec.Group] += len([]rune(truth.Text))
		}
	}
	for name, group := range groups {
		if group.Observed.ShardCount != groupShards[name] {
			return apperr.New(apperr.CodeInvalidArgument, fmt.Sprintf("group %s observes %d shards but manifest contains %d", name, group.Observed.ShardCount, groupShards[name]))
		}
		if group.Observed.TotalCharacters != groupChars[name] {
			return apperr.New(apperr.CodeInvalidArgument, fmt.Sprintf("group %s observes %d characters but truth declares %d", name, group.Observed.TotalCharacters, groupChars[name]))
		}
	}
	return nil
}

func normalizedBoxReason(rect model.Rect) string {
	if rect.X < 0 || rect.Y < 0 || rect.W <= 0 || rect.H <= 0 || rect.X+rect.W > 1.0000001 || rect.Y+rect.H > 1.0000001 {
		return "truth rectangle is outside normalized image bounds or has zero width/height"
	}
	return ""
}

func (p *Pipeline) GroupForImage(imageID string) (model.GroupSpec, error) {
	imageSpec, ok := p.manifest.Image(imageID)
	if !ok {
		return model.GroupSpec{}, apperr.WithImage(apperr.CodeNotFound, "image is absent from manifest", imageID)
	}
	group, ok := p.manifest.Group(imageSpec.Group)
	if !ok {
		return model.GroupSpec{}, apperr.WithImage(apperr.CodeInvalidArgument, "image group is absent from manifest", imageID)
	}
	return group, nil
}

func (p *Pipeline) OrderedImageRecords(ctx context.Context, groups []string) ([]store.ImageRecord, error) {
	filter := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if _, exists := p.manifest.Group(group); !exists {
			return nil, apperr.New(apperr.CodeInvalidArgument, "unknown group: "+group)
		}
		filter[group] = struct{}{}
	}
	records, err := p.store.ListImages(ctx, nil)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]store.ImageRecord, len(records))
	for _, record := range records {
		byID[record.ImageID] = record
	}
	ordered := make([]store.ImageRecord, 0, len(records))
	for _, spec := range p.manifest.Images {
		if len(filter) > 0 {
			if _, keep := filter[spec.Group]; !keep {
				continue
			}
		}
		record, exists := byID[spec.ImageID]
		if !exists {
			return nil, apperr.WithImage(apperr.CodeNotFound, "imported image record is missing", spec.ImageID)
		}
		ordered = append(ordered, record)
	}
	return ordered, nil
}

func copyFile(source, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(target, data, 0o644)
}
