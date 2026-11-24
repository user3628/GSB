package formats

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"supplyaudit/internal/model"
)

// Discover walks a directory and returns recognized manifests sorted by report
// path. Report paths are relative and slash-normalized so reports remain byte
// stable across different temporary work directories.
func Discover(root string) ([]model.SourceSpec, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("input path %s is not a directory", root)
	}
	specs := []model.SourceSpec{}
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		format := Detect(path, data)
		if format == "" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		specs = append(specs, model.SourceSpec{
			ID:         string(format) + ":" + relative,
			Format:     format,
			Path:       path,
			ReportPath: relative,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].ReportPath != specs[j].ReportPath {
			return specs[i].ReportPath < specs[j].ReportPath
		}
		return specs[i].ID < specs[j].ID
	})
	return specs, nil
}

// SpecFromFile infers a single source. reportRoot, when non-empty, controls
// the stable display path used in reports.
func SpecFromFile(path, reportRoot string) (model.SourceSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return model.SourceSpec{}, err
	}
	format := Detect(path, data)
	if format == "" {
		return model.SourceSpec{}, fmt.Errorf("unable to detect source format for %s", path)
	}
	reportPath := filepath.Base(path)
	if reportRoot != "" {
		if relative, err := filepath.Rel(reportRoot, path); err == nil && !strings.HasPrefix(relative, "..") {
			reportPath = filepath.ToSlash(relative)
		}
	}
	return model.SourceSpec{
		ID:         string(format) + ":" + reportPath,
		Format:     format,
		Path:       path,
		ReportPath: filepath.ToSlash(reportPath),
	}, nil
}
