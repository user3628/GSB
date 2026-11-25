package workspace

import (
	"fmt"
	"os"
	"path/filepath"
)

type Paths struct {
	Root         string
	DataDir      string
	ImagesDir    string
	ManifestPath string
	ConfigPath   string
	StateDir     string
	DBPath       string
	LockPath     string
	ArtifactsDir string
	ReportsDir   string
}

func New(root string) (Paths, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return Paths{}, err
	}
	p := Paths{
		Root:         abs,
		DataDir:      filepath.Join(abs, "data"),
		ImagesDir:    filepath.Join(abs, "data", "images"),
		ManifestPath: filepath.Join(abs, "data", "manifest.json"),
		ConfigPath:   filepath.Join(abs, "config.json"),
		StateDir:     filepath.Join(abs, "state"),
		DBPath:       filepath.Join(abs, "state", "state.db"),
		LockPath:     filepath.Join(abs, "state", "publish.lock"),
		ArtifactsDir: filepath.Join(abs, "state", "artifacts"),
		ReportsDir:   filepath.Join(abs, "reports"),
	}
	if err := os.MkdirAll(p.ImagesDir, 0o755); err != nil {
		return Paths{}, err
	}
	if err := os.MkdirAll(p.StateDir, 0o755); err != nil {
		return Paths{}, err
	}
	if err := os.MkdirAll(p.ArtifactsDir, 0o755); err != nil {
		return Paths{}, err
	}
	if err := os.MkdirAll(p.ReportsDir, 0o755); err != nil {
		return Paths{}, err
	}
	return p, nil
}

func (p Paths) ImagePath(rel string) (string, error) {
	path := filepath.Join(p.DataDir, filepath.FromSlash(rel))
	relative, err := filepath.Rel(p.DataDir, path)
	if err != nil {
		return "", err
	}
	if relative == ".." || filepath.IsAbs(relative) || len(relative) >= 3 && relative[:3] == ".."+string(filepath.Separator) {
		return "", fmt.Errorf("image path escapes data directory: %s", rel)
	}
	return path, nil
}

func (p Paths) ArtifactPath(group, imageID, stage string) string {
	return filepath.Join(p.ArtifactsDir, group, imageID, stage+".json")
}
