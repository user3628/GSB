package pipeline

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"imretrace/internal/dataset"
	"imretrace/internal/model"
	"imretrace/internal/store"
)

type Service struct {
	Paths   model.Paths
	Store   *store.Store
	buildMu sync.Mutex
}

func NewService(root string) (*Service, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	paths := model.DefaultPaths(abs)
	for _, dir := range []string{paths.DataDir, paths.ConfigDir, paths.DemoDir, paths.ShardsDir, filepath.Dir(paths.LockPath), paths.ExportDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	st, err := store.Open(context.Background(), paths)
	if err != nil {
		return nil, err
	}
	return &Service{Paths: paths, Store: st}, nil
}

func (s *Service) Close() error { return s.Store.Close() }

func (s *Service) WritableCheck() error {
	f, err := os.CreateTemp(s.Paths.DataDir, ".write-check-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.WriteString("ok"); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *Service) EnsureDemo(ctx context.Context) ([]model.ManifestItem, error) {
	if err := s.WritableCheck(); err != nil {
		return nil, model.NewBiz(model.CodeInternal, "prepare", "", "workspace data dir is not writable: %v", err)
	}
	params := model.DefaultGeneratorParams()
	if b, err := os.ReadFile(s.Paths.ParamsPath); err == nil {
		_ = json.Unmarshal(b, &params)
	} else {
		b, _ := model.CanonicalBytes(params)
		_ = os.WriteFile(s.Paths.ParamsPath, append(b, '\n'), 0o644)
	}
	if _, err := os.Stat(s.Paths.Manifest); err != nil {
		if _, err := dataset.Generate(s.Paths, params); err != nil {
			return nil, err
		}
	}
	items, err := s.ImportManifestFile(ctx, s.Paths.Manifest)
	if err != nil {
		return nil, err
	}
	if policy, err := s.Store.CurrentPolicy(ctx); err != nil {
		return nil, err
	} else if policy == nil {
		p := model.DefaultPolicy()
		if _, _, err := s.Store.ActivatePolicy(ctx, p); err != nil {
			return nil, err
		}
		if err := s.WritePolicySnapshot(p); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func (s *Service) RegenerateDemo(ctx context.Context) ([]model.ManifestItem, error) {
	params := model.DefaultGeneratorParams()
	if b, err := os.ReadFile(s.Paths.ParamsPath); err == nil {
		_ = json.Unmarshal(b, &params)
	}
	_, err := dataset.Generate(s.Paths, params)
	if err != nil {
		return nil, err
	}
	return s.ImportManifestFile(ctx, s.Paths.Manifest)
}

func readJSONL(path string) ([]model.ManifestItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var items []model.ManifestItem
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var item model.ManifestItem
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			return nil, model.NewBiz(model.CodePolicyInvalid, "import", "", "invalid JSONL in %s: %v", path, err)
		}
		items = append(items, item)
	}
	return items, scanner.Err()
}

func (s *Service) ImportManifestFile(ctx context.Context, manifestPath string) ([]model.ManifestItem, error) {
	items, err := readJSONL(manifestPath)
	if err != nil {
		return nil, err
	}
	baseDir := filepath.Dir(manifestPath)
	hashes := map[string]string{}
	for i := range items {
		item := items[i]
		imgPath, err := s.resolveImagePath(item.ImageFile, baseDir)
		if err != nil {
			return nil, model.NewBiz(model.CodeFeatureInvalid, "import", item.ItemID, "resolve image: %v", err)
		}
		hash, err := FingerprintItem(item, imgPath)
		if err != nil {
			return nil, err
		}
		hashes[item.ItemID] = hash
	}
	if err := s.Store.UpsertItems(ctx, items, hashes); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Service) ImportPath(ctx context.Context, path string) ([]model.ManifestItem, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return s.ImportManifestFile(ctx, path)
	}
	var all []model.ManifestItem
	err = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".jsonl") {
			return nil
		}
		items, ierr := s.ImportManifestFile(ctx, p)
		if ierr != nil {
			return ierr
		}
		all = append(all, items...)
		return nil
	})
	return all, err
}

func (s *Service) resolveImagePath(imageFile string, extraDirs ...string) (string, error) {
	if filepath.IsAbs(imageFile) {
		if _, err := os.Stat(imageFile); err == nil {
			return imageFile, nil
		}
	}
	candidates := []string{}
	for _, dir := range extraDirs {
		candidates = append(candidates, filepath.Join(dir, imageFile))
	}
	candidates = append(candidates,
		filepath.Join(s.Paths.DemoDir, imageFile),
		filepath.Join(s.Paths.Root, imageFile),
		imageFile,
	)
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return filepath.Abs(candidate)
		}
	}
	return "", os.ErrNotExist
}

func (s *Service) ImagePath(item model.ManifestItem) (string, error) {
	return s.resolveImagePath(item.ImageFile, s.Paths.DemoDir)
}

func FingerprintItem(item model.ManifestItem, imagePath string) (string, error) {
	imgBytes, err := os.ReadFile(imagePath)
	if err != nil {
		return "", model.NewBiz(model.CodeFeatureInvalid, "import", item.ItemID, "read image: %v", err)
	}
	sum := sha256.Sum256(imgBytes)
	payload := struct {
		ItemID    string `json:"item_id"`
		Caption   string `json:"caption"`
		Lang      string `json:"lang"`
		Source    string `json:"source"`
		ImageFile string `json:"image_file"`
		ImageSHA  string `json:"image_sha256"`
	}{item.ItemID, item.Caption, item.Lang, item.Source, item.ImageFile, hex.EncodeToString(sum[:])}
	return model.HashBytes("input-fingerprint-v1:", model.MustCanonical(payload)), nil
}
