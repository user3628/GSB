package fsx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"hostacl-workbench/internal/apperr"
	"hostacl-workbench/internal/model"
)

type Materializer struct {
	Root     string
	Platform string
}

type rootMarker struct {
	Schema  int    `json:"schema"`
	Product string `json:"product"`
}

func New(root string) (*Materializer, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("managed root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	root = filepath.Clean(abs)
	if err := os.MkdirAll(root, 0o711); err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0o711); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	markerPath := filepath.Join(root, ".workbench-root.json")
	if _, err := os.Stat(markerPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if len(entries) != 0 {
			return nil, fmt.Errorf("managed root must be empty or contain a workbench marker: %s", root)
		}
		raw, _ := json.Marshal(rootMarker{Schema: 1, Product: "hostacl-workbench"})
		if err := os.WriteFile(markerPath, append(raw, '\n'), 0o600); err != nil {
			return nil, err
		}
	}
	treesRoot := filepath.Join(root, "trees")
	if err := os.MkdirAll(treesRoot, 0o711); err != nil {
		return nil, err
	}
	if err := os.Chmod(treesRoot, 0o711); err != nil {
		return nil, err
	}
	return &Materializer{Root: root, Platform: platformName()}, nil
}

func (m *Materializer) Materialize(ctx context.Context, policy *model.Policy, views []model.EffectiveView, intentID, finalName string) (model.ApplyReport, string, error) {
	if err := validateTreeName(finalName); err != nil {
		return model.ApplyReport{}, "", err
	}
	select {
	case <-ctx.Done():
		return model.ApplyReport{}, "", ctx.Err()
	default:
	}
	staging := filepath.Join(m.Root, "trees", ".stage-"+intentID)
	final := filepath.Join(m.Root, "trees", finalName)
	if _, err := os.Stat(final); err == nil {
		return model.ApplyReport{}, "", fmt.Errorf("target tree already exists: %s", final)
	}
	if err := os.RemoveAll(staging); err != nil {
		return model.ApplyReport{}, "", err
	}
	if err := os.MkdirAll(staging, 0o711); err != nil {
		return model.ApplyReport{}, "", err
	}
	if err := os.Chmod(staging, 0o711); err != nil {
		return model.ApplyReport{}, "", err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(staging)
		}
	}()

	if err := createManagedNodes(staging, policy); err != nil {
		return model.ApplyReport{}, "", err
	}
	expected := model.ExpectedAccess(policy, views)
	digests, err := applyPlatformACL(staging, policy, expected)
	if err != nil {
		return model.ApplyReport{}, "", err
	}
	report := model.ApplyReport{
		Version: policy.Version, TreePath: final, Platform: platformName(),
		Paths: make([]model.PathMaterialization, 0, len(policy.Paths)),
	}
	for _, managed := range policy.Paths {
		report.Paths = append(report.Paths, model.PathMaterialization{
			Path: managed.Path, Kind: managed.Kind,
			Expected: expected[managed.Path], ReadbackDigest: digests[managed.Path],
		})
	}
	sort.Slice(report.Paths, func(i, j int) bool { return report.Paths[i].Path < report.Paths[j].Path })
	report.ReadbackDigest = aggregateDigest(digests)
	if err := os.Rename(staging, final); err != nil {
		return model.ApplyReport{}, "", err
	}
	cleanup = false
	report.TreePath = final
	return report, final, nil
}

func createManagedNodes(tree string, policy *model.Policy) error {
	for _, managed := range policy.Paths {
		target := filepath.Join(tree, filepath.FromSlash(managed.Path))
		if err := ensureInside(tree, target); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o711); err != nil {
			return err
		}
		switch managed.Kind {
		case "directory":
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
		case "file":
			content, _ := json.Marshal(map[string]any{"managed_path": managed.Path, "policy_version": policy.Version})
			file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			if _, err := file.Write(append(content, '\n')); err != nil {
				_ = file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		}
	}
	// Container directories that are not managed paths still need traverse-only
	// semantics for every named principal.
	return filepath.Walk(tree, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != tree {
			return os.Chmod(path, 0o711)
		}
		return nil
	})
}

func ensureInside(root, target string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("managed path escapes root: %s", target)
	}
	return nil
}

func validateTreeName(name string) error {
	if name == "" || strings.ContainsAny(name, `/\\`) || name == "." || name == ".." {
		return fmt.Errorf("invalid tree name %q", name)
	}
	return nil
}

func aggregateDigest(digests map[string]string) string {
	keys := make([]string, 0, len(digests))
	for key := range digests {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+digests[key])
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (m *Materializer) Open(ctx context.Context, tree, relativePath, kind, subjectID, mode string) (actual string, kernelDenied bool, engine, identity string, err error) {
	select {
	case <-ctx.Done():
		return "error", false, platformName(), "", ctx.Err()
	default:
	}
	target := filepath.Join(tree, filepath.FromSlash(relativePath))
	if err := ensureInside(tree, target); err != nil {
		return "error", false, platformName(), "", err
	}
	return openAsPlatformIdentity(ctx, tree, target, kind, subjectID, mode)
}

func (m *Materializer) KernelVerificationAvailable() error { return platformVerificationAvailable() }

func (m *Materializer) Cleanup() error {
	root := filepath.Clean(m.Root)
	if root == string(filepath.Separator) || root == "." || root == "" {
		return fmt.Errorf("refusing to clean unsafe root %q", root)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == ".workbench-root.json" || entry.Name() == "trees" {
			if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyMismatch(path string, differences any) error {
	return apperr.With(apperr.ApplyMismatch, "filesystem access control readback differs from the materialized policy", map[string]any{
		"path": path, "differences": differences,
	})
}
