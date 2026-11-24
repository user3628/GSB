package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// PlanTamper describes one injected anomaly, constructed purely via inputs.
type PlanTamper struct {
	Repo       string `json:"repo"`
	Generation int    `json:"generation"`
	Kind       string `json:"kind"` // object-digest | delete-object | drop-generation | replace-manifest
}

type PlanRepo struct {
	Name        string `json:"name"`
	Generations int    `json:"generations"`
}

// Plan is the deterministic sample-generation configuration.
type Plan struct {
	Repos   []PlanRepo   `json:"repos"`
	Tampers []PlanTamper `json:"tampers"`
}

// DefaultPlan yields clean repos plus the four input-level anomaly classes.
func DefaultPlan() *Plan {
	return &Plan{
		Repos: []PlanRepo{
			{Name: "alpha", Generations: 4},
			{Name: "beta", Generations: 4},
			{Name: "gamma", Generations: 4},
			{Name: "delta", Generations: 4},
			{Name: "epsilon", Generations: 4},
		},
		Tampers: []PlanTamper{
			{Repo: "beta", Generation: 3, Kind: "object-digest"},
			{Repo: "gamma", Generation: 2, Kind: "drop-generation"},
			{Repo: "delta", Generation: 2, Kind: "replace-manifest"},
			{Repo: "epsilon", Generation: 2, Kind: "delete-object"},
		},
	}
}

func repoDir(samplesDir, repo string) string { return filepath.Join(samplesDir, "repos", repo) }

func manifestPath(samplesDir, repo string, gen int) string {
	return filepath.Join(repoDir(samplesDir, repo), "manifests", fmt.Sprintf("gen-%06d.json", gen))
}

func objectPath(samplesDir, repo, digest string) string {
	return filepath.Join(repoDir(samplesDir, repo), "objects", digest)
}

func registryPath(samplesDir, repo string) string {
	return filepath.Join(repoDir(samplesDir, repo), "index.json")
}

// deterministicContent derives stable object bytes from repo/gen/path only.
func deterministicContent(repo string, gen int, path string) []byte {
	seed := fmt.Sprintf("%s/%06d/%s/content", repo, gen, path)
	return []byte("data:" + sha256Hex([]byte(seed))[:48])
}

func generationEntries(repo string, gen int) []Entry {
	var entries []Entry
	for i := 0; i < 3; i++ {
		path := fmt.Sprintf("data/file-%d.txt", i)
		content := deterministicContent(repo, gen, path)
		entries = append(entries, Entry{Path: path, Size: int64(len(content)), SHA256: sha256Hex(content)})
	}
	return entries
}

// generateRepo writes a fully honest repo (manifests, objects, registry).
func generateRepo(samplesDir string, rp PlanRepo) error {
	reg := &Registry{Repo: rp.Name}
	prevChain := genesisChain()
	for gen := 1; gen <= rp.Generations; gen++ {
		entries := generationEntries(rp.Name, gen)
		m := &Manifest{Repo: rp.Name, Generation: gen, PrevChainDigest: prevChain, Entries: entries}
		m.ManifestDigest = manifestDigest(m)
		chain := chainNext(prevChain, m.ManifestDigest)
		objDir := filepath.Join(repoDir(samplesDir, rp.Name), "objects")
		if err := os.MkdirAll(objDir, 0o755); err != nil {
			return err
		}
		for _, e := range entries {
			content := deterministicContent(rp.Name, gen, e.Path)
			if err := os.WriteFile(objectPath(samplesDir, rp.Name, e.SHA256), content, 0o644); err != nil {
				return err
			}
		}
		if err := os.MkdirAll(filepath.Dir(manifestPath(samplesDir, rp.Name, gen)), 0o755); err != nil {
			return err
		}
		raw, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(manifestPath(samplesDir, rp.Name, gen), raw, 0o644); err != nil {
			return err
		}
		reg.Generations = append(reg.Generations, RegistryEntry{Generation: gen, ManifestDigest: m.ManifestDigest, ChainDigest: chain})
		prevChain = chain
	}
	if err := os.MkdirAll(repoDir(samplesDir, rp.Name), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(registryPath(samplesDir, rp.Name), raw, 0o644)
}

// applyTamper mutates samples purely at the input level (never via source).
func applyTamper(samplesDir string, t PlanTamper) error {
	switch t.Kind {
	case "object-digest":
		entries := generationEntries(t.Repo, t.Generation)
		target := objectPath(samplesDir, t.Repo, entries[0].SHA256)
		return os.WriteFile(target, append(deterministicContent(t.Repo, t.Generation, entries[0].Path), []byte(":tampered")...), 0o644)
	case "delete-object":
		entries := generationEntries(t.Repo, t.Generation)
		return os.Remove(objectPath(samplesDir, t.Repo, entries[0].SHA256))
	case "drop-generation":
		reg, err := readRegistry(samplesDir, t.Repo)
		if err != nil {
			return err
		}
		kept := reg.Generations[:0]
		for _, g := range reg.Generations {
			if g.Generation != t.Generation {
				kept = append(kept, g)
			}
		}
		reg.Generations = kept
		raw, err := json.MarshalIndent(reg, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(registryPath(samplesDir, t.Repo), raw, 0o644); err != nil {
			return err
		}
		return os.Remove(manifestPath(samplesDir, t.Repo, t.Generation))
	case "replace-manifest":
		// Wholesale replacement: same generation number, rogue content,
		// self-consistent digest, but not the registered digest.
		rogue := &Manifest{Repo: t.Repo, Generation: t.Generation, PrevChainDigest: sha256Hex([]byte("rogue-prev"))}
		for i := 0; i < 2; i++ {
			path := fmt.Sprintf("rogue/file-%d.bin", i)
			content := []byte("rogue:" + sha256Hex([]byte(path))[:32])
			rogue.Entries = append(rogue.Entries, Entry{Path: path, Size: int64(len(content)), SHA256: sha256Hex(content)})
		}
		rogue.ManifestDigest = manifestDigest(rogue)
		raw, err := json.MarshalIndent(rogue, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(manifestPath(samplesDir, t.Repo, t.Generation), raw, 0o644)
	default:
		return fmt.Errorf("unknown tamper kind %q", t.Kind)
	}
}

// GenerateSamples builds the deterministic sample tree from a plan.
func GenerateSamples(samplesDir string, plan *Plan) error {
	if plan == nil {
		plan = DefaultPlan()
	}
	for _, rp := range plan.Repos {
		if err := generateRepo(samplesDir, rp); err != nil {
			return err
		}
	}
	for _, t := range plan.Tampers {
		if err := applyTamper(samplesDir, t); err != nil {
			return err
		}
	}
	return nil
}

func readRegistry(samplesDir, repo string) (*Registry, error) {
	raw, err := os.ReadFile(registryPath(samplesDir, repo))
	if err != nil {
		return nil, err
	}
	var reg Registry
	if err := json.Unmarshal(raw, &reg); err != nil {
		return nil, fmt.Errorf("registry corrupt: %v", err)
	}
	sort.Slice(reg.Generations, func(i, j int) bool { return reg.Generations[i].Generation < reg.Generations[j].Generation })
	return &reg, nil
}

// ListRepos returns repo names under the samples dir, stably sorted.
func ListRepos(samplesDir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(samplesDir, "repos"))
	if err != nil {
		return nil, err
	}
	var repos []string
	for _, e := range entries {
		if e.IsDir() {
			repos = append(repos, e.Name())
		}
	}
	sort.Strings(repos)
	return repos, nil
}
