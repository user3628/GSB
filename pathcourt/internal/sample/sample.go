// Package sample builds the deterministic fixture tree, reparse points and
// path manifests inside an isolated temporary working directory. Nothing is
// sourced from the network or from real accounts; every artefact derives
// from the fixed Specification below so repeated runs are byte-identical.
package sample

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"pathcourt/internal/reparse"
)

// Scenario selects which fault-injection fixture set is materialized.
type Scenario string

const (
	ScenarioBaseline   Scenario = "baseline"
	ScenarioEscape     Scenario = "escape"
	ScenarioCycle      Scenario = "cycle"
	ScenarioDepth      Scenario = "depth"
	ScenarioDangling   Scenario = "dangling"
	ScenarioSnapshot   Scenario = "snapshot-gap"
)

// ParseScenario validates a scenario string.
func ParseScenario(s string) (Scenario, error) {
	switch Scenario(s) {
	case ScenarioBaseline, ScenarioEscape, ScenarioCycle, ScenarioDepth, ScenarioDangling, ScenarioSnapshot:
		return Scenario(s), nil
	default:
		return "", errors.New("unknown scenario: " + s)
	}
}

// Manifest is one batch of paths resolved as a single snapshot unit.
type Manifest struct {
	ID    string   `json:"id"`
	Paths []string `json:"paths"`
}

// LinkSpec declares one reparse point relative to the working directory.
// Scenarios lists the fixture scenarios in which the link is created; an
// empty list means it belongs to every scenario (baseline fixture set).
type LinkSpec struct {
	Name       string        `json:"name"`
	Kind       reparse.Kind  `json:"kind"`
	Target     string        `json:"target"`
	Relative   bool          `json:"relative"`
	Scenarios  []Scenario    `json:"scenarios,omitempty"`
	EmulatedAs string        `json:"emulated_as,omitempty"`
}

// Workspace describes a generated working directory.
type Workspace struct {
	Dir          string
	Root         string
	Outside      string
	Manifests    []Manifest
	Links        []LinkSpec
	Capabilities reparse.Capabilities
	Notes        []string
}

const (
	rootDirName    = "restricted-root"
	outsideDirName = "outside-root"
	manifestDir    = "manifests"
)

// fixedDirs are always created below the restricted root.
var fixedDirs = []string{
	"app",
	"app/bin",
	"app/conf",
	"data",
	"data/records",
	"links",
	"links/rel",
	"deep",
}

// fixedFiles are created below the restricted root.
var fixedFiles = map[string]string{
	"app/bin/tool.exe":        "tool-binary\n",
	"app/conf/settings.json":  `{"name":"settings"}\n`,
	"data/records/r1.txt":     "record-1\n",
	"data/records/r2.txt":     "record-2\n",
	"plain.txt":               "plain-file\n",
}

// outsideFiles are created below the sibling outside-root directory.
var outsideFiles = map[string]string{
	"secret.txt": "outside-secret\n",
	"other/ok.txt": "outside-other\n",
}

// linkSpecs returns the full deterministic reparse set. Links without
// Scenarios are created in every run; scenario links are created only when
// their injection scenario is active.
func linkSpecs() []LinkSpec {
	return []LinkSpec{
		// In-tree symlink to a directory (all scenarios).
		{Name: "links/app-link", Kind: reparse.KindSymlink, Target: "../app", Relative: true},
		// Relative symlink with ".." to an in-tree file.
		{Name: "links/rel/r1-link", Kind: reparse.KindSymlink, Target: "../../data/records/r1.txt", Relative: true},
		// Junction to an in-tree directory.
		{Name: "links/data-junction", Kind: reparse.KindJunction, Target: "../data", Relative: true},
		// Symlink escaping the restricted root (target exists outside).
		{Name: "links/escape-link", Kind: reparse.KindSymlink, Target: "../../outside-root/secret.txt", Relative: true, Scenarios: []Scenario{ScenarioEscape, ScenarioSnapshot}},
		// Junction escaping the restricted root.
		{Name: "links/escape-junction", Kind: reparse.KindJunction, Target: "../../outside-root", Relative: true, Scenarios: []Scenario{ScenarioEscape}},
		// Dangling symlink whose target stays inside the root.
		{Name: "links/dangling-inside", Kind: reparse.KindSymlink, Target: "../data/records/missing.txt", Relative: true, Scenarios: []Scenario{ScenarioDangling}},
		// Dangling symlink whose target is outside and never created.
		{Name: "links/dangling-outside", Kind: reparse.KindSymlink, Target: "../../outside-root/never/created.txt", Relative: true, Scenarios: []Scenario{ScenarioDangling}},
		// Cycle pair: a -> b, b -> a.
		{Name: "links/cycle-a", Kind: reparse.KindSymlink, Target: "./cycle-b", Relative: true, Scenarios: []Scenario{ScenarioCycle}},
		{Name: "links/cycle-b", Kind: reparse.KindSymlink, Target: "./cycle-a", Relative: true, Scenarios: []Scenario{ScenarioCycle}},
		// Depth chain: d0 -> d1 -> ... -> d19 -> real file d20.
		{Name: "deep/d0", Kind: reparse.KindSymlink, Target: "./d1", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d1", Kind: reparse.KindSymlink, Target: "./d2", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d2", Kind: reparse.KindSymlink, Target: "./d3", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d3", Kind: reparse.KindSymlink, Target: "./d4", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d4", Kind: reparse.KindSymlink, Target: "./d5", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d5", Kind: reparse.KindSymlink, Target: "./d6", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d6", Kind: reparse.KindSymlink, Target: "./d7", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d7", Kind: reparse.KindSymlink, Target: "./d8", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d8", Kind: reparse.KindSymlink, Target: "./d9", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d9", Kind: reparse.KindSymlink, Target: "./d10", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d10", Kind: reparse.KindSymlink, Target: "./d11", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d11", Kind: reparse.KindSymlink, Target: "./d12", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d12", Kind: reparse.KindSymlink, Target: "./d13", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d13", Kind: reparse.KindSymlink, Target: "./d14", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d14", Kind: reparse.KindSymlink, Target: "./d15", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d15", Kind: reparse.KindSymlink, Target: "./d16", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d16", Kind: reparse.KindSymlink, Target: "./d17", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d17", Kind: reparse.KindSymlink, Target: "./d18", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d18", Kind: reparse.KindSymlink, Target: "./d19", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
		{Name: "deep/d19", Kind: reparse.KindSymlink, Target: "./d20", Relative: true, Scenarios: []Scenario{ScenarioDepth}},
	}
}

// depthTerminal is the real file at the end of the depth chain.
const depthTerminal = "deep/d20"

// manifestSpecs returns the deterministic manifests. Paths use mixed
// separators and ".." components on purpose.
func manifestSpecs() []Manifest {
	return []Manifest{
		{
			ID: "m01-baseline",
			Paths: []string{
				`app/bin/tool.exe`,
				`app/../data/records/r1.txt`,
				`app/conf/../conf/settings.json`,
				`links/app-link/bin/tool.exe`,
				`links/data-junction/records/r1.txt`,
				`links/rel/r1-link`,
				`./plain.txt`,
				`data/records/../../plain.txt`,
				`APP/BIN/TOOL.EXE`,
			},
		},
		{
			ID: "m02-traversal",
			Paths: []string{
				`../outside-root/secret.txt`,
				`../outside-root/../outside-root/secret.txt`,
				`data/../../outside-root/other/ok.txt`,
				`links/escape-link`,
				`links/escape-junction/secret.txt`,
			},
		},
		{
			ID: "m03-missing",
			Paths: []string{
				`data/records/missing.txt`,
				`app/bin/tool.exe/extra`,
				`links/dangling-inside`,
				`links/dangling-outside`,
			},
		},
		{
			ID: "m04-cycle",
			Paths: []string{
				`links/cycle-a`,
				`links/cycle-b/inside`,
			},
		},
		{
			ID: "m05-depth",
			Paths: []string{
				`deep/d0`,
			},
		},
	}
}

// Generate materializes the fixture tree under workDir. Scenario-specific
// links are always defined in the spec but only created when active matches,
// so baseline manifests stay deterministic for every scenario.
func Generate(workDir string, active Scenario, factory reparse.Factory) (*Workspace, error) {
	if _, err := ParseScenario(string(active)); err != nil {
		return nil, err
	}
	root := filepath.Join(workDir, rootDirName)
	outside := filepath.Join(workDir, outsideDirName)
	for _, d := range fixedDirs {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(d)), 0o755); err != nil {
			return nil, err
		}
	}
	for f, content := range fixedFiles {
		path := filepath.Join(root, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return nil, err
		}
	}
	for f, content := range outsideFiles {
		path := filepath.Join(outside, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Join(workDir, manifestDir), 0o755); err != nil {
		return nil, err
	}

	ws := &Workspace{
		Dir:          workDir,
		Root:         root,
		Outside:      outside,
		Capabilities: factory.Capabilities(),
		Notes:        []string{},
	}

	for _, spec := range linkSpecs() {
		if len(spec.Scenarios) > 0 && !containsScenario(spec.Scenarios, active) {
			continue
		}
		created, note, err := createLink(factory, root, spec)
		if err != nil {
			return nil, err
		}
		if created {
			ws.Links = append(ws.Links, spec)
			if note != "" {
				ws.Notes = append(ws.Notes, note)
			}
		}
	}

	// Terminal file for the depth chain.
	if active == ScenarioDepth {
		terminal := filepath.Join(root, filepath.FromSlash(depthTerminal))
		if err := os.MkdirAll(filepath.Dir(terminal), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(terminal, []byte("depth-terminal\n"), 0o644); err != nil {
			return nil, err
		}
	}

	ws.Manifests = manifestSpecs()
	for _, m := range ws.Manifests {
		data, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(workDir, manifestDir, m.ID+".json"), append(data, '\n'), 0o644); err != nil {
			return nil, err
		}
	}
	return ws, nil
}

func containsScenario(set []Scenario, want Scenario) bool {
	for _, sc := range set {
		if sc == want {
			return true
		}
	}
	return false
}

func createLink(factory reparse.Factory, root string, spec LinkSpec) (bool, string, error) {
	linkPath := filepath.Join(root, filepath.FromSlash(spec.Name))
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		return false, "", err
	}
	linkDir := filepath.Dir(linkPath)
	targetPath := spec.Target
	if spec.Relative {
		// Resolve relative targets against the link directory without
		// letting Join collapse the ".." components early.
		targetPath = filepath.Clean(linkDir + string(filepath.Separator) + filepath.FromSlash(spec.Target))
	} else {
		targetPath = filepath.Clean(targetPath)
	}

	useJunctionFallback := spec.Kind == reparse.KindSymlink && !factory.SupportsSymlinks() && factory.SupportsJunctions()
	if spec.Kind == reparse.KindJunction && !factory.SupportsJunctions() {
		return false, "skipping junction " + spec.Name + ": host lacks junction support", nil
	}

	if useJunctionFallback {
		// Junction targets must be absolute and existing directories.
		if fi, err := os.Stat(targetPath); err == nil && fi.IsDir() {
			if err := factory.Junction(linkPath, targetPath); err != nil {
				return false, "", err
			}
			spec.EmulatedAs = "junction"
			return true, "symlink " + spec.Name + " emulated as junction (no symlink privilege)", nil
		}
		return false, "skipping file/dangling symlink " + spec.Name + ": no symlink privilege and target is not a directory", nil
	}

	switch spec.Kind {
	case reparse.KindJunction:
		// Junction targets are resolved from the link directory just like
		// relative symlink targets; targetPath already carries that result.
		if err := factory.Junction(linkPath, targetPath); err != nil {
			return false, "", err
		}
	default:
		// Relative targets keep their declared spelling for the audit
		// trail; the factory translates separators for the host kernel.
		target := spec.Target
		if !spec.Relative {
			target = targetPath
		}
		if err := factory.Symlink(linkPath, target); err != nil {
			return false, "", err
		}
	}
	return true, "", nil
}
