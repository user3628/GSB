package samples

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed *.json increments/*.json
var content embed.FS

type Info struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Kind        string `json:"kind"`
}

var catalog = []Info{
	{Name: "full-snapshot.json", Description: "Valid version 1 snapshot with normal and boundary resources", Kind: "snapshot"},
	{Name: "boundary-weight-snapshot.json", Description: "Valid snapshot with zero resource demand and boundary 0/1000 weights", Kind: "snapshot"},
	{Name: "components.json", Description: "Deterministic component declarations", Kind: "component-list"},
	{Name: "edges.json", Description: "Deterministic dependency edge list", Kind: "edge-list"},
	{Name: "increments/resource-adjust.json", Description: "Single-component resource adjustment", Kind: "increment"},
	{Name: "increments/probe-adjust.json", Description: "Probe parameter change", Kind: "increment"},
	{Name: "increments/edge-add.json", Description: "Dependency edge addition", Kind: "increment"},
	{Name: "increments/cycle-add.json", Description: "Dependency cycle rejection with partial ordering", Kind: "increment"},
	{Name: "increments/stale-base.json", Description: "Stale base version rejection", Kind: "increment"},
	{Name: "invalid-negative-resource.json", Description: "Negative resource value rejection", Kind: "invalid"},
	{Name: "invalid-overflow-resource.json", Description: "Integer overflow rejection", Kind: "invalid"},
	{Name: "invalid-missing-dependency.json", Description: "Missing dependency reference rejection", Kind: "invalid"},
	{Name: "invalid-weight.json", Description: "Health and resource weights that do not sum to 1000", Kind: "invalid"},
	{Name: "drill-request.json", Description: "Recovery drill covering success, refusal, timeout and rejection", Kind: "drill"},
}

func Catalog() []Info {
	out := make([]Info, len(catalog))
	copy(out, catalog)
	return out
}

func Find(name string) (Info, bool) {
	for _, item := range catalog {
		if item.Name == name {
			return item, true
		}
	}
	return Info{}, false
}

func Read(name string) ([]byte, error) {
	clean := strings.TrimPrefix(strings.ReplaceAll(name, "\\", "/"), "/")
	if clean == "" || strings.Contains(clean, "..") {
		return nil, fmt.Errorf("invalid sample name")
	}
	data, err := content.ReadFile(clean)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func Names() ([]string, error) {
	var out []string
	err := fs.WalkDir(content, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".json") {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func JSON(name string) (json.RawMessage, error) {
	data, err := Read(name)
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return data, nil
}
