package httpapi

import (
	"errors"
	"path"
	"sort"
	"strings"

	sampleassets "gatewayfreshness.local/workbench/samples"
)

func sampleNames() ([]string, error) {
	entries, err := sampleassets.FS.ReadDir("batches")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func sampleFile(name string) ([]byte, error) {
	name = path.Base(strings.TrimSpace(name))
	if name == "" || name == "." || name == "/" || !strings.HasSuffix(name, ".json") {
		return nil, errors.New("invalid sample name")
	}
	return sampleassets.FS.ReadFile("batches/" + name)
}
