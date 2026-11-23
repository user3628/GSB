package probe

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"secverify/internal/model"
)

type AccountState struct {
	Name      string  `json:"name"`
	Objective string  `json:"objective"`
	Unit      string  `json:"unit"`
	Value     float64 `json:"value"`
}

type AuditState struct {
	Category         string  `json:"category"`
	Enabled          bool    `json:"enabled"`
	RetentionSeconds float64 `json:"retention_seconds"`
}

type ServiceState struct {
	ImagePath string `json:"image_path"`
	Name      string `json:"name"`
	StartType string `json:"start_type"`
}

type RegistryState struct {
	ACL  []model.ACLEntry `json:"acl"`
	Key  string           `json:"key"`
	Mode string           `json:"mode"`
}

type Prepared struct {
	Specs []Spec
	Paths map[string]string
}

// Materialize recreates the sandbox from the candidate state. Every user
// controlled path is validated before any write, so an escape attempt cannot
// leave partial files outside the sandbox.
func Materialize(root string, items []model.Item) (*Prepared, *model.BizError) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "cannot resolve sandbox root")
	}
	if err := os.RemoveAll(absRoot); err != nil {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "cannot reset sandbox")
	}
	if err := os.MkdirAll(absRoot, 0o700); err != nil {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "cannot create sandbox")
	}
	if err := os.MkdirAll(filepath.Join(absRoot, "tmp"), 0o700); err != nil {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "cannot create sandbox tmp")
	}

	sorted := append([]model.Item(nil), items...)
	model.SortItems(sorted)
	prepared := &Prepared{Paths: make(map[string]string, len(sorted)), Specs: make([]Spec, 0, len(sorted))}
	occupied := make(map[string]string, len(sorted)*2)
	for _, item := range sorted {
		rel, kind, bizErr := probePathForItem(item)
		if bizErr != nil {
			return nil, bizErr
		}
		if _, err := SecureJoin(absRoot, rel); err != nil {
			return nil, &model.BizError{
				Code:            model.CodeProbeEscape,
				ItemID:          item.ID,
				SourceOffset:    item.SourceOffset,
				Message:         "probe path rejected: " + err.Error(),
				Items:           []string{item.ID},
				FallbackVersion: -1,
			}
		}
		cleanRel := filepath.Clean(rel)
		key := filepath.ToSlash(cleanRel)
		if previous, ok := occupied[key]; ok && previous != item.ID {
			return nil, model.NewError(model.CodeConfigParse, item.ID, item.SourceOffset, "probe path collides with item "+previous)
		}
		occupied[key] = item.ID
		prepared.Paths[item.ID] = cleanRel
		prepared.Specs = append(prepared.Specs, Spec{ItemID: item.ID, Kind: kind, Path: cleanRel})
	}

	for _, item := range sorted {
		rel := prepared.Paths[item.ID]
		path, err := SecureJoin(absRoot, rel)
		if err != nil {
			return nil, &model.BizError{Code: model.CodeProbeEscape, ItemID: item.ID, SourceOffset: item.SourceOffset, Message: err.Error(), Items: []string{item.ID}, FallbackVersion: -1}
		}
		switch item.Kind {
		case model.KindAccount:
			if bizErr := writeAccountFile(path, item); bizErr != nil {
				return nil, bizErr
			}
		case model.KindAudit:
			if bizErr := writeAuditDirectory(path, item); bizErr != nil {
				return nil, bizErr
			}
		case model.KindService:
			if bizErr := writeServiceFile(path, item); bizErr != nil {
				return nil, bizErr
			}
		case model.KindRegistry:
			if bizErr := writeRegistryDirectory(path, item); bizErr != nil {
				return nil, bizErr
			}
		}
	}
	sort.Slice(prepared.Specs, func(i, j int) bool { return prepared.Specs[i].ItemID < prepared.Specs[j].ItemID })
	return prepared, nil
}

func probePathForItem(item model.Item) (string, string, *model.BizError) {
	switch item.Kind {
	case model.KindAccount:
		if item.Account == nil {
			return "", "", model.NewError(model.CodeConfigParse, item.ID, item.SourceOffset, "missing account payload")
		}
		return filepath.Join("account", slug(item.ID)+".json"), item.Kind, nil
	case model.KindAudit:
		if item.Audit == nil {
			return "", "", model.NewError(model.CodeConfigParse, item.ID, item.SourceOffset, "missing audit payload")
		}
		return item.Audit.Path, item.Kind, nil
	case model.KindService:
		if item.Service == nil {
			return "", "", model.NewError(model.CodeConfigParse, item.ID, item.SourceOffset, "missing service payload")
		}
		return filepath.Join("services", slug(item.ID)+".startup"), item.Kind, nil
	case model.KindRegistry:
		if item.Registry == nil {
			return "", "", model.NewError(model.CodeConfigParse, item.ID, item.SourceOffset, "missing registry payload")
		}
		return item.Registry.Path, item.Kind, nil
	default:
		return "", "", model.NewError(model.CodeConfigParse, item.ID, item.SourceOffset, "unsupported kind")
	}
}

func SpecForItem(item model.Item) (Spec, *model.BizError) {
	rel, kind, err := probePathForItem(item)
	if err != nil {
		return Spec{}, err
	}
	return Spec{ItemID: item.ID, Kind: kind, Path: filepath.Clean(rel)}, nil
}

func writeAccountFile(path string, item model.Item) *model.BizError {
	state := AccountState{
		Name:      item.Account.Name,
		Objective: item.Account.Objective,
		Unit:      item.Account.NormalizedUnit,
		Value:     item.Account.NormalizedValue,
	}
	data, _ := json.Marshal(state)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return probeWriteErr(item, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return probeWriteErr(item, err)
	}
	return nil
}

func writeAuditDirectory(path string, item model.Item) *model.BizError {
	mode := modelMode(item.Audit.Mode)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return probeWriteErr(item, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return probeWriteErr(item, err)
	}
	state := AuditState{Category: item.Audit.Category, Enabled: item.Audit.Enabled}
	if item.Audit.RetentionValue != nil {
		state.RetentionSeconds = *item.Audit.RetentionValue
	}
	data, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(path, "state.json"), append(data, '\n'), 0o600); err != nil {
		return probeWriteErr(item, err)
	}
	return nil
}

func writeServiceFile(path string, item model.Item) *model.BizError {
	state := ServiceState{ImagePath: item.Service.ImagePath, Name: item.Service.Name, StartType: item.Service.StartType}
	data, _ := json.Marshal(state)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return probeWriteErr(item, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return probeWriteErr(item, err)
	}
	return nil
}

func writeRegistryDirectory(path string, item model.Item) *model.BizError {
	mode := modelMode(item.Registry.Mode)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return probeWriteErr(item, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return probeWriteErr(item, err)
	}
	state := RegistryState{ACL: append([]model.ACLEntry(nil), item.Registry.ACL...), Key: item.Registry.Key, Mode: item.Registry.Mode}
	data, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(path, "acl.json"), append(data, '\n'), 0o600); err != nil {
		return probeWriteErr(item, err)
	}
	return nil
}

func probeWriteErr(item model.Item, err error) *model.BizError {
	message := err.Error()
	if pathErr, ok := err.(*os.PathError); ok {
		message = pathErr.Op + ": " + pathErr.Err.Error()
	}
	return model.NewError(model.CodeProbeExec, item.ID, item.SourceOffset, "sandbox materialization failed: "+message)
}

func modelMode(raw string) os.FileMode {
	var value uint32
	_, _ = fmt.Sscanf(raw, "%o", &value)
	return os.FileMode(value)
}

func slug(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:8])
}

func ExpectedAccountContent(item model.Item) string {
	state := AccountState{Name: item.Account.Name, Objective: item.Account.Objective, Unit: item.Account.NormalizedUnit, Value: item.Account.NormalizedValue}
	data, _ := json.Marshal(state)
	return string(append(data, '\n'))
}

func ExpectedAuditContent(item model.Item) string {
	state := AuditState{Category: item.Audit.Category, Enabled: item.Audit.Enabled}
	if item.Audit.RetentionValue != nil {
		state.RetentionSeconds = *item.Audit.RetentionValue
	}
	data, _ := json.Marshal(state)
	return string(append(data, '\n'))
}

func ExpectedServiceContent(item model.Item) string {
	state := ServiceState{ImagePath: item.Service.ImagePath, Name: item.Service.Name, StartType: item.Service.StartType}
	data, _ := json.Marshal(state)
	return string(append(data, '\n'))
}

func ExpectedRegistryContent(item model.Item) string {
	state := RegistryState{ACL: append([]model.ACLEntry(nil), item.Registry.ACL...), Key: item.Registry.Key, Mode: item.Registry.Mode}
	data, _ := json.Marshal(state)
	return string(append(data, '\n'))
}
