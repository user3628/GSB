package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"dialogalign/internal/atomicfs"
	"dialogalign/internal/domain"
	"dialogalign/internal/generator"
)

func DataDir(root string) string { return filepath.Join(root, "data") }

func LoadDataset(root string) (domain.Dataset, error) {
	dataDir := DataDir(root)
	sessionPath := filepath.Join(dataDir, "sessions.jsonl")
	turns, err := generator.LoadTurns(sessionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return domain.Dataset{}, prerequisite("sessions", "missing sessions.jsonl; run the generate command first")
		}
		return domain.Dataset{}, err
	}
	if len(turns) == 0 {
		return domain.Dataset{}, prerequisite("sessions", "sessions.jsonl contains no turns")
	}
	if err := validateTurns(turns); err != nil {
		return domain.Dataset{}, err
	}
	var catalog domain.RoleCatalog
	if err := readJSON(filepath.Join(dataDir, "roles.json"), &catalog); err != nil {
		if os.IsNotExist(err) {
			return domain.Dataset{}, prerequisite("roles", "missing roles.json; run the generate command first")
		}
		return domain.Dataset{}, err
	}
	var config domain.WorkbenchConfig
	if err := readJSON(filepath.Join(dataDir, "config.json"), &config); err != nil {
		if os.IsNotExist(err) {
			return domain.Dataset{}, prerequisite("config", "missing config.json; run the generate command first")
		}
		return domain.Dataset{}, err
	}
	var active domain.ActiveMap
	if err := readJSON(filepath.Join(dataDir, "active-map.json"), &active); err != nil {
		if os.IsNotExist(err) {
			return domain.Dataset{}, prerequisite("upgrade", "missing active-map.json; run the generate command first")
		}
		return domain.Dataset{}, err
	}
	mapPaths, err := filepath.Glob(filepath.Join(dataDir, "role_map_*.json"))
	if err != nil {
		return domain.Dataset{}, err
	}
	sort.Strings(mapPaths)
	if len(mapPaths) == 0 {
		return domain.Dataset{}, prerequisite("upgrade", "no versioned role map files found")
	}
	maps := make(map[string]domain.RoleMap, len(mapPaths))
	for _, path := range mapPaths {
		var roleMap domain.RoleMap
		if err := readJSON(path, &roleMap); err != nil {
			return domain.Dataset{}, err
		}
		if roleMap.Version == "" {
			return domain.Dataset{}, domain.BizError(domain.CodeMapVersionConflict, "role map file has empty version: "+path)
		}
		if existing, ok := maps[roleMap.Version]; ok {
			a, _ := domain.CanonicalJSON(existing)
			b, _ := domain.CanonicalJSON(roleMap)
			if string(a) != string(b) {
				return domain.Dataset{}, domain.BizError(domain.CodeMapVersionConflict, "multiple different role maps declare version "+roleMap.Version)
			}
		}
		maps[roleMap.Version] = roleMap
	}
	activeMap, ok := maps[active.Version]
	if !ok {
		return domain.Dataset{}, &domain.BusinessError{Code: domain.CodeMapVersionConflict, Message: "active role map version is not available", Stage: "upgrade", Detail: active.Version}
	}
	contentHash, err := domain.Digest(turns)
	if err != nil {
		return domain.Dataset{}, err
	}
	mapDocs := make([]domain.RoleMap, 0, len(maps))
	for _, roleMap := range maps {
		mapDocs = append(mapDocs, roleMap)
	}
	sort.Slice(mapDocs, func(i, j int) bool { return mapDocs[i].Version < mapDocs[j].Version })
	mapHash, err := domain.Digest(mapDocs)
	if err != nil {
		return domain.Dataset{}, err
	}
	rolesHash, err := domain.Digest(catalog)
	if err != nil {
		return domain.Dataset{}, err
	}
	configHash, err := domain.Digest(config)
	if err != nil {
		return domain.Dataset{}, err
	}
	return domain.Dataset{
		Turns:       turns,
		Catalog:     catalog,
		Maps:        maps,
		ActiveMap:   activeMap,
		Config:      config,
		ContentHash: contentHash,
		MapFileHash: mapHash,
		RolesHash:   rolesHash,
		ConfigHash:  configHash,
	}, nil
}

func validateTurns(turns []domain.TurnInput) error {
	seen := map[string]map[int]struct{}{}
	for _, turn := range turns {
		if strings.TrimSpace(turn.SessionID) == "" {
			return domain.BizError(domain.CodeInvalidRequest, "session_id is required")
		}
		if turn.TurnIndex < 1 {
			return domain.ContextError(domain.CodeInvalidRequest, "turn_index must be positive", turn.SessionID, turn.TurnIndex, "")
		}
		if _, ok := seen[turn.SessionID]; !ok {
			seen[turn.SessionID] = map[int]struct{}{}
		}
		if _, duplicate := seen[turn.SessionID][turn.TurnIndex]; duplicate {
			return domain.ContextError(domain.CodeInvalidRequest, "duplicate session turn", turn.SessionID, turn.TurnIndex, "")
		}
		seen[turn.SessionID][turn.TurnIndex] = struct{}{}
	}
	for session, indices := range seen {
		maxTurn := 0
		for turn := range indices {
			if turn > maxTurn {
				maxTurn = turn
			}
		}
		for turn := 1; turn <= maxTurn; turn++ {
			if _, ok := indices[turn]; !ok {
				return domain.ContextError(domain.CodeInvalidRequest, "turn indices must be contiguous from 1", session, turn, "")
			}
		}
	}
	return nil
}

func GroupTurns(turns []domain.TurnInput) ([]string, map[string][]domain.TurnInput) {
	bySession := map[string][]domain.TurnInput{}
	for _, turn := range turns {
		bySession[turn.SessionID] = append(bySession[turn.SessionID], turn)
	}
	sessions := make([]string, 0, len(bySession))
	for session := range bySession {
		sessions = append(sessions, session)
	}
	sort.Strings(sessions)
	for _, session := range sessions {
		sort.Slice(bySession[session], func(i, j int) bool { return bySession[session][i].TurnIndex < bySession[session][j].TurnIndex })
	}
	return sessions, bySession
}

func readJSON(path string, v any) error {
	blob, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, v)
}

func prerequisite(stage, message string) *domain.BusinessError {
	return &domain.BusinessError{Code: domain.CodeMissingPrerequisite, Message: message, Stage: stage}
}

func SaveRoleMap(root string, roleMap domain.RoleMap) error {
	if roleMap.Version == "" {
		return domain.BizError(domain.CodeMapVersionConflict, "role map version is required")
	}
	path := filepath.Join(DataDir(root), "role_map_"+roleMap.Version+".json")
	if err := atomicfs.WriteJSON(path, roleMap, true); err != nil {
		return err
	}
	return generator.RefreshManifest(root)
}

func ActivateMap(root, version string) error {
	found := false
	paths, err := filepath.Glob(filepath.Join(DataDir(root), "role_map_*.json"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		var roleMap domain.RoleMap
		if err := readJSON(path, &roleMap); err != nil {
			return err
		}
		if roleMap.Version == version {
			found = true
			break
		}
	}
	if !found {
		return &domain.BusinessError{Code: domain.CodeMapVersionConflict, Message: "cannot activate missing role map version", Stage: "upgrade", Detail: version}
	}
	if err := atomicfs.WriteJSON(filepath.Join(DataDir(root), "active-map.json"), domain.ActiveMap{Version: version}, true); err != nil {
		return err
	}
	return generator.RefreshManifest(root)
}

func ImportSessions(root string, blob []byte) error {
	path := filepath.Join(DataDir(root), "sessions.jsonl")
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, ".sessions.import.jsonl")
	if err := atomicfs.WriteFile(tmp, blob, 0o644); err != nil {
		return err
	}
	turns, err := generator.LoadTurns(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return domain.BizError(domain.CodeInvalidRequest, err.Error())
	}
	if err := validateTurns(turns); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := atomicfs.WriteFile(path, blob, 0o644); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Remove(tmp); err != nil {
		return err
	}
	return generator.RefreshManifest(root)
}

func ValidateRoleMap(roleMap domain.RoleMap) error {
	if strings.TrimSpace(roleMap.Version) == "" {
		return domain.BizError(domain.CodeMapVersionConflict, "role map version is required")
	}
	seen := map[string]struct{}{}
	for i, entry := range roleMap.Entries {
		if entry.LegacyCode <= 0 {
			return domain.BizError(domain.CodeInvalidRequest, fmt.Sprintf("entry %d has a non-positive legacy code", i))
		}
		if entry.Role == "" {
			return domain.BizError(domain.CodeInvalidRequest, fmt.Sprintf("entry %d has an empty role", i))
		}
		key := fmt.Sprintf("%d|%s|%d", entry.LegacyCode, entry.Role, entry.Priority)
		if _, ok := seen[key]; ok {
			return domain.BizError(domain.CodeInvalidRequest, fmt.Sprintf("duplicate mapping entry %s", key))
		}
		seen[key] = struct{}{}
	}
	return nil
}
