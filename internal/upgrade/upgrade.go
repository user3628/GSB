package upgrade

import (
	"fmt"
	"sort"
	"strings"

	"dialogalign/internal/domain"
)

type Resolver struct {
	active      domain.RoleMap
	maps        map[string]domain.RoleMap
	catalog     domain.RoleCatalog
	normalize   domain.NormalizationConfig
	rolesByName map[string]domain.RoleDef
}

func NewResolver(active domain.RoleMap, maps map[string]domain.RoleMap, catalog domain.RoleCatalog, cfg domain.NormalizationConfig) (*Resolver, error) {
	roles := make(map[string]domain.RoleDef, len(catalog.Roles))
	for _, role := range catalog.Roles {
		if _, exists := roles[role.Name]; exists {
			return nil, fmt.Errorf("duplicate role %s in catalog", role.Name)
		}
		roles[role.Name] = role
	}
	if _, ok := maps[active.Version]; !ok {
		maps[active.Version] = active
	}
	return &Resolver{active: active, maps: maps, catalog: catalog, normalize: cfg, rolesByName: roles}, nil
}

func (r *Resolver) RoleRange(role string) (domain.ValueRange, bool) {
	def, ok := r.rolesByName[role]
	return def.Range, ok
}

func (r *Resolver) MapVersion() string { return r.active.Version }

// CodeCandidates exposes mapping candidates for old-generation codecs. The
// returned slice is deterministic and includes both compatible and incompatible
// entries, mirroring the data retained for an ambiguity report.
func (r *Resolver) CodeCandidates(code int) []domain.SlotCandidate {
	entries := r.entriesForCode(code)
	out := make([]domain.SlotCandidate, 0, len(entries))
	for _, entry := range entries {
		out = append(out, domain.SlotCandidate{
			Role:       entry.Role,
			Priority:   entry.Priority,
			Range:      domain.CloneRange(entry.NewRange),
			Compatible: true,
			Reason:     "candidate",
		})
	}
	return out
}

func (r *Resolver) UpgradeTurn(turn domain.TurnInput) (domain.UpgradeArtifact, error) {
	artifact := domain.UpgradeArtifact{
		SessionID:   turn.SessionID,
		TurnIndex:   turn.TurnIndex,
		MapVersion:  r.active.Version,
		Slots:       []domain.UpgradedSlot{},
		Ambiguities: []domain.Ambiguity{},
		Diagnostics: []domain.Diagnostic{},
		InputCount:  len(turn.NewSlots) + len(turn.OldSlots),
	}
	order := 0
	for _, slot := range turn.NewSlots {
		upgraded, err := r.upgradeNew(turn, slot, order)
		if err != nil {
			return domain.UpgradeArtifact{}, err
		}
		artifact.Slots = append(artifact.Slots, upgraded)
		artifact.ResolvedRole = append(artifact.ResolvedRole, upgraded.Role)
		order++
	}
	for _, slot := range turn.OldSlots {
		upgraded, ambiguity, diags, err := r.upgradeLegacy(turn, slot, order)
		if err != nil {
			return domain.UpgradeArtifact{}, err
		}
		if ambiguity != nil {
			artifact.Ambiguities = append(artifact.Ambiguities, *ambiguity)
		}
		if upgraded != nil {
			artifact.Slots = append(artifact.Slots, *upgraded)
			artifact.ResolvedRole = append(artifact.ResolvedRole, upgraded.Role)
		}
		artifact.Diagnostics = append(artifact.Diagnostics, diags...)
		order++
	}
	return artifact, nil
}

func (r *Resolver) upgradeNew(turn domain.TurnInput, ann domain.NewSlotAnnotation, order int) (domain.UpgradedSlot, error) {
	if _, ok := r.rolesByName[ann.Role]; !ok {
		return domain.UpgradedSlot{}, contextErr(domain.CodeUnknownRole, "new-generation role does not exist in catalog", turn, ann.Field)
	}
	if ann.Range.Kind == "" {
		return domain.UpgradedSlot{}, contextErr(domain.CodeValueNormalize, "new-generation slot is missing a value range", turn, ann.Field)
	}
	value, err := NormalizeValue(ann.Value, ann.Range.Kind, r.normalize)
	if err != nil {
		return domain.UpgradedSlot{}, contextErr(domain.CodeValueNormalize, err.Error(), turn, ann.Field)
	}
	return domain.UpgradedSlot{
		Role:  ann.Role,
		Range: ann.Range,
		Value: value,
		Source: domain.ValueSource{
			Kind:       "new",
			Field:      fieldName(ann.Field, "new_slot"),
			CharOffset: ann.CharOffset,
		},
		Order: order,
	}, nil
}

func (r *Resolver) upgradeLegacy(turn domain.TurnInput, ann domain.LegacySlotAnnotation, order int) (*domain.UpgradedSlot, *domain.Ambiguity, []domain.Diagnostic, error) {
	field := ann.FieldName()
	if ann.MapVersion == "" {
		return nil, nil, nil, contextErr(domain.CodeMapVersionConflict, "legacy slot is missing declared map version", turn, field)
	}
	if ann.MapVersion != r.active.Version && !contains(r.active.CompatibleDeclarations, ann.MapVersion) {
		return nil, nil, nil, contextErr(domain.CodeMapVersionConflict,
			fmt.Sprintf("declared map version %s is not compatible with active %s", ann.MapVersion, r.active.Version), turn, field)
	}
	declaredMap, ok := r.maps[ann.MapVersion]
	if !ok {
		return nil, nil, nil, contextErr(domain.CodeMapVersionConflict,
			fmt.Sprintf("declared map version %s is unavailable", ann.MapVersion), turn, field)
	}

	entries := r.entriesForCode(ann.RoleCode)
	if len(entries) == 0 {
		return nil, nil, nil, contextErr(domain.CodeUnknownRoleCode,
			fmt.Sprintf("legacy role code %d is absent from active map %s", ann.RoleCode, r.active.Version), turn, field)
	}
	declaredRoles := rolesForCode(declaredMap, ann.RoleCode)
	if len(declaredRoles) == 0 {
		return nil, nil, nil, contextErr(domain.CodeUnknownRoleCode,
			fmt.Sprintf("legacy role code %d is absent from declared map %s", ann.RoleCode, ann.MapVersion), turn, field)
	}

	value, err := NormalizeValue(ann.RawValue, ann.Range.Kind, r.normalize)
	if err != nil {
		return nil, nil, nil, contextErr(domain.CodeValueNormalize, err.Error(), turn, field)
	}

	candidates := make([]domain.SlotCandidate, 0, len(entries))
	for _, entry := range entries {
		if _, ok := r.rolesByName[entry.Role]; !ok {
			return nil, nil, nil, contextErr(domain.CodeUnknownRole,
				fmt.Sprintf("mapping targets unknown role %s", entry.Role), turn, field)
		}
		compatible := true
		reasons := []string{}
		if !RangeCompatible(ann.Range, entry.LegacyRange) {
			compatible = false
			reasons = append(reasons, "legacy declaration is outside mapping range")
		}
		if !ValueCompatible(value, entry.NewRange) {
			compatible = false
			reasons = append(reasons, "normalized value is outside new role range")
		}
		reason := "compatible"
		if !compatible {
			reason = strings.Join(reasons, "; ")
		}
		candidates = append(candidates, domain.SlotCandidate{
			Role:       entry.Role,
			Priority:   entry.Priority,
			Range:      entry.NewRange,
			Compatible: compatible,
			Reason:     reason,
		})
	}

	compatible := make([]domain.SlotCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.Compatible {
			compatible = append(compatible, c)
		}
	}
	if len(compatible) == 0 {
		roles := candidateRoles(candidates)
		be := contextErr(domain.CodeRoleRangeIncompatible,
			fmt.Sprintf("legacy role code %d has no range-compatible target", ann.RoleCode), turn, field)
		be.Candidates = roles
		return nil, nil, nil, be
	}
	sort.SliceStable(compatible, func(i, j int) bool {
		if compatible[i].Priority != compatible[j].Priority {
			return compatible[i].Priority > compatible[j].Priority
		}
		return compatible[i].Role < compatible[j].Role
	})
	topPriority := compatible[0].Priority
	topRoles := []string{}
	for _, c := range compatible {
		if c.Priority == topPriority {
			topRoles = append(topRoles, c.Role)
		}
	}
	declaredRole := ""
	if len(declaredRoles) == 1 {
		declaredRole = declaredRoles[0]
	}
	if len(topRoles) > 1 {
		ambiguity := &domain.Ambiguity{
			SessionID:      turn.SessionID,
			TurnIndex:      turn.TurnIndex,
			Field:          field,
			LegacyRoleCode: ann.RoleCode,
			RawValue:       value.Raw,
			CharOffset:     ann.CharOffset,
			MapVersion:     r.active.Version,
			Candidates:     candidates,
			Reason:         fmt.Sprintf("multiple targets share top priority %d: %s", topPriority, strings.Join(topRoles, ", ")),
		}
		return nil, ambiguity, nil, nil
	}

	selected := compatible[0]
	source := domain.ValueSource{
		Kind:           "legacy",
		Field:          field,
		CharOffset:     ann.CharOffset,
		MapVersion:     ann.MapVersion,
		LegacyRoleCode: domain.IntPtr(ann.RoleCode),
		DeclaredRole:   declaredRole,
	}
	diags := []domain.Diagnostic{}
	if declaredRole != "" && declaredRole != selected.Role {
		diags = append(diags, domain.Diagnostic{
			Code:         "LEGACY_ROLE_UPGRADED",
			Severity:     "warning",
			Message:      fmt.Sprintf("legacy role code %d changed from %s to %s during map upgrade", ann.RoleCode, declaredRole, selected.Role),
			SessionID:    turn.SessionID,
			TurnIndex:    turn.TurnIndex,
			Field:        field,
			Role:         selected.Role,
			PreviousRole: declaredRole,
			CurrentRole:  selected.Role,
			Evidence:     fmt.Sprintf("declared_map=%s active_map=%s role_code=%d", ann.MapVersion, r.active.Version, ann.RoleCode),
		})
	}
	return &domain.UpgradedSlot{
		Role:         selected.Role,
		Range:        selected.Range,
		Value:        value,
		Source:       source,
		DeclaredRole: declaredRole,
		Order:        order,
	}, nil, diags, nil
}

func (r *Resolver) entriesForCode(code int) []domain.RoleMapEntry {
	out := []domain.RoleMapEntry{}
	for _, entry := range r.active.Entries {
		if entry.LegacyCode == code {
			out = append(out, entry)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		if out[i].Role != out[j].Role {
			return out[i].Role < out[j].Role
		}
		return out[i].Note < out[j].Note
	})
	return out
}

func rolesForCode(m domain.RoleMap, code int) []string {
	out := []string{}
	for _, entry := range m.Entries {
		if entry.LegacyCode == code {
			out = append(out, entry.Role)
		}
	}
	sort.Strings(out)
	return out
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func candidateRoles(candidates []domain.SlotCandidate) []string {
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.Role)
	}
	return out
}

func fieldName(field, fallback string) string {
	if field != "" {
		return field
	}
	return fallback
}

func contextErr(code, message string, turn domain.TurnInput, field string) *domain.BusinessError {
	err := domain.ContextError(code, message, turn.SessionID, turn.TurnIndex, field)
	err.Stage = "upgrade"
	return err
}
