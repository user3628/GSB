package engine

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"dialogalign/internal/domain"
	"dialogalign/internal/upgrade"
)

type PreviousSnapshot struct {
	State      domain.TurnState
	Digest     string
	MapVersion string
}

func Align(state domain.TurnState, turn domain.TurnInput, artifact domain.UpgradeArtifact, catalog domain.RoleCatalog, cfg domain.WorkbenchConfig, previousStates []PreviousSnapshot) domain.AlignmentReport {
	report := domain.AlignmentReport{
		SessionID:   state.SessionID,
		TurnIndex:   state.TurnIndex,
		MapVersion:  state.MapVersion,
		OutOfRange:  []domain.RangeFinding{},
		RoleDrifts:  []domain.DriftFinding{},
		Ambiguous:   []domain.AmbiguityFinding{},
		Diagnostics: append([]domain.Diagnostic{}, state.Diagnostics...),
	}
	roleRanges := map[string]domain.ValueRange{}
	for _, role := range catalog.Roles {
		roleRanges[role.Name] = domain.CloneRange(role.Range)
	}
	if cfg.Alignment.OutOfRangeEnabled {
		for _, slot := range state.Confirmed {
			if slot.SourceTurn != state.TurnIndex {
				continue
			}
			valueRange, ok := roleRanges[slot.Role]
			if !ok {
				report.OutOfRange = append(report.OutOfRange, domain.RangeFinding{
					SessionID: state.SessionID, TurnIndex: state.TurnIndex, Role: slot.Role, Field: slot.Source.Field,
					Value: slot.Value, Range: slot.Range, CharOffset: charOffset(turn.UserText, slot),
					Reason: "role is not present in the active semantic role catalog",
				})
				continue
			}
			if !upgrade.ValueCompatible(slot.Value, valueRange) {
				report.OutOfRange = append(report.OutOfRange, domain.RangeFinding{
					SessionID: state.SessionID, TurnIndex: state.TurnIndex, Role: slot.Role, Field: slot.Source.Field,
					Value: slot.Value, Range: valueRange, CharOffset: charOffset(turn.UserText, slot),
					Reason: rangeReason(slot.Value, valueRange),
				})
			}
		}
	}
	if cfg.Alignment.DriftEnabled {
		for _, slot := range state.Confirmed {
			if slot.SourceTurn != state.TurnIndex {
				continue
			}
			previousRole := ""
			reason := ""
			evidenceParts := []string{fmt.Sprintf("declared_map=%s active_map=%s role_code=%d", slot.Source.MapVersion, state.MapVersion, legacyCode(slot))}
			if slot.Source.DeclaredRole != "" && slot.Source.DeclaredRole != slot.Role {
				previousRole = slot.Source.DeclaredRole
				reason = "active mapping table changed the role selected for a legacy code"
			}
			for _, previous := range previousStates {
				if previous.MapVersion == state.MapVersion {
					continue
				}
				for _, priorSlot := range previous.State.Confirmed {
					if priorSlot.SourceTurn != slot.SourceTurn || priorSlot.Source.Field != slot.Source.Field {
						continue
					}
					if priorSlot.Source.LegacyRoleCode == nil || slot.Source.LegacyRoleCode == nil {
						continue
					}
					if *priorSlot.Source.LegacyRoleCode != *slot.Source.LegacyRoleCode || priorSlot.Role == slot.Role {
						continue
					}
					if previousRole == "" {
						previousRole = priorSlot.Role
						reason = "replaced mapping table changed the confirmed role for the same legacy slot"
					}
					evidenceParts = append(evidenceParts, fmt.Sprintf("previous_map=%s prior_state_digest=%s prior_value=%s", previous.MapVersion, previous.Digest, priorSlot.Value.Canonical))
					break
				}
			}
			if previousRole != "" {
				report.RoleDrifts = append(report.RoleDrifts, domain.DriftFinding{
					SessionID: state.SessionID, TurnIndex: state.TurnIndex, SourceTurn: slot.SourceTurn,
					Role: slot.Role, PreviousRole: previousRole, Field: slot.Source.Field,
					LegacyCode: legacyCode(slot), MapVersion: state.MapVersion, CharOffset: charOffset(turn.UserText, slot),
					Reason: reason, Evidence: strings.Join(evidenceParts, "; "),
				})
			}
		}
	}
	if cfg.Alignment.AmbiguityEnabled {
		for _, ambiguity := range artifact.Ambiguities {
			report.Ambiguous = append(report.Ambiguous, domain.AmbiguityFinding{
				SessionID:      ambiguity.SessionID,
				TurnIndex:      ambiguity.TurnIndex,
				Field:          ambiguity.Field,
				LegacyRoleCode: ambiguity.LegacyRoleCode,
				RawValue:       ambiguity.RawValue,
				CharOffset:     ambiguity.CharOffset,
				Candidates:     append([]domain.SlotCandidate(nil), ambiguity.Candidates...),
				Reason:         ambiguity.Reason,
			})
		}
	}
	sortRangeFindings(report.OutOfRange)
	sortDriftFindings(report.RoleDrifts)
	sortAmbiguityFindings(report.Ambiguous)
	max := cfg.Alignment.MaxFindingsPerCategory
	if max > 0 {
		if len(report.OutOfRange) > max {
			report.OutOfRange = report.OutOfRange[:max]
		}
		if len(report.RoleDrifts) > max {
			report.RoleDrifts = report.RoleDrifts[:max]
		}
		if len(report.Ambiguous) > max {
			report.Ambiguous = report.Ambiguous[:max]
		}
	}
	return report
}

func rangeReason(value domain.SlotValue, r domain.ValueRange) string {
	switch r.Kind {
	case domain.KindInteger:
		min, max := "unbounded", "unbounded"
		if r.Min != nil {
			min = fmt.Sprint(*r.Min)
		}
		if r.Max != nil {
			max = fmt.Sprint(*r.Max)
		}
		return fmt.Sprintf("integer %s is outside [%s,%s]", value.Canonical, min, max)
	case domain.KindEnum:
		return fmt.Sprintf("enum %s is not one of [%s]", value.Canonical, strings.Join(r.Enum, ","))
	case domain.KindDateRange:
		return fmt.Sprintf("date range %s is outside [%s,%s]", value.Canonical, r.DateMin, r.DateMax)
	default:
		return "value is outside role range"
	}
}

func charOffset(text string, slot domain.ConfirmedSlot) int {
	if slot.Value.Raw != "" {
		if byteOffset := strings.Index(text, slot.Value.Raw); byteOffset >= 0 {
			return utf8.RuneCountInString(text[:byteOffset])
		}
	}
	return slot.Source.CharOffset
}

func legacyCode(slot domain.ConfirmedSlot) int {
	if slot.Source.LegacyRoleCode == nil {
		return 0
	}
	return *slot.Source.LegacyRoleCode
}

func sortRangeFindings(items []domain.RangeFinding) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].TurnIndex != items[j].TurnIndex {
			return items[i].TurnIndex < items[j].TurnIndex
		}
		if items[i].Role != items[j].Role {
			return items[i].Role < items[j].Role
		}
		if items[i].Field != items[j].Field {
			return items[i].Field < items[j].Field
		}
		return items[i].CharOffset < items[j].CharOffset
	})
}

func sortDriftFindings(items []domain.DriftFinding) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].TurnIndex != items[j].TurnIndex {
			return items[i].TurnIndex < items[j].TurnIndex
		}
		if items[i].SourceTurn != items[j].SourceTurn {
			return items[i].SourceTurn < items[j].SourceTurn
		}
		if items[i].Field != items[j].Field {
			return items[i].Field < items[j].Field
		}
		if items[i].PreviousRole != items[j].PreviousRole {
			return items[i].PreviousRole < items[j].PreviousRole
		}
		return items[i].Role < items[j].Role
	})
}

func sortAmbiguityFindings(items []domain.AmbiguityFinding) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].TurnIndex != items[j].TurnIndex {
			return items[i].TurnIndex < items[j].TurnIndex
		}
		if items[i].Field != items[j].Field {
			return items[i].Field < items[j].Field
		}
		return items[i].LegacyRoleCode < items[j].LegacyRoleCode
	})
}
