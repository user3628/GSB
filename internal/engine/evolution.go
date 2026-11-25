package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"dialogalign/internal/domain"
)

func Evolve(prev *domain.TurnState, turn domain.TurnInput, artifact domain.UpgradeArtifact, cfg domain.WorkbenchConfig, catalog domain.RoleCatalog, finalTurn bool) (domain.TurnState, error) {
	state := domain.TurnState{
		SessionID:   turn.SessionID,
		TurnIndex:   turn.TurnIndex,
		Confirmed:   []domain.ConfirmedSlot{},
		Pending:     []domain.PendingSlot{},
		Changes:     []domain.Change{},
		Rules:       []string{},
		MapVersion:  artifact.MapVersion,
		Diagnostics: []domain.Diagnostic{},
		IntentLog:   []domain.IntentVisit{},
	}
	if prev != nil {
		cloned, err := cloneTurnState(*prev)
		if err != nil {
			return domain.TurnState{}, err
		}
		state.Confirmed = cloned.Confirmed
		state.Pending = cloned.Pending
		state.IntentLog = cloned.IntentLog
		state.Intent = cloned.Intent
	} else {
		state.Intent = cfg.Rules.InitialIntent
		state.IntentLog = append(state.IntentLog, domain.IntentVisit{TurnIndex: turn.TurnIndex, Intent: state.Intent, RuleID: "initial"})
	}

	roles := map[string]domain.RoleDef{}
	for _, role := range catalog.Roles {
		roles[role.Name] = role
	}

	rule, matched := selectRule(cfg.Rules.Rules, state.Intent, turn.UserText)
	if matched {
		state.Rules = appendUnique(state.Rules, rule.ID)
		state.Intent = applyIntentTransition(&state, rule.To, rule.ID, turn.TurnIndex)
		for _, roleName := range rule.Clarify {
			def, ok := roles[roleName]
			if !ok {
				return domain.TurnState{}, &domain.BusinessError{Code: domain.CodeMissingPrerequisite, Message: "intent rule references unknown clarification role", SessionID: turn.SessionID, TurnIndex: domain.IntPtr(turn.TurnIndex), Stage: "evolve", Field: roleName, Detail: rule.ID}
			}
			if findActivePending(state.Pending, roleName) >= 0 {
				continue
			}
			state.Pending = append(state.Pending, domain.PendingSlot{
				Role:          roleName,
				Range:         def.Range,
				Field:         roleName,
				Reason:        "rule " + rule.ID + " requested clarification",
				Source:        domain.ValueSource{Kind: "rule", Field: roleName, MapVersion: artifact.MapVersion},
				RequestedTurn: turn.TurnIndex,
			})
			state.Changes = append(state.Changes, domain.Change{Kind: "pending_add", Role: roleName, RuleID: rule.ID})
		}
		for _, roleName := range rule.ResolvePending {
			for i := range state.Pending {
				if state.Pending[i].Role == roleName && state.Pending[i].ResolvedTurn == nil {
					state.Pending[i].ResolvedTurn = domain.IntPtr(turn.TurnIndex)
					state.Changes = append(state.Changes, domain.Change{Kind: "pending_resolve", Role: roleName, RuleID: rule.ID})
				}
			}
		}
	} else if strings.TrimSpace(turn.IntentHint) != "" && turn.IntentHint != state.Intent {
		state.Intent = applyIntentTransition(&state, turn.IntentHint, "intent_hint", turn.TurnIndex)
		state.Rules = appendUnique(state.Rules, "intent_hint")
	}

	slots := append([]domain.UpgradedSlot(nil), artifact.Slots...)
	sort.SliceStable(slots, func(i, j int) bool { return slots[i].Order < slots[j].Order })
	for _, slot := range slots {
		existing := findConfirmed(state.Confirmed, slot.Role)
		if existing >= 0 {
			if state.Confirmed[existing].Value.Canonical == slot.Value.Canonical {
				continue
			}
			state.Diagnostics = append(state.Diagnostics, domain.Diagnostic{
				Code:        "SLOT_CONFLICT",
				Severity:    "warning",
				Message:     "slot was repeatedly confirmed with a different value; the prior confirmation was preserved",
				SessionID:   turn.SessionID,
				TurnIndex:   turn.TurnIndex,
				Field:       slot.Source.Field,
				Role:        slot.Role,
				PreviousVal: state.Confirmed[existing].Value.Canonical,
				CurrentVal:  slot.Value.Canonical,
				Evidence:    fmt.Sprintf("source_turn=%d", state.Confirmed[existing].SourceTurn),
			})
			state.Changes = append(state.Changes, domain.Change{Kind: "slot_conflict", Role: slot.Role, OldValue: state.Confirmed[existing].Value.Canonical, NewValue: slot.Value.Canonical, Diagnostic: "SLOT_CONFLICT"})
			continue
		}
		state.Confirmed = append(state.Confirmed, domain.ConfirmedSlot{
			Role:       slot.Role,
			Range:      domain.CloneRange(slot.Range),
			Value:      slot.Value,
			Source:     slot.Source,
			SourceTurn: turn.TurnIndex,
		})
		state.Changes = append(state.Changes, domain.Change{Kind: "slot_confirm", Role: slot.Role, NewValue: slot.Value.Canonical})
		for i := range state.Pending {
			if state.Pending[i].Role == slot.Role && state.Pending[i].ResolvedTurn == nil {
				state.Pending[i].ResolvedTurn = domain.IntPtr(turn.TurnIndex)
				state.Changes = append(state.Changes, domain.Change{Kind: "pending_resolve", Role: slot.Role})
			}
		}
	}

	for _, ambiguity := range artifact.Ambiguities {
		keyExists := false
		for _, pending := range state.Pending {
			if pending.Field == ambiguity.Field && pending.Source.LegacyRoleCode != nil && *pending.Source.LegacyRoleCode == ambiguity.LegacyRoleCode {
				keyExists = true
				break
			}
		}
		if !keyExists {
			state.Pending = append(state.Pending, domain.PendingSlot{
				Role:          "",
				Field:         ambiguity.Field,
				Reason:        ambiguity.Reason,
				Candidates:    append([]domain.SlotCandidate(nil), ambiguity.Candidates...),
				Source:        domain.ValueSource{Kind: "legacy", Field: ambiguity.Field, CharOffset: ambiguity.CharOffset, LegacyRoleCode: domain.IntPtr(ambiguity.LegacyRoleCode), MapVersion: ambiguity.MapVersion},
				RequestedTurn: turn.TurnIndex,
			})
			state.Changes = append(state.Changes, domain.Change{Kind: "pending_add", Role: ambiguity.Field, Diagnostic: "AMBIGUOUS_ROLE_MAPPING"})
		}
		candidateNames := make([]string, 0, len(ambiguity.Candidates))
		for _, candidate := range ambiguity.Candidates {
			candidateNames = append(candidateNames, candidate.Role)
		}
		state.Diagnostics = append(state.Diagnostics, domain.Diagnostic{
			Code:       "AMBIGUOUS_ROLE_MAPPING",
			Severity:   "warning",
			Message:    "legacy role code could not be resolved uniquely; all candidates were retained",
			SessionID:  turn.SessionID,
			TurnIndex:  turn.TurnIndex,
			Field:      ambiguity.Field,
			Candidates: candidateNames,
			Evidence:   fmt.Sprintf("legacy_code=%d raw=%s map=%s", ambiguity.LegacyRoleCode, ambiguity.RawValue, ambiguity.MapVersion),
		})
	}
	state.Diagnostics = append(state.Diagnostics, artifact.Diagnostics...)

	if finalTurn {
		for _, pending := range state.Pending {
			if pending.ResolvedTurn != nil {
				continue
			}
			state.Diagnostics = append(state.Diagnostics, domain.Diagnostic{
				Code:      "PENDING_UNANSWERED",
				Severity:  "warning",
				Message:   "pending clarification was not answered by any later turn",
				SessionID: turn.SessionID,
				TurnIndex: turn.TurnIndex,
				Field:     pending.Field,
				Role:      pending.Role,
				AtTurn:    domain.IntPtr(pending.RequestedTurn),
				Evidence:  fmt.Sprintf("requested_turn=%d reason=%s", pending.RequestedTurn, pending.Reason),
			})
		}
	}
	return state, nil
}

func applyIntentTransition(state *domain.TurnState, to, ruleID string, turn int) string {
	from := state.Intent
	if from == to {
		return to
	}
	seenAt := 0
	for _, visit := range state.IntentLog {
		if visit.Intent == to {
			seenAt = visit.TurnIndex
			break
		}
	}
	if seenAt > 0 {
		state.Diagnostics = append(state.Diagnostics, domain.Diagnostic{
			Code:       "INTENT_TRANSITION_LOOP",
			Severity:   "warning",
			Message:    "intent transition returned to a previously visited state; transition history is retained",
			SessionID:  state.SessionID,
			TurnIndex:  turn,
			IntentFrom: from,
			IntentTo:   to,
			AtTurn:     domain.IntPtr(seenAt),
			Evidence:   fmt.Sprintf("rule=%s previous_visit_turn=%d", ruleID, seenAt),
		})
	}
	state.Changes = append(state.Changes, domain.Change{Kind: "intent_transition", OldValue: from, NewValue: to, RuleID: ruleID})
	state.IntentLog = append(state.IntentLog, domain.IntentVisit{TurnIndex: turn, Intent: to, RuleID: ruleID})
	return to
}

func selectRule(rules []domain.IntentRule, currentIntent, text string) (domain.IntentRule, bool) {
	sorted := append([]domain.IntentRule(nil), rules...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Priority != sorted[j].Priority {
			return sorted[i].Priority > sorted[j].Priority
		}
		return sorted[i].ID < sorted[j].ID
	})
	lowerText := strings.ToLower(text)
	for _, rule := range sorted {
		if len(rule.From) > 0 && !containsString(rule.From, currentIntent) {
			continue
		}
		matched := false
		for _, keyword := range rule.KeywordsAny {
			if strings.Contains(lowerText, strings.ToLower(keyword)) {
				matched = true
				break
			}
		}
		if matched {
			return rule, true
		}
	}
	return domain.IntentRule{}, false
}

func findConfirmed(slots []domain.ConfirmedSlot, role string) int {
	for i := range slots {
		if slots[i].Role == role {
			return i
		}
	}
	return -1
}

func findActivePending(slots []domain.PendingSlot, role string) int {
	for i := range slots {
		if slots[i].Role == role && slots[i].ResolvedTurn == nil {
			return i
		}
	}
	return -1
}

func appendUnique(values []string, target string) []string {
	if containsString(values, target) {
		return values
	}
	return append(values, target)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cloneTurnState(in domain.TurnState) (domain.TurnState, error) {
	blob, err := json.Marshal(in)
	if err != nil {
		return domain.TurnState{}, err
	}
	var out domain.TurnState
	if err := json.Unmarshal(blob, &out); err != nil {
		return domain.TurnState{}, err
	}
	return out, nil
}
