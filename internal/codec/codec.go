package codec

import (
	"fmt"
	"sort"

	"dialogalign/internal/domain"
	"dialogalign/internal/upgrade"
)

// NewEncodedSlot is the new-generation wire form. It remains an explicit
// "role name + value range + value" record while carrying enough provenance to
// restore turn ownership.
type NewEncodedSlot struct {
	Role       string            `json:"role"`
	Range      domain.ValueRange `json:"value_range"`
	Value      domain.SlotValue  `json:"value"`
	Field      string            `json:"field,omitempty"`
	CharOffset int               `json:"char_offset"`
	SourceKind string            `json:"source_kind,omitempty"`
	SourceTurn int               `json:"source_turn"`
}

type NewPendingSlot struct {
	Role          string                 `json:"role,omitempty"`
	Range         domain.ValueRange      `json:"value_range,omitempty"`
	Field         string                 `json:"field,omitempty"`
	Reason        string                 `json:"reason"`
	Candidates    []domain.SlotCandidate `json:"candidates,omitempty"`
	RequestedTurn int                    `json:"requested_turn"`
	ResolvedTurn  *int                   `json:"resolved_turn,omitempty"`
}

type NewTurnDocument struct {
	SessionID string           `json:"session_id"`
	TurnIndex int              `json:"turn_index"`
	Intent    string           `json:"intent"`
	Confirmed []NewEncodedSlot `json:"confirmed_slots"`
	Pending   []NewPendingSlot `json:"pending_slots"`
}

type NewSessionDocument struct {
	Version    string            `json:"version"`
	SessionID  string            `json:"session_id"`
	MapVersion string            `json:"map_version"`
	Turns      []NewTurnDocument `json:"turns"`
}

type LegacyEncodedSlot struct {
	RoleCode   int               `json:"role_code"`
	MapVersion string            `json:"map_version"`
	RawValue   string            `json:"raw_value"`
	Range      domain.ValueRange `json:"value_range"`
	Field      string            `json:"field,omitempty"`
	CharOffset int               `json:"char_offset"`
	SourceTurn int               `json:"source_turn"`
}

type LegacyPendingSlot struct {
	RoleCode      int               `json:"role_code"`
	MapVersion    string            `json:"map_version"`
	Range         domain.ValueRange `json:"value_range,omitempty"`
	Field         string            `json:"field,omitempty"`
	Reason        string            `json:"reason"`
	RequestedTurn int               `json:"requested_turn"`
	ResolvedTurn  *int              `json:"resolved_turn,omitempty"`
}

type LegacyTurnDocument struct {
	SessionID string              `json:"session_id"`
	TurnIndex int                 `json:"turn_index"`
	Intent    string              `json:"intent"`
	Confirmed []LegacyEncodedSlot `json:"confirmed_slots"`
	Pending   []LegacyPendingSlot `json:"pending_slots"`
}

type LegacySessionDocument struct {
	Version    string               `json:"version"`
	SessionID  string               `json:"session_id"`
	MapVersion string               `json:"map_version"`
	Turns      []LegacyTurnDocument `json:"turns"`
}

func EncodeNewSession(state domain.SessionState) (NewSessionDocument, error) {
	doc := NewSessionDocument{Version: "new-v1", SessionID: state.SessionID, MapVersion: state.MapVersion, Turns: []NewTurnDocument{}}
	for _, turn := range state.Turns {
		t := NewTurnDocument{
			SessionID: turn.SessionID,
			TurnIndex: turn.TurnIndex,
			Intent:    turn.Intent,
			Confirmed: make([]NewEncodedSlot, 0, len(turn.Confirmed)),
			Pending:   make([]NewPendingSlot, 0, len(turn.Pending)),
		}
		for _, slot := range turn.Confirmed {
			t.Confirmed = append(t.Confirmed, NewEncodedSlot{
				Role:       slot.Role,
				Range:      domain.CloneRange(slot.Range),
				Value:      slot.Value,
				Field:      slot.Source.Field,
				CharOffset: slot.Source.CharOffset,
				SourceKind: slot.Source.Kind,
				SourceTurn: slot.SourceTurn,
			})
		}
		for _, pending := range turn.Pending {
			t.Pending = append(t.Pending, NewPendingSlot{
				Role:          pending.Role,
				Range:         domain.CloneRange(pending.Range),
				Field:         pending.Field,
				Reason:        pending.Reason,
				Candidates:    append([]domain.SlotCandidate(nil), pending.Candidates...),
				RequestedTurn: pending.RequestedTurn,
				ResolvedTurn:  cloneIntPtr(pending.ResolvedTurn),
			})
		}
		doc.Turns = append(doc.Turns, t)
	}
	return doc, nil
}

func DecodeNewSession(doc NewSessionDocument) (domain.SessionState, error) {
	state := domain.SessionState{SessionID: doc.SessionID, MapVersion: doc.MapVersion, Turns: make([]domain.TurnState, 0, len(doc.Turns))}
	for _, turnDoc := range doc.Turns {
		if turnDoc.SessionID != "" && turnDoc.SessionID != doc.SessionID {
			return domain.SessionState{}, fmt.Errorf("turn %d belongs to %s, not %s", turnDoc.TurnIndex, turnDoc.SessionID, doc.SessionID)
		}
		turn := domain.TurnState{
			SessionID:  doc.SessionID,
			TurnIndex:  turnDoc.TurnIndex,
			Intent:     turnDoc.Intent,
			MapVersion: doc.MapVersion,
			Confirmed:  make([]domain.ConfirmedSlot, 0, len(turnDoc.Confirmed)),
			Pending:    make([]domain.PendingSlot, 0, len(turnDoc.Pending)),
		}
		for _, slot := range turnDoc.Confirmed {
			if slot.Role == "" || slot.Range.Kind == "" {
				return domain.SessionState{}, fmt.Errorf("new slot at turn %d lacks role or range", turnDoc.TurnIndex)
			}
			sourceKind := slot.SourceKind
			if sourceKind == "" {
				sourceKind = "new"
			}
			turn.Confirmed = append(turn.Confirmed, domain.ConfirmedSlot{
				Role:       slot.Role,
				Range:      domain.CloneRange(slot.Range),
				Value:      slot.Value,
				SourceTurn: slot.SourceTurn,
				Source: domain.ValueSource{
					Kind:       sourceKind,
					Field:      slot.Field,
					CharOffset: slot.CharOffset,
				},
			})
		}
		for _, pending := range turnDoc.Pending {
			turn.Pending = append(turn.Pending, domain.PendingSlot{
				Role:          pending.Role,
				Range:         domain.CloneRange(pending.Range),
				Field:         pending.Field,
				Reason:        pending.Reason,
				Candidates:    append([]domain.SlotCandidate(nil), pending.Candidates...),
				RequestedTurn: pending.RequestedTurn,
				ResolvedTurn:  cloneIntPtr(pending.ResolvedTurn),
				Source:        domain.ValueSource{Kind: "codec_new"},
			})
		}
		state.Turns = append(state.Turns, turn)
	}
	return state, nil
}

// EncodeLegacySession uses the old integer-code representation. A role is only
// emitted when a code exists whose compatible candidates resolve back to that
// exact role; the encoder never silently picks an ambiguous first candidate.
func EncodeLegacySession(state domain.SessionState, roleMap domain.RoleMap) (LegacySessionDocument, error) {
	doc := LegacySessionDocument{Version: "legacy-v1", SessionID: state.SessionID, MapVersion: roleMap.Version, Turns: []LegacyTurnDocument{}}
	for _, turn := range state.Turns {
		t := LegacyTurnDocument{
			SessionID: turn.SessionID,
			TurnIndex: turn.TurnIndex,
			Intent:    turn.Intent,
			Confirmed: make([]LegacyEncodedSlot, 0, len(turn.Confirmed)),
			Pending:   make([]LegacyPendingSlot, 0, len(turn.Pending)),
		}
		for _, slot := range turn.Confirmed {
			entry, err := chooseLegacyEntry(roleMap, slot)
			if err != nil {
				return LegacySessionDocument{}, fmt.Errorf("turn %d role %s: %w", turn.TurnIndex, slot.Role, err)
			}
			t.Confirmed = append(t.Confirmed, LegacyEncodedSlot{
				RoleCode:   entry.LegacyCode,
				MapVersion: roleMap.Version,
				RawValue:   slot.Value.Canonical,
				Range:      domain.CloneRange(entry.LegacyRange),
				Field:      slot.Source.Field,
				CharOffset: slot.Source.CharOffset,
				SourceTurn: slot.SourceTurn,
			})
		}
		for _, pending := range turn.Pending {
			code := 0
			if pending.Source.LegacyRoleCode != nil {
				code = *pending.Source.LegacyRoleCode
			} else if pending.Role != "" {
				entry, err := chooseLegacyEntryForPending(roleMap, pending.Role)
				if err != nil {
					return LegacySessionDocument{}, fmt.Errorf("turn %d pending role %s: %w", turn.TurnIndex, pending.Role, err)
				}
				code = entry.LegacyCode
			}
			t.Pending = append(t.Pending, LegacyPendingSlot{
				RoleCode:      code,
				MapVersion:    roleMap.Version,
				Range:         domain.CloneRange(pending.Range),
				Field:         pending.Field,
				Reason:        pending.Reason,
				RequestedTurn: pending.RequestedTurn,
				ResolvedTurn:  cloneIntPtr(pending.ResolvedTurn),
			})
		}
		doc.Turns = append(doc.Turns, t)
	}
	return doc, nil
}

// DecodeLegacySession runs the explicit upgrade path and preserves turn
// ownership. It requires a resolver because integer codes alone are not safe.
func DecodeLegacySession(doc LegacySessionDocument, resolver *upgrade.Resolver) (domain.SessionState, error) {
	state := domain.SessionState{SessionID: doc.SessionID, MapVersion: doc.MapVersion, Turns: make([]domain.TurnState, 0, len(doc.Turns))}
	for _, turnDoc := range doc.Turns {
		input := domain.TurnInput{SessionID: doc.SessionID, TurnIndex: turnDoc.TurnIndex}
		oldSlots := make([]domain.LegacySlotAnnotation, 0, len(turnDoc.Confirmed))
		for _, slot := range turnDoc.Confirmed {
			oldSlots = append(oldSlots, domain.LegacySlotAnnotation{
				RoleCode:   slot.RoleCode,
				MapVersion: slot.MapVersion,
				RawValue:   slot.RawValue,
				Range:      slot.Range,
				Field:      slot.Field,
				CharOffset: slot.CharOffset,
			})
		}
		input.OldSlots = oldSlots
		artifact, err := resolver.UpgradeTurn(input)
		if err != nil {
			return domain.SessionState{}, err
		}
		if len(artifact.Ambiguities) > 0 {
			return domain.SessionState{}, fmt.Errorf("legacy codec produced an ambiguous slot at turn %d", turnDoc.TurnIndex)
		}
		turn := domain.TurnState{
			SessionID:  doc.SessionID,
			TurnIndex:  turnDoc.TurnIndex,
			Intent:     turnDoc.Intent,
			MapVersion: doc.MapVersion,
			Confirmed:  make([]domain.ConfirmedSlot, 0, len(artifact.Slots)),
			Pending:    make([]domain.PendingSlot, 0, len(turnDoc.Pending)),
		}
		for i, slot := range artifact.Slots {
			if i >= len(turnDoc.Confirmed) {
				return domain.SessionState{}, fmt.Errorf("legacy decode produced extra slot at turn %d", turnDoc.TurnIndex)
			}
			sourceTurn := turnDoc.Confirmed[i].SourceTurn
			if sourceTurn == 0 {
				sourceTurn = turnDoc.TurnIndex
			}
			turn.Confirmed = append(turn.Confirmed, domain.ConfirmedSlot{
				Role:       slot.Role,
				Range:      slot.Range,
				Value:      slot.Value,
				Source:     slot.Source,
				SourceTurn: sourceTurn,
			})
		}
		for _, pending := range turnDoc.Pending {
			ps := domain.PendingSlot{
				Range:         domain.CloneRange(pending.Range),
				Field:         pending.Field,
				Reason:        pending.Reason,
				RequestedTurn: pending.RequestedTurn,
				ResolvedTurn:  cloneIntPtr(pending.ResolvedTurn),
				Source:        domain.ValueSource{Kind: "legacy", MapVersion: pending.MapVersion, LegacyRoleCode: domain.IntPtr(pending.RoleCode)},
			}
			candidates := resolver.CodeCandidates(pending.RoleCode)
			if len(candidates) == 1 {
				ps.Role = candidates[0].Role
				ps.Range = candidates[0].Range
			} else {
				ps.Candidates = candidates
			}
			turn.Pending = append(turn.Pending, ps)
		}
		state.Turns = append(state.Turns, turn)
	}
	return state, nil
}

func chooseLegacyEntry(roleMap domain.RoleMap, slot domain.ConfirmedSlot) (domain.RoleMapEntry, error) {
	entries := entriesForRole(roleMap, slot.Role)
	candidates := []domain.RoleMapEntry{}
	for _, entry := range entries {
		if !upgrade.RangeCompatible(slot.Range, entry.LegacyRange) {
			continue
		}
		if !upgrade.ValueCompatible(slot.Value, entry.NewRange) {
			continue
		}
		if resolvesOnlyTo(roleMap, entry, slot.Role) {
			candidates = append(candidates, entry)
		}
	}
	if len(candidates) == 0 {
		return domain.RoleMapEntry{}, fmt.Errorf("no unambiguous legacy code can round-trip role %s", slot.Role)
	}
	if slot.Source.LegacyRoleCode != nil {
		for _, candidate := range candidates {
			if candidate.LegacyCode == *slot.Source.LegacyRoleCode {
				return candidate, nil
			}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].LegacyCode != candidates[j].LegacyCode {
			return candidates[i].LegacyCode < candidates[j].LegacyCode
		}
		return candidates[i].Priority > candidates[j].Priority
	})
	return candidates[0], nil
}

func chooseLegacyEntryForPending(roleMap domain.RoleMap, role string) (domain.RoleMapEntry, error) {
	for _, entry := range entriesForRole(roleMap, role) {
		if resolvesOnlyTo(roleMap, entry, role) {
			return entry, nil
		}
	}
	return domain.RoleMapEntry{}, fmt.Errorf("no unambiguous legacy code for pending role %s", role)
}

func resolvesOnlyTo(roleMap domain.RoleMap, entry domain.RoleMapEntry, role string) bool {
	best := entry.Priority
	roles := map[string]struct{}{}
	for _, peer := range roleMap.Entries {
		if peer.LegacyCode != entry.LegacyCode || peer.Priority < best {
			continue
		}
		roles[peer.Role] = struct{}{}
	}
	if len(roles) != 1 {
		return false
	}
	_, ok := roles[role]
	return ok
}

func entriesForRole(roleMap domain.RoleMap, role string) []domain.RoleMapEntry {
	out := []domain.RoleMapEntry{}
	for _, entry := range roleMap.Entries {
		if entry.Role == role {
			out = append(out, entry)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].LegacyCode < out[j].LegacyCode
	})
	return out
}

func cloneIntPtr(v *int) *int {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}
