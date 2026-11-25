package engine

import (
	"context"
	"encoding/json"
	"sort"

	"dialogalign/internal/domain"
	"dialogalign/internal/store"
)

type SessionSummary struct {
	SessionID    string `json:"session_id"`
	TotalTurns   int    `json:"total_turns"`
	AlignedTurns int    `json:"aligned_turns"`
	LatestIntent string `json:"latest_intent,omitempty"`
	MapVersion   string `json:"map_version"`
	Complete     bool   `json:"complete"`
}

func (s *Service) Summary(ctx context.Context) ([]SessionSummary, error) {
	dataset, err := LoadDataset(s.Root)
	if err != nil {
		return nil, err
	}
	st, err := store.Open(s.Root)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	sessions, bySession := GroupTurns(dataset.Turns)
	out := []SessionSummary{}
	for _, sessionID := range sessions {
		summary := SessionSummary{SessionID: sessionID, TotalTurns: len(bySession[sessionID]), MapVersion: dataset.ActiveMap.Version}
		for _, turn := range bySession[sessionID] {
			cp, ok, err := st.ReadCheckpoint(ctx, sessionID, turn.TurnIndex, store.StageAlign)
			if err != nil {
				return nil, err
			}
			if ok && cp.TurnIndex == turn.TurnIndex {
				summary.AlignedTurns++
			}
			stateBlob, _, err := st.LatestState(ctx, sessionID, turn.TurnIndex)
			if err != nil {
				return nil, err
			}
			if len(stateBlob) > 0 {
				var state domain.TurnState
				if err := json.Unmarshal(stateBlob, &state); err != nil {
					return nil, err
				}
				summary.LatestIntent = state.Intent
			}
		}
		summary.Complete = summary.AlignedTurns == summary.TotalTurns
		out = append(out, summary)
	}
	return out, nil
}

func (s *Service) Export(ctx context.Context) (domain.ExportBundle, error) {
	dataset, err := LoadDataset(s.Root)
	if err != nil {
		return domain.ExportBundle{}, err
	}
	configs, err := buildStageConfigs(dataset)
	if err != nil {
		return domain.ExportBundle{}, err
	}
	st, err := store.Open(s.Root)
	if err != nil {
		return domain.ExportBundle{}, err
	}
	defer st.Close()
	sessions, bySession := GroupTurns(dataset.Turns)
	bundle := domain.ExportBundle{
		Version:      "intent-slot-state-v1",
		MapVersion:   dataset.ActiveMap.Version,
		ConfigDigest: configs.global,
		Sessions:     []domain.SessionState{},
		Alignments:   []domain.AlignmentReport{},
		Fingerprints: []domain.SessionFingerprint{},
	}
	for _, sessionID := range sessions {
		sessionState := domain.SessionState{SessionID: sessionID, MapVersion: dataset.ActiveMap.Version, Turns: []domain.TurnState{}}
		for _, turn := range bySession[sessionID] {
			cp, ok, err := st.ReadCheckpoint(ctx, sessionID, turn.TurnIndex, store.StageAlign)
			if err != nil {
				return domain.ExportBundle{}, err
			}
			if !ok {
				return domain.ExportBundle{}, &domain.BusinessError{
					Code: domain.CodeMissingPrerequisite, Message: "cannot export incomplete turn: align checkpoint is missing",
					SessionID: sessionID, TurnIndex: domain.IntPtr(turn.TurnIndex), Stage: store.StageAlign,
				}
			}
			stateJSON, digest, err := st.LatestState(ctx, sessionID, turn.TurnIndex)
			if err != nil {
				return domain.ExportBundle{}, err
			}
			if len(stateJSON) == 0 {
				return domain.ExportBundle{}, &domain.BusinessError{Code: domain.CodeMissingPrerequisite, Message: "cannot export turn without state", SessionID: sessionID, TurnIndex: domain.IntPtr(turn.TurnIndex), Stage: store.StageEvolve}
			}
			if domain.DigestBytes(stateJSON) != digest {
				return domain.ExportBundle{}, &domain.BusinessError{Code: domain.CodeCheckpointTampered, Message: "stored state digest mismatch", SessionID: sessionID, TurnIndex: domain.IntPtr(turn.TurnIndex), Stage: store.StageEvolve}
			}
			var state domain.TurnState
			if err := json.Unmarshal(stateJSON, &state); err != nil {
				return domain.ExportBundle{}, err
			}
			var alignment domain.AlignmentReport
			if err := json.Unmarshal(cp.Payload, &alignment); err != nil {
				return domain.ExportBundle{}, err
			}
			sessionState.Turns = append(sessionState.Turns, state)
			bundle.Alignments = append(bundle.Alignments, alignment)
		}
		cps, err := st.ListCheckpoints(ctx, sessionID)
		if err != nil {
			return domain.ExportBundle{}, err
		}
		fp, err := buildSessionFingerprint(sessionID, dataset.ActiveMap.Version, configs.global, cps)
		if err != nil {
			return domain.ExportBundle{}, err
		}
		bundle.Sessions = append(bundle.Sessions, sessionState)
		bundle.Fingerprints = append(bundle.Fingerprints, fp)
	}
	sort.Slice(bundle.Alignments, func(i, j int) bool {
		if bundle.Alignments[i].SessionID != bundle.Alignments[j].SessionID {
			return bundle.Alignments[i].SessionID < bundle.Alignments[j].SessionID
		}
		return bundle.Alignments[i].TurnIndex < bundle.Alignments[j].TurnIndex
	})
	return bundle, nil
}

func (s *Service) SessionState(ctx context.Context, sessionID string) (domain.SessionState, error) {
	dataset, err := LoadDataset(s.Root)
	if err != nil {
		return domain.SessionState{}, err
	}
	st, err := store.Open(s.Root)
	if err != nil {
		return domain.SessionState{}, err
	}
	defer st.Close()
	sessions, bySession := GroupTurns(dataset.Turns)
	if !containsString(sessions, sessionID) {
		return domain.SessionState{}, &domain.BusinessError{Code: domain.CodeInvalidRequest, Message: "unknown session", SessionID: sessionID}
	}
	out := domain.SessionState{SessionID: sessionID, MapVersion: dataset.ActiveMap.Version, Turns: []domain.TurnState{}}
	for _, turn := range bySession[sessionID] {
		blob, digest, err := st.LatestState(ctx, sessionID, turn.TurnIndex)
		if err != nil {
			return domain.SessionState{}, err
		}
		if len(blob) == 0 {
			continue
		}
		if domain.DigestBytes(blob) != digest {
			return domain.SessionState{}, &domain.BusinessError{Code: domain.CodeCheckpointTampered, Message: "state digest mismatch", SessionID: sessionID, TurnIndex: domain.IntPtr(turn.TurnIndex), Stage: store.StageEvolve}
		}
		var state domain.TurnState
		if err := json.Unmarshal(blob, &state); err != nil {
			return domain.SessionState{}, err
		}
		out.Turns = append(out.Turns, state)
	}
	return out, nil
}

func (s *Service) Alignments(ctx context.Context, sessionID string) ([]domain.AlignmentReport, error) {
	dataset, err := LoadDataset(s.Root)
	if err != nil {
		return nil, err
	}
	st, err := store.Open(s.Root)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	sessions, bySession := GroupTurns(dataset.Turns)
	if sessionID != "" && !containsString(sessions, sessionID) {
		return nil, &domain.BusinessError{Code: domain.CodeInvalidRequest, Message: "unknown session", SessionID: sessionID}
	}
	if sessionID == "" {
		sessionID = ""
	}
	var out []domain.AlignmentReport
	selected := sessions
	if sessionID != "" {
		selected = []string{sessionID}
	}
	for _, id := range selected {
		for _, turn := range bySession[id] {
			blob, _, err := st.LatestAlignment(ctx, id, turn.TurnIndex)
			if err != nil {
				return nil, err
			}
			if len(blob) == 0 {
				continue
			}
			var report domain.AlignmentReport
			if err := json.Unmarshal(blob, &report); err != nil {
				return nil, err
			}
			out = append(out, report)
		}
	}
	return out, nil
}

func (s *Service) Invalidations(ctx context.Context, sessionID string) ([]domain.InvalidationRecord, error) {
	st, err := store.Open(s.Root)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	return st.ListInvalidations(ctx, sessionID)
}

func (s *Service) Stop(ctx context.Context, reason string) error {
	st, err := store.Open(s.Root)
	if err != nil {
		return err
	}
	defer st.Close()
	return st.RequestStop(ctx, reason)
}

func (s *Service) Verify(ctx context.Context) error {
	st, err := store.Open(s.Root)
	if err != nil {
		return err
	}
	defer st.Close()
	return st.VerifyIntegrity(ctx)
}
