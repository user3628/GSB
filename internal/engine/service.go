package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"dialogalign/internal/domain"
	"dialogalign/internal/store"
	"dialogalign/internal/upgrade"
)

type Service struct {
	Root string
}

func New(root string) *Service { return &Service{Root: root} }

type RunOptions struct {
	ClearStop      bool
	Reset          bool
	RunMode        string
	StopAfterTurns int
}

type executor struct {
	service   *Service
	store     *store.Store
	dataset   domain.Dataset
	resolver  *upgrade.Resolver
	configs   stageConfigs
	report    domain.RunReport
	sessions  []string
	bySession map[string][]domain.TurnInput
	stopAfter int
}

func (s *Service) Run(ctx context.Context, opts RunOptions) (domain.RunReport, error) {
	dataset, err := LoadDataset(s.Root)
	if err != nil {
		return domain.RunReport{}, err
	}
	resolver, err := upgrade.NewResolver(dataset.ActiveMap, dataset.Maps, dataset.Catalog, dataset.Config.Normalization)
	if err != nil {
		return domain.RunReport{}, err
	}
	configs, err := buildStageConfigs(dataset)
	if err != nil {
		return domain.RunReport{}, err
	}
	st, err := store.Open(s.Root)
	if err != nil {
		return domain.RunReport{}, err
	}
	defer st.Close()
	if opts.Reset {
		if err := st.Reset(ctx); err != nil {
			return domain.RunReport{}, err
		}
	}
	if opts.ClearStop {
		if err := st.ClearStop(ctx); err != nil {
			return domain.RunReport{}, err
		}
	}
	existingSessions, err := st.Sessions(ctx)
	if err != nil {
		return domain.RunReport{}, err
	}
	mode := opts.RunMode
	if mode == "" {
		if len(existingSessions) == 0 {
			mode = "full"
		} else {
			mode = "incremental"
		}
	}
	sessions, bySession := GroupTurns(dataset.Turns)
	ex := &executor{
		service:   s,
		store:     st,
		dataset:   dataset,
		resolver:  resolver,
		configs:   configs,
		sessions:  sessions,
		bySession: bySession,
		report: domain.RunReport{
			RunMode:      mode,
			MapVersion:   dataset.ActiveMap.Version,
			ConfigDigest: configs.global,
			Sessions:     sessions,
			Completed:    []domain.TurnRef{},
		},
		stopAfter: opts.StopAfterTurns,
	}
	return ex.run(ctx)
}

func (e *executor) run(ctx context.Context) (domain.RunReport, error) {
	for _, sessionID := range e.sessions {
		var previous *domain.TurnState
		turns := e.bySession[sessionID]
		for i, turn := range turns {
			stop, _, err := e.store.StopRequested(ctx)
			if err != nil {
				return e.report, err
			}
			if stop {
				e.report.Stopped = true
				return e.report, nil
			}
			state, turnReused, turnRebuilt, err := e.runTurn(ctx, turn, previous, i == len(turns)-1)
			if err != nil {
				return e.report, err
			}
			previous = &state
			e.report.CompletedTurns++
			e.report.Completed = append(e.report.Completed, domain.TurnRef{SessionID: sessionID, TurnIndex: turn.TurnIndex})
			if turnReused {
				e.report.ReusedTurns++
			}
			if turnRebuilt {
				e.report.RebuiltTurns++
			}
			stop, _, err = e.store.StopRequested(ctx)
			if err != nil {
				return e.report, err
			}
			if stop {
				// The current turn is an indivisible unit and has reached align.
				e.report.Stopped = true
				return e.report, nil
			}
			if e.stopAfter > 0 && e.report.CompletedTurns >= e.stopAfter {
				if err := e.store.RequestStop(ctx, fmt.Sprintf("test/controlled stop after %d turns", e.stopAfter)); err != nil {
					return e.report, err
				}
				e.report.Stopped = true
				return e.report, nil
			}
		}
	}
	return e.report, nil
}

func (e *executor) runTurn(ctx context.Context, turn domain.TurnInput, previous *domain.TurnState, finalTurn bool) (domain.TurnState, bool, bool, error) {
	reusedCount := 0
	rebuilt := false

	parseInput, err := parseInputDigest(turn)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	parseArtifact := domain.ParseArtifact{Turn: turn}
	parsePayload, err := domain.CanonicalJSON(parseArtifact)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	parseOutput := domain.DigestBytes(parsePayload)
	parseReused, err := e.ensureCheckpoint(ctx, turn.SessionID, turn.TurnIndex, store.StageParse, parseInput, parseOutput, parsePayload, "parse content fingerprint changed", &parseArtifact, nil, nil)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	if parseReused {
		reusedCount++
	} else {
		rebuilt = true
	}

	upgradeInput, err := upgradeInputDigest(parseOutput, e.configs)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	upgraded, err := e.resolver.UpgradeTurn(turn)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	upgradePayload, err := domain.CanonicalJSON(upgraded)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	upgradeOutput := domain.DigestBytes(upgradePayload)
	upgradeReused, err := e.ensureCheckpoint(ctx, turn.SessionID, turn.TurnIndex, store.StageUpgrade, upgradeInput, upgradeOutput, upgradePayload, "mapping or normalization fingerprint changed", &upgraded, nil, nil)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	if upgradeReused {
		reusedCount++
	} else {
		rebuilt = true
	}

	prevDigest := ""
	if previous != nil {
		prevDigest, err = domain.Digest(*previous)
		if err != nil {
			return domain.TurnState{}, false, false, err
		}
	}
	evolveInput, err := evolveInputDigest(upgradeOutput, prevDigest, e.configs, finalTurn)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	state, err := Evolve(previous, turn, upgraded, e.dataset.Config, e.dataset.Catalog, finalTurn)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	statePayload, err := domain.CanonicalJSON(state)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	stateDigest := domain.DigestBytes(statePayload)
	evolveReused, err := e.ensureCheckpoint(ctx, turn.SessionID, turn.TurnIndex, store.StageEvolve, evolveInput, stateDigest, statePayload, "intent rules or upstream state fingerprint changed", &state, statePayload, &stateDigest)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	if evolveReused {
		reusedCount++
	} else {
		rebuilt = true
	}

	previousStates, err := e.previousStates(ctx, turn.SessionID, turn.TurnIndex, stateDigest)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	alignReport := Align(state, turn, upgraded, e.dataset.Catalog, e.dataset.Config, previousStates)
	alignPayload, err := domain.CanonicalJSON(alignReport)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	alignOutput := domain.DigestBytes(alignPayload)
	alignInput, err := alignInputDigest(stateDigest, e.configs)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	alignReused, err := e.ensureCheckpoint(ctx, turn.SessionID, turn.TurnIndex, store.StageAlign, alignInput, alignOutput, alignPayload, "alignment threshold or upstream validation fingerprint changed", &alignReport, nil, nil)
	if err != nil {
		return domain.TurnState{}, false, false, err
	}
	if alignReused {
		reusedCount++
	} else {
		rebuilt = true
	}
	return state, reusedCount == 4, rebuilt, nil
}

// ensureCheckpoint reuses a valid checkpoint, or atomically invalidates the
// affected boundary and publishes the freshly computed artifact. The artifact
// arguments decode an existing checkpoint, not recomputation.
func (e *executor) ensureCheckpoint(ctx context.Context, sessionID string, turn int, stage, inputDigest, outputDigest string, payload []byte, reason string, target any, stateJSON []byte, stateDigest *string) (bool, error) {
	existing, ok, err := e.store.ReadCheckpoint(ctx, sessionID, turn, stage)
	if err != nil {
		return false, err
	}
	if ok && existing.InputDigest == inputDigest {
		if err := json.Unmarshal(existing.Payload, target); err != nil {
			return false, err
		}
		return true, nil
	}
	if ok {
		head, err := e.store.HeadSeq(ctx)
		if err != nil {
			return false, err
		}
		result, err := e.store.InvalidateFrom(ctx, sessionID, turn, stage, reason, inputDigest, outputDigest, head)
		if err != nil {
			return false, err
		}
		e.report.Invalidations = append(e.report.Invalidations, domain.InvalidationRecord{
			Seq:             result.Seq,
			SessionID:       sessionID,
			TurnIndex:       turn,
			Stage:           stage,
			Reason:          reason,
			OldInputDigest:  existing.InputDigest,
			NewInputDigest:  inputDigest,
			OldOutputDigest: existing.OutputDigest,
			NewOutputDigest: outputDigest,
		})
	}
	head, err := e.store.HeadSeq(ctx)
	if err != nil {
		return false, err
	}
	req := store.PublishRequest{
		Checkpoint: store.Checkpoint{
			SessionID:    sessionID,
			TurnIndex:    turn,
			Stage:        stage,
			InputDigest:  inputDigest,
			OutputDigest: outputDigest,
			MapVersion:   e.dataset.ActiveMap.Version,
			Payload:      payload,
		},
		ExpectedSeq: head,
	}
	if stage == store.StageEvolve {
		if stateJSON == nil || stateDigest == nil {
			return false, fmt.Errorf("evolve checkpoint missing state artifact")
		}
		req.StateJSON = stateJSON
		req.StateDigest = *stateDigest
	}
	if stage == store.StageAlign {
		req.AlignmentJSON = payload
		req.AlignmentDigest = outputDigest
	}
	if _, err := e.store.Publish(ctx, req); err != nil {
		return false, err
	}
	return false, nil
}

func (e *executor) previousStates(ctx context.Context, sessionID string, turn int, currentDigest string) ([]PreviousSnapshot, error) {
	historical, err := e.store.PreviousState(ctx, sessionID, turn, currentDigest)
	if err != nil || historical == nil {
		return nil, err
	}
	var state domain.TurnState
	if err := json.Unmarshal(historical.StateJSON, &state); err != nil {
		return nil, err
	}
	return []PreviousSnapshot{{State: state, Digest: historical.StateDigest, MapVersion: historical.MapVersion}}, nil
}

func (s *Service) Fingerprints(ctx context.Context, sessionID string) (domain.SessionFingerprint, error) {
	dataset, err := LoadDataset(s.Root)
	if err != nil {
		return domain.SessionFingerprint{}, err
	}
	configs, err := buildStageConfigs(dataset)
	if err != nil {
		return domain.SessionFingerprint{}, err
	}
	st, err := store.Open(s.Root)
	if err != nil {
		return domain.SessionFingerprint{}, err
	}
	defer st.Close()
	checkpoints, err := st.ListCheckpoints(ctx, sessionID)
	if err != nil {
		return domain.SessionFingerprint{}, err
	}
	if len(checkpoints) == 0 {
		return domain.SessionFingerprint{SessionID: sessionID, MapVersion: dataset.ActiveMap.Version, ConfigDigest: configs.global, Turns: []domain.TurnFingerprint{}}, nil
	}
	return buildSessionFingerprint(sessionID, dataset.ActiveMap.Version, configs.global, checkpoints)
}

func buildSessionFingerprint(sessionID, mapVersion, configDigest string, cps []store.Checkpoint) (domain.SessionFingerprint, error) {
	byTurn := map[int][]store.Checkpoint{}
	turnIndices := []int{}
	for _, cp := range cps {
		if _, ok := byTurn[cp.TurnIndex]; !ok {
			turnIndices = append(turnIndices, cp.TurnIndex)
		}
		byTurn[cp.TurnIndex] = append(byTurn[cp.TurnIndex], cp)
	}
	sort.Ints(turnIndices)
	fp := domain.SessionFingerprint{SessionID: sessionID, MapVersion: mapVersion, ConfigDigest: configDigest, Turns: []domain.TurnFingerprint{}}
	for _, turn := range turnIndices {
		stageFPs := []domain.StageFingerprint{}
		for _, cp := range byTurn[turn] {
			stageFPs = append(stageFPs, domain.StageFingerprint{Stage: cp.Stage, InputDigest: cp.InputDigest, OutputDigest: cp.OutputDigest})
		}
		combined, err := domain.Digest(stageFPs)
		if err != nil {
			return domain.SessionFingerprint{}, err
		}
		fp.Turns = append(fp.Turns, domain.TurnFingerprint{SessionID: sessionID, TurnIndex: turn, Stages: stageFPs, Combined: combined})
	}
	all, err := domain.Digest(struct {
		SessionID string                   `json:"session_id"`
		Map       string                   `json:"map_version"`
		Config    string                   `json:"config_digest"`
		Turns     []domain.TurnFingerprint `json:"turns"`
	}{sessionID, mapVersion, configDigest, fp.Turns})
	if err != nil {
		return domain.SessionFingerprint{}, err
	}
	fp.Combined = all
	return fp, nil
}
