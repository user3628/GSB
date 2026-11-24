package domain

import (
	"fmt"
	"sort"

	"replicationlag/internal/apperr"
)

type PositionRelation string

const (
	PositionInit      PositionRelation = "INIT"
	PositionAdvance   PositionRelation = "ADVANCE"
	PositionStall     PositionRelation = "STALL"
	PositionRegressed PositionRelation = "REGRESSED"
)

type LagLevel string

const (
	LagNormal   LagLevel = "NORMAL"
	LagWarn     LagLevel = "WARN"
	LagCritical LagLevel = "CRITICAL"
)

type ReplicaState string

const (
	StateHealthy     ReplicaState = "HEALTHY"
	StateLagWarn     ReplicaState = "LAG_WARN"
	StateLagCritical ReplicaState = "LAG_CRITICAL"
	StateStalled     ReplicaState = "STALLED"
	StateRecovering  ReplicaState = "RECOVERING"
)

var AllStates = []ReplicaState{StateHealthy, StateLagWarn, StateLagCritical, StateStalled, StateRecovering}

// EvolutionStep is one sample in a replica's lag evolution sequence.
type EvolutionStep struct {
	Seq              int              `json:"seq"`
	Observation      ObservationView  `json:"observation"`
	PositionRelation PositionRelation `json:"position_relation"`
	PreviousPosition string           `json:"previous_position,omitempty"`
	AddedIntervals   []SourceSet      `json:"added_intervals"`
	RevokedIntervals []SourceSet      `json:"revoked_intervals"`
	Crossed          bool             `json:"crossed"`
	StallCount       int              `json:"stall_count"`
	LagLevel         LagLevel         `json:"lag_level"`
	PreviousState    ReplicaState     `json:"previous_state"`
	State            ReplicaState     `json:"state"`
	TransitionBasis  []string         `json:"transition_basis"`
	RecoveryStreak   int              `json:"recovery_streak"`
	SuggestedAction  string           `json:"suggested_action"`
}

// EvolutionSequence is the derived sequence for one replica.
type EvolutionSequence struct {
	ReplicaID       string          `json:"replica_id"`
	Steps           []EvolutionStep `json:"steps"`
	CurrentState    ReplicaState    `json:"current_state"`
	LastSampledAtMS int64           `json:"last_sampled_at_ms"`
	StallCount      int             `json:"stall_count"`
	CurrentLagLevel LagLevel        `json:"current_lag_level"`
	SuggestedAction string          `json:"suggested_action"`
	RecoveryStreak  int             `json:"recovery_streak"`
}

// MergeReport summarizes one deterministic merge pass.
type MergeReport struct {
	ReplicaCount     int                 `json:"replica_count"`
	ObservationCount int                 `json:"observation_count"`
	Sequences        []EvolutionSequence `json:"sequences"`
	StateTransitions int                 `json:"state_transitions"`
}

// SortObservations establishes the deterministic merge order. Same-time
// observations are ordered by canonical position only so they can be rejected
// consistently as conflicts later.
func SortObservations(observations []Observation) []Observation {
	out := append([]Observation(nil), observations...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ReplicaID != out[j].ReplicaID {
			return out[i].ReplicaID < out[j].ReplicaID
		}
		if out[i].SampledAtMS != out[j].SampledAtMS {
			return out[i].SampledAtMS < out[j].SampledAtMS
		}
		return out[i].GTID.Normalized().String() < out[j].GTID.Normalized().String()
	})
	return out
}

// BuildSequences normalizes, orders and derives all replica evolution sequences.
// A same-time conflict aborts the entire build.
func BuildSequences(observations []Observation, policy Policy) (MergeReport, error) {
	if err := policy.Validate(); err != nil {
		return MergeReport{}, apperr.New(apperr.CodeBadRequest, "invalid lag policy", map[string]any{"detail": err.Error()})
	}
	normalized := make([]Observation, len(observations))
	for i, observation := range observations {
		observation.GTID = observation.GTID.Normalized()
		observation.PositionName = canonicalPositionOrRaw(observation.PositionName)
		if err := observation.Validate(); err != nil {
			return MergeReport{}, err
		}
		normalized[i] = observation
	}
	normalized = SortObservations(normalized)

	// Reject conflicting snapshots at the same replica/sample time before any
	// sequence calculation. Identical duplicates are collapsed.
	deduped := make([]Observation, 0, len(normalized))
	for i := 0; i < len(normalized); {
		j := i + 1
		for j < len(normalized) && normalized[j].ReplicaID == normalized[i].ReplicaID && normalized[j].SampledAtMS == normalized[i].SampledAtMS {
			if normalized[j].ContentFingerprint() != normalized[i].ContentFingerprint() {
				return MergeReport{}, apperr.New(apperr.CodeObservationConflict, "conflicting observations share a replica and sample time", map[string]any{
					"replica_id":    normalized[i].ReplicaID,
					"sampled_at_ms": normalized[i].SampledAtMS,
					"fingerprints":  []string{normalized[i].ContentFingerprint(), normalized[j].ContentFingerprint()},
				})
			}
			j++
		}
		deduped = append(deduped, normalized[i])
		i = j
	}

	report := MergeReport{ObservationCount: len(deduped)}
	for i := 0; i < len(deduped); {
		j := i + 1
		for j < len(deduped) && deduped[j].ReplicaID == deduped[i].ReplicaID {
			j++
		}
		sequence, err := buildReplicaSequence(deduped[i:j], policy)
		if err != nil {
			return MergeReport{}, err
		}
		report.Sequences = append(report.Sequences, sequence)
		i = j
	}
	report.ReplicaCount = len(report.Sequences)
	for _, sequence := range report.Sequences {
		for _, step := range sequence.Steps {
			if step.PreviousState != step.State {
				report.StateTransitions++
			}
		}
	}
	return report, nil
}

func canonicalPositionOrRaw(position string) string {
	if canonical, err := normalizePosition(position); err == nil {
		return canonical
	}
	return position
}

func buildReplicaSequence(observations []Observation, policy Policy) (EvolutionSequence, error) {
	sequence := EvolutionSequence{ReplicaID: observations[0].ReplicaID}
	state := StateHealthy
	stallCount := 0
	recoveryStreak := 0
	var previous *Observation
	for index, observation := range observations {
		step := EvolutionStep{
			Seq:              index + 1,
			Observation:      observation.View(),
			PreviousState:    state,
			AddedIntervals:   []SourceSet{},
			RevokedIntervals: []SourceSet{},
		}
		if previous == nil {
			step.PositionRelation = PositionInit
			step.TransitionBasis = append(step.TransitionBasis, "first observation for replica initializes evolution at HEALTHY")
		} else {
			step.PreviousPosition = previous.GTID.Normalized().String()
			added := observation.GTID.Difference(previous.GTID)
			revoked := previous.GTID.Difference(observation.GTID)
			step.AddedIntervals = added.Sources
			step.RevokedIntervals = revoked.Sources
			switch {
			case observation.GTID.Equal(previous.GTID):
				step.PositionRelation = PositionStall
				stallCount++
				step.TransitionBasis = append(step.TransitionBasis, fmt.Sprintf("normalized GTID set is unchanged; consecutive stalls=%d", stallCount))
			case observation.GTID.IsSuperset(previous.GTID):
				step.PositionRelation = PositionAdvance
				stallCount = 0
				step.Crossed = crossedGap(previous.GTID, observation.GTID)
				step.TransitionBasis = append(step.TransitionBasis, fmt.Sprintf("GTID set advanced by %d transactions; consecutive stalls reset", added.Cardinality()))
				if step.Crossed {
					step.TransitionBasis = append(step.TransitionBasis, "advance skips at least one transaction number in an existing source")
				}
			default:
				step.PositionRelation = PositionRegressed
				stallCount = 0
				step.TransitionBasis = append(step.TransitionBasis, fmt.Sprintf("GTID set revoked %d transactions; consecutive stalls reset", revoked.Cardinality()))
			}
		}
		step.StallCount = stallCount
		step.LagLevel = lagLevel(observation.LagMS, policy)
		step.TransitionBasis = append(step.TransitionBasis, fmt.Sprintf("lag_ms=%d, warn=%d, critical=%d => %s", observation.LagMS, policy.LagWarnMS, policy.LagCriticalMS, step.LagLevel))

		nextState, nextRecoveryStreak, basis, err := nextState(state, step, recoveryStreak, policy)
		if err != nil {
			return EvolutionSequence{}, err
		}
		step.TransitionBasis = append(step.TransitionBasis, basis...)
		step.State = nextState
		step.RecoveryStreak = nextRecoveryStreak
		step.SuggestedAction = suggestedAction(nextState)
		sequence.Steps = append(sequence.Steps, step)
		state = nextState
		recoveryStreak = nextRecoveryStreak
		previous = &observation
	}
	sequence.CurrentState = state
	sequence.StallCount = stallCount
	sequence.RecoveryStreak = recoveryStreak
	last := sequence.Steps[len(sequence.Steps)-1]
	sequence.LastSampledAtMS = last.Observation.SampledAtMS
	sequence.CurrentLagLevel = last.LagLevel
	sequence.SuggestedAction = last.SuggestedAction
	return sequence, nil
}

func lagLevel(lagMS int64, policy Policy) LagLevel {
	if lagMS > policy.LagCriticalMS {
		return LagCritical
	}
	if lagMS > policy.LagWarnMS {
		return LagWarn
	}
	return LagNormal
}

func nextState(current ReplicaState, step EvolutionStep, recoveryStreak int, policy Policy) (ReplicaState, int, []string, error) {
	// A regression is never a recovery signal. It is treated as a stalled
	// replica because its durable position moved backwards.
	if step.PositionRelation == PositionRegressed {
		return transition(current, StateStalled, []string{"position regression parks replica in STALLED until forward progress resumes"})
	}
	if step.PositionRelation == PositionStall && step.StallCount > policy.StallThreshold {
		return transition(current, StateStalled, []string{fmt.Sprintf("consecutive stalls %d exceeds threshold %d", step.StallCount, policy.StallThreshold)})
	}

	if current == StateRecovering {
		healthySample := step.PositionRelation == PositionAdvance && step.LagLevel == LagNormal
		if healthySample {
			recoveryStreak++
			basis := []string{fmt.Sprintf("recovery sample %d/%d has forward position and lag below critical", recoveryStreak, policy.RecoveryObservations)}
			if recoveryStreak >= policy.RecoveryObservations {
				if err := ValidateTransition(current, StateHealthy); err != nil {
					return current, recoveryStreak, nil, err
				}
				basis = append(basis, "state transition RECOVERING -> HEALTHY", "recovery streak satisfied")
				return StateHealthy, recoveryStreak, basis, nil
			}
			return current, recoveryStreak, append(basis, "remain RECOVERING until recovery streak satisfied"), nil
		}
		recoveryStreak = 0
		switch step.LagLevel {
		case LagCritical:
			return transition(current, StateLagCritical, []string{"lag rose above critical during recovery"})
		case LagWarn:
			return transition(current, StateLagWarn, []string{"lag rose above warning threshold during recovery"})
		default:
			if step.PositionRelation == PositionStall {
				return current, 0, []string{"position stalled during recovery; recovery streak reset"}, nil
			}
			return current, 0, []string{"no forward progress during recovery; recovery streak reset"}, nil
		}
	}

	if current == StateStalled {
		if step.PositionRelation == PositionAdvance && step.LagLevel == LagNormal {
			if policy.RecoveryObservations <= 1 {
				return transition(current, StateHealthy, []string{"forward progress with normal lag satisfies single-sample recovery policy"})
			}
			return StateRecovering, 1, []string{fmt.Sprintf("forward progress with normal lag starts recovery streak 1/%d", policy.RecoveryObservations)}, nil
		}
		return StateStalled, 0, []string{fmt.Sprintf("STALLED remains in force (position=%s, lag=%s)", step.PositionRelation, step.LagLevel)}, nil
	}

	switch step.LagLevel {
	case LagCritical:
		return transition(current, StateLagCritical, []string{"lag exceeds critical threshold"})
	case LagWarn:
		return transition(current, StateLagWarn, []string{"lag exceeds warning threshold"})
	}
	if current == StateHealthy {
		return StateHealthy, 0, []string{"lag and position remain healthy"}, nil
	}
	if step.PositionRelation == PositionAdvance {
		if policy.RecoveryObservations <= 1 {
			return transition(current, StateHealthy, []string{"forward position and normal lag satisfy single-sample recovery policy"})
		}
		return StateRecovering, 1, []string{fmt.Sprintf("unhealthy replica shows forward position and normal lag; recovery streak 1/%d", policy.RecoveryObservations)}, nil
	}
	return current, 0, []string{"normal lag without forward progress; state unchanged"}, nil
}

func transition(from, to ReplicaState, basis []string) (ReplicaState, int, []string, error) {
	if err := ValidateTransition(from, to); err != nil {
		return from, 0, nil, err
	}
	basis = append(basis, fmt.Sprintf("state transition %s -> %s", from, to))
	return to, 0, basis, nil
}

// ValidateTransition enforces the declared state machine graph.
func ValidateTransition(from, to ReplicaState) error {
	if from == to {
		return nil
	}
	allowed := map[ReplicaState]map[ReplicaState]bool{
		StateHealthy:     {StateLagWarn: true, StateLagCritical: true, StateStalled: true},
		StateLagWarn:     {StateHealthy: true, StateLagCritical: true, StateStalled: true, StateRecovering: true},
		StateLagCritical: {StateLagWarn: true, StateStalled: true, StateRecovering: true},
		StateStalled:     {StateRecovering: true},
		StateRecovering:  {StateHealthy: true, StateLagWarn: true, StateLagCritical: true, StateStalled: true},
	}
	if allowed[from][to] {
		return nil
	}
	return apperr.New(apperr.CodeInvalidTransition, "replica state transition is not allowed", map[string]any{
		"from": from,
		"to":   to,
	})
}

func crossedGap(previous, current GTIDSet) bool {
	prevBySource := map[string][]Interval{}
	for _, src := range previous.Normalized().Sources {
		prevBySource[src.Source] = src.Intervals
	}
	for _, src := range current.Difference(previous).Normalized().Sources {
		old, ok := prevBySource[src.Source]
		if !ok || len(old) == 0 {
			continue
		}
		oldMax := old[len(old)-1].End
		for _, added := range src.Intervals {
			if oldMax != ^uint64(0) && added.Start > oldMax+1 {
				return true
			}
		}
	}
	return false
}

func suggestedAction(state ReplicaState) string {
	switch state {
	case StateHealthy:
		return "CONTINUE_MONITORING"
	case StateLagWarn:
		return "INSPECT_REPLAY_THROUGHPUT"
	case StateLagCritical:
		return "SHIFT_READ_TRAFFIC_AND_INVESTIGATE"
	case StateStalled:
		return "INVESTIGATE_REPLICATION_BREAK_AND_ALERT"
	case StateRecovering:
		return "HOLD_TRAFFIC_SHIFT_UNTIL_RECOVERY_CONFIRMED"
	default:
		return "INVESTIGATE"
	}
}
