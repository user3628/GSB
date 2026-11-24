package motion

import (
	"math"

	"motionlab/internal/model"
)

func AdvanceTruth(state model.MotionState, spec model.SequenceSpec, step int) model.MotionState {
	next := state
	next.Step = step
	next.X += next.VX
	next.Y += next.VY
	next.ScaleX += 0.0042 + math.Abs(next.VX)*0.00022
	next.ScaleY += 0.0028 + math.Abs(next.VY)*0.00030
	next.Rotation += math.Sin(float64(step)*0.41) * 0.004
	if step == spec.DirectionChangeStep && !next.EventApplied {
		next.VX = -next.VX * 0.78
		next.VY = next.VY * 0.62
		next.Rotation += 0.14
		next.EventApplied = true
	}
	next.VX *= spec.VelocityDecay
	next.VY *= spec.VelocityDecay
	return next
}

func TruthTrajectory(spec model.SequenceSpec) []model.MotionState {
	states := make([]model.MotionState, spec.TotalFrames)
	states[0] = spec.InitialState
	for step := 1; step < spec.TotalFrames; step++ {
		states[step] = AdvanceTruth(states[step-1], spec, step)
	}
	return states
}

func ProfileFor(spec model.SequenceSpec, branchID int) model.BranchProfile {
	profiles := []model.BranchProfile{
		{BranchID: 0, VelocityScale: 1, TurnX: -0.78, TurnY: 0.62, DecayScale: 1, DeformDrift: 0,
			Description: "baseline: exact decay and post-turn hypothesis"},
		{BranchID: 1, VelocityScale: 1.014, TurnX: -0.61, TurnY: 0.48, DecayScale: 0.996, DeformDrift: 0.0011,
			Description: "soft right turn with slightly faster drift"},
		{BranchID: 2, VelocityScale: 0.982, TurnX: -0.97, TurnY: 0.30, DecayScale: 1.006, DeformDrift: -0.0013,
			Description: "hard reversal with slower lateral decay"},
		{BranchID: 3, VelocityScale: 1.042, TurnX: 0.70, TurnY: -0.34, DecayScale: 0.990, DeformDrift: 0.0021,
			Description: "wrong-direction recovery hypothesis"},
		{BranchID: 4, VelocityScale: 0.946, TurnX: -0.43, TurnY: 0.82, DecayScale: 1.012, DeformDrift: -0.0028,
			Description: "low-energy turn with strong vertical continuation"},
	}
	if branchID >= 0 && branchID < len(profiles) {
		return profiles[branchID]
	}
	r := newPRNG(spec.Seed + int64(branchID)*7919)
	sign := 1.0
	if r.intn(2) == 0 {
		sign = -1
	}
	return model.BranchProfile{
		BranchID:      branchID,
		VelocityScale: 0.94 + r.float64()*0.14,
		TurnX:         -0.35 - r.float64()*0.7,
		TurnY:         sign * (0.2 + r.float64()*0.7),
		DecayScale:    0.98 + r.float64()*0.05,
		DeformDrift:   (r.float64() - 0.5) * 0.006,
		Description:   "deterministic generated hypothesis",
	}
}

func AdvanceBranch(state model.MotionState, spec model.SequenceSpec, profile model.BranchProfile, step int) model.MotionState {
	next := state
	next.Step = step
	next.X += next.VX * profile.VelocityScale
	next.Y += next.VY * profile.VelocityScale
	next.ScaleX += 0.0042 + math.Abs(next.VX)*0.00022 + profile.DeformDrift
	next.ScaleY += 0.0028 + math.Abs(next.VY)*0.00030 - profile.DeformDrift*0.35
	next.Rotation += math.Sin(float64(step)*0.41)*0.004 + math.Abs(profile.DeformDrift)*0.03
	if step == spec.DirectionChangeStep && !next.EventApplied {
		next.VX = next.VX * profile.TurnX
		next.VY = next.VY * profile.TurnY
		next.Rotation += 0.14
		next.EventApplied = true
	}
	decay := spec.VelocityDecay * profile.DecayScale
	next.VX *= decay
	next.VY *= decay
	return next
}

func CheckRange(s model.MotionState, r model.StateRange) bool {
	return inRange(s.X, r.X) && inRange(s.Y, r.Y) && inRange(s.VX, r.VX) &&
		inRange(s.VY, r.VY) && inRange(s.ScaleX, r.ScaleX) && inRange(s.ScaleY, r.ScaleY)
}

func inRange(v float64, r model.FloatRange) bool {
	return v >= r.Min && v <= r.Max
}
