package model

// 合法迁移表：UNKNOWN 初始；探测结论驱动内部四态；DRAINING 为人工摘除入口。
var legalTransitions = map[Phase]map[Phase]bool{
	PhaseUnknown: {
		PhaseHealthy:   true,
		PhaseUnhealthy: true,
		PhaseDraining:  true,
	},
	PhaseHealthy: {
		PhaseSuspect:  true,
		PhaseDraining: true,
	},
	PhaseSuspect: {
		PhaseHealthy:   true,
		PhaseUnhealthy: true,
		PhaseDraining:  true,
	},
	PhaseUnhealthy: {
		PhaseHealthy:  true,
		PhaseSuspect:  true,
		PhaseDraining: true,
	},
	PhaseDraining: {
		// 人工摘除后只能由人工恢复入口回到 UNKNOWN，重新走探测确认。
		PhaseUnknown: true,
	},
}

// CanTransition 判断阶段迁移是否合法。同阶段重复提交（含对 DRAINING 再次摘除）
// 不构成合法迁移，必须由调用方以 E_INVALID_TRANSITION 拒绝且不修改已提交状态。
func CanTransition(from, to Phase) bool {
	if from == to {
		return false
	}
	return legalTransitions[from][to]
}

// AssertTransition 校验迁移，非法时返回 E_INVALID_TRANSITION 且不修改任何已提交状态。
func AssertTransition(from, to Phase) error {
	if !CanTransition(from, to) {
		return NewBizError(CodeInvalidTransition,
			"illegal availability phase transition",
			"from="+string(from)+" to="+string(to))
	}
	return nil
}

// IsDispatchable 声明该阶段实例是否可获得新的调度请求。
// DRAINING / UNHEALTHY 必须从分配中移除。
func IsDispatchable(p Phase) bool {
	return p == PhaseHealthy || p == PhaseSuspect
}

// StateCounter 维护连续成功/失败计数，并依据阈值给出目标阶段。
type StateCounter struct {
	Phase            Phase
	SuccessStreak    int
	FailureStreak    int
	SuccessThreshold int
	FailureThreshold int
}

// ObserveSuccess 应用一次探测成功，返回目标阶段与是否发生阶段变化。
func (c *StateCounter) ObserveSuccess() (Phase, bool) {
	c.SuccessStreak++
	c.FailureStreak = 0
	target := c.Phase
	switch c.Phase {
	case PhaseUnknown:
		if c.SuccessStreak >= c.SuccessThreshold {
			target = PhaseHealthy
		}
	case PhaseSuspect:
		if c.SuccessStreak >= c.SuccessThreshold {
			target = PhaseHealthy
		}
	case PhaseUnhealthy:
		if c.SuccessStreak >= c.SuccessThreshold {
			target = PhaseHealthy
		}
	}
	changed := target != c.Phase
	if changed {
		c.Phase = target
		c.SuccessStreak = 0
		c.FailureStreak = 0
	}
	return target, changed
}

// ObserveFailure 应用一次探测失败，返回目标阶段与是否发生阶段变化。
func (c *StateCounter) ObserveFailure() (Phase, bool) {
	c.FailureStreak++
	c.SuccessStreak = 0
	target := c.Phase
	switch c.Phase {
	case PhaseUnknown:
		if c.FailureStreak >= c.FailureThreshold {
			target = PhaseUnhealthy
		}
	case PhaseHealthy:
		if c.FailureStreak >= c.FailureThreshold {
			target = PhaseSuspect
		}
	case PhaseSuspect:
		if c.FailureStreak >= c.FailureThreshold {
			target = PhaseUnhealthy
		}
	}
	changed := target != c.Phase
	if changed {
		c.Phase = target
		c.SuccessStreak = 0
		c.FailureStreak = 0
	}
	return target, changed
}
