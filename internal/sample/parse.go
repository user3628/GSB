package sample

import (
	"fmt"
	"sort"

	"mitmpin/internal/certutil"
)

// ValidateSnapshot 校验单个策略快照：版本号、策略标识唯一性、哈希算法受支持。
func ValidateSnapshot(s Snapshot) error {
	if s.Version <= 0 {
		return NewBizError(E_POLICY_VERSION_CONFLICT, fmt.Sprintf("快照版本号须为正数: %d", s.Version))
	}
	seen := make(map[string]bool, len(s.Policies))
	for _, policy := range s.Policies {
		if policy.PolicyID == "" {
			return NewBizError(E_POLICY_MISSING, "存在空策略标识")
		}
		if seen[policy.PolicyID] {
			return NewBizError(E_POLICY_MISSING, fmt.Sprintf("快照 v%d 内策略标识重复: %s", s.Version, policy.PolicyID))
		}
		seen[policy.PolicyID] = true
		switch policy.Mode {
		case PinModeSPKI:
			for _, pin := range policy.Pins {
				if !certutil.SupportedAlgo(pin.Algo) {
					return NewBizError(E_HASH_ALGO_UNSUPPORTED,
						fmt.Sprintf("策略 %s 固定项 %s 算法 %q 不受支持", policy.PolicyID, pin.ID, pin.Algo))
				}
				if pin.Digest == "" {
					return NewBizError(E_POLICY_MISSING, fmt.Sprintf("策略 %s 固定项 %s 摘要为空", policy.PolicyID, pin.ID))
				}
			}
		case PinModeRoot:
			for _, pin := range policy.Pins {
				if _, err := certutil.ParseCert(pin.RootPEM); err != nil {
					return NewBizError(E_CHAIN_BROKEN,
						fmt.Sprintf("策略 %s 受信根 %s PEM 无效: %v", policy.PolicyID, pin.ID, err))
				}
			}
		default:
			return NewBizError(E_POLICY_MISSING, fmt.Sprintf("策略 %s 模式未知: %s", policy.PolicyID, policy.Mode))
		}
		if len(policy.AllowedVersions) == 0 {
			return NewBizError(E_POLICY_MISSING, fmt.Sprintf("策略 %s 未声明允许协议版本", policy.PolicyID))
		}
	}
	return nil
}

// policyIndex 建立某快照内 policyID -> Policy 的映射。
func policyIndex(s Snapshot) map[string]Policy {
	idx := make(map[string]Policy, len(s.Policies))
	for _, policy := range s.Policies {
		idx[policy.PolicyID] = policy
	}
	return idx
}

// ValidateObservation 校验单条观测：策略引用存在、事件序号连续、证书链结构完好。
// snap 为该连接绑定的策略快照。
func ValidateObservation(obs Observation, snap Snapshot) error {
	idx := policyIndex(snap)
	if _, ok := idx[obs.PolicyID]; !ok {
		return NewBizError(E_POLICY_MISSING,
			fmt.Sprintf("连接 %s 引用的策略 %s 在快照 v%d 中缺失", obs.ConnID, obs.PolicyID, snap.Version))
	}
	if err := validateEventSeq(obs); err != nil {
		return err
	}
	if _, err := certutil.ParseChain(obs.ChainPEM); err != nil {
		return NewBizError(E_CHAIN_BROKEN, fmt.Sprintf("连接 %s 证书链结构损坏: %v", obs.ConnID, err))
	}
	return nil
}

func validateEventSeq(obs Observation) error {
	if len(obs.Events) == 0 {
		return NewBizError(E_EVENT_SEQ_GAP, fmt.Sprintf("连接 %s 事件序列为空", obs.ConnID))
	}
	for i, ev := range obs.Events {
		want := i + 1
		if ev.Seq != want {
			return NewBizError(E_EVENT_SEQ_GAP,
				fmt.Sprintf("连接 %s 事件序号跳变: 期望 %d 实际 %d", obs.ConnID, want, ev.Seq))
		}
		if !knownEventType(ev.Type) {
			return NewBizError(E_EVENT_SEQ_GAP, fmt.Sprintf("连接 %s 事件类型未知: %s", obs.ConnID, ev.Type))
		}
	}
	return nil
}

func knownEventType(t string) bool {
	switch t {
	case EvHandshakeStart, EvCertReceived, EvPinCheck, EvHandshakeDone, EvAborted:
		return true
	}
	return false
}

// ValidateBatch 做批次级结构校验：快照版本递增、每个快照自身合法。
func ValidateBatch(b Batch) error {
	if len(b.Snapshots) == 0 {
		return NewBizError(E_POLICY_MISSING, "批次缺少初始策略快照")
	}
	versions := make([]int, 0, len(b.Snapshots))
	seen := make(map[int]bool)
	for _, snap := range b.Snapshots {
		if err := ValidateSnapshot(snap); err != nil {
			return err
		}
		if seen[snap.Version] {
			return NewBizError(E_POLICY_VERSION_CONFLICT, fmt.Sprintf("快照版本号重复: %d", snap.Version))
		}
		seen[snap.Version] = true
		versions = append(versions, snap.Version)
	}
	sort.Ints(versions)
	for i := 1; i < len(versions); i++ {
		if versions[i] <= versions[i-1] {
			return NewBizError(E_POLICY_VERSION_CONFLICT, "快照版本号未严格递增")
		}
	}
	for _, update := range b.Updates {
		if err := ValidateSnapshot(update.Snapshot); err != nil {
			return err
		}
	}
	return nil
}
