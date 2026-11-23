package pipeline

import (
	"fmt"

	"mitmpin/internal/sample"
)

// applyFaults 依据注入规格构造故障输入。注入只作用于批次深拷贝副本，
// 不修改源码、不污染调用方输入；不同连接通过策略副本相互隔离。
func applyFaults(b *sample.Batch) map[string]string {
	faultCodes := make(map[string]string)
	faultByConn := make(map[string]sample.FaultSpec)
	for _, fault := range b.Faults {
		faultByConn[fault.Target] = fault
		faultCodes[fault.Target] = expectedFaultCode(fault.Kind)
	}

	// 为需要策略篡改的连接构造独立策略快照副本，按原快照版本号映射。
	policyOverrides := make(map[string]map[int]sample.Policy) // connID -> version -> policy
	for connID, fault := range faultByConn {
		switch fault.Kind {
		case FaultPinMismatch:
			policyOverrides[connID] = brokenPinOverrides(b, fault)
		case FaultHotUpdateRace:
			insertRaceUpdate(b, connID)
		}
	}

	for i := range b.Observations {
		obs := &b.Observations[i]
		fault, ok := faultByConn[obs.ConnID]
		if !ok {
			continue
		}
		switch fault.Kind {
		case FaultChainBroken:
			obs.ChainPEM = removeIntermediate(obs.ChainPEM)
		case FaultEventSeqGap:
			if len(obs.Events) >= 3 {
				obs.Events[2].Seq = obs.Events[2].Seq + 10
				for j := 3; j < len(obs.Events); j++ {
					obs.Events[j].Seq = obs.Events[j-1].Seq + 1
				}
			}
		}
	}

	// 把逐连接策略覆盖应用到绑定层：Run 中通过 bindingWithOverrides 取用。
	policyOverridesByConn = policyOverrides
	return faultCodes
}

// expectedFaultCode 返回五类故障对应的预期业务码。
func expectedFaultCode(kind string) string {
	switch kind {
	case FaultPinMismatch:
		return sample.E_MITM_SUSPECTED
	case FaultHotUpdateRace:
		return sample.E_OK // 连接沿用握手开始时策略，固定校验仍通过
	case FaultChainBroken:
		return sample.E_CHAIN_BROKEN
	case FaultEventSeqGap:
		return sample.E_EVENT_SEQ_GAP
	case FaultSnapshotVersionGap:
		return sample.E_SNAPSHOT_VERSION
	default:
		return sample.E_OK
	}
}

// brokenPinOverrides 为目标连接构造每个快照版本的“坏固定项”策略副本。
func brokenPinOverrides(b *sample.Batch, fault sample.FaultSpec) map[int]sample.Policy {
	policyID := fault.PolicyID
	if policyID == "" {
		policyID = targetPolicyID(b, fault.Target)
	}
	overrides := make(map[int]sample.Policy)
	collect := func(snap sample.Snapshot) {
		for _, pol := range snap.Policies {
			if pol.PolicyID != policyID || pol.Mode != sample.PinModeSPKI {
				continue
			}
			bad := pol
			bad.Pins = append([]sample.PinItem(nil), pol.Pins...)
			for idx := range bad.Pins {
				bad.Pins[idx] = sample.PinItem{
					ID: bad.Pins[idx].ID, Algo: "sha256",
					Digest: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
				}
			}
			overrides[snap.Version] = bad
		}
	}
	for _, snap := range b.Snapshots {
		collect(snap)
	}
	for _, update := range b.Updates {
		collect(update.Snapshot)
	}
	return overrides
}

func targetPolicyID(b *sample.Batch, connID string) string {
	for _, obs := range b.Observations {
		if obs.ConnID == connID {
			return obs.PolicyID
		}
	}
	return ""
}

// insertRaceUpdate 在目标连接“开始之后、固定校验之前”插入一个高版本整体替换快照。
// 该连接已进入握手阶段，必须沿用开始时绑定版本；其后新连接采用新版本。
func insertRaceUpdate(b *sample.Batch, connID string) {
	order := 0
	for i, candidate := range b.Observations {
		if candidate.ConnID == connID {
			order = i
			break
		}
	}
	maxVersion := 0
	for _, snap := range b.Snapshots {
		if snap.Version > maxVersion {
			maxVersion = snap.Version
		}
	}
	for _, update := range b.Updates {
		if update.Snapshot.Version > maxVersion {
			maxVersion = update.Snapshot.Version
		}
	}
	policyID := targetPolicyID(b, connID)
	raceSnap := sample.Snapshot{
		Version: maxVersion + 1,
		Marker:  fmt.Sprintf("marker-race-snapshot-v%d", maxVersion+1),
		Policies: []sample.Policy{{
			PolicyID:        policyID,
			Mode:            sample.PinModeSPKI,
			AllowedVersions: []string{"tls1.3"},
			Pins: []sample.PinItem{{
				ID: "race-only-pin", Algo: "sha256",
				Digest: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=",
			}},
		}},
	}
	// AfterSeq=order+1：目标连接（order）之后生效，目标连接自身仍绑定旧版本。
	b.Updates = append(b.Updates, sample.UpdateStep{AfterSeq: order + 1, Snapshot: raceSnap})
}

// policyOverridesByConn 保存本轮注入产生的逐连接策略覆盖。
var policyOverridesByConn map[string]map[int]sample.Policy

// bindingFor 返回某连接实际绑定的快照，应用故障注入产生的策略覆盖副本。
func bindingFor(connID string, bound sample.Snapshot) sample.Snapshot {
	if overrides, ok := policyOverridesByConn[connID]; ok {
		if badPolicy, ok := overrides[bound.Version]; ok {
			copied := bound
			policies := append([]sample.Policy(nil), bound.Policies...)
			for i := range policies {
				if policies[i].PolicyID == badPolicy.PolicyID {
					policies[i] = badPolicy
				}
			}
			copied.Policies = policies
			return copied
		}
	}
	return bound
}
