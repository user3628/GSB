package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"mitmpin/internal/sample"
)

// CanonicalPins 返回排序后的固定项摘要，保证 map/切片顺序不影响内容哈希。
func canonicalPins(pins []sample.PinItem) []string {
	parts := make([]string, 0, len(pins))
	for _, pin := range pins {
		if pin.Digest != "" {
			parts = append(parts, pin.ID+"|"+pin.Algo+"|"+pin.Digest)
		} else {
			d := sha256.Sum256([]byte(pin.RootPEM))
			parts = append(parts, pin.ID+"|root|"+hex.EncodeToString(d[:]))
		}
	}
	sort.Strings(parts)
	return parts
}

// ContentHash 计算快照策略集合的规范化内容哈希（与版本号、marker 无关）。
// 只要任一策略的固定项、模式或允许版本发生整体替换式变化，哈希即变化。
func ContentHash(s sample.Snapshot) string {
	// 排序前复制切片与底层数组，避免修改调用方原始 Pins 顺序。
	policies := append([]sample.Policy(nil), s.Policies...)
	sort.SliceStable(policies, func(i, j int) bool { return policies[i].PolicyID < policies[j].PolicyID })
	for i := range policies {
		policies[i].Pins = append([]sample.PinItem(nil), policies[i].Pins...)
		policies[i].AllowedVersions = append([]string(nil), policies[i].AllowedVersions...)
		sort.SliceStable(policies[i].Pins, func(a, b int) bool {
			return policies[i].Pins[a].ID < policies[i].Pins[b].ID
		})
	}

	h := sha256.New()
	for _, policy := range policies {
		h.Write([]byte("P:" + policy.PolicyID + ":" + policy.Mode + "\n"))
		versions := append([]string(nil), policy.AllowedVersions...)
		sort.Strings(versions)
		for _, v := range versions {
			h.Write([]byte("V:" + v + "\n"))
		}
		for _, line := range canonicalPins(policy.Pins) {
			h.Write([]byte("I:" + line + "\n"))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// EqualContent 比较两个快照的策略内容是否完全一致（忽略版本号与 marker）。
func EqualContent(a, b sample.Snapshot) bool {
	return ContentHash(a) == ContentHash(b)
}

// LatestSnapshot 返回批次中版本号最大的快照。
func LatestSnapshot(snaps []sample.Snapshot) sample.Snapshot {
	latest := snaps[0]
	for _, s := range snaps[1:] {
		if s.Version > latest.Version {
			latest = s
		}
	}
	return latest
}
