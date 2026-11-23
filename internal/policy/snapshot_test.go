package policy

import (
	"testing"

	"mitmpin/internal/generator"
	"mitmpin/internal/sample"
)

func TestContentHash_ReplacementDetection(t *testing.T) {
	m, err := generator.BuildMaterial()
	if err != nil {
		t.Fatal(err)
	}
	v1, _ := generator.SnapshotV1(m)
	v2 := generator.SnapshotV2(m)

	if ContentHash(v1) == ContentHash(v2) {
		t.Fatal("整体替换式更新必须产生不同内容哈希")
	}

	// 仅版本号不同、内容一致 -> 内容哈希相同。
	revision := v1
	revision.Version = 99
	revision.Marker = "changed-marker"
	if !EqualContent(v1, revision) {
		t.Fatal("策略内容一致时 EqualContent 必须为 true（忽略版本号/marker）")
	}

	// 篡改单个固定摘要 -> 哈希变化（深拷贝避免污染原快照）。
	tampered := sample.Snapshot{Version: v1.Version, Marker: v1.Marker}
	for _, pol := range v1.Policies {
		polCopy := pol
		polCopy.Pins = append([]sample.PinItem(nil), pol.Pins...)
		polCopy.Pins[0] = sample.PinItem{ID: polCopy.Pins[0].ID, Algo: "sha256",
			Digest: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
		tampered.Policies = append(tampered.Policies, polCopy)
	}
	if EqualContent(v1, tampered) {
		t.Fatal("固定摘要变化必须被内容哈希识别")
	}
}

func TestLatestSnapshot(t *testing.T) {
	m, _ := generator.BuildMaterial()
	v1, _ := generator.SnapshotV1(m)
	v2 := generator.SnapshotV2(m)
	if LatestSnapshot([]sample.Snapshot{v2, v1}).Version != 2 {
		t.Fatal("应返回版本号最大的快照")
	}
}
