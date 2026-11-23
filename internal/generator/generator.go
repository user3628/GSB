package generator

import (
	"fmt"

	"mitmpin/internal/certutil"
	"mitmpin/internal/sample"
)

// Material 是生成器产出的确定性证书与策略素材。
type Material struct {
	Root         *certutil.CA
	Intermediate *certutil.CA
	OtherRoot    *certutil.CA // 用于构造 MITM / 链断裂场景的异根
	LeafGood     *certutil.Certificate
	LeafOther    *certutil.Certificate
}

// BuildMaterial 在内存中生成确定性证书层级，不访问网络。
func BuildMaterial() (*Material, error) {
	root, err := certutil.NewRoot(1001, "mitmpin-root-ca")
	if err != nil {
		return nil, err
	}
	inter, err := certutil.NewIntermediate(2001, "mitmpin-intermediate-ca", root)
	if err != nil {
		return nil, err
	}
	otherRoot, err := certutil.NewRoot(1002, "mitmpin-other-root-ca")
	if err != nil {
		return nil, err
	}
	leafGood, err := certutil.NewLeaf(3001, "svc.internal", []string{"svc.internal", "www.svc.internal"}, inter)
	if err != nil {
		return nil, err
	}
	leafOther, err := certutil.NewLeaf(3002, "attacker.internal", []string{"attacker.internal", "svc.internal"}, otherRoot)
	if err != nil {
		return nil, err
	}
	return &Material{
		Root: root, Intermediate: inter, OtherRoot: otherRoot,
		LeafGood: leafGood, LeafOther: leafOther,
	}, nil
}

// SnapshotV1 构造初始 SPKI 固定策略快照（固定叶 + 中间证书）。
func SnapshotV1(m *Material) (sample.Snapshot, error) {
	leafPin, err := spkiPin(m.LeafGood, "pin-leaf", "sha256")
	if err != nil {
		return sample.Snapshot{}, err
	}
	interPin, err := spkiCert(m.Intermediate.Cert, "pin-intermediate", "sha256")
	if err != nil {
		return sample.Snapshot{}, err
	}
	return sample.Snapshot{
		Version: 1,
		Marker:  "marker-snapshot-v1",
		Policies: []sample.Policy{{
			PolicyID:        "pol-svc",
			Mode:            sample.PinModeSPKI,
			AllowedVersions: []string{"tls1.2", "tls1.3"},
			Pins:            []sample.PinItem{leafPin, interPin},
		}},
	}, nil
}

// SnapshotV2 构造整体替换式更新：替换为受信根集合模式，允许版本收窄。
func SnapshotV2(m *Material) sample.Snapshot {
	return sample.Snapshot{
		Version: 2,
		Marker:  "marker-snapshot-v2",
		Policies: []sample.Policy{{
			PolicyID:        "pol-svc",
			Mode:            sample.PinModeRoot,
			AllowedVersions: []string{"tls1.3"},
			Pins: []sample.PinItem{{
				ID: "trust-root", Algo: "", RootPEM: certutil.EncodeCert(m.Root.Cert),
			}},
		}},
	}
}

func spkiCert(cert *certutil.Certificate, id, algo string) (sample.PinItem, error) {
	digest, err := certutil.SPKIDigest(cert, algo)
	if err != nil {
		return sample.PinItem{}, err
	}
	return sample.PinItem{ID: id, Algo: algo, Digest: digest}, nil
}

// spkiPin 包装 *x509.Certificate（Material 中 Leaf 字段类型）。
func spkiPin(cert *certutil.Certificate, id, algo string) (sample.PinItem, error) {
	return spkiCert(cert, id, algo)
}

func goodChainPEM(m *Material) string {
	return certutil.EncodeChain([]*certutil.Certificate{m.LeafGood, m.Intermediate.Cert, m.Root.Cert})
}

func standardEvents(markerPrefix string, n int) []sample.Event {
	types := []string{
		sample.EvHandshakeStart, sample.EvCertReceived, sample.EvPinCheck,
		sample.EvHandshakeDone,
	}
	events := make([]sample.Event, 0, n)
	for i := 0; i < n; i++ {
		ev := sample.Event{Seq: i + 1, Type: types[i], Marker: fmt.Sprintf("%s-e%d", markerPrefix, i+1)}
		events = append(events, ev)
	}
	return events
}

// NormalConn 构造一条正常连接。
func NormalConn(m *Material, connID string) sample.Observation {
	return sample.Observation{
		ConnID: connID, PolicyID: "pol-svc", SNI: "svc.internal",
		Protocol: "tls1.3", ChainPEM: goodChainPEM(m),
		Events: standardEvents(connID, 4),
	}
}

// MITMConn 构造疑似中间人连接：叶证书与签发链均来自异根，SPKI 全部不命中。
func MITMConn(m *Material, connID string) sample.Observation {
	chainPEM := certutil.EncodeChain([]*certutil.Certificate{
		m.LeafOther, m.OtherRoot.Cert,
	})
	return sample.Observation{
		ConnID: connID, PolicyID: "pol-svc", SNI: "svc.internal",
		Protocol: "tls1.3", ChainPEM: chainPEM,
		Events: standardEvents(connID, 4),
	}
}

// BrokenChainConn 构造证书链断裂连接：缺失中间 CA。
func BrokenChainConn(m *Material, connID string) sample.Observation {
	chainPEM := certutil.EncodeChain([]*certutil.Certificate{m.LeafGood, m.Root.Cert})
	return sample.Observation{
		ConnID: connID, PolicyID: "pol-svc", SNI: "svc.internal",
		Protocol: "tls1.3", ChainPEM: chainPEM,
		Events: standardEvents(connID, 4),
	}
}

// SeqGapConn 构造事件序号跳变连接（1,2,4,5）。
func SeqGapConn(m *Material, connID string) sample.Observation {
	events := standardEvents(connID, 4)
	events[2].Seq = 4
	events[3].Seq = 5
	return sample.Observation{
		ConnID: connID, PolicyID: "pol-svc", SNI: "svc.internal",
		Protocol: "tls1.3", ChainPEM: goodChainPEM(m), Events: events,
	}
}

// SNIMismatchConn 构造 SNI 与 SAN 不匹配连接。
func SNIMismatchConn(m *Material, connID string) sample.Observation {
	return sample.Observation{
		ConnID: connID, PolicyID: "pol-svc", SNI: "evil.example",
		Protocol: "tls1.3", ChainPEM: goodChainPEM(m),
		Events: standardEvents(connID, 4),
	}
}

// ProtocolBadConn 构造协议版本超范围连接。
func ProtocolBadConn(m *Material, connID string) sample.Observation {
	return sample.Observation{
		ConnID: connID, PolicyID: "pol-svc", SNI: "svc.internal",
		Protocol: "tls1.0", ChainPEM: goodChainPEM(m),
		Events: standardEvents(connID, 4),
	}
}

// LongConn 构造事件数较多的连接，用于预算裁决（重复 pin_check 事件）。
func LongConn(m *Material, connID string, eventCount int) sample.Observation {
	events := []sample.Event{{Seq: 1, Type: sample.EvHandshakeStart, Marker: connID + "-e1"}}
	for i := 2; i <= eventCount-1; i++ {
		events = append(events, sample.Event{Seq: i, Type: sample.EvPinCheck,
			Marker: fmt.Sprintf("%s-e%d", connID, i)})
	}
	events = append(events, sample.Event{Seq: eventCount, Type: sample.EvHandshakeDone,
		Marker: fmt.Sprintf("%s-e%d", connID, eventCount)})
	return sample.Observation{
		ConnID: connID, PolicyID: "pol-svc", SNI: "svc.internal",
		Protocol: "tls1.3", ChainPEM: goodChainPEM(m), Events: events,
	}
}

// Batch 构造覆盖五类样本的标准批次，含一次热更新步骤。
func Batch(m *Material) (sample.Batch, error) {
	v1, err := SnapshotV1(m)
	if err != nil {
		return sample.Batch{}, err
	}
	v2 := SnapshotV2(m)
	observations := []sample.Observation{
		NormalConn(m, "conn-normal-1"),
		NormalConn(m, "conn-normal-2"),
		MITMConn(m, "conn-mitm-1"),
		BrokenChainConn(m, "conn-broken-1"),
		SeqGapConn(m, "conn-seqgap-1"),
		SNIMismatchConn(m, "conn-sni-1"),
		ProtocolBadConn(m, "conn-proto-1"),
		NormalConn(m, "conn-normal-3"),
	}
	// 热更新在第 5 个连接开始前生效：前 4 条绑定 v1，其后绑定 v2。
	updates := []sample.UpdateStep{{AfterSeq: 4, Snapshot: v2}}
	return sample.Batch{
		BatchID:      "batch-deterministic-001",
		Marker:       "marker-batch-001",
		Snapshots:    []sample.Snapshot{v1},
		Updates:      updates,
		Observations: observations,
	}, nil
}
