package sample

import (
	"testing"

	"mitmpin/internal/certutil"
)

// 本地构造最小确定性素材，避免 sample 测试反向依赖 generator 形成导入环。
func testSnapshot(t *testing.T) (Snapshot, func() string) {
	t.Helper()
	root, err := certutil.NewRoot(1001, "root-ca")
	if err != nil {
		t.Fatal(err)
	}
	inter, err := certutil.NewIntermediate(2001, "inter-ca", root)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := certutil.NewLeaf(3001, "svc.internal", []string{"svc.internal"}, inter)
	if err != nil {
		t.Fatal(err)
	}
	leafDigest, err := certutil.SPKIDigest(leaf, "sha256")
	if err != nil {
		t.Fatal(err)
	}
	interDigest, err := certutil.SPKIDigest(inter.Cert, "sha256")
	if err != nil {
		t.Fatal(err)
	}
	snap := Snapshot{
		Version: 1, Marker: "m1",
		Policies: []Policy{{
			PolicyID: "pol-svc", Mode: PinModeSPKI,
			AllowedVersions: []string{"tls1.2", "tls1.3"},
			Pins: []PinItem{
				{ID: "pin-leaf", Algo: "sha256", Digest: leafDigest},
				{ID: "pin-inter", Algo: "sha256", Digest: interDigest},
			},
		}},
	}
	chainPEM := certutil.EncodeChain([]*certutil.Certificate{leaf, inter.Cert, root.Cert})
	return snap, func() string { return chainPEM }
}

func TestValidateSnapshot_UniquePolicyAndAlgo(t *testing.T) {
	snap, _ := testSnapshot(t)
	if err := ValidateSnapshot(snap); err != nil {
		t.Fatalf("合法快照被拒: %v", err)
	}
	dup := snap
	dup.Policies = append(dup.Policies, snap.Policies[0])
	if CodeOf(ValidateSnapshot(dup)) != E_POLICY_MISSING {
		t.Fatal("重复策略标识应返回 E_POLICY_MISSING")
	}
	badAlgo := snap
	badAlgo.Policies[0].Pins[0].Algo = "sha512-256"
	if CodeOf(ValidateSnapshot(badAlgo)) != E_HASH_ALGO_UNSUPPORTED {
		t.Fatal("未知算法应返回 E_HASH_ALGO_UNSUPPORTED")
	}
}

func TestValidateObservation_SeqGapAndChain(t *testing.T) {
	snap, chainPEM := testSnapshot(t)
	good := Observation{
		ConnID: "c1", PolicyID: "pol-svc", SNI: "svc.internal", Protocol: "tls1.3",
		ChainPEM: chainPEM(),
		Events: []Event{
			{Seq: 1, Type: EvHandshakeStart, Marker: "e1"},
			{Seq: 2, Type: EvCertReceived, Marker: "e2"},
			{Seq: 3, Type: EvPinCheck, Marker: "e3"},
			{Seq: 4, Type: EvHandshakeDone, Marker: "e4"},
		},
	}
	if err := ValidateObservation(good, snap); err != nil {
		t.Fatalf("合法观测被拒: %v", err)
	}

	broken := good
	broken.Events = append([]Event(nil), good.Events...)
	broken.ChainPEM = "-----BEGIN CERTIFICATE-----\nQQ\n-----END CERTIFICATE-----\n"
	if CodeOf(ValidateObservation(broken, snap)) != E_CHAIN_BROKEN {
		t.Fatal("证书链损坏应返回 E_CHAIN_BROKEN")
	}

	gap := good
	gap.Events = append([]Event(nil), good.Events...)
	gap.Events[2].Seq = 4
	gap.Events[3].Seq = 5
	if CodeOf(ValidateObservation(gap, snap)) != E_EVENT_SEQ_GAP {
		t.Fatal("事件序号跳变应返回 E_EVENT_SEQ_GAP")
	}

	missing := good
	missing.PolicyID = "nope"
	if CodeOf(ValidateObservation(missing, snap)) != E_POLICY_MISSING {
		t.Fatal("策略缺失应返回 E_POLICY_MISSING")
	}
}

func TestParseChain_Structure(t *testing.T) {
	_, chainPEM := testSnapshot(t)
	chain, err := certutil.ParseChain(chainPEM())
	if err != nil {
		t.Fatalf("正常链解析失败: %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("期望 3 张证书，实际 %d", len(chain))
	}
	if _, err := certutil.ParseChain("not-a-pem"); err == nil {
		t.Fatal("非 PEM 输入应报错")
	}
}
