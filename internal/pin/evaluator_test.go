package pin

import (
	"testing"

	"mitmpin/internal/certutil"
	"mitmpin/internal/generator"
	"mitmpin/internal/sample"
)

func chain(t *testing.T, pemStr string) []*certutil.Certificate {
	t.Helper()
	c, err := certutil.ParseChain(pemStr)
	if err != nil {
		t.Fatalf("链解析失败: %v", err)
	}
	return c
}

func policyOf(t *testing.T, snap sample.Snapshot, id string) sample.Policy {
	for _, p := range snap.Policies {
		if p.PolicyID == id {
			return p
		}
	}
	t.Fatalf("策略 %s 不存在", id)
	return sample.Policy{}
}

func TestEvaluate_SPKIModes(t *testing.T) {
	m, err := generator.BuildMaterial()
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := generator.SnapshotV1(m)
	pol := policyOf(t, snap, "pol-svc")

	// 正常连接：SPKI 命中叶证书。
	good := generator.NormalConn(m, "good")
	v := Evaluate(good, chain(t, good.ChainPEM), pol, snap.Version)
	if v.Code != sample.E_OK {
		t.Fatalf("正常连接应 E_OK，实际 %s (%+v)", v.Code, v.Findings)
	}
	if v.LeafFingerprint == "" {
		t.Fatal("应记录叶证书指纹")
	}

	// MITM 连接：异根叶证书，SPKI 全部不命中。
	mitm := generator.MITMConn(m, "mitm")
	v = Evaluate(mitm, chain(t, mitm.ChainPEM), pol, snap.Version)
	if v.Code != sample.E_MITM_SUSPECTED {
		t.Fatalf("异根连接应 E_MITM_SUSPECTED，实际 %s", v.Code)
	}
	matched := false
	for _, hit := range v.Hits {
		if hit.Matched {
			matched = true
		}
	}
	if matched {
		t.Fatal("MITM 场景不应有命中固定项")
	}
}

func TestEvaluate_TrustedRoots(t *testing.T) {
	m, _ := generator.BuildMaterial()
	snap := generator.SnapshotV2(m)
	pol := policyOf(t, snap, "pol-svc")

	good := generator.NormalConn(m, "good")
	v := Evaluate(good, chain(t, good.ChainPEM), pol, snap.Version)
	if v.Code != sample.E_OK {
		t.Fatalf("受信根模式正常链应通过: %s (%+v)", v.Code, v.Findings)
	}

	// 异根叶证书无法构建受信路径 -> MITM。
	mitm := generator.MITMConn(m, "mitm")
	v = Evaluate(mitm, chain(t, mitm.ChainPEM), pol, snap.Version)
	if v.Code != sample.E_MITM_SUSPECTED {
		t.Fatalf("异根应 E_MITM_SUSPECTED，实际 %s", v.Code)
	}
}

func TestEvaluate_IndependentCodes(t *testing.T) {
	m, _ := generator.BuildMaterial()
	snap, _ := generator.SnapshotV1(m)
	pol := policyOf(t, snap, "pol-svc")

	sniBad := generator.SNIMismatchConn(m, "sni")
	v := Evaluate(sniBad, chain(t, sniBad.ChainPEM), pol, snap.Version)
	if v.Code != sample.E_SNI_MISMATCH {
		t.Fatalf("SNI 不匹配应 E_SNI_MISMATCH，实际 %s", v.Code)
	}
	found := false
	for _, f := range v.Findings {
		if f.Code == sample.E_SNI_MISMATCH {
			found = true
		}
	}
	if !found {
		t.Fatal("Findings 中应保留独立 SNI 检查项")
	}

	protoBad := generator.ProtocolBadConn(m, "proto")
	v = Evaluate(protoBad, chain(t, protoBad.ChainPEM), pol, snap.Version)
	if v.Code != sample.E_PROTOCOL_NOT_ALLOWED {
		t.Fatalf("协议超范围应 E_PROTOCOL_NOT_ALLOWED，实际 %s", v.Code)
	}
}
