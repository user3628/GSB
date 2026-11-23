package certutil

import "testing"

func TestCheckChainStructure(t *testing.T) {
	root, err := NewRoot(1, "root")
	if err != nil {
		t.Fatal(err)
	}
	inter, err := NewIntermediate(2, "inter", root)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := NewLeaf(3, "leaf", []string{"leaf"}, inter)
	if err != nil {
		t.Fatal(err)
	}
	otherRoot, _ := NewRoot(4, "other")

	good := []*Certificate{leaf, inter.Cert, root.Cert}
	if err := CheckChainStructure(good); err != nil {
		t.Fatalf("完整链应通过结构校验: %v", err)
	}

	// 缺失中间 CA：叶与根无直接签发关系。
	broken := []*Certificate{leaf, root.Cert}
	if err := CheckChainStructure(broken); err == nil {
		t.Fatal("缺失中间证书必须判定链结构不连续")
	}

	// 异根拼接：叶由 inter 签发，却拼接 otherRoot。
	foreign := []*Certificate{leaf, inter.Cert, otherRoot.Cert}
	if err := CheckChainStructure(foreign); err == nil {
		t.Fatal("异根拼接必须判定链结构不连续")
	}

	// 末张非自签根。
	noRoot := []*Certificate{leaf, inter.Cert}
	if err := CheckChainStructure(noRoot); err == nil {
		t.Fatal("末张非自签根必须报错")
	}
}

func TestSPKIDigest_AlgoSupport(t *testing.T) {
	root, _ := NewRoot(9, "r")
	if _, err := SPKIDigest(root.Cert, "sha1"); err == nil {
		t.Fatal("sha1 应判定为不支持算法")
	}
	d256, err := SPKIDigest(root.Cert, "sha256")
	if err != nil || d256 == "" {
		t.Fatalf("sha256 摘要失败: %v", err)
	}
	d384, err := SPKIDigest(root.Cert, "sha384")
	if err != nil || d384 == "" {
		t.Fatalf("sha384 摘要失败: %v", err)
	}
	if d256 == d384 {
		t.Fatal("不同算法摘要不应相同")
	}
}
