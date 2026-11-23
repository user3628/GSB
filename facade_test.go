package mitmpin_test

import (
	"path/filepath"
	"testing"

	mitmpin "mitmpin"
	"mitmpin/internal/sample"
)

func TestFacadeEndToEnd(t *testing.T) {
	batch, err := mitmpin.GenerateSampleBatch()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	st, err := mitmpin.OpenStore(filepath.Join(dir, "facade.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	result, err := mitmpin.Run(st, batch, sample.Budget{
		Concurrency: 3, GlobalMemory: 100000, MaxEventPerConn: 100,
	})
	if err != nil {
		t.Fatalf("管线运行失败: %v", err)
	}
	first, err := mitmpin.ExportReport(result.Report)
	if err != nil || len(first) == 0 {
		t.Fatalf("报告导出失败: %v", err)
	}

	// 独立第二运行：字节级一致。
	st2, err := mitmpin.OpenStore(filepath.Join(t.TempDir(), "facade2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	result2, err := mitmpin.Run(st2, batch, sample.Budget{
		Concurrency: 3, GlobalMemory: 100000, MaxEventPerConn: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, _ := mitmpin.ExportReport(result2.Report)
	if string(first) != string(second) {
		t.Fatal("两次运行报告必须字节级一致")
	}

	// 单连接固定校验接口可用。
	verdict, err := mitmpin.VerifyPinning(batch.Observations[0], batch.Snapshots[0])
	if err != nil {
		t.Fatal(err)
	}
	if verdict.Code != sample.E_OK {
		t.Fatalf("正常连接固定校验应通过，实际 %s", verdict.Code)
	}

	// 内容哈希接口识别整体替换。
	if mitmpin.PolicyContentHash(batch.Snapshots[0]) ==
		mitmpin.PolicyContentHash(batch.Updates[0].Snapshot) {
		t.Fatal("热更新快照内容哈希必须变化")
	}
}
