// 一键运行示例：在临时目录内生成确定性证书与五类观测样本，
// 完成固定校验、热更新绑定、并发调度并导出可复核报告。全程无网络。
//
// 运行：go run ./examples/quickstart
package main

import (
	"fmt"
	"os"
	"path/filepath"

	mitmpin "mitmpin"
	"mitmpin/internal/generator"
	"mitmpin/internal/sample"
)

func main() {
	workDir, err := os.MkdirTemp("", "mitmpin-quickstart-*")
	must(err)
	fmt.Printf("临时工作目录: %s\n", workDir)

	// 1) 本地确定性素材与批次（正常 / MITM / 热更新 / 链断裂 / 事件跳变）。
	material, err := generator.BuildMaterial()
	must(err)
	batch, err := generator.Batch(material)
	must(err)

	// 2) 打开临时目录下的 SQLite（纯 Go 驱动，无需 CGO/GCC）。
	dbPath := filepath.Join(workDir, "data", "mitmpin.db")
	store, err := mitmpin.OpenStore(dbPath)
	must(err)
	defer store.Close()

	// 3) 并发度 4、全局内存预算 100000、单连接事件上限 100。
	result, err := mitmpin.Run(store, batch, sample.Budget{
		Concurrency: 4, GlobalMemory: 100000, MaxEventPerConn: 100,
	})
	must(err)

	// 4) 导出确定性 JSON 报告并打印逐条摘要。
	reportPath := filepath.Join(workDir, "data", "report.json")
	data, err := mitmpin.ExportReport(result.Report)
	must(err)
	must(os.WriteFile(reportPath, data, 0o644))

	fmt.Println("连接结论（按标识排序）:")
	for _, conn := range result.Results {
		fmt.Printf("  %-16s 策略v%d 终态=%-22s 事件=%d\n",
			conn.ConnID, conn.PolicyVersion, conn.FinalCode, conn.ProcessedEvents)
	}
	consistent := true
	for _, r := range result.Replays {
		if !r.AllConsistent {
			consistent = false
		}
	}
	fmt.Printf("交互侧重放与结算侧逐条一致: %v\n", consistent)
	fmt.Printf("报告路径: %s（%d 字节）\n", reportPath, len(data))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}
