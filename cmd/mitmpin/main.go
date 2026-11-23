// mitmpin 命令行：本地确定性 MITM 检测与证书固定校验管线。
// 不发起任何真实网络握手，所有证书与样本均在内存/临时目录生成。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	mitmpin "mitmpin"
	"mitmpin/internal/generator"
	"mitmpin/internal/policy"
	"mitmpin/internal/sample"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "generate":
		cmdGenerate(os.Args[2:])
	case "run":
		cmdRun(os.Args[2:])
	case "inject":
		cmdInject(os.Args[2:])
	case "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `mitmpin - 中间人检测与证书固定校验管线（纯本地、确定性）

用法:
  mitmpin generate -out <sample.json>           生成五类确定性样本
  mitmpin run -in <sample.json> -db <data.db> -report <report.json> [预算参数]
  mitmpin inject -kind <故障类型> -target <连接标识> -in <sample.json> -db <data.db>

故障类型:
  pin_mismatch | hot_update_race | chain_broken | event_seq_gap | snapshot_version_gap

预算参数:
  -concurrency N   并发度（默认 4）
  -global-mem N    全局内存预算（事件成本单位，默认 100000）
  -max-events N    单连接事件数上限（默认 100）
`)
}

func cmdGenerate(args []string) {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	out := fs.String("out", "", "样本输出 JSON 路径")
	_ = fs.Parse(args)
	if *out == "" {
		fmt.Fprintln(os.Stderr, "缺少 -out 参数")
		os.Exit(2)
	}
	batch, err := mitmpin.GenerateSampleBatch()
	must(err)
	must(writeJSON(*out, batch))
	fmt.Printf("已生成确定性样本: %s（连接 %d 条，快照 %d 个）\n",
		*out, len(batch.Observations), len(batch.Snapshots))
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	in := fs.String("in", "", "样本 JSON 路径")
	db := fs.String("db", filepath.Join("data", "mitmpin.db"), "SQLite 数据文件")
	reportPath := fs.String("report", filepath.Join("data", "report.json"), "报告输出路径")
	concurrency := fs.Int("concurrency", 4, "并发度")
	globalMem := fs.Int("global-mem", 100000, "全局内存预算")
	maxEvents := fs.Int("max-events", 100, "单连接事件数上限")
	_ = fs.Parse(args)
	if *in == "" {
		fmt.Fprintln(os.Stderr, "缺少 -in 参数")
		os.Exit(2)
	}

	batch, err := mitmpin.LoadBatchFile(*in)
	must(err)
	st, err := mitmpin.OpenStore(*db)
	must(err)
	defer st.Close()

	result, err := mitmpin.Run(st, batch, sample.Budget{
		Concurrency: *concurrency, GlobalMemory: *globalMem, MaxEventPerConn: *maxEvents,
	})
	must(err)

	reportBytes, err := mitmpin.ExportReport(result.Report)
	must(err)
	must(os.MkdirAll(filepath.Dir(*reportPath), 0o755))
	must(os.WriteFile(*reportPath, reportBytes, 0o644))

	printSummary(result)
	fmt.Printf("报告已写入: %s（%d 字节）\n", *reportPath, len(reportBytes))
}

func cmdInject(args []string) {
	fs := flag.NewFlagSet("inject", flag.ExitOnError)
	kind := fs.String("kind", "", "故障类型")
	target := fs.String("target", "", "受影响连接标识")
	in := fs.String("in", "", "样本 JSON 路径")
	db := fs.String("db", filepath.Join("data", "mitmpin-inject.db"), "SQLite 数据文件")
	reportPath := fs.String("report", filepath.Join("data", "report-inject.json"), "报告输出路径")
	concurrency := fs.Int("concurrency", 4, "并发度")
	globalMem := fs.Int("global-mem", 100000, "全局内存预算")
	maxEvents := fs.Int("max-events", 100, "单连接事件数上限")
	_ = fs.Parse(args)
	if *kind == "" || *target == "" || *in == "" {
		fmt.Fprintln(os.Stderr, "inject 需要 -kind、-target 与 -in 参数")
		os.Exit(2)
	}

	batch, err := mitmpin.LoadBatchFile(*in)
	must(err)
	batch.Faults = append(batch.Faults, sample.FaultSpec{Kind: *kind, Target: *target})

	st, err := mitmpin.OpenStore(*db)
	must(err)
	defer st.Close()

	budget := sample.Budget{
		Concurrency: *concurrency, GlobalMemory: *globalMem, MaxEventPerConn: *maxEvents,
	}

	// snapshot_version_gap 需要先正常运行落检查点，再注入跳号后恢复。
	if *kind == "snapshot_version_gap" {
		runGapRecovery(st, batch, budget, *target, *reportPath)
		return
	}

	result, err := mitmpin.Run(st, batch, budget)
	must(err)
	reportBytes, err := mitmpin.ExportReport(result.Report)
	must(err)
	must(os.WriteFile(*reportPath, reportBytes, 0o644))
	printSummary(result)
	expected, ok := result.FaultCodes[*target]
	fmt.Printf("注入 %s -> 连接 %s 预期业务码 %s（命中=%v）\n", *kind, *target, expected, ok)
	fmt.Printf("报告已写入: %s\n", *reportPath)
}

func runGapRecovery(st *mitmpin.Store, batch sample.Batch, budget sample.Budget,
	connID, reportPath string) {
	// 第一阶段：正常运行，产生连续检查点链。
	result, err := mitmpin.Run(st, batch, budget)
	must(err)

	// 第二阶段：在 SQLite 层制造检查点版本跳号（不改源码）。
	must(mitmpin.InjectSnapshotVersionGap(st, connID))

	// 第三阶段：重启恢复，应返回 E_SNAPSHOT_VERSION 并回退到最近连续版本。
	bound, ok := result.Bindings[connID]
	if !ok {
		fail("找不到连接 %s 的策略绑定", connID)
	}
	obs := findObservation(batch.Observations, connID)
	recovered, err := mitmpin.Recover(st, obs, bound, budget.MaxEventPerConn)
	code := sample.CodeOf(err)
	if code != sample.E_SNAPSHOT_VERSION {
		fail("预期 E_SNAPSHOT_VERSION，实际 %s", code)
	}
	fmt.Printf("快照跳号注入成功: 连接 %s 恢复返回 %s，回退后已处理事件 %d\n",
		connID, code, recovered.ProcessedEvents)

	// 恢复后再次运行应从连续检查点继续并成功，已提交结论不回退。
	recovered2, err := mitmpin.Recover(st, obs, bound, budget.MaxEventPerConn)
	must(err)
	if recovered2.FinalCode != sample.E_OK && recovered2.FinalCode != sample.EvAborted {
		// 正常样本恢复后应通过。
		fmt.Printf("注意：恢复后终态业务码 %s\n", recovered2.FinalCode)
	}
	reportBytes, err := mitmpin.ExportReport(result.Report)
	must(err)
	must(os.WriteFile(reportPath, reportBytes, 0o644))
	fmt.Printf("报告已写入: %s\n", reportPath)
}

func printSummary(result *mitmpin.RunResult) {
	fmt.Println("==== 报告摘要 ====")
	for _, snap := range result.SnapshotInfo {
		fmt.Printf("快照 v%d marker=%s content=%s\n", snap.Version, snap.Marker, short(snap.ContentHash))
	}
	conns := append([]mitmpin.ConnResult(nil), result.Results...)
	sort.Slice(conns, func(i, j int) bool { return conns[i].ConnID < conns[j].ConnID })
	for _, conn := range conns {
		budgetCode := sample.E_OK
		if planned, ok := result.BudgetPlan.Find(conn.ConnID); ok {
			budgetCode = planned.BudgetCode
		}
		replayOK := false
		for _, r := range result.Replays {
			if r.ConnID == conn.ConnID {
				replayOK = r.AllConsistent
			}
		}
		fmt.Printf("连接 %-16s 策略v%-2d 终态=%-22s 预算=%-18s 事件=%d 重放一致=%v\n",
			conn.ConnID, conn.PolicyVersion, conn.FinalCode, budgetCode,
			conn.ProcessedEvents, replayOK)
	}
}

func findObservation(obs []sample.Observation, connID string) sample.Observation {
	for _, o := range obs {
		if o.ConnID == connID {
			return o
		}
	}
	fail("观测中找不到连接 %s", connID)
	return sample.Observation{}
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func short(hash string) string {
	if len(hash) > 12 {
		return hash[:12] + "..."
	}
	return hash
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "错误: "+format+"\n", args...)
	os.Exit(1)
}

// 保持 policy 包引用，避免示例扩展时未使用导入。
var _ = policy.EqualContent

// 生成器引用，供后续扩展自定义命令。
var _ = generator.SnapshotV1
