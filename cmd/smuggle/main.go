// smuggle 是 HTTP 请求走私规范化裁决组件的命令行入口。
// 全部子命令仅使用本地文件，重复运行输出字节级一致。
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"smuggle/smuggle"
)

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func freshStore(path string) *smuggle.Store {
	os.Remove(path)
	st, err := smuggle.OpenStore(path)
	if err != nil {
		fail(err)
	}
	return st
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println(`usage: smuggle <command> [flags]
commands:
  gensamples  生成本地确定性样本目录
  run         对样本目录执行批裁决并导出 JSON 报告
  inject      通过参数注入畸形样本并执行异常链路验证
  verify      校验某连接的快照版本链连续性
  all         一键运行 gensamples + run + inject`)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "gensamples":
		cmdGensamples(os.Args[2:])
	case "run":
		cmdRun(os.Args[2:])
	case "inject":
		cmdInject(os.Args[2:])
	case "verify":
		cmdVerify(os.Args[2:])
	case "all":
		cmdAll(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", os.Args[1])
		os.Exit(2)
	}
}

func cmdGensamples(args []string) {
	fs := flag.NewFlagSet("gensamples", flag.ExitOnError)
	dir := fs.String("dir", "samples", "样本输出目录")
	fs.Parse(args)
	if err := smuggle.WriteSamples(*dir); err != nil {
		fail(err)
	}
	fmt.Println("samples written to", *dir)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	samples := fs.String("samples", "samples", "样本目录")
	db := fs.String("db", filepath.Join("out", "adjudicate.db"), "SQLite 快照库路径")
	report := fs.String("report", filepath.Join("out", "report.json"), "报告输出路径")
	workers := fs.Int("workers", 4, "并发工作协程数")
	threshold := fs.Int("threshold", 64, "流式扫描路径阈值（估计请求数）")
	budget := fs.Int("budget", 1024, "单连接请求数上限")
	concurrency := fs.Int("concurrency", 4, "并发度上限")
	queue := fs.Int("queue", 16, "等待队列容量")
	fs.Parse(args)

	conns, err := smuggle.LoadSamples(*samples)
	if err != nil {
		fail(err)
	}
	st := freshStore(*db)
	defer st.Close()
	cfg := smuggle.DefaultConfig()
	cfg.Workers = *workers
	cfg.StreamThreshold = *threshold
	cfg.MaxRequestsPerConn = *budget
	cfg.MaxConcurrency = *concurrency
	cfg.MaxQueue = *queue
	eng := smuggle.NewEngine(cfg, st)

	reps := eng.Run(conns)
	rep := smuggle.BuildReport(cfg, reps)
	data, err := smuggle.MarshalReport(rep)
	if err != nil {
		fail(err)
	}
	if err := os.MkdirAll(filepath.Dir(*report), 0o755); err != nil {
		fail(err)
	}
	if err := os.WriteFile(*report, data, 0o644); err != nil {
		fail(err)
	}
	for _, c := range rep.Connections {
		fmt.Printf("%s verdict=%s code=%s offset=%d path=%s snapshot=v%d\n",
			c.Conn, c.Verdict, c.Code, c.Offset, c.Path, c.SnapshotVersion)
	}
	fmt.Println("report written to", *report)
}

func cmdInject(args []string) {
	fs := flag.NewFlagSet("inject", flag.ExitOnError)
	db := fs.String("db", filepath.Join("out", "inject.db"), "SQLite 快照库路径")
	report := fs.String("report", filepath.Join("out", "inject-report.json"), "注入报告输出路径")
	anomalies := fs.String("anomalies", "all", "注入类型，逗号分隔或 all")
	fs.Parse(args)

	var list []smuggle.Anomaly
	if *anomalies == "all" {
		list = smuggle.AllAnomalies()
	} else {
		for _, s := range strings.Split(*anomalies, ",") {
			list = append(list, smuggle.Anomaly(strings.TrimSpace(s)))
		}
	}
	st := freshStore(*db)
	defer st.Close()
	eng := smuggle.NewEngine(smuggle.DefaultConfig(), st)
	results, err := smuggle.RunInjection(eng, st, list)
	if err != nil {
		fail(err)
	}
	if err := os.MkdirAll(filepath.Dir(*report), 0o755); err != nil {
		fail(err)
	}
	data, err := smuggle.MarshalReport(&smuggle.Report{Marker: smuggle.ReportMarker})
	_ = data
	b, err := marshalJSON(results)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*report, b, 0o644); err != nil {
		fail(err)
	}
	ok := true
	for _, r := range results {
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
			ok = false
		}
		fmt.Printf("%s %s expected=%s observed=%s verdict=%s snapshot=v%d->v%d isolated=%v\n",
			status, r.Anomaly, r.ExpectedCode, r.ObservedViewCode, r.Verdict,
			r.SnapshotVersion, r.ResubmittedVersion, r.Isolated)
	}
	if !ok {
		os.Exit(1)
	}
}

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	db := fs.String("db", filepath.Join("out", "adjudicate.db"), "SQLite 快照库路径")
	conn := fs.String("conn", "", "连接标识")
	fs.Parse(args)
	st, err := smuggle.OpenStore(*db)
	if err != nil {
		fail(err)
	}
	defer st.Close()
	snaps, code := st.LoadChain(*conn)
	fmt.Printf("conn=%s code=%s contiguous_versions=%d\n", *conn, code, len(snaps))
	if code != smuggle.CodeOK {
		os.Exit(1)
	}
}

func cmdAll(args []string) {
	fs := flag.NewFlagSet("all", flag.ExitOnError)
	workdir := fs.String("workdir", "out", "工作目录（样本、数据库与报告均在其下）")
	fs.Parse(args)
	samples := filepath.Join(*workdir, "samples")
	if err := smuggle.WriteSamples(samples); err != nil {
		fail(err)
	}
	cmdRun([]string{
		"-samples", samples,
		"-db", filepath.Join(*workdir, "adjudicate.db"),
		"-report", filepath.Join(*workdir, "report.json"),
	})
	cmdInject([]string{
		"-db", filepath.Join(*workdir, "inject.db"),
		"-report", filepath.Join(*workdir, "inject-report.json"),
	})
}
