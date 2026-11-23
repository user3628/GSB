// capver 是 Windows 进程隔离与能力限制的可复现验证组件。
//
// 用法：
//
//	capver gen-samples                     输出内置确定性样本策略（JSON）
//	capver run [-db D] [-inject K] ...     执行一次验证会话并导出报告
//	capver replay -db D [-version N]       回放策略快照
//	capver recover -db D                   强制终止后的恢复检查
//	capver probe ...                       （内部）受测探针子进程
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"capver/capver"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var code int
	switch os.Args[1] {
	case "gen-samples":
		code = cmdGenSamples(os.Args[2:])
	case "run":
		code = cmdRun(os.Args[2:])
	case "replay":
		code = cmdReplay(os.Args[2:])
	case "recover":
		code = cmdRecover(os.Args[2:])
	case "probe":
		os.Exit(capver.RunProbe(os.Args[2:]))
	default:
		usage()
		code = 2
	}
	os.Exit(code)
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: capver <command> [flags]
commands:
  gen-samples            print deterministic sample policy (JSON)
  run                    run a verification session and export report
  replay                 replay a policy snapshot from the store
  recover                recovery check after forced termination
  probe                  (internal) restricted probe child process`)
}

// exitCode 把业务码映射为稳定进程退出码。
func exitCode(c capver.Code) int {
	switch c {
	case capver.CodeOK:
		return 0
	case capver.CodeCapDenied:
		return 11
	case capver.CodeCapLeak:
		return 12
	case capver.CodeHandleEscape:
		return 13
	case capver.CodeSnapshotVersion:
		return 14
	case capver.CodeProcessTimeout:
		return 15
	case capver.CodeUnsupported:
		return 16
	case capver.CodePolicyInvalid:
		return 17
	case capver.CodeSessionLimit:
		return 18
	default:
		return 19
	}
}

func cmdGenSamples(args []string) int {
	fs := flag.NewFlagSet("gen-samples", flag.ContinueOnError)
	out := fs.String("out", "", "output file (default stdout)")
	_ = fs.Parse(args)
	p := capver.GenerateSamplePolicy()
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 19
	}
	data = append(data, '\n')
	if *out == "" {
		os.Stdout.Write(data)
		return 0
	}
	if err := os.WriteFile(*out, data, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 19
	}
	return 0
}

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	dbPath := fs.String("db", filepath.Join(os.TempDir(), "capver-verify.db"), "sqlite store path")
	samples := fs.String("samples", "", "policy json file (default: built-in deterministic samples)")
	inject := fs.String("inject", "", "anomaly injection: unauthorized-write|child-escape|handle-escape|cap-leak|snapshot-gap")
	reportOut := fs.String("report", "", "report output file (default stdout)")
	session := fs.String("session", "sess-1", "session id")
	maxSessions := fs.Int("max-sessions", 2, "max concurrent sessions")
	maxProcs := fs.Int("max-procs", 8, "max processes per session")
	timeout := fs.Duration("timeout", 10*time.Second, "per-process timeout")
	slowTest := fs.Bool("slow-test", false, "append a long-sleep process to exercise E_PROCESS_TIMEOUT")
	gapSkip := fs.Int64("gap-skip", 2, "snapshot gap injection skip")
	_ = fs.Parse(args)

	var policy *capver.Policy
	if *samples != "" {
		data, err := os.ReadFile(*samples)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 19
		}
		p, err := capver.ParsePolicy(data)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitCode(capver.CodeOf(err))
		}
		policy = p
	} else {
		policy = capver.GenerateSamplePolicy()
	}

	store, err := capver.OpenStore(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(capver.CodeOf(err))
	}
	defer store.Close()

	verifier := capver.NewVerifier(store, capver.NewRunner())
	opts := capver.VerifyOptions{
		SessionID: *session,
		Inject:    capver.InjectKind(*inject),
		Limits: capver.SessionLimits{
			MaxConcurrentSessions:  *maxSessions,
			MaxProcessesPerSession: *maxProcs,
			ProcessTimeout:         *timeout,
		},
		SlowTest: *slowTest,
	}

	// 快照跳号注入：先提交基线快照，再注入跳号，最后触发恢复加载。
	if opts.Inject == capver.InjectSnapshotGap {
		base := capver.ApplyInjection(policy, capver.InjectNone)
		if _, err := store.LoadLatest(); err != nil {
			if _, cerr := store.CommitSnapshot(base); cerr != nil {
				fmt.Fprintln(os.Stderr, cerr)
				return exitCode(capver.CodeOf(cerr))
			}
		}
		if _, err := store.InjectSnapshotGap(*gapSkip); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitCode(capver.CodeOf(err))
		}
	}

	report, err := verifier.Verify(context.Background(), policy, opts)
	if report != nil {
		data := capver.ExportReport(report)
		if *reportOut == "" {
			os.Stdout.Write(data)
		} else if werr := os.WriteFile(*reportOut, data, 0o600); werr != nil {
			fmt.Fprintln(os.Stderr, werr)
			return 19
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(capver.CodeOf(err))
	}
	// 注入场景：以注入进程的业务码作为进程退出码。
	if k := opts.Inject; k != capver.InjectNone && k != capver.InjectSnapshotGap {
		for _, s := range report.Sessions {
			for _, pr := range s.Processes {
				if pr.ProcID == k.InjectedProcID() {
					return exitCode(pr.Code)
				}
			}
		}
	}
	return 0
}

func cmdReplay(args []string) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	dbPath := fs.String("db", filepath.Join(os.TempDir(), "capver-verify.db"), "sqlite store path")
	version := fs.Int64("version", 0, "snapshot version (default: latest continuous)")
	_ = fs.Parse(args)

	store, err := capver.OpenStore(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(capver.CodeOf(err))
	}
	defer store.Close()
	verifier := capver.NewVerifier(store, nil)
	snap, err := verifier.Replay(*version)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(capver.CodeOf(err))
	}
	out := map[string]any{
		"version": snap.Version,
		"hash":    snap.Hash,
		"policy":  snap.Policy,
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	os.Stdout.Write(append(data, '\n'))
	return 0
}

func cmdRecover(args []string) int {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	dbPath := fs.String("db", filepath.Join(os.TempDir(), "capver-verify.db"), "sqlite store path")
	_ = fs.Parse(args)

	store, err := capver.OpenStore(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(capver.CodeOf(err))
	}
	defer store.Close()
	snap, conclusion, err := store.Recover()
	out := map[string]any{
		"recovery": conclusion,
	}
	if snap != nil {
		out["snapshotVersion"] = snap.Version
		out["policyHash"] = snap.Policy.Hash()
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	os.Stdout.Write(append(data, '\n'))
	if err != nil && snap == nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(capver.CodeOf(err))
	}
	if conclusion == capver.RecoveryFallback {
		return exitCode(capver.CodeSnapshotVersion)
	}
	return 0
}
