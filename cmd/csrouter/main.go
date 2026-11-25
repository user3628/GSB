package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"csrouter/internal/api"
	"csrouter/internal/app"
	"csrouter/internal/domain"
	"csrouter/internal/generator"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		var biz *domain.BizError
		if errors.As(err, &biz) {
			printJSON(biz)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `csrouter <command> [flags]
commands: generate, validate, run, resume, stop, status, report, replace-shard, change-config, rollback, serve, demo`)
}

func run(command string, args []string) error {
	switch command {
	case "generate":
		fs := flag.NewFlagSet("generate", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		seed := fs.Int64("seed", generator.DefaultSeed, "fixed seed")
		doImport := fs.Bool("import", true, "import generated bundle into SQLite")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		if *doImport {
			result, err := a.GenerateAndImport(*seed)
			if err != nil {
				return err
			}
			printJSON(result)
			return nil
		}
		result, err := a.Generate(*seed)
		if err != nil {
			return err
		}
		validation, err := a.ValidateBundle()
		if err != nil {
			return err
		}
		printJSON(map[string]any{"generate": result, "validation": validation})
		if !validation.OK {
			return fmt.Errorf("shard validation failed")
		}
		return nil
	case "validate":
		a, err := openApp(args)
		if err != nil {
			return err
		}
		defer a.Close()
		result, err := a.ValidateBundle()
		if err != nil {
			return err
		}
		summary := map[string]any{"ok": result.OK, "shards": map[string]any{}}
		shards := summary["shards"].(map[string]any)
		for path, shard := range result.Shards {
			shards[path] = map[string]any{
				"ok": shard.OK, "declared_rows": shard.DeclaredRows, "actual_rows": shard.ActualRows,
				"declared_columns": shard.DeclaredColumns, "actual_columns": shard.ActualColumns,
				"sha256": shard.SHA256,
			}
		}
		printJSON(summary)
		if !result.OK {
			return fmt.Errorf("validation failed")
		}
		return nil
	case "run", "resume":
		fs := flag.NewFlagSet(command, flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		tier := fs.String("tier", "small", "small|medium")
		stopAfter := fs.Int("stop-after", 0, "test helper: request stop after N tickets")
		stage := fs.String("stage", "", "optional through-stage: normalize|intent|priority|verify")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		stopSignals := make(chan os.Signal, 2)
		signal.Notify(stopSignals, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-stopSignals
			_ = a.DB.RequestStop()
			<-stopSignals
			os.Exit(130)
		}()
		result, err := a.RunTier(*tier, app.RunOptions{
			ClearStop: true, StopAfterTickets: *stopAfter, ThroughStage: *stage,
		})
		if err != nil {
			return err
		}
		printRunSummary(result)
		return nil
	case "stop":
		fs := flag.NewFlagSet("stop", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		if err := a.DB.RequestStop(); err != nil {
			return err
		}
		printJSON(map[string]string{"code": domain.CodeStopRequested})
		return nil
	case "status":
		fs := flag.NewFlagSet("status", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		tier := fs.String("tier", "small", "tier")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		policy, _ := a.DB.LatestPolicy(*tier)
		statuses, err := a.DB.StageStatuses(*tier)
		if err != nil {
			return err
		}
		fp, _ := a.DB.FingerprintSnapshot(*tier)
		stop, _ := a.DB.StopRequested()
		printJSON(map[string]any{"code": "OK", "tier": *tier, "policy_version": version(policy), "stage_status": statuses, "fingerprints": fp, "stop_requested": stop})
		return nil
	case "report":
		fs := flag.NewFlagSet("report", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		tier := fs.String("tier", "small", "tier")
		comparison := fs.String("comparison", "", "optional HIT|MISS|FALSE_POSITIVE|...")
		full := fs.Bool("full", false, "print full JSON report")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		report, err := a.DB.GetReport(*tier)
		if err != nil {
			return err
		}
		if report == nil {
			return fmt.Errorf("report not found; run %s first", *tier)
		}
		if *full {
			printJSON(report)
			return nil
		}
		out := map[string]any{
			"tier": report.Tier, "fingerprint": report.ReportFingerprint,
			"totals": report.Totals, "comparison_counts": report.ComparisonCounts,
			"intent_label_accuracy": report.IntentLabelAccuracy,
			"priority_accuracy":     report.PriorityAccuracy,
			"reference":             report.Reference, "strategy": report.Strategy.SelectedStrategy,
			"report_path": a.ReportPath(*tier),
		}
		if *comparison != "" {
			details := []domain.VerificationDetail{}
			for _, d := range report.Details {
				if d.Comparison == *comparison {
					details = append(details, d)
				}
			}
			out["comparison_filter"] = *comparison
			out["details"] = details
		}
		printJSON(out)
		return nil
	case "replace-shard":
		fs := flag.NewFlagSet("replace-shard", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		tier := fs.String("tier", "small", "tier")
		shard := fs.String("shard", "", "manifest shard rel path")
		file := fs.String("file", "", "replacement JSONL path")
		demo := fs.Bool("demo", false, "use deterministic one-ticket text mutation")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		if *demo || *shard == "" {
			result, biz := a.DemoReplaceShard(*tier)
			if biz != nil {
				return biz
			}
			printJSON(result)
			return nil
		}
		result, biz := a.ReplaceShardFile(*tier, *shard, *file)
		if biz != nil {
			return biz
		}
		printJSON(result)
		return nil
	case "change-config":
		fs := flag.NewFlagSet("change-config", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		tier := fs.String("tier", "small", "tier")
		key := fs.String("key", "", "config key")
		rawValue := fs.String("value", "", "JSON value")
		note := fs.String("note", "", "change note")
		if err := fs.Parse(args); err != nil {
			return err
		}
		var value any
		if err := json.Unmarshal([]byte(*rawValue), &value); err != nil {
			return fmt.Errorf("value must be JSON: %w", err)
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		result, biz := a.ChangeConfig(app.ConfigChangeRequest{Tier: *tier, Key: *key, Value: value, Note: *note})
		if biz != nil {
			return biz
		}
		printJSON(result)
		return nil
	case "rollback":
		fs := flag.NewFlagSet("rollback", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		tier := fs.String("tier", "small", "tier")
		target := fs.Int64("version", 1, "target policy version")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		result, biz := a.RollbackConfig(*tier, *target)
		if biz != nil {
			return biz
		}
		printJSON(result)
		return nil
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		root := fs.String("root", "data", "data root")
		addr := fs.String("addr", "127.0.0.1:18099", "loopback listen address")
		if err := fs.Parse(args); err != nil {
			return err
		}
		a, err := app.New(*root)
		if err != nil {
			return err
		}
		defer a.Close()
		server := api.NewServer(a, *addr)
		fmt.Printf("csrouter listening on http://%s (root=%s)\n", *addr, a.Root)
		return server.ListenAndServe()
	case "demo":
		return runDemo(args)
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func openApp(args []string) (*app.App, error) {
	fs := flag.NewFlagSet("app", flag.ContinueOnError)
	root := fs.String("root", "data", "data root")
	_ = fs.Parse(args)
	return app.New(*root)
}

func printRunSummary(result *app.RunResult) {
	if result.Report == nil {
		printJSON(map[string]any{"code": result.Code, "message": result.Message, "tier": result.Tier, "completed": result.CompletedTickets, "total": result.TotalTickets, "strategy": result.Strategy.SelectedStrategy})
		return
	}
	printJSON(map[string]any{
		"code": result.Code, "tier": result.Tier, "completed": result.CompletedTickets,
		"strategy":              result.Strategy.SelectedStrategy,
		"report_fingerprint":    result.Report.ReportFingerprint,
		"comparison_counts":     result.Report.ComparisonCounts,
		"reference_pass":        result.Report.Reference.Pass,
		"reference":             result.Report.Reference,
		"priority_accuracy":     result.Report.PriorityAccuracy,
		"intent_label_accuracy": result.Report.IntentLabelAccuracy,
	})
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func version(p *domain.Policy) int64 {
	if p == nil {
		return 0
	}
	return p.EffectiveVersion()
}
