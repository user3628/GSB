package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"secverify/internal/config"
	"secverify/internal/engine"
	"secverify/internal/model"
	"secverify/internal/report"
	"secverify/internal/sample"
	"secverify/internal/snapshot"
)

type commonOptions struct {
	workdir      string
	reportPath   string
	keep         bool
	maxTx        int
	maxProbes    int
	waitTimeout  time.Duration
	probeTimeout time.Duration
	probeDelay   time.Duration
}

func addCommonFlags(fs *flag.FlagSet, opts *commonOptions) {
	fs.StringVar(&opts.workdir, "workdir", "", "empty/new working directory")
	fs.StringVar(&opts.reportPath, "report", "-", "report path, or - for stdout")
	fs.BoolVar(&opts.keep, "keep", false, "keep automatically-created temp directory")
	fs.IntVar(&opts.maxTx, "max-tx", 4, "maximum in-flight change transactions")
	fs.IntVar(&opts.maxProbes, "max-probes", 2, "maximum in-flight probe processes")
	fs.DurationVar(&opts.waitTimeout, "wait-timeout", 2*time.Second, "contention wait threshold")
	fs.DurationVar(&opts.probeTimeout, "probe-timeout", 10*time.Second, "probe subprocess timeout")
	fs.DurationVar(&opts.probeDelay, "probe-delay", 0, "delay while holding a probe slot")
}

func prepareRoot(opts commonOptions) (string, func(), error) {
	if opts.workdir == "" {
		root, err := os.MkdirTemp("", "secverify-lab-")
		if err != nil {
			return "", nil, err
		}
		cleanup := func() {}
		if !opts.keep {
			cleanup = func() { _ = os.RemoveAll(root) }
		}
		return root, cleanup, nil
	}
	root, err := filepath.Abs(opts.workdir)
	if err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", nil, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", nil, err
	}
	if len(entries) > 0 {
		return "", nil, fmt.Errorf("workdir %s must be empty", root)
	}
	return root, func() {}, nil
}

func engineOpts(root, scenario string, opts commonOptions) (engine.Options, string) {
	dir := filepath.Join(root, scenario)
	return engine.Options{
		DBPath:          filepath.Join(dir, "engine.db"),
		WorkDir:         dir,
		MaxTransactions: opts.maxTx,
		MaxProbes:       opts.maxProbes,
		WaitTimeout:     opts.waitTimeout,
		ProbeTimeout:    opts.probeTimeout,
		ProbeDelay:      opts.probeDelay,
	}, dir
}

func writeReportFile(path string, r report.VerificationReport) ([]byte, error) {
	data, err := report.Marshal(r)
	if err != nil {
		return nil, err
	}
	if path == "" || path == "-" {
		if _, err := os.Stdout.Write(data); err != nil {
			return nil, err
		}
		return data, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(abs, data, 0o644); err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "report=%s\n", abs)
	return data, nil
}

func loadBaseline(path string) (*config.Baseline, *model.BizError) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, model.NewError(model.CodeConfigParse, "", 0, err.Error())
	}
	return config.ParseBaseline(data)
}

func readFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func writeSamples(root string) (sample.PathSet, error) {
	return sample.WriteFiles(filepath.Join(root, "samples"))
}

func roundCode(result engine.RoundResult) string {
	if result.Issue == nil {
		return model.CodeOK
	}
	return result.Issue.Code
}

func joinItems(values []string) string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return strings.Join(copyValues, ",")
}

func containsAll(actual, expected []string) bool {
	set := make(map[string]struct{}, len(actual))
	for _, item := range actual {
		set[item] = struct{}{}
	}
	for _, item := range expected {
		if _, ok := set[item]; !ok {
			return false
		}
	}
	return true
}

func chainVersionList(chain *snapshot.Chain) string {
	if chain == nil || len(chain.Versions) == 0 {
		return "<empty>"
	}
	parts := make([]string, 0, len(chain.Versions))
	for _, version := range chain.Versions {
		parts = append(parts, fmt.Sprintf("%d", version))
	}
	return strings.Join(parts, ",")
}

func refsFromChain(chain *snapshot.Chain) []engine.SnapshotRef {
	if chain == nil {
		return []engine.SnapshotRef{}
	}
	refs := make([]engine.SnapshotRef, 0, len(chain.Snapshots))
	for _, snap := range chain.Snapshots {
		refs = append(refs, engine.SnapshotRef{
			Version:     snap.Version,
			ContentHash: snap.ContentHash,
			ParentHash:  snap.ParentHash,
			StateHash:   snap.Payload.StateHash,
			ChangeHash:  snap.Payload.ChangeHash,
		})
	}
	return refs
}

func namespaceReport(engineRef *engine.Engine, namespace string, rounds []engine.RoundResult) (report.NamespaceReport, error) {
	chain, bizErr := engineRef.RawChain(context.Background(), namespace)
	if bizErr != nil {
		return report.NamespaceReport{}, bizErr
	}
	return namespaceReportFromChain(namespace, rounds, chain), nil
}

func namespaceReportFromChain(namespace string, rounds []engine.RoundResult, chain *snapshot.Chain) report.NamespaceReport {
	out := report.NamespaceReport{
		Namespace:    namespace,
		Rounds:       rounds,
		VersionChain: refsFromChain(chain),
	}
	if chain.Latest != nil {
		out.FinalVersion = chain.Latest.Version
		out.FinalStateHash = chain.Latest.Payload.StateHash
	}
	return out
}

func latestFromChain(chain *snapshot.Chain) *snapshot.Snapshot {
	if chain == nil || chain.Latest == nil {
		return nil
	}
	return chain.Latest
}

func summarize(result report.VerificationReport) {
	fmt.Fprintf(os.Stderr, "scenario=%s result=%s checks=%d injections=%d crash_checks=%d\n",
		result.Scenario, result.Result, len(result.Checks), len(result.Injections), len(result.CrashChecks))
}
