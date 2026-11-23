package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"dnsdetect"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "dnsdetect:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "demo":
		return runDemo(args[1:])
	case "samples":
		return runSamples(args[1:])
	case "replay":
		return runReplay(args[1:])
	case "verify":
		return runVerify(args[1:])
	case "help", "-h", "--help":
		return usage()
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usageText)
	}
}

const usageText = `usage:
  dnsdetect demo   --workdir DIR [--keep-state] [--report FILE]
  dnsdetect samples --workdir DIR
  dnsdetect replay --db FILE [--version N] [--report FILE]
  dnsdetect verify --workdir DIR
`

func usage() error {
	fmt.Fprint(os.Stdout, usageText)
	return nil
}

func runVerify(args []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	workdir := flags.String("workdir", ".dnsdetect-verify", "deterministic verification directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	absolute, err := filepath.Abs(*workdir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(absolute, 0o755); err != nil {
		return err
	}
	dataset, err := dnsdetect.GenerateDataset(filepath.Join(absolute, "samples"))
	if err != nil {
		return err
	}
	config := dnsdetect.DefaultConfig()
	config.WindowAnchorTicks = dataset.ObservationStartTicks
	dbPath := filepath.Join(absolute, "verify.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(dbPath + suffix)
	}
	store, err := dnsdetect.OpenSnapshotStore(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	engine := dnsdetect.NewEngine(config, store)
	first, err := engine.Process(context.Background(), dataset.Sources)
	if err != nil {
		return err
	}
	counts := make(map[string]int)
	for _, finding := range first.Report.Findings {
		counts[finding.Code]++
	}
	for _, code := range []string{
		dnsdetect.CodeUnsolicitedResponse,
		dnsdetect.CodeIrrelevantAdditional,
		dnsdetect.CodeTTLMismatch,
		dnsdetect.CodeMultiAddress,
	} {
		if counts[code] != 1 {
			return fmt.Errorf("verification expected one %s, got %d", code, counts[code])
		}
	}
	firstBytes, err := first.Report.CanonicalBytes()
	if err != nil {
		return err
	}
	second, err := engine.Process(context.Background(), dataset.Sources)
	if err != nil {
		return err
	}
	secondBytes, err := second.Report.CanonicalBytes()
	if err != nil {
		return err
	}
	if !bytes.Equal(firstBytes, secondBytes) || second.Snapshot.Version != first.Snapshot.Version {
		return fmt.Errorf("verification report is not byte-stable or snapshot is not idempotent")
	}

	for _, sample := range dataset.Malformed {
		if sample.ExpectedCode == dnsdetect.CodeSnapshotVersion {
			gapStore, err := dnsdetect.OpenSnapshotStore(sample.Path)
			if err != nil {
				return err
			}
			result, loadErr := gapStore.Load(context.Background())
			gapStore.Close()
			if dnsdetect.CodeOf(loadErr) != sample.ExpectedCode || result.Latest == nil || result.Latest.Version != 1 {
				return fmt.Errorf("verification %s: got code %s fallback %+v", sample.Name, dnsdetect.CodeOf(loadErr), result.Latest)
			}
			continue
		}
		_, processErr := engine.Process(context.Background(), []dnsdetect.SourceSpec{{Path: sample.Path, Kind: sample.Kind}})
		if dnsdetect.CodeOf(processErr) != sample.ExpectedCode {
			return fmt.Errorf("verification %s: got code %s, want %s", sample.Name, dnsdetect.CodeOf(processErr), sample.ExpectedCode)
		}
		latest, err := store.Latest(context.Background())
		if err != nil || latest == nil || latest.Version != first.Snapshot.Version {
			return fmt.Errorf("verification %s advanced snapshot: %+v, %v", sample.Name, latest, err)
		}
	}
	fmt.Printf("verified malformed=6 findings=4 pairs=%d snapshot_version=%d report_bytes=%d\n",
		len(first.Report.Pairs), first.Snapshot.Version, len(firstBytes))
	return nil
}

func runDemo(args []string) error {
	flags := flag.NewFlagSet("demo", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	workdir := flags.String("workdir", ".dnsdetect-demo", "deterministic sample and state directory")
	keepState := flags.Bool("keep-state", false, "retain an existing snapshot database (idempotent runs still reuse matching content)")
	reportPath := flags.String("report", "", "report path (default: WORKDIR/report.json)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	absolute, err := filepath.Abs(*workdir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(absolute, 0o755); err != nil {
		return err
	}
	dbPath := filepath.Join(absolute, "state.db")
	if !*keepState {
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + "-wal")
		_ = os.Remove(dbPath + "-shm")
	}
	dataset, err := dnsdetect.GenerateDataset(filepath.Join(absolute, "samples"))
	if err != nil {
		return err
	}
	config := dnsdetect.DefaultConfig()
	config.WindowAnchorTicks = dataset.ObservationStartTicks
	store, err := dnsdetect.OpenSnapshotStore(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	engine := dnsdetect.NewEngine(config, store)
	result, err := engine.Process(context.Background(), dataset.Sources)
	if err != nil {
		return err
	}
	if *reportPath == "" {
		*reportPath = filepath.Join(absolute, "report.json")
	}
	if err := dnsdetect.ExportReport(*reportPath, result.Report); err != nil {
		return err
	}
	counts := make(map[string]int)
	for _, finding := range result.Report.Findings {
		counts[finding.Code]++
	}
	fmt.Printf("snapshot_version=%d content_digest=%s\n", result.Snapshot.Version, result.Snapshot.ContentDigest)
	fmt.Printf("sources=%d pairs=%d findings=%d uncertain=%d\n", len(result.Report.Sources), len(result.Report.Pairs), len(result.Report.Findings), len(result.Report.Uncertain))
	for _, code := range []string{
		dnsdetect.CodeUnsolicitedResponse,
		dnsdetect.CodeIrrelevantAdditional,
		dnsdetect.CodeTTLMismatch,
		dnsdetect.CodeMultiAddress,
	} {
		fmt.Printf("%s=%d\n", code, counts[code])
	}
	fmt.Printf("report=%s\n", *reportPath)
	return nil
}

func runSamples(args []string) error {
	flags := flag.NewFlagSet("samples", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	workdir := flags.String("workdir", ".dnsdetect-demo", "deterministic sample directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	dataset, err := dnsdetect.GenerateDataset(*workdir)
	if err != nil {
		return err
	}
	fmt.Printf("samples=%s\n", dataset.Dir)
	for _, source := range dataset.Sources {
		fmt.Printf("source=%s kind=%s\n", source.Path, source.Kind)
	}
	for _, sample := range dataset.Malformed {
		fmt.Printf("malformed=%s code=%s\n", sample.Path, sample.ExpectedCode)
	}
	return nil
}

func runReplay(args []string) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	dbPath := flags.String("db", "", "snapshot SQLite database")
	version := flags.Int64("version", 0, "snapshot version (0 means latest contiguous)")
	reportPath := flags.String("report", "", "output JSON report (default stdout)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dbPath == "" {
		return fmt.Errorf("replay requires --db")
	}
	store, err := dnsdetect.OpenSnapshotStore(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	report, replayErr := store.Replay(context.Background(), *version)
	if report == nil {
		return replayErr
	}
	if *reportPath != "" {
		if err := dnsdetect.ExportReport(*reportPath, report); err != nil {
			return err
		}
		fmt.Printf("report=%s\n", *reportPath)
	} else {
		if err := dnsdetect.WriteReport(os.Stdout, report); err != nil {
			return err
		}
	}
	if replayErr != nil {
		return replayErr
	}
	return nil
}
