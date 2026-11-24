package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"supplyaudit/internal/formats"
	"supplyaudit/internal/merge"
	"supplyaudit/internal/model"
	"supplyaudit/internal/pipeline"
	"supplyaudit/internal/report"
	"supplyaudit/internal/sample"
	"supplyaudit/internal/snapshot"
	"supplyaudit/internal/verify"
)

const usageText = `supplyaudit - deterministic local dependency-supply-chain auditor

Usage:
  supplyaudit sample  --workdir DIR [--clean]
  supplyaudit audit   --input DIR --db FILE --report FILE [options]
  supplyaudit trace   --db FILE --component KEY [--snapshot N] [options]
  supplyaudit replay  --db FILE --snapshot N [--report FILE] [options]
  supplyaudit config  [--format-version v1|v2] [--priority LIST]
  supplyaudit verify  --workdir DIR [--report FILE]

Run "supplyaudit <command> -h" for command flags.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usageText)
		return errors.New("a subcommand is required")
	}
	ctx := context.Background()
	switch args[0] {
	case "sample":
		return runSample(args[1:])
	case "audit":
		return runAudit(ctx, args[1:])
	case "trace":
		return runTrace(ctx, args[1:])
	case "replay":
		return runReplay(ctx, args[1:])
	case "config":
		return runConfig(args[1:])
	case "verify":
		return runVerify(ctx, args[1:])
	case "-h", "--help", "help":
		fmt.Print(usageText)
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type fileOptions struct {
	Input                  string   `json:"input"`
	DB                     string   `json:"db"`
	Report                 string   `json:"report"`
	FormatVersion          string   `json:"format_version"`
	Priority               []string `json:"priority"`
	MaxConcurrency         int      `json:"max_concurrency"`
	MaxComponentsPerSource int      `json:"max_components_per_source"`
	NoCommit               bool     `json:"no_commit"`
	FailOnCodes            []string `json:"fail_on_codes"`
	Sources                []string `json:"sources"`
}

func runAudit(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("audit", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var input, dbPath, reportPath, configPath, formatVersion, priorityValue string
	var maxConcurrency, maxComponents int
	var noCommit bool
	var explicitSources, failOnCodes stringList
	flags.StringVar(&input, "input", "", "directory containing manifests")
	flags.StringVar(&dbPath, "db", "", "SQLite snapshot database")
	flags.StringVar(&reportPath, "report", "", "JSON report output path")
	flags.StringVar(&configPath, "config", "", "JSON configuration file")
	flags.StringVar(&formatVersion, "format-version", "", "binary adapter target: v1 or v2")
	flags.StringVar(&priorityValue, "priority", "", "source priority: binary,jsonl,textblock")
	flags.IntVar(&maxConcurrency, "max-concurrency", 0, "maximum in-flight parse sessions")
	flags.IntVar(&maxComponents, "max-components", 0, "maximum retained components per source")
	flags.BoolVar(&noCommit, "no-commit", false, "run and report without committing a snapshot")
	flags.Var(&explicitSources, "source", "explicit manifest path (repeatable)")
	flags.Var(&failOnCodes, "fail-on-code", "do not commit when this code is present (repeatable or comma-separated)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	visited := make(map[string]bool)
	flags.Visit(func(current *flag.Flag) { visited[current.Name] = true })
	var fileConfig fileOptions
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &fileConfig); err != nil {
			return fmt.Errorf("invalid config file: %w", err)
		}
	}
	chooseString := func(flagName, flagValue, configValue, fallback string) string {
		if visited[flagName] {
			return flagValue
		}
		if configValue != "" {
			return configValue
		}
		return fallback
	}
	input = chooseString("input", input, fileConfig.Input, "")
	dbPath = chooseString("db", dbPath, fileConfig.DB, "supplyaudit.db")
	reportPath = chooseString("report", reportPath, fileConfig.Report, "audit-report.json")
	formatVersion = chooseString("format-version", formatVersion, fileConfig.FormatVersion, "v2")
	if !visited["priority"] && len(fileConfig.Priority) > 0 {
		priorityValue = strings.Join(fileConfig.Priority, ",")
	}
	if priorityValue == "" {
		priorityValue = "binary,jsonl,textblock"
	}
	if !visited["max-concurrency"] && fileConfig.MaxConcurrency > 0 {
		maxConcurrency = fileConfig.MaxConcurrency
	}
	if maxConcurrency == 0 {
		maxConcurrency = 3
	}
	if !visited["max-components"] && fileConfig.MaxComponentsPerSource > 0 {
		maxComponents = fileConfig.MaxComponentsPerSource
	}
	if maxComponents == 0 {
		maxComponents = 1000
	}
	if !visited["no-commit"] {
		noCommit = fileConfig.NoCommit
	}
	if len(explicitSources) == 0 && len(fileConfig.Sources) > 0 {
		explicitSources = append(explicitSources, fileConfig.Sources...)
	}
	if len(failOnCodes) == 0 && len(fileConfig.FailOnCodes) > 0 {
		failOnCodes = append(failOnCodes, fileConfig.FailOnCodes...)
	}
	priority, err := parsePriority(priorityValue)
	if err != nil {
		return err
	}
	config := pipeline.Config{
		BinaryFormatVersion:    formatVersion,
		Priority:               priority,
		MaxConcurrency:         maxConcurrency,
		MaxComponentsPerSource: maxComponents,
	}
	if err := config.Normalize(); err != nil {
		return err
	}
	if input == "" && len(explicitSources) == 0 {
		return errors.New("audit requires --input or at least one --source")
	}
	sources := []model.SourceSpec{}
	if input != "" {
		discovered, err := formats.Discover(input)
		if err != nil {
			return err
		}
		sources = append(sources, discovered...)
	}
	for _, path := range explicitSources {
		spec, err := formats.SpecFromFile(path, input)
		if err != nil {
			return err
		}
		sources = append(sources, spec)
	}
	if len(sources) == 0 {
		return errors.New("no recognized source manifests found")
	}
	result, err := pipeline.Run(sources, config)
	if err != nil {
		return err
	}
	data := report.BuildAuditData(result)
	payload, err := report.MarshalAuditData(data)
	if err != nil {
		return err
	}

	store, err := snapshot.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	existingChain, err := store.Chain(ctx)
	if err != nil {
		return err
	}
	nextVersion := 1
	if len(existingChain) > 0 {
		nextVersion = existingChain[len(existingChain)-1].Version + 1
	}
	section := report.SnapshotSection{RequestedVersion: nextVersion, Status: "not_requested", Chain: existingChain}
	blocking := result.BlockingIssueCodes()
	policyCodes := matchCodes(result.AllCodes(), expandCodes(failOnCodes))
	var commitErr error
	shouldCommit := !noCommit && len(blocking) == 0 && len(policyCodes) == 0
	switch {
	case noCommit:
		section.Status = "not_requested"
	case len(blocking) > 0:
		section.Status = "blocked_parse_error"
		section.Error = strings.Join(blocking, ",")
	case len(policyCodes) > 0:
		section.Status = "rejected_by_policy"
		section.Error = strings.Join(policyCodes, ",")
	case shouldCommit:
		meta, err := store.Append(ctx, payload, result.RequiredFormats, config.PriorityStrings(), config.AcceptedFormats())
		if err != nil {
			commitErr = err
			section.Status = "commit_error"
			section.Error = model.CodeOf(err)
			if section.Error == "" {
				section.Error = err.Error()
			}
		} else {
			section.Status = "committed"
			section.CommittedVersion = meta.Version
		}
	}
	chain, chainErr := store.Chain(ctx)
	if chainErr != nil && commitErr == nil {
		commitErr = chainErr
	}
	section.Chain = chain
	finalReport := report.Build(data, section)
	if _, err := report.Write(reportPath, finalReport); err != nil {
		return err
	}
	if commitErr != nil {
		return commitErr
	}
	if len(blocking) > 0 {
		return fmt.Errorf("audit report written to %s; snapshot not advanced due to %s", reportPath, strings.Join(blocking, ","))
	}
	if len(policyCodes) > 0 {
		return fmt.Errorf("audit report written to %s; snapshot rejected by --fail-on-code for %s", reportPath, strings.Join(policyCodes, ","))
	}
	fmt.Fprintf(os.Stdout, "audit report: %s\nsnapshot: %s\n", reportPath, section.Status)
	return nil
}

type traceReport struct {
	ReportSchemaVersion int                  `json:"report_schema_version"`
	Marker              string               `json:"marker"`
	SampleTimestamp     string               `json:"sample_timestamp"`
	SnapshotVersion     int                  `json:"snapshot_version"`
	ComponentKey        string               `json:"component_key"`
	Found               bool                 `json:"found"`
	Component           *merge.ComponentView `json:"component,omitempty"`
	Findings            []model.Finding      `json:"findings"`
	SnapshotChain       []snapshot.Meta      `json:"snapshot_chain"`
}

func runTrace(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("trace", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var dbPath, componentKey, formatVersion, priorityValue, outputPath string
	var version int
	flags.StringVar(&dbPath, "db", "supplyaudit.db", "SQLite snapshot database")
	flags.StringVar(&componentKey, "component", "", "normalized or raw component key")
	flags.IntVar(&version, "snapshot", 0, "snapshot version; 0 selects latest")
	flags.StringVar(&formatVersion, "format-version", "v2", "compatibility target: v1 or v2")
	flags.StringVar(&priorityValue, "priority", "binary,jsonl,textblock", "compatibility source priority")
	flags.StringVar(&outputPath, "report", "", "optional output file; stdout when empty")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if componentKey == "" {
		return errors.New("trace requires --component")
	}
	priority, err := parsePriority(priorityValue)
	if err != nil {
		return err
	}
	config := pipeline.Config{BinaryFormatVersion: formatVersion, Priority: priority, MaxConcurrency: 1, MaxComponentsPerSource: 1}
	if err := config.Normalize(); err != nil {
		return err
	}
	store, err := snapshot.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if version == 0 {
		version, err = store.LatestVersion(ctx)
		if err != nil {
			return err
		}
	}
	loaded, err := store.Load(ctx, version, snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: config.AcceptedFormats(), Priority: config.PriorityStrings()})
	if err != nil {
		return err
	}
	data, err := report.ParseAuditData(loaded.Payload)
	if err != nil {
		return err
	}
	chain, err := store.Chain(ctx)
	if err != nil {
		return err
	}
	key := strings.ToLower(strings.TrimSpace(componentKey))
	trace := traceReport{
		ReportSchemaVersion: report.ReportSchemaVersion,
		Marker:              report.SampleTimestamp,
		SampleTimestamp:     report.SampleTimestamp,
		SnapshotVersion:     version,
		ComponentKey:        key,
		Findings:            []model.Finding{},
		SnapshotChain:       chain,
	}
	for index := range data.Components {
		if data.Components[index].ComponentKey == key {
			component := data.Components[index]
			trace.Component = &component
			trace.Found = true
			break
		}
	}
	for _, finding := range data.Findings {
		if finding.ComponentKey == key {
			trace.Findings = append(trace.Findings, finding)
		}
	}
	return writeJSON(outputPath, trace)
}

func runReplay(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var dbPath, formatVersion, priorityValue, outputPath string
	var version int
	flags.StringVar(&dbPath, "db", "supplyaudit.db", "SQLite snapshot database")
	flags.IntVar(&version, "snapshot", 0, "snapshot version to replay")
	flags.StringVar(&formatVersion, "format-version", "v2", "compatibility target: v1 or v2")
	flags.StringVar(&priorityValue, "priority", "binary,jsonl,textblock", "compatibility source priority")
	flags.StringVar(&outputPath, "report", "", "optional output file; stdout when empty")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if version <= 0 {
		return errors.New("replay requires --snapshot N")
	}
	priority, err := parsePriority(priorityValue)
	if err != nil {
		return err
	}
	config := pipeline.Config{BinaryFormatVersion: formatVersion, Priority: priority, MaxConcurrency: 1, MaxComponentsPerSource: 1}
	if err := config.Normalize(); err != nil {
		return err
	}
	store, err := snapshot.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	loaded, err := store.Load(ctx, version, snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: config.AcceptedFormats(), Priority: config.PriorityStrings()})
	if err != nil {
		return err
	}
	data, err := report.ParseAuditData(loaded.Payload)
	if err != nil {
		return err
	}
	chain, err := store.Chain(ctx)
	if err != nil {
		return err
	}
	replayed := report.Build(data, report.SnapshotSection{
		RequestedVersion: version,
		CommittedVersion: version,
		Status:           "replay",
		Chain:            chain,
	})
	return writeJSON(outputPath, replayed)
}

type configReport struct {
	ReportSchemaVersion    int      `json:"report_schema_version"`
	Marker                 string   `json:"marker"`
	SampleTimestamp        string   `json:"sample_timestamp"`
	FormatVersion          string   `json:"format_version"`
	Priority               []string `json:"priority"`
	AcceptedFormats        []string `json:"accepted_formats"`
	MaxConcurrency         int      `json:"max_concurrency"`
	MaxComponentsPerSource int      `json:"max_components_per_source"`
}

func runConfig(args []string) error {
	flags := flag.NewFlagSet("config", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var formatVersion, priorityValue string
	var maxConcurrency, maxComponents int
	flags.StringVar(&formatVersion, "format-version", "v2", "target format version: v1 or v2")
	flags.StringVar(&priorityValue, "priority", "binary,jsonl,textblock", "source priority")
	flags.IntVar(&maxConcurrency, "max-concurrency", 3, "maximum in-flight sessions")
	flags.IntVar(&maxComponents, "max-components", 1000, "maximum components per source")
	if err := flags.Parse(args); err != nil {
		return err
	}
	priority, err := parsePriority(priorityValue)
	if err != nil {
		return err
	}
	config := pipeline.Config{BinaryFormatVersion: formatVersion, Priority: priority, MaxConcurrency: maxConcurrency, MaxComponentsPerSource: maxComponents}
	if err := config.Normalize(); err != nil {
		return err
	}
	value := configReport{
		ReportSchemaVersion:    report.ReportSchemaVersion,
		Marker:                 report.SampleTimestamp,
		SampleTimestamp:        report.SampleTimestamp,
		FormatVersion:          config.BinaryFormatVersion,
		Priority:               config.PriorityStrings(),
		AcceptedFormats:        config.AcceptedFormats(),
		MaxConcurrency:         config.MaxConcurrency,
		MaxComponentsPerSource: config.MaxComponentsPerSource,
	}
	data, err := report.Marshal(value)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func runSample(args []string) error {
	flags := flag.NewFlagSet("sample", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var workDir string
	var clean bool
	flags.StringVar(&workDir, "workdir", "", "sample work directory")
	flags.BoolVar(&clean, "clean", false, "remove owned input/ and faults/ directories first")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if workDir == "" {
		return errors.New("sample requires --workdir")
	}
	layout, err := sample.Generate(workDir, clean)
	if err != nil {
		return err
	}
	manifest, err := sample.LoadFaultManifest(layout.FaultManifest)
	if err != nil {
		return err
	}
	value := struct {
		SchemaVersion  int      `json:"schema_version"`
		Marker         string   `json:"marker"`
		Sources        []string `json:"sources"`
		FaultManifest  string   `json:"fault_manifest"`
		FaultScenarios int      `json:"fault_scenarios"`
	}{
		SchemaVersion:  1,
		Marker:         report.SampleTimestamp,
		Sources:        []string{sample.BinaryV1File, sample.BinaryV2File, sample.JSONLFile, sample.TextFile},
		FaultManifest:  sample.FaultManifestFile,
		FaultScenarios: len(manifest.Scenarios),
	}
	data, err := report.Marshal(value)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}

func runVerify(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var workDir, reportPath string
	flags.StringVar(&workDir, "workdir", "", "sample and state work directory")
	flags.StringVar(&reportPath, "report", "", "verification report path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if workDir == "" {
		return errors.New("verify requires --workdir")
	}
	if reportPath == "" {
		reportPath = filepath.Join(workDir, "verification-report.json")
	}
	verification, err := verify.Run(ctx, workDir)
	if err != nil {
		return err
	}
	if _, err := report.Write(reportPath, verification); err != nil {
		return err
	}
	status := "PASS"
	if !verification.AllPassed {
		status = "FAIL"
	}
	fmt.Fprintf(os.Stdout, "verification: %s\nreport: %s\n", status, reportPath)
	if !verification.AllPassed {
		return errors.New("one or more fault-injection assertions failed")
	}
	return nil
}

func parsePriority(value string) ([]model.SourceFormat, error) {
	pieces := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ';' || r == ' ' })
	if len(pieces) == 0 {
		return nil, errors.New("priority is empty")
	}
	priority := make([]model.SourceFormat, 0, len(pieces))
	for _, piece := range pieces {
		var format model.SourceFormat
		switch strings.ToLower(strings.TrimSpace(piece)) {
		case "binary", "binary-collector", "bin", "collector":
			format = model.FormatBinary
		case "json", "jsonl", "json-export", "jsonlines":
			format = model.FormatJSONL
		case "text", "textblock", "textblock-legacy", "legacy":
			format = model.FormatTextBlock
		default:
			return nil, fmt.Errorf("unknown priority format %q", piece)
		}
		priority = append(priority, format)
	}
	return priority, nil
}

func writeJSON(path string, value any) error {
	if path == "" {
		data, err := report.Marshal(value)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(data)
		return err
	}
	_, err := report.Write(path, value)
	return err
}

func expandCodes(values []string) []string {
	out := []string{}
	for _, value := range values {
		for _, code := range strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ';' || r == ' ' }) {
			if code != "" {
				out = append(out, code)
			}
		}
	}
	sort.Strings(out)
	return out
}

func matchCodes(observed, expected []string) []string {
	out := []string{}
	set := make(map[string]struct{}, len(observed))
	for _, code := range observed {
		set[code] = struct{}{}
	}
	for _, code := range expected {
		if _, ok := set[code]; ok {
			out = append(out, code)
		}
	}
	return model.NormalizeCodes(out)
}
