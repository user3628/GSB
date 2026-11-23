package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/policy"
	"baselineworkbench/internal/report"
	"baselineworkbench/internal/samples"
	"baselineworkbench/internal/snapshot"
)

type RunOptions struct {
	SamplesRoot   string
	DatabasePath  string
	Config        engine.Config
	ForceVersions map[string]int64
}

type RunOutput struct {
	Report      report.Report
	ReportJSON  []byte
	SummaryText string
	Manifest    samples.Manifest
}

func Run(ctx context.Context, options RunOptions) (RunOutput, error) {
	manifest, err := samples.LoadManifest(options.SamplesRoot)
	if err != nil {
		return RunOutput{}, fmt.Errorf("load sample manifest: %w", err)
	}
	inputs, err := loadInputs(options.SamplesRoot, manifest)
	if err != nil {
		return RunOutput{}, err
	}
	var referenceInput *engine.Input
	for i := range inputs {
		if slash(inputs[i].PackagePath) == slash(manifest.Reference) {
			copy := inputs[i]
			referenceInput = &copy
			break
		}
	}
	if referenceInput == nil {
		return RunOutput{}, fmt.Errorf("reference package %q not found", manifest.Reference)
	}
	reference := engine.Prepare(*referenceInput)
	if len(reference.Issues) > 0 {
		return RunOutput{}, fmt.Errorf("reference package is invalid: %s", reference.Issues[0].Message)
	}
	results := engine.Run(ctx, inputs, reference, options.Config)
	store, err := snapshot.Open(options.DatabasePath)
	if err != nil {
		return RunOutput{}, fmt.Errorf("open snapshot store: %w", err)
	}
	defer store.Close()
	commitErrors := make(map[string]*codes.BusinessError)
	for _, result := range results {
		if !result.Complete || len(result.Codes) > 0 {
			continue
		}
		payload, err := snapshot.EncodeResult(result.PackagePath, manifest.SampleMarker, result)
		if err != nil {
			return RunOutput{}, fmt.Errorf("encode snapshot for %s: %w", result.PackagePath, err)
		}
		appendOptions := snapshot.AppendOptions{}
		if forced, ok := options.ForceVersions[result.PackagePath]; ok {
			appendOptions.ForcedVersion = forced
		}
		if _, business := store.Append(ctx, result.PackagePath, payload, manifest.SampleMarker, appendOptions); business != nil {
			commitErrors[result.PackagePath] = business
		}
	}
	chains := make(map[string]snapshot.Chain, len(results))
	for index := range results {
		result := &results[index]
		chain, err := store.LoadChain(ctx, result.PackagePath)
		if err != nil {
			return RunOutput{}, fmt.Errorf("load snapshot chain for %s: %w", result.PackagePath, err)
		}
		chains[result.PackagePath] = chain
		if chain.ErrorCode != codes.None {
			result.RecoveryCode = chain.ErrorCode
			if chain.Active != nil {
				content, decodeErr := snapshot.DecodeContent(chain.Active.Payload)
				if decodeErr != nil {
					return RunOutput{}, fmt.Errorf("decode recovered snapshot %s v%d: %w", result.PackagePath, chain.Active.Version, decodeErr)
				}
				result.RecoveredFromVersion = chain.Active.Version
				applyDiagnostic(result, content)
			}
		}
	}
	built := report.Build(report.BuildOptions{
		SampleMarker:     manifest.SampleMarker,
		ReferencePackage: manifest.Reference,
		Config:           options.Config,
		Results:          results,
		Chains:           chains,
		CommitErrors:     commitErrors,
	})
	encoded, err := report.JSONBytes(built)
	if err != nil {
		return RunOutput{}, err
	}
	return RunOutput{
		Report:      built,
		ReportJSON:  encoded,
		SummaryText: report.TerminalSummary(built),
		Manifest:    manifest,
	}, nil
}

func loadInputs(root string, manifest samples.Manifest) ([]engine.Input, error) {
	inputs := make([]engine.Input, 0, len(manifest.Packages))
	for _, pkg := range manifest.Packages {
		baselinePath := filepath.Join(root, filepath.FromSlash(pkg.Path), filepath.FromSlash(pkg.Baseline))
		repairsPath := filepath.Join(root, filepath.FromSlash(pkg.Path), filepath.FromSlash(pkg.Repairs))
		baseline, err := os.ReadFile(baselinePath)
		if err != nil {
			return nil, fmt.Errorf("read baseline %s: %w", baselinePath, err)
		}
		repairs, err := os.ReadFile(repairsPath)
		if err != nil {
			return nil, fmt.Errorf("read repairs %s: %w", repairsPath, err)
		}
		inputs = append(inputs, engine.Input{
			PackagePath: slash(pkg.Path),
			Baseline:    baseline,
			Repairs:     repairs,
		})
	}
	return inputs, nil
}

func slash(path string) string { return strings.ReplaceAll(path, "\\", "/") }

func applyDiagnostic(result *engine.Result, content snapshot.Content) {
	result.Items = append([]policy.Item{}, content.Items...)
	result.EffectiveCount = len(content.Items)
	if content.Diagnostic == nil {
		return
	}
	diagnostic := content.Diagnostic
	result.ScoreMicros = diagnostic.ScoreMicros
	result.ErrorUpperMicros = diagnostic.ErrorUpperMicros
	result.ScoreInterval = diagnostic.ScoreInterval
	result.Comparisons = append([]engine.Comparison{}, diagnostic.Comparisons...)
	result.Deterministic = diagnostic.Deterministic
	result.Uncertain = diagnostic.Uncertain
	result.Ledger = diagnostic.Ledger.Clone()
	result.ProcessedItems = len(diagnostic.Comparisons)
	result.Codes = append([]codes.Code{}, diagnostic.Codes...)
	result.Code = codes.Primary(result.Codes)
}
