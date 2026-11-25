package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/config"
	"ocrlab/internal/engine"
	"ocrlab/internal/model"
	"ocrlab/internal/store"
)

type BundleProduct struct {
	ImageID string          `json:"image_id"`
	Stage   string          `json:"stage"`
	Digest  string          `json:"digest"`
	Product json.RawMessage `json:"product"`
}

type Bundle struct {
	SchemaVersion  string          `json:"schema_version"`
	ManifestDigest string          `json:"manifest_digest"`
	ConfigVersion  string          `json:"config_version"`
	Products       []BundleProduct `json:"products"`
}

type VerificationSummary struct {
	Equal            bool   `json:"equal"`
	ReportDigest     string `json:"report_digest"`
	FullReportDigest string `json:"full_report_digest"`
	BundleDigest     string `json:"bundle_digest"`
	FullBundleDigest string `json:"full_bundle_digest"`
	FirstDifference  string `json:"first_difference,omitempty"`
}

func (p *Pipeline) LoadProduct(ctx context.Context, imageID, stage string, target any) (store.Checkpoint, string, error) {
	checkpoint, exists, err := p.store.GetCheckpoint(ctx, imageID, stage)
	if err != nil {
		return store.Checkpoint{}, "", err
	}
	if !exists {
		return store.Checkpoint{}, "", &apperr.Error{Code: apperr.CodeNotFound, Message: "stage checkpoint has not been produced", ImageID: imageID, Stage: stage}
	}
	if err := json.Unmarshal(checkpoint.Product, target); err != nil {
		return store.Checkpoint{}, "", &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: "checkpoint product cannot be decoded", ImageID: imageID, Stage: stage, Reason: "product_malformed"}
	}
	return checkpoint, checkpoint.ProductDigest, nil
}

func (p *Pipeline) Report(ctx context.Context) (model.Report, []byte, error) {
	return p.ReportForGroups(ctx, nil)
}

func (p *Pipeline) ReportForGroups(ctx context.Context, groups []string) (model.Report, []byte, error) {
	scoped := p.scopedManifest(groups)
	if len(scoped.Images) == 0 {
		return model.Report{}, nil, apperr.New(apperr.CodeNotFound, "no images are selected for the diagnostic report")
	}
	diagnostics := make([]model.ImageDiagnostic, 0, len(scoped.Images))
	for _, imageSpec := range scoped.Images {
		var diagnostic model.ImageDiagnostic
		_, _, err := p.LoadProduct(ctx, imageSpec.ImageID, model.StageDiagnostic, &diagnostic)
		if err != nil {
			return model.Report{}, nil, err
		}
		diagnostics = append(diagnostics, diagnostic)
	}
	report, err := BuildScopedReport(scoped, p.cfg, diagnostics)
	if err != nil {
		return model.Report{}, nil, err
	}
	report.ManifestDigest = p.manifestDigest
	reportBytes, err := canonical.Marshal(report)
	if err != nil {
		return model.Report{}, nil, err
	}
	return report, reportBytes, nil
}

func BuildScopedReport(manifest model.Manifest, cfg config.Config, diagnostics []model.ImageDiagnostic) (model.Report, error) {
	return engine.BuildReport(manifest, cfg, diagnostics)
}

func (p *Pipeline) Export(ctx context.Context, target string) (model.Report, string, error) {
	report, reportBytes, err := p.Report(ctx)
	if err != nil {
		return model.Report{}, "", err
	}
	if target == "" {
		target = filepath.Join(p.paths.ReportsDir, "report.json")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return model.Report{}, "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".report-*")
	if err != nil {
		return model.Report{}, "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(reportBytes); err != nil {
		tmp.Close()
		return model.Report{}, "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return model.Report{}, "", err
	}
	if err := tmp.Close(); err != nil {
		return model.Report{}, "", err
	}
	if err := os.Rename(tmpName, target); err != nil {
		return model.Report{}, "", err
	}
	digest := canonical.DigestBytes(reportBytes)
	checksumPath := target + ".sha256"
	if err := os.WriteFile(checksumPath, []byte(digest+"  "+filepath.Base(target)+"\n"), 0o644); err != nil {
		return model.Report{}, "", err
	}
	return report, digest, nil
}

func (p *Pipeline) BuildBundle(ctx context.Context) (Bundle, []byte, error) {
	checkpoints, err := p.store.AllCheckpoints(ctx)
	if err != nil {
		return Bundle{}, nil, err
	}
	byKey := make(map[string]store.Checkpoint, len(checkpoints))
	for _, checkpoint := range checkpoints {
		byKey[checkpoint.ImageID+"/"+checkpoint.Stage] = checkpoint
	}
	bundle := Bundle{SchemaVersion: model.SchemaVersion, ManifestDigest: p.manifestDigest, ConfigVersion: p.cfg.Version()}
	bundle.Products = make([]BundleProduct, 0, len(p.manifest.Images)*4)
	for _, imageSpec := range p.manifest.Images {
		for _, stage := range []string{model.StageDetect, model.StageRecognize, model.StageStrategy, model.StageDiagnostic} {
			checkpoint, exists := byKey[imageSpec.ImageID+"/"+stage]
			if !exists {
				return Bundle{}, nil, &apperr.Error{Code: apperr.CodeNotFound, Message: "cannot bundle incomplete pipeline", ImageID: imageSpec.ImageID, Stage: stage}
			}
			if canonical.DigestBytes(checkpoint.Product) != checkpoint.ProductDigest {
				return Bundle{}, nil, &apperr.Error{Code: apperr.CodeStaleCheckpoint, Message: "checkpoint product digest mismatch", ImageID: imageSpec.ImageID, Stage: stage, Reason: "digest_mismatch"}
			}
			bundle.Products = append(bundle.Products, BundleProduct{
				ImageID: imageSpec.ImageID, Stage: stage,
				Digest: checkpoint.ProductDigest, Product: append(json.RawMessage(nil), checkpoint.Product...),
			})
		}
	}
	bundleBytes, err := canonical.Marshal(bundle)
	if err != nil {
		return Bundle{}, nil, err
	}
	return bundle, bundleBytes, nil
}

func (p *Pipeline) VerifyIncremental(ctx context.Context) (VerificationSummary, error) {
	_, reportBytes, err := p.Report(ctx)
	if err != nil {
		return VerificationSummary{}, err
	}
	_, bundleBytes, err := p.BuildBundle(ctx)
	if err != nil {
		return VerificationSummary{}, err
	}
	tempRoot, err := os.MkdirTemp("", "ocrlab-full-*")
	if err != nil {
		return VerificationSummary{}, err
	}
	defer os.RemoveAll(tempRoot)
	if err := copyFile(p.paths.ConfigPath, filepath.Join(tempRoot, "config.json")); err != nil {
		return VerificationSummary{}, err
	}
	if err := copyTree(p.paths.DataDir, filepath.Join(tempRoot, "data")); err != nil {
		return VerificationSummary{}, err
	}
	full, err := Open(ctx, tempRoot)
	if err != nil {
		return VerificationSummary{}, err
	}
	defer full.Close()
	if _, err := full.Run(ctx, RunOptions{Rebuild: true}); err != nil {
		return VerificationSummary{}, apperr.Wrap(apperr.CodeInternal, "full rebuild during verification", err)
	}
	_, fullReportBytes, err := full.Report(ctx)
	if err != nil {
		return VerificationSummary{}, err
	}
	_, fullBundleBytes, err := full.BuildBundle(ctx)
	if err != nil {
		return VerificationSummary{}, err
	}
	summary := VerificationSummary{
		Equal: true, ReportDigest: canonical.DigestBytes(reportBytes), FullReportDigest: canonical.DigestBytes(fullReportBytes),
		BundleDigest: canonical.DigestBytes(bundleBytes), FullBundleDigest: canonical.DigestBytes(fullBundleBytes),
	}
	if first := firstBundleDifference(bundleBytes, fullBundleBytes); first != "" {
		summary.Equal = false
		summary.FirstDifference = first
		return summary, &apperr.Error{Code: apperr.CodeIncrementalMismatch, Message: "incremental artifacts differ from a clean full build", Item: first, Expected: summary.FullBundleDigest, Actual: summary.BundleDigest}
	}
	if canonical.DigestBytes(reportBytes) != canonical.DigestBytes(fullReportBytes) {
		summary.Equal = false
		summary.FirstDifference = "report"
		return summary, &apperr.Error{Code: apperr.CodeIncrementalMismatch, Message: "incremental diagnostic report differs from a clean full build", Item: "report", Expected: summary.FullReportDigest, Actual: summary.ReportDigest}
	}
	return summary, nil
}

func firstBundleDifference(leftBytes, rightBytes []byte) string {
	var left, right Bundle
	if err := json.Unmarshal(leftBytes, &left); err != nil {
		return "left_bundle_decode"
	}
	if err := json.Unmarshal(rightBytes, &right); err != nil {
		return "right_bundle_decode"
	}
	leftMap := make(map[string]BundleProduct, len(left.Products))
	rightMap := make(map[string]BundleProduct, len(right.Products))
	for _, product := range left.Products {
		leftMap[product.ImageID+"/"+product.Stage] = product
	}
	for _, product := range right.Products {
		rightMap[product.ImageID+"/"+product.Stage] = product
	}
	keys := make([]string, 0, len(leftMap)+len(rightMap))
	for key := range leftMap {
		keys = append(keys, key)
	}
	for key := range rightMap {
		if _, exists := leftMap[key]; !exists {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		leftProduct, leftExists := leftMap[key]
		rightProduct, rightExists := rightMap[key]
		if !leftExists {
			return key + ":missing_incremental"
		}
		if !rightExists {
			return key + ":missing_full"
		}
		if leftProduct.Digest != rightProduct.Digest || string(leftProduct.Product) != string(rightProduct.Product) {
			return key
		}
	}
	if string(leftBytes) != string(rightBytes) {
		return "bundle_metadata"
	}
	return ""
}

func (p *Pipeline) scopedManifest(groups []string) model.Manifest {
	if len(groups) == 0 {
		return p.manifest
	}
	filter := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		filter[group] = struct{}{}
	}
	scoped := model.Manifest{
		SchemaVersion: p.manifest.SchemaVersion, Seed: p.manifest.Seed,
		GeneratorVersion: p.manifest.GeneratorVersion,
	}
	for _, group := range p.manifest.Groups {
		if _, keep := filter[group.Name]; keep {
			scoped.Groups = append(scoped.Groups, group)
		}
	}
	for _, imageSpec := range p.manifest.Images {
		if _, keep := filter[imageSpec.Group]; keep {
			scoped.Images = append(scoped.Images, imageSpec)
		}
	}
	return scoped
}

func copyTree(source, target string) error {
	return filepath.Walk(source, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if info.IsDir() {
			return os.MkdirAll(destination, info.Mode())
		}
		return copyFile(path, destination)
	})
}
