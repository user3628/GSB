package demo

import (
	"context"
	"path/filepath"

	"regworkbench/internal/canonical"
	"regworkbench/internal/config"
	"regworkbench/internal/dataset"
	"regworkbench/internal/experiment"
)

type DatasetSummary struct {
	Key                string  `json:"key"`
	File               string  `json:"file"`
	Rows               int     `json:"rows"`
	Columns            int     `json:"columns"`
	ConfigVersion      string  `json:"config_version"`
	State              string  `json:"state"`
	CheckpointSequence int64   `json:"checkpoint_sequence"`
	SelectedLambda     float64 `json:"selected_lambda"`
	SelectedL1Ratio    float64 `json:"selected_l1_ratio"`
	SelectedMetricMean float64 `json:"selected_metric_mean"`
	SelectedMetricStd  float64 `json:"selected_metric_std"`
	CoefficientDigest  string  `json:"coefficient_digest"`
	CoefficientFileSHA string  `json:"coefficient_file_sha256"`
	ValidationFileSHA  string  `json:"validation_file_sha256"`
}

type Summary struct {
	Version  string           `json:"version"`
	Seed     int64            `json:"seed"`
	Samples  string           `json:"samples_dir"`
	Datasets []DatasetSummary `json:"datasets"`
}

func Run(out string, full bool) (*Summary, error) {
	samplesDir := filepath.Join(out, "samples")
	_, err := dataset.GenerateDirectory(samplesDir, dataset.DefaultSeed)
	if err != nil {
		return nil, err
	}
	summary := &Summary{Version: "regworkbench.demo/v1", Seed: dataset.DefaultSeed, Samples: samplesDir}
	for _, key := range []string{"small", "medium"} {
		ds, err := dataset.LoadDataset(filepath.Join(samplesDir, dataset.ManifestName), key)
		if err != nil {
			return nil, err
		}
		cfg := FastConfig(key)
		if full {
			cfg = config.Default(key)
		}
		root := filepath.Join(out, key)
		sess, err := experiment.CreateSession(root, cfg, ds, experiment.Hooks{})
		if err != nil {
			return nil, err
		}
		rep, err := sess.Run(context.Background())
		if err != nil {
			_ = sess.Close()
			return nil, err
		}
		snap := sess.Snapshot()
		coefSHA, _ := canonical.SHA256File(filepath.Join(root, "exports", "coefficients.json"))
		valSHA, _ := canonical.SHA256File(filepath.Join(root, "exports", "validation_summary.json"))
		item := DatasetSummary{
			Key: key, File: ds.Spec.File, Rows: ds.Spec.Rows, Columns: ds.Spec.Columns,
			ConfigVersion: sess.ConfigVer, State: rep.State, CheckpointSequence: rep.MonotonicSequence,
			SelectedLambda:     snap.Validation.Basis.SelectedLambda,
			SelectedL1Ratio:    snap.Validation.Basis.SelectedL1Ratio,
			SelectedMetricMean: snap.Validation.Selected.MeanMetric,
			SelectedMetricStd:  snap.Validation.Selected.StdMetric,
			CoefficientDigest:  snap.FinalModel.Digest,
			CoefficientFileSHA: coefSHA, ValidationFileSHA: valSHA,
		}
		summary.Datasets = append(summary.Datasets, item)
		_ = sess.Close()
	}
	b, err := canonical.JSON(summary)
	if err != nil {
		return nil, err
	}
	if err := canonical.AtomicWriteFile(filepath.Join(out, "demo-summary.json"), append(b, '\n'), 0o644); err != nil {
		return nil, err
	}
	return summary, nil
}
