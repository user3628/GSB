package configx

import "featureplatform/internal/textx"

func (c Config) ScopedStageFingerprint(task, stage string) string {
	fps := c.Fingerprints()
	switch stage {
	case "normalize":
		return fingerprint("scoped-normalize/v1:", struct {
			NormalizerVersion string `json:"normalizer_version"`
		}{textx.NormalizerVersion})
	case "label":
		return fingerprint("scoped-label/v1:", struct {
			LabelTableVersion string            `json:"label_table_version"`
			Task              string            `json:"task"`
			LabelMapping      map[string]string `json:"label_mapping"`
		}{c.LabelTableVersion, task, c.LabelMappings[task]})
	case "feature_map":
		return fingerprint("scoped-feature/v1:", struct {
			SchemaVersion string        `json:"schema_version"`
			Task          string        `json:"task"`
			HashDim       int           `json:"hash_dim"`
			TaskVersion   int           `json:"task_version"`
			VersionPolicy VersionPolicy `json:"version_policy"`
			TaskMapping   TaskMapping   `json:"task_mapping"`
		}{"unified-hash-v1", task, c.HashDim, c.TaskVersions[task], c.VersionPolicy, c.TaskMappings[task]})
	case "coverage":
		return fingerprint("scoped-coverage/v1:", struct {
			Task      string  `json:"task"`
			Threshold float64 `json:"threshold"`
		}{task, c.ThresholdFor(task)})
	case "config":
		return fps.ConfigFingerprint
	default:
		return ""
	}
}
