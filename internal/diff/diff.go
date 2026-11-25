package diff

import (
	"fmt"
	"sort"

	"deployverify/internal/model"
	"deployverify/internal/spec"
)

type Item struct {
	Environment   string `json:"environment"`
	Baseline      string `json:"baseline"`
	Service       string `json:"service"`
	DiffType      string `json:"diffType"`
	ConfigKey     string `json:"configKey,omitempty"`
	Severity      string `json:"severity"`
	BaselineValue any    `json:"baselineValue"`
	ActualValue   any    `json:"actualValue"`
}

type BehindUnit struct {
	Environment     string `json:"environment"`
	Service         string `json:"service"`
	BaselineVersion string `json:"baselineVersion"`
	ActualVersion   string `json:"actualVersion"`
	MinorBehind     int    `json:"minorBehind,omitempty"`
}

type Report struct {
	Baseline       string       `json:"baseline"`
	ToleranceUnits int          `json:"toleranceUnits"`
	Items          []Item       `json:"items"`
	BehindUnits    []BehindUnit `json:"behindUnits"`
	Drift          bool         `json:"drift"`
	Code           string       `json:"code"`
}

type Options struct {
	Baseline       string
	ToleranceUnits int
}

func Compare(snapshot *model.Snapshot, options Options) (*Report, error) {
	if snapshot == nil {
		return nil, model.NewError(model.CodeNotFound, "no committed snapshot", nil)
	}
	envs := map[string]model.Environment{}
	for _, env := range snapshot.Environments {
		envs[env.Name] = env
	}
	baselineName := options.Baseline
	if baselineName == "" {
		ordered := spec.SortedEnvironments(snapshot.Environments)
		if len(ordered) == 0 {
			return nil, model.NewError(model.CodeSpecInvalid, "no environments", nil)
		}
		baselineName = ordered[0].Name
	}
	baseline, ok := envs[baselineName]
	if !ok {
		return nil, model.NewError(model.CodeNotFound, fmt.Sprintf("baseline environment %q not found", baselineName), map[string]any{"environment": baselineName})
	}
	report := &Report{Baseline: baselineName, ToleranceUnits: options.ToleranceUnits, Items: []Item{}, BehindUnits: []BehindUnit{}}
	baselineUnits := map[string]model.Unit{}
	serviceSet := map[string]bool{}
	for _, unit := range baseline.Units {
		baselineUnits[unit.Service] = unit
		serviceSet[unit.Service] = true
	}
	for envName, env := range envs {
		if envName == baselineName {
			continue
		}
		actualUnits := map[string]model.Unit{}
		for _, unit := range env.Units {
			actualUnits[unit.Service] = unit
			serviceSet[unit.Service] = true
		}
		for service := range serviceSet {
			baseUnit, inBase := baselineUnits[service]
			actualUnit, inActual := actualUnits[service]
			if !inBase {
				report.Items = append(report.Items, Item{
					Environment: envName, Baseline: baselineName, Service: service,
					DiffType: "missing_in_baseline", Severity: "medium",
					BaselineValue: nil, ActualValue: "present",
				})
				continue
			}
			if !inActual {
				report.Items = append(report.Items, Item{
					Environment: envName, Baseline: baselineName, Service: service,
					DiffType: "missing_in_environment", Severity: "critical",
					BaselineValue: "present", ActualValue: nil,
				})
				continue
			}
			if baseUnit.ImageDigest != actualUnit.ImageDigest {
				report.Items = append(report.Items, Item{
					Environment: envName, Baseline: baselineName, Service: service,
					DiffType: "image_digest", Severity: "critical",
					BaselineValue: baseUnit.ImageDigest, ActualValue: actualUnit.ImageDigest,
				})
			}
			if baseUnit.DesiredVersion != actualUnit.DesiredVersion {
				severity := "high"
				comparison := model.CompareVersion(actualUnit.DesiredVersion, baseUnit.DesiredVersion)
				if comparison < 0 {
					severity = "high"
				}
				report.Items = append(report.Items, Item{
					Environment: envName, Baseline: baselineName, Service: service,
					DiffType: "desired_version", Severity: severity,
					BaselineValue: baseUnit.DesiredVersion, ActualValue: actualUnit.DesiredVersion,
				})
				if comparison < 0 {
					behind := BehindUnit{
						Environment: envName, Service: service,
						BaselineVersion: baseUnit.DesiredVersion, ActualVersion: actualUnit.DesiredVersion,
					}
					bv, berr := model.ParseSemVer(baseUnit.DesiredVersion)
					av, aerr := model.ParseSemVer(actualUnit.DesiredVersion)
					if berr == nil && aerr == nil {
						behind.MinorBehind = (bv.Major-av.Major)*100 + (bv.Minor - av.Minor)
						if behind.MinorBehind < 0 {
							behind.MinorBehind = 0
						}
					}
					report.BehindUnits = append(report.BehindUnits, behind)
				}
			}
			keys := map[string]bool{}
			for key := range baseUnit.Config {
				keys[key] = true
			}
			for key := range actualUnit.Config {
				keys[key] = true
			}
			for key := range keys {
				bv := baseUnit.Config[key]
				av := actualUnit.Config[key]
				if bv == av {
					continue
				}
				severity := "medium"
				if key == "LOG_LEVEL" {
					severity = "low"
				}
				if key == "FEATURE_AUTH" || key == "TLS_MODE" {
					severity = "high"
				}
				report.Items = append(report.Items, Item{
					Environment: envName, Baseline: baselineName, Service: service,
					DiffType: "config_key", ConfigKey: key, Severity: severity,
					BaselineValue: valueOrMissing(baseUnit.Config, key),
					ActualValue:   valueOrMissing(actualUnit.Config, key),
				})
			}
		}
	}
	sortItems(report.Items)
	sort.SliceStable(report.BehindUnits, func(i, j int) bool {
		if report.BehindUnits[i].Environment != report.BehindUnits[j].Environment {
			return report.BehindUnits[i].Environment < report.BehindUnits[j].Environment
		}
		return report.BehindUnits[i].Service < report.BehindUnits[j].Service
	})
	if options.ToleranceUnits < 0 {
		report.ToleranceUnits = 0
	}
	if len(report.BehindUnits) > report.ToleranceUnits {
		report.Drift = true
		report.Code = model.CodeEnvDrift
	} else {
		report.Code = "OK"
	}
	return report, nil
}

func valueOrMissing(config map[string]string, key string) any {
	value, ok := config[key]
	if !ok {
		return nil
	}
	return value
}

func severityRank(severity string) int {
	switch severity {
	case "critical":
		return 0
	case "high":
		return 1
	case "medium":
		return 2
	default:
		return 3
	}
}

func sortItems(items []Item) {
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if severityRank(a.Severity) != severityRank(b.Severity) {
			return severityRank(a.Severity) < severityRank(b.Severity)
		}
		if a.Environment != b.Environment {
			return a.Environment < b.Environment
		}
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		if a.DiffType != b.DiffType {
			return a.DiffType < b.DiffType
		}
		return a.ConfigKey < b.ConfigKey
	})
}
