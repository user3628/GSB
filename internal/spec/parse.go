package spec

import (
	"encoding/json"
	"fmt"
	"sort"

	"deployverify/internal/model"
)

func ParseSnapshot(data []byte) (*model.Snapshot, error) {
	var snapshot model.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, model.NewError(model.CodeSpecInvalid, "snapshot JSON is invalid", []model.Issue{{Path: "$", Message: err.Error()}})
	}
	if err := model.VerifyChecksum(snapshot, snapshot.Checksum); err != nil {
		return nil, err
	}
	if err := ValidateSnapshot(&snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func ParseIncrement(data []byte, current *model.Snapshot) (*model.Increment, error) {
	var increment model.Increment
	if err := json.Unmarshal(data, &increment); err != nil {
		return nil, model.NewError(model.CodeSpecInvalid, "increment JSON is invalid", []model.Issue{{Path: "$", Message: err.Error()}})
	}
	if increment.Checksum != "" {
		if err := model.VerifyChecksum(increment, increment.Checksum); err != nil {
			return nil, err
		}
	}
	if increment.BaseVersion < 0 {
		return nil, invalid([]model.Issue{{Path: "baseVersion", Message: "must be non-negative"}})
	}
	candidate, err := ApplyIncrement(current, &increment)
	if err != nil {
		return nil, err
	}
	if err := ValidateSnapshot(candidate); err != nil {
		return nil, err
	}
	return &increment, nil
}

func ValidateSnapshot(snapshot *model.Snapshot) error {
	issues := make([]model.Issue, 0)
	if snapshot == nil {
		return invalid([]model.Issue{{Path: "$", Message: "snapshot is required"}})
	}
	if snapshot.Version <= 0 {
		issues = append(issues, model.Issue{Path: "version", Message: "must be positive"})
	}
	if snapshot.GeneratedAt.IsZero() {
		issues = append(issues, model.Issue{Path: "generatedAt", Message: "must be an RFC3339 timestamp"})
	}
	if len(snapshot.Environments) == 0 {
		issues = append(issues, model.Issue{Path: "environments", Message: "at least one environment is required"})
	}
	envNames := map[string]bool{}
	for ei := range snapshot.Environments {
		env := &snapshot.Environments[ei]
		ep := fmt.Sprintf("environments[%d]", ei)
		if env.Name == "" {
			issues = append(issues, model.Issue{Path: ep + ".name", Message: "environment name is required"})
		} else if envNames[env.Name] {
			issues = append(issues, model.Issue{Path: ep + ".name", Message: "environment name must be unique"})
		} else {
			envNames[env.Name] = true
		}
		if env.Priority <= 0 {
			issues = append(issues, model.Issue{Path: ep + ".priority", Message: "priority must be positive; lower means earlier"})
		}
		services := map[string]bool{}
		for ui := range env.Units {
			unit := &env.Units[ui]
			up := fmt.Sprintf("%s.units[%d](%s)", ep, ui, unit.Service)
			if !model.ValidServiceName(unit.Service) {
				issues = append(issues, model.Issue{Path: up + ".service", Message: "service must match ^[a-z][a-z0-9-]*$"})
			} else if services[unit.Service] {
				issues = append(issues, model.Issue{Path: up + ".service", Message: "service must be unique within environment"})
			} else {
				services[unit.Service] = true
			}
			if !model.ValidImageDigest(unit.ImageDigest) {
				issues = append(issues, model.Issue{Path: up + ".imageDigest", Message: "imageDigest must be sha256:<64 lowercase hex>"})
			}
			if unit.DesiredVersion == "" {
				issues = append(issues, model.Issue{Path: up + ".desiredVersion", Message: "desiredVersion is required"})
			} else if _, err := model.ParseSemVer(unit.DesiredVersion); err != nil {
				issues = append(issues, model.Issue{Path: up + ".desiredVersion", Message: "desiredVersion must be semantic version x.y.z"})
			}
			for key, value := range unit.Config {
				if key == "" {
					issues = append(issues, model.Issue{Path: up + ".config", Message: "config keys must not be empty"})
				}
				_ = value
			}
			if unit.Probe != nil && unit.Probe.TimeoutMS < 0 {
				issues = append(issues, model.Issue{Path: up + ".probe.timeoutMs", Message: "timeoutMs must be non-negative"})
			}
		}
		for ui := range env.Units {
			unit := &env.Units[ui]
			for di, dep := range unit.DependsOn {
				if !services[dep] {
					issues = append(issues, model.Issue{
						Path:    fmt.Sprintf("%s.units[%d].dependsOn[%d]", ep, ui, di),
						Message: fmt.Sprintf("dependency %q does not exist in environment %q", dep, env.Name),
					})
				}
				if dep == unit.Service {
					issues = append(issues, model.Issue{Path: fmt.Sprintf("%s.units[%d].dependsOn[%d]", ep, ui, di), Message: "self dependency is not allowed"})
				}
			}
		}
	}
	if len(issues) > 0 {
		return invalid(issues)
	}
	return nil
}

func ApplyIncrement(current *model.Snapshot, increment *model.Increment) (*model.Snapshot, error) {
	candidate := &model.Snapshot{Version: increment.BaseVersion + 1, GeneratedAt: increment.GeneratedAt}
	if current != nil {
		candidate.Environments = cloneEnvironments(current.Environments)
	}
	envIndex := map[string]int{}
	for i := range candidate.Environments {
		envIndex[candidate.Environments[i].Name] = i
	}
	for ci, change := range increment.Changes {
		if change.Environment == "" {
			return nil, invalid([]model.Issue{{Path: fmt.Sprintf("changes[%d].environment", ci), Message: "environment is required"}})
		}
		ei, ok := envIndex[change.Environment]
		if !ok {
			return nil, invalid([]model.Issue{{Path: fmt.Sprintf("changes[%d].environment", ci), Message: "environment does not exist in current snapshot"}})
		}
		if !model.ValidServiceName(change.Service) {
			return nil, invalid([]model.Issue{{Path: fmt.Sprintf("changes[%d].service", ci), Message: "service name is invalid"}})
		}
		ui := -1
		for i := range candidate.Environments[ei].Units {
			if candidate.Environments[ei].Units[i].Service == change.Service {
				ui = i
				break
			}
		}
		unit := change.Unit
		if unit.Config == nil {
			unit.Config = map[string]string{}
		}
		if ui >= 0 {
			candidate.Environments[ei].Units[ui] = unit
		} else {
			candidate.Environments[ei].Units = append(candidate.Environments[ei].Units, unit)
		}
	}
	return candidate, nil
}

func cloneEnvironments(in []model.Environment) []model.Environment {
	out := make([]model.Environment, len(in))
	for i, env := range in {
		out[i] = model.Environment{Name: env.Name, Priority: env.Priority, Units: make([]model.Unit, len(env.Units))}
		for j, unit := range env.Units {
			cp := unit
			if unit.Config != nil {
				cp.Config = make(map[string]string, len(unit.Config))
				for k, v := range unit.Config {
					cp.Config[k] = v
				}
			} else {
				cp.Config = map[string]string{}
			}
			cp.DependsOn = append([]string(nil), unit.DependsOn...)
			if unit.Probe != nil {
				probe := *unit.Probe
				cp.Probe = &probe
			}
			out[i].Units[j] = cp
		}
	}
	return out
}

func SortedEnvironments(envs []model.Environment) []model.Environment {
	out := append([]model.Environment(nil), envs...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func invalid(issues []model.Issue) error {
	return model.NewError(model.CodeSpecInvalid, "specification validation failed", map[string]any{"issues": issues})
}
