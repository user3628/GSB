package derive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"deployverify/internal/model"
	"deployverify/internal/spec"
)

type Change struct {
	Ref    model.Ref `json:"ref"`
	Fields []string  `json:"fields,omitempty"`
}

type CascadeReport struct {
	Changed              []Change    `json:"changed"`
	Affected             []model.Ref `json:"affected"`
	CycleEnvironments    []string    `json:"cycleEnvironments,omitempty"`
	InvalidatedArtifacts []string    `json:"invalidatedArtifacts"`
	RebuiltArtifacts     []string    `json:"rebuiltArtifacts"`
}

type PlanItem struct {
	Version        int64            `json:"version"`
	Environment    string           `json:"environment"`
	Service        string           `json:"service"`
	Layer          int              `json:"layer"`
	Action         string           `json:"action"`
	Status         string           `json:"status"`
	ImageDigest    string           `json:"imageDigest"`
	DesiredVersion string           `json:"desiredVersion"`
	ConfigHash     string           `json:"configHash"`
	DependsOn      []string         `json:"dependsOn"`
	Probe          *model.ProbeSpec `json:"probe,omitempty"`
}

type Layer struct {
	Index    int      `json:"index"`
	Services []string `json:"services"`
}

type Topology struct {
	Environment string   `json:"environment"`
	Status      string   `json:"status"`
	Layers      []Layer  `json:"layers"`
	Cycle       []string `json:"cycle,omitempty"`
	Blocked     []string `json:"blocked,omitempty"`
}

type EnvironmentScore struct {
	Environment    string            `json:"environment"`
	Priority       int               `json:"priority"`
	ImageDigest    string            `json:"imageDigest"`
	DesiredVersion string            `json:"desiredVersion"`
	Config         map[string]string `json:"config"`
}

type Score struct {
	Service        string                      `json:"service"`
	Score          float64                     `json:"score"`
	Consistent     bool                        `json:"consistent"`
	PresentIn      []string                    `json:"presentIn"`
	MismatchFields []string                    `json:"mismatchFields"`
	Environments   map[string]EnvironmentScore `json:"environments"`
}

type ArtifactRef struct {
	Version      int64  `json:"version"`
	ArtifactType string `json:"artifactType"`
	ArtifactKey  string `json:"artifactKey"`
	Environment  string `json:"environment,omitempty"`
	Service      string `json:"service,omitempty"`
	UnitEnv      string `json:"unitEnv,omitempty"`
	UnitService  string `json:"unitService,omitempty"`
	ConfigKey    string `json:"configKey,omitempty"`
}

type Bundle struct {
	Version    int64          `json:"version"`
	Plans      []PlanItem     `json:"plans"`
	Topologies []Topology     `json:"topologies"`
	Scores     []Score        `json:"scores"`
	Refs       []ArtifactRef  `json:"refs"`
	Cascade    *CascadeReport `json:"cascade,omitempty"`
}

func ConfigHash(config map[string]string) string {
	if len(config) == 0 {
		return ""
	}
	data, _ := json.Marshal(config)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func BuildBundle(current, previous *model.Snapshot, changes []Change) *Bundle {
	bundle := &Bundle{Version: current.Version}
	orderedEnvs := spec.SortedEnvironments(current.Environments)
	prevIndex := map[model.Ref]model.Unit{}
	if previous != nil {
		for _, env := range previous.Environments {
			for _, unit := range env.Units {
				prevIndex[model.Ref{Environment: env.Name, Service: unit.Service}] = unit
			}
		}
	}
	for _, env := range orderedEnvs {
		topology := BuildTopology(env)
		bundle.Topologies = append(bundle.Topologies, topology)
		layerOf := map[string]int{}
		for _, layer := range topology.Layers {
			for _, service := range layer.Services {
				layerOf[service] = layer.Index
			}
		}
		units := append([]model.Unit(nil), env.Units...)
		sort.SliceStable(units, func(i, j int) bool {
			li, oki := layerOf[units[i].Service]
			lj, okj := layerOf[units[j].Service]
			if oki != okj {
				return oki
			}
			if li != lj {
				return li < lj
			}
			return units[i].Service < units[j].Service
		})
		for _, unit := range units {
			action := "create"
			if old, ok := prevIndex[model.Ref{Environment: env.Name, Service: unit.Service}]; ok {
				if UnitsEqual(old, unit) {
					action = "unchanged"
				} else {
					action = "update"
				}
			}
			layer, ok := layerOf[unit.Service]
			if !ok {
				layer = -1
			}
			bundle.Plans = append(bundle.Plans, PlanItem{
				Version: current.Version, Environment: env.Name, Service: unit.Service,
				Layer: layer, Action: action, Status: "pending", ImageDigest: unit.ImageDigest,
				DesiredVersion: unit.DesiredVersion, ConfigHash: ConfigHash(unit.Config),
				DependsOn: append([]string(nil), unit.DependsOn...), Probe: unit.Probe,
			})
		}
	}
	bundle.Scores = BuildScores(current)
	bundle.Refs = BuildRefs(bundle)
	if len(changes) > 0 {
		bundle.Cascade = CascadeArtifacts(current, changes, bundle)
	}
	return bundle
}

func UnitsEqual(a, b model.Unit) bool {
	if a.ImageDigest != b.ImageDigest || a.DesiredVersion != b.DesiredVersion {
		return false
	}
	if ConfigHash(a.Config) != ConfigHash(b.Config) {
		return false
	}
	if len(a.DependsOn) != len(b.DependsOn) {
		return false
	}
	ad := append([]string(nil), a.DependsOn...)
	bd := append([]string(nil), b.DependsOn...)
	sort.Strings(ad)
	sort.Strings(bd)
	for i := range ad {
		if ad[i] != bd[i] {
			return false
		}
	}
	return true
}

func BuildTopology(env model.Environment) Topology {
	units := map[string]model.Unit{}
	dependents := map[string][]string{}
	remaining := map[string]int{}
	for _, unit := range env.Units {
		units[unit.Service] = unit
		remaining[unit.Service] = len(unit.DependsOn)
		for _, dep := range unit.DependsOn {
			dependents[dep] = append(dependents[dep], unit.Service)
		}
	}
	for dep := range dependents {
		sort.Strings(dependents[dep])
	}
	result := Topology{Environment: env.Name, Status: "complete"}
	ready := make([]string, 0)
	for service, count := range remaining {
		if count == 0 {
			ready = append(ready, service)
		}
	}
	sort.Strings(ready)
	layerIndex := 0
	layered := map[string]bool{}
	for len(ready) > 0 {
		result.Layers = append(result.Layers, Layer{Index: layerIndex, Services: append([]string(nil), ready...)})
		for _, service := range ready {
			layered[service] = true
			for _, dependent := range dependents[service] {
				remaining[dependent]--
				if remaining[dependent] == 0 {
					// Defer collection until the whole wave is processed.
				}
			}
		}
		nextSet := map[string]bool{}
		for _, service := range ready {
			for _, dependent := range dependents[service] {
				if remaining[dependent] == 0 && !layered[dependent] {
					nextSet[dependent] = true
				}
			}
		}
		next := make([]string, 0, len(nextSet))
		for service := range nextSet {
			next = append(next, service)
		}
		sort.Strings(next)
		ready = next
		layerIndex++
	}
	blocked := make([]string, 0)
	for service := range units {
		if !layered[service] {
			blocked = append(blocked, service)
		}
	}
	sort.Strings(blocked)
	if len(blocked) > 0 {
		result.Status = "degraded_dependency_cycle"
		result.Blocked = blocked
		result.Cycle = FindCycle(env)
	}
	return result
}

func FindCycle(env model.Environment) []string {
	adj := map[string][]string{}
	for _, unit := range env.Units {
		deps := append([]string(nil), unit.DependsOn...)
		sort.Strings(deps)
		adj[unit.Service] = deps
	}
	starts := make([]string, 0, len(adj))
	for service := range adj {
		starts = append(starts, service)
	}
	sort.Strings(starts)
	color := map[string]int{}
	stack := []string{}
	var cycle []string
	var dfs func(string) bool
	dfs = func(service string) bool {
		color[service] = 1
		stack = append(stack, service)
		for _, dep := range adj[service] {
			if _, ok := adj[dep]; !ok {
				continue
			}
			if color[dep] == 0 {
				if dfs(dep) {
					return true
				}
			} else if color[dep] == 1 {
				start := 0
				for i, item := range stack {
					if item == dep {
						start = i
						break
					}
				}
				cycle = append([]string(nil), stack[start:]...)
				return true
			}
		}
		stack = stack[:len(stack)-1]
		color[service] = 2
		return false
	}
	for _, service := range starts {
		if color[service] == 0 && dfs(service) {
			return cycle
		}
	}
	return nil
}

func BuildScores(snapshot *model.Snapshot) []Score {
	byService := map[string]map[string]EnvironmentScore{}
	envOrder := map[string]int{}
	for _, env := range snapshot.Environments {
		envOrder[env.Name] = env.Priority
		for _, unit := range env.Units {
			if byService[unit.Service] == nil {
				byService[unit.Service] = map[string]EnvironmentScore{}
			}
			config := unit.Config
			if config == nil {
				config = map[string]string{}
			}
			byService[unit.Service][env.Name] = EnvironmentScore{
				Environment: env.Name, Priority: env.Priority, ImageDigest: unit.ImageDigest,
				DesiredVersion: unit.DesiredVersion, Config: config,
			}
		}
	}
	services := make([]string, 0, len(byService))
	for service := range byService {
		services = append(services, service)
	}
	sort.Strings(services)
	scores := make([]Score, 0, len(services))
	for _, service := range services {
		envs := byService[service]
		names := make([]string, 0, len(envs))
		for name := range envs {
			names = append(names, name)
		}
		sort.Slice(names, func(i, j int) bool {
			if envOrder[names[i]] != envOrder[names[j]] {
				return envOrder[names[i]] < envOrder[names[j]]
			}
			return names[i] < names[j]
		})
		matched, total := 0, 0
		mismatchSet := map[string]bool{}
		base := envs[names[0]]
		for _, name := range names[1:] {
			candidate := envs[name]
			total += 2
			if base.ImageDigest == candidate.ImageDigest {
				matched++
			} else {
				mismatchSet["imageDigest"] = true
			}
			if base.DesiredVersion == candidate.DesiredVersion {
				matched++
			} else {
				mismatchSet["desiredVersion"] = true
			}
			keys := map[string]bool{}
			for key := range base.Config {
				keys[key] = true
			}
			for key := range candidate.Config {
				keys[key] = true
			}
			for key := range keys {
				total++
				if base.Config[key] == candidate.Config[key] {
					matched++
				} else {
					mismatchSet["config."+key] = true
				}
			}
		}
		present := append([]string(nil), names...)
		mismatch := make([]string, 0, len(mismatchSet))
		for field := range mismatchSet {
			mismatch = append(mismatch, field)
		}
		sort.Strings(mismatch)
		scoreValue := 100.0
		if total > 0 {
			scoreValue = float64(matched) / float64(total) * 100
		}
		scores = append(scores, Score{
			Service: service, Score: round2(scoreValue), Consistent: len(mismatch) == 0,
			PresentIn: present, MismatchFields: mismatch, Environments: envs,
		})
	}
	return scores
}

func round2(value float64) float64 {
	return float64(int(value*100+0.5)) / 100
}

func BuildRefs(bundle *Bundle) []ArtifactRef {
	refs := make([]ArtifactRef, 0)
	for _, plan := range bundle.Plans {
		key := fmt.Sprintf("plan:%s:%s", plan.Environment, plan.Service)
		refs = append(refs, ArtifactRef{Version: bundle.Version, ArtifactType: "plan", ArtifactKey: key, Environment: plan.Environment, Service: plan.Service, UnitEnv: plan.Environment, UnitService: plan.Service})
		// Config keys are recovered from the current snapshot by the store? Plan only has hash,
		// so config-specific refs are added by BuildRefsFromSnapshot.
	}
	for _, topology := range bundle.Topologies {
		key := "topology:" + topology.Environment
		for _, layer := range topology.Layers {
			for _, service := range layer.Services {
				refs = append(refs, ArtifactRef{Version: bundle.Version, ArtifactType: "topology", ArtifactKey: key, Environment: topology.Environment, Service: service, UnitEnv: topology.Environment, UnitService: service})
			}
		}
		for _, service := range topology.Blocked {
			refs = append(refs, ArtifactRef{Version: bundle.Version, ArtifactType: "topology", ArtifactKey: key, Environment: topology.Environment, Service: service, UnitEnv: topology.Environment, UnitService: service})
		}
	}
	for _, score := range bundle.Scores {
		key := "score:" + score.Service
		for _, envName := range score.PresentIn {
			envScore := score.Environments[envName]
			refs = append(refs, ArtifactRef{Version: bundle.Version, ArtifactType: "score", ArtifactKey: key, Environment: envName, Service: score.Service, UnitEnv: envName, UnitService: score.Service})
			configKeys := make([]string, 0, len(envScore.Config))
			for configKey := range envScore.Config {
				configKeys = append(configKeys, configKey)
			}
			sort.Strings(configKeys)
			for _, configKey := range configKeys {
				refs = append(refs, ArtifactRef{Version: bundle.Version, ArtifactType: "score", ArtifactKey: key, Environment: envName, Service: score.Service, UnitEnv: envName, UnitService: score.Service, ConfigKey: configKey})
			}
		}
	}
	return refs
}

// BuildConfigRefs adds config-key references to plan and topology artifacts. Score config refs are
// generated with scores because their artifact key is service-scoped.
func BuildConfigRefs(version int64, snapshot *model.Snapshot) []ArtifactRef {
	refs := make([]ArtifactRef, 0)
	for _, env := range snapshot.Environments {
		for _, unit := range env.Units {
			keys := make([]string, 0, len(unit.Config))
			for key := range unit.Config {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				refs = append(refs, ArtifactRef{
					Version: version, ArtifactType: "plan",
					ArtifactKey: fmt.Sprintf("plan:%s:%s", env.Name, unit.Service),
					Environment: env.Name, Service: unit.Service,
					UnitEnv: env.Name, UnitService: unit.Service, ConfigKey: key,
				})
				refs = append(refs, ArtifactRef{
					Version: version, ArtifactType: "topology",
					ArtifactKey: "topology:" + env.Name,
					Environment: env.Name, Service: unit.Service,
					UnitEnv: env.Name, UnitService: unit.Service, ConfigKey: key,
				})
			}
		}
	}
	return refs
}

func CascadeArtifacts(snapshot *model.Snapshot, changes []Change, bundle *Bundle) *CascadeReport {
	affected := CascadeUnits(snapshot, changes)
	affectedSet := map[string]model.Ref{}
	for _, ref := range affected {
		affectedSet[ref.Environment+"/"+ref.Service] = ref
	}
	keySet := map[string]bool{}
	cycleEnvs := map[string]bool{}
	for _, ref := range affected {
		keySet[fmt.Sprintf("plan:%s:%s", ref.Environment, ref.Service)] = true
		keySet["topology:"+ref.Environment] = true
		keySet["score:"+ref.Service] = true
	}
	for _, topology := range bundle.Topologies {
		if topology.Status != "complete" && keySet["topology:"+topology.Environment] {
			cycleEnvs[topology.Environment] = true
		}
	}
	invalidated := sortedKeys(keySet)
	rebuiltSet := map[string]bool{}
	for _, plan := range bundle.Plans {
		rebuiltSet[fmt.Sprintf("plan:%s:%s", plan.Environment, plan.Service)] = true
	}
	for _, topology := range bundle.Topologies {
		rebuiltSet["topology:"+topology.Environment] = true
	}
	for _, score := range bundle.Scores {
		rebuiltSet["score:"+score.Service] = true
	}
	return &CascadeReport{
		Changed: changes, Affected: affected,
		CycleEnvironments:    sortedStringSet(cycleEnvs),
		InvalidatedArtifacts: invalidated, RebuiltArtifacts: sortedKeys(rebuiltSet),
	}
}

func CascadeUnits(snapshot *model.Snapshot, changes []Change) []model.Ref {
	byEnv := map[string]map[string]model.Unit{}
	reverse := map[string]map[string][]string{}
	for _, env := range snapshot.Environments {
		byEnv[env.Name] = map[string]model.Unit{}
		reverse[env.Name] = map[string][]string{}
		for _, unit := range env.Units {
			byEnv[env.Name][unit.Service] = unit
			for _, dep := range unit.DependsOn {
				reverse[env.Name][dep] = append(reverse[env.Name][dep], unit.Service)
			}
		}
	}
	affectedSet := map[string]model.Ref{}
	for _, change := range changes {
		envServices, ok := reverse[change.Ref.Environment]
		if !ok {
			continue
		}
		_ = envServices
		queue := []string{change.Ref.Service}
		for len(queue) > 0 {
			service := queue[0]
			queue = queue[1:]
			ref := model.Ref{Environment: change.Ref.Environment, Service: service}
			key := ref.String()
			if affectedSet[key].Service != "" {
				continue
			}
			if _, exists := byEnv[change.Ref.Environment][service]; !exists && service != change.Ref.Service {
				continue
			}
			affectedSet[key] = ref
			for _, dependent := range reverse[change.Ref.Environment][service] {
				depRef := model.Ref{Environment: change.Ref.Environment, Service: dependent}
				if _, seen := affectedSet[depRef.String()]; !seen {
					queue = append(queue, dependent)
				}
			}
		}
	}
	affected := make([]model.Ref, 0, len(affectedSet))
	for _, ref := range affectedSet {
		affected = append(affected, ref)
	}
	sort.Slice(affected, func(i, j int) bool {
		if affected[i].Environment != affected[j].Environment {
			return affected[i].Environment < affected[j].Environment
		}
		return affected[i].Service < affected[j].Service
	})
	return affected
}

func sortedKeys(input map[string]bool) []string {
	out := make([]string, 0, len(input))
	for key := range input {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func sortedStringSet(input map[string]bool) []string { return sortedKeys(input) }

// ChangedFields compares old and new units and returns fields relevant to cascade behavior.
func ChangedFields(old, next model.Unit) []string {
	fields := make([]string, 0)
	if old.ImageDigest != next.ImageDigest {
		fields = append(fields, "imageDigest")
	}
	if ConfigHash(old.Config) != ConfigHash(next.Config) {
		fields = append(fields, "config")
	}
	if !equalStringSet(old.DependsOn, next.DependsOn) {
		fields = append(fields, "dependsOn")
	}
	if old.DesiredVersion != next.DesiredVersion {
		fields = append(fields, "desiredVersion")
	}
	sort.Strings(fields)
	return fields
}

func equalStringSet(a, b []string) bool {
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	if len(aa) != len(bb) {
		return false
	}
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
