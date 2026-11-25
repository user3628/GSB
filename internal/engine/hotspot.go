package engine

import (
	"fmt"
	"math"
	"sort"
	"time"

	"profilehotspot/internal/model"
)

const (
	ModeExact       = "exact"
	ModeApproximate = "approximate"
	fixedScale      = int64(1_000_000_000_000)
)

type ConfidenceInterval struct {
	Level  float64 `json:"level"`
	Lower  float64 `json:"lower"`
	Upper  float64 `json:"upper"`
	Method string  `json:"method"`
}

type FrameHotspot struct {
	FrameID               string             `json:"frame_id"`
	FrameName             string             `json:"frame_name"`
	HotspotRank           int                `json:"hotspot_rank"`
	TieGroup              int                `json:"tie_group"`
	FixedEstimate         int64              `json:"fixed_estimate"`
	EstimatedShare        float64            `json:"estimated_share"`
	ExactShare            float64            `json:"exact_share"`
	AbsoluteError         float64            `json:"absolute_error"`
	BiasUpperBound        float64            `json:"bias_upper_bound"`
	WithinBiasBound       bool               `json:"within_bias_bound"`
	ConfidenceInterval    ConfidenceInterval `json:"confidence_interval"`
	ExactCount            int64              `json:"exact_count"`
	ExactWeightedCount    float64            `json:"exact_weighted_count"`
	SelectedCount         int64              `json:"selected_count"`
	SelectedWeightedCount float64            `json:"selected_weighted_count"`
}

type ChannelSampling struct {
	Channel        string  `json:"channel"`
	TargetPermille int     `json:"target_permille"`
	TargetRatio    float64 `json:"target_ratio"`
	BiasCorrection float64 `json:"bias_correction"`
}

type StratumSummary struct {
	LayerKey          string   `json:"layer_key"`
	DepthPrefixes     []int    `json:"depth_prefixes"`
	PopulationSamples int64    `json:"population_samples"`
	SelectedSamples   int64    `json:"selected_samples"`
	Channels          []string `json:"channels"`
	ReservoirIDs      []string `json:"reservoir_ids"`
}

type ReservoirBasis struct {
	ReservoirID             string  `json:"reservoir_id"`
	Channel                 string  `json:"channel"`
	LayerKey                string  `json:"layer_key"`
	Capacity                int     `json:"capacity"`
	SeenCount               int64   `json:"seen_count"`
	SelectedCount           int     `json:"selected_count"`
	ReplacedTotal           int64   `json:"replaced_total"`
	RejectedTotal           int64   `json:"rejected_total"`
	EffectiveBiasCorrection float64 `json:"effective_bias_correction"`
	ChannelTargetPermille   int     `json:"channel_target_permille"`
	ReplacementRule         string  `json:"replacement_rule"`
}

type PathDecision struct {
	Mode              string `json:"mode"`
	Reason            string `json:"reason"`
	PopulationSamples int64  `json:"population_samples"`
	ExactLimit        int    `json:"exact_limit"`
	Statistic         string `json:"statistic"`
	UsedReservoir     bool   `json:"used_reservoir"`
}

type ApproximationBasis struct {
	Rule                   string   `json:"rule"`
	PopulationSamples      int64    `json:"population_samples"`
	ExactLimit             int      `json:"exact_limit"`
	SelectedSamples        int      `json:"selected_samples"`
	SelectedWeightFraction float64  `json:"selected_weight_fraction"`
	StratificationKeys     []string `json:"stratification_keys"`
	DepthPrefixes          []int    `json:"depth_prefixes"`
	Reason                 string   `json:"reason"`
}

type HotspotConclusion struct {
	TopTieGroup       int      `json:"top_tie_group"`
	LeaderFrameID     string   `json:"leader_frame_id"`
	TopTieFrameIDs    []string `json:"top_tie_frame_ids"`
	TopTieSize        int      `json:"top_tie_size"`
	RelativeTolerance float64  `json:"relative_tolerance"`
	TolerancePPM      int64    `json:"tolerance_ppm"`
}

type ServiceHotspot struct {
	Service                 string              `json:"service"`
	Path                    PathDecision        `json:"path"`
	PopulationSamples       int64               `json:"population_samples"`
	DuplicateObservations   int64               `json:"duplicate_observations"`
	TotalObservations       int64               `json:"total_observations"`
	ReservoirCapacity       int                 `json:"reservoir_capacity"`
	SelectedSamples         int                 `json:"selected_samples"`
	SelectedWeightedTotal   float64             `json:"selected_weighted_total"`
	PopulationWeightedTotal float64             `json:"population_weighted_total"`
	SelectedWeightFraction  float64             `json:"selected_weight_fraction"`
	BiasUpperBound          float64             `json:"bias_upper_bound"`
	BiasBoundFormula        string              `json:"bias_bound_formula"`
	ConfidenceMethod        string              `json:"confidence_method"`
	Channels                []ChannelSampling   `json:"channels"`
	Strata                  []StratumSummary    `json:"strata"`
	Reservoirs              []ReservoirBasis    `json:"reservoirs"`
	ApproximationBasis      *ApproximationBasis `json:"approximation_basis,omitempty"`
	Frames                  []FrameHotspot      `json:"frames"`
	Conclusion              HotspotConclusion   `json:"conclusion"`
}

type ModeSummary struct {
	ExactServices       int `json:"exact_services"`
	ApproximateServices int `json:"approximate_services"`
}

type RankingPolicy struct {
	FixedPointScale   int64   `json:"fixed_point_scale"`
	RelativeTolerance float64 `json:"relative_tolerance"`
	TolerancePPM      int64   `json:"tolerance_ppm"`
	Algorithm         string  `json:"algorithm"`
}

type HotspotReport struct {
	PolicyVersion     uint64           `json:"policy_version"`
	CommitSeq         int64            `json:"commit_seq"`
	BuiltAt           time.Time        `json:"built_at"`
	ElapsedMS         int64            `json:"elapsed_ms"`
	RoundTimeBudgetMS int64            `json:"round_time_budget_ms"`
	WithinBudget      bool             `json:"within_budget"`
	Ranking           RankingPolicy    `json:"ranking"`
	Modes             ModeSummary      `json:"modes"`
	Services          []ServiceHotspot `json:"services"`
}

type serviceAggregate struct {
	frames          map[string]*frameAggregate
	weightedTotal   float64
	sampleCount     int64
	occurrenceTotal int64
}

type frameAggregate struct {
	name        string
	weighted    float64
	occurrences int64
}

func newServiceAggregate() *serviceAggregate {
	return &serviceAggregate{frames: make(map[string]*frameAggregate)}
}

func (a *serviceAggregate) addSample(sample SampleRecord, policy model.Policy) {
	weight := sample.EffectiveWeight(policy)
	a.weightedTotal += weight
	a.sampleCount++
	seenFrames := make(map[string]struct{}, len(sample.Frames))
	for _, frame := range sample.Frames {
		if _, ok := seenFrames[frame.ID]; ok {
			continue
		}
		seenFrames[frame.ID] = struct{}{}
		entry := a.frames[frame.ID]
		if entry == nil {
			entry = &frameAggregate{name: frame.Name}
			if entry.name == "" {
				entry.name = frame.ID
			}
			a.frames[frame.ID] = entry
		}
		entry.weighted += weight
		entry.occurrences++
		a.occurrenceTotal++
	}
}

func BuildHotspots(policy model.Policy, commitSeq int64, builtAt time.Time, samples []SampleRecord, reservoirs []Reservoir) HotspotReport {
	byService := make(map[string][]SampleRecord)
	for _, sample := range samples {
		byService[sample.Service] = append(byService[sample.Service], sample)
	}
	reservoirsByService := make(map[string][]Reservoir)
	for _, reservoir := range reservoirs {
		reservoirsByService[reservoir.Service] = append(reservoirsByService[reservoir.Service], reservoir)
	}
	serviceNames := make([]string, 0, len(byService))
	for service := range byService {
		serviceNames = append(serviceNames, service)
	}
	sort.Strings(serviceNames)

	report := HotspotReport{
		PolicyVersion:     policy.Version,
		CommitSeq:         commitSeq,
		BuiltAt:           builtAt.UTC(),
		RoundTimeBudgetMS: policy.RoundTimeBudgetMS,
		Ranking: RankingPolicy{
			FixedPointScale:   fixedScale,
			RelativeTolerance: policy.RankingRelativeTolerance,
			TolerancePPM:      int64(math.Round(policy.RankingRelativeTolerance * 1_000_000)),
			Algorithm:         "fixed-point descending; values within relative tolerance form a tie group; frame_id ascending breaks ties",
		},
		Services: make([]ServiceHotspot, 0, len(serviceNames)),
	}

	for _, service := range serviceNames {
		serviceSamples := byService[service]
		serviceReservoirs := reservoirsByService[service]
		sort.Slice(serviceSamples, func(i, j int) bool { return serviceSamples[i].SeenOrder < serviceSamples[j].SeenOrder })
		sort.Slice(serviceReservoirs, func(i, j int) bool {
			if serviceReservoirs[i].Channel != serviceReservoirs[j].Channel {
				return serviceReservoirs[i].Channel < serviceReservoirs[j].Channel
			}
			return serviceReservoirs[i].LayerKey < serviceReservoirs[j].LayerKey
		})

		svcCfg := policy.ServiceConfig(service)
		exactAgg := newServiceAggregate()
		var duplicateObservations int64
		for _, sample := range serviceSamples {
			exactAgg.addSample(sample, policy)
			duplicateObservations += sample.DuplicateCount
		}
		mode := ModeExact
		pathReason := fmt.Sprintf("population_samples (%d) <= exact_limit (%d)", len(serviceSamples), svcCfg.ExactLimit)
		selectedIDs := make(map[string]struct{})
		if int64(len(serviceSamples)) <= int64(svcCfg.ExactLimit) {
			for _, sample := range serviceSamples {
				selectedIDs[sample.ID] = struct{}{}
			}
		} else {
			mode = ModeApproximate
			pathReason = fmt.Sprintf("population_samples (%d) > exact_limit (%d)", len(serviceSamples), svcCfg.ExactLimit)
			for _, reservoir := range serviceReservoirs {
				for _, member := range reservoir.Members {
					selectedIDs[member.SampleID] = struct{}{}
				}
			}
		}

		selectedAgg := newServiceAggregate()
		for _, sample := range serviceSamples {
			if _, ok := selectedIDs[sample.ID]; ok {
				selectedAgg.addSample(sample, policy)
			}
		}
		selectedFraction := 0.0
		if exactAgg.weightedTotal > 0 {
			selectedFraction = selectedAgg.weightedTotal / exactAgg.weightedTotal
		}
		if selectedFraction > 1 {
			selectedFraction = 1
		}
		if selectedFraction < 0 {
			selectedFraction = 0
		}
		biasBound := 1 - selectedFraction
		if mode == ModeExact {
			biasBound = 0
		}

		frames := make([]FrameHotspot, 0, len(exactAgg.frames))
		frameIDs := make([]string, 0, len(exactAgg.frames))
		for frameID := range exactAgg.frames {
			frameIDs = append(frameIDs, frameID)
		}
		for frameID := range selectedAgg.frames {
			if _, ok := exactAgg.frames[frameID]; !ok {
				frameIDs = append(frameIDs, frameID)
			}
		}
		sort.Strings(frameIDs)
		for _, frameID := range frameIDs {
			exactFrame := exactAgg.frames[frameID]
			selectedFrame := selectedAgg.frames[frameID]
			exactShare := 0.0
			if exactAgg.weightedTotal > 0 && exactFrame != nil {
				exactShare = exactFrame.weighted / exactAgg.weightedTotal
			}
			estimate := exactShare
			if mode == ModeApproximate {
				estimate = 0
				if selectedAgg.weightedTotal > 0 && selectedFrame != nil {
					estimate = selectedFrame.weighted / selectedAgg.weightedTotal
				}
			}
			name := frameID
			if exactFrame != nil && exactFrame.name != "" {
				name = exactFrame.name
			} else if selectedFrame != nil && selectedFrame.name != "" {
				name = selectedFrame.name
			}
			var exactCount, selectedCount int64
			var exactWeighted, selectedWeighted float64
			if exactFrame != nil {
				exactCount = exactFrame.occurrences
				exactWeighted = exactFrame.weighted
			}
			if selectedFrame != nil {
				selectedCount = selectedFrame.occurrences
				selectedWeighted = selectedFrame.weighted
			}
			difference := math.Abs(estimate - exactShare)
			frames = append(frames, FrameHotspot{
				FrameID:         frameID,
				FrameName:       name,
				FixedEstimate:   int64(math.Round(estimate * float64(fixedScale))),
				EstimatedShare:  estimate,
				ExactShare:      exactShare,
				AbsoluteError:   difference,
				BiasUpperBound:  biasBound,
				WithinBiasBound: difference <= biasBound+1e-12,
				ConfidenceInterval: ConfidenceInterval{
					Level:  0.95,
					Lower:  math.Max(0, estimate-biasBound),
					Upper:  math.Min(1, estimate+biasBound),
					Method: "finite-population retained-mass bound; deterministic and conservative for at least 95% coverage",
				},
				ExactCount:            exactCount,
				ExactWeightedCount:    exactWeighted,
				SelectedCount:         selectedCount,
				SelectedWeightedCount: selectedWeighted,
			})
		}
		rankFrames(frames, report.Ranking.TolerancePPM)

		strata := buildStrata(service, serviceReservoirs, policy)
		reservoirBasis := make([]ReservoirBasis, 0, len(serviceReservoirs))
		for _, reservoir := range serviceReservoirs {
			reservoirBasis = append(reservoirBasis, ReservoirBasis{
				ReservoirID:             reservoir.ID,
				Channel:                 reservoir.Channel,
				LayerKey:                reservoir.LayerKey,
				Capacity:                reservoir.Capacity,
				SeenCount:               reservoir.SeenCount,
				SelectedCount:           reservoir.SelectedCount,
				ReplacedTotal:           reservoir.ReplacedTotal,
				RejectedTotal:           reservoir.RejectedTotal,
				EffectiveBiasCorrection: reservoir.References.EffectiveBiasCorrection,
				ChannelTargetPermille:   reservoir.References.ChannelTargetPermille,
				ReplacementRule:         reservoir.References.ReplacementRule,
			})
		}

		serviceHotspot := ServiceHotspot{
			Service:                 service,
			Path:                    PathDecision{Mode: mode, Reason: pathReason, PopulationSamples: int64(len(serviceSamples)), ExactLimit: svcCfg.ExactLimit, Statistic: statisticName(mode), UsedReservoir: mode == ModeApproximate},
			PopulationSamples:       int64(len(serviceSamples)),
			DuplicateObservations:   duplicateObservations,
			TotalObservations:       int64(len(serviceSamples)) + duplicateObservations,
			ReservoirCapacity:       svcCfg.ReservoirCapacity,
			SelectedSamples:         int(selectedAgg.sampleCount),
			SelectedWeightedTotal:   selectedAgg.weightedTotal,
			PopulationWeightedTotal: exactAgg.weightedTotal,
			SelectedWeightFraction:  selectedFraction,
			BiasUpperBound:          biasBound,
			BiasBoundFormula:        "1 - selected_weighted_total / population_weighted_total",
			ConfidenceMethod:        "bias bound applied symmetrically around estimate; coverage is conservative",
			Channels:                channelSummaries(policy),
			Strata:                  strata,
			Reservoirs:              reservoirBasis,
			Frames:                  frames,
			Conclusion:              buildConclusion(frames, report.Ranking),
		}
		if mode == ModeApproximate {
			strataKeys := make([]string, 0, len(strata))
			for _, stratum := range strata {
				strataKeys = append(strataKeys, stratum.LayerKey)
			}
			serviceHotspot.ApproximationBasis = &ApproximationBasis{
				Rule:                   policy.ReplacementRule,
				PopulationSamples:      int64(len(serviceSamples)),
				ExactLimit:             svcCfg.ExactLimit,
				SelectedSamples:        int(selectedAgg.sampleCount),
				SelectedWeightFraction: selectedFraction,
				StratificationKeys:     strataKeys,
				DepthPrefixes:          append([]int(nil), policy.Stratification.DepthPrefixes...),
				Reason:                 pathReason,
			}
			report.Modes.ApproximateServices++
		} else {
			report.Modes.ExactServices++
		}
		report.Services = append(report.Services, serviceHotspot)
	}
	return report
}

func statisticName(mode string) string {
	if mode == ModeExact {
		return "exact full-population weighted count"
	}
	return "deterministic weighted reservoir estimate"
}

func channelSummaries(policy model.Policy) []ChannelSampling {
	names := make([]string, 0, len(policy.Channels))
	for name := range policy.Channels {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ChannelSampling, 0, len(names))
	for _, name := range names {
		cfg := policy.Channels[name]
		out = append(out, ChannelSampling{
			Channel:        name,
			TargetPermille: cfg.TargetPermille,
			TargetRatio:    float64(cfg.TargetPermille) / 1000,
			BiasCorrection: cfg.BiasCorrection,
		})
	}
	return out
}

func buildStrata(service string, reservoirs []Reservoir, policy model.Policy) []StratumSummary {
	byKey := make(map[string]*StratumSummary)
	for _, reservoir := range reservoirs {
		summary := byKey[reservoir.LayerKey]
		if summary == nil {
			summary = &StratumSummary{
				LayerKey:      reservoir.LayerKey,
				DepthPrefixes: append([]int(nil), policy.Stratification.DepthPrefixes...),
				Channels:      []string{},
				ReservoirIDs:  []string{},
			}
			byKey[reservoir.LayerKey] = summary
		}
		summary.PopulationSamples += reservoir.SeenCount
		summary.SelectedSamples += int64(reservoir.SelectedCount)
		summary.Channels = append(summary.Channels, reservoir.Channel)
		summary.ReservoirIDs = append(summary.ReservoirIDs, reservoir.ID)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]StratumSummary, 0, len(keys))
	for _, key := range keys {
		item := byKey[key]
		sort.Strings(item.Channels)
		sort.Strings(item.ReservoirIDs)
		out = append(out, *item)
	}
	return out
}

func rankFrames(frames []FrameHotspot, tolerancePPM int64) {
	if len(frames) == 0 {
		return
	}
	sort.Slice(frames, func(i, j int) bool {
		if frames[i].FixedEstimate != frames[j].FixedEstimate {
			return frames[i].FixedEstimate > frames[j].FixedEstimate
		}
		return frames[i].FrameID < frames[j].FrameID
	})
	group := 0
	anchor := frames[0].FixedEstimate
	frames[0].TieGroup = group
	for i := 1; i < len(frames); i++ {
		if !withinTolerance(anchor, frames[i].FixedEstimate, tolerancePPM) {
			group++
			anchor = frames[i].FixedEstimate
		}
		frames[i].TieGroup = group
	}
	sort.Slice(frames, func(i, j int) bool {
		if frames[i].TieGroup != frames[j].TieGroup {
			return frames[i].TieGroup < frames[j].TieGroup
		}
		return frames[i].FrameID < frames[j].FrameID
	})
	for i := range frames {
		frames[i].HotspotRank = i + 1
	}
}

func withinTolerance(anchor, value, tolerancePPM int64) bool {
	if anchor == value {
		return true
	}
	diff := anchor - value
	if diff < 0 {
		diff = -diff
	}
	return diff*1_000_000 <= anchor*tolerancePPM
}

func buildConclusion(frames []FrameHotspot, ranking RankingPolicy) HotspotConclusion {
	if len(frames) == 0 {
		return HotspotConclusion{RelativeTolerance: ranking.RelativeTolerance, TolerancePPM: ranking.TolerancePPM}
	}
	topGroup := frames[0].TieGroup
	ids := make([]string, 0)
	for _, frame := range frames {
		if frame.TieGroup != topGroup {
			break
		}
		ids = append(ids, frame.FrameID)
	}
	sort.Strings(ids)
	leader := ""
	if len(ids) > 0 {
		leader = ids[0]
	}
	return HotspotConclusion{
		TopTieGroup:       topGroup,
		LeaderFrameID:     leader,
		TopTieFrameIDs:    ids,
		TopTieSize:        len(ids),
		RelativeTolerance: ranking.RelativeTolerance,
		TolerancePPM:      ranking.TolerancePPM,
	}
}
