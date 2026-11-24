package cockpitaudio

import (
	"math"
	"sort"
)

type DelayDetail struct {
	CommandID               string  `json:"command_id"`
	ZoneA                   string  `json:"zone_a"`
	ZoneB                   string  `json:"zone_b"`
	StartDelayMS            int     `json:"start_delay_ms_b_minus_a"`
	EndDelayMS              int     `json:"end_delay_ms_b_minus_a"`
	DelayMS                 float64 `json:"delay_ms_b_minus_a"`
	ObservedDistanceDiffM   float64 `json:"observed_distance_diff_m"`
	GeometryDistanceM       float64 `json:"geometry_distance_m"`
	GeometryExpectedDelayMS float64 `json:"geometry_expected_delay_ms"`
}

type AttributionCandidate struct {
	ZoneID           string             `json:"zone_id"`
	Word             string             `json:"word,omitempty"`
	LexicalLogScore  float64            `json:"lexical_log_score"`
	DelayLogScore    float64            `json:"delay_log_score"`
	PriorityLogScore float64            `json:"priority_log_score"`
	TotalScore       float64            `json:"total_score"`
	Interval         *Interval          `json:"matched_interval,omitempty"`
	Evidence         map[string]float64 `json:"evidence,omitempty"`
}

type Attribution struct {
	ShardID           string                 `json:"shard_id"`
	CommandID         string                 `json:"command_id"`
	Word              string                 `json:"word"`
	StartMS           int                    `json:"start_ms"`
	EndMS             int                    `json:"end_ms"`
	Ambiguous         bool                   `json:"ambiguous"`
	ResponsibleZoneID string                 `json:"responsible_zone_id,omitempty"`
	TopZoneID         string                 `json:"top_zone_id"`
	RunnerUpZoneID    string                 `json:"runner_up_zone_id"`
	ScoreGap          float64                `json:"score_gap"`
	GapThreshold      float64                `json:"gap_threshold"`
	Candidates        []AttributionCandidate `json:"candidates"`
	PairDelays        []DelayDetail          `json:"pair_delays"`
	FirstDifference   string                 `json:"-"`
}

type ShardAttribution struct {
	ShardID           string        `json:"shard_id"`
	ConfigFingerprint string        `json:"config_fingerprint"`
	EditVersion       int           `json:"edit_version"`
	Items             []Attribution `json:"attributions"`
}

type AttributionOptions struct {
	Intervals         []Interval
	Priorities        map[string]float64
	PriorityOverrides map[string]float64
	HitOverrides      map[string]map[string]CandidateHit
	EditVersion       int
}

func RunAttribution(shard *ShardManifest, vad *ShardVAD, cfg Config, opts AttributionOptions) (*ShardAttribution, *Error) {
	if err := cfg.ResolveAttribution(shard.ShardID); err != nil {
		return nil, err
	}
	priorities := map[string]float64{}
	for zone, value := range cfg.Priorities {
		priorities[zone] = value
	}
	for zone, value := range opts.Priorities {
		priorities[zone] = value
	}
	for zone, value := range opts.PriorityOverrides {
		priorities[zone] = value
	}
	intervals := opts.Intervals
	if intervals == nil && vad != nil {
		for _, z := range vad.Zones {
			intervals = append(intervals, z.Intervals...)
		}
	}

	out := &ShardAttribution{
		ShardID: shard.ShardID, ConfigFingerprint: AttributionConfigFingerprint(cfg),
		EditVersion: opts.EditVersion, Items: []Attribution{},
	}
	for _, command := range shard.Commands {
		item := attributeCommand(shard, command, intervals, cfg, priorities, opts.HitOverrides)
		out.Items = append(out.Items, item)
	}
	return out, nil
}

func mustConfigFingerprint(cfg Config) string {
	rc, err := cfg.ResolveVAD("")
	if err != nil {
		return "invalid-config"
	}
	return rc.ConfigFingerprint
}

func attributeCommand(shard *ShardManifest, command Command, intervals []Interval, cfg Config, priorities map[string]float64, overrides map[string]map[string]CandidateHit) Attribution {
	matched := map[string]*Interval{}
	for _, zoneID := range ZoneOrder {
		matched[zoneID] = matchInterval(zoneID, command, intervals)
	}

	pairDelays := buildPairDelays(command.ID, command.ShardID, matched, cfg)
	candidates := make([]AttributionCandidate, 0, len(ZoneOrder))
	for _, zoneID := range ZoneOrder {
		hit := findHit(command, zoneID, overrides)
		lexical := hit.Strength
		if hit.ZoneID == "" {
			lexical = -6
		}

		delayScore := 0.0
		evidence := map[string]float64{}
		if matched[zoneID] == nil {
			delayScore -= 8
			evidence["missing_candidate_interval_penalty"] = -8
		}
		for _, other := range ZoneOrder {
			if other == zoneID {
				continue
			}
			detail, ok := orientedDelay(pairDelays, command.ID, zoneID, other)
			if !ok || matched[zoneID] == nil || matched[other] == nil {
				delayScore -= 1.5
				continue
			}
			geomDelay := geometryDelayMS(cfg, zoneID, other)
			mismatch := detail.DelayMS - geomDelay
			likelihood := -0.5 * (mismatch / cfg.TDOASigmaMS) * (mismatch / cfg.TDOASigmaMS)
			delayScore += likelihood
			key := "tdoa_ms_" + other
			evidence[key] = Round(detail.DelayMS, 6)
			evidence["mismatch_ms_"+other] = Round(mismatch, 6)
			evidence["log_likelihood_"+other] = Round(likelihood, 6)
		}

		priority := 1.0
		if priorities != nil && priorities[zoneID] > 0 {
			priority = priorities[zoneID]
		}
		priorityLog := cfg.PriorityWeight * math.Log(priority)
		total := lexical + delayScore + priorityLog
		cand := AttributionCandidate{
			ZoneID: zoneID, Word: hit.Word,
			LexicalLogScore: Round(lexical, 6), DelayLogScore: Round(delayScore, 6),
			PriorityLogScore: Round(priorityLog, 6), TotalScore: Round(total, 6),
			Evidence: roundEvidence(evidence),
		}
		if in := matched[zoneID]; in != nil {
			intervalCopy := *in
			cand.Interval = &intervalCopy
		}
		candidates = append(candidates, cand)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].TotalScore != candidates[j].TotalScore {
			return candidates[i].TotalScore > candidates[j].TotalScore
		}
		return candidates[i].ZoneID < candidates[j].ZoneID
	})

	gap := 0.0
	runnerUp := ""
	if len(candidates) >= 2 {
		gap = Round(candidates[0].TotalScore-candidates[1].TotalScore, 6)
		runnerUp = candidates[1].ZoneID
	}
	ambiguous := gap < cfg.AttributionGapThreshold
	item := Attribution{
		ShardID: shard.ShardID, CommandID: command.ID, Word: command.Word,
		StartMS: command.StartMS, EndMS: command.EndMS, Ambiguous: ambiguous,
		TopZoneID: candidates[0].ZoneID, RunnerUpZoneID: runnerUp,
		ScoreGap: gap, GapThreshold: cfg.AttributionGapThreshold,
		Candidates: candidates, PairDelays: pairDelays,
	}
	if !ambiguous {
		item.ResponsibleZoneID = candidates[0].ZoneID
	}
	return item
}

func matchInterval(zoneID string, command Command, intervals []Interval) *Interval {
	var best Interval
	bestOverlap := 0
	found := false
	for _, in := range intervals {
		if in.ZoneID != zoneID {
			continue
		}
		overlap := minInt(command.EndMS, in.EndMS) - maxInt(command.StartMS, in.StartMS)
		if overlap <= 0 {
			continue
		}
		if !found || overlap > bestOverlap || (overlap == bestOverlap && in.StartMS < best.StartMS) {
			best = in
			bestOverlap = overlap
			found = true
		}
	}
	if !found {
		return nil
	}
	return &best
}

func buildPairDelays(commandID, shardID string, matched map[string]*Interval, cfg Config) []DelayDetail {
	out := []DelayDetail{}
	for i := 0; i < len(ZoneOrder); i++ {
		for j := i + 1; j < len(ZoneOrder); j++ {
			a, b := ZoneOrder[i], ZoneOrder[j]
			ia, ib := matched[a], matched[b]
			if ia == nil || ib == nil {
				continue
			}
			startDelta := ib.StartMS - ia.StartMS
			endDelta := ib.EndMS - ia.EndMS
			delay := Round(float64(startDelta+endDelta)/2.0, 3)
			geomDistance := geometryDistance(cfg, a, b)
			out = append(out, DelayDetail{
				CommandID: commandID, ZoneA: a, ZoneB: b,
				StartDelayMS: startDelta, EndDelayMS: endDelta, DelayMS: delay,
				ObservedDistanceDiffM:   Round(cfg.SpeedMPS*delay/1000.0, 6),
				GeometryDistanceM:       Round(geomDistance, 6),
				GeometryExpectedDelayMS: Round(geomDistance/cfg.SpeedMPS*1000.0, 6),
			})
		}
	}
	return out
}

func orientedDelay(delays []DelayDetail, commandID, candidate, other string) (DelayDetail, bool) {
	for _, d := range delays {
		if d.CommandID != commandID {
			continue
		}
		if d.ZoneA == candidate && d.ZoneB == other {
			return d, true
		}
		if d.ZoneA == other && d.ZoneB == candidate {
			cp := d
			cp.ZoneA, cp.ZoneB = candidate, other
			cp.StartDelayMS = -d.StartDelayMS
			cp.EndDelayMS = -d.EndDelayMS
			cp.DelayMS = -d.DelayMS
			cp.ObservedDistanceDiffM = -d.ObservedDistanceDiffM
			return cp, true
		}
	}
	return DelayDetail{}, false
}

func geometryDistance(cfg Config, a, b string) float64 {
	pa := cfg.Geometry[a]
	pb := cfg.Geometry[b]
	return math.Hypot(pa.X-pb.X, pa.Y-pb.Y)
}

func geometryDelayMS(cfg Config, source, mic string) float64 {
	return geometryDistance(cfg, source, mic) / cfg.SpeedMPS * 1000.0
}

func findHit(command Command, zoneID string, overrides map[string]map[string]CandidateHit) CandidateHit {
	if byZone, ok := overrides[command.ID]; ok {
		if hit, ok := byZone[zoneID]; ok {
			return hit
		}
	}
	for _, hit := range command.Hits {
		if hit.ZoneID == zoneID {
			return hit
		}
	}
	return CandidateHit{ZoneID: zoneID}
}

func roundEvidence(in map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = Round(v, 6)
	}
	return out
}
