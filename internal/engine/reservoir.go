package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"sort"

	"profilehotspot/internal/model"
)

type SampleRecord struct {
	ID             string
	ContentHash    string
	TimestampMS    int64
	Service        string
	FirstChannel   string
	Frames         []model.Frame
	Weight         int64
	SeenOrder      int64
	FirstBatchID   string
	FirstCommitSeq int64
	DuplicateCount int64
	ReceivedCount  int64
}

func (s SampleRecord) EffectiveWeight(policy model.Policy) float64 {
	return float64(s.Weight) * policy.EffectiveBias(s.Service, s.FirstChannel)
}

type ReservoirMember struct {
	SampleID  string  `json:"sample_id"`
	Priority  float64 `json:"priority"`
	Weight    int64   `json:"weight"`
	SeenOrder int64   `json:"seen_order"`
}

type ReservoirReferences struct {
	PolicyVersion             uint64  `json:"policy_version"`
	StratificationFingerprint string  `json:"stratification_fingerprint"`
	HotspotPolicyFingerprint  string  `json:"hotspot_policy_fingerprint"`
	ReplacementRule           string  `json:"replacement_rule"`
	ChannelTargetPermille     int     `json:"channel_target_permille"`
	ChannelBiasCorrection     float64 `json:"channel_bias_correction"`
	ServiceBiasCorrection     float64 `json:"service_bias_correction"`
	EffectiveBiasCorrection   float64 `json:"effective_bias_correction"`
	ReservoirCapacity         int     `json:"reservoir_capacity"`
	ExactLimit                int     `json:"exact_limit"`
}

type Reservoir struct {
	ID                        string              `json:"id"`
	PolicyVersion             uint64              `json:"policy_version"`
	Service                   string              `json:"service"`
	Channel                   string              `json:"channel"`
	LayerKey                  string              `json:"layer_key"`
	DepthPrefixes             []int               `json:"depth_prefixes"`
	Capacity                  int                 `json:"capacity"`
	SeenCount                 int64               `json:"seen_count"`
	DuplicateObservationCount int64               `json:"duplicate_observation_count"`
	SelectedCount             int                 `json:"selected_count"`
	Members                   []ReservoirMember   `json:"members"`
	ReplacedSampleIDs         []string            `json:"replaced_sample_ids"`
	ReplacedTotal             int64               `json:"replaced_total"`
	RejectedTotal             int64               `json:"rejected_total"`
	References                ReservoirReferences `json:"references"`
}

func reservoirID(policyVersion uint64, service, channel, layerKey string) string {
	sum := sha256.Sum256([]byte(service + "\x00" + channel + "\x00" + layerKey))
	return "rv_" + hex.EncodeToString(sum[:12])
}

// priority is the deterministic weighted-bottom-k key. A lower value is
// retained. SHA-256 provides a reproducible uniform variate so the retained set
// is independent of goroutine scheduling and Go's runtime random source.
func priority(sampleID, reservoirID string, weight int64) float64 {
	sum := sha256.Sum256([]byte(model.ReplacementRule + "|" + sampleID + "|" + reservoirID))
	v := binary.BigEndian.Uint64(sum[:8])
	u := (float64(v) + 1.0) / (float64(math.MaxUint64) + 2.0)
	return -math.Log(u) / float64(weight)
}

func memberLess(a, b ReservoirMember) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.SampleID < b.SampleID
}

func BuildReservoirs(policy model.Policy, samples []SampleRecord) []Reservoir {
	ordered := append([]SampleRecord(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].SeenOrder != ordered[j].SeenOrder {
			return ordered[i].SeenOrder < ordered[j].SeenOrder
		}
		return ordered[i].ID < ordered[j].ID
	})

	byID := make(map[string]*Reservoir)
	out := make([]Reservoir, 0)
	for _, sample := range ordered {
		svcCfg := policy.ServiceConfig(sample.Service)
		chCfg, _ := policy.ChannelConfig(sample.FirstChannel)
		layerKey := policy.LayerKey(sample.Service, sample.Frames)
		id := reservoirID(policy.Version, sample.Service, sample.FirstChannel, layerKey)
		res := byID[id]
		if res == nil {
			res = &Reservoir{
				ID:                id,
				PolicyVersion:     policy.Version,
				Service:           sample.Service,
				Channel:           sample.FirstChannel,
				LayerKey:          layerKey,
				DepthPrefixes:     append([]int(nil), policy.Stratification.DepthPrefixes...),
				Capacity:          svcCfg.ReservoirCapacity,
				Members:           make([]ReservoirMember, 0, svcCfg.ReservoirCapacity),
				ReplacedSampleIDs: make([]string, 0, minInt(svcCfg.ReservoirCapacity, 32)),
				References: ReservoirReferences{
					PolicyVersion:             policy.Version,
					StratificationFingerprint: policy.StratificationFingerprint(),
					HotspotPolicyFingerprint:  policy.HotspotPolicyFingerprint(sample.Service, sample.FirstChannel),
					ReplacementRule:           policy.ReplacementRule,
					ChannelTargetPermille:     chCfg.TargetPermille,
					ChannelBiasCorrection:     chCfg.BiasCorrection,
					ServiceBiasCorrection:     svcCfg.BiasCorrection,
					EffectiveBiasCorrection:   policy.EffectiveBias(sample.Service, sample.FirstChannel),
					ReservoirCapacity:         svcCfg.ReservoirCapacity,
					ExactLimit:                svcCfg.ExactLimit,
				},
			}
			byID[id] = res
			out = append(out, *res)
		}
		res = byID[id]
		res.SeenCount++
		res.DuplicateObservationCount += sample.DuplicateCount

		candidate := ReservoirMember{
			SampleID:  sample.ID,
			Priority:  priority(sample.ID, id, sample.Weight),
			Weight:    sample.Weight,
			SeenOrder: sample.SeenOrder,
		}
		if len(res.Members) < res.Capacity {
			res.Members = append(res.Members, candidate)
			sort.Slice(res.Members, func(i, j int) bool { return memberLess(res.Members[i], res.Members[j]) })
			continue
		}
		worst := len(res.Members) - 1
		if memberLess(candidate, res.Members[worst]) {
			replaced := res.Members[worst].SampleID
			res.ReplacedTotal++
			res.ReplacedSampleIDs = appendBounded(res.ReplacedSampleIDs, replaced, res.Capacity)
			res.Members[worst] = candidate
			sort.Slice(res.Members, func(i, j int) bool { return memberLess(res.Members[i], res.Members[j]) })
		} else {
			res.RejectedTotal++
		}
	}

	for _, res := range byID {
		res.SelectedCount = len(res.Members)
	}
	// Keep a stable response order independent of map iteration.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		if a.Channel != b.Channel {
			return a.Channel < b.Channel
		}
		return a.LayerKey < b.LayerKey
	})
	for i := range out {
		out[i] = *byID[out[i].ID]
	}
	return out
}

func appendBounded(values []string, value string, capacity int) []string {
	if capacity < 1 {
		capacity = 1
	}
	values = append(values, value)
	if len(values) > capacity {
		copy(values, values[len(values)-capacity:])
		values = values[:capacity]
	}
	return values
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
