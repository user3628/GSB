package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultReservoirCapacity = 64
	DefaultExactLimit        = 64
	DefaultRoundBudgetMS     = 100
	DefaultRankingTolerance  = 0.005
	ReplacementRule          = "weighted-bottom-k-sha256-v1"
)

type ChannelPolicy struct {
	TargetPermille int     `json:"target_permille"`
	BiasCorrection float64 `json:"bias_correction"`
}

func (c *ChannelPolicy) UnmarshalJSON(data []byte) error {
	type aux struct {
		TargetPermille *int     `json:"target_permille"`
		BiasCorrection *float64 `json:"bias_correction"`
	}
	var a aux
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	if a.TargetPermille == nil {
		return fmt.Errorf("target_permille is required")
	}
	c.TargetPermille = *a.TargetPermille
	c.BiasCorrection = 1
	if a.BiasCorrection != nil {
		c.BiasCorrection = *a.BiasCorrection
	}
	return nil
}

type ServicePolicy struct {
	ReservoirCapacity int     `json:"reservoir_capacity"`
	ExactLimit        int     `json:"exact_limit"`
	BiasCorrection    float64 `json:"bias_correction"`
}

func (s *ServicePolicy) UnmarshalJSON(data []byte) error {
	type aux struct {
		ReservoirCapacity *int     `json:"reservoir_capacity"`
		ExactLimit        *int     `json:"exact_limit"`
		BiasCorrection    *float64 `json:"bias_correction"`
	}
	var a aux
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	s.ReservoirCapacity = 0
	s.ExactLimit = -1
	s.BiasCorrection = 1
	if a.ReservoirCapacity != nil {
		s.ReservoirCapacity = *a.ReservoirCapacity
	}
	if a.ExactLimit != nil {
		s.ExactLimit = *a.ExactLimit
	}
	if a.BiasCorrection != nil {
		s.BiasCorrection = *a.BiasCorrection
	}
	return nil
}

type Stratification struct {
	DepthPrefixes []int  `json:"depth_prefixes"`
	Separator     string `json:"separator"`
}

type Policy struct {
	Version                  uint64                   `json:"version"`
	GeneratedAt              time.Time                `json:"generated_at"`
	Channels                 map[string]ChannelPolicy `json:"channels"`
	Services                 map[string]ServicePolicy `json:"services"`
	Stratification           Stratification           `json:"stratification"`
	DefaultReservoirCapacity int                      `json:"default_reservoir_capacity"`
	DefaultExactLimit        int                      `json:"default_exact_limit"`
	RoundTimeBudgetMS        int64                    `json:"round_time_budget_ms"`
	RankingRelativeTolerance float64                  `json:"ranking_relative_tolerance"`
	ReplacementRule          string                   `json:"replacement_rule"`
}

func (p *Policy) Normalize() error {
	if p.Version == 0 {
		return &ValidationError{Field: "version", Message: "policy version must be a positive integer", Value: p.Version}
	}
	if p.GeneratedAt.IsZero() {
		return &ValidationError{Field: "generated_at", Message: "generated_at is required and must be RFC3339"}
	}
	p.GeneratedAt = p.GeneratedAt.UTC()
	if len(p.Channels) == 0 {
		return &ValidationError{Field: "channels", Message: "at least one channel policy is required"}
	}
	channels := make(map[string]ChannelPolicy, len(p.Channels))
	for rawName, cfg := range p.Channels {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return &ValidationError{Field: "channels", Message: "channel name must not be empty"}
		}
		if cfg.TargetPermille < 1 || cfg.TargetPermille > 1000 {
			return &ValidationError{Field: "channels." + name + ".target_permille", Message: "target sampling rate must be an integer in [1,1000]", Value: cfg.TargetPermille}
		}
		if cfg.BiasCorrection == 0 {
			cfg.BiasCorrection = 1
		}
		if cfg.BiasCorrection < 0 || math.IsNaN(cfg.BiasCorrection) || math.IsInf(cfg.BiasCorrection, 0) {
			return &ValidationError{Field: "channels." + name + ".bias_correction", Message: "bias correction must be finite and positive", Value: cfg.BiasCorrection}
		}
		channels[name] = cfg
	}
	p.Channels = channels

	if p.DefaultReservoirCapacity < 0 {
		return &ValidationError{Field: "default_reservoir_capacity", Message: "default reservoir capacity must be positive"}
	}
	if p.DefaultReservoirCapacity == 0 {
		p.DefaultReservoirCapacity = DefaultReservoirCapacity
	}
	if p.DefaultExactLimit < 0 {
		return &ValidationError{Field: "default_exact_limit", Message: "default exact limit must not be negative"}
	}
	if p.DefaultExactLimit == 0 {
		p.DefaultExactLimit = DefaultExactLimit
	}

	services := make(map[string]ServicePolicy, len(p.Services))
	for rawName, cfg := range p.Services {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return &ValidationError{Field: "services", Message: "service name must not be empty"}
		}
		if cfg.ReservoirCapacity == 0 {
			cfg.ReservoirCapacity = p.DefaultReservoirCapacity
		}
		if cfg.ReservoirCapacity < 1 {
			return &ValidationError{Field: "services." + name + ".reservoir_capacity", Message: "reservoir capacity must be positive", Value: cfg.ReservoirCapacity}
		}
		if cfg.ExactLimit < 0 {
			cfg.ExactLimit = p.DefaultExactLimit
		}
		if cfg.BiasCorrection == 0 {
			cfg.BiasCorrection = 1
		}
		if cfg.BiasCorrection < 0 || math.IsNaN(cfg.BiasCorrection) || math.IsInf(cfg.BiasCorrection, 0) {
			return &ValidationError{Field: "services." + name + ".bias_correction", Message: "bias correction must be finite and positive", Value: cfg.BiasCorrection}
		}
		services[name] = cfg
	}
	p.Services = services

	if len(p.Stratification.DepthPrefixes) == 0 {
		p.Stratification.DepthPrefixes = []int{1, 3}
	}
	seenDepth := make(map[int]struct{}, len(p.Stratification.DepthPrefixes))
	depths := make([]int, 0, len(p.Stratification.DepthPrefixes))
	for _, depth := range p.Stratification.DepthPrefixes {
		if depth < 1 || depth > 256 {
			return &ValidationError{Field: "stratification.depth_prefixes", Message: "depth prefixes must be in [1,256]", Value: depth}
		}
		if _, ok := seenDepth[depth]; ok {
			continue
		}
		seenDepth[depth] = struct{}{}
		depths = append(depths, depth)
	}
	sort.Ints(depths)
	p.Stratification.DepthPrefixes = depths
	p.Stratification.Separator = strings.TrimSpace(p.Stratification.Separator)
	if p.Stratification.Separator == "" {
		p.Stratification.Separator = ">"
	}
	if strings.ContainsAny(p.Stratification.Separator, "\r\n\t") {
		return &ValidationError{Field: "stratification.separator", Message: "separator must not contain control whitespace"}
	}

	if p.RoundTimeBudgetMS == 0 {
		p.RoundTimeBudgetMS = DefaultRoundBudgetMS
	}
	if p.RoundTimeBudgetMS < 1 {
		return &ValidationError{Field: "round_time_budget_ms", Message: "round time budget must be positive", Value: p.RoundTimeBudgetMS}
	}
	if p.RankingRelativeTolerance < 0 || p.RankingRelativeTolerance > 1 || math.IsNaN(p.RankingRelativeTolerance) {
		return &ValidationError{Field: "ranking_relative_tolerance", Message: "ranking tolerance must be in [0,1]", Value: p.RankingRelativeTolerance}
	}
	if p.ReplacementRule == "" {
		p.ReplacementRule = ReplacementRule
	}
	if p.ReplacementRule != ReplacementRule {
		return &ValidationError{Field: "replacement_rule", Message: "unsupported replacement rule", Value: p.ReplacementRule}
	}
	return nil
}

func (p Policy) ServiceConfig(service string) ServicePolicy {
	if cfg, ok := p.Services[service]; ok {
		return cfg
	}
	return ServicePolicy{
		ReservoirCapacity: p.DefaultReservoirCapacity,
		ExactLimit:        p.DefaultExactLimit,
		BiasCorrection:    1,
	}
}

func (p Policy) ChannelConfig(channel string) (ChannelPolicy, bool) {
	cfg, ok := p.Channels[channel]
	return cfg, ok
}

func (p Policy) EffectiveBias(service, channel string) float64 {
	serviceBias := p.ServiceConfig(service).BiasCorrection
	channelBias := 1.0
	if cfg, ok := p.Channels[channel]; ok {
		channelBias = cfg.BiasCorrection
	}
	return serviceBias * channelBias
}

func (p Policy) LayerKey(service string, frames []Frame) string {
	parts := []string{"service=" + service}
	for _, depth := range p.Stratification.DepthPrefixes {
		n := depth
		if n > len(frames) {
			n = len(frames)
		}
		ids := make([]string, 0, n)
		for i := 0; i < n; i++ {
			ids = append(ids, frames[i].ID)
		}
		parts = append(parts, fmt.Sprintf("depth_%d=%s", depth, strings.Join(ids, p.Stratification.Separator)))
	}
	return strings.Join(parts, "|")
}

func stableJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (p Policy) StratificationFingerprint() string {
	return shortHash(stableJSON(struct {
		Depths    []int  `json:"depths"`
		Separator string `json:"separator"`
	}{p.Stratification.DepthPrefixes, p.Stratification.Separator}))
}

func (p Policy) HotspotPolicyFingerprint(service, channel string) string {
	svc := p.ServiceConfig(service)
	ch := ChannelPolicy{TargetPermille: 1000, BiasCorrection: 1}
	if configured, ok := p.Channels[channel]; ok {
		ch = configured
	}
	return shortHash(stableJSON(struct {
		Service          string  `json:"service"`
		Channel          string  `json:"channel"`
		Capacity         int     `json:"capacity"`
		ExactLimit       int     `json:"exact_limit"`
		ServiceBias      float64 `json:"service_bias"`
		ChannelBias      float64 `json:"channel_bias"`
		TargetPermille   int     `json:"target_permille"`
		Stratification   string  `json:"stratification"`
		RankingTolerance float64 `json:"ranking_tolerance"`
		ReplacementRule  string  `json:"replacement_rule"`
	}{service, channel, svc.ReservoirCapacity, svc.ExactLimit, svc.BiasCorrection, ch.BiasCorrection, ch.TargetPermille, p.StratificationFingerprint(), p.RankingRelativeTolerance, p.ReplacementRule}))
}

func (p Policy) Clone() Policy {
	data, _ := json.Marshal(p)
	var clone Policy
	_ = json.Unmarshal(data, &clone)
	return clone
}

type ChannelPatch struct {
	TargetPermille *int     `json:"target_permille,omitempty"`
	BiasCorrection *float64 `json:"bias_correction,omitempty"`
}

type ServicePatch struct {
	ReservoirCapacity *int     `json:"reservoir_capacity,omitempty"`
	ExactLimit        *int     `json:"exact_limit,omitempty"`
	BiasCorrection    *float64 `json:"bias_correction,omitempty"`
}

type PolicyIncrement struct {
	BaseVersion              uint64                  `json:"base_version"`
	Version                  *uint64                 `json:"version,omitempty"`
	GeneratedAt              *time.Time              `json:"generated_at,omitempty"`
	Channels                 map[string]ChannelPatch `json:"channels,omitempty"`
	Services                 map[string]ServicePatch `json:"services,omitempty"`
	Stratification           *Stratification         `json:"stratification,omitempty"`
	DefaultReservoirCapacity *int                    `json:"default_reservoir_capacity,omitempty"`
	DefaultExactLimit        *int                    `json:"default_exact_limit,omitempty"`
	RoundTimeBudgetMS        *int64                  `json:"round_time_budget_ms,omitempty"`
	RankingTolerance         *float64                `json:"ranking_relative_tolerance,omitempty"`
	ReplacementRule          *string                 `json:"replacement_rule,omitempty"`
}

type VersionMismatchError struct {
	Expected uint64
	Actual   uint64
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf("increment version must be %d, got %d", e.Expected, e.Actual)
}

func (inc PolicyIncrement) Merge(base Policy, generatedAt time.Time) (Policy, error) {
	next := base.Clone()
	next.Version = inc.BaseVersion + 1
	if inc.Version != nil && *inc.Version != next.Version {
		return Policy{}, &VersionMismatchError{Expected: next.Version, Actual: *inc.Version}
	}
	if inc.GeneratedAt != nil {
		next.GeneratedAt = inc.GeneratedAt.UTC()
	} else {
		next.GeneratedAt = generatedAt.UTC()
	}
	if next.Version == 0 {
		return Policy{}, &VersionMismatchError{Expected: 1, Actual: 0}
	}

	for rawName, patch := range inc.Channels {
		name := strings.TrimSpace(rawName)
		cfg, exists := next.Channels[name]
		if !exists {
			cfg = ChannelPolicy{BiasCorrection: 1}
		}
		if patch.TargetPermille != nil {
			cfg.TargetPermille = *patch.TargetPermille
		}
		if patch.BiasCorrection != nil {
			cfg.BiasCorrection = *patch.BiasCorrection
		}
		if cfg.TargetPermille == 0 {
			return Policy{}, &ValidationError{Field: "channels." + name + ".target_permille", Message: "target_permille is required for a new or incomplete channel patch"}
		}
		next.Channels[name] = cfg
	}
	for rawName, patch := range inc.Services {
		name := strings.TrimSpace(rawName)
		cfg, exists := next.Services[name]
		if !exists {
			cfg = ServicePolicy{ExactLimit: -1, BiasCorrection: 1}
		}
		if patch.ReservoirCapacity != nil {
			cfg.ReservoirCapacity = *patch.ReservoirCapacity
		}
		if patch.ExactLimit != nil {
			cfg.ExactLimit = *patch.ExactLimit
		}
		if patch.BiasCorrection != nil {
			cfg.BiasCorrection = *patch.BiasCorrection
		}
		next.Services[name] = cfg
	}
	if inc.Stratification != nil {
		next.Stratification = *inc.Stratification
	}
	if inc.DefaultReservoirCapacity != nil {
		next.DefaultReservoirCapacity = *inc.DefaultReservoirCapacity
	}
	if inc.DefaultExactLimit != nil {
		next.DefaultExactLimit = *inc.DefaultExactLimit
	}
	if inc.RoundTimeBudgetMS != nil {
		next.RoundTimeBudgetMS = *inc.RoundTimeBudgetMS
	}
	if inc.RankingTolerance != nil {
		next.RankingRelativeTolerance = *inc.RankingTolerance
	}
	if inc.ReplacementRule != nil {
		next.ReplacementRule = *inc.ReplacementRule
	}
	if err := next.Normalize(); err != nil {
		return Policy{}, err
	}
	return next, nil
}

type PolicyChanges struct {
	ChangedChannels       []string `json:"changed_channels"`
	ChangedServices       []string `json:"changed_services"`
	StratificationChanged bool     `json:"stratification_changed"`
	RankingChanged        bool     `json:"ranking_changed"`
	GlobalDefaultsChanged bool     `json:"global_defaults_changed"`
	ReplacementRuleChange bool     `json:"replacement_rule_changed"`
}

func (c PolicyChanges) HasChanges() bool {
	return len(c.ChangedChannels) > 0 || len(c.ChangedServices) > 0 || c.StratificationChanged || c.RankingChanged || c.GlobalDefaultsChanged || c.ReplacementRuleChange
}

func DiffPolicies(old, next Policy) PolicyChanges {
	var out PolicyChanges
	channelNames := map[string]struct{}{}
	for name := range old.Channels {
		channelNames[name] = struct{}{}
	}
	for name := range next.Channels {
		channelNames[name] = struct{}{}
	}
	for name := range channelNames {
		a, aok := old.Channels[name]
		b, bok := next.Channels[name]
		if !aok || !bok || a.TargetPermille != b.TargetPermille || a.BiasCorrection != b.BiasCorrection {
			out.ChangedChannels = append(out.ChangedChannels, name)
		}
	}
	serviceNames := map[string]struct{}{}
	for name := range old.Services {
		serviceNames[name] = struct{}{}
	}
	for name := range next.Services {
		serviceNames[name] = struct{}{}
	}
	for name := range serviceNames {
		a, aok := old.Services[name]
		b, bok := next.Services[name]
		if !aok || !bok || a.ReservoirCapacity != b.ReservoirCapacity || a.ExactLimit != b.ExactLimit || a.BiasCorrection != b.BiasCorrection {
			out.ChangedServices = append(out.ChangedServices, name)
		}
	}
	sort.Strings(out.ChangedChannels)
	sort.Strings(out.ChangedServices)
	out.StratificationChanged = old.StratificationFingerprint() != next.StratificationFingerprint()
	out.RankingChanged = old.RankingRelativeTolerance != next.RankingRelativeTolerance
	out.GlobalDefaultsChanged = old.DefaultReservoirCapacity != next.DefaultReservoirCapacity || old.DefaultExactLimit != next.DefaultExactLimit || old.RoundTimeBudgetMS != next.RoundTimeBudgetMS
	out.ReplacementRuleChange = old.ReplacementRule != next.ReplacementRule
	return out
}

func FormatPermille(permille int) string {
	return strconv.Itoa(permille) + "/1000"
}
