package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	StatusObserving   = "OBSERVING"
	StatusPassed      = "PASSED"
	StatusRollingBack = "ROLLING_BACK"
	StatusRolledBack  = "ROLLED_BACK"
	StatusEscalated   = "ESCALATED"

	MetricErrorRate   = "error_rate"
	MetricLatencyMS   = "latency_ms"
	MetricSuccessRate = "success_rate"

	ChangeConfig = "config_deploy"
	ChangeImage  = "image_upgrade"
	ChangeScale  = "scale"
)

type Thresholds struct {
	ErrorRatePPM          int64 `json:"error_rate_ppm"`
	LatencyDegradationPPM int64 `json:"latency_degradation_ppm"`
	SuccessDropPPM        int64 `json:"success_drop_ppm"`
	LevelTolerancePPM     int64 `json:"level_tolerance_ppm"`
}

type WeightsBP struct {
	ErrorBP   int64 `json:"error_bp"`
	LatencyBP int64 `json:"latency_bp"`
	SuccessBP int64 `json:"success_bp"`
}

type RiskLimits struct {
	PassBP     int64 `json:"pass_bp"`
	MediumBP   int64 `json:"medium_bp"`
	HighBP     int64 `json:"high_bp"`
	ExtremeBP  int64 `json:"extreme_bp"`
	RollbackBP int64 `json:"rollback_bp"`
}

type Policy struct {
	Version             int64      `json:"version"`
	GeneratedAtMS       int64      `json:"generated_at_ms"`
	ObservationWindowMS int64      `json:"observation_window_ms"`
	BucketMS            int64      `json:"bucket_ms"`
	MinSamples          int64      `json:"min_samples"`
	FastSampleLimit     int64      `json:"fast_sample_limit"`
	Thresholds          Thresholds `json:"thresholds"`
	WeightsBP           WeightsBP  `json:"weights_bp"`
	Risk                RiskLimits `json:"risk"`
	AutoRollback        bool       `json:"auto_rollback"`
	CooldownMS          int64      `json:"cooldown_ms"`
	Checksum            string     `json:"checksum"`
}

func (p Policy) CanonicalChecksum() (string, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return "", err
	}
	delete(object, "checksum")
	canonical, err := json.Marshal(object)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func (p *Policy) Validate() error {
	if p.Version <= 0 {
		return newBiz("E_POLICY_INVALID", "策略版本号必须为正整数", map[string]any{"field": "version"})
	}
	if p.GeneratedAtMS <= 0 {
		return newBiz("E_POLICY_INVALID", "策略生成时间必须为正毫秒时间戳", map[string]any{"field": "generated_at_ms"})
	}
	if p.ObservationWindowMS < 1000 {
		return newBiz("E_POLICY_INVALID", "观察窗口不得小于 1000ms", map[string]any{"field": "observation_window_ms"})
	}
	if p.BucketMS <= 0 || p.BucketMS > p.ObservationWindowMS {
		return newBiz("E_POLICY_INVALID", "时间桶必须大于 0 且不超过观察窗口", map[string]any{"field": "bucket_ms"})
	}
	if p.MinSamples <= 0 || p.FastSampleLimit <= 0 {
		return newBiz("E_POLICY_INVALID", "最小样本数与快速采样报告上限必须为正", map[string]any{
			"field": "fast_sample_limit", "min_samples": p.MinSamples, "fast_sample_limit": p.FastSampleLimit,
		})
	}
	if p.CooldownMS < 0 {
		return newBiz("E_POLICY_INVALID", "冷却期不得为负", map[string]any{"field": "cooldown_ms"})
	}
	for name, value := range map[string]int64{
		"error_rate_ppm": p.Thresholds.ErrorRatePPM, "latency_degradation_ppm": p.Thresholds.LatencyDegradationPPM,
		"success_drop_ppm": p.Thresholds.SuccessDropPPM, "level_tolerance_ppm": p.Thresholds.LevelTolerancePPM,
	} {
		if value < 0 {
			return newBiz("E_POLICY_INVALID", "策略阈值不得为负", map[string]any{"field": name})
		}
	}
	if p.Thresholds.ErrorRatePPM == 0 || p.Thresholds.LatencyDegradationPPM == 0 || p.Thresholds.SuccessDropPPM == 0 {
		return newBiz("E_POLICY_INVALID", "三项指标阈值必须为正", map[string]any{"field": "thresholds"})
	}
	if p.WeightsBP.ErrorBP < 0 || p.WeightsBP.LatencyBP < 0 || p.WeightsBP.SuccessBP < 0 ||
		p.WeightsBP.ErrorBP+p.WeightsBP.LatencyBP+p.WeightsBP.SuccessBP != 10000 {
		return newBiz("E_POLICY_INVALID", "风险权重必须为非负且合计 10000bp", map[string]any{"field": "weights_bp"})
	}
	levels := []int64{p.Risk.PassBP, p.Risk.MediumBP, p.Risk.HighBP, p.Risk.ExtremeBP, p.Risk.RollbackBP}
	for _, value := range levels {
		if value < 0 || value > 10000 {
			return newBiz("E_POLICY_INVALID", "风险边界必须位于 0..10000bp", map[string]any{"field": "risk"})
		}
	}
	if !(p.Risk.PassBP <= p.Risk.MediumBP && p.Risk.MediumBP < p.Risk.HighBP && p.Risk.HighBP < p.Risk.ExtremeBP) {
		return newBiz("E_POLICY_INVALID", "风险等级边界必须严格递增，且 pass 不高于 medium", map[string]any{"field": "risk"})
	}
	if p.Risk.RollbackBP < p.Risk.PassBP {
		return newBiz("E_POLICY_INVALID", "自动回滚阈值不得低于放行阈值", map[string]any{"field": "risk.rollback_bp"})
	}
	if p.Checksum == "" {
		return newBiz("E_POLICY_INVALID", "策略必须携带 checksum", map[string]any{"field": "checksum"})
	}
	expected, err := p.CanonicalChecksum()
	if err != nil {
		return newBiz("E_POLICY_INVALID", "策略摘要无法计算", map[string]any{"cause": err.Error()})
	}
	if !strings.EqualFold(p.Checksum, expected) {
		return newBiz("E_POLICY_INVALID", "策略摘要不匹配", map[string]any{
			"field": "checksum", "provided": p.Checksum, "expected": expected,
		})
	}
	p.Checksum = strings.ToLower(p.Checksum)
	return nil
}

type Baseline struct {
	TimestampMS    int64 `json:"timestamp_ms"`
	ErrorRatePPM   int64 `json:"error_rate_ppm"`
	P90LatencyMS   int64 `json:"p90_latency_ms"`
	SuccessRatePPM int64 `json:"success_rate_ppm"`
}

type Batch struct {
	ID                  string   `json:"id"`
	Service             string   `json:"service"`
	ChangeType          string   `json:"change_type"`
	BatchSize           int64    `json:"batch_size"`
	Baseline            Baseline `json:"baseline"`
	Status              string   `json:"status"`
	Revision            int64    `json:"revision"`
	CreatedAtMS         int64    `json:"created_at_ms"`
	UpdatedAtMS         int64    `json:"updated_at_ms"`
	LastObservationTSMS int64    `json:"last_observation_ts_ms"`
}

var servicePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{1,63}$`)

func (b *Batch) Validate() error {
	if b.ID == "" || len(b.ID) > 80 {
		return newBiz("E_BATCH_INVALID", "批次 ID 必须非空且不超过 80 字符", map[string]any{"field": "id"})
	}
	if !servicePattern.MatchString(b.Service) {
		return newBiz("E_BATCH_INVALID", "服务名格式非法", map[string]any{"field": "service", "value": b.Service})
	}
	switch b.ChangeType {
	case ChangeConfig, ChangeImage, ChangeScale:
	default:
		return newBiz("E_BATCH_INVALID", "变更类型非法", map[string]any{"field": "change_type", "value": b.ChangeType})
	}
	if b.BatchSize <= 0 {
		return newBiz("E_BATCH_INVALID", "批次规模必须为正", map[string]any{"field": "batch_size"})
	}
	if b.Baseline.TimestampMS <= 0 || b.Baseline.P90LatencyMS <= 0 {
		return newBiz("E_BATCH_INVALID", "基线时间戳与 P90 延迟必须为正", map[string]any{"field": "baseline"})
	}
	if b.Baseline.ErrorRatePPM < 0 || b.Baseline.ErrorRatePPM > 1_000_000 ||
		b.Baseline.SuccessRatePPM < 0 || b.Baseline.SuccessRatePPM > 1_000_000 {
		return newBiz("E_BATCH_INVALID", "基线错误率/成功率必须位于 0..1000000ppm", map[string]any{"field": "baseline"})
	}
	return nil
}

type Observation struct {
	Seq              int64   `json:"seq"`
	TimestampMS      int64   `json:"timestamp_ms"`
	MetricType       string  `json:"metric_type"`
	SampleCount      int64   `json:"sample_count"`
	SuccessCount     int64   `json:"success_count"`
	LatencySamplesMS []int64 `json:"latency_samples_ms,omitempty"`
}

func (o Observation) Validate() error {
	if o.Seq <= 0 {
		return newBiz("E_OBSERVATION_INVALID", "观测序号必须为正", map[string]any{"seq": o.Seq})
	}
	if o.TimestampMS <= 0 {
		return newBiz("E_OBSERVATION_INVALID", "观测时间戳必须为正毫秒时间戳", map[string]any{"seq": o.Seq})
	}
	switch o.MetricType {
	case MetricErrorRate, MetricSuccessRate:
		if o.SampleCount < 0 || o.SuccessCount < 0 || o.SuccessCount > o.SampleCount {
			return newBiz("E_OBSERVATION_INVALID", "样本数和成功数必须满足 0 <= success <= sample", map[string]any{"seq": o.Seq})
		}
		if len(o.LatencySamplesMS) != 0 {
			return newBiz("E_OBSERVATION_INVALID", "非延迟指标不得包含延迟样本", map[string]any{"seq": o.Seq})
		}
	case MetricLatencyMS:
		if o.SampleCount < 0 || int64(len(o.LatencySamplesMS)) != o.SampleCount {
			return newBiz("E_OBSERVATION_INVALID", "延迟指标的 sample_count 必须等于延迟样本数量", map[string]any{
				"seq": o.Seq, "sample_count": o.SampleCount, "latency_values": len(o.LatencySamplesMS),
			})
		}
		if o.SuccessCount != 0 {
			return newBiz("E_OBSERVATION_INVALID", "延迟指标不得携带 success_count", map[string]any{"seq": o.Seq})
		}
		for _, value := range o.LatencySamplesMS {
			if value < 0 {
				return newBiz("E_OBSERVATION_INVALID", "延迟样本不得为负", map[string]any{"seq": o.Seq, "value": value})
			}
		}
	default:
		return newBiz("E_OBSERVATION_INVALID", "指标类型非法", map[string]any{"seq": o.Seq, "metric_type": o.MetricType})
	}
	return nil
}

func (o Observation) PayloadHash() string {
	normalized := struct {
		Seq              int64   `json:"seq"`
		TimestampMS      int64   `json:"timestamp_ms"`
		MetricType       string  `json:"metric_type"`
		SampleCount      int64   `json:"sample_count"`
		SuccessCount     int64   `json:"success_count"`
		LatencySamplesMS []int64 `json:"latency_samples_ms"`
	}{o.Seq, o.TimestampMS, o.MetricType, o.SampleCount, o.SuccessCount, append([]int64(nil), o.LatencySamplesMS...)}
	raw, _ := json.Marshal(normalized)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func ValidTransition(from, to string) bool {
	allowed := map[string]map[string]bool{
		StatusObserving:   {StatusPassed: true, StatusRollingBack: true, StatusEscalated: true},
		StatusRollingBack: {StatusRolledBack: true, StatusEscalated: true},
		StatusPassed:      {},
		StatusRolledBack:  {},
		StatusEscalated:   {},
	}
	return allowed[from][to]
}

type BizError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
}

func (e *BizError) Error() string { return e.Code + ": " + e.Message }

func NewBizError(code, message string, context map[string]any) error {
	return &BizError{Code: code, Message: message, Context: context}
}

func newBiz(code, message string, context map[string]any) error {
	return &BizError{Code: code, Message: message, Context: context}
}

func AsBizError(err error) (*BizError, bool) {
	var biz *BizError
	if errors.As(err, &biz) {
		return biz, true
	}
	return nil, false
}

func SortedObservationSeqs(observations []Observation) []int64 {
	seqs := make([]int64, 0, len(observations))
	for _, observation := range observations {
		seqs = append(seqs, observation.Seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

func FormatID(prefix string, value int64) string { return fmt.Sprintf("%s-%d", prefix, value) }
