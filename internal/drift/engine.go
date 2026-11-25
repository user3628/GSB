package drift

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"auditdrift/internal/model"
)

const levelDenominator = int64(2500)

type SourceValue struct {
	Source string
	Value  model.Value
}

func EvaluateTarget(sessionID, host string, baseline model.Baseline, observations []model.Observation, probe *SourceValue, viewVersion int64) (model.Conclusion, error) {
	conclusion := model.Conclusion{
		SessionID:     sessionID,
		Host:          host,
		BaselineID:    baseline.ID,
		Category:      baseline.Category,
		Status:        model.TaskDone,
		DriftCategory: model.DriftNone,
		RepairCost:    baseline.SeverityWeight,
		ViewVersion:   viewVersion,
		Details:       map[string]any{},
	}

	sources := make([]SourceValue, 0, len(observations)+1)
	for _, obs := range observations {
		source := obs.Source
		if source == "" {
			source = "observation"
		}
		sources = append(sources, SourceValue{Source: source, Value: model.CanonicalValue(obs.Actual)})
	}
	if probe != nil {
		probe.Value = model.CanonicalValue(probe.Value)
		sources = append(sources, *probe)
	}

	unique := uniqueValues(sources)
	if len(unique) > 1 {
		values := make([]map[string]any, 0, len(unique))
		for _, item := range unique {
			values = append(values, map[string]any{"source": item.Source, "value": item.Value})
		}
		conclusion.DriftCategory = model.DriftSourceConflict
		conclusion.OffsetPermille = 1000
		conclusion.Details["conflicting_values"] = values
		conclusion.Recommendation = "冻结该主机的自动修复，先核对采集代理与 HTTP 探测来源，消除来源冲突后再复核。"
		finalizeLevel(&conclusion, baseline.SeverityWeight)
		return conclusion, nil
	}

	if len(sources) == 0 {
		conclusion.DriftCategory = model.DriftMissing
		conclusion.OffsetPermille = 1000
		conclusion.MissingElements = []string{"observation"}
		conclusion.Recommendation = fmt.Sprintf("在 %s 上补采 %s，并确认采集通道与探测端点可用。", host, baseline.ID)
		finalizeLevel(&conclusion, baseline.SeverityWeight)
		return conclusion, nil
	}

	actual := unique[0].Value
	var err error
	switch baseline.Category {
	case model.CategoryAccountPolicy, model.CategoryAuditPolicy:
		err = evaluateNumeric(&conclusion, baseline, actual)
	case model.CategoryServiceStartup:
		err = evaluateService(&conclusion, baseline, actual)
	case model.CategoryRegistryACL:
		err = evaluateRegistry(&conclusion, baseline, actual)
	default:
		err = model.NewCodedError(model.ErrBaselineInvalid, "%s: unsupported category %q", baseline.ID, baseline.Category)
	}
	if err != nil {
		return model.Conclusion{}, err
	}
	finalizeLevel(&conclusion, baseline.SeverityWeight)
	return conclusion, nil
}

func uniqueValues(sources []SourceValue) []SourceValue {
	seen := make(map[string]SourceValue)
	order := make([]string, 0)
	for _, source := range sources {
		key := canonicalKey(source.Value)
		if _, ok := seen[key]; !ok {
			order = append(order, key)
		}
		seen[key] = source
	}
	out := make([]SourceValue, 0, len(order))
	for _, key := range order {
		out = append(out, seen[key])
	}
	return out
}

func canonicalKey(value model.Value) string {
	data, _ := json.Marshal(model.CanonicalValue(value))
	return string(data)
}

func evaluateNumeric(c *model.Conclusion, b model.Baseline, actual model.Value) error {
	if actual.Kind != model.KindNumber {
		return model.NewCodedError(model.ErrValueTypeMismatch, "%s: expected numeric value for %s, got %s", b.ID, b.Category, actual.Kind)
	}
	iv := b.Expected.Interval
	c.Details["expected_interval"] = iv.String()
	c.Details["actual_number"] = actual.Number
	if iv.Contains(actual.Number) {
		return nil
	}
	c.DriftCategory = model.DriftValueOffset
	distance := int64(0)
	if actual.Number < iv.Min {
		boundary := iv.Min
		if !iv.MinInclusive {
			boundary = iv.Min + 1
		}
		distance = boundary - actual.Number
		if distance < 1 {
			distance = 1
		}
	} else {
		boundary := iv.Max
		if !iv.MaxInclusive {
			boundary = iv.Max - 1
		}
		distance = actual.Number - boundary
		if distance < 1 {
			distance = 1
		}
	}
	width := iv.Max - iv.Min
	scale := width
	if scale < 1 {
		scale = 1
	}
	c.OffsetPermille = min64(1000, distance*1000/scale)
	c.Details["endpoint_inclusion"] = map[string]bool{"min_inclusive": iv.MinInclusive, "max_inclusive": iv.MaxInclusive}
	c.Details["distance_from_admissible_set"] = distance
	c.Recommendation = fmt.Sprintf("将数值从 %d 调整到声明区间 %s；端点包含关系为 min_inclusive=%t,max_inclusive=%t。", actual.Number, iv.String(), iv.MinInclusive, iv.MaxInclusive)
	return nil
}

func evaluateService(c *model.Conclusion, b model.Baseline, actual model.Value) error {
	if actual.Kind != model.KindString {
		return model.NewCodedError(model.ErrValueTypeMismatch, "%s: expected service startup string, got %s", b.ID, actual.Kind)
	}
	allowed := b.Expected.AllowedEnum
	sort.Strings(allowed)
	c.Details["declared_enum"] = allowed
	c.Details["actual_text"] = actual.Text
	if !contains(allowed, actual.Text) {
		c.DriftCategory = model.DriftValueOffset
		c.OffsetPermille = 1000
		c.Details["outside_declared_enum"] = true
		c.Recommendation = fmt.Sprintf("实测启动类型 %q 不在声明枚举 %s 中；应先修正采集值，再将服务启动类型设置为 %q。", actual.Text, strings.Join(allowed, "/"), b.Expected.ExpectedText)
		return nil
	}
	if actual.Text == b.Expected.ExpectedText {
		return nil
	}
	c.DriftCategory = model.DriftValueOffset
	c.OffsetPermille = 1000
	c.Recommendation = fmt.Sprintf("将服务启动类型从 %q 调整为声明枚举值 %q。", actual.Text, b.Expected.ExpectedText)
	return nil
}

func evaluateRegistry(c *model.Conclusion, b model.Baseline, actual model.Value) error {
	if actual.Kind != model.KindSet {
		return model.NewCodedError(model.ErrValueTypeMismatch, "%s: expected permission set, got %s", b.ID, actual.Kind)
	}
	expected := model.CanonicalValue(model.Value{Kind: model.KindSet, Set: b.Expected.RequiredSet}).Set
	actualSet := model.CanonicalValue(actual).Set
	c.Details["expected_set"] = expected
	c.Details["actual_set"] = actualSet
	missing := difference(expected, actualSet)
	extra := difference(actualSet, expected)
	c.MissingElements = missing
	c.ExtraElements = extra
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	switch {
	case len(missing) > 0 && len(extra) == 0:
		c.DriftCategory = model.DriftMissing
	case len(extra) > 0 && len(missing) == 0:
		c.DriftCategory = model.DriftExtra
	default:
		c.DriftCategory = model.DriftValueOffset
	}
	score := int64(len(missing))*1000 + int64(len(extra))*500
	c.OffsetPermille = min64(1000, score/int64(max1(len(expected))))
	c.Recommendation = registryRecommendation(b.ID, missing, extra)
	return nil
}

func registryRecommendation(id string, missing, extra []string) string {
	parts := []string{fmt.Sprintf("复核注册表键 %s 的 ACL 继承与最小权限。", id)}
	if len(missing) > 0 {
		parts = append(parts, "补齐缺失权限: "+strings.Join(missing, ", ")+"。")
	}
	if len(extra) > 0 {
		parts = append(parts, "移除多余权限主体/位: "+strings.Join(extra, ", ")+"。")
	}
	return strings.Join(parts, "")
}

func finalizeLevel(c *model.Conclusion, weight int) {
	if c.DriftCategory == model.DriftNone {
		c.OwnLevel = 0
		c.DriftLevel = 0
		c.ScoreScaled = 0
		c.LevelRounding = model.RoundingExplanation{Method: "integer fixed-point; conformant level is zero"}
		return
	}
	c.ScoreScaled = int64(weight)*1000 + c.OffsetPermille
	numerator := c.ScoreScaled - 1
	q := numerator / levelDenominator
	r := numerator % levelDenominator
	level := int(1 + q)
	if level < 1 {
		level = 1
	}
	if level > 4 {
		level = 4
	}
	c.OwnLevel = level
	c.DriftLevel = level
	c.LevelRounding = model.RoundingExplanation{
		Method:      "floor((weight*1000 + offset_permille - 1) / 2500), clamped to L1-L4",
		Numerator:   numerator,
		Denominator: levelDenominator,
		Quotient:    q,
		Remainder:   r,
		Note:        "全部使用整数/千分比定点数，不使用浮点等级合成。",
	}
}

func contains(items []string, want string) bool {
	i := sort.SearchStrings(items, want)
	return i < len(items) && items[i] == want
}

func difference(a, b []string) []string {
	setB := make(map[string]struct{}, len(b))
	for _, item := range b {
		setB[item] = struct{}{}
	}
	out := make([]string, 0)
	for _, item := range a {
		if _, ok := setB[item]; !ok {
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
