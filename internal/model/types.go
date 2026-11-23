package model

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Phase 为实例可用性五阶段之一。
type Phase string

const (
	PhaseUnknown   Phase = "UNKNOWN"
	PhaseHealthy   Phase = "HEALTHY"
	PhaseSuspect   Phase = "SUSPECT"
	PhaseUnhealthy Phase = "UNHEALTHY"
	PhaseDraining  Phase = "DRAINING"
)

// ProbeSpec 为实例声明的探测参数。
type ProbeSpec struct {
	Mode             string `json:"mode"`              // http 或 tcp
	Path             string `json:"path"`              // HTTP 探测路径
	ExpectedStatus   int    `json:"expected_status"`   // 期望 HTTP 状态码
	BodyMarker       string `json:"body_marker"`       // 响应体必须包含的标记
	TimeoutMillis    int    `json:"timeout_millis"`    // 单次探测独立超时
	SuccessThreshold int    `json:"success_threshold"` // 连续成功阈值
	FailureThreshold int    `json:"failure_threshold"` // 连续失败阈值
}

// Instance 是后端池实例声明。
type Instance struct {
	ID     string    `json:"id"`
	Port   int       `json:"port"`
	Weight int       `json:"weight"` // 千分比权重，1-1000
	Zone   string    `json:"zone"`
	Probe  ProbeSpec `json:"probe"`
}

// RequestRecord 是确定性客户端请求轨迹中的一条。
type RequestRecord struct {
	TimestampMillis int64  `json:"timestamp_millis"`
	ClientID        string `json:"client_id"`
	RequestWeight   int    `json:"request_weight"`
	RequestBytes    int64  `json:"request_bytes"`
}

// ZoneAffinity 声明可用区亲和比例（千分比，合计需为 1000）。
type ZoneAffinity struct {
	Ratios map[string]int `json:"ratios"`
}

// Sample 为 samples 目录下整份样本。
type Sample struct {
	Instances []Instance      `json:"instances"`
	Trace     []RequestRecord `json:"trace"`
	Affinity  ZoneAffinity    `json:"affinity"`
}

// RuntimeConfig 为调度器运行期约束。
type RuntimeConfig struct {
	GlobalConcurrency int `json:"global_concurrency"`
	PerInstanceLimit  int `json:"per_instance_limit"`
	QueueCapacity     int `json:"queue_capacity"`
	RoundBudgetMillis int `json:"round_budget_millis"`
	ProbeConcurrency  int `json:"probe_concurrency"`
}

// SnapshotVersion 是一次完整提交的可用集合 + 归一化权重视图。
type SnapshotVersion struct {
	Version       int64            `json:"version"`
	Reason        string           `json:"reason"`
	InstanceState map[string]Phase `json:"instance_state"`
	// ZoneWeights 为按可用区归一化后的实例千分比权重（区内合计 1000）。
	ZoneWeights map[string]map[string]int `json:"zone_weights"`
	// ZoneRatios 为本版本生效的可用区亲和比例。
	ZoneRatios map[string]int `json:"zone_ratios"`
}

// Decision 记录一次调度决策依据，便于按请求回溯。
type Decision struct {
	RequestSeq       int    `json:"request_seq"`
	ClientID         string `json:"client_id"`
	SnapshotVersion  int64  `json:"snapshot_version"`
	HashValue        uint64 `json:"hash_value"`
	PreferredZone    string `json:"preferred_zone"`
	SelectedZone     string `json:"selected_zone"`
	SelectedInstance string `json:"selected_instance"`
	Fallback         bool   `json:"fallback"`
	FallbackReason   string `json:"fallback_reason,omitempty"`
	WeightInterval   string `json:"weight_interval"`
}

// ProbeOutcome 为单次探测结论。
type ProbeOutcome struct {
	InstanceID string
	Success    bool
	StatusCode int
	ErrCode    string // E_PROBE_REFUSED / E_PROBE_TIMEOUT / E_PROBE_FAILED / ""
	ErrMsg     string
	ElapsedMS  int64
}

// Validate 校验整份样本；失败统一返回 E_INSTANCE_INVALID，整份样本不生效。
func (s *Sample) Validate() error {
	seenInstance := make(map[string]struct{})
	if len(s.Instances) == 0 {
		return NewBizError(CodeInstanceInvalid, "instance list is empty", "")
	}
	for i, ins := range s.Instances {
		ctx := fmt.Sprintf("instances[%d]", i)
		if ins.ID == "" {
			return NewBizError(CodeInstanceInvalid, "instance id is empty", ctx)
		}
		if _, dup := seenInstance[ins.ID]; dup {
			return NewBizError(CodeInstanceInvalid, "duplicate instance id", ctx+" id="+ins.ID)
		}
		seenInstance[ins.ID] = struct{}{}
		if ins.Port < 1 || ins.Port > 65535 {
			return NewBizError(CodeInstanceInvalid, "port must be in 1-65535", ctx)
		}
		if ins.Weight < 1 || ins.Weight > 1000 {
			return NewBizError(CodeInstanceInvalid, "weight must be a positive integer in 1-1000", ctx)
		}
		if ins.Zone == "" {
			return NewBizError(CodeInstanceInvalid, "zone must not be empty", ctx)
		}
		if ins.Probe.Mode != "http" && ins.Probe.Mode != "tcp" {
			return NewBizError(CodeInstanceInvalid, "probe mode must be http or tcp", ctx)
		}
		if ins.Probe.Mode == "http" {
			if ins.Probe.Path == "" || ins.Probe.ExpectedStatus == 0 || ins.Probe.BodyMarker == "" {
				return NewBizError(CodeInstanceInvalid, "http probe requires path, expected_status and body_marker", ctx)
			}
		}
		if ins.Probe.TimeoutMillis <= 0 {
			return NewBizError(CodeInstanceInvalid, "probe timeout_millis must be positive", ctx)
		}
		if ins.Probe.SuccessThreshold <= 0 || ins.Probe.FailureThreshold <= 0 {
			return NewBizError(CodeInstanceInvalid, "success/failure threshold must be positive", ctx)
		}
	}

	if len(s.Trace) == 0 {
		return NewBizError(CodeInstanceInvalid, "request trace is empty", "trace")
	}
	seenClient := make(map[string]struct{})
	for i, req := range s.Trace {
		ctx := fmt.Sprintf("trace[%d]", i)
		if req.ClientID == "" {
			return NewBizError(CodeInstanceInvalid, "client_id must not be empty", ctx)
		}
		// 客户端标识唯一：同一条轨迹中不允许出现重复客户端标识。
		if _, dup := seenClient[req.ClientID]; dup {
			return NewBizError(CodeInstanceInvalid, "duplicate client id in trace", ctx+" client="+req.ClientID)
		}
		seenClient[req.ClientID] = struct{}{}
		if req.RequestWeight <= 0 {
			return NewBizError(CodeInstanceInvalid, "request_weight must be a positive integer", ctx)
		}
		if req.RequestBytes <= 0 {
			return NewBizError(CodeInstanceInvalid, "request_bytes must be a positive integer", ctx)
		}
	}

	ratioSum := 0
	for zone, ratio := range s.Affinity.Ratios {
		if zone == "" {
			return NewBizError(CodeInstanceInvalid, "affinity zone must not be empty", "affinity")
		}
		if ratio < 0 || ratio > 1000 {
			return NewBizError(CodeInstanceInvalid, "affinity ratio must be in 0-1000", "affinity zone="+zone)
		}
		ratioSum += ratio
	}
	if len(s.Affinity.Ratios) == 0 || ratioSum != 1000 {
		return NewBizError(CodeInstanceInvalid, "affinity ratios must sum to 1000", fmt.Sprintf("sum=%d", ratioSum))
	}
	return nil
}

// SortedZones 返回有序可用区名，保证跨模块视图确定性。
func SortedZones(m map[string]int) []string {
	zones := make([]string, 0, len(m))
	for z := range m {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	return zones
}

// CloneSnapshot 深拷贝快照，避免调用方修改已提交版本。
func CloneSnapshot(s *SnapshotVersion) *SnapshotVersion {
	buf, _ := json.Marshal(s)
	var out SnapshotVersion
	_ = json.Unmarshal(buf, &out)
	return &out
}
