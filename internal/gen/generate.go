package gen

import (
	"bufio"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"queryrewrite/internal/policy"
	"queryrewrite/internal/spec"
)

// FixedSeed makes every generated artifact reproducible byte-for-byte.
const FixedSeed int64 = 20261125

// DemoPolicy builds the deterministic baseline strategy snapshot (v1).
func DemoPolicy() spec.Policy {
	return spec.Policy{
		Version:           1,
		ParentVersion:     0,
		CandidateDepth:    6,
		StrategyPerMille:  map[string]int{"history": 120, "synonym": 880},
		CacheCapacity:     2,
		MinPrefixRunes:    6,
		PrefixBlockRunes:  3,
		HitGainThreshold:  0.20,
		FailureUpperLimit: 0.85,
		EvalBudgetPerTurn: 6,
		Weights: spec.DecisionWeights{
			Overlap: 1.4, Hit: 1.6, Age: 0.15, FailPenalty: 1.0,
		},
		Kind: "baseline",
	}
}

// DemoSessions hand-crafts deterministic sessions covering every required
// cache outcome: exact hit, partial hit, not reusable, same-turn contention
// and the empty-query stable business error.
func DemoSessions() []spec.QueryTurn {
	s1 := "sess-alpha"
	s2 := "sess-beta"
	s3 := "sess-gamma"
	return []spec.QueryTurn{
		// --- alpha: builds slots, then exact and partial hits -------------
		{SessionID: s1, Turn: 1, Locale: "zh-CN",
			RawQuery: "无线 蓝牙耳机 价格",
			History:  []string{"无线蓝牙耳机价钱", "蓝牙耳麦报价"}},
		{SessionID: s1, Turn: 2, Locale: "zh-CN",
			RawQuery: "无线蓝牙耳机 价格",
			History:  []string{"无线蓝牙耳机价钱"}},
		{SessionID: s1, Turn: 3, Locale: "zh-CN",
			RawQuery: "无线蓝牙音箱 价格",
			History:  []string{"无线蓝牙音箱报价"}},
		{SessionID: s1, Turn: 4, Locale: "zh-CN",
			RawQuery: "   ",
			History:  []string{"不会被使用"}},

		// --- beta: not reusable + cache-full eviction path ----------------
		{SessionID: s2, Turn: 1, Locale: "zh-CN",
			RawQuery: "笔记本电脑 推荐",
			History:  []string{"便携计算机推荐", "笔记本主机推荐"}},
		{SessionID: s2, Turn: 2, Locale: "zh-CN",
			RawQuery: "相机镜头 退货",
			History:  []string{"照相机镜头售后", "摄像镜头退款"}},
		{SessionID: s2, Turn: 3, Locale: "en-US",
			RawQuery: "coffee shop nearby",
			History:  []string{"latte shop nearby", "espresso cafe nearby"}},

		// --- gamma: same-turn prefix contention ---------------------------
		{SessionID: s3, Turn: 1, Locale: "zh-CN",
			RawQuery: "跑步鞋 男款 优惠",
			History:  []string{"跑步鞋男款折扣", "慢跑运动鞋男款促销", "跑步鞋男款折扣"}},
		{SessionID: s3, Turn: 2, Locale: "zh-CN",
			RawQuery: "机票酒店 查询",
			History:  []string{"航班宾馆查询", "机票旅店查询", "航班旅店查询"}},
	}
}

// WriteShards writes sessions as JSONL shard files, one file per session.
func WriteShards(dir string, sessions []spec.QueryTurn) ([]string, error) {
	bySession := map[string][]spec.QueryTurn{}
	order := []string{}
	for _, q := range sessions {
		if _, ok := bySession[q.SessionID]; !ok {
			order = append(order, q.SessionID)
		}
		bySession[q.SessionID] = append(bySession[q.SessionID], q)
	}
	paths := make([]string, 0, len(order))
	for _, sid := range order {
		path := filepath.Join(dir, "shard-"+sid+".jsonl")
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		w := bufio.NewWriter(f)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		for _, q := range bySession[sid] {
			if err := enc.Encode(q); err != nil {
				f.Close()
				return nil, err
			}
		}
		if err := w.Flush(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
		paths = append(paths, path)
	}
	return paths, nil
}

// WritePolicy writes the YAML strategy snapshot with its content digest.
func WritePolicy(dir string, p spec.Policy) (string, error) {
	p.ContentDigest = policy.Digest(p)
	path := filepath.Join(dir, "policy.yaml")
	b, err := yaml.Marshal(p)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// JitterSample is an optional fixed-seed RNG demonstration kept deterministic;
// the core pipeline never consumes randomness.
func JitterSample(n int) []int {
	r := rand.New(rand.NewSource(FixedSeed))
	out := make([]int, n)
	for i := range out {
		out[i] = r.Intn(1000)
	}
	return out
}
