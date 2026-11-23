package smuggle

import (
	"encoding/json"
	"sync"
)

// Config 控制批处理资源与算法路径选择。
type Config struct {
	Workers            int `json:"workers"`
	MaxConcurrency     int `json:"max_concurrency"`
	MaxQueue           int `json:"max_queue"`
	MaxRequestsPerConn int `json:"max_requests_per_conn"`
	StreamThreshold    int `json:"stream_threshold"`
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		Workers:            4,
		MaxConcurrency:     4,
		MaxQueue:           16,
		MaxRequestsPerConn: 1024,
		StreamThreshold:    64,
	}
}

// Connection 是一条待裁决连接的成对原始报文。
type Connection struct {
	ID      string
	Proxy   []byte
	Backend []byte
}

// ConnReport 是单个连接的裁决报告条目。
type ConnReport struct {
	Conn              string     `json:"conn"`
	Verdict           Verdict    `json:"verdict"`
	Code              Code       `json:"code"`
	Offset            int        `json:"offset"`
	Field             string     `json:"field"`
	Path              string     `json:"path"`
	Threshold         int        `json:"threshold"`
	EstimatedRequests int        `json:"estimated_requests"`
	ProxyCount        int        `json:"proxy_count"`
	BackendCount      int        `json:"backend_count"`
	ProxyCode         Code       `json:"proxy_code"`
	BackendCode       Code       `json:"backend_code"`
	SnapshotVersion   int64      `json:"snapshot_version"`
	ProxyRequests     []Boundary `json:"proxy_requests"`
	BackendRequests   []Boundary `json:"backend_requests"`
}

// canonicalPayload 生成快照负载（不含快照版本号）。
func (r ConnReport) canonicalPayload() []byte {
	r.SnapshotVersion = 0
	b, _ := json.Marshal(r)
	return b
}

// Engine 是并发连接批处理裁决引擎；会话间状态隔离。
type Engine struct {
	cfg   Config
	store *Store
}

// NewEngine 创建裁决引擎；store 可为 nil（不持久化快照）。
func NewEngine(cfg Config, store *Store) *Engine {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.MaxConcurrency < 1 {
		cfg.MaxConcurrency = 1
	}
	return &Engine{cfg: cfg, store: store}
}

// Config 返回引擎配置。
func (e *Engine) Config() Config { return e.cfg }

// AdjudicateOne 在独立会话中裁决单个连接并提交快照。
func (e *Engine) AdjudicateOne(conn Connection) ConnReport {
	path, est := SelectPath(conn.Proxy, conn.Backend, e.cfg.StreamThreshold)
	var adj Adjudication
	var pv, bv View
	if path == PathPrecise {
		adj, pv, bv = adjudicatePrecise(conn.Proxy, conn.Backend, e.cfg.MaxRequestsPerConn)
	} else {
		adj, pv, bv = adjudicateStreaming(conn.Proxy, conn.Backend, e.cfg.MaxRequestsPerConn)
	}
	rep := ConnReport{
		Conn:              conn.ID,
		Verdict:           adj.Verdict,
		Code:              adj.Code,
		Offset:            adj.Offset,
		Field:             adj.Field,
		Path:              path,
		Threshold:         e.cfg.StreamThreshold,
		EstimatedRequests: est,
		ProxyCount:        adj.ProxyCount,
		BackendCount:      adj.BackendCount,
		ProxyCode:         adj.ProxyCode,
		BackendCode:       adj.BackendCode,
		ProxyRequests:     pv.Boundaries,
		BackendRequests:   bv.Boundaries,
	}
	if pv.Code == CodeRequestBudget || bv.Code == CodeRequestBudget {
		// 单连接请求数超限：以 E_REQUEST_BUDGET 终止，保留已裁决结果。
		rep.Code = CodeRequestBudget
	}
	if e.store != nil {
		if snap, err := e.store.Commit(conn.ID, rep.canonicalPayload()); err == nil {
			rep.SnapshotVersion = snap.Version
		}
	}
	return rep
}

// Run 对一批连接执行并发裁决。
// 超出并发与等待队列容量（按提交序号确定性判定）的连接返回 E_BATCH_CONTENTION。
func (e *Engine) Run(conns []Connection) []ConnReport {
	reports := make([]ConnReport, len(conns))
	capacity := e.cfg.MaxConcurrency + e.cfg.MaxQueue

	type job struct {
		idx  int
		conn Connection
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	for w := 0; w < e.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				reports[j.idx] = e.AdjudicateOne(j.conn)
			}
		}()
	}
	for i, conn := range conns {
		if i >= capacity {
			reports[i] = ConnReport{
				Conn:      conn.ID,
				Verdict:   VerdictNone,
				Code:      CodeBatchContention,
				Offset:    -1,
				Path:      "none",
				Threshold: e.cfg.StreamThreshold,
			}
			continue
		}
		jobs <- job{idx: i, conn: conn}
	}
	close(jobs)
	wg.Wait()
	return reports
}
