package experiment

import (
	"encoding/json"
	"strings"
	"time"

	"regworkbench/internal/cv"
	"regworkbench/internal/workerr"
)

type QueryRequest struct {
	Kind     string   `json:"kind"`
	Fold     *int     `json:"fold,omitempty"`
	Lambda   *float64 `json:"lambda,omitempty"`
	L1Ratio  *float64 `json:"l1_ratio,omitempty"`
	BudgetMS *int64   `json:"latency_budget_ms,omitempty"`
	QueryID  string   `json:"query_id,omitempty"`
}

type QueryResponse struct {
	QueryID   string          `json:"query_id"`
	Kind      string          `json:"kind"`
	ElapsedMS float64         `json:"elapsed_ms"`
	BudgetMS  int64           `json:"budget_ms"`
	CacheHit  bool            `json:"cache_hit"`
	CacheKey  string          `json:"cache_key"`
	Result    json.RawMessage `json:"result"`
}

type TrajectoryView struct {
	Fold          int               `json:"fold"`
	Lambda        float64           `json:"lambda"`
	L1Ratio       float64           `json:"l1_ratio"`
	Unregularized bool              `json:"unregularized"`
	Iterations    int               `json:"iterations"`
	Trajectory    []json.RawMessage `json:"trajectory,omitempty"`
	Records       any               `json:"records,omitempty"`
}

type FoldMetricsView struct {
	Fold    int                `json:"fold"`
	Metrics []PointMetricsView `json:"points"`
}

type PointMetricsView struct {
	Lambda         float64 `json:"lambda"`
	L1Ratio        float64 `json:"l1_ratio"`
	Unregularized  bool    `json:"unregularized"`
	Iterations     int     `json:"iterations"`
	Converged      bool    `json:"converged"`
	SelectedMetric float64 `json:"selected_metric"`
	Accuracy       float64 `json:"accuracy"`
	LogLoss        float64 `json:"log_loss"`
	AUC            float64 `json:"auc"`
}

func (s *Session) Query(req QueryRequest) (*QueryResponse, error) {
	qid := req.QueryID
	if qid == "" {
		qid = "query-" + padQueryID(s.querySeq.Add(1))
	}
	start := time.Now()
	budgetMS := s.Cfg.LatencyBudgetMS
	if req.BudgetMS != nil {
		budgetMS = *req.BudgetMS
	}
	snap := s.Snapshot()
	if snap == nil {
		return nil, workerr.New(workerr.ENotFound, "session snapshot is not initialized")
	}
	foldKeyValue := -1
	if req.Fold != nil {
		foldKeyValue = *req.Fold
	}
	pointPart := ""
	if req.Lambda != nil && req.L1Ratio != nil {
		pointPart = pointID(*req.Lambda, *req.L1Ratio)
	}
	cacheKey := cv.CacheKey{
		SampleFingerprint: s.DS.RawSHA,
		ConfigVersion:     s.ConfigVer,
		Fold:              foldKeyValue,
		Kind:              req.Kind,
		Point:             pointPart,
	}

	view, extraCachePart, err := s.buildQueryView(req, snap)
	if err != nil {
		return nil, err
	}
	if extraCachePart != "" {
		cacheKey.Point = cacheKey.Point + "|" + extraCachePart
	}
	b, hit, err := s.Cache.GetOrMarshal(cacheKey, view)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	elapsedMS := float64(elapsed.Microseconds()) / 1000.0
	if budgetMS <= 0 || elapsed > time.Duration(budgetMS)*time.Millisecond {
		e := workerr.New(workerr.ELatencyBudget, "query exceeded declared latency budget")
		e.QueryID = qid
		e.ElapsedMS = elapsedMS
		e.BudgetMS = float64(budgetMS)
		return nil, e
	}
	return &QueryResponse{
		QueryID: qid, Kind: req.Kind, ElapsedMS: elapsedMS, BudgetMS: budgetMS,
		CacheHit: hit, CacheKey: cacheKey.String(), Result: b,
	}, nil
}

func (s *Session) buildQueryView(req QueryRequest, snap *Snapshot) (any, string, error) {
	switch req.Kind {
	case "state", "status", "snapshot":
		return snap, "", nil
	case "fingerprints", "fingerprint":
		return snap.Fingerprints, "", nil
	case "trajectory":
		return s.trajectoryView(req, snap)
	case "metrics", "fold_metrics":
		return s.metricsView(req, snap)
	case "current_coefficients", "best_coefficients", "current_best":
		if snap.CurrentBest == nil {
			return nil, "", workerr.New(workerr.ENotFound, "current best coefficients are not available yet")
		}
		return snap.CurrentBest, "", nil
	case "final_model", "model", "coefficients":
		if snap.FinalModel == nil {
			return nil, "", workerr.New(workerr.ENotFound, "final model is not complete")
		}
		return snap.FinalModel, "", nil
	case "selection", "validation", "summary":
		if snap.Validation == nil {
			return nil, "", workerr.New(workerr.ENotFound, "validation summary is not available yet")
		}
		return snap.Validation, "", nil
	default:
		return nil, "", workerr.New(workerr.ENotFound, "unknown query kind: "+req.Kind)
	}
}

func (s *Session) trajectoryView(req QueryRequest, snap *Snapshot) (any, string, error) {
	if req.Fold == nil {
		folds := make([]*FoldArtifact, 0, s.Cfg.KFolds)
		for f := 0; f < s.Cfg.KFolds; f++ {
			if art := snap.Folds[f]; art != nil {
				folds = append(folds, art)
			}
		}
		return TrajectoryExport{SampleFingerprint: s.DS.RawSHA, ConfigVersion: s.ConfigVer, Folds: folds}, "", nil
	}
	f := *req.Fold
	art := snap.Folds[f]
	if req.Lambda != nil && req.L1Ratio != nil {
		if p := pointInFold(art, *req.Lambda, *req.L1Ratio); p != nil {
			return p, "", nil
		}
		if snap.Live != nil && snap.Live.Fold == f && snap.Live.Lambda == *req.Lambda && snap.Live.L1Ratio == *req.L1Ratio {
			return snap.Live, "iter=" + uint64String(uint64(snap.Live.Iteration)), nil
		}
		return nil, "", workerr.New(workerr.ENotFound, "trajectory point not available")
	}
	if art == nil {
		if snap.Live != nil && snap.Live.Fold == f {
			return snap.Live, "iter=" + uint64String(uint64(snap.Live.Iteration)), nil
		}
		return nil, "", workerr.New(workerr.ENotFound, "fold trajectory not available")
	}
	return art, "", nil
}

func (s *Session) metricsView(req QueryRequest, snap *Snapshot) (any, string, error) {
	if req.Fold == nil {
		return nil, "", workerr.New(workerr.EInvalidFold, "fold is required for fold_metrics")
	}
	f := *req.Fold
	art := snap.Folds[f]
	if art == nil {
		return nil, "", workerr.New(workerr.ENotFound, "fold metrics not available")
	}
	view := FoldMetricsView{Fold: f}
	for _, p := range art.Points {
		if req.Lambda != nil && req.L1Ratio != nil && (p.Lambda != *req.Lambda || p.L1Ratio != *req.L1Ratio) {
			continue
		}
		view.Metrics = append(view.Metrics, PointMetricsView{
			Lambda: p.Lambda, L1Ratio: p.L1Ratio, Unregularized: p.Unregularized,
			Iterations: p.Iterations, Converged: p.Converged, SelectedMetric: p.SelectedMetric,
			Accuracy: p.Metrics.Accuracy, LogLoss: p.Metrics.LogLoss, AUC: p.Metrics.AUC,
		})
	}
	if len(view.Metrics) == 0 {
		return nil, "", workerr.New(workerr.ENotFound, "matching fold metric point not available")
	}
	return view, "", nil
}

func uint64String(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func padQueryID(v uint64) string {
	s := uint64String(v)
	if len(s) >= 6 {
		return s
	}
	return strings.Repeat("0", 6-len(s)) + s
}
