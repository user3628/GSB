package service

import (
	"context"

	"netpolcompiler/internal/compiler"
	"netpolcompiler/internal/fixedpoint"
)

type ViewResponse struct {
	Namespace          string                      `json:"namespace"`
	ObservationVersion int64                       `json:"observationVersion"`
	UsedVersion        int64                       `json:"usedVersion"`
	CurrentVersion     int64                       `json:"currentVersion"`
	Freshness          string                      `json:"freshness"`
	ReadPolicy         string                      `json:"readPolicy"`
	View               *compiler.CompiledNamespace `json:"view"`
}

type AdjudicationResponse struct {
	Namespace          string                     `json:"namespace"`
	ObservationVersion int64                      `json:"observationVersion"`
	UsedVersion        int64                      `json:"usedVersion"`
	CurrentVersion     int64                      `json:"currentVersion"`
	Freshness          string                     `json:"freshness"`
	Decision           *compiler.EvaluationResult `json:"decision"`
}

type ConsistencyQuery struct {
	ObservationVersion int64
	ThresholdQ         *int64
	TolerancePpm       *int64
}

type ThresholdEvaluation struct {
	ThresholdQ           int64                                   `json:"thresholdQ"`
	RelativeTolerancePpm int64                                   `json:"relativeTolerancePpm"`
	Decisions            map[string]fixedpoint.ThresholdDecision `json:"decisions"`
}

type ConsistencyResponse struct {
	ObservationVersion int64                  `json:"observationVersion"`
	UsedVersion        int64                  `json:"usedVersion"`
	CurrentVersion     int64                  `json:"currentVersion"`
	Freshness          string                 `json:"freshness"`
	Report             *compiler.GlobalReport `json:"report"`
	Threshold          *ThresholdEvaluation   `json:"thresholdEvaluation,omitempty"`
}

func (s *Service) GetView(namespace string, observedVersion int64) (*ViewResponse, *BizError) {
	cacheSnapshot := s.cache.Load()
	if cacheSnapshot == nil {
		biz := NewBizError("E_INTERNAL", 503, "compiled cache is not initialized")
		return nil, biz
	}
	if observedVersion <= 0 {
		observedVersion = cacheSnapshot.version
	}

	if pending := s.pending.Load(); pending != nil {
		if _, affected := pending.Namespaces[namespace]; affected && observedVersion >= pending.TargetVersion {
			viewVersion := int64(0)
			if current := cacheSnapshot.views[namespace]; current != nil {
				viewVersion = current.Version
			}
			biz := NewBizError("E_STALE_READ", 409, "namespace cascade rebuild has not completed for the observed version")
			biz.Context = map[string]any{
				"namespace":       namespace,
				"viewVersion":     viewVersion,
				"observedVersion": observedVersion,
				"targetVersion":   pending.TargetVersion,
				"lagVersions":     observedVersion - viewVersion,
				"reason":          "cascade_rebuild_incomplete:" + pending.Reason,
				"safeReadVersion": viewVersion,
			}
			return nil, biz
		}
	}

	if observedVersion >= cacheSnapshot.version {
		view := cacheSnapshot.views[namespace]
		if view == nil {
			biz := NewBizError("E_NAMESPACE_NOT_FOUND", 404, "no compiled view exists for namespace")
			biz.Context["namespace"] = namespace
			biz.Context["observedVersion"] = observedVersion
			biz.Context["currentVersion"] = cacheSnapshot.version
			return nil, biz
		}
		freshness := "observed_version_served"
		if view.Version < observedVersion {
			freshness = "complete_namespace_view_older_than_observation_namespace_unaffected_or_last_ready"
		}
		return &ViewResponse{
			Namespace: namespace, ObservationVersion: observedVersion, UsedVersion: view.Version,
			CurrentVersion: cacheSnapshot.version, Freshness: freshness,
			ReadPolicy: "return a ready view whose version is not greater than observationVersion; reject only when an observed target has an incomplete cascade intent",
			View:       view,
		}, nil
	}

	historical, err := s.store.LoadViewAtOrBefore(context.Background(), namespace, observedVersion)
	if err != nil {
		return nil, NewBizError("E_INTERNAL", 500, err.Error())
	}
	if historical == nil {
		biz := NewBizError("E_STALE_READ", 409, "no ready compiled view is available at or before the observed version")
		biz.Context = map[string]any{"namespace": namespace, "viewVersion": 0, "observedVersion": observedVersion, "reason": "no_ready_view_at_or_before_observation_version"}
		return nil, biz
	}
	return &ViewResponse{
		Namespace: namespace, ObservationVersion: observedVersion, UsedVersion: historical.Version,
		CurrentVersion: cacheSnapshot.version, Freshness: "historical_committed_view",
		ReadPolicy: "served immutable SQLite view with version <= observationVersion", View: historical,
	}, nil
}

func (s *Service) Adjudicate(request compiler.EvaluationRequest) RawResult {
	view, biz := s.GetView(request.Namespace, request.ObservationVersion)
	if biz != nil {
		return FailureResult(biz)
	}
	request.ObservationVersion = view.ObservationVersion
	decision := compiler.Evaluate(view.View, request)
	return SuccessResult(200, AdjudicationResponse{
		Namespace: request.Namespace, ObservationVersion: view.ObservationVersion, UsedVersion: view.UsedVersion,
		CurrentVersion: view.CurrentVersion, Freshness: view.Freshness, Decision: decision,
	})
}

func (s *Service) GetConsistency(query ConsistencyQuery) (*ConsistencyResponse, *BizError) {
	cacheSnapshot := s.cache.Load()
	if cacheSnapshot == nil {
		return nil, NewBizError("E_INTERNAL", 503, "compiled cache is not initialized")
	}
	if query.ObservationVersion <= 0 {
		query.ObservationVersion = cacheSnapshot.version
	}
	tolerancePpm := int64(50_000)
	if query.TolerancePpm != nil {
		tolerancePpm = *query.TolerancePpm
	}

	if pending := s.pending.Load(); pending != nil && query.ObservationVersion >= pending.TargetVersion {
		namespaces := make([]string, 0, len(pending.Namespaces))
		for namespace := range pending.Namespaces {
			namespaces = append(namespaces, namespace)
		}
		biz := NewBizError("E_STALE_READ", 409, "cross-namespace consistency cascade rebuild is incomplete")
		biz.Context = map[string]any{
			"viewVersion": cacheSnapshot.version, "observedVersion": query.ObservationVersion,
			"targetVersion": pending.TargetVersion, "affectedNamespaces": namespaces,
			"reason": "cross_namespace_consistency_cascade_incomplete",
		}
		return nil, biz
	}

	response := &ConsistencyResponse{ObservationVersion: query.ObservationVersion, CurrentVersion: cacheSnapshot.version}
	var report *compiler.GlobalReport
	if query.ObservationVersion >= cacheSnapshot.version {
		report = compiler.BuildGlobalReport(cacheSnapshot.version, cacheSnapshot.views, tolerancePpm)
		response.UsedVersion = cacheSnapshot.version
		response.Freshness = "observed_version_served"
	} else {
		historical, err := s.store.LoadGlobalReportAtOrBefore(context.Background(), query.ObservationVersion)
		if err != nil {
			return nil, NewBizError("E_INTERNAL", 500, err.Error())
		}
		if historical == nil {
			biz := NewBizError("E_STALE_READ", 409, "no consistency report is available at or before the observed version")
			biz.Context["observedVersion"] = query.ObservationVersion
			return nil, biz
		}
		historical.Ranking = fixedpoint.Rank(historical.Namespaces, tolerancePpm)
		report = historical
		response.UsedVersion = historical.Version
		response.Freshness = "historical_committed_report"
	}
	response.Report = report

	thresholdQ := report.DefaultThresholdQ
	if query.ThresholdQ != nil {
		thresholdQ = *query.ThresholdQ
	}
	evaluation := &ThresholdEvaluation{ThresholdQ: thresholdQ, RelativeTolerancePpm: tolerancePpm, Decisions: map[string]fixedpoint.ThresholdDecision{}}
	for _, item := range report.Namespaces {
		evaluation.Decisions[item.Namespace] = fixedpoint.CompareThreshold(item.ScoreQ, thresholdQ, tolerancePpm)
	}
	response.Threshold = evaluation
	return response, nil
}
