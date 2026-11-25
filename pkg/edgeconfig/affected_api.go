package edgeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
)

type AffectedRequest struct {
	RequestID    string              `json:"request_id"`
	Version      int64               `json:"version,omitempty"`
	ChangedKeys  []string            `json:"changed_keys,omitempty"`
	SeedTriggers map[string][]string `json:"seed_triggers,omitempty"`
}

func (s *Service) RecomputeAffected(ctx context.Context, request AffectedRequest) (*AffectedReport, error) {
	return runWrite(s, ctx, "affected_recompute", request.RequestID, request, func(job *writeJob[AffectedReport]) (*AffectedReport, *APIError) {
		current, apiErr := currentVersionAPI(job.ctx, s.store)
		if apiErr != nil {
			return nil, apiErr
		}
		targetVersion := request.Version
		if targetVersion == 0 {
			targetVersion = current
		}
		changedKeys := append([]string(nil), request.ChangedKeys...)
		seedTriggers := request.SeedTriggers
		if seedTriggers == nil {
			seedTriggers = make(map[string][]string)
		}

		if len(changedKeys) == 0 && targetVersion > 0 {
			var payloadJSON string
			err := s.db.QueryRowContext(job.ctx, `SELECT payload_json FROM increments WHERE version=?`, targetVersion).Scan(&payloadJSON)
			if err == sql.ErrNoRows {
				// Snapshot or explicit version: use all currently declared keys.
				topology, err := s.LoadTopology(job.ctx)
				if err != nil {
					return nil, internalError(err)
				}
				for _, key := range topology.Keys {
					changedKeys = append(changedKeys, key.Path)
				}
			} else if err != nil {
				return nil, internalError(err)
			} else {
				var increment Increment
				if err := json.Unmarshal([]byte(payloadJSON), &increment); err != nil {
					return nil, internalError(err)
				}
				for _, change := range increment.Changes {
					changedKeys = append(changedKeys, change.Path)
				}
				changedKeys = append(changedKeys, increment.RemoveKeys...)
				for _, subChange := range increment.SubscriptionChanges {
					seedTriggers[subChange.NodeID] = append(seedTriggers[subChange.NodeID], subChange.Add...)
					seedTriggers[subChange.NodeID] = append(seedTriggers[subChange.NodeID], subChange.Remove...)
				}
			}
		}

		graph, policy, err := s.LoadGraph(job.ctx)
		if err != nil {
			return nil, internalError(err)
		}
		report := graph.AnalyzeImpact(ImpactInput{
			BaseVersion:   targetVersion - 1,
			TargetVersion: targetVersion,
			ChangedKeys:   changedKeys,
			SeedTriggers:  seedTriggers,
		})
		_ = policy
		apiErr = job.Complete(func(tx *sql.Tx) error {
			return saveAffectedReportTx(job.ctx, tx, targetVersion, report, s.NowMS())
		}, report, targetVersion)
		if apiErr != nil {
			return nil, apiErr
		}
		return report, nil
	})
}

func (s *Service) GetAffectedReport(ctx context.Context, version int64) (*AffectedReport, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM affected_reports WHERE version=?`, version).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, NewAPIError(http.StatusNotFound, CodeNotFound, "affected report not found for version", map[string]int64{"version": version})
	}
	if err != nil {
		return nil, err
	}
	var report AffectedReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return nil, err
	}
	return &report, nil
}
