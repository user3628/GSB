package registry

import (
	"context"
	"database/sql"
	"sort"
	"time"
)

// buildDerived computes all projections from one immutable prospective state.
// Availability is deliberately conservative: only HEALTHY instances receive
// traffic weight. SUSPECT, UNKNOWN, UNHEALTHY and DRAINING are excluded.
func buildDerived(revision int64, instances []RuntimeInstance, nowMS int64) DerivedView {
	sorted := append([]RuntimeInstance(nil), instances...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ServiceName != sorted[j].ServiceName {
			return sorted[i].ServiceName < sorted[j].ServiceName
		}
		return sorted[i].InstanceID < sorted[j].InstanceID
	})

	view := DerivedView{Revision: revision, Status: "COMPLETE"}
	byService := make(map[string][]RuntimeInstance)
	for _, in := range sorted {
		byService[in.ServiceName] = append(byService[in.ServiceName], in)
		expires := int64(0)
		remaining := int64(0)
		if in.LastHeartbeatMS > 0 {
			expires = in.LastHeartbeatMS + in.LeaseTTLMS
			remaining = expires - nowMS
			if remaining < 0 {
				remaining = 0
			}
		}
		view.ProbePlans = append(view.ProbePlans, ProbePlan{
			Revision: revision, ServiceName: in.ServiceName, InstanceID: in.InstanceID,
			Endpoint: in.Endpoint, Probe: in.Probe, Priority: in.Priority, LeaseTTLMS: in.LeaseTTLMS,
			LeaseExpiresAtMS: expires, LeaseRemainingMS: remaining,
			ConfigFingerprint: in.ConfigFingerprint, HealthState: in.State,
		})
	}

	services := make([]string, 0, len(byService))
	for service := range byService {
		services = append(services, service)
	}
	sort.Strings(services)
	for _, service := range services {
		group := byService[service]
		summary := HealthSummary{Revision: revision, ServiceName: service, Total: len(group)}
		for _, in := range group {
			summary.TotalWeight += int64(in.Weight)
			switch in.State {
			case StateHealthy:
				summary.Healthy++
			case StateSuspect:
				summary.Suspect++
			case StateUnhealthy:
				summary.Unhealthy++
			case StateDraining:
				summary.Draining++
			default:
				summary.Unknown++
			}
			if in.State == StateHealthy {
				summary.Available++
				summary.AvailableWeight += int64(in.Weight)
			}
		}
		view.HealthSummary = append(view.HealthSummary, summary)

		var availableWeight int64
		for _, in := range group {
			if in.State == StateHealthy {
				availableWeight += int64(in.Weight)
			}
		}
		if availableWeight > 0 {
			type remainder struct {
				index int
				value int64
			}
			shares := make([]int64, len(group))
			rems := make([]remainder, 0, len(group))
			var allocated int64
			for i, in := range group {
				if in.State != StateHealthy {
					continue
				}
				numerator := int64(in.Weight) * 1_000_000
				shares[i] = numerator / availableWeight
				rems = append(rems, remainder{index: i, value: numerator % availableWeight})
				allocated += shares[i]
			}
			sort.Slice(rems, func(i, j int) bool {
				if rems[i].value != rems[j].value {
					return rems[i].value > rems[j].value
				}
				left, right := group[rems[i].index], group[rems[j].index]
				if left.ServiceName != right.ServiceName {
					return left.ServiceName < right.ServiceName
				}
				return left.InstanceID < right.InstanceID
			})
			for i := 0; i < int(1_000_000-allocated) && i < len(rems); i++ {
				shares[rems[i].index]++
			}
			for i, in := range group {
				view.Weights = append(view.Weights, WeightAllocation{
					Revision: revision, ServiceName: service, InstanceID: in.InstanceID,
					Weight: in.Weight, Available: in.State == StateHealthy, SharePPM: shares[i],
				})
			}
		} else {
			for _, in := range group {
				view.Weights = append(view.Weights, WeightAllocation{
					Revision: revision, ServiceName: service, InstanceID: in.InstanceID,
					Weight: in.Weight, Available: false, SharePPM: 0,
				})
			}
		}
	}
	return view
}

func stageDerived(ctx context.Context, tx *sql.Tx, view DerivedView, basedOnVersion int64, reason string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO derived_revisions(revision,based_on_version,status,reason,created_at)
		VALUES(?,?,?,?,?)`, view.Revision, basedOnVersion, "STAGING", reason, nowText()); err != nil {
		return err
	}
	for _, h := range view.HealthSummary {
		if _, err := tx.ExecContext(ctx, `INSERT INTO health_summary(
			revision,service_name,total,healthy,suspect,unhealthy,unknown_count,draining,
			available_count,available_weight,total_weight) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			view.Revision, h.ServiceName, h.Total, h.Healthy, h.Suspect, h.Unhealthy, h.Unknown,
			h.Draining, h.Available, h.AvailableWeight, h.TotalWeight); err != nil {
			return err
		}
	}
	for _, w := range view.Weights {
		available := 0
		if w.Available {
			available = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO weight_allocations(
			revision,service_name,instance_id,weight,available,share_ppm) VALUES(?,?,?,?,?,?)`,
			w.Revision, w.ServiceName, w.InstanceID, w.Weight, available, w.SharePPM); err != nil {
			return err
		}
	}
	for _, p := range view.ProbePlans {
		if _, err := tx.ExecContext(ctx, `INSERT INTO probe_plans(
			revision,service_name,instance_id,endpoint,probe_kind,probe_path,expected_status,
			expected_marker,probe_timeout_ms,priority,lease_ttl_ms,lease_expires_at_ms,
			lease_remaining_ms,config_fingerprint,health_state)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			p.Revision, p.ServiceName, p.InstanceID, p.Endpoint, string(p.Probe.Kind), p.Probe.Path,
			p.Probe.ExpectedStatus, p.Probe.ExpectedMarker, p.Probe.TimeoutMS, p.Priority,
			p.LeaseTTLMS, p.LeaseExpiresAtMS, p.LeaseRemainingMS, p.ConfigFingerprint, string(p.HealthState)); err != nil {
			return err
		}
	}
	return nil
}

func activateDerived(ctx context.Context, tx *sql.Tx, view DerivedView, snapshotVersion int64) error {
	if _, err := tx.ExecContext(ctx, `UPDATE derived_revisions SET status='COMPLETE' WHERE revision=?`, view.Revision); err != nil {
		return err
	}
	if err := setMeta(ctx, tx, "current_derived_revision", view.Revision); err != nil {
		return err
	}
	if snapshotVersion >= 0 {
		if err := setMeta(ctx, tx, "current_version", snapshotVersion); err != nil {
			return err
		}
	}
	return nil
}

func loadDerivedView(ctx context.Context, q queryer, revision int64) (DerivedView, error) {
	view := DerivedView{Revision: revision}
	if revision == 0 {
		view.Status = "COMPLETE"
		return view, nil
	}
	if err := q.QueryRowContext(ctx, `SELECT based_on_version,status FROM derived_revisions WHERE revision=?`, revision).
		Scan(&view.SnapshotVersion, &view.Status); err != nil {
		return view, normalizeSQLErr(err)
	}
	rows, err := q.QueryContext(ctx, `SELECT revision,service_name,total,healthy,suspect,unhealthy,
		unknown_count,draining,available_count,available_weight,total_weight
		FROM health_summary WHERE revision=? ORDER BY service_name`, revision)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		var h HealthSummary
		if err := rows.Scan(&h.Revision, &h.ServiceName, &h.Total, &h.Healthy, &h.Suspect,
			&h.Unhealthy, &h.Unknown, &h.Draining, &h.Available, &h.AvailableWeight, &h.TotalWeight); err != nil {
			rows.Close()
			return view, err
		}
		view.HealthSummary = append(view.HealthSummary, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return view, err
	}
	rows, err = q.QueryContext(ctx, `SELECT revision,service_name,instance_id,weight,available,share_ppm
		FROM weight_allocations WHERE revision=? ORDER BY service_name,instance_id`, revision)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		var w WeightAllocation
		var available int
		if err := rows.Scan(&w.Revision, &w.ServiceName, &w.InstanceID, &w.Weight, &available, &w.SharePPM); err != nil {
			rows.Close()
			return view, err
		}
		w.Available = available == 1
		view.Weights = append(view.Weights, w)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return view, err
	}
	rows, err = q.QueryContext(ctx, `SELECT revision,service_name,instance_id,endpoint,probe_kind,
		probe_path,expected_status,expected_marker,probe_timeout_ms,priority,lease_ttl_ms,
		lease_expires_at_ms,lease_remaining_ms,config_fingerprint,health_state
		FROM probe_plans WHERE revision=? ORDER BY service_name,instance_id`, revision)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		var p ProbePlan
		var kind, state string
		if err := rows.Scan(&p.Revision, &p.ServiceName, &p.InstanceID, &p.Endpoint, &kind,
			&p.Probe.Path, &p.Probe.ExpectedStatus, &p.Probe.ExpectedMarker, &p.Probe.TimeoutMS,
			&p.Priority, &p.LeaseTTLMS, &p.LeaseExpiresAtMS, &p.LeaseRemainingMS,
			&p.ConfigFingerprint, &state); err != nil {
			rows.Close()
			return view, err
		}
		p.Probe.Kind = ProbeKind(kind)
		p.HealthState = State(state)
		view.ProbePlans = append(view.ProbePlans, p)
	}
	rows.Close()
	return view, rows.Err()
}

func nowMillis() int64 { return time.Now().UnixMilli() }
