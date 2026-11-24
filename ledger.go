package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
)

type RebuildRequest struct {
	CommitID  string   `json:"commit_id,omitempty"`
	RouteKeys []string `json:"route_keys,omitempty"`
	AsOfMS    int64    `json:"as_of_ms,omitempty"`
	Tolerance string   `json:"tolerance,omitempty"`
}

type RebuildResult struct {
	CommitID         string             `json:"commit_id"`
	ViewVersion      int64              `json:"view_version"`
	AsOfMS           int64              `json:"as_of_ms"`
	RoutesRebuilt    int                `json:"routes_rebuilt"`
	Views            []ConservationView `json:"views"`
	IdempotentReplay bool               `json:"idempotent_replay,omitempty"`
}

func (r *RebuildResult) MarkIdempotentReplay() {
	if r != nil {
		r.IdempotentReplay = true
	}
}

type VersionLedger struct {
	Version            string `json:"version"`
	EntryCount         int64  `json:"entry_count"`
	TotalRequestWeight int64  `json:"total_request_weight"`
}

type LedgerView struct {
	RouteKey        string          `json:"route_key"`
	ViewVersion     int64           `json:"view_version"`
	ObservedVersion int64           `json:"observed_version,omitempty"`
	EntryCount      int             `json:"entry_count"`
	TotalWeight     int64           `json:"total_request_weight"`
	Versions        []VersionLedger `json:"versions"`
	Entries         []LedgerEntry   `json:"entries"`
}

func (w *Workbench) SubmitSnapshot(ctx context.Context, snapshot WeightSnapshot) (*CommitResult, *Error) {
	if err := NormalizeSnapshot(&snapshot); err != nil {
		return nil, err
	}
	return executeCommit[CommitResult](ctx, w, snapshot.CommitID, snapshot, commitOptions{
		Kind: "snapshot", IncrementView: true,
	}, func(wc *writeContext) (*CommitResult, *Error) {
		currentSnapshot, err := txMetaInt(wc.Ctx, wc.Tx, "snapshot_version")
		if err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		expected := currentSnapshot + 1
		if snapshot.SnapshotVersion != expected {
			return nil, NewError(CodeVersionGap, "snapshot version is not the next committed version").
				WithContextMap(map[string]any{
					"current_snapshot_version":  currentSnapshot,
					"expected_snapshot_version": expected,
					"provided_snapshot_version": snapshot.SnapshotVersion,
				})
		}
		payload, _ := json.Marshal(snapshot)
		if _, err := wc.Tx.ExecContext(wc.Ctx, `
			INSERT INTO snapshots(snapshot_version,generated_at_ms,digest,payload_json,commit_id,view_version,created_at_ms)
			VALUES(?,?,?,?,?,?,?)`,
			snapshot.SnapshotVersion, snapshot.GeneratedAtMS, snapshot.Digest, string(payload),
			wc.CommitID, wc.NextView, wc.NowMS); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		if _, err := wc.Tx.ExecContext(wc.Ctx, `UPDATE meta SET value=? WHERE key='snapshot_version'`,
			itoa64(snapshot.SnapshotVersion)); err != nil {
			return nil, WrapError(CodeInternal, err)
		}

		routes := make([]string, 0, len(snapshot.Weights))
		for _, route := range sortedKeys(snapshot.Weights) {
			routes = append(routes, route)
			for _, version := range sortedKeys(snapshot.Weights[route]) {
				weight := snapshot.Weights[route][version]
				if _, err := wc.Tx.ExecContext(wc.Ctx, `
					INSERT INTO route_weights(view_version,snapshot_version,route_key,version,weight_permille,source,migration_id)
					VALUES(?,?,?,?,?,'snapshot','')`,
					wc.NextView, snapshot.SnapshotVersion, route, version, weight); err != nil {
					return nil, WrapError(CodeInternal, err)
				}
			}
		}
		if err := invalidateDerivedTx(wc.Ctx, wc.Tx, routes, "new_weight_snapshot", wc.NowMS); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		tolerance := w.defaultTolerance()
		if _, err := rebuildViewsTx(wc, snapshot.SnapshotVersion, wc.NowMS, tolerance, routes); err != nil {
			return nil, err
		}
		return &CommitResult{
			CommitID: wc.CommitID, Kind: "snapshot", ViewVersion: wc.NextView,
			SnapshotVersion: snapshot.SnapshotVersion,
		}, nil
	})
}

func (w *Workbench) SubmitEntryBatch(ctx context.Context, batch EntryBatch) (*CommitResult, *Error) {
	normalized, validationErr := NormalizeEntryBatch(&batch)
	if validationErr != nil {
		return nil, validationErr
	}
	request := EntryBatch{CommitID: batch.CommitID, Entries: normalized}
	return executeCommit[CommitResult](ctx, w, batch.CommitID, request, commitOptions{
		Kind: "entries", IncrementView: true,
	}, func(wc *writeContext) (*CommitResult, *Error) {
		snapshotVersion, err := txMetaInt(wc.Ctx, wc.Tx, "snapshot_version")
		if err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		for seq, entry := range normalized {
			candidatesJSON, _ := json.Marshal(entry.Candidates)
			weightsJSON, _ := json.Marshal(entry.Weights)
			if _, err := wc.Tx.ExecContext(wc.Ctx, `
				INSERT INTO ledger_entries(
					intent_id,commit_id,seq,timestamp_ms,route_key,client_id,hit_version,
				candidates_json,weights_json,request_weight,state,view_version,snapshot_version)
			VALUES(?,?,?,?,?,?,?,?,?,?, 'committed', ?,?)`,
				wc.IntentID, wc.CommitID, seq, entry.TimestampMS, entry.RouteKey, entry.ClientID,
				entry.HitVersion, string(candidatesJSON), string(weightsJSON), entry.RequestWeight,
				wc.NextView, snapshotVersion); err != nil {
				return nil, WrapError(CodeInternal, err)
			}
		}
		if err := copyRouteWeightsTx(wc.Ctx, wc.Tx, wc.CurrentView, wc.NextView, snapshotVersion); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		routes := routeSet(normalized)
		if err := invalidateDerivedTx(wc.Ctx, wc.Tx, routes, "new_ledger_entries", wc.NowMS); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		if _, err := rebuildViewsTx(wc, snapshotVersion, wc.NowMS, w.defaultTolerance(), nil); err != nil {
			return nil, err
		}
		return &CommitResult{
			CommitID: wc.CommitID, Kind: "entries", ViewVersion: wc.NextView, Accepted: len(normalized),
			SnapshotVersion: snapshotVersion,
		}, nil
	})
}

func (w *Workbench) SubmitMigration(ctx context.Context, fragment MigrationFragment) (*CommitResult, *Error) {
	if err := NormalizeMigration(&fragment); err != nil {
		return nil, err
	}
	return executeCommit[CommitResult](ctx, w, fragment.CommitID, fragment, commitOptions{
		Kind: "migration", IncrementView: true,
	}, func(wc *writeContext) (*CommitResult, *Error) {
		if fragment.BaseVersion != wc.CurrentView {
			return nil, NewError(CodeStaleBase, "migration base_version is not the current committed view version").
				WithContextMap(map[string]any{
					"migration_id":    fragment.MigrationID,
					"base_version":    fragment.BaseVersion,
					"current_version": wc.CurrentView,
					"view_version":    wc.CurrentView,
				})
		}
		snapshotVersion, err := txMetaInt(wc.Ctx, wc.Tx, "snapshot_version")
		if err != nil {
			return nil, WrapError(CodeInternal, err)
		}

		var existingCommit string
		err = wc.Tx.QueryRowContext(wc.Ctx, `SELECT commit_id FROM migrations WHERE migration_id=?`, fragment.MigrationID).Scan(&existingCommit)
		if err == nil {
			if existingCommit != wc.CommitID {
				return nil, NewError(CodeConflictingLedger, "migration_id already exists with another commit").
					WithContextMap(map[string]any{"migration_id": fragment.MigrationID, "existing_commit_id": existingCommit})
			}
		} else if !isNoRows(err) {
			return nil, WrapError(CodeInternal, err)
		}

		var fromWeight, toWeight int
		err = wc.Tx.QueryRowContext(wc.Ctx, `
			SELECT weight_permille FROM route_weights
			WHERE view_version=? AND route_key=? AND version=?`,
			wc.CurrentView, fragment.RouteKey, fragment.FromVersion).Scan(&fromWeight)
		if isNoRows(err) {
			return nil, NewError(CodeLedgerInvalid, "from_version has no committed route weight").
				WithContextMap(map[string]any{"route_key": fragment.RouteKey, "version": fragment.FromVersion, "view_version": wc.CurrentView})
		}
		if err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		err = wc.Tx.QueryRowContext(wc.Ctx, `
			SELECT weight_permille FROM route_weights
			WHERE view_version=? AND route_key=? AND version=?`,
			wc.CurrentView, fragment.RouteKey, fragment.ToVersion).Scan(&toWeight)
		if isNoRows(err) {
			return nil, NewError(CodeLedgerInvalid, "to_version is not a candidate in committed route weights").
				WithContextMap(map[string]any{"route_key": fragment.RouteKey, "version": fragment.ToVersion, "view_version": wc.CurrentView})
		}
		if err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		if fragment.RatioPermille > fromWeight {
			return nil, NewError(CodeMigrationRatioInvalid, "migration ratio exceeds source version weight").
				WithContextMap(map[string]any{
					"migration_id":           fragment.MigrationID,
					"ratio_permille":         fragment.RatioPermille,
					"source_weight_permille": fromWeight,
				})
		}

		if raceErr := checkMigrationRaceTx(wc, fragment); raceErr != nil {
			return nil, raceErr
		}

		if err := copyRouteWeightsTx(wc.Ctx, wc.Tx, wc.CurrentView, wc.NextView, snapshotVersion); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		duringFrom := fromWeight - fragment.RatioPermille
		duringTo := toWeight + fragment.RatioPermille
		for _, item := range []struct {
			version string
			weight  int
		}{{fragment.FromVersion, duringFrom}, {fragment.ToVersion, duringTo}} {
			if _, err := wc.Tx.ExecContext(wc.Ctx, `
				UPDATE route_weights SET weight_permille=?,source='migration',migration_id=?
				WHERE view_version=? AND route_key=? AND version=?`,
				item.weight, fragment.MigrationID, wc.NextView, fragment.RouteKey, item.version); err != nil {
				return nil, WrapError(CodeInternal, err)
			}
		}

		endAt := fragment.StartAtMS + fragment.TransitionMS
		payload, _ := json.Marshal(fragment)
		if _, err := wc.Tx.ExecContext(wc.Ctx, `
			INSERT INTO migrations(
				migration_id,commit_id,base_version,view_version,route_key,from_version,to_version,
				ratio_permille,start_at_ms,transition_ms,end_at_ms,status,payload_json,created_at_ms)
			VALUES(?,?,?,?,?,?,?,?,?,?,?, 'committed', ?,?)`,
			fragment.MigrationID, wc.CommitID, fragment.BaseVersion, wc.NextView, fragment.RouteKey,
			fragment.FromVersion, fragment.ToVersion, fragment.RatioPermille, fragment.StartAtMS,
			fragment.TransitionMS, endAt, string(payload), wc.NowMS); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		if err := invalidateDerivedTx(wc.Ctx, wc.Tx, []string{fragment.RouteKey}, "migration_ratio_change", wc.NowMS); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		if _, err := rebuildViewsTx(wc, snapshotVersion, fragment.StartAtMS, w.defaultTolerance(), []string{fragment.RouteKey}); err != nil {
			return nil, err
		}
		return &CommitResult{CommitID: wc.CommitID, Kind: "migration", ViewVersion: wc.NextView}, nil
	})
}

func (w *Workbench) Rebuild(ctx context.Context, request RebuildRequest) (*RebuildResult, *Error) {
	tolerance, err := ParseTolerance(request.Tolerance)
	if err != nil {
		return nil, WrapError(CodeLedgerInvalid, err)
	}
	commitID := request.CommitID
	if commitID == "" {
		commitID = fmt.Sprintf("rebuild:%d:%d:%s", w.currentViewVersionOrZero(ctx), request.AsOfMS, canonicalHash(request.RouteKeys))
	}
	return executeCommit[RebuildResult](ctx, w, commitID, request, commitOptions{
		Kind: "rebuild", IncrementView: false,
	}, func(wc *writeContext) (*RebuildResult, *Error) {
		asOf := request.AsOfMS
		if asOf == 0 {
			asOf = wc.NowMS
		}
		snapshotVersion, err := txMetaInt(wc.Ctx, wc.Tx, "snapshot_version")
		if err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		routes := request.RouteKeys
		if len(routes) == 0 {
			routes, err = knownRoutesTx(wc.Ctx, wc.Tx, wc.NextView)
			if err != nil {
				return nil, WrapError(CodeInternal, err)
			}
		}
		views, bizErr := rebuildViewsTx(wc, snapshotVersion, asOf, tolerance, routes)
		if bizErr != nil {
			return nil, bizErr
		}
		return &RebuildResult{
			CommitID: wc.CommitID, ViewVersion: wc.NextView, AsOfMS: asOf,
			RoutesRebuilt: len(views), Views: views,
		}, nil
	})
}

func (w *Workbench) GetConservation(ctx context.Context, routeKey string, observedVersion, asOfMS int64, toleranceText string) (*ConservationView, *Error) {
	current, bizErr := checkObservedVersion(ctx, w.db, observedVersion)
	if bizErr != nil {
		return nil, bizErr
	}
	tolerance, err := ParseTolerance(toleranceText)
	if err != nil {
		return nil, WrapError(CodeLedgerInvalid, err)
	}
	if asOfMS == 0 {
		asOfMS = w.NowMS()
	}
	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	defer func() { _ = tx.Rollback() }()
	snapshotVersion, err := txMetaInt(ctx, tx, "snapshot_version")
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	known, err := routeKnownTx(ctx, tx, current, routeKey)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	if !known {
		return nil, NewError(CodeNotFound, "route key not found").WithContext("route_key", routeKey)
	}
	view, bizErr := buildViewTx(ctx, tx, routeKey, current, snapshotVersion, asOfMS, tolerance)
	if bizErr != nil {
		return nil, bizErr
	}
	return view, nil
}

func (w *Workbench) VerifyConservation(ctx context.Context, routeKey string, observedVersion, asOfMS int64, tolerance string) (*ConservationView, *Error) {
	view, err := w.GetConservation(ctx, routeKey, observedVersion, asOfMS, tolerance)
	if err != nil {
		return nil, err
	}
	if !view.Balanced {
		violations := []VersionConservation{}
		for _, version := range view.Versions {
			if !version.WithinTolerance {
				violations = append(violations, version)
			}
		}
		first := violations[0]
		return view, NewError(CodeWeightImbalance, "actual weight ratio deviates from declared candidate ratio beyond tolerance").
			WithContextMap(map[string]any{
				"route_key":        routeKey,
				"version":          first.Version,
				"expected_ratio":   first.ExpectedRatio,
				"actual_ratio":     first.ActualRatio,
				"abs_deviation":    first.AbsDeviation,
				"tolerance":        view.Tolerance,
				"view_version":     view.ViewVersion,
				"observed_version": observedVersion,
				"violations":       violations,
			})
	}
	return view, nil
}

func (w *Workbench) GetLedger(ctx context.Context, routeKey string, observedVersion int64) (*LedgerView, *Error) {
	current, bizErr := checkObservedVersion(ctx, w.db, observedVersion)
	if bizErr != nil {
		return nil, bizErr
	}
	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	defer func() { _ = tx.Rollback() }()
	known, err := routeKnownTx(ctx, tx, current, routeKey)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	if !known {
		return nil, NewError(CodeNotFound, "route key not found").WithContext("route_key", routeKey)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT timestamp_ms,route_key,client_id,hit_version,candidates_json,weights_json,request_weight,snapshot_version
		FROM ledger_entries WHERE route_key=? AND state='committed'
		ORDER BY timestamp_ms,id`, routeKey)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	defer rows.Close()
	entries := []LedgerEntry{}
	totals := map[string]*VersionLedger{}
	var totalWeight int64
	for rows.Next() {
		var entry LedgerEntry
		var candidatesJSON, weightsJSON string
		if err := rows.Scan(&entry.TimestampMS, &entry.RouteKey, &entry.ClientID, &entry.HitVersion,
			&candidatesJSON, &weightsJSON, &entry.RequestWeight, &entry.SnapshotVersion); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		_ = json.Unmarshal([]byte(candidatesJSON), &entry.Candidates)
		_ = json.Unmarshal([]byte(weightsJSON), &entry.Weights)
		entry.CandidateWeights = entry.Weights
		entries = append(entries, entry)
		totalWeight += entry.RequestWeight
		item := totals[entry.HitVersion]
		if item == nil {
			item = &VersionLedger{Version: entry.HitVersion}
			totals[entry.HitVersion] = item
		}
		item.EntryCount++
		item.TotalRequestWeight += entry.RequestWeight
	}
	versions := make([]VersionLedger, 0, len(totals))
	for _, key := range sortedKeys(totals) {
		versions = append(versions, *totals[key])
	}
	return &LedgerView{
		RouteKey: routeKey, ViewVersion: current, ObservedVersion: observedVersion,
		EntryCount: len(entries), TotalWeight: totalWeight, Versions: versions, Entries: entries,
	}, nil
}

func (w *Workbench) ListRoutes(ctx context.Context) ([]string, int64, *Error) {
	version, err := w.store.ViewVersion(ctx)
	if err != nil {
		return nil, 0, WrapError(CodeInternal, err)
	}
	rows, err := w.db.QueryContext(ctx, `
		SELECT DISTINCT route_key FROM (
			SELECT route_key FROM ledger_entries WHERE state='committed'
			UNION SELECT route_key FROM route_weights
			UNION SELECT route_key FROM migrations
		) ORDER BY route_key`)
	if err != nil {
		return nil, 0, WrapError(CodeInternal, err)
	}
	defer rows.Close()
	routes := []string{}
	for rows.Next() {
		var route string
		if err := rows.Scan(&route); err != nil {
			return nil, 0, WrapError(CodeInternal, err)
		}
		routes = append(routes, route)
	}
	return routes, version, nil
}

func (w *Workbench) defaultTolerance() *big.Rat {
	value := w.cfg.DefaultTolerance
	if value == "" {
		value = "0.01"
	}
	rat, _ := ParseTolerance(value)
	return rat
}

func (w *Workbench) currentViewVersionOrZero(ctx context.Context) int64 {
	version, err := w.store.ViewVersion(ctx)
	if err != nil {
		return 0
	}
	return version
}

func routeSet(entries []LedgerEntry) []string {
	set := map[string]struct{}{}
	for _, entry := range entries {
		set[entry.RouteKey] = struct{}{}
	}
	routes := make([]string, 0, len(set))
	for route := range set {
		routes = append(routes, route)
	}
	sort.Strings(routes)
	return routes
}

func itoa64(value int64) string {
	return fmtInt64(value)
}

func isNoRows(err error) bool { return err == sql.ErrNoRows }
