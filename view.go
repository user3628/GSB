package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/big"
	"sort"
	"strconv"
)

func fmtInt64(value int64) string { return strconv.FormatInt(value, 10) }

func copyRouteWeightsTx(ctx context.Context, tx *sql.Tx, fromVersion, toVersion, snapshotVersion int64) error {
	if fromVersion == toVersion {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO route_weights(view_version,snapshot_version,route_key,version,weight_permille,source,migration_id)
		SELECT ?,snapshot_version,route_key,version,weight_permille,source,migration_id
		FROM route_weights WHERE view_version=?`, toVersion, fromVersion)
	return err
}

func knownRoutesTx(ctx context.Context, tx *sql.Tx, viewVersion int64) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT route_key FROM (
			SELECT route_key FROM ledger_entries WHERE state='committed'
			UNION SELECT route_key FROM route_weights WHERE view_version<=?
			UNION SELECT route_key FROM migrations
		) ORDER BY route_key`, viewVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	routes := []string{}
	for rows.Next() {
		var route string
		if err := rows.Scan(&route); err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	return routes, rows.Err()
}

func routeKnownTx(ctx context.Context, tx *sql.Tx, viewVersion int64, routeKey string) (bool, error) {
	var count int
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM (
			SELECT 1 FROM ledger_entries WHERE route_key=? AND state='committed'
			UNION ALL
			SELECT 1 FROM route_weights WHERE route_key=? AND view_version<=?
			UNION ALL
			SELECT 1 FROM migrations WHERE route_key=?
		)`, routeKey, routeKey, viewVersion, routeKey).Scan(&count)
	return count > 0, err
}

func invalidateDerivedTx(ctx context.Context, tx *sql.Tx, routeKeys []string, reason string, atMS int64) error {
	for _, routeKey := range routeKeys {
		if _, err := tx.ExecContext(ctx, `
			UPDATE derived_lineage SET valid=0,invalidation_reason=?,updated_at_ms=?
			WHERE route_key=? AND valid=1`, reason, atMS, routeKey); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE probe_results SET is_current=0 WHERE route_key=? AND is_current=1`, routeKey); err != nil {
			return err
		}
	}
	return nil
}

func checkMigrationRaceTx(wc *writeContext, fragment MigrationFragment) *Error {
	rows, err := wc.Tx.QueryContext(wc.Ctx, `
		SELECT migration_id,ratio_permille,start_at_ms,end_at_ms
		FROM migrations
		WHERE route_key=? AND status='committed'`, fragment.RouteKey)
	if err != nil {
		return WrapError(CodeInternal, err)
	}
	defer rows.Close()
	for rows.Next() {
		var otherID string
		var otherRatio int
		var otherStart, otherEnd int64
		if err := rows.Scan(&otherID, &otherRatio, &otherStart, &otherEnd); err != nil {
			return WrapError(CodeInternal, err)
		}
		newEnd := fragment.StartAtMS + fragment.TransitionMS
		overlapStart := maxInt64(otherStart, fragment.StartAtMS)
		overlapEnd := minInt64(otherEnd, newEnd)
		if overlapStart < overlapEnd && otherRatio+fragment.RatioPermille != 1000 {
			return NewError(CodeMigrationRace, "overlapping migration fragments have incompatible ratios").
				WithContextMap(map[string]any{
					"route_key":          fragment.RouteKey,
					"migration_id":       fragment.MigrationID,
					"other_migration_id": otherID,
					"migration_ids":      []string{otherID, fragment.MigrationID},
					"ratios_permille":    map[string]int{otherID: otherRatio, fragment.MigrationID: fragment.RatioPermille},
					"overlap": map[string]any{
						"start_at_ms": overlapStart,
						"end_at_ms":   overlapEnd,
						"duration_ms": overlapEnd - overlapStart,
					},
				})
		}
	}
	return WrapError(CodeInternal, rows.Err())
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func effectiveWeightsTx(
	ctx context.Context, tx *sql.Tx, viewVersion int64, routeKey string, asOfMS int64,
) (map[string]int, string, string, error) {
	var effectiveViewValue sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT MAX(view_version) FROM route_weights
		WHERE route_key=? AND view_version<=?`, routeKey, viewVersion).Scan(&effectiveViewValue)
	if err != nil && !isNoRows(err) {
		return nil, "", "", err
	}
	effectiveView := effectiveViewValue.Int64
	if !effectiveViewValue.Valid {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM route_weights WHERE route_key=?`, routeKey).Scan(&count); err != nil {
			return nil, "", "", err
		}
		if count == 0 {
			return map[string]int{}, "", "", nil
		}
	}
	weights := map[string]int{}
	var migrationID sql.NullString
	rows, err := tx.QueryContext(ctx, `
		SELECT version,weight_permille,migration_id FROM route_weights
		WHERE route_key=? AND view_version=?`, routeKey, effectiveView)
	if err != nil {
		return nil, "", "", err
	}
	for rows.Next() {
		var version, mig string
		var weight int
		if err := rows.Scan(&version, &weight, &mig); err != nil {
			_ = rows.Close()
			return nil, "", "", err
		}
		weights[version] = weight
		if mig != "" {
			migrationID = sql.NullString{String: mig, Valid: true}
		}
	}
	if err := rows.Close(); err != nil {
		return nil, "", "", err
	}
	if !migrationID.Valid {
		return weights, "committed", "", nil
	}

	var record MigrationRecord
	var payload string
	var status string
	var committedView int64
	err = tx.QueryRowContext(ctx, `
		SELECT migration_id,base_version,view_version,route_key,from_version,to_version,ratio_permille,
			start_at_ms,transition_ms,end_at_ms,status,payload_json
		FROM migrations WHERE migration_id=?`, migrationID.String).Scan(
		&record.MigrationID, &record.BaseVersion, &committedView, &record.RouteKey,
		&record.FromVersion, &record.ToVersion, &record.RatioPermille, &record.StartAtMS,
		&record.TransitionMS, &record.EndAtMS, &status, &payload)
	if err != nil {
		return weights, "committed", migrationID.String, nil
	}

	switch {
	case asOfMS < record.StartAtMS:
		record.Phase = "pending"
		baseWeights := map[string]int{}
		baseRows, err := tx.QueryContext(ctx, `
			SELECT version,weight_permille FROM route_weights
			WHERE view_version=? AND route_key=?`, record.BaseVersion, routeKey)
		if err != nil {
			return nil, "", "", err
		}
		for baseRows.Next() {
			var version string
			var weight int
			if err := baseRows.Scan(&version, &weight); err != nil {
				_ = baseRows.Close()
				return nil, "", "", err
			}
			baseWeights[version] = weight
		}
		_ = baseRows.Close()
		if len(baseWeights) > 0 {
			weights = baseWeights
		}
	case asOfMS >= record.EndAtMS:
		record.Phase = "completed"
		fromWeight := weights[record.FromVersion]
		weights[record.FromVersion] = 0
		weights[record.ToVersion] += fromWeight
	default:
		record.Phase = "transitioning"
	}
	record.Status = status
	return weights, record.Phase, record.MigrationID, nil
}

func buildViewTx(
	ctx context.Context,
	tx *sql.Tx,
	routeKey string,
	viewVersion, snapshotVersion, asOfMS int64,
	tolerance *big.Rat,
) (*ConservationView, *Error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT hit_version,request_weight,weights_json
		FROM ledger_entries
		WHERE route_key=? AND state='committed'`, routeKey)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	type entryAggregate struct {
		count       int64
		hitWeight   int64
		expectedNum *big.Int
	}
	aggregates := map[string]*entryAggregate{}
	var totalWeight, entryCount int64

	getAgg := func(version string) *entryAggregate {
		item := aggregates[version]
		if item == nil {
			item = &entryAggregate{expectedNum: big.NewInt(0)}
			aggregates[version] = item
		}
		return item
	}

	for rows.Next() {
		var hitVersion, weightsJSON string
		var requestWeight int64
		if err := rows.Scan(&hitVersion, &requestWeight, &weightsJSON); err != nil {
			_ = rows.Close()
			return nil, WrapError(CodeInternal, err)
		}
		var weights map[string]int
		if err := json.Unmarshal([]byte(weightsJSON), &weights); err != nil {
			_ = rows.Close()
			return nil, NewError(CodeLedgerInvalid, "persisted weights_json is corrupt").WithContext("route_key", routeKey)
		}
		entryCount++
		totalWeight += requestWeight
		getAgg(hitVersion).count++
		getAgg(hitVersion).hitWeight += requestWeight
		for version, permille := range weights {
			agg := getAgg(version)
			agg.expectedNum.Add(agg.expectedNum, big.NewInt(requestWeight*int64(permille)))
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, WrapError(CodeInternal, err)
	}

	effectiveWeights, phase, migrationID, err := effectiveWeightsTx(ctx, tx, viewVersion, routeKey, asOfMS)
	if err != nil && !isNoRows(err) {
		return nil, WrapError(CodeInternal, err)
	}

	versionSet := map[string]struct{}{}
	for version := range aggregates {
		versionSet[version] = struct{}{}
	}
	for version := range effectiveWeights {
		versionSet[version] = struct{}{}
	}
	versions := sortedKeys(versionSet)

	view := &ConservationView{
		RouteKey: routeKey, ViewVersion: viewVersion, SnapshotVersion: snapshotVersion,
		AsOfMS: asOfMS, TotalRequestWeight: totalWeight, EntryCount: entryCount,
		HasData:   totalWeight > 0 || len(effectiveWeights) > 0,
		Tolerance: tolerance.FloatString(RatioDecimalPlaces),
		Rounding: RoundingInfo{
			DecimalPlaces: RatioDecimalPlaces, Mode: RoundingMode,
		},
		Versions: []VersionConservation{}, Balanced: true,
		EffectiveWeights: effectiveWeights, MigrationID: migrationID, MigrationPhase: phase,
	}

	den := new(big.Int).Mul(big.NewInt(totalWeight), big.NewInt(1000))
	if totalWeight <= 0 {
		den = big.NewInt(1)
	}
	expectedSumScaled := int64(0)
	actualSumScaled := int64(0)
	hasEffectiveWeights := len(effectiveWeights) > 0
	for _, version := range versions {
		agg := getAgg(version)
		actualScaled := RoundRatioScaled(big.NewInt(agg.hitWeight), big.NewInt(maxInt64(totalWeight, 1)))
		actualRat := big.NewRat(agg.hitWeight, maxInt64(totalWeight, 1))
		declaredRat := new(big.Rat).SetFrac(agg.expectedNum, den)
		expectedRat := declaredRat
		expectedScaled := RoundRatioScaled(agg.expectedNum, den)
		expectedRatio := BigFormatRatio(agg.expectedNum, den)
		effectivePermille := effectiveWeights[version]
		if hasEffectiveWeights {
			expectedRat = big.NewRat(int64(effectivePermille), 1000)
			expectedScaled = int64(effectivePermille) * (RatioScale / 1000)
			expectedRatio = FormatRatio(int64(effectivePermille), 1000)
		}
		expectedSumScaled += expectedScaled
		actualSumScaled += actualScaled

		deviation := new(big.Rat).Sub(actualRat, expectedRat)
		deviation.Abs(deviation)
		within := deviation.Cmp(tolerance) <= 0
		if !within {
			view.Balanced = false
		}
		view.Versions = append(view.Versions, VersionConservation{
			Version: version, EntryCount: agg.count, HitWeight: agg.hitWeight,
			ActualRatio:            FormatRatio(agg.hitWeight, maxInt64(totalWeight, 1)),
			ExpectedRatio:          expectedRatio,
			DeclaredExpectedRatio:  BigFormatRatio(agg.expectedNum, den),
			SnapshotWeightPermille: effectivePermille,
			SnapshotRatio:          FormatRatio(int64(effectivePermille), 1000),
			AbsDeviation:           BigFormatRatio(deviation.Num(), deviation.Denom()),
			WithinTolerance:        within,
		})
	}
	view.Rounding.ExpectedCumulativeError = FormatScaledRatio(expectedSumScaled - RatioScale)
	view.Rounding.ActualCumulativeError = FormatScaledRatio(actualSumScaled - RatioScale)
	view.ViewHash = computeViewHash(view)
	return view, nil
}

func computeViewHash(view *ConservationView) string {
	clone := *view
	clone.ViewHash = ""
	return canonicalHash(clone)
}

func rebuildViewsTx(
	wc *writeContext,
	snapshotVersion, asOfMS int64,
	tolerance *big.Rat,
	routeKeys []string,
) ([]ConservationView, *Error) {
	routes := routeKeys
	if len(routes) == 0 {
		var err error
		routes, err = knownRoutesTx(wc.Ctx, wc.Tx, wc.NextView)
		if err != nil {
			return nil, WrapError(CodeInternal, err)
		}
	}
	sort.Strings(routes)
	views := make([]ConservationView, 0, len(routes))
	for _, route := range routes {
		view, bizErr := buildViewTx(wc.Ctx, wc.Tx, route, wc.NextView, snapshotVersion, asOfMS, tolerance)
		if bizErr != nil {
			return nil, bizErr
		}
		viewJSON, _ := json.Marshal(view)
		balanced := 0
		if view.Balanced {
			balanced = 1
		}
		if _, err := wc.Tx.ExecContext(wc.Ctx, `
			INSERT OR REPLACE INTO route_views(
				view_version,route_key,as_of_ms,snapshot_version,balanced,view_hash,view_json)
			VALUES(?,?,?,?,?,?,?)`,
			wc.NextView, route, asOfMS, snapshotVersion, balanced, view.ViewHash, string(viewJSON)); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		if _, err := wc.Tx.ExecContext(wc.Ctx, `
			INSERT INTO derived_lineage(
				view_version,route_key,snapshot_version,ledger_entry_count,ledger_total_weight,
				conservation_hash,valid,invalidation_reason,updated_at_ms)
			VALUES(?,?,?,?,?,?,1,'',?)
			ON CONFLICT(view_version,route_key) DO UPDATE SET
				snapshot_version=excluded.snapshot_version,
				ledger_entry_count=excluded.ledger_entry_count,
				ledger_total_weight=excluded.ledger_total_weight,
				conservation_hash=excluded.conservation_hash,
				valid=1,invalidation_reason='',updated_at_ms=excluded.updated_at_ms`,
			wc.NextView, route, snapshotVersion, view.EntryCount, view.TotalRequestWeight,
			view.ViewHash, wc.NowMS); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		views = append(views, *view)
	}
	return views, nil
}
