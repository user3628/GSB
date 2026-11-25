package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"deployverify/internal/derive"
	"deployverify/internal/model"
)

func (s *Store) LoadSnapshot(ctx context.Context) (*model.Snapshot, error) {
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	if version == 0 {
		return &model.Snapshot{Version: 0, Environments: []model.Environment{}}, nil
	}
	return loadSnapshotTx(ctx, s.db, version)
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func loadSnapshotTx(ctx context.Context, q queryRower, version int64) (*model.Snapshot, error) {
	if version == 0 {
		return &model.Snapshot{Version: 0, Environments: []model.Environment{}}, nil
	}
	snapshot := &model.Snapshot{Version: version, Environments: []model.Environment{}}
	var generatedAt, checksum string
	err := q.QueryRowContext(ctx, `SELECT generated_at, checksum FROM snapshots WHERE version=?`, version).Scan(&generatedAt, &checksum)
	if err != nil {
		return nil, err
	}
	snapshot.Checksum = checksum

	envRows, err := q.QueryContext(ctx, `SELECT name, priority FROM environments ORDER BY priority ASC, name ASC`)
	if err != nil {
		return nil, err
	}
	type envRow struct {
		env     model.Environment
		unitMap map[string]int
	}
	envData := map[string]*envRow{}
	envNames := []string{}
	for envRows.Next() {
		var name string
		var priority int
		if err := envRows.Scan(&name, &priority); err != nil {
			envRows.Close()
			return nil, err
		}
		item := &envRow{env: model.Environment{Name: name, Priority: priority, Units: []model.Unit{}}, unitMap: map[string]int{}}
		envData[name] = item
		envNames = append(envNames, name)
	}
	envRows.Close()
	if err := envRows.Err(); err != nil {
		return nil, err
	}

	unitRows, err := q.QueryContext(ctx, `SELECT environment, service, image_digest, desired_version, probe_json FROM units ORDER BY service ASC`)
	if err != nil {
		return nil, err
	}
	for unitRows.Next() {
		var envName, service, digest, desired, probeJSON string
		if err := unitRows.Scan(&envName, &service, &digest, &desired, &probeJSON); err != nil {
			unitRows.Close()
			return nil, err
		}
		data, ok := envData[envName]
		if !ok {
			continue
		}
		unit := model.Unit{Service: service, ImageDigest: digest, DesiredVersion: desired, Config: map[string]string{}, DependsOn: []string{}}
		if probeJSON != "" && probeJSON != "{}" {
			var probe model.ProbeSpec
			if err := json.Unmarshal([]byte(probeJSON), &probe); err == nil {
				unit.Probe = &probe
			}
		}
		data.unitMap[service] = len(data.env.Units)
		data.env.Units = append(data.env.Units, unit)
	}
	unitRows.Close()
	if err := unitRows.Err(); err != nil {
		return nil, err
	}

	configRows, err := q.QueryContext(ctx, `SELECT environment, service, config_key, config_value FROM config_kv ORDER BY config_key ASC`)
	if err != nil {
		return nil, err
	}
	for configRows.Next() {
		var envName, service, key, value string
		if err := configRows.Scan(&envName, &service, &key, &value); err != nil {
			configRows.Close()
			return nil, err
		}
		if data, ok := envData[envName]; ok {
			if idx, ok := data.unitMap[service]; ok {
				data.env.Units[idx].Config[key] = value
			}
		}
	}
	configRows.Close()
	if err := configRows.Err(); err != nil {
		return nil, err
	}

	depRows, err := q.QueryContext(ctx, `SELECT environment, service, depends_on FROM dependencies ORDER BY ordinal ASC, depends_on ASC`)
	if err != nil {
		return nil, err
	}
	for depRows.Next() {
		var envName, service, dep string
		if err := depRows.Scan(&envName, &service, &dep); err != nil {
			depRows.Close()
			return nil, err
		}
		if data, ok := envData[envName]; ok {
			if idx, ok := data.unitMap[service]; ok {
				data.env.Units[idx].DependsOn = append(data.env.Units[idx].DependsOn, dep)
			}
		}
	}
	depRows.Close()
	if err := depRows.Err(); err != nil {
		return nil, err
	}

	for _, name := range envNames {
		snapshot.Environments = append(snapshot.Environments, envData[name].env)
	}
	return snapshot, nil
}

func (s *Store) LoadDerived(ctx context.Context) (*derive.Bundle, error) {
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	bundle := &derive.Bundle{Version: version, Plans: []derive.PlanItem{}, Topologies: []derive.Topology{}, Scores: []derive.Score{}, Refs: []derive.ArtifactRef{}}
	if version == 0 {
		return bundle, nil
	}
	planRows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM plans WHERE version=? ORDER BY environment ASC, layer ASC, service ASC`, version)
	if err != nil {
		return nil, err
	}
	defer planRows.Close()
	for planRows.Next() {
		var raw string
		if err := planRows.Scan(&raw); err != nil {
			return nil, err
		}
		var plan derive.PlanItem
		if err := json.Unmarshal([]byte(raw), &plan); err != nil {
			return nil, err
		}
		bundle.Plans = append(bundle.Plans, plan)
	}
	if err := planRows.Err(); err != nil {
		return nil, err
	}
	topoRows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM topologies WHERE version=? ORDER BY environment ASC`, version)
	if err != nil {
		return nil, err
	}
	defer topoRows.Close()
	for topoRows.Next() {
		var raw string
		if err := topoRows.Scan(&raw); err != nil {
			return nil, err
		}
		var topology derive.Topology
		if err := json.Unmarshal([]byte(raw), &topology); err != nil {
			return nil, err
		}
		bundle.Topologies = append(bundle.Topologies, topology)
	}
	if err := topoRows.Err(); err != nil {
		return nil, err
	}
	scoreRows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM scores WHERE version=? ORDER BY service ASC`, version)
	if err != nil {
		return nil, err
	}
	defer scoreRows.Close()
	for scoreRows.Next() {
		var raw string
		if err := scoreRows.Scan(&raw); err != nil {
			return nil, err
		}
		var score derive.Score
		if err := json.Unmarshal([]byte(raw), &score); err != nil {
			return nil, err
		}
		bundle.Scores = append(bundle.Scores, score)
	}
	if err := scoreRows.Err(); err != nil {
		return nil, err
	}
	refRows, err := s.db.QueryContext(ctx, `SELECT version, artifact_type, artifact_key, environment, service, unit_env, unit_service, config_key FROM artifact_refs WHERE version=? ORDER BY artifact_type, artifact_key, config_key`, version)
	if err != nil {
		return nil, err
	}
	defer refRows.Close()
	for refRows.Next() {
		var ref derive.ArtifactRef
		if err := refRows.Scan(&ref.Version, &ref.ArtifactType, &ref.ArtifactKey, &ref.Environment, &ref.Service, &ref.UnitEnv, &ref.UnitService, &ref.ConfigKey); err != nil {
			return nil, err
		}
		bundle.Refs = append(bundle.Refs, ref)
	}
	return bundle, refRows.Err()
}

type RefFilter struct {
	Environment string
	Service     string
	ConfigKey   string
	ArtifactKey string
}

func (s *Store) ListRefs(ctx context.Context, filter RefFilter) ([]derive.ArtifactRef, error) {
	query := `SELECT version, artifact_type, artifact_key, environment, service, unit_env, unit_service, config_key FROM artifact_refs WHERE 1=1`
	args := []any{}
	if filter.Environment != "" {
		query += ` AND unit_env=?`
		args = append(args, filter.Environment)
	}
	if filter.Service != "" {
		query += ` AND unit_service=?`
		args = append(args, filter.Service)
	}
	if filter.ConfigKey != "" {
		query += ` AND config_key=?`
		args = append(args, filter.ConfigKey)
	}
	if filter.ArtifactKey != "" {
		query += ` AND artifact_key=?`
		args = append(args, filter.ArtifactKey)
	}
	query += ` ORDER BY artifact_type, artifact_key, config_key`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := []derive.ArtifactRef{}
	for rows.Next() {
		var ref derive.ArtifactRef
		if err := rows.Scan(&ref.Version, &ref.ArtifactType, &ref.ArtifactKey, &ref.Environment, &ref.Service, &ref.UnitEnv, &ref.UnitService, &ref.ConfigKey); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func (s *Store) LatestCascade(ctx context.Context) (*derive.CascadeReport, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT payload_json FROM cascade_events ORDER BY id DESC LIMIT 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var report derive.CascadeReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func (s *Store) ListSnapshots(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version, kind, generated_at, checksum FROM snapshots ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var version int64
		var kind, generatedAt, checksum string
		if err := rows.Scan(&version, &kind, &generatedAt, &checksum); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"version": version, "kind": kind, "generatedAt": generatedAt, "checksum": checksum})
	}
	return out, rows.Err()
}
