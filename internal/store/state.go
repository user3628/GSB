package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"example.com/gsb-router/internal/model"
	"example.com/gsb-router/internal/routing"
	_ "modernc.org/sqlite"
)

type runtimeState struct {
	version    int64
	snapshot   model.Snapshot
	tables     map[string]*model.LogicalTable
	caches     map[string]RouteCache
	aggregates map[string]AggregateCache
}

type Store struct {
	db       *sql.DB
	mu       sync.RWMutex
	state    *runtimeState
	commitMu sync.Mutex
	now      func() time.Time
	recovery *RecoveryReport
}

func Open(path string) (*Store, error) {
	dsn := path
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve sqlite path: %w", err)
		}
		slashPath := filepath.ToSlash(absolute)
		if !strings.HasPrefix(slashPath, "/") {
			slashPath = "/" + slashPath
		}
		uri := &url.URL{Scheme: "file", Path: slashPath}
		query := uri.Query()
		query.Add("_pragma", "busy_timeout(5000)")
		query.Add("_pragma", "journal_mode(WAL)")
		query.Add("_pragma", "synchronous(FULL)")
		query.Add("_pragma", "foreign_keys(ON)")
		uri.RawQuery = query.Encode()
		dsn = uri.String()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
	}
	store := &Store{db: db, now: time.Now}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.recover(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.loadState(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) loadState(ctx context.Context) error {
	var version int64
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT version, snapshot_json FROM snapshots ORDER BY version DESC LIMIT 1`).Scan(&version, &raw)
	if err == sql.ErrNoRows {
		s.mu.Lock()
		s.state = emptyState()
		s.mu.Unlock()
		return nil
	}
	if err != nil {
		return fmt.Errorf("load current snapshot: %w", err)
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return fmt.Errorf("decode current snapshot: %w", err)
	}
	caches, err := loadRouteCaches(ctx, s.db)
	if err != nil {
		return err
	}
	aggregates, err := loadAggregateCaches(ctx, s.db)
	if err != nil {
		return err
	}
	state := buildRuntimeState(snapshot, caches, aggregates)
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
	return nil
}

func emptyState() *runtimeState {
	return &runtimeState{
		tables:     map[string]*model.LogicalTable{},
		caches:     map[string]RouteCache{},
		aggregates: map[string]AggregateCache{},
	}
}

func buildRuntimeState(snapshot model.Snapshot, caches map[string]RouteCache, aggregates map[string]AggregateCache) *runtimeState {
	snapshot = model.CanonicalizeSnapshot(snapshot)
	tables := make(map[string]*model.LogicalTable, len(snapshot.LogicalTables))
	for i := range snapshot.LogicalTables {
		table := snapshot.LogicalTables[i]
		tables[table.ID] = &table
	}
	return &runtimeState{
		version:    snapshot.Version,
		snapshot:   snapshot,
		tables:     tables,
		caches:     caches,
		aggregates: aggregates,
	}
}

func (s *Store) currentVersion() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.version
}

func (s *Store) StateSummary() StateSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return summarizeState(s.state)
}

func summarizeState(state *runtimeState) StateSummary {
	summary := StateSummary{
		CurrentVersion: state.version,
		Tables:         []TableState{},
		Aggregates:     []AggregateCache{},
		CacheAligned:   true,
	}
	if state.version == 0 {
		return summary
	}
	snapshot := state.snapshot.Clone()
	summary.Snapshot = &snapshot
	summary.PhysicalTables = len(snapshot.PhysicalTables)
	dbs := map[string]struct{}{}
	for _, table := range snapshot.LogicalTables {
		dbs[table.LogicalDB] = struct{}{}
		physicals := make([]string, 0, len(table.Ranges))
		seen := map[string]struct{}{}
		for _, shard := range table.Ranges {
			if _, ok := seen[shard.PhysicalTable]; !ok {
				seen[shard.PhysicalTable] = struct{}{}
				physicals = append(physicals, shard.PhysicalTable)
			}
		}
		sort.Strings(physicals)
		cache, ok := state.caches[table.ID]
		if !ok || cache.ExpectedVersion > cache.CacheVersion {
			summary.CacheAligned = false
		}
		summary.Tables = append(summary.Tables, TableState{
			TableID:      table.ID,
			LogicalDB:    table.LogicalDB,
			ShardKey:     table.ShardKey,
			Algorithm:    table.Algorithm.Type,
			RangeCount:   len(table.Ranges),
			Physicals:    physicals,
			CacheVersion: cache.CacheVersion,
			Expected:     cache.ExpectedVersion,
		})
	}
	for _, aggregate := range state.aggregates {
		summary.Aggregates = append(summary.Aggregates, aggregate)
	}
	sort.Slice(summary.Aggregates, func(i, j int) bool { return summary.Aggregates[i].ViewID < summary.Aggregates[j].ViewID })
	sort.Slice(summary.Tables, func(i, j int) bool { return summary.Tables[i].TableID < summary.Tables[j].TableID })
	summary.LogicalDBCount = len(dbs)
	return summary
}

func (s *Store) Route(query routing.Query) (RouteResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := s.state
	if state.version == 0 {
		return RouteResponse{}, errsTableNotFound(query.TableID)
	}
	table, ok := state.tables[query.TableID]
	if !ok {
		return RouteResponse{}, errsTableNotFound(query.TableID)
	}
	cache, ok := state.caches[query.TableID]
	if !ok {
		return RouteResponse{}, cacheInconsistent(query.TableID, "route cache row is missing")
	}
	if query.Observed > state.version {
		return RouteResponse{}, observedAhead(query.TableID, query.Observed, state.version)
	}
	if query.Observed < state.version {
		return RouteResponse{}, staleRead(query.TableID, cache.CacheVersion, cache.ExpectedVersion, query.Observed, state.version, "observed_version_replaced")
	}
	if cache.ExpectedVersion > cache.CacheVersion {
		return RouteResponse{}, staleRead(query.TableID, cache.CacheVersion, cache.ExpectedVersion, query.Observed, state.version, "cache_propagation_incomplete")
	}
	result, err := routing.Compute(table, query)
	if err != nil {
		return RouteResponse{}, err
	}
	return RouteResponse{
		Result:          result,
		ObservedVersion: query.Observed,
		CurrentVersion:  state.version,
		CacheVersion:    cache.CacheVersion,
		ExpectedVersion: cache.ExpectedVersion,
		UsedVersion:     cache.CacheVersion,
		VersionBound:    true,
		Deterministic:   true,
	}, nil
}
