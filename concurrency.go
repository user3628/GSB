package cockpitaudio

import (
	"context"
	"sync"
)

type QueryFront struct {
	mu    sync.RWMutex
	ds    *Dataset
	cfg   Config
	edit  *EditState
	cache *PipelineCache
	svc   *QueryService
}

func NewQueryFront(ds *Dataset, cfg Config) (*QueryFront, *Error) {
	cache, result, err := NewPipelineCache(ds, cfg, nil)
	if err != nil {
		return nil, err
	}
	edit := cache.EditState()
	svc, qerr := NewQueryService(cfg, result)
	if qerr != nil {
		return nil, qerr
	}
	return &QueryFront{ds: ds, cfg: cfg.Clone(), edit: edit, cache: cache, svc: svc}, nil
}

func (f *QueryFront) Query(ctx context.Context, req QueryRequest) (*QueryResponse, *Error) {
	f.mu.RLock()
	svc := f.svc
	f.mu.RUnlock()
	return svc.Query(ctx, req)
}

func (f *QueryFront) ApplyEdit(op EditOp) (EditRecord, *Error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, err := f.edit.Apply(op)
	if err != nil {
		return EditRecord{}, err
	}
	result, _, rerr := f.cache.Refresh(f.cfg, f.edit)
	if rerr != nil {
		return record, rerr
	}
	svc, qerr := NewQueryService(f.cfg, result)
	if qerr != nil {
		return record, qerr
	}
	f.svc = svc
	return record, nil
}

func (f *QueryFront) EditVersion() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.edit.CurrentVersion()
}

type recordedQuery struct {
	Request  QueryRequest
	Response *QueryResponse
}

type ConcurrencyAudit struct {
	OK                 bool   `json:"ok"`
	QueriesChecked     int    `json:"queries_checked"`
	EditsApplied       int    `json:"edits_applied"`
	MaxObservedVersion int    `json:"max_observed_version"`
	UniqueVersions     []int  `json:"unique_versions"`
	FirstMismatch      string `json:"first_mismatch,omitempty"`
}

func RunConcurrentQueryAudit(ds *Dataset, cfg Config, ops []EditOp, requests []QueryRequest, repetitions int) (*ConcurrencyAudit, *Error) {
	if repetitions <= 0 {
		repetitions = 8
	}
	front, err := NewQueryFront(ds, cfg)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	var recordMu sync.Mutex
	records := []recordedQuery{}
	queryErrs := []*Error{}

	workerCount := 6
	tasks := make(chan QueryRequest)
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range tasks {
				resp, qerr := front.Query(context.Background(), req)
				recordMu.Lock()
				if qerr != nil {
					queryErrs = append(queryErrs, qerr)
				} else {
					records = append(records, recordedQuery{Request: req, Response: resp})
				}
				recordMu.Unlock()
			}
		}()
	}

	var editWG sync.WaitGroup
	editWG.Add(1)
	go func() {
		defer editWG.Done()
		for _, op := range ops {
			_, _ = front.ApplyEdit(op)
		}
	}()

	for r := 0; r < repetitions; r++ {
		for _, req := range requests {
			tasks <- req
		}
	}
	close(tasks)
	wg.Wait()
	editWG.Wait()

	if len(queryErrs) > 0 {
		return nil, queryErrs[0]
	}

	base, berr := BuildSession(ds, cfg, nil)
	if berr != nil {
		return nil, berr
	}
	baseIntervals := collectIntervals(base)
	fullCache := map[int]*SessionResult{0: base}
	versionSet := map[int]bool{0: true}
	maxVersion := 0

	audit := &ConcurrencyAudit{OK: true, QueriesChecked: len(records), EditsApplied: len(ops)}
	for _, rec := range records {
		v := rec.Response.EditVersion
		versionSet[v] = true
		if v > maxVersion {
			maxVersion = v
		}
		full, ok := fullCache[v]
		if !ok {
			edit := NewEditState(baseIntervals, cfg.Priorities)
			for i := 0; i < v && i < len(ops); i++ {
				if _, aerr := edit.Apply(ops[i]); aerr != nil {
					return audit, aerr
				}
			}
			built, berr := BuildSession(ds, cfg, edit)
			if berr != nil {
				return audit, berr
			}
			full = built
			fullCache[v] = full
		}
		expected, rerr := RecomputeQueryCanonical(cfg, full, rec.Request)
		if rerr != nil {
			return audit, rerr
		}
		if string(expected) != string(rec.Response.Result) {
			audit.OK = false
			audit.FirstMismatch = "query " + rec.Response.QueryID + " edit_version " + itoa(v)
			return audit, NewError(CodeIncrementalMismatch, StageQuery, "", audit.FirstMismatch)
		}
	}
	audit.MaxObservedVersion = maxVersion
	versions := make([]int, 0, len(versionSet))
	for v := range versionSet {
		versions = append(versions, v)
	}
	sortInts(versions)
	audit.UniqueVersions = versions
	return audit, nil
}

func sortInts(in []int) {
	for i := 1; i < len(in); i++ {
		for j := i; j > 0 && in[j] < in[j-1]; j-- {
			in[j], in[j-1] = in[j-1], in[j]
		}
	}
}
