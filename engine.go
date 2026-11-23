package dnsdetect

import (
	"context"
	"sort"
	"sync"
)

type RunResult struct {
	Report       *Report       `json:"report"`
	Adjudication *Adjudication `json:"-"`
	Snapshot     *Snapshot     `json:"-"`
	Batch        *SourceBatch  `json:"-"`
}

type Engine struct {
	config Config
	store  *SnapshotStore

	statsMu   sync.RWMutex
	lastStats SchedulerStats
	active    *Scheduler
}

func NewEngine(config Config, store *SnapshotStore) *Engine {
	config = config.WithDefaults()
	if config.SourcePriorities == nil {
		config.SourcePriorities = DefaultConfig().SourcePriorities
	}
	return &Engine{config: config, store: store}
}

func (e *Engine) Config() Config { return e.config.WithDefaults() }

func (e *Engine) SchedulerStats() SchedulerStats {
	e.statsMu.RLock()
	active := e.active
	last := e.lastStats
	e.statsMu.RUnlock()
	if active != nil {
		return active.Stats()
	}
	return last
}

// Process loads sources concurrently in isolated sessions, merges and
// adjudicates deterministically, then atomically commits one immutable
// snapshot. Any source or adjudication error returns before snapshot commit.
func (e *Engine) Process(ctx context.Context, specs []SourceSpec) (*RunResult, error) {
	config := e.config.WithDefaults()
	loadOptions := LoadOptions{
		MaxEvents:         config.MaxEventsPerSession,
		MaxSpanTicks:      config.MaxSpanTicksPerSession,
		WindowSpanTicks:   config.WindowSpanTicks,
		WindowAnchorTicks: config.WindowAnchorTicks,
		SourcePriorities:  config.SourcePriorities,
	}
	scheduler := NewScheduler(config.MaxConcurrency, loadOptions)
	e.statsMu.Lock()
	e.active = scheduler
	e.statsMu.Unlock()
	batch := scheduler.Load(ctx, specs)
	e.statsMu.Lock()
	e.lastStats = scheduler.Stats()
	e.active = nil
	e.statsMu.Unlock()
	if err := batch.FirstError(); err != nil {
		return nil, err
	}
	adjudication, err := Adjudicate(batch.Events, config)
	if err != nil {
		return nil, err
	}
	ledger := MergeLedgers([]ErrorLedger{batch.Ledger})
	formatVersions := make([]int, 0, len(batch.Sessions))
	for _, session := range batch.Sessions {
		if session.FormatVersion > 0 {
			formatVersions = append(formatVersions, session.FormatVersion)
		}
	}
	report := buildReport(config, batch.Sessions, ledger, adjudication, formatVersions)
	if e.store == nil {
		return &RunResult{Report: report, Adjudication: adjudication, Batch: batch}, nil
	}
	content, err := report.CanonicalBytes()
	if err != nil {
		return nil, newBusinessError(CodeSnapshotFormat, "", -1, "cannot encode snapshot content", err)
	}
	ledgerDigest, err := ledger.Digest()
	if err != nil {
		return nil, newBusinessError(CodeSnapshotFormat, "", -1, "cannot digest error ledger", err)
	}
	priorities := make([]SourcePriority, 0, len(config.SourcePriorities))
	for kind, priority := range config.SourcePriorities {
		priorities = append(priorities, SourcePriority{Kind: kind, Priority: priority})
	}
	createdTicks := config.WindowAnchorTicks
	if len(batch.Events) > 0 {
		createdTicks = batch.Events[0].TimeTicks
		for _, event := range batch.Events[1:] {
			if event.TimeTicks > createdTicks {
				createdTicks = event.TimeTicks
			}
		}
	}
	snapshot, err := e.store.Commit(ctx, SnapshotDraft{
		CreatedTicks:     createdTicks,
		FormatVersions:   formatVersions,
		SourcePriorities: priorities,
		LedgerDigest:     ledgerDigest,
		WindowProgress:   adjudication.Windows,
		Content:          content,
	})
	if err != nil {
		return nil, err
	}
	report.Snapshot = &SnapshotInfo{
		Version:           snapshot.Version,
		CreatedTicks:      snapshot.CreatedTicks,
		ContentDigest:     snapshot.ContentDigest,
		PrevContentDigest: snapshot.PrevContentDigest,
		LedgerDigest:      snapshot.LedgerDigest,
	}
	return &RunResult{Report: report, Adjudication: adjudication, Snapshot: snapshot, Batch: batch}, nil
}

func sortedPriorities(priorities map[SourceKind]int) []SourcePriority {
	out := make([]SourcePriority, 0, len(priorities))
	for kind, priority := range priorities {
		out = append(out, SourcePriority{Kind: kind, Priority: priority})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}
