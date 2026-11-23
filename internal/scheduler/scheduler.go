// Package scheduler runs independent parser sessions with bounded concurrency,
// an explicit waiting queue and a per-session event budget. Session state is
// isolated: each source gets its own Result and shares no mutable parser state.
package scheduler

import (
	"sort"
	"sync"

	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/normalizer"
)

// Source is one input file with an explicit format.
type Source struct {
	Path   string
	Format string // codes.SourceBinary / SourceJSONL / SourceText
	Data   []byte
}

// Config controls resource limits.
type Config struct {
	Concurrency int   // max simultaneously active parser sessions
	EventBudget int64 // max accepted events per session; <=0 means unlimited
}

// AggCount is one source-path x business-code aggregation row.
type AggCount struct {
	SourcePath string `json:"source_path"`
	Code       string `json:"code"`
	Count      int    `json:"count"`
}

// WaitEntry records a source that could not start immediately.
type WaitEntry struct {
	SourcePath string `json:"source_path"`
	Queued     bool   `json:"queued"`
}

// Outcome is the deterministic, aggregated result of a full run.
type Outcome struct {
	Results     []*normalizer.Result `json:"results"`
	Aggregates  []AggCount           `json:"aggregates"`
	WaitQueue   []WaitEntry          `json:"wait_queue"`
	MaxParallel int                  `json:"max_parallel_observed"`
}

// Run executes all sources. Determinism note: aggregation and result ordering
// are sorted before return, so concurrency cannot change the output bytes.
func Run(sources []Source, cfg Config) *Outcome {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	var (
		mu      sync.Mutex
		results = make([]*normalizer.Result, len(sources))
		queue   []WaitEntry
		peak    int
		wg      sync.WaitGroup
	)
	sem := make(chan struct{}, cfg.Concurrency)

	for i, src := range sources {
		// Dispatch order determines queuing deterministically: the first
		// Concurrency sources fill the slots, every later source is enqueued.
		queued := i >= cfg.Concurrency
		if queued {
			queue = append(queue, WaitEntry{SourcePath: src.Path, Queued: true})
		}
		wg.Add(1)

		go func(idx int, s Source) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			mu.Lock()
			using := len(sem)
			if using > peak {
				peak = using
			}
			mu.Unlock()
			budget := func(accepted int) bool {
				return cfg.EventBudget > 0 && int64(accepted) >= cfg.EventBudget
			}
			var res *normalizer.Result
			switch s.Format {
			case codes.SourceBinary:
				res = normalizer.ParseBinary(s.Path, s.Data, budget)
			case codes.SourceJSONL:
				res = normalizer.ParseJSONL(s.Path, s.Data, budget)
			default:
				res = normalizer.ParseText(s.Path, s.Data, budget)
			}
			mu.Lock()
			results[idx] = res
			mu.Unlock()
		}(i, src)
	}
	wg.Wait()

	out := &Outcome{Results: results, MaxParallel: peak}
	sort.Slice(out.WaitQueue, func(i, j int) bool { return out.WaitQueue[i].SourcePath < out.WaitQueue[j].SourcePath })
	out.WaitQueue = dedupeWait(queue)
	out.Aggregates = Reaggregate(results)
	return out
}

// Reaggregate recomputes source-path x business-code counts. It is exported so
// the pipeline can refresh totals after a malformed-input strategy removes
// tainted events.
func Reaggregate(results []*normalizer.Result) []AggCount {
	return aggregate(results)
}

func dedupeWait(in []WaitEntry) []WaitEntry {
	seen := map[string]bool{}
	var out []WaitEntry
	// Sort first for stable output.
	sort.Slice(in, func(i, j int) bool { return in[i].SourcePath < in[j].SourcePath })
	for _, w := range in {
		if seen[w.SourcePath] {
			continue
		}
		seen[w.SourcePath] = true
		out = append(out, w)
	}
	return out
}

func aggregate(results []*normalizer.Result) []AggCount {
	type key struct{ path, code string }
	counts := map[key]int{}
	for _, r := range results {
		if r == nil {
			continue
		}
		// Fatal header-level errors still count in the aggregation.
		if r.Fatal != nil {
			counts[key{r.SourcePath, r.Fatal.Code}]++
		}
		for _, e := range r.Errors {
			counts[key{r.SourcePath, e.Code}]++
		}
	}
	var out []AggCount
	for k, n := range counts {
		out = append(out, AggCount{SourcePath: k.path, Code: k.code, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourcePath != out[j].SourcePath {
			return out[i].SourcePath < out[j].SourcePath
		}
		return out[i].Code < out[j].Code
	})
	return out
}
