package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// RepoOutcome couples one repo's inspection result with its snapshot version.
type RepoOutcome struct {
	Repo            string         `json:"repo"`
	OK              bool           `json:"ok"`
	SnapshotVersion int            `json:"snapshot_version"`
	Result          *InspectResult `json:"result,omitempty"`
	Error           string         `json:"error,omitempty"`
}

// BatchResult summarises one concurrent multi-repo inspection run.
type BatchResult struct {
	Concurrency int           `json:"concurrency"`
	MaxInFlight int64         `json:"max_in_flight"`
	Outcomes    []RepoOutcome `json:"outcomes"`
}

// RunBatch inspects every repo under samplesDir with bounded concurrency.
// Repos beyond the concurrency limit wait in a queue; a per-repo generation
// budget failure terminates only that repo's session.
func RunBatch(samplesDir, stateDir string, concurrency, budget int) (*BatchResult, *Store, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	repos, err := ListRepos(samplesDir)
	if err != nil {
		return nil, nil, err
	}
	store, err := OpenStore(stateDir)
	if err != nil {
		return nil, nil, err
	}
	sem := make(chan struct{}, concurrency)
	var inFlight, maxInFlight atomic.Int64
	var mu sync.Mutex
	outcomes := make([]RepoOutcome, 0, len(repos))
	var wg sync.WaitGroup
	for _, repo := range repos {
		repo := repo
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{} // waiting queue: block until a slot frees
			defer func() { <-sem }()
			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				m := maxInFlight.Load()
				if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
					break
				}
			}
			outcome := RepoOutcome{Repo: repo, OK: true}
			sessID, err := store.SessionStart(repo)
			if err != nil {
				outcome.OK = false
				outcome.Error = err.Error()
				mu.Lock()
				outcomes = append(outcomes, outcome)
				mu.Unlock()
				return
			}
			res, err := InspectRepo(samplesDir, repo, budget)
			if err != nil {
				outcome.OK = false
				outcome.Error = err.Error()
				store.SessionFinish(sessID, "error", err.Error())
				mu.Lock()
				outcomes = append(outcomes, outcome)
				mu.Unlock()
				return
			}
			version, err := store.CommitSnapshot(repo, res, false)
			if err != nil {
				outcome.OK = false
				outcome.Error = err.Error()
				store.SessionFinish(sessID, "error", err.Error())
				mu.Lock()
				outcomes = append(outcomes, outcome)
				mu.Unlock()
				return
			}
			outcome.Result = res
			outcome.SnapshotVersion = version
			status := "done"
			detail := fmt.Sprintf("restore_generation=%d", res.RestoreGen)
			for _, c := range res.Codes {
				if c == EGenerationBudget {
					status = "budget-exceeded"
					outcome.OK = false
				}
			}
			store.SessionFinish(sessID, status, detail)
			mu.Lock()
			outcomes = append(outcomes, outcome)
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].Repo < outcomes[j].Repo })
	failed := 0
	for _, o := range outcomes {
		if !o.OK {
			failed++
		}
	}
	br := &BatchResult{Concurrency: concurrency, MaxInFlight: maxInFlight.Load(), Outcomes: outcomes}
	if err := store.RecordBatchStats(int(br.MaxInFlight), len(outcomes), failed); err != nil {
		return nil, nil, err
	}
	return br, store, nil
}
