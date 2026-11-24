package main

import (
	"encoding/json"
	"sort"
)

// Report is the deterministic, byte-stable structured output.
type Report struct {
	ReportID string       `json:"report_id"`
	Tick     string       `json:"tick"`
	Repos    []RepoReport `json:"repos"`
}

type RepoReport struct {
	Repo            string        `json:"repo"`
	SnapshotVersion int           `json:"snapshot_version"`
	SnapshotCode    string        `json:"snapshot_code,omitempty"`
	ChainIntact     bool          `json:"chain_intact"`
	RestoreGen      int           `json:"restore_generation"`
	RestoreFiles    []string      `json:"restore_files,omitempty"`
	Excluded        []ExcludedGen `json:"excluded,omitempty"`
	Codes           []string      `json:"codes"`
	Timeline        []GenResult   `json:"timeline"`
}

// BuildReport assembles the report from the newest contiguous snapshot of
// every repo present in the store. Ordering is stable: repos sorted by
// relative path, generations by generation number; the only time field is
// the fixed sampling tick.
func BuildReport(store *Store) (*Report, error) {
	repos, err := store.ListSnapshotRepos()
	if err != nil {
		return nil, err
	}
	rep := &Report{ReportID: "bire-report-v1", Tick: fixedTick}
	for _, repo := range repos {
		res, version, code, err := store.LatestResult(repo)
		if err != nil {
			return nil, err
		}
		rr := RepoReport{
			Repo:            repo,
			SnapshotVersion: version,
			SnapshotCode:    code,
			ChainIntact:     res.ChainIntact,
			RestoreGen:      res.RestoreGen,
			RestoreFiles:    res.RestoreFiles,
			Excluded:        res.Excluded,
			Codes:           res.Codes,
			Timeline:        res.Generations,
		}
		sort.Slice(rr.Timeline, func(i, j int) bool { return rr.Timeline[i].Generation < rr.Timeline[j].Generation })
		rep.Repos = append(rep.Repos, rr)
	}
	sort.Slice(rep.Repos, func(i, j int) bool { return rep.Repos[i].Repo < rep.Repos[j].Repo })
	return rep, nil
}

// MarshalReport renders the report with stable key/order and a trailing
// newline so repeated runs over unchanged input are byte-identical.
func MarshalReport(rep *Report) ([]byte, error) {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
