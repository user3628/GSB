// Package report assembles deterministic structured diagnostics for a snapshot.
package report

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"sandboxpolicy/business"
	"sandboxpolicy/policy"
	"sandboxpolicy/probe"
	"sandboxpolicy/snapshot"
)

const SchemaVersion = 1

// Report is the stable external representation used by CLI and automation.
type Report struct {
	SchemaVersion int                    `json:"schema_version"`
	Namespace     string                 `json:"namespace"`
	Version       int                    `json:"version"`
	Digest        string                 `json:"digest"`
	Chain         []snapshot.ChainEntry  `json:"snapshot_chain"`
	ChainWarning  *snapshot.ChainWarning `json:"chain_warning,omitempty"`
	Closure       policy.Closure         `json:"change_closure"`
	StateIssues   []policy.AffectedRule  `json:"state_issues"`
	AffectedRules []AffectedRule         `json:"affected_rules"`
	Probe         *probe.Report          `json:"probe,omitempty"`
	BusinessCodes []business.Code        `json:"business_codes"`
}

// AffectedRule is the merged, rule-ID-sorted view of all invalidated rules.
type AffectedRule struct {
	RuleID string        `json:"rule_id"`
	Code   business.Code `json:"code"`
	Effect string        `json:"effect,omitempty"`
	Action string        `json:"action,omitempty"`
	Ref    string        `json:"ref,omitempty"`
	Direct bool          `json:"direct"`
	Via    []string      `json:"via"`
}

type Options struct {
	Probe     bool
	ProbeOpts probe.Options
}

// Build loads a historical snapshot, binds its stored closure, scans lingering
// unresolved dependencies, optionally probes, and returns stable sorted output.
func Build(ctx context.Context, store *snapshot.Store, namespace string, version int, opts Options) (*Report, error) {
	if version == 0 {
		current, _, err := store.Current(ctx, namespace)
		if err != nil {
			return nil, err
		}
		version = current.Version
	}
	snap, chain, err := store.SnapshotAt(ctx, namespace, version)
	if err != nil {
		return nil, err
	}
	if snap.Policy == nil {
		return nil, business.New(business.CodeSnapshotVersion, "snapshot policy content is unavailable")
	}
	chainEntries := make([]snapshot.ChainEntry, 0, snap.Version)
	for _, entry := range chain.Entries {
		if entry.Version <= snap.Version {
			chainEntries = append(chainEntries, entry)
		}
	}
	if chainEntries == nil {
		chainEntries = []snapshot.ChainEntry{}
	}
	stateIssues := policy.StateIssues(snap.Policy)
	if stateIssues == nil {
		stateIssues = []policy.AffectedRule{}
	}
	report := &Report{
		SchemaVersion: SchemaVersion,
		Namespace:     snap.Namespace,
		Version:       snap.Version,
		Digest:        snap.Digest,
		Chain:         chainEntries,
		ChainWarning:  chain.Warning,
		Closure:       snap.Closure,
		StateIssues:   stateIssues,
		AffectedRules: mergeAffected(snap.Closure, stateIssues),
		BusinessCodes: []business.Code{},
	}
	if opts.Probe {
		probeReport, err := probe.Run(ctx, snap.Policy, snap.Version, opts.ProbeOpts)
		if err != nil {
			return nil, err
		}
		report.Probe = probeReport
	}
	report.BusinessCodes = collectCodes(report)
	return report, nil
}

func mergeAffected(closure policy.Closure, state []policy.AffectedRule) []AffectedRule {
	merged := make(map[string]AffectedRule)
	add := func(item policy.AffectedRule) {
		candidate := AffectedRule{RuleID: item.RuleID, Code: item.Code, Effect: item.Effect, Action: item.Action, Ref: item.Ref, Direct: item.Direct, Via: append([]string(nil), item.Via...)}
		existing, ok := merged[item.RuleID]
		if !ok {
			merged[item.RuleID] = candidate
			return
		}
		if candidate.Code < existing.Code {
			existing.Code = candidate.Code
		}
		existing.Direct = existing.Direct || candidate.Direct
		if compareVia(candidate.Via, existing.Via) < 0 {
			existing.Via = candidate.Via
		}
		if existing.Effect == "" {
			existing.Effect = candidate.Effect
		}
		if existing.Action == "" {
			existing.Action = candidate.Action
		}
		if existing.Ref == "" {
			existing.Ref = candidate.Ref
		}
		merged[item.RuleID] = existing
	}
	for _, item := range closure.Direct {
		add(item)
	}
	for _, item := range closure.Transitive {
		add(item)
	}
	for _, item := range state {
		add(item)
	}
	out := make([]AffectedRule, 0, len(merged))
	for _, item := range merged {
		if item.Via == nil {
			item.Via = []string{}
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuleID < out[j].RuleID })
	return out
}

func compareVia(a, b []string) int {
	return strings.Compare(strings.Join(a, "\x00"), strings.Join(b, "\x00"))
}

func collectCodes(report *Report) []business.Code {
	codes := make([]business.Code, 0)
	codes = append(codes, report.Closure.BusinessCodes...)
	for _, issue := range report.StateIssues {
		codes = append(codes, issue.Code)
	}
	if report.Probe != nil {
		codes = append(codes, report.Probe.BusinessCodes...)
	}
	if report.ChainWarning != nil {
		codes = append(codes, report.ChainWarning.Code)
	}
	return business.Codes(codes...)
}

// Marshal returns stable, indented JSON with a trailing newline.
func Marshal(report *Report) ([]byte, error) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// WriteFile writes the deterministic report atomically within its target dir.
func WriteFile(path string, report *Report) error {
	data, err := Marshal(report)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tempFile, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temp := tempFile.Name()
	defer os.Remove(temp)
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Chmod(0o644); err != nil {
		tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		// Windows rename does not replace an existing destination.
		if runtime.GOOS != "windows" {
			return err
		}
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return err
		}
		return os.Rename(temp, path)
	}
	return nil
}
