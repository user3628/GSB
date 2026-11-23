package probe

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"sandboxpolicy/business"
	"sandboxpolicy/policy"
)

// Run executes file read/write and process-start probes. Registry-only rules
// are intentionally absent because the contract requires real probes only for
// file and process boundaries.
func Run(ctx context.Context, p *policy.Policy, version int, opts Options) (*Report, error) {
	if p == nil {
		return nil, business.New(business.CodeProbeFailed, "policy is unavailable")
	}
	parent := opts.TempParent
	if parent == "" {
		parent = os.TempDir()
	}
	root, err := os.MkdirTemp(parent, "sandbox-policy-probe-")
	if err != nil {
		return nil, business.New(business.CodeProbeFailed, "cannot create isolated probe workspace")
	}
	cleanupRoot := func() error {
		_ = os.Chmod(root, 0o755)
		return os.RemoveAll(root)
	}
	defer cleanupRoot()

	timeout := time.Duration(opts.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	dependency := make(map[string]business.Code)
	for _, issue := range policy.StateIssues(p) {
		if _, exists := dependency[issue.RuleID]; !exists {
			dependency[issue.RuleID] = issue.Code
		}
	}

	report := &Report{Namespace: p.Namespace, Version: version, Results: []RuleResult{}, BusinessCodes: []business.Code{}}
	rules := append([]policy.Rule(nil), p.Rules...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	for _, rule := range rules {
		if rule.Action != policy.ActionFileRead && rule.Action != policy.ActionFileWrite && rule.Action != policy.ActionProcessStart {
			continue
		}
		if code, blocked := dependency[rule.ID]; blocked {
			report.Results = append(report.Results, RuleResult{RuleID: rule.ID, Action: rule.Action, Ref: rule.Ref, Expected: rule.Effect, Observed: "not_probed", Status: "dependency_invalid", Match: false, Code: code, Detail: "upstream dependency is invalid"})
			continue
		}
		var result RuleResult
		switch rule.Action {
		case policy.ActionFileRead, policy.ActionFileWrite:
			result = probeFile(ctx, root, p, rule)
		case policy.ActionProcessStart:
			result = probeProcess(ctx, root, p, rule, timeout)
		}
		report.Results = append(report.Results, result)
	}
	sort.Slice(report.Results, func(i, j int) bool { return report.Results[i].RuleID < report.Results[j].RuleID })
	summarize(report)
	if err := cleanupRoot(); err != nil {
		return nil, business.New(business.CodeProbeFailed, "probe workspace cleanup failed")
	}
	return report, nil
}

func probeFile(ctx context.Context, root string, p *policy.Policy, rule policy.Rule) RuleResult {
	result := RuleResult{RuleID: rule.ID, Action: rule.Action, Ref: rule.Ref, Expected: rule.Effect}
	if err := ctx.Err(); err != nil {
		return probeFailure(result, "probe context ended before file operation")
	}
	dir, ok := directoryByID(p, rule.Ref)
	if !ok {
		return probeFailure(result, "declared directory is unavailable")
	}
	base := fixtureBase(root, dir.Path, dir.ID)
	ruleDir := filepath.Join(base, safeSegment(rule.ID))
	if !inside(root, ruleDir) {
		return probeFailure(result, "fixture path escaped isolated workspace")
	}
	if err := os.MkdirAll(ruleDir, 0o755); err != nil {
		return probeFailure(result, "cannot create file probe fixture directory")
	}
	target := filepath.Join(ruleDir, "probe.txt")
	if err := os.WriteFile(target, []byte("sandbox-policy-probe\n"), 0o600); err != nil {
		return probeFailure(result, "cannot create file probe fixture")
	}
	guard := func() error { return os.Chmod(target, 0o600) }
	if rule.Effect == policy.EffectDeny {
		class := denyRead
		if rule.Action == policy.ActionFileWrite {
			class = denyWrite
		}
		restore, err := applyDeny(target, class)
		if err != nil {
			return probeFailure(result, "cannot apply restrictive permissions to file probe fixture")
		}
		guard = restore
	}
	defer guard()

	if rule.Action == policy.ActionFileRead {
		_, err := os.ReadFile(target)
		observed := "deny"
		if err == nil {
			observed = "allow"
		}
		result.Observed = observed
		switch {
		case rule.Effect == policy.EffectAllow && observed == "allow":
			return matched(result, "read succeeded")
		case rule.Effect == policy.EffectDeny && observed == "deny":
			return matched(result, "read was denied")
		case rule.Effect == policy.EffectDeny && observed == "allow":
			return probeFailure(result, "restrictive read permission was not enforced by the host")
		default:
			return probeMismatch(result, "allowed read was denied by the host")
		}
	}

	file, err := os.OpenFile(target, os.O_WRONLY|os.O_APPEND, 0o600)
	observed := "deny"
	if err == nil {
		_, writeErr := file.Write([]byte("x"))
		_ = file.Close()
		if writeErr == nil {
			observed = "allow"
		}
	}
	result.Observed = observed
	switch {
	case rule.Effect == policy.EffectAllow && observed == "allow":
		return matched(result, "write succeeded")
	case rule.Effect == policy.EffectDeny && observed == "deny":
		return matched(result, "write was denied")
	case rule.Effect == policy.EffectDeny && observed == "allow":
		return probeFailure(result, "restrictive write permission was not enforced by the host")
	default:
		return probeMismatch(result, "allowed write was denied by the host")
	}
}

func probeProcess(ctx context.Context, root string, p *policy.Policy, rule policy.Rule, timeout time.Duration) RuleResult {
	result := RuleResult{RuleID: rule.ID, Action: rule.Action, Ref: rule.Ref, Expected: rule.Effect}
	if _, ok := processGroupByID(p, rule.Ref); !ok {
		return probeFailure(result, "declared process group is unavailable")
	}
	self, err := os.Executable()
	if err != nil {
		return probeFailure(result, "cannot resolve probe executable")
	}
	ruleDir := filepath.Join(root, "process", safeSegment(rule.ID))
	if !inside(root, ruleDir) {
		return probeFailure(result, "process fixture path escaped isolated workspace")
	}
	if err := os.MkdirAll(ruleDir, 0o755); err != nil {
		return probeFailure(result, "cannot create process probe fixture directory")
	}
	name := "probe-executable"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	exePath := filepath.Join(ruleDir, name)
	if err := copyExecutable(self, exePath); err != nil {
		return probeFailure(result, "cannot copy isolated probe executable")
	}
	restore := func() error { return os.Chmod(exePath, 0o755) }
	if rule.Effect == policy.EffectDeny {
		denyRestore, err := applyDeny(exePath, denyExec)
		if err != nil {
			return probeFailure(result, "cannot apply restrictive process permission")
		}
		restore = denyRestore
	}
	defer restore()

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(runCtx, exePath, "probe-helper")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	startErr := cmd.Start()
	observed := "deny"
	if startErr == nil {
		observed = "allow"
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}
	result.Observed = observed
	if runCtx.Err() != nil {
		result.Observed = "error"
		return probeFailure(result, "process probe timed out")
	}
	switch {
	case rule.Effect == policy.EffectAllow && observed == "allow":
		return matched(result, "process started")
	case rule.Effect == policy.EffectDeny && observed == "deny":
		return matched(result, "process start was denied")
	case rule.Effect == policy.EffectDeny && observed == "allow":
		return probeFailure(result, "restrictive execute permission was not enforced by the host")
	default:
		return probeMismatch(result, "allowed process start was denied by the host")
	}
}

func matched(result RuleResult, detail string) RuleResult {
	result.Status = "matched"
	result.Match = true
	result.Detail = detail
	return result
}

func probeFailure(result RuleResult, detail string) RuleResult {
	result.Status = "probe_failed"
	result.Match = false
	result.Code = business.CodeProbeFailed
	result.Detail = detail
	if result.Observed == "" {
		result.Observed = "error"
	}
	return result
}

func probeMismatch(result RuleResult, detail string) RuleResult {
	result.Status = "mismatch"
	result.Match = false
	result.Code = business.CodeProbeMismatch
	result.Detail = detail
	return result
}

func summarize(report *Report) {
	codes := make([]business.Code, 0)
	for _, result := range report.Results {
		report.Summary.Total++
		switch result.Status {
		case "matched":
			report.Summary.Matched++
		case "mismatch":
			report.Summary.Mismatched++
			codes = append(codes, result.Code)
		case "probe_failed":
			report.Summary.Failed++
			codes = append(codes, result.Code)
		case "dependency_invalid":
			report.Summary.Skipped++
			codes = append(codes, result.Code)
		}
	}
	report.BusinessCodes = business.Codes(codes...)
}

func directoryByID(p *policy.Policy, id string) (policy.Directory, bool) {
	for _, d := range p.Directories {
		if d.ID == id {
			return d, true
		}
	}
	return policy.Directory{}, false
}

func processGroupByID(p *policy.Policy, id string) (policy.ProcessGroup, bool) {
	for _, g := range p.ProcessGroups {
		if g.ID == id {
			return g, true
		}
	}
	return policy.ProcessGroup{}, false
}

func fixtureBase(root, declaredPath, fallback string) string {
	raw := strings.ReplaceAll(declaredPath, "\\", "/")
	if index := strings.Index(raw, ":"); index >= 0 {
		raw = raw[index+1:]
	}
	raw = strings.TrimLeft(raw, "/")
	parts := strings.Split(raw, "/")
	clean := make([]string, 0, len(parts)+1)
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			continue
		}
		clean = append(clean, safeSegment(part))
	}
	if len(clean) == 0 {
		clean = append(clean, safeSegment(fallback))
	}
	base := filepath.Join(append([]string{root}, clean...)...)
	if !inside(root, base) {
		return filepath.Join(root, safeSegment(fallback))
	}
	return base
}

func safeSegment(value string) string {
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}
	if builder.Len() == 0 {
		return "_"
	}
	return builder.String()
}

func inside(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func copyExecutable(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Chmod(target, 0o755)
}
