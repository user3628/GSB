package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"secverify/internal/model"
)

const (
	SyncMarker   = "SECVERIFY_PROBE_SYNC_V1"
	WorkerEnvKey = "SECVERIFY_PROBE_WORKER"
)

type Spec struct {
	ItemID string `json:"item_id"`
	Kind   string `json:"kind"`
	Path   string `json:"path"`
}

type Request struct {
	Root  string `json:"root"`
	Specs []Spec `json:"specs"`
}

type Observation struct {
	ItemID     string `json:"item_id"`
	Method     string `json:"method"`
	SyncMarker string `json:"sync_marker"`
	Exists     bool   `json:"exists"`
	Mode       string `json:"mode,omitempty"`
	Content    string `json:"content,omitempty"`
	ErrorCode  string `json:"error_code,omitempty"`
	Message    string `json:"message,omitempty"`
}

type WorkerResponse struct {
	Observations []Observation `json:"observations"`
	ErrorCode    string        `json:"error_code,omitempty"`
	Message      string        `json:"message,omitempty"`
}

type Runner struct {
	WorkerPath string
	WorkerArgs []string
	Timeout    time.Duration
	Delay      time.Duration
}

func (r *Runner) Run(ctx context.Context, root string, specs []Spec) ([]Observation, *model.BizError) {
	if r.Delay > 0 {
		timer := time.NewTimer(r.Delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, model.NewError(model.CodeProbeExec, "", -1, "probe delay cancelled")
		case <-timer.C:
		}
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "cannot resolve probe root")
	}
	request := Request{Root: rootAbs, Specs: append([]Spec(nil), specs...)}
	sort.Slice(request.Specs, func(i, j int) bool { return request.Specs[i].ItemID < request.Specs[j].ItemID })
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "cannot encode worker request")
	}
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	workerPath := r.WorkerPath
	if workerPath == "" {
		workerPath, err = os.Executable()
		if err != nil {
			return nil, model.NewError(model.CodeProbeExec, "", -1, "cannot locate worker executable")
		}
	}
	cmd := exec.CommandContext(runCtx, workerPath, r.WorkerArgs...)
	cmd.Dir = rootAbs
	cmd.Env = workerEnv(rootAbs)
	cmd.Stdin = bytes.NewReader(requestData)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if runCtx.Err() != nil {
			return nil, model.NewError(model.CodeProbeExec, "", -1, "probe worker timed out")
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, model.NewError(model.CodeProbeExec, "", -1, "probe worker failed: "+msg)
	}
	var response WorkerResponse
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "invalid worker response")
	}
	if response.ErrorCode != "" {
		return nil, model.NewError(response.ErrorCode, "", -1, response.Message)
	}
	if len(response.Observations) != len(specs) {
		return nil, model.NewError(model.CodeProbeExec, "", -1, "worker observation count mismatch")
	}
	sort.Slice(response.Observations, func(i, j int) bool {
		return response.Observations[i].ItemID < response.Observations[j].ItemID
	})
	for i := range response.Observations {
		if response.Observations[i].SyncMarker == "" {
			response.Observations[i].SyncMarker = SyncMarker
		}
	}
	return response.Observations, nil
}

func workerEnv(root string) []string {
	env := []string{
		WorkerEnvKey + "=1",
		"GOMAXPROCS=1",
		"HOME=" + root,
		"TMPDIR=" + filepath.Join(root, "tmp"),
		"TMP=" + filepath.Join(root, "tmp"),
		"TEMP=" + filepath.Join(root, "tmp"),
	}
	for _, key := range []string{"SystemRoot", "WINDIR", "COMSPEC"} {
		if value := os.Getenv(key); value != "" {
			env = append(env, key+"="+value)
		}
	}
	if path := os.Getenv("PATH"); path != "" {
		env = append(env, "PATH="+path)
	}
	return env
}

// SecureJoin resolves a relative probe path and rejects absolute paths, parent
// traversal, volume changes, and symlinks whose resolved target leaves root.
func SecureJoin(root, rel string) (string, error) {
	if rel == "" || strings.ContainsRune(rel, '\x00') || filepath.IsAbs(rel) || filepath.VolumeName(rel) != "" {
		return "", fmt.Errorf("unsafe path %q", rel)
	}
	if !filepath.IsLocal(rel) {
		return "", fmt.Errorf("path escapes sandbox: %q", rel)
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootResolved, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", err
	}
	candidate := filepath.Join(rootAbs, filepath.Clean(rel))
	if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
		if !withinRoot(rootResolved, resolved) {
			return "", fmt.Errorf("symlink escapes sandbox: %q", rel)
		}
		return resolved, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	existing := candidate
	remainder := make([]string, 0)
	for {
		parent := filepath.Dir(existing)
		if parent == existing {
			return "", fmt.Errorf("cannot resolve sandbox path %q", rel)
		}
		remainder = append([]string{filepath.Base(existing)}, remainder...)
		existing = parent
		if _, err := os.Lstat(existing); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return "", err
		}
	}
	resolvedExisting, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", err
	}
	if !withinRoot(rootResolved, resolvedExisting) {
		return "", fmt.Errorf("parent symlink escapes sandbox: %q", rel)
	}
	resolved := filepath.Join(append([]string{resolvedExisting}, remainder...)...)
	if !withinRoot(rootResolved, resolved) {
		return "", fmt.Errorf("resolved path escapes sandbox: %q", rel)
	}
	return resolved, nil
}

func withinRoot(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	if err != nil || filepath.IsAbs(rel) {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
