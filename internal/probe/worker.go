package probe

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

func init() {
	if os.Getenv(WorkerEnvKey) == "1" {
		os.Exit(runWorker(os.Stdin, os.Stdout))
	}
}

func runWorker(stdin io.Reader, stdout io.Writer) int {
	response := WorkerResponse{}
	var request Request
	decoder := json.NewDecoder(io.LimitReader(stdin, 8<<20))
	if err := decoder.Decode(&request); err != nil {
		response.ErrorCode = "E_PROBE_EXEC"
		response.Message = "cannot decode worker request"
		_ = json.NewEncoder(stdout).Encode(response)
		return 2
	}
	if !filepath.IsAbs(request.Root) {
		response.ErrorCode = "E_PROBE_EXEC"
		response.Message = "worker root must be absolute"
		_ = json.NewEncoder(stdout).Encode(response)
		return 2
	}
	if info, err := os.Stat(request.Root); err != nil || !info.IsDir() {
		response.ErrorCode = "E_PROBE_EXEC"
		response.Message = "worker root is not a directory"
		_ = json.NewEncoder(stdout).Encode(response)
		return 2
	}
	sort.Slice(request.Specs, func(i, j int) bool { return request.Specs[i].ItemID < request.Specs[j].ItemID })
	observations := make([]Observation, 0, len(request.Specs))
	for _, spec := range request.Specs {
		obs := Observation{ItemID: spec.ItemID, SyncMarker: SyncMarker}
		path, err := SecureJoin(request.Root, spec.Path)
		if err != nil {
			obs.Method = "path_guard_v1"
			obs.ErrorCode = "E_PROBE_ESCAPE"
			obs.Message = err.Error()
			observations = append(observations, obs)
			continue
		}
		switch spec.Kind {
		case "account_policy":
			obs = observeRegularFile(path, obs, "child_fs_stat_perm+child_json_read_v1")
		case "audit_policy":
			obs = observeDirectory(path, obs, "child_fs_stat_perm+child_json_read_v1")
			if obs.Exists && obs.ErrorCode == "" {
				statePath, joinErr := SecureJoin(request.Root, filepath.Join(spec.Path, "state.json"))
				if joinErr != nil {
					obs.ErrorCode = "E_PROBE_ESCAPE"
					obs.Message = joinErr.Error()
				} else if content, readErr := readLimited(statePath); readErr != nil {
					obs.Message = "state.json: " + probeErrorText(readErr)
				} else {
					obs.Content = content
				}
			}
		case "service_start_type":
			obs = observeRegularFile(path, obs, "child_fs_read_declaration_v1")
		case "registry_key_permissions":
			obs = observeDirectory(path, obs, "child_fs_stat_perm+child_json_read_v1")
			if obs.Exists && obs.ErrorCode == "" {
				aclPath, joinErr := SecureJoin(request.Root, filepath.Join(spec.Path, "acl.json"))
				if joinErr != nil {
					obs.ErrorCode = "E_PROBE_ESCAPE"
					obs.Message = joinErr.Error()
				} else if content, readErr := readLimited(aclPath); readErr != nil {
					obs.Message = "acl.json: " + probeErrorText(readErr)
				} else {
					obs.Content = content
				}
			}
		default:
			response.ErrorCode = "E_PROBE_EXEC"
			response.Message = "unsupported probe kind " + spec.Kind
			_ = json.NewEncoder(stdout).Encode(response)
			return 2
		}
		observations = append(observations, obs)
	}
	response.Observations = observations
	if err := json.NewEncoder(stdout).Encode(response); err != nil {
		return 2
	}
	return 0
}

func observeRegularFile(path string, obs Observation, method string) Observation {
	obs.Method = method
	info, err := os.Stat(path)
	if err != nil {
		obs.Message = probeErrorText(err)
		return obs
	}
	if !info.Mode().IsRegular() {
		obs.Message = "path is not a regular file"
		return obs
	}
	obs.Exists = true
	obs.Mode = fmt.Sprintf("%04o", info.Mode().Perm())
	content, err := readLimited(path)
	if err != nil {
		obs.Message = probeErrorText(err)
		return obs
	}
	obs.Content = content
	return obs
}

func observeDirectory(path string, obs Observation, method string) Observation {
	obs.Method = method
	info, err := os.Stat(path)
	if err != nil {
		obs.Message = probeErrorText(err)
		return obs
	}
	if !info.IsDir() {
		obs.Message = "path is not a directory"
		return obs
	}
	obs.Exists = true
	obs.Mode = fmt.Sprintf("%04o", info.Mode().Perm())
	return obs
}

func readLimited(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func probeErrorText(err error) string {
	if pathErr, ok := err.(*os.PathError); ok {
		return pathErr.Op + ": " + pathErr.Err.Error()
	}
	return err.Error()
}
