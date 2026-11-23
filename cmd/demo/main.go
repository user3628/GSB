// demo 是可重复的本地回环演示编排器：
// 启动测试端点与工作台 -> 加载样本 -> 全量探测（含超时/拒绝）->
// 可用区回退 -> 调度派发 -> 强制终止 -> 重启恢复，全程只访问 127.0.0.1。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type proc struct {
	name string
	cmd  *exec.Cmd
}

func main() {
	dataDir := flag.String("data-dir", "data", "data directory")
	keep := flag.Bool("keep", false, "keep demo processes and database after run")
	flag.Parse()

	workDir, _ := os.Getwd()
	binDir := filepath.Join(workDir, *dataDir, "bin")
	must(os.MkdirAll(binDir, 0o755))
	must(os.MkdirAll(filepath.Join(workDir, *dataDir), 0o755))

	fmt.Println("== [0/7] building binaries (CGO_ENABLED=0) ==")
	serverBin := filepath.Join(binDir, "workbench"+exeSuffix())
	upstreamBin := filepath.Join(binDir, "testupstream"+exeSuffix())
	runCmd(workDir, "go", "build", "-o", serverBin, "./cmd/server")
	runCmd(workDir, "go", "build", "-o", upstreamBin, "./cmd/testupstream")

	dbPath := filepath.Join(workDir, *dataDir, "demo-workbench.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(dbPath + suffix)
	}
	apiPort := freePort()
	var procs []*proc
	cleanup := func() {
		if *keep {
			return
		}
		for _, p := range procs {
			if p.cmd.Process != nil {
				_ = p.cmd.Process.Kill()
			}
		}
	}
	defer cleanup()

	fmt.Println("== [1/7] starting loopback test endpoints ==")
	startUpstream := func(name string, port int, extra ...string) *proc {
		args := append([]string{"-port", fmt.Sprint(port), "-marker", "UPSTREAM-OK"}, extra...)
		cmd := exec.Command(upstreamBin, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		must(cmd.Start())
		p := &proc{name: name, cmd: cmd}
		procs = append(procs, p)
		return p
	}
	startUpstream("inst-a1", 19091)
	startUpstream("inst-b1", 19092)
	// 19093 提供 /slow，5 秒不响应，探测超时 300ms 触发真实超时。
	startUpstream("inst-timeout", 19093, "-slow-millis", "5000")
	// 19099 故意不监听 -> TCP 连接拒绝 E_PROBE_REFUSED。
	waitTCP(19091, 3*time.Second)
	waitTCP(19092, 3*time.Second)
	waitTCP(19093, 3*time.Second)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	startWorkbench := func(env ...string) *proc {
		cmd := exec.Command(serverBin,
			"-addr", fmt.Sprintf("127.0.0.1:%d", apiPort),
			"-db", dbPath)
		queueCap := os.Getenv("WB_DEMO_QUEUE")
		if queueCap == "" {
			queueCap = "2"
		}
		cmd.Env = append(os.Environ(),
			"WB_GLOBAL=3", "WB_PER_INSTANCE=2", "WB_QUEUE="+queueCap, "WB_BUDGET_MS=4000")
		cmd.Env = append(cmd.Env, env...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		must(cmd.Start())
		p := &proc{name: "workbench", cmd: cmd}
		procs = append(procs, p)
		waitHTTP(baseURL+"/healthz", 5*time.Second)
		return p
	}

	fmt.Println("== [2/7] starting workbench and loading deterministic sample ==")
	wb := startWorkbench()
	sample := mustReadJSON("samples/sample.json")
	resp := post(baseURL+"/v1/samples/load", sample)
	pretty("sample load", resp)

	fmt.Println("== [3/7] full parallel probe round (healthy / timeout / refused) ==")
	probeResp := post(baseURL+"/v1/probe-rounds", map[string]string{"round_id": "probe-1"})
	pretty("probe round probe-1", probeResp)
	probeHistory := get(baseURL + "/v1/rounds/probe-1/probes")
	pretty("probe history", probeHistory)
	assertCode(probeHistory, "E_PROBE_TIMEOUT", "inst-timeout")
	assertCode(probeHistory, "E_PROBE_REFUSED", "inst-refused")

	fmt.Println("== [4/7] availability phases and normalized weight snapshot ==")
	pretty("instances", get(baseURL+"/v1/instances"))
	pretty("latest snapshot", get(baseURL+"/v1/snapshots/latest"))

	fmt.Println("== [5/7] availability-zone fallback: drain zone-a healthy inst-a1 ==")
	pretty("drain inst-a1", post(baseURL+"/v1/instances/inst-a1/drain",
		map[string]string{"reason": "demo manual drain for zone fallback"}))
	dispatchResp := post(baseURL+"/v1/dispatch-rounds", map[string]string{"round_id": "dispatch-1"})
	pretty("dispatch round dispatch-1", dispatchResp)
	pretty("request seq=0 decision", get(baseURL+"/v1/requests/0"))
	pretty("dispatch details", get(baseURL+"/v1/rounds/dispatch-1/dispatches"))

	fmt.Println("== [5b/7] bounded queue full returns E_DISPATCH_QUEUE_FULL ==")
	queueFullResp := post(baseURL+"/v1/dispatch-rounds", map[string]string{"round_id": "dispatch-queue-full"})
	pretty("queue-full response (remaining requests exceed capacity)", queueFullResp)

	fmt.Println("== [6/7] invalid transition guard (DRAINING -> HEALTHY must fail) ==")
	errResp := postExpectError(baseURL+"/v1/instances/inst-a1/drain",
		map[string]string{"reason": "repeat drain while DRAINING is illegal"})
	pretty("illegal transition response", errResp)

	fmt.Println("== [7/7] SIGKILL mid-dispatch then restart recovery ==")
	// 先恢复 inst-a1 到 UNKNOWN，再探测一次让其回到 HEALTHY，产生新的待调度请求需要第二轮。
	pretty("resume inst-a1", post(baseURL+"/v1/instances/inst-a1/resume", map[string]string{}))
	pretty("probe round probe-2", post(baseURL+"/v1/probe-rounds", map[string]string{"round_id": "probe-2"}))
	killWorkbench(wb)
	wb = nil

	// 以强杀模式重启：计划落盘后执行一半派发即 os.Exit(97)，OPEN 意图保留。
	wb = startWorkbench("WB_KILL_AFTER_PLAN=dispatch-kill")
	killResp, killErr := postRaw(baseURL+"/v1/dispatch-rounds",
		map[string]string{"round_id": "dispatch-kill"})
	if killErr != nil {
		fmt.Printf("kill-round connection ended as expected: %v\n", killErr)
	} else {
		fmt.Printf("kill-round HTTP response: %s\n", string(killResp))
	}
	_ = wb.cmd.Wait()
	procs = procs[:len(procs)-1]
	wb = nil

	// 普通重启：启动恢复扫描 OPEN 意图，回滚未提交派发并保留最后完整快照。
	wb = startWorkbench()
	recovery := get(baseURL + "/v1/recovery/latest")
	pretty("recovery report", recovery)
	pretty("instances after recovery", get(baseURL+"/v1/instances"))
	pretty("request seq=0 after recovery", get(baseURL+"/v1/requests/0"))

	// 恢复后重跑调度，验证回滚请求可再次被完整派发。
	pretty("redispatch dispatch-2", post(baseURL+"/v1/dispatch-rounds",
		map[string]string{"round_id": "dispatch-2"}))
	fmt.Println("demo completed successfully; all traffic stayed on 127.0.0.1")
}

func post(url string, body any) []byte {
	return do("POST", url, body, http.StatusOK, http.StatusTooManyRequests)
}

func postExpectError(url string, body any) []byte {
	return do("POST", url, body, http.StatusConflict, http.StatusBadRequest, http.StatusBadGateway)
}

func postRaw(url string, body any) ([]byte, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return raw, err
}

func get(url string) []byte {
	return do("GET", url, nil, http.StatusOK)
}

func do(method, url string, body any, allowed ...int) []byte {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		must(err)
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, reader)
	must(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	must(err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	must(err)
	ok := false
	for _, code := range allowed {
		if resp.StatusCode == code {
			ok = true
		}
	}
	if !ok {
		fail("unexpected status %d from %s: %s", resp.StatusCode, url, string(raw))
	}
	return raw
}

func assertCode(raw []byte, wantCode, instanceID string) {
	var payload struct {
		Probes []struct {
			InstanceID string `json:"instance_id"`
			ErrCode    string `json:"err_code"`
		} `json:"probes"`
	}
	must(json.Unmarshal(raw, &payload))
	for _, p := range payload.Probes {
		if p.InstanceID == instanceID {
			if p.ErrCode != wantCode {
				fail("expected %s for %s, got %q", wantCode, instanceID, p.ErrCode)
			}
			fmt.Printf("assertion ok: %s -> %s\n", instanceID, wantCode)
			return
		}
	}
	fail("instance %s missing in probe history", instanceID)
}

func killWorkbench(p *proc) {
	if p == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
}

func pretty(title string, raw []byte) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		fmt.Printf("-- %s (raw): %s\n", title, string(raw))
		return
	}
	buf, _ := json.MarshalIndent(v, "", "  ")
	fmt.Printf("-- %s:\n%s\n", title, string(buf))
}

func waitTCP(port int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	fail("timeout waiting for tcp %s", addr)
}

func waitHTTP(url string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fail("timeout waiting for http %s", url)
}

func freePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func runCmd(dir, name string, args ...string) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		fail("command %s %s failed: %v\n%s", name, strings.Join(args, " "), err, string(out))
	}
	fmt.Print(string(out))
}

func mustReadJSON(path string) map[string]any {
	raw, err := os.ReadFile(path)
	must(err)
	var out map[string]any
	must(json.Unmarshal(raw, &out))
	return out
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "DEMO FAILURE: "+format+"\n", args...)
	os.Exit(1)
}

func exeSuffix() string {
	if strings.EqualFold(os.Getenv("OS"), "Windows_NT") {
		return ".exe"
	}
	return ""
}
