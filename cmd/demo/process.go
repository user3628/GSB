package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type childServer struct {
	cmd  *exec.Cmd
	addr string
	logs *strings.Builder
}

func startChildServer(exe, dbPath, snapshotDir string, extraEnv []string) (*childServer, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, "server", "-addr", "127.0.0.1:0", "-db", dbPath, "-snapshot-dir", snapshotDir)
	var logs strings.Builder
	cmd.Stderr = &logs
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Env = append(os.Environ(), extraEnv...)
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	addrCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "MVCC_LISTEN_ADDR=") {
				addrCh <- strings.TrimPrefix(line, "MVCC_LISTEN_ADDR=")
				return
			}
		}
	}()

	select {
	case addr := <-addrCh:
		return &childServer{cmd: cmd, addr: "http://" + addr, logs: &logs}, nil
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, &startupError{logs: logs.String()}
	}
}

type startupError struct{ logs string }

func (e *startupError) Error() string { return "server did not print listen address: " + e.logs }

func (c *childServer) kill() error {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return nil
	}
	_ = c.cmd.Process.Kill()
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		return c.cmd.Process.Kill()
	}
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return os.ErrNotExist
}

func freeLoopbackAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr, nil
}

var _ = io.Discard
