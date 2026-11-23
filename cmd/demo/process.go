package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type workbenchProcess struct {
	cmd     *exec.Cmd
	baseURL string
	done    chan error
	logs    *logBuffer
}

func findRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from working directory")
		}
		dir = parent
	}
}

func buildWorkbench(root, output string) error {
	cmd := exec.Command("go", "build", "-o", output, "./cmd/workbench")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build workbench: %w\n%s", err, raw)
	}
	return nil
}

func startWorkbench(root, binary, dbPath, webhookURL string, extraArgs []string) (*workbenchProcess, error) {
	args := []string{
		"-addr", "127.0.0.1:0", "-db", dbPath, "-rollback-url", webhookURL, "-version", "demo",
	}
	args = append(args, extraArgs...)
	cmd := exec.Command(binary, args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "WORKBENCH_DEMO=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	logs := &logBuffer{}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	proc := &workbenchProcess{cmd: cmd, done: make(chan error, 1), logs: logs}
	urlCh := make(chan string, 1)
	go scanOutput(stdout, logs, urlCh)
	go scanOutput(stderr, logs, nil)
	go func() {
		proc.done <- cmd.Wait()
	}()
	select {
	case baseURL := <-urlCh:
		proc.baseURL = baseURL
		return proc, nil
	case err := <-proc.done:
		return nil, fmt.Errorf("workbench exited before ready: %v\n%s", err, logs.String())
	case <-time.After(12 * time.Second):
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("workbench did not become ready\n%s", logs.String())
	}
}

func scanOutput(reader io.Reader, logs *logBuffer, urlCh chan<- string) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		logs.Write([]byte(line + "\n"))
		if strings.HasPrefix(line, "WORKBENCH_LISTENING ") {
			url := strings.TrimSpace(strings.TrimPrefix(line, "WORKBENCH_LISTENING "))
			if urlCh != nil {
				urlCh <- url
			}
		}
	}
}

func (p *workbenchProcess) wait(timeout time.Duration) error {
	select {
	case err := <-p.done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for workbench (pid %d)", p.cmd.Process.Pid)
	}
}

func (p *workbenchProcess) stop() error {
	if p.cmd.Process == nil {
		return nil
	}
	if runtime.GOOS != "windows" {
		_ = p.cmd.Process.Signal(os.Interrupt)
	} else {
		_ = p.cmd.Process.Kill()
	}
	select {
	case err := <-p.done:
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 130 {
			return nil
		}
		return err
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		return p.wait(2 * time.Second)
	}
}

func (p *workbenchProcess) kill() {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
}
