package pool

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"localsearch/multirecall/internal/model"
	"localsearch/multirecall/internal/worker"
)

type Executor interface {
	RunRound(ctx context.Context, tasks []worker.Task) ([]worker.Result, error)
	Close() error
}

type Inline struct {
	Snapshot worker.Snapshot
	runner   *worker.Runner
}

func (i *Inline) RunRound(ctx context.Context, tasks []worker.Task) ([]worker.Result, error) {
	results := make([]worker.Result, len(tasks))
	if i.runner == nil {
		i.runner = worker.NewRunner(i.Snapshot)
	}
	for index, task := range tasks {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}
		results[index] = i.runner.Execute(task)
	}
	return results, nil
}

func (i *Inline) Close() error { return nil }

type Process struct {
	executable string
	snapshot   string
	procs      []*processWorker
	mu         sync.Mutex
	closed     bool
}

type processWorker struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	enc    *json.Encoder
	dec    *json.Decoder
	reader *bufio.Reader
	stderr *limitedBuffer
	closed bool
}

type limitedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.max <= 0 {
		b.max = 64 * 1024
	}
	remaining := b.max - b.buf.Len()
	if remaining > 0 {
		if len(data) > remaining {
			_, _ = b.buf.Write(data[:remaining])
		} else {
			_, _ = b.buf.Write(data)
		}
	}
	return len(data), nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func NewProcess(executable, snapshotPath string, workers int) (*Process, error) {
	if workers < 1 {
		workers = 1
	}
	p := &Process{executable: executable, snapshot: snapshotPath, procs: make([]*processWorker, 0, workers)}
	for i := 0; i < workers; i++ {
		proc, err := startWorker(executable, snapshotPath)
		if err != nil {
			_ = p.Close()
			return nil, err
		}
		p.procs = append(p.procs, proc)
	}
	return p, nil
}

func startWorker(executable, snapshotPath string) (*processWorker, error) {
	cmd := exec.Command(executable)
	cmd.Env = append(os.Environ(), worker.EnvWorker+"=1", worker.EnvSnapshot+"="+snapshotPath)
	stderr := &limitedBuffer{}
	cmd.Stderr = stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("start worker process: %w", err)
	}
	reader := bufio.NewReaderSize(stdout, 64*1024)
	return &processWorker{
		cmd:    cmd,
		stdin:  stdin,
		stdout: stdout,
		enc:    json.NewEncoder(stdin),
		dec:    json.NewDecoder(reader),
		reader: reader,
		stderr: stderr,
	}, nil
}

func (p *Process) RunRound(ctx context.Context, tasks []worker.Task) ([]worker.Result, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("process pool is closed")
	}
	p.mu.Unlock()
	results := make([]worker.Result, len(tasks))
	if len(tasks) == 0 {
		return results, nil
	}
	workers := len(p.procs)
	if workers > len(tasks) {
		workers = len(tasks)
	}
	chunks := make([][]int, workers)
	for i := range tasks {
		chunks[i%workers] = append(chunks[i%workers], i)
	}
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for shard := 0; shard < workers; shard++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			proc := p.procs[shard]
			for _, taskIndex := range chunks[shard] {
				tasks[taskIndex].Shard = shard
				select {
				case <-ctx.Done():
					results[taskIndex] = worker.Result{Shard: shard, QueryID: tasks[taskIndex].Query.QueryID,
						Error: &model.WorkerFailure{Code: model.CodeWorkerFailure, Message: ctx.Err().Error(), Shard: shard, QueryID: tasks[taskIndex].Query.QueryID}}
					continue
				default:
				}
				if proc.closed {
					results[taskIndex] = deadWorkerResult(shard, tasks[taskIndex].Query.QueryID, "worker process is unavailable", proc.stderr.String())
					continue
				}
				result, err := proc.execute(ctx, tasks[taskIndex])
				if err != nil {
					proc.closed = true
					_ = proc.stdin.Close()
					results[taskIndex] = deadWorkerResult(shard, tasks[taskIndex].Query.QueryID, err.Error(), proc.stderr.String())
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				results[taskIndex] = result
				if result.Error != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = &model.WorkerFailure{Code: result.Error.Code, Message: result.Error.Message, Shard: shard, QueryID: result.Error.QueryID}
					}
					errMu.Unlock()
				}
			}
		}(shard)
	}
	wg.Wait()
	return results, firstErr
}

func deadWorkerResult(shard int, queryID, message, stderr string) worker.Result {
	if stderr != "" {
		message += ": " + stderr
	}
	return worker.Result{Shard: shard, QueryID: queryID, Error: &model.WorkerFailure{
		Code: model.CodeWorkerFailure, Message: message, Shard: shard, QueryID: queryID,
	}}
}

func (w *processWorker) execute(ctx context.Context, task worker.Task) (worker.Result, error) {
	if err := ctx.Err(); err != nil {
		return worker.Result{}, err
	}
	if err := w.enc.Encode(task); err != nil {
		return worker.Result{}, fmt.Errorf("write worker task for %s: %w", task.Query.QueryID, err)
	}
	var result worker.Result
	if err := w.dec.Decode(&result); err != nil {
		return worker.Result{}, fmt.Errorf("read worker result for %s: %w", task.Query.QueryID, err)
	}
	if result.QueryID == "" {
		result.QueryID = task.Query.QueryID
	}
	result.Shard = task.Shard
	return result, nil
}

func (p *Process) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	procs := append([]*processWorker(nil), p.procs...)
	p.mu.Unlock()
	var firstErr error
	for _, proc := range procs {
		if proc == nil {
			continue
		}
		if !proc.closed {
			_ = proc.stdin.Close()
			_ = proc.stdout.Close()
			if err := proc.cmd.Wait(); err != nil && firstErr == nil {
				firstErr = err
			}
			proc.closed = true
			continue
		}
		_ = proc.cmd.Process.Kill()
		_ = proc.cmd.Wait()
	}
	return firstErr
}
