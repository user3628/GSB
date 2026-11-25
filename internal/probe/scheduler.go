package probe

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"deployverify/internal/model"
)

type Task struct {
	Ref      model.Ref
	Priority int
	URL      string
	Method   string
	Timeout  time.Duration
}

type Result struct {
	Ref          model.Ref `json:"ref"`
	Status       string    `json:"status"`
	Code         string    `json:"code,omitempty"`
	URL          string    `json:"url,omitempty"`
	LatencyMS    int64     `json:"latencyMs"`
	Detail       string    `json:"detail,omitempty"`
	PersistError string    `json:"persistError,omitempty"`
}

type Completion struct {
	RunID     string
	Result    Result
	ElapsedMS int64
}

type Sink interface {
	CommitCompletion(context.Context, Completion) error
}

type Config struct {
	GlobalConcurrency int
	EnvConcurrency    int
	QueueCapacity     int
	Budget            time.Duration
	DefaultTimeout    time.Duration
}

type Report struct {
	RunID             string         `json:"runId"`
	Empty             bool           `json:"empty"`
	Conclusion        string         `json:"conclusion"`
	Status            string         `json:"status"`
	Code              string         `json:"code,omitempty"`
	Config            Config         `json:"config"`
	StartedAt         time.Time      `json:"startedAt"`
	ElapsedMS         int64          `json:"elapsedMs"`
	BudgetMS          int64          `json:"budgetMs"`
	ConsumedBudgetMS  int64          `json:"consumedBudgetMs"`
	Results           []Result       `json:"results"`
	Rejected          []Task         `json:"rejected,omitempty"`
	Duplicates        []model.Ref    `json:"duplicates,omitempty"`
	GlobalConcurrency int            `json:"globalConcurrency"`
	EnvConcurrency    int            `json:"envConcurrency"`
	QueueCapacity     int            `json:"queueCapacity"`
	TotalTasks        int            `json:"totalTasks"`
	StartedCount      int            `json:"startedCount"`
	SucceededCount    int            `json:"succeededCount"`
	FailedCount       int            `json:"failedCount"`
	TimeoutCount      int            `json:"timeoutCount"`
	SkippedCount      int            `json:"skippedCount"`
	DuplicateCount    int            `json:"duplicateCount"`
	PeakGlobalActive  int            `json:"peakGlobalActive"`
	PeakEnvActive     map[string]int `json:"peakEnvActive,omitempty"`
}

type Executor func(ctx context.Context, task Task) Result

type Scheduler struct {
	Executor Executor
	Sink     Sink
}

func NormalizeConfig(cfg Config) Config {
	if cfg.GlobalConcurrency <= 0 {
		cfg.GlobalConcurrency = 2
	}
	if cfg.EnvConcurrency <= 0 {
		cfg.EnvConcurrency = 1
	}
	if cfg.QueueCapacity < 0 {
		cfg.QueueCapacity = 0
	}
	if cfg.Budget <= 0 {
		cfg.Budget = 2 * time.Second
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = time.Second
	}
	return cfg
}

func StableTasks(tasks []Task) []Task {
	out := append([]Task(nil), tasks...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		if out[i].Ref.Environment != out[j].Ref.Environment {
			return out[i].Ref.Environment < out[j].Ref.Environment
		}
		return out[i].Ref.Service < out[j].Ref.Service
	})
	return out
}

// Run executes tasks under global/env concurrency, bounded queue and round budget.
// It is safe for one round; callers should create a new Scheduler/run for each round.
func (s *Scheduler) Run(parent context.Context, runID string, input []Task, cfg Config) Report {
	cfg = NormalizeConfig(cfg)
	startedAt := time.Now()
	deadline := startedAt.Add(cfg.Budget)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()

	unique, duplicates := dedupe(input)
	tasks := StableTasks(unique)
	report := Report{
		RunID: runID, Config: cfg, StartedAt: startedAt, BudgetMS: cfg.Budget.Milliseconds(),
		GlobalConcurrency: cfg.GlobalConcurrency, EnvConcurrency: cfg.EnvConcurrency,
		QueueCapacity: cfg.QueueCapacity, Duplicates: duplicates, DuplicateCount: len(duplicates),
		Results: []Result{}, Rejected: []Task{}, PeakEnvActive: map[string]int{},
	}
	if len(tasks) == 0 {
		report.Empty = true
		report.Status = "empty_round"
		report.Conclusion = "QUEUE_EMPTY: no deployment units matched this probe round"
		report.ElapsedMS = time.Since(startedAt).Milliseconds()
		return report
	}
	report.TotalTasks = len(tasks)

	exec := s.Executor
	if exec == nil {
		exec = ExecuteHTTP
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	globalActive := 0
	envActive := map[string]int{}
	queue := []Task{}
	seen := map[string]bool{}
	queueFull := false

	canStartLocked := func(task Task) bool {
		if time.Now().After(deadline) {
			return false
		}
		if globalActive >= cfg.GlobalConcurrency {
			return false
		}
		if envActive[task.Ref.Environment] >= cfg.EnvConcurrency {
			return false
		}
		return true
	}
	admitFromQueueLocked := func() []Task {
		launched := []Task{}
		remaining := []Task{}
		for _, task := range queue {
			if !time.Now().Before(deadline) {
				report.Results = append(report.Results, skipped(task, model.CodeProbeBudget, "round budget expired before queued task started"))
				report.SkippedCount++
				continue
			}
			if canStartLocked(task) {
				globalActive++
				envActive[task.Ref.Environment]++
				if globalActive > report.PeakGlobalActive {
					report.PeakGlobalActive = globalActive
				}
				if envActive[task.Ref.Environment] > report.PeakEnvActive[task.Ref.Environment] {
					report.PeakEnvActive[task.Ref.Environment] = envActive[task.Ref.Environment]
				}
				report.StartedCount++
				wg.Add(1)
				launched = append(launched, task)
			} else {
				remaining = append(remaining, task)
			}
		}
		queue = remaining
		return launched
	}

	var runTask func(task Task)
	runTask = func(task Task) {
		defer wg.Done()
		result := exec(ctx, task)
		if result.LatencyMS == 0 {
			result.LatencyMS = time.Since(startedAt).Milliseconds()
		}
		mu.Lock()
		globalActive--
		envActive[task.Ref.Environment]--
		launched := admitFromQueueLocked()
		mu.Unlock()
		for _, next := range launched {
			go runTask(next)
		}

		completion := Completion{RunID: runID, Result: result, ElapsedMS: time.Since(startedAt).Milliseconds()}
		if s.Sink != nil {
			if err := s.Sink.CommitCompletion(parent, completion); err != nil {
				result.PersistError = err.Error()
			}
		}
		mu.Lock()
		report.Results = append(report.Results, result)
		switch result.Status {
		case "success":
			report.SucceededCount++
		case "timeout":
			report.TimeoutCount++
		case "skipped":
			report.SkippedCount++
		default:
			report.FailedCount++
		}
		mu.Unlock()
	}

	for index, task := range tasks {
		if !time.Now().Before(deadline) {
			mu.Lock()
			for _, remaining := range tasks[index:] {
				report.Results = append(report.Results, skipped(remaining, model.CodeProbeBudget, "round budget expired before admission"))
				report.SkippedCount++
			}
			mu.Unlock()
			break
		}
		mu.Lock()
		if canStartLocked(task) {
			globalActive++
			envActive[task.Ref.Environment]++
			report.PeakGlobalActive = maxInt(report.PeakGlobalActive, globalActive)
			report.PeakEnvActive[task.Ref.Environment] = maxInt(report.PeakEnvActive[task.Ref.Environment], envActive[task.Ref.Environment])
			report.StartedCount++
			wg.Add(1)
			mu.Unlock()
			go runTask(task)
			continue
		}
		if len(queue) >= cfg.QueueCapacity {
			queueFull = true
			report.Code = model.CodeProbeQueueFull
			report.Rejected = append(report.Rejected, task)
			report.Rejected = append(report.Rejected, tasks[index+1:]...)
			mu.Unlock()
			break
		}
		queue = append(queue, task)
		seen[task.Ref.String()] = true
		mu.Unlock()
	}

	if queueFull {
		cancel()
		mu.Lock()
		for _, task := range queue {
			report.Results = append(report.Results, skipped(task, model.CodeProbeQueueFull, "queued task canceled because bounded queue became full"))
			report.SkippedCount++
		}
		queue = nil
		mu.Unlock()
	}
	wg.Wait()
	cancel()

	sortResults(report.Results)
	report.ElapsedMS = time.Since(startedAt).Milliseconds()
	report.ConsumedBudgetMS = report.ElapsedMS
	switch {
	case queueFull:
		report.Status = "queue_full"
		report.Conclusion = "E_PROBE_QUEUE_FULL: bounded waiting queue was full"
	case report.SucceededCount == len(tasks):
		report.Status = "completed"
		report.Conclusion = "all probes succeeded"
	case report.TimeoutCount > 0 || report.FailedCount > 0 || report.SkippedCount > 0:
		report.Status = "completed_with_errors"
		report.Conclusion = "probe round completed with timeout, failure or budget skips"
	default:
		report.Status = "completed"
		report.Conclusion = "probe round completed"
	}
	return report
}

func dedupe(tasks []Task) ([]Task, []model.Ref) {
	seen := map[string]bool{}
	out := []Task{}
	duplicates := []model.Ref{}
	for _, task := range tasks {
		key := task.Ref.String()
		if seen[key] {
			duplicates = append(duplicates, task.Ref)
			continue
		}
		seen[key] = true
		out = append(out, task)
	}
	return out, duplicates
}

func skipped(task Task, code, detail string) Result {
	return Result{Ref: task.Ref, Status: "skipped", Code: code, URL: task.URL, Detail: detail}
}

func sortResults(results []Result) {
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Ref.Environment != results[j].Ref.Environment {
			return results[i].Ref.Environment < results[j].Ref.Environment
		}
		return results[i].Ref.Service < results[j].Ref.Service
	})
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func IsDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}
