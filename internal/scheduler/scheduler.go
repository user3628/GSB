// Package scheduler runs concurrent tracing windows under resource budgets.
package scheduler

import (
	"sort"
	"sync"

	"flowtrace/internal/adjudicate"
	"flowtrace/internal/codes"
	"flowtrace/internal/latency"
	"flowtrace/internal/model"
	"flowtrace/internal/policy"
)

// Config tunes concurrency and budgets.
type Config struct {
	MaxConcurrent       int   // max windows traced at once; extras wait in queue
	MaxEventsPerWindow  int   // per-window event budget
	SessionMemoryBudget int64 // per-session estimated memory budget in bytes
}

// WindowInput is one window's workload: events in arrival order.
type WindowInput struct {
	ID     string
	Events []model.Event
}

// Session groups windows whose mark state is isolated from other sessions.
type Session struct {
	ID      string
	Policy  *policy.Snapshot
	Windows []WindowInput
}

// WindowResult is the outcome of one window.
type WindowResult struct {
	SessionID  string
	WindowID   string
	Code       string // OK or E_EVENT_BUDGET
	Incomplete bool   // budget-terminated: excluded from consistency compare
	Window     *adjudicate.Window
	EventCount int
}

// estimateBytes approximates the in-memory footprint of a window's events.
func estimateBytes(events []model.Event) int64 {
	var n int64
	for _, ev := range events {
		n += int64(64 + len(ev.Subject) + len(ev.SrcField) + len(ev.DstField))
	}
	return n
}

// Run schedules all sessions' windows. Windows beyond the concurrency limit
// wait in a FIFO queue instead of being rejected. Windows exceeding the
// per-window event budget terminate with E_EVENT_BUDGET and are marked
// incomplete. Sessions are isolated: each window gets a fresh adjudicator.
func Run(cfg Config, sessions []Session, sampler *latency.Sampler) []WindowResult {
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	var tasks []task
	for _, s := range sessions {
		// Session memory budget: windows are admitted while the session's
		// cumulative estimate fits; the rest wait (queued behind).
		var used int64
		var admitted, overflow []WindowInput
		for _, w := range s.Windows {
			est := estimateBytes(w.Events)
			if cfg.SessionMemoryBudget > 0 && used+est > cfg.SessionMemoryBudget {
				overflow = append(overflow, w)
				continue
			}
			used += est
			admitted = append(admitted, w)
		}
		tasks = append(tasks, wrap(s, admitted)...)
		tasks = append(tasks, wrap(s, overflow)...)
	}

	queue := make(chan task, len(tasks))
	for _, t := range tasks {
		queue <- t
	}
	close(queue)

	var mu sync.Mutex
	var results []WindowResult
	var wg sync.WaitGroup
	for i := 0; i < cfg.MaxConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range queue {
				res := WindowResult{SessionID: t.session.ID, WindowID: t.win.ID, Code: codes.OK, EventCount: len(t.win.Events)}
				w := adjudicate.NewWindow(t.win.ID, t.session.Policy, sampler)
				if cfg.MaxEventsPerWindow > 0 && len(t.win.Events) > cfg.MaxEventsPerWindow {
					res.Code = codes.EEventBudget
					res.Incomplete = true
				} else {
					for _, ev := range t.win.Events {
						w.OnEvent(ev)
					}
					w.Settle()
				}
				res.Window = w
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(results, func(a, b int) bool {
		if results[a].SessionID != results[b].SessionID {
			return results[a].SessionID < results[b].SessionID
		}
		return results[a].WindowID < results[b].WindowID
	})
	return results
}

type task struct {
	session Session
	win     WindowInput
}

func wrap(s Session, wins []WindowInput) []task {
	out := make([]task, 0, len(wins))
	for _, w := range wins {
		out = append(out, task{s, w})
	}
	return out
}
