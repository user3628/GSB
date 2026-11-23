package capver

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SessionLimits 并发验证会话的资源限制。
type SessionLimits struct {
	MaxConcurrentSessions  int           // 并发会话上限，超出进入等待队列
	MaxProcessesPerSession int           // 单会话进程数上限
	ProcessTimeout         time.Duration // 单进程运行时长上限
}

// DefaultSessionLimits 返回默认资源限制。
func DefaultSessionLimits() SessionLimits {
	return SessionLimits{
		MaxConcurrentSessions:  2,
		MaxProcessesPerSession: 8,
		ProcessTimeout:         10 * time.Second,
	}
}

// SessionManager 控制并发验证会话：超并发上限的会话进入等待队列而非直接拒绝。
type SessionManager struct {
	limits SessionLimits
	sem    chan struct{}

	mu     sync.Mutex
	events []Event
}

// NewSessionManager 创建会话管理器。
func NewSessionManager(limits SessionLimits) *SessionManager {
	if limits.MaxConcurrentSessions <= 0 {
		limits.MaxConcurrentSessions = 1
	}
	if limits.MaxProcessesPerSession <= 0 {
		limits.MaxProcessesPerSession = 1
	}
	if limits.ProcessTimeout <= 0 {
		limits.ProcessTimeout = 10 * time.Second
	}
	return &SessionManager{limits: limits, sem: make(chan struct{}, limits.MaxConcurrentSessions)}
}

// Limits 返回当前资源限制。
func (m *SessionManager) Limits() SessionLimits { return m.limits }

func (m *SessionManager) record(ev Event) {
	m.mu.Lock()
	m.events = append(m.events, ev)
	m.mu.Unlock()
}

// Events 返回已记录事件（调用方负责排序）。
func (m *SessionManager) Events() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Event(nil), m.events...)
}

// Acquire 获取一个会话槽位；超限时阻塞等待并记录 queued 事件。
func (m *SessionManager) Acquire(ctx context.Context, sessionID string) (release func(), err error) {
	select {
	case m.sem <- struct{}{}:
		m.record(Event{SessionID: sessionID, Kind: "started"})
		return func() { <-m.sem }, nil
	default:
	}
	m.record(Event{SessionID: sessionID, Kind: "queued", Reason: "max-concurrent-sessions"})
	select {
	case m.sem <- struct{}{}:
		m.record(Event{SessionID: sessionID, Kind: "started", Reason: "dequeued"})
		return func() { <-m.sem }, nil
	case <-ctx.Done():
		return nil, newError(CodeSessionLimit, "session wait cancelled", ctx.Err())
	}
}

// CheckProcessCount 校验单会话进程数上限。
func (m *SessionManager) CheckProcessCount(sessionID string, n int) error {
	if n > m.limits.MaxProcessesPerSession {
		m.record(Event{SessionID: sessionID, Kind: "rejected",
			Reason: fmt.Sprintf("processes %d exceed per-session limit %d", n, m.limits.MaxProcessesPerSession)})
		return newError(CodeSessionLimit,
			fmt.Sprintf("session %s: %d processes exceed limit %d", sessionID, n, m.limits.MaxProcessesPerSession), nil)
	}
	return nil
}

// ProcessContext 为单进程创建带超时上限的上下文。
func (m *SessionManager) ProcessContext(ctx context.Context, sessionID, procID string) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, m.limits.ProcessTimeout)
}

// RecordKill 记录进程树终止事件。
func (m *SessionManager) RecordKill(sessionID, reason string) {
	m.record(Event{SessionID: sessionID, Kind: "killed", Reason: reason})
}

// RecordTimeout 记录超时终止事件。
func (m *SessionManager) RecordTimeout(sessionID, procID string) {
	m.record(Event{SessionID: sessionID, Kind: "timeout", Reason: "process " + procID + " exceeded time limit"})
}

// RecordFinish 记录会话结束事件。
func (m *SessionManager) RecordFinish(sessionID string) {
	m.record(Event{SessionID: sessionID, Kind: "finished"})
}
