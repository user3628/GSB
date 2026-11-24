package resource

import (
	"context"
	"sync"

	"keylife/internal/model"
)

type Config struct {
	MaxConcurrency      int `json:"max_concurrency"`
	DefaultSubjectLimit int `json:"default_subject_limit"`
}

type Stats struct {
	MaxConcurrency   int            `json:"max_concurrency"`
	Running          int            `json:"running"`
	Queued           int            `json:"queued"`
	PendingBySubject map[string]int `json:"pending_by_subject"`
}

// Controller blocks excess global work in a queue and applies an immediate
// per-subject pending-request budget.
type Controller struct {
	cfg Config
	sem chan struct{}

	mu      sync.Mutex
	pending map[string]int
	running int
	queued  int
}

func New(cfg Config) *Controller {
	if cfg.MaxConcurrency < 1 {
		cfg.MaxConcurrency = 1
	}
	if cfg.DefaultSubjectLimit < 1 {
		cfg.DefaultSubjectLimit = 1
	}
	return &Controller{
		cfg:     cfg,
		sem:     make(chan struct{}, cfg.MaxConcurrency),
		pending: make(map[string]int),
	}
}

func (c *Controller) Config() Config { return c.cfg }

// Acquire reserves a subject budget slot immediately. If no global execution
// slot is free, the caller waits in the queue rather than being rejected.
func (c *Controller) Acquire(ctx context.Context, subject string, subjectLimit int) (func(), error) {
	if subjectLimit <= 0 {
		subjectLimit = c.cfg.DefaultSubjectLimit
	}
	c.mu.Lock()
	if c.pending[subject] >= subjectLimit {
		c.mu.Unlock()
		return nil, model.NewError(model.CodeKeyBudget, "subject %s has reached %d pending requests", subject, subjectLimit)
	}
	c.pending[subject]++
	c.queued++
	c.mu.Unlock()

	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		c.mu.Lock()
		c.pending[subject]--
		if c.pending[subject] == 0 {
			delete(c.pending, subject)
		}
		c.queued--
		c.mu.Unlock()
		return nil, ctx.Err()
	}

	c.mu.Lock()
	c.queued--
	c.running++
	c.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			<-c.sem
			c.mu.Lock()
			c.running--
			c.pending[subject]--
			if c.pending[subject] == 0 {
				delete(c.pending, subject)
			}
			c.mu.Unlock()
		})
	}
	return release, nil
}

func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending := make(map[string]int, len(c.pending))
	for k, v := range c.pending {
		pending[k] = v
	}
	return Stats{
		MaxConcurrency:   c.cfg.MaxConcurrency,
		Running:          c.running,
		Queued:           c.queued,
		PendingBySubject: pending,
	}
}
