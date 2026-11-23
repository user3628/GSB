package fault

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	ImportAfterIntent   = "import_after_intent"
	ImportDuringRebuild = "import_during_rebuild"
	ImportAfterCommit   = "import_after_commit"
	PatchAfterIntent    = "patch_after_intent"
	PatchDuringRebuild  = "patch_during_rebuild"
	PatchAfterCommit    = "patch_after_commit"
)

var Stages = []string{
	ImportAfterIntent, ImportDuringRebuild, ImportAfterCommit,
	PatchAfterIntent, PatchDuringRebuild, PatchAfterCommit,
}

type Rule struct {
	Stage     string        `json:"stage"`
	Mode      string        `json:"mode"`
	Delay     time.Duration `json:"delay"`
	Skip      int           `json:"skip"`
	Remaining int           `json:"remaining"`
}

type publicRule struct {
	Stage     string `json:"stage"`
	Mode      string `json:"mode"`
	DelayMS   int64  `json:"delay_ms,omitempty"`
	Remaining int    `json:"remaining"`
}

type Controller struct {
	mu    sync.Mutex
	rules map[string]*Rule
	hook  func(stage string)
}

func New() *Controller {
	return &Controller{rules: map[string]*Rule{}}
}

// SetHook is intended for in-process tests. In production the zero hook causes
// crash rules to terminate the process with exit code 137 and pause rules to
// sleep long enough for an external supervisor to send SIGKILL.
func (c *Controller) SetHook(hook func(stage string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hook = hook
}

func (c *Controller) Arm(stage, mode string, skip int, delay time.Duration) error {
	if !validStage(stage) {
		return fmt.Errorf("unknown fault stage %q", stage)
	}
	if mode != "crash" && mode != "pause" {
		return fmt.Errorf("fault mode must be crash or pause")
	}
	if skip < 0 {
		return fmt.Errorf("skip must be non-negative")
	}
	if mode == "pause" && delay <= 0 {
		delay = 30 * time.Second
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rules[stage] = &Rule{Stage: stage, Mode: mode, Delay: delay, Skip: skip, Remaining: skip + 1}
	return nil
}

func (c *Controller) Disarm(stage string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.rules, stage)
}

func (c *Controller) Checkpoint(stage string) {
	c.mu.Lock()
	rule := c.rules[stage]
	if rule == nil {
		c.mu.Unlock()
		return
	}
	if rule.Remaining > 1 {
		rule.Remaining--
		rule.Skip--
		c.mu.Unlock()
		return
	}
	delete(c.rules, stage)
	hook := c.hook
	delay := rule.Delay
	mode := rule.Mode
	c.mu.Unlock()
	if hook != nil {
		hook(stage)
		return
	}
	if mode == "crash" {
		// Deliberately bypass deferred cleanup: recovery must rely on durable
		// intent records, not on a graceful shutdown path.
		os.Exit(137)
	}
	time.Sleep(delay)
}

func (c *Controller) Rules() []publicRule {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]publicRule, 0, len(c.rules))
	for _, rule := range c.rules {
		out = append(out, publicRule{Stage: rule.Stage, Mode: rule.Mode, DelayMS: rule.Delay.Milliseconds(), Remaining: rule.Remaining})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Stage < out[j].Stage })
	return out
}

func validStage(stage string) bool {
	for _, candidate := range Stages {
		if candidate == stage {
			return true
		}
	}
	return false
}
