package resource

import (
	"context"
	"testing"
	"time"

	"keylife/internal/model"
)

func TestGlobalQueueAndSubjectBudget(t *testing.T) {
	controller := New(Config{MaxConcurrency: 1, DefaultSubjectLimit: 2})
	release1, err := controller.Acquire(context.Background(), "alpha", 2)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	go func() {
		release, err := controller.Acquire(context.Background(), "alpha", 2)
		if err != nil {
			t.Errorf("queued acquire failed: %v", err)
			acquired <- nil
			return
		}
		acquired <- release
	}()
	time.Sleep(30 * time.Millisecond)
	stats := controller.Stats()
	if stats.Queued != 1 || stats.Running != 1 {
		t.Fatalf("request was not queued: %+v", stats)
	}
	if _, err := controller.Acquire(context.Background(), "alpha", 2); model.CodeOf(err) != model.CodeKeyBudget {
		t.Fatalf("subject budget code=%s want=%s", model.CodeOf(err), model.CodeKeyBudget)
	}
	release1()
	release2 := <-acquired
	if release2 == nil {
		t.Fatal("queued request never acquired")
	}
	release2()
	if stats := controller.Stats(); stats.Running != 0 || stats.Queued != 0 {
		t.Fatalf("controller did not drain: %+v", stats)
	}
}
