package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

type testEnv struct {
	t     *testing.T
	db    *sql.DB
	svc   *Service
	sched *Scheduler
	ts    *httptest.Server
}

func newTestEnv(t *testing.T, leaseTTL time.Duration) *testEnv {
	t.Helper()
	db, err := openDB(filepath.Join(t.TempDir(), "workbench.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	svc := NewService(db, "", leaseTTL)
	sched := NewScheduler(svc, SchedulerConfig{GlobalConcurrency: 2,
		PerPartitionLeaseLimit: 1, QueueCapacity: 32, RoundBudgetMS: 250,
		QueryTimeoutMS: 150, MinDispatchMS: 2})
	app := NewApp(svc, sched, "", false)
	ts := httptest.NewServer(app.Routes())
	svc.coordinatorURL = ts.URL
	if _, err := svc.RecoverIntents(context.Background()); err != nil {
		t.Fatalf("initial recovery: %v", err)
	}
	env := &testEnv{t: t, db: db, svc: svc, sched: sched, ts: ts}
	t.Cleanup(func() {
		ts.Close()
		_ = svc.Close()
	})
	return env
}

func (e *testEnv) topic(name string, partitions int, rule string, priorities ...int) Topic {
	e.t.Helper()
	topic, err := e.svc.CreateTopic(context.Background(), Topic{Name: name,
		Partitions: partitions, RoutingRule: rule, PartitionPriority: priorities})
	if err != nil {
		e.t.Fatalf("create topic %s: %v", name, err)
	}
	return topic
}

func (e *testEnv) publish(messageID, topic string, partition int, body string, txn string, requestID string) PublishResult {
	e.t.Helper()
	req := PublishRequest{MessageID: messageID, Topic: topic, Partition: &partition,
		Body: body, TxnID: txn, Transactional: txn != "", RequestID: requestID}
	out, err := e.svc.Publish(context.Background(), req)
	if err != nil {
		e.t.Fatalf("publish %s: %v", messageID, err)
	}
	return out
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	var we *WorkbenchError
	if !errors.As(err, &we) {
		t.Fatalf("expected WorkbenchError %s, got %T: %v", code, err, err)
	}
	if we.Code != code {
		t.Fatalf("expected %s, got %s: %v", code, we.Code, err)
	}
}

func (e *testEnv) partition(view string, p int) PartitionView {
	e.t.Helper()
	got, err := e.svc.GetPartition(context.Background(), view, p)
	if err != nil {
		e.t.Fatalf("get partition: %v", err)
	}
	return got
}
