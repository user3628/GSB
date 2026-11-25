package loopback_test

import (
	"context"
	"net"
	"testing"
	"time"

	"leasearbiter/internal/core"
	"leasearbiter/internal/loopback"
)

func testDecision(holderID string) core.HealDecision {
	return core.HealDecision{
		HolderID:             holderID,
		Action:               core.HealActionRequireEarlyRenew,
		Trigger:              "LATE_OUTSIDE_TOLERANCE",
		DriftMagnitudeMS:     30,
		PreviousLeaseMS:      1000,
		EarlyRenewPointMS:    420,
		DecidedAtMS:          1_800_000_000_000,
		ExpectedRecoveryAtMS: 1_800_000_000_420,
	}
}

func TestDispatchSuccessAndDelayedResponse(t *testing.T) {
	server, err := loopback.Start("127.0.0.1:0", loopback.HandlerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	dispatcher := loopback.NewDispatcher(server.URL(), time.Second)
	receipt, bizErr := dispatcher.DispatchRaw(context.Background(), testDecision("h1"), loopback.DispatchOptions{DelayMS: 25}, "req-1", 1)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if !receipt.Applied || receipt.RequestID != "req-1" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	last, ok := server.Handler().LastHeal()
	if !ok || last.HolderID != "h1" || last.Action != core.HealActionRequireEarlyRenew {
		t.Fatalf("endpoint did not receive heal: %+v", last)
	}
}

func TestDispatchTimeoutClosesUnderlyingConnection(t *testing.T) {
	closed := make(chan loopback.RequestEvent, 1)
	server, err := loopback.Start("127.0.0.1:0", loopback.HandlerConfig{
		OnClosed: func(event loopback.RequestEvent) {
			closed <- event
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	dispatcher := loopback.NewDispatcher(server.URL(), 80*time.Millisecond)
	started := time.Now()
	_, bizErr := dispatcher.DispatchRaw(context.Background(), testDecision("h-timeout"), loopback.DispatchOptions{Inject: "block"}, "req-timeout", 1)
	if bizErr == nil || bizErr.Code != core.CodeHealTimeout {
		t.Fatalf("expected %s, got %#v", core.CodeHealTimeout, bizErr)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("timeout took too long: %s", elapsed)
	}
	select {
	case event := <-closed:
		if !event.Closed || event.Path != "/v1/heal" {
			t.Fatalf("unexpected close event: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe the client-side connection closure")
	}
}

func TestDispatchRejectionAndConnectionRefused(t *testing.T) {
	server, err := loopback.Start("127.0.0.1:0", loopback.HandlerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := loopback.NewDispatcher(server.URL(), time.Second)
	_, bizErr := dispatcher.DispatchRaw(context.Background(), testDecision("h-reject"), loopback.DispatchOptions{Inject: "reject"}, "req-reject", 1)
	if bizErr == nil || bizErr.Code != core.CodeHealRejected {
		t.Fatalf("expected %s, got %#v", core.CodeHealRejected, bizErr)
	}
	server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	refused := loopback.NewDispatcher("http://"+address, 300*time.Millisecond)
	_, bizErr = refused.DispatchRaw(context.Background(), testDecision("h-refused"), loopback.DispatchOptions{}, "req-refused", 1)
	if bizErr == nil || bizErr.Code != core.CodeHealRejected {
		t.Fatalf("expected connection refusal as %s, got %#v", core.CodeHealRejected, bizErr)
	}
}

func TestLateResponseHitsPerDispatchTimeout(t *testing.T) {
	server, err := loopback.Start("127.0.0.1:0", loopback.HandlerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	dispatcher := loopback.NewDispatcher(server.URL(), 40*time.Millisecond)
	_, bizErr := dispatcher.DispatchRaw(
		context.Background(),
		testDecision("h-late"),
		loopback.DispatchOptions{Inject: "late", DelayMS: 200},
		"req-late",
		1,
	)
	if bizErr == nil || bizErr.Code != core.CodeHealTimeout {
		t.Fatalf("expected %s for a late response, got %#v", core.CodeHealTimeout, bizErr)
	}
}
