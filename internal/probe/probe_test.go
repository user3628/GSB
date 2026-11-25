package probe_test

import (
	"context"
	"net"
	"testing"
	"time"

	"auditdrift/internal/drift"
	"auditdrift/internal/model"
	"auditdrift/internal/probe"
)

func TestRealHTTPProbeSuccessAndDriftConclusion(t *testing.T) {
	endpoint := probe.NewTestEndpoint()
	defer endpoint.Close()
	endpoint.SetValue("host-a", "svc", model.Value{Kind: model.KindString, Text: "Manual"}, time.Now().UnixMilli())
	client := probe.NewClient(endpoint.URL(), time.Second)
	obs, err := client.Probe(context.Background(), "host-a", "svc")
	if err != nil {
		t.Fatal(err)
	}
	baseline := model.Baseline{ID: "svc", Category: model.CategoryServiceStartup, SeverityWeight: 8, Expected: model.Expectation{ExpectedText: "Automatic", AllowedEnum: []string{"Automatic", "Manual", "Disabled"}}}
	conclusion, err := drift.EvaluateTarget("s", "host-a", baseline, nil, &drift.SourceValue{Source: obs.Source, Value: obs.Actual}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if conclusion.DriftCategory != model.DriftValueOffset {
		t.Fatalf("expected normal drift, got %s error=%s", conclusion.DriftCategory, conclusion.ErrorCode)
	}
}

func TestProbeTimeoutClosesConnectionAndReturnsCodedError(t *testing.T) {
	endpoint := probe.NewTestEndpoint()
	defer endpoint.Close()
	endpoint.SetDelay("slow", time.Second)
	tracker := probe.NewConnTracker()
	dialer := &net.Dialer{Timeout: time.Second}
	client := probe.NewClientWithConfig(probe.ClientConfig{
		BaseURL: endpoint.URL(), Timeout: 60 * time.Millisecond, DialContext: tracker.Wrap(dialer),
	})
	start := time.Now()
	_, err := client.Probe(context.Background(), "h", "slow")
	if model.CodeOf(err) != model.ErrProbeTimeout {
		t.Fatalf("expected timeout code, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("independent timeout took too long: %s", elapsed)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, closed, active := tracker.Stats()
		if closed >= 1 && active == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	opened, closed, active := tracker.Stats()
	t.Fatalf("connection not closed/released: opened=%d closed=%d active=%d", opened, closed, active)
}

func TestProbeRefused(t *testing.T) {
	url, err := probe.ClosedLoopbackURL()
	if err != nil {
		t.Fatal(err)
	}
	client := probe.NewClient(url, time.Second)
	_, err = client.Probe(context.Background(), "h", "b")
	if model.CodeOf(err) != model.ErrProbeRefused {
		t.Fatalf("expected refused, got %v", err)
	}
}
