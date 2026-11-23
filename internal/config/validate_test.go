package config

import (
	"math"
	"testing"

	"upstreampool/internal/model"
)

func TestDurationOverflowRejected(t *testing.T) {
	if _, err := DurationMS(math.MaxInt64, 1); err == nil {
		t.Fatal("expected overflow error")
	}
	got, err := DurationMS(1000, 10000)
	if err != nil || got != 100 {
		t.Fatalf("DurationMS = %d, %v", got, err)
	}
}

func TestEndpointValidation(t *testing.T) {
	d := model.Defaults{QueueWaitTimeoutMS: 10, QueueLimit: 2, DialTimeoutMS: 10, HandshakeMS: 1, DrainMS: 1, ResponseBaseMS: 1, ResponseRateBPS: 1000, WriteTimeoutMS: 100, ReadTimeoutMS: 100, RateChunkBytes: 1024}
	ep := model.EndpointSpec{ID: "e", Address: "127.0.0.1:1", MaxConnections: 1, MaxReuses: 1, IdleReclaimMS: 0, Health: "up", QueuePolicy: "queue"}
	if err := ValidateEndpoint(&ep, &d); err != nil {
		t.Fatal(err)
	}
	if ep.Health != "UP" || ep.QueuePolicy != "QUEUE" {
		t.Fatalf("not normalized: %+v", ep)
	}
	ep.Address = "203.0.113.10:80"
	if err := ValidateEndpoint(&ep, &d); err == nil {
		t.Fatal("non-loopback target must be rejected")
	}
}
