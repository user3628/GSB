// Package sample deterministically generates the loopback registration fixture
// used by tests, the CLI verifier and the interactive workbench.
package sample

import (
	"math/rand"
	"sort"

	"registrylab/registry"
	"registrylab/testfixture"
)

// Options are all inputs to the fixed-seed generator. Coherent() options with
// the same seed/version/addresses always produce byte-identical samples.
type Options struct {
	Seed        int64
	Version     int64
	GeneratedAt string
	BaseURL     string
	TCPAddr     string
	RefusedAddr string
}

// Generate returns a digest-complete snapshot in canonical identity order.
func Generate(opts Options) registry.SnapshotRequest {
	if opts.Seed == 0 {
		opts.Seed = 20261126
	}
	if opts.Version == 0 {
		opts.Version = 1
	}
	if opts.GeneratedAt == "" {
		opts.GeneratedAt = "2026-11-26T00:00:00Z"
	}
	rng := rand.New(rand.NewSource(opts.Seed))
	base := trimRight(opts.BaseURL)

	makeInstance := func(service, id string, probe registry.ProbeSpec, endpoint string, ttl int64) registry.Instance {
		return registry.Instance{
			ServiceName: service, InstanceID: id, Endpoint: endpoint,
			Weight: 100 + rng.Intn(901), LeaseTTLMS: ttl, Probe: probe,
			Priority: rng.Intn(101), SuccessThreshold: 1 + rng.Intn(2), FailureThreshold: 1 + rng.Intn(2),
		}
	}
	httpProbe := func(path string, status int, marker string, timeout int) registry.ProbeSpec {
		return registry.ProbeSpec{Kind: registry.ProbeHTTP, Path: path, ExpectedStatus: status,
			ExpectedMarker: marker, TimeoutMS: timeout}
	}

	instances := []registry.Instance{
		makeInstance("billing", "billing-http-ok", httpProbe("/probe/ok", 200, testfixture.BodyOK, 350), base, 3000),
		makeInstance("billing", "billing-http-bad-status", httpProbe("/probe/bad-status", 200, testfixture.BodyOK, 350), base, 3000),
		makeInstance("billing", "billing-lease-1", registry.ProbeSpec{Kind: registry.ProbeLease, TimeoutMS: 100}, base+"/heartbeat", 1200),
		makeInstance("catalog", "catalog-http-ok", httpProbe("/probe/ok", 200, testfixture.BodyOK, 350), base, 3000),
		makeInstance("catalog", "catalog-http-slow", httpProbe("/probe/slow", 200, testfixture.BodyOK, 90), base, 3000),
		makeInstance("catalog", "catalog-bad-marker", httpProbe("/probe/bad-marker", 200, testfixture.BodyOK, 350), base, 3000),
		makeInstance("orders", "orders-http-ok", httpProbe("/probe/ok", 200, testfixture.BodyOK, 350), base, 3000),
		makeInstance("orders", "orders-tcp-ok", registry.ProbeSpec{Kind: registry.ProbeTCP, TimeoutMS: 200}, opts.TCPAddr, 3000),
		makeInstance("orders", "orders-tcp-refused", registry.ProbeSpec{Kind: registry.ProbeTCP, TimeoutMS: 200}, opts.RefusedAddr, 3000),
	}
	sort.Slice(instances, func(i, j int) bool {
		if instances[i].ServiceName != instances[j].ServiceName {
			return instances[i].ServiceName < instances[j].ServiceName
		}
		return instances[i].InstanceID < instances[j].InstanceID
	})
	return registry.SnapshotRequest{
		Version: opts.Version, GeneratedAt: opts.GeneratedAt, Instances: instances,
		Digest: registry.ComputeDigest(opts.Version, opts.GeneratedAt, instances),
	}
}

// DeltaExample returns a small deterministic delta against a base version.
func DeltaExample(base int64, version int64, baseURL string) registry.DeltaRequest {
	return registry.DeltaRequest{
		BaseVersion: base, Version: version,
		Upserts: []registry.Instance{{
			ServiceName: "catalog", InstanceID: "catalog-http-ok", Endpoint: baseURL,
			Weight: 777, LeaseTTLMS: 2500, Priority: 50, SuccessThreshold: 1, FailureThreshold: 1,
			Probe: registry.ProbeSpec{Kind: registry.ProbeHTTP, Path: "/probe/ok", ExpectedStatus: 200,
				ExpectedMarker: testfixture.BodyOK, TimeoutMS: 250},
		}},
	}
}

func trimRight(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
