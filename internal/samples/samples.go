// Package samples is the built-in deterministic generator. It produces all
// sample sets and key material locally; nothing is fetched from the network.
package samples

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"

	"iotrust/internal/engine"
)

// KeySeed derives the local signing key deterministically so reports are
// byte-identical across runs.
const KeySeed = "iotrust-local-deterministic-signing-key-v1"

// Key returns the deterministic local signing key.
func Key() []byte {
	sum := sha256.Sum256([]byte(KeySeed))
	return sum[:]
}

// KeyHex is the hex form of the local signing key, written to the work dir.
func KeyHex() string { return hex.EncodeToString(Key()) }

// Scenarios lists all built-in scenarios, including the six fault
// injections required for acceptance.
var Scenarios = []string{
	"normal",
	"fingerprint-dup",
	"score-overflow",
	"cred-expired",
	"snapshot-gap",
	"auth-contention",
	"budget-exhausted",
}

func fingerprint(deviceID string) string {
	sum := sha256.Sum256([]byte("fp:" + deviceID))
	return hex.EncodeToString(sum[:])
}

func ev(seq, delta, mult int64) engine.BehaviorEvent {
	return engine.BehaviorEvent{Seq: seq, Delta: delta, Mult: mult}
}

func dev(id, tenant string, risk, cpu, mem, time int64, caps []string, events []engine.BehaviorEvent) engine.DeviceClaim {
	return engine.DeviceClaim{
		DeviceID:     id,
		TenantID:     tenant,
		Fingerprint:  fingerprint(id),
		Capabilities: caps,
		Risk:         risk,
		NeedCPU:      cpu,
		NeedMem:      mem,
		NeedTime:     time,
		Events:       events,
	}
}

func defaultParams() engine.Params {
	return engine.Params{
		Concurrency:   4,
		GlobalCPU:     100000,
		GlobalMem:     100000,
		PerDeviceTime: 1000,
		MaxInFlight:   4,
		WaitQueueMax:  16,
	}
}

func policies() []engine.TenantPolicy {
	return []engine.TenantPolicy{
		{TenantID: "tenant-a", PolicyVersion: 1, TrustThreshold: 600000, AllowedCaps: []string{"read", "write", "telemetry"}, CredTTL: 1000},
		{TenantID: "tenant-b", PolicyVersion: 3, TrustThreshold: 300000, AllowedCaps: []string{"read"}, CredTTL: 500},
	}
}

func normalClaims() []engine.DeviceClaim {
	return []engine.DeviceClaim{
		dev("dev-a1", "tenant-a", 900, 100, 100, 10, []string{"read", "telemetry"}, []engine.BehaviorEvent{ev(1, 150000, 2), ev(2, 250000, 2)}),
		dev("dev-a2", "tenant-a", 700, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 250000, 3)}),
		dev("dev-a3", "tenant-a", 500, 100, 100, 10, []string{"read", "write"}, []engine.BehaviorEvent{ev(1, 325000, 2)}),
		dev("dev-b1", "tenant-b", 600, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 125000, 4)}),
		dev("dev-b2", "tenant-b", 400, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 300000, 3)}),
		dev("dev-b3", "tenant-b", 300, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 175000, 2)}),
	}
}

func normalClaims2() []engine.DeviceClaim {
	return []engine.DeviceClaim{
		dev("dev-a1", "tenant-a", 900, 100, 100, 10, []string{"read", "telemetry"}, []engine.BehaviorEvent{ev(1, 150000, 2), ev(2, 250000, 2), ev(3, 50000, 1)}),
		dev("dev-a2", "tenant-a", 700, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 250000, 3), ev(2, 10000, 5)}),
		dev("dev-a3", "tenant-a", 500, 100, 100, 10, []string{"read", "write"}, []engine.BehaviorEvent{ev(1, 325000, 2), ev(2, 25000, 2)}),
		dev("dev-b1", "tenant-b", 600, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 125000, 4), ev(2, 20000, 2)}),
		dev("dev-b2", "tenant-b", 400, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 300000, 3), ev(2, 5000, 4)}),
		dev("dev-b3", "tenant-b", 300, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 175000, 2), ev(2, 30000, 1)}),
	}
}

// Generate builds the deterministic sample set for a scenario.
func Generate(name string) (*engine.SampleSet, error) {
	base := &engine.SampleSet{
		Name:     name,
		Rounds:   1,
		Now:      1000,
		VerifyAt: 1500,
		Params:   defaultParams(),
		Policies: policies(),
	}
	switch name {
	case "normal":
		base.Rounds = 2
		base.Claims = normalClaims()
		base.Claims2 = normalClaims2()
	case "fingerprint-dup":
		claims := []engine.DeviceClaim{
			dev("dev-a1", "tenant-a", 900, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 300000, 3)}),
			dev("dev-x2", "tenant-a", 800, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 300000, 3)}),
			dev("dev-a3", "tenant-a", 700, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 300000, 3)}),
		}
		claims[1].Fingerprint = fingerprint("dev-a1")
		base.Claims = claims
	case "score-overflow":
		base.Claims = []engine.DeviceClaim{
			dev("dev-o1", "tenant-a", 900, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, math.MaxInt64, 2)}),
			dev("dev-o2", "tenant-a", 800, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 300000, 3)}),
		}
	case "cred-expired":
		base.Now = 1000
		base.VerifyAt = 5000
		base.Claims = []engine.DeviceClaim{
			dev("dev-e1", "tenant-a", 900, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
			dev("dev-e2", "tenant-a", 800, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 400000, 2)}),
		}
	case "snapshot-gap":
		base.Claims = []engine.DeviceClaim{
			dev("dev-s1", "tenant-a", 900, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
			dev("dev-s2", "tenant-b", 800, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 200000, 2)}),
		}
	case "auth-contention":
		base.Contention = 8
		base.Claims = []engine.DeviceClaim{
			dev("dev-c1", "tenant-a", 900, 100, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
		}
	case "budget-exhausted":
		base.Params = engine.Params{
			Concurrency:   2,
			GlobalCPU:     10000,
			GlobalMem:     10000,
			PerDeviceTime: 100,
			MaxInFlight:   2,
			WaitQueueMax:  8,
		}
		base.Claims = []engine.DeviceClaim{
			dev("dev-g1", "tenant-a", 900, 4000, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
			dev("dev-g2", "tenant-a", 800, 4000, 100, 500, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
			dev("dev-g3", "tenant-a", 700, 4000, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
			dev("dev-g4", "tenant-a", 600, 4000, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
			dev("dev-g5", "tenant-a", 500, 4000, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
			dev("dev-g6", "tenant-a", 400, 4000, 100, 10, []string{"read"}, []engine.BehaviorEvent{ev(1, 350000, 2)}),
		}
	default:
		return nil, fmt.Errorf("unknown scenario %q", name)
	}
	return base, nil
}
