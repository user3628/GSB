package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"gatewayfreshness.local/workbench/internal/config"
	"gatewayfreshness.local/workbench/internal/freshness"
)

const baseRef = int64(1800000000000)

type batchFile struct {
	Filename               string                   `json:"-"`
	SchemaVersion          int                      `json:"schema_version"`
	BatchID                string                   `json:"batch_id"`
	Mode                   string                   `json:"mode"`
	GatewayReferenceTimeMS *int64                   `json:"gateway_reference_time_ms,omitempty"`
	Requests               []freshness.RequestInput `json:"requests"`
}

type reqSpec struct {
	id          string
	ts          int64
	route       string
	client      string
	nonce       string
	weight      int64
	toleranceMS int64
	windowMS    int64
	badSig      bool
}

func main() {
	configPath := flag.String("config", "samples/config.json", "config path")
	outDir := flag.String("out", "samples/batches", "sample output directory")
	flag.Parse()
	cfg, err := config.Load(*configPath)
	if err != nil {
		fail(err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fail(err)
	}
	for _, sample := range buildSamples(cfg.SigningKeyBytes()) {
		data, err := json.MarshalIndent(sample, "", "  ")
		if err != nil {
			fail(err)
		}
		data = append(data, '\n')
		name := sample.Filename
		if name == "" {
			name = sample.BatchID + ".json"
		}
		if err := os.WriteFile(filepath.Join(*outDir, name), data, 0o644); err != nil {
			fail(err)
		}
	}
	fmt.Printf("wrote deterministic samples to %s\n", *outDir)
}

func buildSamples(key []byte) []batchFile {
	mk := func(batchID, mode string, ref int64, specs ...reqSpec) batchFile {
		reqs := make([]freshness.RequestInput, 0, len(specs))
		for _, sp := range specs {
			r := freshness.Request{
				RequestID: sp.id, TimestampMS: sp.ts, RouteKey: sp.route, ClientID: sp.client,
				Nonce: sp.nonce, Weight: sp.weight, ToleranceMS: sp.toleranceMS, WindowMS: sp.windowMS,
			}
			sig := freshness.Signature(key, r)
			if sp.badSig {
				sig = "0000000000000000000000000000000000000000000000000000000000000000"
			}
			reqs = append(reqs, freshness.RequestInput{
				RequestID: sp.id, TimestampMS: sp.ts, RouteKey: sp.route, ClientID: sp.client,
				Nonce: sp.nonce, Signature: sig, Weight: sp.weight, ToleranceMS: sp.toleranceMS, WindowMS: sp.windowMS,
			})
		}
		refCopy := ref
		return batchFile{Filename: batchID + ".json", SchemaVersion: 1, BatchID: batchID, Mode: mode, GatewayReferenceTimeMS: &refCopy, Requests: reqs}
	}

	alpha := func(id string, ts, tolerance, window int64) reqSpec {
		return reqSpec{id: id, ts: ts, route: "orders.create", client: "client-alpha", nonce: "nonce-alpha-00000001", weight: 2, toleranceMS: tolerance, windowMS: window}
	}
	return []batchFile{
		mk("01-sample-accept-001", freshness.ModeFull, baseRef,
			alpha("req-alpha-accept", baseRef+100, 500, 5000),
			reqSpec{id: "req-beta-accept", ts: baseRef - 500, route: "payments.capture", client: "client-beta", nonce: "nonce-beta-000000001", weight: 1, toleranceMS: 500, windowMS: 3000},
		),
		mk("02-sample-boundary-002", freshness.ModeFull, baseRef,
			reqSpec{id: "req-upper-pass", ts: baseRef + 500, route: "boundary.upper", client: "client-gamma", nonce: "nonce-gamma-000000001", weight: 1, toleranceMS: 500, windowMS: 2000},
			reqSpec{id: "req-upper-fail", ts: baseRef + 501, route: "boundary.upper", client: "client-gamma", nonce: "nonce-gamma-000000002", weight: 1, toleranceMS: 500, windowMS: 2000},
			reqSpec{id: "req-lower-pass", ts: baseRef - 500, route: "boundary.lower", client: "client-delta", nonce: "nonce-delta-000000001", weight: 1, toleranceMS: 500, windowMS: 2000},
			reqSpec{id: "req-lower-fail", ts: baseRef - 501, route: "boundary.lower", client: "client-epsilon", nonce: "nonce-epsilon-00000001", weight: 1, toleranceMS: 500, windowMS: 2000},
		),
		mk("03-sample-signature-003", freshness.ModeFull, baseRef,
			reqSpec{id: "req-bad-signature", ts: baseRef, route: "signature.check", client: "client-zeta", nonce: "nonce-zeta-0000000001", weight: 1, toleranceMS: 500, windowMS: 5000, badSig: true},
		),
		mk("04-sample-replay-004", freshness.ModeFull, baseRef+100,
			alpha("req-alpha-replay", baseRef+100, 500, 5000),
		),
		mk("05-sample-fast-005", freshness.ModeFast, baseRef+200,
			reqSpec{id: "req-fast-001", ts: baseRef + 200, route: "fast.precheck", client: "client-fast", nonce: "nonce-fast-0000000001", weight: 1, toleranceMS: 500, windowMS: 5000},
			reqSpec{id: "req-fast-002", ts: baseRef + 200, route: "fast.precheck", client: "client-fast", nonce: "nonce-fast-0000000002", weight: 1, toleranceMS: 500, windowMS: 5000},
			reqSpec{id: "req-fast-suffix-bad", ts: baseRef + 200, route: "fast.suffix", client: "client-fast", nonce: "nonce-fast-0000000003", weight: 1, toleranceMS: 500, windowMS: 5000, badSig: true},
			reqSpec{id: "req-fast-suffix-004", ts: baseRef + 200, route: "fast.suffix", client: "client-fast", nonce: "nonce-fast-0000000004", weight: 1, toleranceMS: 500, windowMS: 5000},
		),
		mk("06-sample-reclaim-base-006", freshness.ModeFull, baseRef+6000,
			alpha("req-alpha-reclaim-base", baseRef+6000, 100, 1000),
		),
		mk("07-sample-reclaim-trigger-007", freshness.ModeFull, baseRef+8000,
			reqSpec{id: "req-reclaim-trigger", ts: baseRef + 8000, route: "maintenance.reclaim", client: "client-eta", nonce: "nonce-eta-00000000001", weight: 1, toleranceMS: 100, windowMS: 1000},
		),
		mk("08-sample-reoccupy-008", freshness.ModeFull, baseRef+9000,
			alpha("req-alpha-reoccupy", baseRef+9000, 100, 1000),
		),
		mk("09-sample-recovery-009", freshness.ModeFull, baseRef+10000,
			reqSpec{id: "req-recovery", ts: baseRef + 10000, route: "recovery.probe", client: "client-recovery", nonce: "nonce-recovery-0000001", weight: 1, toleranceMS: 100, windowMS: 5000},
		),
		mk("10-sample-mode-full-010", freshness.ModeFull, baseRef+11000,
			reqSpec{id: "req-mode-001", ts: baseRef + 11000, route: "mode.compare", client: "client-mode", nonce: "nonce-mode-0000000001", weight: 1, toleranceMS: 100, windowMS: 5000},
			reqSpec{id: "req-mode-002", ts: baseRef + 11000, route: "mode.compare", client: "client-mode", nonce: "nonce-mode-0000000002", weight: 1, toleranceMS: 100, windowMS: 5000},
			reqSpec{id: "req-mode-003", ts: baseRef + 11000, route: "mode.compare", client: "client-mode", nonce: "nonce-mode-0000000003", weight: 1, toleranceMS: 100, windowMS: 5000},
			reqSpec{id: "req-mode-004", ts: baseRef + 11000, route: "mode.compare", client: "client-mode", nonce: "nonce-mode-0000000004", weight: 1, toleranceMS: 100, windowMS: 5000},
		),
		func() batchFile {
			sample := mk("01-sample-accept-001-conflict", freshness.ModeFull, baseRef,
				alpha("req-alpha-accept", baseRef+100, 500, 5000),
			)
			sample.BatchID = "01-sample-accept-001"
			sample.Filename = "conflict-same-id-001.json"
			// same batch id, but different weight/content => E_CONFLICTING_REQUEST
			sample.Requests[0].Weight = 3
			r := freshness.Request{
				RequestID: sample.Requests[0].RequestID, TimestampMS: sample.Requests[0].TimestampMS,
				RouteKey: sample.Requests[0].RouteKey, ClientID: sample.Requests[0].ClientID,
				Nonce: sample.Requests[0].Nonce, Weight: sample.Requests[0].Weight,
				ToleranceMS: sample.Requests[0].ToleranceMS, WindowMS: sample.Requests[0].WindowMS,
			}
			sample.Requests[0].Signature = freshness.Signature(key, r)
			return sample
		}(),
		mk("11-sample-invalid-nonce", freshness.ModeFull, baseRef,
			reqSpec{id: "req-invalid-nonce", ts: baseRef, route: "invalid.nonce", client: "client-invalid", nonce: "short", weight: 1, toleranceMS: 500, windowMS: 5000},
		),
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
