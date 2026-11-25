package sample

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"nonceguard/internal/canonical"
	"nonceguard/internal/model"
)

const DemoReferenceMs int64 = 1795507200000

func deterministicNonce(label string) string {
	sum := sha256.Sum256(append([]byte("nonce|"+label+"|"), canonical.FixedSeed...))
	return hex.EncodeToString(sum[:16])
}

func signOrPanic(req *model.Request, fields []string) {
	sig, err := canonical.SignRequest(*req, fields)
	if err != nil {
		panic(err)
	}
	req.SignatureDigest = sig
}

func signed(id string, ts int64, route, client, nonce string, weight int64, fields []string) model.Request {
	req := model.Request{
		RequestID:   id,
		TimestampMs: ts,
		RouteKey:    route,
		ClientID:    client,
		Nonce:       nonce,
		Weight:      weight,
	}
	signOrPanic(&req, fields)
	return req
}

func CanonicalBatch(referenceMs int64) model.Batch {
	if referenceMs <= 0 {
		referenceMs = DemoReferenceMs
	}
	fields := model.DefaultSnapshot(1, referenceMs).SignatureFields
	nonceA := deterministicNonce("client-a-alpha")
	nonceB := deterministicNonce("client-a-beta")
	nonceC := deterministicNonce("client-b-gamma")

	okAlpha := signed("req-ok-alpha", referenceMs+1_000, "/v1/orders", "client-a", nonceA, 10, fields)
	okBeta := signed("req-ok-beta", referenceMs+2_000, "/v1/payments", "client-a", nonceB, 20, fields)

	badSig := signed("req-bad-signature", referenceMs+3_000, "/v1/refunds", "client-a", deterministicNonce("bad-signature"), 30, fields)
	if badSig.SignatureDigest[len(badSig.SignatureDigest)-1] == '0' {
		badSig.SignatureDigest = badSig.SignatureDigest[:63] + "1"
	} else {
		badSig.SignatureDigest = badSig.SignatureDigest[:63] + "0"
	}

	early := signed("req-timestamp-early", referenceMs-300_001, "/v1/orders", "client-a", deterministicNonce("early"), 40, fields)
	late := signed("req-timestamp-late", referenceMs+300_001, "/v1/orders", "client-a", deterministicNonce("late"), 50, fields)
	replay := signed("req-nonce-replay-alpha", referenceMs+4_000, "/v1/orders", "client-a", nonceA, 11, fields)
	gamma := signed("req-ok-gamma", referenceMs+5_000, "/v1/settlements", "client-b", nonceC, 60, fields)

	duplicate := okAlpha

	conflict := signed("req-ok-beta", referenceMs+2_000, "/v1/payments", "client-a", nonceB, 999, fields)

	fastDelta := signed("req-fast-delta", referenceMs+6_000, "/v1/inventory", "client-c", deterministicNonce("fast-delta"), 70, fields)

	return model.Batch{
		BatchID:     "demo-canonical-batch",
		ReferenceMs: referenceMs,
		Mode:        model.ModeFull,
		Requests: []model.Request{
			okAlpha,
			okBeta,
			badSig,
			early,
			late,
			replay,
			gamma,
			duplicate,
			conflict,
			fastDelta,
		},
	}
}

func ValidBatch(batchID string, count int, referenceMs int64) model.Batch {
	if referenceMs <= 0 {
		referenceMs = DemoReferenceMs
	}
	if count < 1 {
		count = 1
	}
	fields := model.DefaultSnapshot(1, referenceMs).SignatureFields
	reqs := make([]model.Request, 0, count)
	for i := 0; i < count; i++ {
		label := fmt.Sprintf("%s-%06d", batchID, i)
		reqs = append(reqs, signed(
			fmt.Sprintf("req-%06d", i+1),
			referenceMs+int64(i%120)*1000,
			fmt.Sprintf("/routes/%02d", i%8),
			fmt.Sprintf("client-%02d", i%4),
			deterministicNonce(label),
			int64(1+i%37),
			fields,
		))
	}
	return model.Batch{BatchID: batchID, ReferenceMs: referenceMs, Mode: model.ModeFull, Requests: reqs}
}

func RecycleBatches(referenceMs int64) (first model.Batch, activeRepeat model.Batch, boundaryRepeat model.Batch, expiredRepeat model.Batch) {
	if referenceMs <= 0 {
		referenceMs = DemoReferenceMs
	}
	fields := model.DefaultSnapshot(1, referenceMs).SignatureFields
	nonce := deterministicNonce("recycle-nonce")
	firstReq := signed("req-recycle-first", referenceMs, "/v1/recycle", "client-recycle", nonce, 1, fields)
	first = model.Batch{BatchID: "demo-recycle-first", ReferenceMs: referenceMs, Requests: []model.Request{firstReq}}

	mk := func(id string, at int64) model.Batch {
		req := signed(id, at, "/v1/recycle", "client-recycle", nonce, 2, fields)
		return model.Batch{BatchID: id + "-batch", ReferenceMs: at, Requests: []model.Request{req}}
	}
	activeRepeat = mk("req-recycle-active", referenceMs+60_000)
	boundaryRepeat = mk("req-recycle-boundary", referenceMs+600_000)
	expiredRepeat = mk("req-recycle-expired", referenceMs+600_001)
	return
}

func InvalidBatchJSON() []byte {
	return []byte(`{
  "batch_id":"demo-invalid-batch",
  "reference_ms":` + fmt.Sprint(DemoReferenceMs) + `,
  "requests":[
    {
      "request_id":"req-bad-nonce",
      "timestamp_ms":` + fmt.Sprint(DemoReferenceMs+1000) + `,
      "route_key":"/v1/invalid",
      "client_id":"client-x",
      "nonce":"bad!",
      "signature_digest":"0000000000000000000000000000000000000000000000000000000000000000",
      "weight":0
    },
    {
      "request_id":"req-weight-overflow",
      "timestamp_ms":` + fmt.Sprint(DemoReferenceMs+2000) + `,
      "route_key":"/v1/invalid",
      "client_id":"client-x",
      "nonce":"0123456789abcdef0123456789abcdef",
      "signature_digest":"1111111111111111111111111111111111111111111111111111111111111111",
      "weight":999999999999999999999999999999999
    }
  ]
}`)
}

func ConfigVersionTwo(referenceMs int64) model.Snapshot {
	cfg := model.DefaultSnapshot(2, referenceMs)
	cfg.MinOffsetMs = -120_000
	cfg.MaxOffsetMs = 180_000
	cfg.NonceWindowMs = 420_000
	cfg.Digest = canonical.ConfigDigest(cfg.DigestInput())
	return cfg
}

func GappedConfig(current model.Snapshot) model.Snapshot {
	cfg := model.DefaultSnapshot(current.Version+2, current.GeneratedAtMs+1)
	cfg.MinOffsetMs = current.MinOffsetMs
	cfg.MaxOffsetMs = current.MaxOffsetMs
	cfg.NonceWindowMs = current.NonceWindowMs
	cfg.SignatureFields = append([]string(nil), current.SignatureFields...)
	cfg.Digest = canonical.ConfigDigest(cfg.DigestInput())
	return cfg
}
