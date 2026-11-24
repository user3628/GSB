package sample

import (
	"encoding/base64"
	"time"

	"mitmpipeline/internal/codec"
	"mitmpipeline/internal/domain"
)

type Fixture struct {
	Good     *Bundle
	Attacker *Bundle

	Accepted       domain.Record
	IdentityAlias  domain.Record
	JSONAccepted   domain.Record
	Degraded       domain.Record
	HotUpdate      domain.Record
	MITM           domain.Record
	Aborted        domain.Record
	ChainInvalid   domain.Record
	CrashCandidate domain.Record

	UnsupportedJSON []byte
	CorruptBinary   []byte
}

func NewFixture() *Fixture {
	good := MustBundle("fixed-seed/good/v1", "Good", "good.internal")
	attacker := MustBundle("fixed-seed/attacker/v1", "Attacker", "mitm.internal")
	startMS := FixedTime.UnixMilli()
	zeroPin := "sha256:" + base64.StdEncoding.EncodeToString(make([]byte, 32))

	accepted := domain.Record{
		Format: domain.FormatBinary, FormatVersion: 2,
		ConnectionID: "ACCEPT-20251124-0001", SNI: "good.internal", ProtocolVersion: "TLS 1.3",
		ObservedStartMS: startMS, CertChainPEM: good.ChainPEM,
		Policy: domain.Policy{Version: "pol-accepted-v1", Mode: domain.PolicyModeSPKI, SPKIPins: []string{good.LeafPin}, TrustedRoots: []string{good.RootCanonical}},
		Events: completeEvents(1_000_000, 12_250_000, 24_650_000, 40_499_999),
	}

	identityAlias := accepted
	identityAlias.ConnectionID = "accept-20251124-0001"
	identityAlias.SNI = "GOOD.INTERNAL."
	identityAlias.Format = domain.FormatJSONL
	identityAlias.FormatVersion = 2

	jsonAccepted := accepted
	jsonAccepted.ConnectionID = "JSON-ACCEPT-20251124-0002"
	jsonAccepted.Format = domain.FormatJSONL
	jsonAccepted.Events = absoluteJSONEvents(FixedTime, []time.Duration{
		time.Millisecond, 12*time.Millisecond + 250*time.Microsecond,
		24*time.Millisecond + 650*time.Microsecond, 40*time.Millisecond + 499*time.Microsecond,
	}, true)

	degraded := domain.Record{
		Format: domain.FormatJSONL, FormatVersion: 1,
		ConnectionID: "degrade-legacy-0001", SNI: "good.internal",
		ObservedStartMS: startMS, CertChainPEM: good.ChainPEM,
		Policy: domain.Policy{Version: "pol-legacy-root", Mode: domain.PolicyModeRoot, TrustedRoots: []string{good.RootCanonical}},
		Events: absoluteJSONEvents(FixedTime.Truncate(time.Second).Add(time.Second), []time.Duration{
			0, time.Second, 2 * time.Second, 3 * time.Second,
		}, false),
	}
	degraded.ProtocolVersion = ""

	hot := domain.Record{
		Format: domain.FormatBinary, FormatVersion: 2,
		ConnectionID: "hot-update-0001", SNI: "good.internal", ProtocolVersion: "TLS 1.3",
		ObservedStartMS: startMS, CertChainPEM: good.ChainPEM,
		Policy: domain.Policy{Version: "pol-hot-v1", Mode: domain.PolicyModeSPKI, SPKIPins: []string{good.LeafPin}, TrustedRoots: []string{good.RootCanonical}},
		PolicyTimeline: []domain.PolicyTimelineEntry{{
			Policy:            domain.Policy{Version: "pol-hot-v2", Mode: domain.PolicyModeRoot, TrustedRoots: []string{zeroPin}},
			EffectiveOffsetNS: 25_000_000,
		}},
		Events: []domain.Event{
			{Seq: 1, Type: domain.EventHandshakeStart, OffsetNS: 1_000_000},
			{Seq: 2, Type: domain.EventCertificateReceived, OffsetNS: 10_000_000},
			{Seq: 3, Type: domain.EventPinVerification, OffsetNS: 20_123_456},
			{Seq: 4, Type: domain.EventPinVerification, OffsetNS: 30_650_499},
			{Seq: 5, Type: domain.EventHandshakeComplete, OffsetNS: 40_999_999},
		},
	}

	mitm := domain.Record{
		Format: domain.FormatBinary, FormatVersion: 2,
		ConnectionID: "mitm-bad-root-0001", SNI: "mitm.internal", ProtocolVersion: "TLS 1.3",
		ObservedStartMS: startMS, CertChainPEM: attacker.ChainPEM,
		Policy: domain.Policy{Version: "pol-mitm-v1", Mode: domain.PolicyModeSPKI, SPKIPins: []string{zeroPin}, TrustedRoots: []string{good.RootCanonical}},
		Events: completeEvents(1_000_000, 11_000_000, 22_000_777, 35_000_000),
	}

	aborted := domain.Record{
		Format: domain.FormatBinary, FormatVersion: 2,
		ConnectionID: "aborted-0001", SNI: "good.internal", ProtocolVersion: "TLS 1.3",
		ObservedStartMS: startMS, CertChainPEM: good.ChainPEM,
		Policy: domain.Policy{Version: "pol-abort-v1", Mode: domain.PolicyModeSPKI, SPKIPins: []string{good.LeafPin}, TrustedRoots: []string{good.RootCanonical}},
		Events: []domain.Event{
			{Seq: 1, Type: domain.EventHandshakeStart, OffsetNS: 1_000_000},
			{Seq: 2, Type: domain.EventCertificateReceived, OffsetNS: 10_000_000},
			{Seq: 3, Type: domain.EventPinVerification, OffsetNS: 20_000_000},
			{Seq: 4, Type: domain.EventAbnormalTermination, OffsetNS: 21_000_000},
		},
	}

	crashCandidate := mitm
	crashCandidate.ConnectionID = "crash-recovery-0001"

	chainInvalid := domain.Record{
		Format: domain.FormatBinary, FormatVersion: 2,
		ConnectionID: "chain-missing-root-0001", SNI: "good.internal", ProtocolVersion: "TLS 1.3",
		ObservedStartMS: startMS, CertChainPEM: good.LeafPEM + good.IntermediatePEM,
		Policy: domain.Policy{Version: "pol-chain-invalid-v1", Mode: domain.PolicyModeRoot, TrustedRoots: []string{good.RootCanonical}},
		Events: completeEvents(1_000_000, 10_000_000, 20_000_000, 30_000_000),
	}

	corruptSource, err := codec.EncodeBinary([]domain.Record{accepted}, 2)
	if err != nil {
		panic(err)
	}
	corrupt := append([]byte(nil), corruptSource...)
	corrupt[len(corrupt)-2] ^= 0xff

	return &Fixture{
		Good: good, Attacker: attacker,
		Accepted: accepted, IdentityAlias: identityAlias, JSONAccepted: jsonAccepted,
		Degraded: degraded, HotUpdate: hot, MITM: mitm, Aborted: aborted, ChainInvalid: chainInvalid,
		CrashCandidate:  crashCandidate,
		UnsupportedJSON: []byte(`{"format":"jsonl","format_version":99,"connection_id":"future-001","quantum_required":{"algorithm":"unknown-pq"}}` + "\n"),
		CorruptBinary:   corrupt,
	}
}

func completeEvents(start, cert, pin, complete int64) []domain.Event {
	return []domain.Event{
		{Seq: 1, Type: domain.EventHandshakeStart, OffsetNS: start},
		{Seq: 2, Type: domain.EventCertificateReceived, OffsetNS: cert},
		{Seq: 3, Type: domain.EventPinVerification, OffsetNS: pin},
		{Seq: 4, Type: domain.EventHandshakeComplete, OffsetNS: complete},
	}
}

func absoluteJSONEvents(base time.Time, offsets []time.Duration, nano bool) []domain.Event {
	types := []string{
		domain.EventHandshakeStart, domain.EventCertificateReceived,
		domain.EventPinVerification, domain.EventHandshakeComplete,
	}
	events := make([]domain.Event, 0, len(offsets))
	for i, offset := range offsets {
		when := base.Add(offset).UTC()
		text := when.Format(time.RFC3339Nano)
		if !nano {
			text = when.Format(time.RFC3339)
		}
		events = append(events, domain.Event{Seq: i + 1, Type: types[i], TimeRFC3339: text})
	}
	return events
}
