package pki_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/pki"
	"mitmpipeline/internal/sample"
)

func testAtMS() int64 { return sample.FixedTime.Add(25 * time.Millisecond).UnixMilli() }

func TestShuffledChainAndPinHit(t *testing.T) {
	fx := sample.NewFixture()
	policy := domain.Policy{
		Version: "test-v1", Mode: domain.PolicyModeSPKI,
		SPKIPins: []string{fx.Good.LeafPin}, TrustedRoots: []string{fx.Good.RootCanonical},
	}
	report, err := pki.EvaluateChain(fx.Good.ShuffledPEM, pki.EvaluateOptions{SNI: "good.internal", AtMS: testAtMS(), Policy: policy})
	if err != nil {
		t.Fatalf("expected shuffled chain to validate: %v", err)
	}
	if !report.PolicyMatched || !report.RootTrusted || !report.SNIMatched {
		t.Fatalf("expected pin/root/SNI match: %+v", report)
	}
	if len(report.OrderedSerials) != 3 {
		t.Fatalf("expected leaf/intermediate/root order, got %v", report.OrderedSerials)
	}
	if len(report.HitEntries) != 1 || len(report.MissedEntries) != 0 {
		t.Fatalf("unexpected entry results hit=%v miss=%v", report.HitEntries, report.MissedEntries)
	}
	for _, cert := range report.Certificates {
		if cert.ValidityRemainingMS <= 0 {
			t.Fatalf("expected positive validity margin: %+v", cert)
		}
	}
}

func TestMissingRootIsChainInvalidNotMITM(t *testing.T) {
	fx := sample.NewFixture()
	policy := domain.Policy{Mode: domain.PolicyModeRoot, TrustedRoots: []string{fx.Good.RootCanonical}}
	_, err := pki.EvaluateChain(fx.Good.LeafPEM+fx.Good.IntermediatePEM, pki.EvaluateOptions{SNI: "good.internal", AtMS: testAtMS(), Policy: policy})
	if err == nil || err.Code != domain.CodeChainInvalid {
		t.Fatalf("expected E_CHAIN_INVALID, got %v", err)
	}
}

func TestUntrustedRootAndPinMissClassifyMITM(t *testing.T) {
	fx := sample.NewFixture()
	policy := domain.Policy{Mode: domain.PolicyModeSPKI, SPKIPins: []string{"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}, TrustedRoots: []string{fx.Good.RootCanonical}}
	report, err := pki.EvaluateChain(fx.Attacker.ChainPEM, pki.EvaluateOptions{SNI: "mitm.internal", AtMS: testAtMS(), Policy: policy})
	if err != nil {
		t.Fatalf("untrusted-but-present root is an adjudication result, not chain error: %v", err)
	}
	if report.PolicyMatched || report.RootTrusted {
		t.Fatalf("expected policy miss and untrusted root: %+v", report)
	}
}

func TestUnsupportedSignatureIsChainInvalid(t *testing.T) {
	fx := sample.NewFixture()
	certs, err := pki.ParseCertificates(fx.Good.ChainPEM)
	if err != nil {
		t.Fatal(err)
	}
	for _, cert := range certs {
		if !cert.IsCA {
			cert.SignatureAlgorithm = x509.SHA1WithRSA
		}
	}
	_, perr := pki.EvaluateCertificates(certs, pki.EvaluateOptions{AtMS: testAtMS(), Policy: domain.Policy{Mode: domain.PolicyModeSPKI}})
	if perr == nil || perr.Code != domain.CodeChainInvalid {
		t.Fatalf("expected unsupported signature E_CHAIN_INVALID, got %v", perr)
	}
}

func TestIssuerCycleRejected(t *testing.T) {
	now := sample.FixedTime
	_, key1, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub1 := key1.Public().(ed25519.PublicKey)
	_, key2, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub2 := key2.Public().(ed25519.PublicKey)

	tpl1 := caTemplateForTest("Cycle CA 1", big.NewInt(11), now)
	tpl2 := caTemplateForTest("Cycle CA 2", big.NewInt(22), now)
	der1, err := x509.CreateCertificate(rand.Reader, tpl1, tpl2, pub1, key2)
	if err != nil {
		t.Fatal(err)
	}
	der2, err := x509.CreateCertificate(rand.Reader, tpl2, tpl1, pub2, key1)
	if err != nil {
		t.Fatal(err)
	}
	cert1, err := x509.ParseCertificate(der1)
	if err != nil {
		t.Fatal(err)
	}
	cert2, err := x509.ParseCertificate(der2)
	if err != nil {
		t.Fatal(err)
	}

	_, leafKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(33), Subject: pkix.Name{CommonName: "cycle-leaf"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, cert1, leafKey.Public(), key1)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatal(err)
	}

	_, perr := pki.EvaluateCertificates([]*x509.Certificate{leaf, cert1, cert2}, pki.EvaluateOptions{
		AtMS: testAtMS(), Policy: domain.Policy{Mode: domain.PolicyModeRoot},
	})
	if perr == nil || perr.Code != domain.CodeChainInvalid {
		t.Fatalf("expected cycle E_CHAIN_INVALID, got %v", perr)
	}
}

func TestCrossSignedIntermediateBuildsToTrustedRoot(t *testing.T) {
	fx := sample.NewFixture()
	_, altPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	altTemplate := caTemplateForTest("Alternate Trusted Root", big.NewInt(99), sample.FixedTime)
	altDER, err := x509.CreateCertificate(rand.Reader, altTemplate, altTemplate, altPrivate.Public(), altPrivate)
	if err != nil {
		t.Fatal(err)
	}
	altRoot, err := x509.ParseCertificate(altDER)
	if err != nil {
		t.Fatal(err)
	}

	crossTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(100),
		Subject:               fx.Good.Intermediate.Subject,
		SubjectKeyId:          fx.Good.Intermediate.SubjectKeyId,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		NotBefore:             sample.FixedTime.Add(-time.Hour),
		NotAfter:              sample.FixedTime.Add(24 * time.Hour),
	}
	crossDER, err := x509.CreateCertificate(rand.Reader, crossTemplate, altRoot, fx.Good.Intermediate.PublicKey, altPrivate)
	if err != nil {
		t.Fatal(err)
	}
	crossPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: crossDER})
	altPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: altDER})

	policy := domain.Policy{Mode: domain.PolicyModeRoot, TrustedRoots: []string{pki.CanonicalRoot(altRoot)}}
	report, perr := pki.EvaluateChain(fx.Good.LeafPEM+string(crossPEM)+string(altPEM), pki.EvaluateOptions{
		SNI: "good.internal", AtMS: testAtMS(), Policy: policy,
	})
	if perr != nil {
		t.Fatalf("cross-signed path should validate: %v", perr)
	}
	if !report.RootTrusted || !report.PolicyMatched || len(report.OrderedSerials) != 3 {
		t.Fatalf("unexpected cross-signed report: %+v", report)
	}
}

func caTemplateForTest(cn string, serial *big.Int, now time.Time) *x509.Certificate {
	return &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: cn},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
}
