package engine

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"internalpki/workbench/internal/model"
	"internalpki/workbench/internal/sampleio"
)

const (
	baseCPUUnits       int64 = 120
	cpuUnitsPerDepth   int64 = 80
	baseMemoryBytes    int64 = 32 * 1024
	memoryBytesPerStep int64 = 16 * 1024
)

// Cost is the deterministic resource estimate used by both budget admission
// and accounting. It intentionally does not depend on wall-clock timing.
type Cost struct {
	CPUUnits    int64
	MemoryBytes int64
}

func EstimateCost(depth int) Cost {
	if depth < 1 {
		depth = 1
	}
	d := int64(depth)
	return Cost{
		CPUUnits:    baseCPUUnits + cpuUnitsPerDepth*d,
		MemoryBytes: baseMemoryBytes + memoryBytesPerStep*d,
	}
}

// Evaluate performs the offline equivalent of TLS chain verification: the
// supplied roots.pem is the only trust store, the requested SNI is applied as
// x509 DNSName verification, and serverAuth EKU is required. No network or
// system root pool is consulted.
func Evaluate(sample *sampleio.Sample, at time.Time) model.SampleResult {
	result := model.SampleResult{
		Path:           sample.Path,
		ChainDepth:     sample.EffectiveDepth(),
		ExpectedResult: expectedResult(sample.Meta.ExpectedResult),
	}
	if sample.LoadFinding != nil {
		result.Outcome = model.OutcomeFailed
		result.State = model.StateFailed
		result.Findings = []model.Finding{*sample.LoadFinding}
		finishResult(&result)
		return result
	}

	topology := inspectTopology(sample)
	findings := append([]model.Finding(nil), topology.Findings...)
	findings = append(findings, handshakeFindings(sample, topology.Complete)...)

	if topology.CanVerify && len(sample.Chain) > 0 {
		findings = append(findings, verifyWithX509(sample, topology.Complete, at)...)
	}

	result.Findings = findings
	if len(topology.Complete) > 0 {
		result.Chain = summarizeChain(topology.Complete, sample.Roots)
		result.ChainDepth = len(topology.Complete)
	} else {
		result.Chain = summarizeChain(sample.Chain, sample.Roots)
	}
	result.Links = append([]model.ChainLink(nil), topology.Links...)
	if len(result.Findings) == 0 {
		result.State = model.StateCompleted
		result.Outcome = model.OutcomePassed
	} else {
		result.State = model.StateFailed
		result.Outcome = model.OutcomeFailed
	}
	finishResult(&result)
	return result
}

// BudgetResult constructs a complete, serializable result for an admission
// decision without fabricating certificate evidence.
func BudgetResult(sample *sampleio.Sample, code, field, expected, actual, detail string, state, outcome string) model.SampleResult {
	result := model.SampleResult{
		Path:           sample.Path,
		ChainDepth:     sample.EffectiveDepth(),
		ExpectedResult: expectedResult(sample.Meta.ExpectedResult),
		State:          state,
		Outcome:        outcome,
		Findings: []model.Finding{{
			Code:     code,
			Field:    field,
			Expected: expected,
			Actual:   actual,
			Detail:   detail,
		}},
	}
	if len(sample.Chain) > 0 {
		result.Chain = summarizeChain(sample.Chain, sample.Roots)
	}
	finishResult(&result)
	return result
}

func finishResult(result *model.SampleResult) {
	result.BusinessCode = model.PrimaryCode(result.Findings)
	if result.BusinessCode == "" {
		result.ActualResult = "OK"
	} else {
		result.ActualResult = result.BusinessCode
	}
	result.ExpectationMatched = normalizeExpected(result.ExpectedResult) == normalizeExpected(result.ActualResult)
	for _, finding := range result.Findings {
		if finding.Code == result.BusinessCode && finding.Certificate != nil {
			copy := *finding.Certificate
			result.FailureCertificate = &copy
			break
		}
	}
}

func expectedResult(expected string) string {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return "OK"
	}
	return expected
}

func normalizeExpected(expected string) string {
	if expected == "" || strings.EqualFold(expected, "OK") || strings.EqualFold(expected, "PASS") {
		return "OK"
	}
	return strings.ToUpper(expected)
}

func handshakeFindings(sample *sampleio.Sample, chain []*x509.Certificate) []model.Finding {
	if len(chain) == 0 || len(sample.Chain) == 0 {
		return nil
	}
	leaf := sample.Chain[0]
	var findings []model.Finding

	if strings.TrimSpace(sample.Meta.SNI) == "" {
		findings = append(findings, model.Finding{
			Code:     model.CodeHostnameMismatch,
			Field:    "sni",
			Expected: "non-empty SNI",
			Actual:   "empty",
			Detail:   "meta.json does not provide an SNI",
		})
	} else if err := leaf.VerifyHostname(sample.Meta.SNI); err != nil {
		findings = append(findings, hostnameFinding(sample.Meta.SNI, leaf, err.Error()))
	}

	supported := make([]string, 0, len(sample.Meta.ServerTLSVersions))
	targetVersion, targetOK := parseTLSVersion(sample.Meta.TLSVersion)
	targetSupported := false
	for _, raw := range sample.Meta.ServerTLSVersions {
		version, ok := parseTLSVersion(raw)
		if !ok {
			continue
		}
		supported = append(supported, canonicalTLSVersion(version))
		if targetOK && version == targetVersion {
			targetSupported = true
		}
	}
	sort.Strings(supported)
	if !targetOK {
		findings = append(findings, model.Finding{
			Code:     model.CodeTLSVersionMismatch,
			Field:    "tls_version",
			Expected: "tls1.0, tls1.1, tls1.2, or tls1.3",
			Actual:   sample.Meta.TLSVersion,
			Detail:   "requested protocol version is not recognized",
		})
	} else if !targetSupported {
		actual := strings.Join(supported, ",")
		if actual == "" {
			actual = "none"
		}
		findings = append(findings, model.Finding{
			Code:     model.CodeTLSVersionMismatch,
			Field:    "tls_version",
			Expected: canonicalTLSVersion(targetVersion),
			Actual:   actual,
			Detail:   "requested protocol version is outside the server-declared range",
		})
	}

	for i := 0; i+1 < len(chain); i++ {
		child, issuer := chain[i], chain[i+1]
		if signatureAlgorithmCompatible(child.SignatureAlgorithm, issuer.PublicKey) {
			continue
		}
		findings = append(findings, model.Finding{
			Code:        model.CodeSignatureAlgMismatch,
			Field:       fmt.Sprintf("chain[%d].signature_algorithm", i),
			Expected:    issuerPublicKeyFamily(issuer.PublicKey),
			Actual:      child.SignatureAlgorithm.String(),
			Detail:      "signature algorithm does not match issuer public-key type",
			Certificate: certificateIdentity(child),
		})
	}

	if !allowsServerAuth(leaf.ExtKeyUsage) {
		actual := make([]string, 0, len(leaf.ExtKeyUsage))
		for _, usage := range leaf.ExtKeyUsage {
			actual = append(actual, extKeyUsageName(usage))
		}
		sort.Strings(actual)
		findings = append(findings, model.Finding{
			Code:        model.CodeExtKeyUsage,
			Field:       "extended_key_usage",
			Expected:    "serverAuth",
			Actual:      strings.Join(actual, ","),
			Detail:      "leaf certificate does not permit TLS server authentication",
			Certificate: certificateIdentity(leaf),
		})
	}
	if leaf.KeyUsage != 0 && leaf.KeyUsage&(x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment|x509.KeyUsageKeyAgreement) == 0 {
		findings = append(findings, model.Finding{
			Code:        model.CodeExtKeyUsage,
			Field:       "key_usage",
			Expected:    "digitalSignature, keyEncipherment, or keyAgreement",
			Actual:      fmt.Sprintf("key_usage_mask=%d", int(leaf.KeyUsage)),
			Detail:      "leaf certificate key usage is incompatible with a TLS server",
			Certificate: certificateIdentity(leaf),
		})
	}
	if len(leaf.UnhandledCriticalExtensions) > 0 {
		oids := make([]string, 0, len(leaf.UnhandledCriticalExtensions))
		for _, oid := range leaf.UnhandledCriticalExtensions {
			oids = append(oids, oid.String())
		}
		sort.Strings(oids)
		findings = append(findings, model.Finding{
			Code:        model.CodeUnhandledCriticalExt,
			Field:       "critical_extensions",
			Expected:    "no unhandled critical extensions",
			Actual:      strings.Join(oids, ","),
			Detail:      "verification cannot safely ignore these critical extensions",
			Certificate: certificateIdentity(leaf),
		})
	}
	return findings
}

func verifyWithX509(sample *sampleio.Sample, complete []*x509.Certificate, at time.Time) []model.Finding {
	leaf := sample.Chain[0]
	intermediates := x509.NewCertPool()
	for i := 1; i < len(complete); i++ {
		cert := complete[i]
		if i == len(complete)-1 && hasValidSelfSignature(cert) {
			continue
		}
		intermediates.AddCert(cert)
	}
	opts := x509.VerifyOptions{
		Roots:         sample.RootPool,
		Intermediates: intermediates,
		CurrentTime:   at,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if sample.Meta.SNI != "" {
		opts.DNSName = sample.Meta.SNI
	}
	_, err := leaf.Verify(opts)
	if err == nil {
		return nil
	}
	return mapVerifyError(err, sample, leaf, at)
}

func mapVerifyError(err error, sample *sampleio.Sample, leaf *x509.Certificate, at time.Time) []model.Finding {
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		cert := hostnameErr.Certificate
		if cert == nil {
			cert = leaf
		}
		return []model.Finding{hostnameFinding(hostnameErr.Host, cert, hostnameErr.Error())}
	}

	var invalid x509.CertificateInvalidError
	if errors.As(err, &invalid) {
		cert := invalid.Cert
		if cert == nil {
			cert = leaf
		}
		switch invalid.Reason {
		case x509.Expired:
			return []model.Finding{{
				Code:        model.CodeExpired,
				Field:       "validity",
				Expected:    fmt.Sprintf("valid at %s", at.UTC().Format(time.RFC3339)),
				Actual:      fmt.Sprintf("not_before=%s not_after=%s", cert.NotBefore.UTC().Format(time.RFC3339), cert.NotAfter.UTC().Format(time.RFC3339)),
				Detail:      invalid.Error(),
				Certificate: certificateIdentity(cert),
			}}
		case x509.IncompatibleUsage, x509.CANotAuthorizedForExtKeyUsage:
			return []model.Finding{certificateFinding(model.CodeExtKeyUsage, "extended_key_usage", "serverAuth", invalid.Detail, cert)}
		case x509.TooManyIntermediates:
			return []model.Finding{certificateFinding(model.CodePathLenExceeded, "path_len", "within CA path length constraint", invalid.Detail, cert)}
		case x509.NameMismatch, x509.NotAuthorizedToSign:
			return []model.Finding{certificateFinding(model.CodeChainBroken, "chain_link", "issuer/subject connected with a valid signature", invalid.Detail, cert)}
		default:
			return []model.Finding{certificateFinding(model.CodeChainUntrusted, "chain_validation", "valid chain to roots.pem", invalid.Error(), cert)}
		}
	}

	var constraint x509.ConstraintViolationError
	if errors.As(err, &constraint) {
		return []model.Finding{certificateFinding(model.CodeChainBroken, "signing_constraint", "issuer permitted to sign certificate", constraint.Error(), leaf)}
	}

	var insecure x509.InsecureAlgorithmError
	if errors.As(err, &insecure) {
		return []model.Finding{certificateFinding(model.CodeSignatureAlgMismatch, "signature_algorithm", "secure and compatible algorithm", insecure.Error(), leaf)}
	}

	var unknown x509.UnknownAuthorityError
	if errors.As(err, &unknown) {
		cert := unknown.Cert
		if cert == nil {
			cert = leaf
		}
		return []model.Finding{certificateFinding(model.CodeChainUntrusted, "trust_anchor", "chain anchored in roots.pem", unknown.Error(), cert)}
	}

	return []model.Finding{certificateFinding(model.CodeChainUntrusted, "verification", "valid chain to roots.pem", err.Error(), leaf)}
}

func hostnameFinding(host string, cert *x509.Certificate, detail string) model.Finding {
	names := append([]string(nil), cert.DNSNames...)
	for _, ip := range cert.IPAddresses {
		names = append(names, ip.String())
	}
	sort.Strings(names)
	actual := strings.Join(names, ",")
	if actual == "" {
		actual = "no SAN names"
	}
	return model.Finding{
		Code:        model.CodeHostnameMismatch,
		Field:       "sni",
		Expected:    host,
		Actual:      actual,
		Detail:      detail,
		Certificate: certificateIdentity(cert),
	}
}

func allowsServerAuth(usages []x509.ExtKeyUsage) bool {
	if len(usages) == 0 {
		return true
	}
	for _, usage := range usages {
		if usage == x509.ExtKeyUsageAny || usage == x509.ExtKeyUsageServerAuth {
			return true
		}
	}
	return false
}

func signatureAlgorithmCompatible(algorithm x509.SignatureAlgorithm, publicKey any) bool {
	switch publicKey.(type) {
	case *rsa.PublicKey:
		switch algorithm {
		case x509.SHA256WithRSA, x509.SHA384WithRSA, x509.SHA512WithRSA,
			x509.SHA256WithRSAPSS, x509.SHA384WithRSAPSS, x509.SHA512WithRSAPSS:
			return true
		}
	case *ecdsa.PublicKey:
		switch algorithm {
		case x509.ECDSAWithSHA256, x509.ECDSAWithSHA384, x509.ECDSAWithSHA512:
			return true
		}
	case ed25519.PublicKey:
		return algorithm == x509.PureEd25519
	}
	return false
}

func issuerPublicKeyFamily(publicKey any) string {
	switch publicKey.(type) {
	case *rsa.PublicKey:
		return "RSA"
	case *ecdsa.PublicKey:
		return "ECDSA"
	case ed25519.PublicKey:
		return "Ed25519"
	default:
		return fmt.Sprintf("%T", publicKey)
	}
}

func parseTLSVersion(raw string) (uint16, bool) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "tls1.0", "tlsv1.0", "1.0", "10":
		return tls.VersionTLS10, true
	case "tls1.1", "tlsv1.1", "1.1", "11":
		return tls.VersionTLS11, true
	case "tls1.2", "tlsv1.2", "1.2", "12":
		return tls.VersionTLS12, true
	case "tls1.3", "tlsv1.3", "1.3", "13":
		return tls.VersionTLS13, true
	default:
		return 0, false
	}
}

func canonicalTLSVersion(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "tls1.0"
	case tls.VersionTLS11:
		return "tls1.1"
	case tls.VersionTLS12:
		return "tls1.2"
	case tls.VersionTLS13:
		return "tls1.3"
	default:
		return fmt.Sprintf("0x%04x", version)
	}
}
