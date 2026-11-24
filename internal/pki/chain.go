package pki

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	"mitmpipeline/internal/domain"
	normid "mitmpipeline/internal/normalize"
)

type EvaluateOptions struct {
	SNI    string
	AtMS   int64
	Policy domain.Policy
}

type builtChain struct {
	certs      []*x509.Certificate
	cycleFound bool
}

func EvaluateChain(pemChain string, opts EvaluateOptions) (*domain.ChainReport, *domain.Error) {
	certs, perr := ParseCertificates(pemChain)
	if perr != nil {
		report := &domain.ChainReport{Valid: false, PolicyMode: opts.Policy.Mode, ErrorCode: domain.CodeChainInvalid}
		return report, perr
	}
	return EvaluateCertificates(certs, opts)
}

func EvaluateCertificates(certs []*x509.Certificate, opts EvaluateOptions) (*domain.ChainReport, *domain.Error) {
	report := &domain.ChainReport{
		SNIMatched: true,
		PolicyMode: opts.Policy.Mode,
	}

	at := time.UnixMilli(opts.AtMS).UTC()
	for _, cert := range certs {
		validity := domain.CertValidity{
			Subject:             cert.Subject.String(),
			Issuer:              cert.Issuer.String(),
			Serial:              normid.Serial(cert.SerialNumber.Text(16)),
			SPKISHA256:          SPKIHash(cert),
			CertSHA256:          CertHash(cert),
			NotBeforeMS:         cert.NotBefore.UnixMilli(),
			NotAfterMS:          cert.NotAfter.UnixMilli(),
			ValidityRemainingMS: cert.NotAfter.UnixMilli() - opts.AtMS,
			SupportedSignature:  supportedSignature(cert.SignatureAlgorithm),
			SignatureAlgorithm:  cert.SignatureAlgorithm.String(),
			IsCA:                cert.IsCA,
		}
		report.Certificates = append(report.Certificates, validity)
		if !validity.SupportedSignature {
			e := domain.NewError(domain.CodeChainInvalid, "certificate uses unsupported signature algorithm").
				WithContext("subject", validity.Subject).
				WithContext("signature_algorithm", validity.SignatureAlgorithm)
			report.ErrorCode = domain.CodeChainInvalid
			report.Findings = append(report.Findings, domain.Evidence{Kind: "signature_unsupported", Severity: "critical", Code: domain.CodeChainInvalid, Message: e.Message, Details: e.Context})
			return report, e
		}
		if at.Before(cert.NotBefore) || at.After(cert.NotAfter) {
			report.Valid = false
			report.ErrorCode = domain.CodeChainInvalid
			message := "certificate is outside its validity window"
			report.Findings = append(report.Findings, domain.Evidence{Kind: "validity_failure", Severity: "critical", Code: domain.CodeChainInvalid, Message: message, Details: map[string]any{"subject": validity.Subject, "validity_remaining_ms": validity.ValidityRemainingMS}})
			return report, domain.NewError(domain.CodeChainInvalid, message).WithContext("subject", validity.Subject)
		}
	}

	leaf := selectLeaf(certs)
	if leaf == nil {
		e := domain.NewError(domain.CodeChainInvalid, "no leaf certificate found")
		report.ErrorCode = domain.CodeChainInvalid
		return report, e
	}
	if opts.SNI != "" {
		if err := leaf.VerifyHostname(normid.SNI(opts.SNI)); err != nil {
			report.SNIMatched = false
			report.Findings = append(report.Findings, domain.Evidence{Kind: "sni_mismatch", Severity: "critical", Code: "E_IDENTITY_MISMATCH", Message: "leaf certificate does not cover SNI", Details: map[string]any{"sni": opts.SNI, "error": err.Error()}})
		}
	}

	path := buildBestPath(leaf, certs, opts.Policy.TrustedRoots)
	if path == nil || len(path.certs) == 0 {
		report.ErrorCode = domain.CodeChainInvalid
		message := "certificate chain is missing a reachable root"
		if path != nil && path.cycleFound {
			message = "certificate chain contains an issuer cycle"
		}
		report.Findings = append(report.Findings, domain.Evidence{Kind: "path_failure", Severity: "critical", Code: domain.CodeChainInvalid, Message: message})
		return report, domain.NewError(domain.CodeChainInvalid, message).WithContext("cycle_found", path != nil && path.cycleFound)
	}

	for i, cert := range path.certs {
		report.OrderedSerials = append(report.OrderedSerials, normid.Serial(cert.SerialNumber.Text(16)))
		if i > 0 {
			child := path.certs[i-1]
			if err := child.CheckSignatureFrom(cert); err != nil {
				report.ErrorCode = domain.CodeChainInvalid
				message := "certificate link signature verification failed"
				report.Findings = append(report.Findings, domain.Evidence{Kind: "signature_failure", Severity: "critical", Code: domain.CodeChainInvalid, Message: message, Details: map[string]any{"child": child.Subject.String(), "issuer": cert.Subject.String(), "error": err.Error()}})
				return report, domain.WrapError(domain.CodeChainInvalid, err, message)
			}
		}
	}

	root := path.certs[len(path.certs)-1]
	report.RootSHA256 = CertHash(root)
	report.RootTrusted = hashListContains(opts.Policy.TrustedRoots, report.RootSHA256)

	computedSPKI := map[string]bool{}
	for _, cert := range path.certs {
		computedSPKI[SPKIHash(cert)] = true
	}

	switch opts.Policy.Mode {
	case domain.PolicyModeSPKI:
		report.EntryResults = evaluateEntries(opts.Policy.SPKIPins, func(value string) bool {
			normalized, err := NormalizeHashValue(value)
			return err == nil && computedSPKI[normalized]
		})
	case domain.PolicyModeRoot:
		report.EntryResults = evaluateEntries(opts.Policy.TrustedRoots, func(value string) bool {
			return hashEntryMatches(value, report.RootSHA256)
		})
	default:
		e := domain.NewError(domain.CodeRecordCorrupt, "unsupported policy mode").WithContext("mode", opts.Policy.Mode)
		report.ErrorCode = domain.CodeRecordCorrupt
		return report, e
	}

	for _, entry := range report.EntryResults {
		if entry.Matched {
			report.HitEntries = append(report.HitEntries, entry.Value)
		} else {
			report.MissedEntries = append(report.MissedEntries, entry.Value)
		}
	}
	report.PolicyMatched = len(report.HitEntries) > 0
	report.Valid = report.ErrorCode == ""

	if report.PolicyMatched {
		report.Findings = append(report.Findings, domain.Evidence{Kind: "policy_hit", Severity: "info", Message: "fixed policy entry matched", Details: map[string]any{"entries": report.HitEntries}})
	} else {
		report.Findings = append(report.Findings, domain.Evidence{Kind: "policy_miss", Severity: "warning", Message: "no fixed policy entry matched", Details: map[string]any{"missed_entries": report.MissedEntries}})
	}
	if report.RootTrusted {
		report.Findings = append(report.Findings, domain.Evidence{Kind: "root_trusted", Severity: "info", Message: "terminal root is in trusted root set", Details: map[string]any{"root_sha256": report.RootSHA256}})
	} else {
		report.Findings = append(report.Findings, domain.Evidence{Kind: "root_untrusted", Severity: "critical", Message: "terminal root is not in trusted root set", Details: map[string]any{"root_sha256": report.RootSHA256}})
	}
	return report, nil
}

func ParseCertificates(pemChain string) ([]*x509.Certificate, *domain.Error) {
	var certs []*x509.Certificate
	rest := []byte(pemChain)
	blocks := 0
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		blocks++
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, domain.WrapError(domain.CodeChainInvalid, err, "failed to parse X.509 certificate")
		}
		certs = append(certs, cert)
	}
	if blocks == 0 || len(certs) == 0 {
		return nil, domain.NewError(domain.CodeChainInvalid, "PEM chain contains no parseable certificate")
	}
	return certs, nil
}

func selectLeaf(certs []*x509.Certificate) *x509.Certificate {
	for _, cert := range certs {
		if !cert.IsCA {
			return cert
		}
	}
	if len(certs) > 0 {
		return certs[0]
	}
	return nil
}

func buildBestPath(leaf *x509.Certificate, certs []*x509.Certificate, trustedRootEntries []string) *builtChain {
	var paths []*builtChain
	cycleFound := false

	var dfs func(current *x509.Certificate, path []*x509.Certificate, seen map[string]bool)
	dfs = func(current *x509.Certificate, path []*x509.Certificate, seen map[string]bool) {
		if isSelfSigned(current) {
			copied := append([]*x509.Certificate(nil), path...)
			paths = append(paths, &builtChain{certs: copied})
			return
		}
		issuers := findIssuers(current, certs)
		for _, issuer := range issuers {
			fingerprint := CertHash(issuer)
			if seen[fingerprint] {
				cycleFound = true
				continue
			}
			nextSeen := cloneSeen(seen)
			nextSeen[fingerprint] = true
			dfs(issuer, append(path, issuer), nextSeen)
		}
	}

	initialSeen := map[string]bool{CertHash(leaf): true}
	dfs(leaf, []*x509.Certificate{leaf}, initialSeen)
	if len(paths) == 0 {
		return &builtChain{cycleFound: cycleFound}
	}

	for _, path := range paths {
		root := path.certs[len(path.certs)-1]
		if hashListContains(trustedRootEntries, CertHash(root)) {
			return path
		}
	}
	shortest := paths[0]
	for _, path := range paths[1:] {
		if len(path.certs) < len(shortest.certs) {
			shortest = path
		}
	}
	return shortest
}

func findIssuers(child *x509.Certificate, certs []*x509.Certificate) []*x509.Certificate {
	var issuers []*x509.Certificate
	for _, candidate := range certs {
		if bytes.Equal(child.RawIssuer, candidate.RawSubject) && child.CheckSignatureFrom(candidate) == nil {
			issuers = append(issuers, candidate)
		}
	}
	return issuers
}

func isSelfSigned(cert *x509.Certificate) bool {
	return bytes.Equal(cert.RawSubject, cert.RawIssuer) && cert.CheckSignatureFrom(cert) == nil
}

func cloneSeen(values map[string]bool) map[string]bool {
	out := make(map[string]bool, len(values)+1)
	for key, value := range values {
		out[key] = value
	}
	return out
}

func supportedSignature(algorithm x509.SignatureAlgorithm) bool {
	switch algorithm {
	case x509.SHA256WithRSA, x509.SHA384WithRSA, x509.SHA512WithRSA,
		x509.SHA256WithRSAPSS, x509.SHA384WithRSAPSS, x509.SHA512WithRSAPSS,
		x509.ECDSAWithSHA256, x509.ECDSAWithSHA384,
		x509.ECDSAWithSHA512, x509.PureEd25519:
		return true
	default:
		return false
	}
}

func evaluateEntries(entries []string, match func(string) bool) []domain.PolicyEntryResult {
	results := make([]domain.PolicyEntryResult, 0, len(entries))
	for _, entry := range entries {
		results = append(results, domain.PolicyEntryResult{Value: entry, Matched: match(entry)})
	}
	return results
}

func hashListContains(entries []string, computedHex string) bool {
	for _, entry := range entries {
		if hashEntryMatches(entry, computedHex) {
			return true
		}
	}
	return false
}

func FormatSerial(cert *x509.Certificate) string {
	return normid.Serial(fmt.Sprintf("%X", cert.SerialNumber))
}
