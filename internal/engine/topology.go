package engine

import (
	"bytes"
	"crypto/x509"
	"fmt"
	"sort"
	"strings"

	"internalpki/workbench/internal/model"
	"internalpki/workbench/internal/sampleio"
)

type topologyResult struct {
	Findings  []model.Finding
	Complete  []*x509.Certificate
	Links     []model.ChainLink
	CanVerify bool
}

func inspectTopology(sample *sampleio.Sample) topologyResult {
	result := topologyResult{CanVerify: true}
	if len(sample.Chain) == 0 {
		result.CanVerify = false
		result.Findings = append(result.Findings, model.Finding{
			Code:     model.CodeChainBroken,
			Field:    "chain",
			Expected: "non-empty leaf-to-root chain",
			Actual:   "empty",
			Detail:   "chain.pem contains no certificates",
		})
		return result
	}

	result.Complete = append(result.Complete, sample.Chain...)
	trustedRoots := make(map[string]struct{}, len(sample.Roots))
	for _, root := range sample.Roots {
		trustedRoots[string(root.Raw)] = struct{}{}
	}

	for i, cert := range sample.Chain {
		if !bytes.Equal(cert.RawSubject, cert.RawIssuer) {
			continue
		}
		if !hasValidSelfSignature(cert) {
			result.CanVerify = false
			result.Findings = append(result.Findings, certificateFinding(
				model.CodeChainBroken, "self_signature", "valid self-signature", "self-issued certificate has an invalid signature", cert,
			))
			continue
		}
		if _, trusted := trustedRoots[string(cert.Raw)]; trusted {
			continue
		}
		result.CanVerify = false
		if i == len(sample.Chain)-1 && cert.IsCA {
			result.Findings = append(result.Findings, certificateFinding(
				model.CodeChainUntrusted, "trust_anchor", "certificate present in roots.pem", "self-signed CA absent from roots.pem", cert,
			))
		} else {
			result.Findings = append(result.Findings, certificateFinding(
				model.CodeUntrustedSelfSigned, "self_signed_certificate", "issued by a trusted CA", "self-signed and not a trust anchor", cert,
			))
		}
	}

	for i := 0; i < len(sample.Chain)-1; i++ {
		child, parent := sample.Chain[i], sample.Chain[i+1]
		connected := bytes.Equal(child.RawIssuer, parent.RawSubject)
		signatureValid := false
		var signatureErr string
		if connected {
			if err := child.CheckSignatureFrom(parent); err != nil {
				signatureErr = err.Error()
			} else {
				signatureValid = true
			}
		}
		result.Links = append(result.Links, model.ChainLink{
			From:                   i,
			To:                     i + 1,
			IssuerSubjectConnected: connected,
			SignatureValid:         signatureValid,
			Error:                  signatureErr,
		})
		if !connected || !signatureValid {
			result.CanVerify = false
			detail := "issuer and subject do not connect"
			if connected && signatureErr != "" {
				detail = signatureErr
			}
			result.Findings = append(result.Findings, certificateFinding(
				model.CodeChainBroken, "chain_link", "issuer/subject connected with a valid signature", detail, child,
			))
		}
	}

	last := sample.Chain[len(sample.Chain)-1]
	if !bytes.Equal(last.RawSubject, last.RawIssuer) {
		candidate := trustedIssuer(last, sample.Roots)
		if candidate == nil {
			result.CanVerify = false
			result.Findings = append(result.Findings, certificateFinding(
				model.CodeChainBroken, "chain_anchor", "trusted root from roots.pem", "no matching trusted issuer", last,
			))
		} else {
			result.Complete = append(result.Complete, candidate)
		}
	}

	for i, cert := range result.Complete {
		if i == 0 {
			continue
		}
		if !cert.IsCA {
			result.CanVerify = false
			result.Findings = append(result.Findings, certificateFinding(
				model.CodeChainBroken, "ca_constraint", "CA:TRUE", "CA:FALSE", cert,
			))
		}
		if cert.MaxPathLen > 0 || cert.MaxPathLenZero {
			intermediatesBelow := 0
			for j := 1; j < i; j++ {
				below := result.Complete[j]
				if below.IsCA && !bytes.Equal(below.RawSubject, below.RawIssuer) {
					intermediatesBelow++
				}
			}
			if intermediatesBelow > cert.MaxPathLen {
				result.CanVerify = false
				result.Findings = append(result.Findings, certificateFinding(
					model.CodePathLenExceeded,
					"path_len",
					fmt.Sprintf("<= %d non-self-issued intermediates", cert.MaxPathLen),
					fmt.Sprintf("%d non-self-issued intermediates", intermediatesBelow),
					cert,
				))
			}
		}
	}

	for i := 0; i < len(result.Complete)-1; i++ {
		if i < len(result.Links) {
			continue
		}
		child, parent := result.Complete[i], result.Complete[i+1]
		connected := bytes.Equal(child.RawIssuer, parent.RawSubject)
		signatureValid := false
		var signatureErr string
		if connected {
			if err := child.CheckSignatureFrom(parent); err != nil {
				signatureErr = err.Error()
			} else {
				signatureValid = true
			}
		}
		result.Links = append(result.Links, model.ChainLink{
			From:                   i,
			To:                     i + 1,
			IssuerSubjectConnected: connected,
			SignatureValid:         signatureValid,
			Error:                  signatureErr,
		})
		if !connected || !signatureValid {
			result.CanVerify = false
			detail := "issuer and subject do not connect"
			if connected && signatureErr != "" {
				detail = signatureErr
			}
			result.Findings = append(result.Findings, certificateFinding(
				model.CodeChainBroken, "chain_link", "issuer/subject connected with a valid signature", detail, child,
			))
		}
	}

	return result
}

func trustedIssuer(child *x509.Certificate, roots []*x509.Certificate) *x509.Certificate {
	for _, root := range roots {
		if !root.IsCA || !bytes.Equal(child.RawIssuer, root.RawSubject) {
			continue
		}
		if err := child.CheckSignatureFrom(root); err == nil {
			return root
		}
	}
	return nil
}

func certificateFinding(code, field, expected, actual string, cert *x509.Certificate) model.Finding {
	return model.Finding{
		Code:        code,
		Field:       field,
		Expected:    expected,
		Actual:      actual,
		Certificate: certificateIdentity(cert),
	}
}

func certificateIdentity(cert *x509.Certificate) *model.CertificateIdentity {
	if cert == nil {
		return nil
	}
	return &model.CertificateIdentity{
		Subject: cert.Subject.String(),
		Serial:  strings.ToUpper(cert.SerialNumber.Text(16)),
	}
}

func summarizeChain(chain []*x509.Certificate, roots []*x509.Certificate) []model.CertificateSummary {
	trusted := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		trusted[string(root.Raw)] = struct{}{}
	}
	out := make([]model.CertificateSummary, 0, len(chain))
	for i, cert := range chain {
		role := "intermediate"
		if i == 0 {
			role = "leaf"
		}
		selfIssued := bytes.Equal(cert.RawSubject, cert.RawIssuer)
		if selfIssued && i == len(chain)-1 {
			if _, ok := trusted[string(cert.Raw)]; ok {
				role = "root"
			} else {
				role = "untrusted_root"
			}
		}
		dnsNames := append([]string(nil), cert.DNSNames...)
		sort.Strings(dnsNames)
		ips := make([]string, 0, len(cert.IPAddresses))
		for _, ip := range cert.IPAddresses {
			ips = append(ips, ip.String())
		}
		sort.Strings(ips)
		eku := make([]string, 0, len(cert.ExtKeyUsage))
		for _, usage := range cert.ExtKeyUsage {
			eku = append(eku, extKeyUsageName(usage))
		}
		sort.Strings(eku)
		critical := make([]string, 0, len(cert.UnhandledCriticalExtensions))
		for _, oid := range cert.UnhandledCriticalExtensions {
			critical = append(critical, oid.String())
		}
		sort.Strings(critical)
		out = append(out, model.CertificateSummary{
			Position:             i,
			Role:                 role,
			Subject:              cert.Subject.String(),
			Issuer:               cert.Issuer.String(),
			Serial:               strings.ToUpper(cert.SerialNumber.Text(16)),
			NotBefore:            cert.NotBefore.UTC().Format("2006-01-02T15:04:05Z"),
			NotAfter:             cert.NotAfter.UTC().Format("2006-01-02T15:04:05Z"),
			IsCA:                 cert.IsCA,
			SelfSigned:           selfIssued && hasValidSelfSignature(cert),
			PublicKeyAlgorithm:   cert.PublicKeyAlgorithm.String(),
			SignatureAlgorithm:   cert.SignatureAlgorithm.String(),
			DNSNames:             dnsNames,
			IPAddresses:          ips,
			KeyUsage:             int(cert.KeyUsage),
			ExtKeyUsage:          eku,
			BasicConstraintsOK:   cert.BasicConstraintsValid,
			MaxPathLen:           cert.MaxPathLen,
			MaxPathLenZero:       cert.MaxPathLenZero,
			UnhandledCriticalExt: critical,
		})
	}
	return out
}

func hasValidSelfSignature(cert *x509.Certificate) bool {
	if cert == nil || !bytes.Equal(cert.RawSubject, cert.RawIssuer) {
		return false
	}
	return cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature) == nil
}

func extKeyUsageName(usage x509.ExtKeyUsage) string {
	switch usage {
	case x509.ExtKeyUsageAny:
		return "any"
	case x509.ExtKeyUsageServerAuth:
		return "serverAuth"
	case x509.ExtKeyUsageClientAuth:
		return "clientAuth"
	case x509.ExtKeyUsageCodeSigning:
		return "codeSigning"
	case x509.ExtKeyUsageEmailProtection:
		return "emailProtection"
	case x509.ExtKeyUsageIPSECEndSystem:
		return "ipsecEndSystem"
	case x509.ExtKeyUsageIPSECTunnel:
		return "ipsecTunnel"
	case x509.ExtKeyUsageIPSECUser:
		return "ipsecUser"
	case x509.ExtKeyUsageTimeStamping:
		return "timeStamping"
	case x509.ExtKeyUsageOCSPSigning:
		return "ocspSigning"
	case x509.ExtKeyUsageMicrosoftServerGatedCrypto:
		return "microsoftServerGatedCrypto"
	case x509.ExtKeyUsageNetscapeServerGatedCrypto:
		return "netscapeServerGatedCrypto"
	default:
		return fmt.Sprintf("ext_key_usage_%d", int(usage))
	}
}
