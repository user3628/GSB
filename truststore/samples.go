package truststore

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"sort"
	"time"
)

const sampleValidityStart = "2024-01-01T00:00:00Z"
const sampleValidityEnd = "2034-01-01T00:00:00Z"

// SampleSet is the deterministic, local-only PKI fixture used by examples and
// tests. All private keys are derived from fixed labels and are not persisted in
// snapshots; only public certificates are stored.
type SampleSet struct {
	Certificates     []CertificateRecord
	Roots            []string
	Intermediates    []string
	Scopes           []TrustScope
	Chains           []ChainSpec
	CertIDByName     map[string]string
	PEMByName        map[string]string
	MissingIssuerPEM string
}

// ID returns the certificate fingerprint for a sample alias.
func (s *SampleSet) ID(name string) string { return s.CertIDByName[name] }

// InitialContent returns a canonical initial trust snapshot at version 1 when
// bootstrapped by Store.Bootstrap.
func (s *SampleSet) InitialContent() (SnapshotContent, error) {
	content := SnapshotContent{
		Certificates:    append([]CertificateRecord(nil), s.Certificates...),
		RootIDs:         append([]string(nil), s.Roots...),
		IntermediateIDs: append([]string(nil), s.Intermediates...),
		Scopes:          cloneScopes(s.Scopes),
		HostChains:      cloneChains(s.Chains),
	}
	return content.Normalize()
}

func cloneScopes(scopes []TrustScope) []TrustScope {
	out := make([]TrustScope, len(scopes))
	for i, scope := range scopes {
		out[i] = TrustScope{
			ID:                     scope.ID,
			AllowedRootIDs:         append([]string(nil), scope.AllowedRootIDs...),
			AllowedIntermediateIDs: append([]string(nil), scope.AllowedIntermediateIDs...),
		}
	}
	return out
}

func cloneChains(chains []ChainSpec) []ChainSpec {
	out := make([]ChainSpec, len(chains))
	for i, chain := range chains {
		out[i] = ChainSpec{
			ID:              chain.ID,
			LeafID:          chain.LeafID,
			IntermediateIDs: append([]string(nil), chain.IntermediateIDs...),
			ScopeID:         chain.ScopeID,
			Description:     chain.Description,
		}
	}
	return out
}

type sampleCertSpec struct {
	name         string
	commonName   string
	organization string
	serial       int64
	keyLabel     string
	parentName   string
	parentKey    string
	isCA         bool
	maxPathLen   int
	zeroPathLen  bool
	dnsNames     []string
}

// GenerateSamples builds the complete local fixture. Ed25519 signing and fixed
// serial numbers, names and validity windows make the output byte-identical on
// every run and on every supported OS.
func GenerateSamples() (*SampleSet, error) {
	specs := []sampleCertSpec{
		{name: "root-alpha", commonName: "Demo Root Alpha", organization: "Local PKI", serial: 1001, keyLabel: "root-alpha-key", isCA: true, maxPathLen: 4},
		{name: "root-beta", commonName: "Demo Root Beta", organization: "Local PKI", serial: 1002, keyLabel: "root-beta-key", isCA: true, maxPathLen: 4},
		{name: "root-gamma", commonName: "Demo Root Gamma", organization: "Local PKI", serial: 1003, keyLabel: "root-gamma-key", isCA: true, maxPathLen: 4},
		{name: "intermediate-main-alpha", commonName: "Demo Issuing CA", organization: "Local PKI", serial: 2001, keyLabel: "issuing-ca-key", parentName: "root-alpha", parentKey: "root-alpha-key", isCA: true, maxPathLen: 1},
		{name: "intermediate-main-beta", commonName: "Demo Issuing CA", organization: "Local PKI", serial: 2002, keyLabel: "issuing-ca-key", parentName: "root-beta", parentKey: "root-beta-key", isCA: true, maxPathLen: 1},
		{name: "intermediate-main-gamma", commonName: "Demo Issuing CA", organization: "Local PKI", serial: 2003, keyLabel: "issuing-ca-key", parentName: "root-gamma", parentKey: "root-gamma-key", isCA: true, maxPathLen: 1},
		{name: "dept-ca", commonName: "Demo Department CA", organization: "Local PKI", serial: 2101, keyLabel: "dept-ca-key", parentName: "root-alpha", parentKey: "root-alpha-key", isCA: true, maxPathLen: 1},
		{name: "team-ca", commonName: "Demo Team CA", organization: "Local PKI", serial: 2102, keyLabel: "team-ca-key", parentName: "dept-ca", parentKey: "dept-ca-key", isCA: true, maxPathLen: 0, zeroPathLen: true},
		{name: "restricted-ca", commonName: "Demo Restricted CA", organization: "Local PKI", serial: 2201, keyLabel: "restricted-ca-key", parentName: "root-alpha", parentKey: "root-alpha-key", isCA: true, maxPathLen: 0, zeroPathLen: true},
		{name: "sub-ca", commonName: "Demo Sub CA", organization: "Local PKI", serial: 2202, keyLabel: "sub-ca-key", parentName: "restricted-ca", parentKey: "restricted-ca-key", isCA: true, maxPathLen: 0, zeroPathLen: true},
		{name: "rogue-ca", commonName: "Demo Rogue CA", organization: "Local PKI", serial: 2301, keyLabel: "rogue-ca-key", isCA: true, maxPathLen: 1},
		{name: "broken-issuer", commonName: "Demo Missing Issuer CA", organization: "Local PKI", serial: 2401, keyLabel: "broken-issuer-key", isCA: true, maxPathLen: 0, zeroPathLen: true},
		{name: "leaf-svc-alpha", commonName: "svc-alpha.local", organization: "Local PKI", serial: 3001, keyLabel: "leaf-svc-alpha-key", parentName: "intermediate-main-alpha", parentKey: "issuing-ca-key", dnsNames: []string{"svc-alpha.local"}},
		{name: "leaf-svc-beta", commonName: "svc-beta.local", organization: "Local PKI", serial: 3002, keyLabel: "leaf-svc-beta-key", parentName: "intermediate-main-beta", parentKey: "issuing-ca-key", dnsNames: []string{"svc-beta.local"}},
		{name: "leaf-cross", commonName: "cross.local", organization: "Local PKI", serial: 3003, keyLabel: "leaf-cross-key", parentName: "intermediate-main-alpha", parentKey: "issuing-ca-key", dnsNames: []string{"cross.local"}},
		{name: "leaf-dept", commonName: "dept.local", organization: "Local PKI", serial: 3004, keyLabel: "leaf-dept-key", parentName: "dept-ca", parentKey: "dept-ca-key", dnsNames: []string{"dept.local"}},
		{name: "leaf-team", commonName: "team.local", organization: "Local PKI", serial: 3005, keyLabel: "leaf-team-key", parentName: "team-ca", parentKey: "team-ca-key", dnsNames: []string{"team.local"}},
		{name: "leaf-restricted", commonName: "restricted.local", organization: "Local PKI", serial: 3006, keyLabel: "leaf-restricted-key", parentName: "sub-ca", parentKey: "sub-ca-key", dnsNames: []string{"restricted.local"}},
		{name: "leaf-rogue", commonName: "rogue.local", organization: "Local PKI", serial: 3007, keyLabel: "leaf-rogue-key", parentName: "rogue-ca", parentKey: "rogue-ca-key", dnsNames: []string{"rogue.local"}},
		{name: "leaf-broken", commonName: "broken.local", organization: "Local PKI", serial: 3008, keyLabel: "leaf-broken-key", parentName: "broken-issuer", parentKey: "broken-issuer-key", dnsNames: []string{"broken.local"}},
	}

	keys := make(map[string]ed25519.PrivateKey)
	keyFor := func(label string) ed25519.PrivateKey {
		if key, ok := keys[label]; ok {
			return key
		}
		seed := sha256.Sum256([]byte("pkitrust/sample/v1/" + label))
		key := ed25519.NewKeyFromSeed(seed[:])
		keys[label] = key
		return key
	}

	byName := make(map[string]CertificateRecord, len(specs))
	certsByName := make(map[string]*x509.Certificate, len(specs))
	pemByName := make(map[string]string, len(specs))
	ordered := make([]CertificateRecord, 0, len(specs))
	notBefore, err := time.Parse(time.RFC3339, sampleValidityStart)
	if err != nil {
		return nil, err
	}
	notAfter, err := time.Parse(time.RFC3339, sampleValidityEnd)
	if err != nil {
		return nil, err
	}

	for _, spec := range specs {
		key := keyFor(spec.keyLabel)
		subject := pkix.Name{CommonName: spec.commonName, Organization: []string{spec.organization}}
		subjectKeyID := publicKeyID(key.Public().(ed25519.PublicKey))
		template := &x509.Certificate{
			SerialNumber:          big.NewInt(spec.serial),
			Subject:               subject,
			NotBefore:             notBefore,
			NotAfter:              notAfter,
			BasicConstraintsValid: true,
			IsCA:                  spec.isCA,
			MaxPathLen:            spec.maxPathLen,
			MaxPathLenZero:        spec.zeroPathLen,
			SubjectKeyId:          subjectKeyID,
			DNSNames:              append([]string(nil), spec.dnsNames...),
		}
		if spec.isCA {
			if spec.maxPathLen < 0 {
				template.MaxPathLen = -1
			}
			template.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
		} else {
			template.KeyUsage = x509.KeyUsageDigitalSignature
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		}

		var parentCert *x509.Certificate
		var signer ed25519.PrivateKey
		if spec.parentName == "" {
			parentCert = template
			signer = key
			template.AuthorityKeyId = subjectKeyID
		} else {
			parentCert = certsByName[spec.parentName]
			if parentCert == nil {
				return nil, fmt.Errorf("sample parent %q not generated before %q", spec.parentName, spec.name)
			}
			signer = keyFor(spec.parentKey)
			template.AuthorityKeyId = append([]byte(nil), parentCert.SubjectKeyId...)
		}
		der, err := x509.CreateCertificate(rand.Reader, template, parentCert, key.Public(), signer)
		if err != nil {
			return nil, fmt.Errorf("create sample certificate %s: %w", spec.name, err)
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, fmt.Errorf("parse generated sample certificate %s: %w", spec.name, err)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		record, _, err := ParseCertificatePEM(spec.name, pemBytes)
		if err != nil {
			return nil, err
		}
		byName[spec.name] = record
		certsByName[spec.name] = cert
		pemByName[spec.name] = string(pemBytes)
	}

	// Keep generation order deterministic and stable in the fixture, even
	// though snapshots canonicalize certificates by fingerprint.
	for _, spec := range specs {
		ordered = append(ordered, byName[spec.name])
	}
	ids := make(map[string]string, len(byName))
	for name, record := range byName {
		ids[name] = record.ID
	}

	set := &SampleSet{
		Certificates:     ordered,
		Roots:            []string{"root-alpha", "root-beta"},
		Intermediates:    []string{"intermediate-main-alpha", "intermediate-main-beta", "dept-ca", "team-ca", "restricted-ca", "sub-ca"},
		CertIDByName:     ids,
		PEMByName:        pemByName,
		MissingIssuerPEM: pemByName["broken-issuer"],
		Scopes: []TrustScope{
			{
				ID:                     "scope-core",
				AllowedRootIDs:         []string{"root-alpha"},
				AllowedIntermediateIDs: []string{"intermediate-main-alpha", "dept-ca", "team-ca"},
			},
			{
				ID:                     "scope-beta",
				AllowedRootIDs:         []string{"root-beta"},
				AllowedIntermediateIDs: []string{"intermediate-main-beta"},
			},
			{
				ID:                     "scope-cross",
				AllowedRootIDs:         []string{"root-alpha", "root-gamma"},
				AllowedIntermediateIDs: []string{"intermediate-main-alpha", "intermediate-main-gamma"},
			},
			{
				ID:                     "scope-restricted",
				AllowedRootIDs:         []string{"root-alpha"},
				AllowedIntermediateIDs: []string{"restricted-ca", "sub-ca"},
			},
		},
		Chains: []ChainSpec{
			{ID: "chain-broken", LeafID: "leaf-broken", IntermediateIDs: nil, Description: "missing issuer sample"},
			{ID: "chain-cross", LeafID: "leaf-cross", IntermediateIDs: []string{"intermediate-main-alpha"}, ScopeID: "scope-cross", Description: "cross-signed replacement sample"},
			{ID: "chain-dept", LeafID: "leaf-dept", IntermediateIDs: []string{"dept-ca"}, ScopeID: "scope-core", Description: "direct dependent sample"},
			{ID: "chain-restricted", LeafID: "leaf-restricted", IntermediateIDs: []string{"sub-ca", "restricted-ca"}, ScopeID: "scope-restricted", Description: "path length violation sample"},
			{ID: "chain-rogue", LeafID: "leaf-rogue", IntermediateIDs: []string{"rogue-ca"}, Description: "self-signed non-root sample"},
			{ID: "chain-svc-alpha", LeafID: "leaf-svc-alpha", IntermediateIDs: []string{"intermediate-main-alpha"}, ScopeID: "scope-core"},
			{ID: "chain-svc-beta", LeafID: "leaf-svc-beta", IntermediateIDs: []string{"intermediate-main-beta"}, ScopeID: "scope-beta"},
			{ID: "chain-team", LeafID: "leaf-team", IntermediateIDs: []string{"team-ca", "dept-ca"}, ScopeID: "scope-core", Description: "transitive dependent sample"},
		},
	}
	// Remove the intentionally unreachable issuer from snapshot material. It is
	// retained separately for diagnostics only.
	filtered := set.Certificates[:0]
	for _, record := range set.Certificates {
		if record.Name != "broken-issuer" {
			filtered = append(filtered, record)
		}
	}
	set.Certificates = filtered
	sort.Slice(set.Certificates, func(i, j int) bool { return set.Certificates[i].ID < set.Certificates[j].ID })
	return set, nil
}

func publicKeyID(public ed25519.PublicKey) []byte {
	sum := sha256.Sum256(public)
	return append([]byte(nil), sum[:20]...)
}
