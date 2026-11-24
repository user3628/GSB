package truststore

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"sort"
	"strings"
	"time"
)

const snapshotSchemaVersion = "1"

// CertificateRecord is the persisted, canonical representation of a sample or
// host certificate. ID is the SHA-256 fingerprint of the DER bytes; Name is an
// optional human-friendly alias used by change sets and examples.
type CertificateRecord struct {
	ID                 string `json:"id"`
	Name               string `json:"name,omitempty"`
	PEM                string `json:"pem"`
	Subject            string `json:"subject"`
	Issuer             string `json:"issuer"`
	SerialNumber       string `json:"serial_number"`
	SubjectKeyID       string `json:"subject_key_id,omitempty"`
	AuthorityKeyID     string `json:"authority_key_id,omitempty"`
	PublicKeyHash      string `json:"public_key_hash"`
	IsCA               bool   `json:"is_ca"`
	PathLen            int    `json:"path_len"`
	SelfSigned         bool   `json:"self_signed"`
	SignatureAlgorithm string `json:"signature_algorithm"`
	NotBefore          string `json:"not_before"`
	NotAfter           string `json:"not_after"`
}

// CertificateID returns the canonical fingerprint for a DER certificate.
func CertificateID(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}

// ParseCertificatePEM parses exactly one PEM certificate block. The trust
// store never consults the system root pool.
func ParseCertificatePEM(name string, data []byte) (CertificateRecord, *x509.Certificate, error) {
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return CertificateRecord{}, nil, NewError(CodeInputInvalid, "parse PEM", "expected a CERTIFICATE PEM block", nil)
	}
	if len(strings.TrimSpace(string(rest))) != 0 {
		return CertificateRecord{}, nil, NewError(CodeInputInvalid, "parse PEM", "multiple PEM blocks are not supported", nil)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return CertificateRecord{}, nil, NewError(CodeInputInvalid, "parse X.509", "invalid certificate", err)
	}
	pubDer, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return CertificateRecord{}, nil, NewError(CodeInputInvalid, "marshal public key", "", err)
	}
	pubHash := sha256.Sum256(pubDer)
	rec := CertificateRecord{
		ID:                 CertificateID(cert.Raw),
		Name:               name,
		PEM:                string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})),
		Subject:            cert.Subject.String(),
		Issuer:             cert.Issuer.String(),
		SerialNumber:       cert.SerialNumber.String(),
		SubjectKeyID:       strings.ToLower(hex.EncodeToString(cert.SubjectKeyId)),
		AuthorityKeyID:     strings.ToLower(hex.EncodeToString(cert.AuthorityKeyId)),
		PublicKeyHash:      hex.EncodeToString(pubHash[:]),
		IsCA:               cert.IsCA,
		PathLen:            cert.MaxPathLen,
		SelfSigned:         cert.Subject.String() == cert.Issuer.String(),
		SignatureAlgorithm: cert.SignatureAlgorithm.String(),
		NotBefore:          cert.NotBefore.UTC().Format(time.RFC3339),
		NotAfter:           cert.NotAfter.UTC().Format(time.RFC3339),
	}
	if !cert.BasicConstraintsValid {
		rec.IsCA = false
	}
	return rec, cert, nil
}

// TrustScope restricts which configured roots and intermediates a named host
// chain may use. The lists are explicit: an empty list permits no certificate
// of that layer. Chains without a scope use the complete trust library.
type TrustScope struct {
	ID                     string   `json:"id"`
	AllowedRootIDs         []string `json:"allowed_root_ids"`
	AllowedIntermediateIDs []string `json:"allowed_intermediate_ids"`
}

// ChainSpec is a host-side chain sample. IntermediateIDs may be supplied in any
// order; validation reconstructs issuance relationships from X.509 fields.
type ChainSpec struct {
	ID              string   `json:"id"`
	LeafID          string   `json:"leaf_id"`
	IntermediateIDs []string `json:"intermediate_ids"`
	ScopeID         string   `json:"scope_id,omitempty"`
	Description     string   `json:"description,omitempty"`
}

// SnapshotContent is the complete material needed to replay a trust snapshot.
type SnapshotContent struct {
	Certificates    []CertificateRecord `json:"certificates"`
	RootIDs         []string            `json:"root_ids"`
	IntermediateIDs []string            `json:"intermediate_ids"`
	RevokedIDs      []string            `json:"revoked_ids"`
	SupersededIDs   []string            `json:"superseded_ids"`
	Scopes          []TrustScope        `json:"scopes"`
	HostChains      []ChainSpec         `json:"host_chains"`
}

// Clone returns a deep copy suitable for transactional mutation.
func (c SnapshotContent) Clone() SnapshotContent {
	out := c
	out.Certificates = append([]CertificateRecord(nil), c.Certificates...)
	out.RootIDs = append([]string(nil), c.RootIDs...)
	out.IntermediateIDs = append([]string(nil), c.IntermediateIDs...)
	out.RevokedIDs = append([]string(nil), c.RevokedIDs...)
	out.SupersededIDs = append([]string(nil), c.SupersededIDs...)
	out.Scopes = make([]TrustScope, len(c.Scopes))
	for i, scope := range c.Scopes {
		out.Scopes[i] = TrustScope{
			ID:                     scope.ID,
			AllowedRootIDs:         append([]string(nil), scope.AllowedRootIDs...),
			AllowedIntermediateIDs: append([]string(nil), scope.AllowedIntermediateIDs...),
		}
	}
	out.HostChains = make([]ChainSpec, len(c.HostChains))
	for i, chain := range c.HostChains {
		out.HostChains[i] = ChainSpec{
			ID:              chain.ID,
			LeafID:          chain.LeafID,
			IntermediateIDs: append([]string(nil), chain.IntermediateIDs...),
			ScopeID:         chain.ScopeID,
			Description:     chain.Description,
		}
	}
	return out
}

func (c SnapshotContent) certificateIndex() map[string]CertificateRecord {
	index := make(map[string]CertificateRecord, len(c.Certificates))
	for _, cert := range c.Certificates {
		index[cert.ID] = cert
	}
	return index
}

func (c SnapshotContent) scopeIndex() map[string]TrustScope {
	index := make(map[string]TrustScope, len(c.Scopes))
	for _, scope := range c.Scopes {
		index[scope.ID] = scope
	}
	return index
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func sortedSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortedUnique(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	return sortedSet(set)
}

// Normalize validates references, fills parsed certificate fields, resolves
// aliases to fingerprints and produces canonical ordering. It is idempotent.
func (c SnapshotContent) Normalize() (SnapshotContent, error) {
	out := c.Clone()
	certs := make([]CertificateRecord, 0, len(out.Certificates))
	seenIDs := make(map[string]struct{})
	for _, supplied := range out.Certificates {
		if supplied.PEM == "" {
			return SnapshotContent{}, invalid("normalize trust content", "certificate %q has no PEM", supplied.ID)
		}
		parsed, _, err := ParseCertificatePEM(supplied.Name, []byte(supplied.PEM))
		if err != nil {
			return SnapshotContent{}, err
		}
		if supplied.ID != "" && supplied.ID != parsed.ID {
			return SnapshotContent{}, invalid("normalize trust content", "certificate %q fingerprint does not match PEM", supplied.ID)
		}
		if supplied.Name != "" {
			parsed.Name = supplied.Name
		}
		if _, exists := seenIDs[parsed.ID]; exists {
			continue
		}
		seenIDs[parsed.ID] = struct{}{}
		certs = append(certs, parsed)
	}
	sort.Slice(certs, func(i, j int) bool { return certs[i].ID < certs[j].ID })
	out.Certificates = certs

	byID := out.certificateIndex()
	byName := make(map[string]string)
	bySKID := make(map[string][]string)
	for _, cert := range certs {
		if cert.Name != "" {
			if previous, ok := byName[cert.Name]; ok && previous != cert.ID {
				return SnapshotContent{}, invalid("normalize trust content", "duplicate certificate alias %q", cert.Name)
			}
			byName[cert.Name] = cert.ID
		}
		if cert.SubjectKeyID != "" {
			bySKID[cert.SubjectKeyID] = append(bySKID[cert.SubjectKeyID], cert.ID)
		}
	}
	resolve := func(ref, field string) (string, error) {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return "", invalid("normalize trust content", "%s contains an empty certificate reference", field)
		}
		if _, ok := byID[ref]; ok {
			return ref, nil
		}
		if id, ok := byName[ref]; ok {
			return id, nil
		}
		if ids := bySKID[strings.ToLower(ref)]; len(ids) == 1 {
			return ids[0], nil
		}
		return "", invalid("normalize trust content", "%s references unknown or ambiguous certificate %q", field, ref)
	}
	resolveList := func(values []string, field string) ([]string, error) {
		resolved := make([]string, 0, len(values))
		for _, ref := range values {
			id, err := resolve(ref, field)
			if err != nil {
				return nil, err
			}
			resolved = append(resolved, id)
		}
		return sortedUnique(resolved), nil
	}

	var err error
	if out.RootIDs, err = resolveList(out.RootIDs, "root_ids"); err != nil {
		return SnapshotContent{}, err
	}
	if out.IntermediateIDs, err = resolveList(out.IntermediateIDs, "intermediate_ids"); err != nil {
		return SnapshotContent{}, err
	}
	if out.RevokedIDs, err = resolveList(out.RevokedIDs, "revoked_ids"); err != nil {
		return SnapshotContent{}, err
	}
	if out.SupersededIDs, err = resolveList(out.SupersededIDs, "superseded_ids"); err != nil {
		return SnapshotContent{}, err
	}

	rootSet := stringSet(out.RootIDs)
	intermediateSet := stringSet(out.IntermediateIDs)
	revokedSet := stringSet(out.RevokedIDs)
	for id := range rootSet {
		if _, ok := byID[id]; !ok {
			return SnapshotContent{}, invalid("normalize trust content", "trusted root %q has no certificate record", id)
		}
		if _, revoked := revokedSet[id]; revoked {
			return SnapshotContent{}, invalid("normalize trust content", "certificate %q is both trusted and revoked", id)
		}
	}
	for id := range intermediateSet {
		if _, ok := byID[id]; !ok {
			return SnapshotContent{}, invalid("normalize trust content", "trusted intermediate %q has no certificate record", id)
		}
		if _, revoked := revokedSet[id]; revoked {
			return SnapshotContent{}, invalid("normalize trust content", "certificate %q is both trusted and revoked", id)
		}
		if _, root := rootSet[id]; root {
			return SnapshotContent{}, invalid("normalize trust content", "certificate %q is both a root and an intermediate", id)
		}
	}

	scopes := make([]TrustScope, 0, len(out.Scopes))
	seenScopes := make(map[string]struct{})
	for _, scope := range out.Scopes {
		scope.ID = strings.TrimSpace(scope.ID)
		if scope.ID == "" {
			return SnapshotContent{}, invalid("normalize trust content", "scope has an empty ID")
		}
		if _, exists := seenScopes[scope.ID]; exists {
			return SnapshotContent{}, invalid("normalize trust content", "duplicate scope %q", scope.ID)
		}
		seenScopes[scope.ID] = struct{}{}
		scope.AllowedRootIDs, err = resolveList(scope.AllowedRootIDs, "scope "+scope.ID+" allowed_root_ids")
		if err != nil {
			return SnapshotContent{}, err
		}
		scope.AllowedIntermediateIDs, err = resolveList(scope.AllowedIntermediateIDs, "scope "+scope.ID+" allowed_intermediate_ids")
		if err != nil {
			return SnapshotContent{}, err
		}
		// A scope may pre-authorize a certificate that becomes trusted in a
		// later incremental version (for example a cross-signed replacement).
		// References must exist, but need not currently be trusted.
		for _, id := range scope.AllowedRootIDs {
			if _, exists := byID[id]; !exists {
				return SnapshotContent{}, invalid("normalize trust content", "scope %q references unknown root %q", scope.ID, id)
			}
		}
		for _, id := range scope.AllowedIntermediateIDs {
			if _, exists := byID[id]; !exists {
				return SnapshotContent{}, invalid("normalize trust content", "scope %q references unknown intermediate %q", scope.ID, id)
			}
		}
		scopes = append(scopes, scope)
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i].ID < scopes[j].ID })
	out.Scopes = scopes
	scopeIDs := out.scopeIndex()

	chains := make([]ChainSpec, 0, len(out.HostChains))
	seenChains := make(map[string]struct{})
	for _, chain := range out.HostChains {
		chain.ID = strings.TrimSpace(chain.ID)
		if chain.ID == "" {
			return SnapshotContent{}, invalid("normalize trust content", "host chain has an empty ID")
		}
		if _, exists := seenChains[chain.ID]; exists {
			return SnapshotContent{}, invalid("normalize trust content", "duplicate host chain %q", chain.ID)
		}
		seenChains[chain.ID] = struct{}{}
		if chain.LeafID, err = resolve(chain.LeafID, "chain "+chain.ID+" leaf_id"); err != nil {
			return SnapshotContent{}, err
		}
		if chain.IntermediateIDs, err = resolveList(chain.IntermediateIDs, "chain "+chain.ID+" intermediate_ids"); err != nil {
			return SnapshotContent{}, err
		}
		if chain.ScopeID != "" {
			if _, ok := scopeIDs[chain.ScopeID]; !ok {
				return SnapshotContent{}, invalid("normalize trust content", "host chain %q references unknown scope %q", chain.ID, chain.ScopeID)
			}
		}
		chains = append(chains, chain)
	}
	sort.Slice(chains, func(i, j int) bool { return chains[i].ID < chains[j].ID })
	out.HostChains = chains
	return out, nil
}

// ContentHash hashes the complete canonical snapshot content.
func (c SnapshotContent) ContentHash() (string, error) {
	normalized, err := c.Normalize()
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type trustHashView struct {
	RootIDs         []string     `json:"root_ids"`
	IntermediateIDs []string     `json:"intermediate_ids"`
	RevokedIDs      []string     `json:"revoked_ids"`
	SupersededIDs   []string     `json:"superseded_ids"`
	Scopes          []TrustScope `json:"scopes"`
}

// TrustHash hashes only trust-relevant material. Replacing a leaf certificate
// without changing trust semantics does not alter this hash.
func (c SnapshotContent) TrustHash() (string, error) {
	normalized, err := c.Normalize()
	if err != nil {
		return "", err
	}
	view := trustHashView{
		RootIDs:         normalized.RootIDs,
		IntermediateIDs: normalized.IntermediateIDs,
		RevokedIDs:      normalized.RevokedIDs,
		SupersededIDs:   normalized.SupersededIDs,
		Scopes:          normalized.Scopes,
	}
	data, err := json.Marshal(view)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// Snapshot is a complete, replayable version of a trust namespace.
type Snapshot struct {
	Namespace     string          `json:"namespace"`
	Version       int64           `json:"version"`
	ParentVersion int64           `json:"parent_version"`
	ContentHash   string          `json:"content_hash"`
	TrustHash     string          `json:"trust_hash"`
	Replacement   bool            `json:"replacement"`
	Content       SnapshotContent `json:"content"`
}

// SnapshotRef is the compact form used in reports.
type SnapshotRef struct {
	Version       int64  `json:"version"`
	ParentVersion int64  `json:"parent_version"`
	ContentHash   string `json:"content_hash"`
	TrustHash     string `json:"trust_hash"`
	Replacement   bool   `json:"replacement"`
}

func (s Snapshot) Ref() SnapshotRef {
	return SnapshotRef{
		Version:       s.Version,
		ParentVersion: s.ParentVersion,
		ContentHash:   s.ContentHash,
		TrustHash:     s.TrustHash,
		Replacement:   s.Replacement,
	}
}

func (s Snapshot) Validate() error {
	if s.Version < 1 {
		return NewError(CodeSnapshotVersion, "validate snapshot", fmt.Sprintf("invalid version %d", s.Version), nil)
	}
	normalized, err := s.Content.Normalize()
	if err != nil {
		return err
	}
	contentHash, err := normalized.ContentHash()
	if err != nil {
		return err
	}
	trustHash, err := normalized.TrustHash()
	if err != nil {
		return err
	}
	if s.ContentHash != "" && s.ContentHash != contentHash {
		return NewError(CodeSnapshotVersion, "validate snapshot", fmt.Sprintf("version %d content hash mismatch", s.Version), nil)
	}
	if s.TrustHash != "" && s.TrustHash != trustHash {
		return NewError(CodeSnapshotVersion, "validate snapshot", fmt.Sprintf("version %d trust hash mismatch", s.Version), nil)
	}
	return nil
}

func detectWholeReplacement(previous, next SnapshotContent) bool {
	oldRoots := stringSet(previous.RootIDs)
	newRoots := stringSet(next.RootIDs)
	if len(oldRoots) == 0 || len(newRoots) == 0 {
		return false
	}
	for id := range oldRoots {
		if _, retained := newRoots[id]; retained {
			return false
		}
	}
	return true
}
