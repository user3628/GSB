package truststore

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// OperationType identifies one of the supported incremental trust operations.
type OperationType string

const (
	OperationAddRoot               OperationType = "add_root"
	OperationRevoke                OperationType = "revoke"
	OperationReplaceCrossSignature OperationType = "replace_cross_signature"
	OperationTightenScope          OperationType = "tighten_scope"
)

// CertificateInput allows a change set to introduce new certificate material.
// Supplying a stable alias is optional but makes diagnostics easier to read.
type CertificateInput struct {
	Name string `json:"name,omitempty"`
	PEM  string `json:"pem"`
}

// Operation is deliberately flat so it can be encoded in configuration files
// without custom unmarshalling. Only fields relevant to Type are interpreted.
type Operation struct {
	Type                  OperationType `json:"type"`
	CertID                string        `json:"cert_id,omitempty"`
	Reason                string        `json:"reason,omitempty"`
	OldCertID             string        `json:"old_cert_id,omitempty"`
	NewCertID             string        `json:"new_cert_id,omitempty"`
	ScopeID               string        `json:"scope_id,omitempty"`
	RemoveRootIDs         []string      `json:"remove_root_ids,omitempty"`
	RemoveIntermediateIDs []string      `json:"remove_intermediate_ids,omitempty"`
}

// Changeset is applied as one SQLite transaction and produces one snapshot.
type Changeset struct {
	Certificates []CertificateInput `json:"certificates,omitempty"`
	Operations   []Operation        `json:"operations"`
}

// ApplyOptions contains request-scoped concurrency and fault-injection controls.
type ApplyOptions struct {
	RequestID       string
	ExpectedVersion *int64
	WaitTimeout     time.Duration
	Diagnostics     *ApplyDiagnostics
}

// ApplyDiagnostics intentionally exposes deterministic process-failure test
// points. Hosts leave it nil; the example CLI uses it to verify crash recovery.
type ApplyDiagnostics struct {
	MarkerBeforeCommit string
	MarkerAfterCommit  string
	HoldBeforeCommit   time.Duration
	HoldAfterCommit    time.Duration
}

type certLookup struct {
	byID   map[string]CertificateRecord
	byName map[string]string
	bySKID map[string][]string
}

func newCertLookup(content SnapshotContent) (*certLookup, error) {
	lookup := &certLookup{
		byID:   make(map[string]CertificateRecord, len(content.Certificates)),
		byName: make(map[string]string),
		bySKID: make(map[string][]string),
	}
	for _, cert := range content.Certificates {
		lookup.byID[cert.ID] = cert
		if cert.Name != "" {
			if existing, ok := lookup.byName[cert.Name]; ok && existing != cert.ID {
				return nil, invalid("resolve certificate", "duplicate alias %q", cert.Name)
			}
			lookup.byName[cert.Name] = cert.ID
		}
		if cert.SubjectKeyID != "" {
			lookup.bySKID[cert.SubjectKeyID] = append(lookup.bySKID[cert.SubjectKeyID], cert.ID)
		}
	}
	for key := range lookup.bySKID {
		sort.Strings(lookup.bySKID[key])
	}
	return lookup, nil
}

func (l *certLookup) resolve(ref, field string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", invalid("apply changeset", "%s contains an empty certificate reference", field)
	}
	if _, ok := l.byID[ref]; ok {
		return ref, nil
	}
	if id, ok := l.byName[ref]; ok {
		return id, nil
	}
	if ids := l.bySKID[strings.ToLower(ref)]; len(ids) == 1 {
		return ids[0], nil
	}
	return "", invalid("apply changeset", "%s references unknown or ambiguous certificate %q", field, ref)
}

type mutationState struct {
	content       SnapshotContent
	lookup        *certLookup
	roots         map[string]struct{}
	intermediates map[string]struct{}
	revoked       map[string]struct{}
	superseded    map[string]struct{}
	scopes        map[string]TrustScope
	origins       map[string]struct{}
}

func newMutationState(content SnapshotContent, changeset Changeset) (*mutationState, error) {
	normalized, err := content.Normalize()
	if err != nil {
		return nil, err
	}
	state := &mutationState{
		content:       normalized,
		roots:         stringSet(normalized.RootIDs),
		intermediates: stringSet(normalized.IntermediateIDs),
		revoked:       stringSet(normalized.RevokedIDs),
		superseded:    stringSet(normalized.SupersededIDs),
		scopes:        normalized.scopeIndex(),
		origins:       make(map[string]struct{}),
	}
	for _, input := range changeset.Certificates {
		input.Name = strings.TrimSpace(input.Name)
		if strings.TrimSpace(input.PEM) == "" {
			return nil, invalid("apply changeset", "certificate input %q has no PEM", input.Name)
		}
		record, _, err := ParseCertificatePEM(input.Name, []byte(input.PEM))
		if err != nil {
			return nil, err
		}
		existing, exists := state.certificateByID(record.ID)
		if exists {
			if existing.PEM != record.PEM {
				return nil, invalid("apply changeset", "certificate %q has conflicting PEM content", record.ID)
			}
			if record.Name != "" && existing.Name != "" && record.Name != existing.Name {
				return nil, invalid("apply changeset", "certificate %q has conflicting aliases %q and %q", record.ID, existing.Name, record.Name)
			}
			continue
		}
		state.content.Certificates = append(state.content.Certificates, record)
	}
	state.lookup, err = newCertLookup(state.content)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (s *mutationState) certificateByID(id string) (CertificateRecord, bool) {
	for _, cert := range s.content.Certificates {
		if cert.ID == id {
			return cert, true
		}
	}
	return CertificateRecord{}, false
}

func (s *mutationState) apply(changeset Changeset) error {
	if len(changeset.Operations) == 0 {
		return invalid("apply changeset", "changeset must contain at least one operation")
	}
	for index, operation := range changeset.Operations {
		if err := s.applyOne(index, operation); err != nil {
			return err
		}
	}
	return nil
}

func (s *mutationState) applyOne(index int, operation Operation) error {
	opName := fmt.Sprintf("operation[%d] %s", index, operation.Type)
	switch operation.Type {
	case OperationAddRoot:
		id, err := s.lookup.resolve(operation.CertID, opName+" cert_id")
		if err != nil {
			return err
		}
		if _, exists := s.roots[id]; exists {
			return invalid("apply changeset", "%s: certificate %q is already a trusted root", opName, id)
		}
		if _, exists := s.intermediates[id]; exists {
			return invalid("apply changeset", "%s: certificate %q is already a trusted intermediate", opName, id)
		}
		if _, revoked := s.revoked[id]; revoked {
			return invalid("apply changeset", "%s: revoked certificate %q cannot become a root", opName, id)
		}
		if _, superseded := s.superseded[id]; superseded {
			return invalid("apply changeset", "%s: superseded certificate %q cannot become a root", opName, id)
		}
		record := s.lookup.byID[id]
		if !record.IsCA || !record.SelfSigned {
			return invalid("apply changeset", "%s: root candidate %q must be a self-signed CA", opName, id)
		}
		_, parsed, err := ParseCertificatePEM(record.Name, []byte(record.PEM))
		if err != nil {
			return err
		}
		if err := parsed.CheckSignatureFrom(parsed); err != nil {
			return invalid("apply changeset", "%s: root candidate %q has an invalid self-signature", opName, id)
		}
		s.roots[id] = struct{}{}
		return nil

	case OperationRevoke:
		id, err := s.lookup.resolve(operation.CertID, opName+" cert_id")
		if err != nil {
			return err
		}
		_, isRoot := s.roots[id]
		_, isIntermediate := s.intermediates[id]
		if !isRoot && !isIntermediate {
			return invalid("apply changeset", "%s: certificate %q is not a trusted CA", opName, id)
		}
		if _, already := s.revoked[id]; already {
			return invalid("apply changeset", "%s: certificate %q is already revoked", opName, id)
		}
		delete(s.roots, id)
		delete(s.intermediates, id)
		s.revoked[id] = struct{}{}
		s.origins[id] = struct{}{}
		s.removeFromAllScopes(id)
		return nil

	case OperationReplaceCrossSignature:
		oldID, err := s.lookup.resolve(operation.OldCertID, opName+" old_cert_id")
		if err != nil {
			return err
		}
		newID, err := s.lookup.resolve(operation.NewCertID, opName+" new_cert_id")
		if err != nil {
			return err
		}
		if oldID == newID {
			return invalid("apply changeset", "%s: old and new certificates are identical", opName)
		}
		oldRecord := s.lookup.byID[oldID]
		newRecord := s.lookup.byID[newID]
		_, oldIsRoot := s.roots[oldID]
		_, oldIsIntermediate := s.intermediates[oldID]
		if !oldIsRoot && !oldIsIntermediate {
			return invalid("apply changeset", "%s: old certificate %q is not a trusted root or intermediate", opName, oldID)
		}
		if _, revoked := s.revoked[newID]; revoked {
			return invalid("apply changeset", "%s: replacement certificate %q is revoked", opName, newID)
		}
		if _, superseded := s.superseded[newID]; superseded {
			return invalid("apply changeset", "%s: replacement certificate %q is superseded", opName, newID)
		}
		if _, root := s.roots[newID]; root {
			return invalid("apply changeset", "%s: replacement certificate %q is already trusted", opName, newID)
		}
		if _, intermediate := s.intermediates[newID]; intermediate {
			return invalid("apply changeset", "%s: replacement certificate %q is already trusted", opName, newID)
		}
		if !oldRecord.IsCA || !newRecord.IsCA {
			return invalid("apply changeset", "%s: cross-signature replacement requires CA certificates", opName)
		}
		if oldRecord.Subject != newRecord.Subject || oldRecord.SubjectKeyID != newRecord.SubjectKeyID || oldRecord.PublicKeyHash != newRecord.PublicKeyHash {
			return invalid("apply changeset", "%s: certificates do not represent the same CA key and subject", opName)
		}
		if oldRecord.Issuer == newRecord.Issuer && oldRecord.AuthorityKeyID == newRecord.AuthorityKeyID {
			return invalid("apply changeset", "%s: replacement is not a distinct cross-signature", opName)
		}
		if oldIsRoot {
			delete(s.roots, oldID)
			s.roots[newID] = struct{}{}
			record := newRecord
			if !record.SelfSigned {
				return invalid("apply changeset", "%s: replacement root anchor must be self-signed", opName)
			}
		} else if oldIsIntermediate {
			delete(s.intermediates, oldID)
			s.intermediates[newID] = struct{}{}
		}
		s.superseded[oldID] = struct{}{}
		s.replaceInAllScopes(oldID, newID)
		return nil

	case OperationTightenScope:
		scopeID := strings.TrimSpace(operation.ScopeID)
		scope, ok := s.scopes[scopeID]
		if !ok {
			return invalid("apply changeset", "%s: scope %q does not exist", opName, scopeID)
		}
		if len(operation.RemoveRootIDs) == 0 && len(operation.RemoveIntermediateIDs) == 0 {
			return invalid("apply changeset", "%s: tighten_scope requires at least one removal", opName)
		}
		rootAllowed := stringSet(scope.AllowedRootIDs)
		intermediateAllowed := stringSet(scope.AllowedIntermediateIDs)
		changed := false
		for _, ref := range operation.RemoveRootIDs {
			id, err := s.lookup.resolve(ref, opName+" remove_root_ids")
			if err != nil {
				return err
			}
			if _, present := rootAllowed[id]; !present {
				return invalid("apply changeset", "%s: root %q is not allowed by scope %q", opName, id, scopeID)
			}
			delete(rootAllowed, id)
			s.origins[id] = struct{}{}
			changed = true
		}
		for _, ref := range operation.RemoveIntermediateIDs {
			id, err := s.lookup.resolve(ref, opName+" remove_intermediate_ids")
			if err != nil {
				return err
			}
			if _, present := intermediateAllowed[id]; !present {
				return invalid("apply changeset", "%s: intermediate %q is not allowed by scope %q", opName, id, scopeID)
			}
			delete(intermediateAllowed, id)
			s.origins[id] = struct{}{}
			changed = true
		}
		if !changed {
			return invalid("apply changeset", "%s: scope %q did not change", opName, scopeID)
		}
		scope.AllowedRootIDs = sortedSet(rootAllowed)
		scope.AllowedIntermediateIDs = sortedSet(intermediateAllowed)
		s.scopes[scopeID] = scope
		return nil
	default:
		return invalid("apply changeset", "%s: unsupported operation type", opName)
	}
}

func (s *mutationState) removeFromAllScopes(id string) {
	for scopeID, scope := range s.scopes {
		roots := stringSet(scope.AllowedRootIDs)
		intermediates := stringSet(scope.AllowedIntermediateIDs)
		delete(roots, id)
		delete(intermediates, id)
		scope.AllowedRootIDs = sortedSet(roots)
		scope.AllowedIntermediateIDs = sortedSet(intermediates)
		s.scopes[scopeID] = scope
	}
}

func (s *mutationState) replaceInAllScopes(oldID, newID string) {
	for scopeID, scope := range s.scopes {
		rootAllowed := stringSet(scope.AllowedRootIDs)
		intermediateAllowed := stringSet(scope.AllowedIntermediateIDs)
		if _, ok := rootAllowed[oldID]; ok {
			delete(rootAllowed, oldID)
			rootAllowed[newID] = struct{}{}
		}
		if _, ok := intermediateAllowed[oldID]; ok {
			delete(intermediateAllowed, oldID)
			intermediateAllowed[newID] = struct{}{}
		}
		scope.AllowedRootIDs = sortedSet(rootAllowed)
		scope.AllowedIntermediateIDs = sortedSet(intermediateAllowed)
		s.scopes[scopeID] = scope
	}
}

func (s *mutationState) finalContent() (SnapshotContent, error) {
	content := s.content.Clone()
	content.RootIDs = sortedSet(s.roots)
	content.IntermediateIDs = sortedSet(s.intermediates)
	content.RevokedIDs = sortedSet(s.revoked)
	content.SupersededIDs = sortedSet(s.superseded)
	content.Scopes = make([]TrustScope, 0, len(s.scopes))
	for _, scope := range s.scopes {
		content.Scopes = append(content.Scopes, scope)
	}
	sort.Slice(content.Scopes, func(i, j int) bool { return content.Scopes[i].ID < content.Scopes[j].ID })
	normalized, err := content.Normalize()
	if err != nil {
		return SnapshotContent{}, err
	}
	return normalized, nil
}

func (s *mutationState) originIDs() []string { return sortedSet(s.origins) }
