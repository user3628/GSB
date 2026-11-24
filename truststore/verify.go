package truststore

import (
	"crypto/x509"
	"fmt"
	"sort"
	"strings"
	"time"
)

// VerificationOptions controls non-structural validation. CurrentTime is
// primarily useful for deterministic tests; zero means time.Now().UTC().
type VerificationOptions struct {
	CurrentTime time.Time
}

// ChainResult is the stable verdict for one host chain.
type ChainResult struct {
	ChainID   string   `json:"chain_id"`
	Code      Code     `json:"code"`
	Detail    string   `json:"detail,omitempty"`
	ScopeID   string   `json:"scope_id,omitempty"`
	AnchorID  string   `json:"anchor_id,omitempty"`
	PathIDs   []string `json:"path_ids"`
	PathNames []string `json:"path_names"`
}

type parsedCertificate struct {
	record CertificateRecord
	cert   *x509.Certificate
}

type validationContext struct {
	content         SnapshotContent
	now             time.Time
	certificates    map[string]parsedCertificate
	roots           map[string]struct{}
	intermediates   map[string]struct{}
	revoked         map[string]struct{}
	superseded      map[string]struct{}
	scopes          map[string]TrustScope
	candidateIssuer map[string][]parsedCertificate
}

func newValidationContext(content SnapshotContent, options VerificationOptions) (*validationContext, error) {
	now := options.CurrentTime
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ctx := &validationContext{
		content:         content,
		now:             now.UTC(),
		certificates:    make(map[string]parsedCertificate, len(content.Certificates)),
		roots:           stringSet(content.RootIDs),
		intermediates:   stringSet(content.IntermediateIDs),
		revoked:         stringSet(content.RevokedIDs),
		superseded:      stringSet(content.SupersededIDs),
		scopes:          content.scopeIndex(),
		candidateIssuer: make(map[string][]parsedCertificate),
	}
	for _, record := range content.Certificates {
		_, cert, err := ParseCertificatePEM(record.Name, []byte(record.PEM))
		if err != nil {
			return nil, err
		}
		ctx.certificates[record.ID] = parsedCertificate{record: record, cert: cert}
	}
	// Host-supplied intermediates are always eligible candidates so that an
	// operational mistake is diagnosed as a chain problem rather than silently
	// switching to an unrelated trusted path. Revoked and superseded records are
	// included solely to produce the precise E_DEP_INVALIDATED verdict.
	candidateIDs := make(map[string]struct{})
	for id := range ctx.intermediates {
		candidateIDs[id] = struct{}{}
	}
	for id := range ctx.roots {
		candidateIDs[id] = struct{}{}
	}
	for id := range ctx.revoked {
		candidateIDs[id] = struct{}{}
	}
	for id := range ctx.superseded {
		candidateIDs[id] = struct{}{}
	}
	for _, chain := range content.HostChains {
		for _, id := range chain.IntermediateIDs {
			candidateIDs[id] = struct{}{}
		}
	}
	for id := range candidateIDs {
		parsed, ok := ctx.certificates[id]
		if !ok {
			return nil, invalid("validate chain", "candidate certificate %q has no record", id)
		}
		ctx.candidateIssuer[parsed.record.Subject] = append(ctx.candidateIssuer[parsed.record.Subject], parsed)
	}
	for subject := range ctx.candidateIssuer {
		sort.Slice(ctx.candidateIssuer[subject], func(i, j int) bool {
			return ctx.candidateIssuer[subject][i].record.ID < ctx.candidateIssuer[subject][j].record.ID
		})
	}
	return ctx, nil
}

type pathFailure struct {
	code   Code
	detail string
	path   []string
}

func failureRank(code Code) int {
	switch code {
	case CodeDependencyInvalidated:
		return 110
	case CodePathLenExceeded:
		return 100
	case CodeSignatureInvalid:
		return 95
	case CodeSelfSignedNonRoot:
		return 85
	case CodeScopeDenied:
		return 80
	case CodeCertificateExpired, CodeCertificateNotYet:
		return 70
	case CodeChainBroken:
		return 10
	default:
		return 0
	}
}

func betterFailure(current, candidate *pathFailure) *pathFailure {
	if candidate == nil {
		return current
	}
	if current == nil {
		return candidate
	}
	if failureRank(candidate.code) > failureRank(current.code) {
		return candidate
	}
	if failureRank(candidate.code) < failureRank(current.code) {
		return current
	}
	if strings.Join(candidate.path, "|") < strings.Join(current.path, "|") {
		return candidate
	}
	return current
}

// ValidateChain evaluates one host chain against the explicit trust library in
// content. It never invokes x509's system-root convenience APIs.
func ValidateChain(content SnapshotContent, chain ChainSpec, options VerificationOptions) ChainResult {
	ctx, err := newValidationContext(content, options)
	if err != nil {
		return ChainResult{ChainID: chain.ID, Code: CodeOf(err), Detail: err.Error(), ScopeID: chain.ScopeID, PathIDs: []string{}, PathNames: []string{}}
	}
	leaf, ok := ctx.certificates[chain.LeafID]
	if !ok {
		return ChainResult{ChainID: chain.ID, Code: CodeTrustInvalid, Detail: "leaf certificate is missing", ScopeID: chain.ScopeID, PathIDs: []string{}, PathNames: []string{}}
	}
	scope, scopeOK := ctx.scopes[chain.ScopeID]
	if chain.ScopeID != "" && !scopeOK {
		return ChainResult{ChainID: chain.ID, Code: CodeTrustInvalid, Detail: fmt.Sprintf("scope %q is missing", chain.ScopeID), ScopeID: chain.ScopeID, PathIDs: []string{}, PathNames: []string{}}
	}

	visited := make(map[string]bool)
	var walk func(current parsedCertificate, path []parsedCertificate) ([]parsedCertificate, *pathFailure)
	walk = func(current parsedCertificate, path []parsedCertificate) ([]parsedCertificate, *pathFailure) {
		if visited[current.record.ID] {
			return nil, &pathFailure{code: CodeChainBroken, detail: "issuer cycle detected", path: appendPathIDs(path, current)}
		}
		visited[current.record.ID] = true
		defer delete(visited, current.record.ID)

		path = append(path, current)
		if _, isRoot := ctx.roots[current.record.ID]; isRoot {
			if failure := ctx.validateAnchor(path, chain.ScopeID, scope); failure != nil {
				return nil, failure
			}
			return path, nil
		}

		if current.record.SelfSigned {
			return nil, &pathFailure{code: CodeSelfSignedNonRoot, detail: fmt.Sprintf("self-signed certificate %q is not a trusted root", current.record.Name), path: pathIDs(path)}
		}

		var best *pathFailure
		candidates := ctx.candidateIssuer[current.record.Issuer]
		if len(candidates) == 0 {
			return nil, &pathFailure{code: CodeChainBroken, detail: fmt.Sprintf("issuer %q is not present in the chain or trust library", current.record.Issuer), path: pathIDs(path)}
		}
		for _, parent := range candidates {
			if !issuerMatches(current, parent) {
				continue
			}
			if _, revoked := ctx.revoked[parent.record.ID]; revoked {
				best = betterFailure(best, &pathFailure{code: CodeDependencyInvalidated, detail: fmt.Sprintf("issuer %q is revoked", parent.record.Name), path: pathIDs(append(path, parent))})
				continue
			}
			if _, superseded := ctx.superseded[parent.record.ID]; superseded {
				best = betterFailure(best, &pathFailure{code: CodeDependencyInvalidated, detail: fmt.Sprintf("issuer %q was replaced by a cross-signature", parent.record.Name), path: pathIDs(append(path, parent))})
				continue
			}
			if !parent.cert.BasicConstraintsValid || !parent.cert.IsCA {
				best = betterFailure(best, &pathFailure{code: CodeChainBroken, detail: fmt.Sprintf("issuer %q is not a CA", parent.record.Name), path: pathIDs(append(path, parent))})
				continue
			}
			if parent.cert.KeyUsage != 0 && parent.cert.KeyUsage&x509.KeyUsageCertSign == 0 {
				best = betterFailure(best, &pathFailure{code: CodeChainBroken, detail: fmt.Sprintf("issuer %q lacks certificate signing usage", parent.record.Name), path: pathIDs(append(path, parent))})
				continue
			}
			if err := current.cert.CheckSignatureFrom(parent.cert); err != nil {
				best = betterFailure(best, &pathFailure{code: CodeSignatureInvalid, detail: fmt.Sprintf("signature by %q is invalid", parent.record.Name), path: pathIDs(append(path, parent))})
				continue
			}
			var pathLenFailure *pathFailure
			if failure := checkPathLengths(append(path, parent)); failure != nil {
				pathLenFailure = failure
				best = betterFailure(best, failure)
			}
			if _, trusted := ctx.intermediates[parent.record.ID]; !trusted {
				if _, root := ctx.roots[parent.record.ID]; !root {
					code := CodeChainBroken
					detail := fmt.Sprintf("intermediate %q is not trusted", parent.record.Name)
					if parent.record.SelfSigned {
						code = CodeSelfSignedNonRoot
						detail = fmt.Sprintf("self-signed certificate %q is neither a root nor a trusted intermediate", parent.record.Name)
					}
					best = betterFailure(best, &pathFailure{code: code, detail: detail, path: pathIDs(append(path, parent))})
					continue
				}
			}
			complete, failure := walk(parent, path)
			if failure == nil && pathLenFailure == nil {
				return complete, nil
			}
			if failure == nil {
				best = betterFailure(best, pathLenFailure)
				continue
			}
			best = betterFailure(best, failure)
		}
		if best == nil {
			best = &pathFailure{code: CodeChainBroken, detail: fmt.Sprintf("no usable issuer for %q", current.record.Name), path: pathIDs(path)}
		}
		return nil, best
	}

	complete, failure := walk(leaf, nil)
	if failure != nil {
		return ChainResult{
			ChainID:   chain.ID,
			Code:      failure.code,
			Detail:    failure.detail,
			ScopeID:   chain.ScopeID,
			PathIDs:   append([]string(nil), failure.path...),
			PathNames: ctx.namesForIDs(failure.path),
		}
	}
	if failure := ctx.checkValidity(complete); failure != nil {
		return failure.result(chain, ctx)
	}
	if failure := ctx.checkScope(complete, chain.ScopeID, scope); failure != nil {
		return failure.result(chain, ctx)
	}
	ids := pathIDs(complete)
	return ChainResult{
		ChainID:   chain.ID,
		Code:      CodeOK,
		ScopeID:   chain.ScopeID,
		AnchorID:  complete[len(complete)-1].record.ID,
		PathIDs:   ids,
		PathNames: ctx.namesForIDs(ids),
	}
}

func (f *pathFailure) result(chain ChainSpec, ctx *validationContext) ChainResult {
	return ChainResult{
		ChainID:   chain.ID,
		Code:      f.code,
		Detail:    f.detail,
		ScopeID:   chain.ScopeID,
		PathIDs:   append([]string(nil), f.path...),
		PathNames: ctx.namesForIDs(f.path),
	}
}

func (ctx *validationContext) validateAnchor(path []parsedCertificate, scopeID string, scope TrustScope) *pathFailure {
	anchor := path[len(path)-1]
	if !anchor.cert.BasicConstraintsValid || !anchor.cert.IsCA {
		return &pathFailure{code: CodeChainBroken, detail: fmt.Sprintf("trusted root %q is not a CA", anchor.record.Name), path: pathIDs(path)}
	}
	if anchor.cert.KeyUsage != 0 && anchor.cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return &pathFailure{code: CodeChainBroken, detail: fmt.Sprintf("trusted root %q lacks certificate signing usage", anchor.record.Name), path: pathIDs(path)}
	}
	if anchor.cert.Subject.String() == anchor.cert.Issuer.String() {
		if err := anchor.cert.CheckSignatureFrom(anchor.cert); err != nil {
			return &pathFailure{code: CodeSignatureInvalid, detail: fmt.Sprintf("root %q self-signature is invalid", anchor.record.Name), path: pathIDs(path)}
		}
	}
	if failure := checkPathLengths(path); failure != nil {
		return failure
	}
	if failure := ctx.checkValidity(path); failure != nil {
		return failure
	}
	return ctx.checkScope(path, scopeID, scope)
}

func (ctx *validationContext) checkValidity(path []parsedCertificate) *pathFailure {
	for _, item := range path {
		if ctx.now.Before(item.cert.NotBefore) {
			return &pathFailure{code: CodeCertificateNotYet, detail: fmt.Sprintf("certificate %q is not yet valid", item.record.Name), path: pathIDs(path)}
		}
		if ctx.now.After(item.cert.NotAfter) {
			return &pathFailure{code: CodeCertificateExpired, detail: fmt.Sprintf("certificate %q has expired", item.record.Name), path: pathIDs(path)}
		}
	}
	return nil
}

func (ctx *validationContext) checkScope(path []parsedCertificate, scopeID string, scope TrustScope) *pathFailure {
	if scopeID == "" {
		return nil
	}
	anchor := path[len(path)-1].record.ID
	allowedRoots := stringSet(scope.AllowedRootIDs)
	if _, ok := allowedRoots[anchor]; !ok {
		return &pathFailure{code: CodeScopeDenied, detail: fmt.Sprintf("root %q is outside scope %q", anchor, scopeID), path: pathIDs(path)}
	}
	allowedIntermediates := stringSet(scope.AllowedIntermediateIDs)
	for _, item := range path[1 : len(path)-1] {
		if _, ok := allowedIntermediates[item.record.ID]; !ok {
			return &pathFailure{code: CodeScopeDenied, detail: fmt.Sprintf("intermediate %q is outside scope %q", item.record.Name, scopeID), path: pathIDs(path)}
		}
	}
	return nil
}

func (ctx *validationContext) namesForIDs(ids []string) []string {
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		if item, ok := ctx.certificates[id]; ok {
			name := item.record.Name
			if name == "" {
				name = id
			}
			names = append(names, name)
		} else {
			names = append(names, id)
		}
	}
	return names
}

func issuerMatches(child, parent parsedCertificate) bool {
	if child.record.Issuer != parent.record.Subject {
		return false
	}
	if child.record.AuthorityKeyID != "" && parent.record.SubjectKeyID != "" && child.record.AuthorityKeyID != parent.record.SubjectKeyID {
		return false
	}
	return true
}

func checkPathLengths(path []parsedCertificate) *pathFailure {
	for i := 1; i < len(path); i++ {
		issuer := path[i].cert
		if !issuer.BasicConstraintsValid || !issuer.IsCA {
			continue
		}
		maxPathLen := -1
		if issuer.MaxPathLen > 0 || (issuer.MaxPathLen == 0 && issuer.MaxPathLenZero) {
			maxPathLen = issuer.MaxPathLen
		}
		if maxPathLen < 0 {
			continue
		}
		below := 0
		for j := 1; j < i; j++ {
			item := path[j].cert
			if item.IsCA && item.RawSubject != nil && item.RawIssuer != nil && string(item.RawSubject) != string(item.RawIssuer) {
				below++
			}
		}
		if below > maxPathLen {
			return &pathFailure{
				code:   CodePathLenExceeded,
				detail: fmt.Sprintf("CA %q permits %d intermediate CA(s) below it, found %d", path[i].record.Name, maxPathLen, below),
				path:   pathIDs(path),
			}
		}
	}
	return nil
}

func pathIDs(path []parsedCertificate) []string {
	ids := make([]string, len(path))
	for i, item := range path {
		ids[i] = item.record.ID
	}
	return ids
}

func appendPathIDs(path []parsedCertificate, current parsedCertificate) []string {
	ids := pathIDs(path)
	return append(ids, current.record.ID)
}

// VerifyAll evaluates every host chain in deterministic chain-ID order.
func VerifyAll(content SnapshotContent, options VerificationOptions) []ChainResult {
	normalized, err := content.Normalize()
	if err != nil {
		return []ChainResult{{ChainID: "*", Code: CodeOf(err), Detail: err.Error(), PathIDs: []string{}, PathNames: []string{}}}
	}
	results := make([]ChainResult, 0, len(normalized.HostChains))
	for _, chain := range normalized.HostChains {
		results = append(results, ValidateChain(normalized, chain, options))
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].ChainID != results[j].ChainID {
			return results[i].ChainID < results[j].ChainID
		}
		return results[i].Code < results[j].Code
	})
	return results
}
