package truststore

import (
	"sort"
	"strings"
)

// DependencyNode is a certificate participating in a dependency closure.
type DependencyNode struct {
	CertID string `json:"cert_id"`
	Name   string `json:"name,omitempty"`
}

// PropagationPath records the shortest issuance path from an invalidation
// origin to a directly or transitively dependent certificate.
type PropagationPath struct {
	Depth     int              `json:"depth"`
	CertIDs   []string         `json:"cert_ids"`
	CertNames []string         `json:"cert_names"`
	Nodes     []DependencyNode `json:"nodes"`
}

// AffectedChain is the post-change verdict for a host chain that depends on an
// invalidated certificate.
type AffectedChain struct {
	ChainID   string   `json:"chain_id"`
	Tier      string   `json:"tier"`
	Code      Code     `json:"code"`
	PathIDs   []string `json:"path_ids"`
	PathNames []string `json:"path_names"`
}

// InvalidationClosure is the deterministic downstream impact of revocations or
// scope tightening.
type InvalidationClosure struct {
	Code               Code              `json:"code"`
	OriginCertIDs      []string          `json:"origin_cert_ids"`
	OriginNames        []string          `json:"origin_names"`
	DirectCertIDs      []string          `json:"direct_cert_ids"`
	TransitiveCertIDs  []string          `json:"transitive_cert_ids"`
	DirectChainIDs     []string          `json:"direct_chain_ids"`
	TransitiveChainIDs []string          `json:"transitive_chain_ids"`
	Propagation        []PropagationPath `json:"propagation"`
	AffectedChains     []AffectedChain   `json:"affected_chains"`
}

func buildInvalidationClosure(content SnapshotContent, origins []string, options VerificationOptions) (*InvalidationClosure, error) {
	if len(origins) == 0 {
		return nil, nil
	}
	normalized, err := content.Normalize()
	if err != nil {
		return nil, err
	}
	byID := normalized.certificateIndex()
	originSet := stringSet(origins)
	for id := range originSet {
		if _, ok := byID[id]; !ok {
			return nil, invalid("build invalidation closure", "origin %q has no certificate record", id)
		}
	}

	children := make(map[string][]string)
	for _, child := range normalized.Certificates {
		for _, parent := range normalized.Certificates {
			if child.ID == parent.ID {
				continue
			}
			childParsed := parsedCertificate{record: child}
			parentParsed := parsedCertificate{record: parent}
			if issuerMatches(childParsed, parentParsed) {
				children[parent.ID] = append(children[parent.ID], child.ID)
			}
		}
	}
	for parent := range children {
		children[parent] = sortedUnique(children[parent])
	}

	type queuedPath struct {
		id   string
		path []string
	}
	queue := make([]queuedPath, 0, len(origins))
	for _, id := range sortedUnique(origins) {
		queue = append(queue, queuedPath{id: id, path: []string{id}})
	}
	bestDepth := make(map[string]int)
	bestPath := make(map[string][]string)
	for _, item := range queue {
		bestDepth[item.id] = 0
		bestPath[item.id] = append([]string(nil), item.path...)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, child := range children[current.id] {
			path := append(append([]string(nil), current.path...), child)
			depth := len(path) - 1
			existingDepth, seen := bestDepth[child]
			if seen && (existingDepth < depth || (existingDepth == depth && strings.Join(bestPath[child], "|") <= strings.Join(path, "|"))) {
				continue
			}
			bestDepth[child] = depth
			bestPath[child] = path
			queue = append(queue, queuedPath{id: child, path: path})
		}
	}

	closure := &InvalidationClosure{
		Code:           CodeDependencyInvalidated,
		OriginCertIDs:  sortedUnique(origins),
		Propagation:    []PropagationPath{},
		AffectedChains: []AffectedChain{},
	}
	closure.OriginNames = namesFor(byID, closure.OriginCertIDs)
	for id, depth := range bestDepth {
		if depth == 0 {
			continue
		}
		if depth == 1 {
			closure.DirectCertIDs = append(closure.DirectCertIDs, id)
		} else {
			closure.TransitiveCertIDs = append(closure.TransitiveCertIDs, id)
		}
		path := bestPath[id]
		closure.Propagation = append(closure.Propagation, PropagationPath{
			Depth:     depth,
			CertIDs:   append([]string(nil), path...),
			CertNames: namesFor(byID, path),
			Nodes:     nodesFor(byID, path),
		})
	}
	closure.DirectCertIDs = sortedUnique(closure.DirectCertIDs)
	closure.TransitiveCertIDs = sortedUnique(closure.TransitiveCertIDs)
	sort.Slice(closure.Propagation, func(i, j int) bool {
		if closure.Propagation[i].Depth != closure.Propagation[j].Depth {
			return closure.Propagation[i].Depth < closure.Propagation[j].Depth
		}
		return strings.Join(closure.Propagation[i].CertIDs, "|") < strings.Join(closure.Propagation[j].CertIDs, "|")
	})

	descendants := stringSet(closure.OriginCertIDs)
	for id := range bestDepth {
		descendants[id] = struct{}{}
	}
	results := VerifyAll(normalized, options)
	resultByChain := make(map[string]ChainResult, len(results))
	for _, result := range results {
		resultByChain[result.ChainID] = result
	}
	chainsByID := make(map[string]ChainSpec, len(normalized.HostChains))
	for _, chain := range normalized.HostChains {
		chainsByID[chain.ID] = chain
	}
	for _, chain := range normalized.HostChains {
		result := resultByChain[chain.ID]
		if result.Code == CodeOK {
			continue
		}
		if !pathOverlaps(result.PathIDs, descendants) && !pathOverlaps(chain.IntermediateIDs, descendants) {
			continue
		}
		tier := "transitive"
		if len(result.PathIDs) <= 1 {
			tier = "direct"
		} else if _, direct := originSet[result.PathIDs[1]]; direct {
			tier = "direct"
		}
		closure.AffectedChains = append(closure.AffectedChains, AffectedChain{
			ChainID:   chain.ID,
			Tier:      tier,
			Code:      result.Code,
			PathIDs:   append([]string(nil), result.PathIDs...),
			PathNames: append([]string(nil), result.PathNames...),
		})
		if tier == "direct" {
			closure.DirectChainIDs = append(closure.DirectChainIDs, chain.ID)
		} else {
			closure.TransitiveChainIDs = append(closure.TransitiveChainIDs, chain.ID)
		}
	}
	sort.Slice(closure.AffectedChains, func(i, j int) bool {
		if closure.AffectedChains[i].ChainID != closure.AffectedChains[j].ChainID {
			return closure.AffectedChains[i].ChainID < closure.AffectedChains[j].ChainID
		}
		return closure.AffectedChains[i].Code < closure.AffectedChains[j].Code
	})
	closure.DirectChainIDs = sortedUnique(closure.DirectChainIDs)
	closure.TransitiveChainIDs = sortedUnique(closure.TransitiveChainIDs)
	return closure, nil
}

func pathOverlaps(path []string, set map[string]struct{}) bool {
	for _, id := range path {
		if _, ok := set[id]; ok {
			return true
		}
	}
	return false
}

func namesFor(byID map[string]CertificateRecord, ids []string) []string {
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		name := byID[id].Name
		if name == "" {
			name = id
		}
		names = append(names, name)
	}
	return names
}

func nodesFor(byID map[string]CertificateRecord, ids []string) []DependencyNode {
	nodes := make([]DependencyNode, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, DependencyNode{CertID: id, Name: byID[id].Name})
	}
	return nodes
}
