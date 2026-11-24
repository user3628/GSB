package main

import (
	"fmt"
	"os"
	"sort"
)

const fixedTick = "SAMPLE-T0"

func addCode(codes []string, code string) []string {
	for _, c := range codes {
		if c == code {
			return codes
		}
	}
	return append(codes, code)
}

// InspectRepo walks the generation chain of one repo and returns the full
// verdict: per-generation codes, chain state and the restore-point ruling.
// budget > 0 caps the number of generations inspected in this session.
func InspectRepo(samplesDir, repo string, budget int) (*InspectResult, error) {
	reg, err := readRegistry(samplesDir, repo)
	if err != nil {
		return nil, err
	}
	res := &InspectResult{Repo: repo, Tick: fixedTick, ChainIntact: true}
	prevChain := genesisChain()
	chainBroken := false
	for i, regEntry := range reg.Generations {
		if budget > 0 && i >= budget {
			res.Codes = addCode(res.Codes, EGenerationBudget)
			res.ChainIntact = false
			break
		}
		gr := GenResult{Generation: regEntry.Generation, Trusted: true}
		raw, err := os.ReadFile(manifestPath(samplesDir, repo, regEntry.Generation))
		if err != nil {
			gr.Codes = addCode(gr.Codes, EManifestMissing)
			gr.Trusted = false
			res.Generations = append(res.Generations, gr)
			chainBroken = true
			continue
		}
		m, code, err := parseManifest(raw)
		if err != nil {
			gr.Codes = addCode(gr.Codes, code)
			gr.Trusted = false
			res.Generations = append(res.Generations, gr)
			chainBroken = true
			continue
		}
		actualDigest := manifestDigest(m)
		gr.ManifestDigest = actualDigest
		gr.Files = len(m.Entries)
		// Wholesale replacement is detected by content-digest divergence
		// from the registry, never by comparing generation numbers.
		if actualDigest != regEntry.ManifestDigest || m.ManifestDigest != regEntry.ManifestDigest {
			gr.Codes = addCode(gr.Codes, EManifestReplaced)
			gr.Trusted = false
		}
		// Chain linkage: declared prev-chain vs actually computed chain.
		if m.PrevChainDigest != prevChain {
			gr.Codes = addCode(gr.Codes, EChainBroken)
			gr.ExpectedPrevChain = prevChain
			gr.ActualPrevChain = m.PrevChainDigest
			gr.Trusted = false
		}
		computed := chainNext(prevChain, actualDigest)
		if computed != regEntry.ChainDigest {
			gr.Codes = addCode(gr.Codes, EChainBroken)
			gr.ExpectedChain = regEntry.ChainDigest
			gr.ActualChain = computed
			gr.Trusted = false
		}
		// Object-level verification: presence, length and content digest.
		for _, e := range m.Entries {
			content, err := os.ReadFile(objectPath(samplesDir, repo, e.SHA256))
			if err != nil {
				gr.Codes = addCode(gr.Codes, EObjectMissing)
				gr.Trusted = false
				continue
			}
			if int64(len(content)) != e.Size || sha256Hex(content) != e.SHA256 {
				gr.Codes = addCode(gr.Codes, EObjectDigest)
				gr.Trusted = false
			}
		}
		if len(gr.Codes) > 0 {
			res.ChainIntact = false
		}
		if chainBroken && gr.Trusted {
			// Chain continuity is broken upstream; this generation cannot
			// be attested even if its own checks pass.
			gr.Trusted = false
			gr.Codes = addCode(gr.Codes, EChainBroken)
			res.ChainIntact = false
		}
		// Only manifest-level failures break the hash chain for descendant
		// generations; object-level tampering taints this generation alone.
		for _, c := range gr.Codes {
			if c != EObjectMissing && c != EObjectDigest {
				chainBroken = true
			}
		}
		prevChain = computed
		res.Generations = append(res.Generations, gr)
	}
	decideRestorePoint(samplesDir, res)
	return res, nil
}

// decideRestorePoint picks the newest trusted generation and derives the
// restore boundary. Replaced generations are never eligible.
func decideRestorePoint(samplesDir string, res *InspectResult) {
	restoreIdx := -1
	for i, gr := range res.Generations {
		if gr.Trusted {
			restoreIdx = i
		}
	}
	if restoreIdx < 0 {
		res.RestoreGen = 0
		res.Codes = addCode(res.Codes, ENoTrustedRestore)
		return
	}
	res.RestoreGen = res.Generations[restoreIdx].Generation
	// Restore file set: union of entries of all trusted generations up to
	// and including the restore point, newest generation winning.
	files := map[string]string{}
	for _, gr := range res.Generations[:restoreIdx+1] {
		if !gr.Trusted {
			continue
		}
		m, code, err := loadManifest(samplesDir, res.Repo, gr.Generation)
		if err != nil || code != "" {
			continue
		}
		for _, e := range m.Entries {
			files[e.Path] = e.SHA256
		}
	}
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	res.RestoreFiles = paths
	for _, gr := range res.Generations {
		if !gr.Trusted {
			res.Excluded = append(res.Excluded, ExcludedGen{Generation: gr.Generation, Reasons: gr.Codes})
		}
	}
}

func loadManifest(samplesDir, repo string, gen int) (*Manifest, string, error) {
	raw, err := os.ReadFile(manifestPath(samplesDir, repo, gen))
	if err != nil {
		return nil, EManifestMissing, fmt.Errorf("manifest missing: %v", err)
	}
	return parseManifest(raw)
}
