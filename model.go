package main

// Stable business codes surfaced in trajectories, snapshots and reports.
const (
	EManifestTruncated = "E_MANIFEST_TRUNCATED"
	EDigestFormat      = "E_DIGEST_FORMAT"
	EObjectMissing     = "E_OBJECT_MISSING"
	EObjectDigest      = "E_OBJECT_DIGEST"
	EChainBroken       = "E_CHAIN_BROKEN"
	EManifestReplaced  = "E_MANIFEST_REPLACED"
	EManifestMissing   = "E_MANIFEST_MISSING"
	ENoTrustedRestore  = "E_NO_TRUSTED_RESTORE"
	ESnapshotVersion   = "E_SNAPSHOT_VERSION"
	ESnapshotDigest    = "E_SNAPSHOT_DIGEST"
	EGenerationBudget  = "E_GENERATION_BUDGET"
)

// Entry is one file recorded in a generation manifest.
type Entry struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest describes one backup generation.
type Manifest struct {
	Repo            string  `json:"repo"`
	Generation      int     `json:"generation"`
	PrevChainDigest string  `json:"prev_chain_digest"`
	Entries         []Entry `json:"entries"`
	ManifestDigest  string  `json:"manifest_digest"`
}

// RegistryEntry is the tamper-evident registration of one generation.
type RegistryEntry struct {
	Generation     int    `json:"generation"`
	ManifestDigest string `json:"manifest_digest"`
	ChainDigest    string `json:"chain_digest"`
}

// Registry is the repo-level index of registered generations.
type Registry struct {
	Repo        string          `json:"repo"`
	Generations []RegistryEntry `json:"generations"`
}

// GenResult is the inspection verdict for one generation.
type GenResult struct {
	Generation        int      `json:"generation"`
	Trusted           bool     `json:"trusted"`
	Codes             []string `json:"codes"`
	ManifestDigest    string   `json:"manifest_digest,omitempty"`
	ExpectedChain     string   `json:"expected_chain_digest,omitempty"`
	ActualChain       string   `json:"actual_chain_digest,omitempty"`
	ExpectedPrevChain string   `json:"expected_prev_chain_digest,omitempty"`
	ActualPrevChain   string   `json:"actual_prev_chain_digest,omitempty"`
	Files             int      `json:"files"`
}

// ExcludedGen records why a generation is outside the restore boundary.
type ExcludedGen struct {
	Generation int      `json:"generation"`
	Reasons    []string `json:"reasons"`
}

// InspectResult is the full outcome of one inspection round for a repo.
type InspectResult struct {
	Repo         string        `json:"repo"`
	Tick         string        `json:"tick"`
	Generations  []GenResult   `json:"generations"`
	ChainIntact  bool          `json:"chain_intact"`
	RestoreGen   int           `json:"restore_generation"`
	RestoreFiles []string      `json:"restore_files,omitempty"`
	Excluded     []ExcludedGen `json:"excluded,omitempty"`
	Codes        []string      `json:"codes"`
}

// Snapshot is one committed, versioned repo state.
type Snapshot struct {
	Repo    string `json:"repo"`
	Version int    `json:"version"`
	Payload string `json:"payload"`
	Digest  string `json:"digest"`
}
