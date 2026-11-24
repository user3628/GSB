package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
)

var digestRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func genesisChain() string {
	return sha256Hex([]byte("bire/genesis/v1"))
}

func chainNext(prevChain, manifestDigest string) string {
	return sha256Hex([]byte(prevChain + ":" + manifestDigest))
}

// canonicalManifest is the byte-stable form the manifest digest commits to.
func canonicalManifest(m *Manifest) []byte {
	type canon struct {
		Repo            string  `json:"repo"`
		Generation      int     `json:"generation"`
		PrevChainDigest string  `json:"prev_chain_digest"`
		Entries         []Entry `json:"entries"`
	}
	entries := make([]Entry, len(m.Entries))
	copy(entries, m.Entries)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	b, err := json.Marshal(canon{m.Repo, m.Generation, m.PrevChainDigest, entries})
	if err != nil {
		panic(err)
	}
	return b
}

func manifestDigest(m *Manifest) string {
	return sha256Hex(canonicalManifest(m))
}

func validDigest(s string) bool { return digestRe.MatchString(s) }

// parseManifest decodes raw manifest bytes and classifies failures with
// stable business codes. A JSON syntax error is reported as truncation.
func parseManifest(raw []byte) (*Manifest, string, error) {
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, EManifestTruncated, fmt.Errorf("manifest not decodable (truncated or corrupt): %v", err)
	}
	if m.Generation < 1 {
		return nil, EManifestTruncated, fmt.Errorf("manifest missing generation")
	}
	if !validDigest(m.PrevChainDigest) || !validDigest(m.ManifestDigest) {
		return nil, EDigestFormat, fmt.Errorf("manifest digest fields must be 64 lowercase hex chars")
	}
	for _, e := range m.Entries {
		if !validDigest(e.SHA256) || e.Size < 0 {
			return nil, EDigestFormat, fmt.Errorf("entry %q has invalid digest or size", e.Path)
		}
	}
	return &m, "", nil
}
