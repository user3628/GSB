package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"blockpool"
)

type deterministicRNG struct{ state uint64 }

func newRNG(seed uint64) *deterministicRNG {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return &deterministicRNG{state: seed}
}

func (r *deterministicRNG) next() uint64 {
	// SplitMix64: tiny, fully specified, and stable across Go releases.
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *deterministicRNG) intn(n int) int {
	if n <= 1 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func main() {
	seed := flag.Uint64("seed", 20261124, "fixed deterministic seed")
	out := flag.String("out", filepath.Join("samples", "fixed.json"), "sample output path")
	flag.Parse()

	sample, err := generate(*seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	data, err := json.MarshalIndent(sample, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s: %d namespaces, %d nodes, %d blocks, %d actions, seed=%d\n",
		*out, len(sample.Namespaces), len(sample.Nodes), len(sample.Blocks), len(sample.Actions), *seed)
}

func generate(seed uint64) (blockpool.Sample, error) {
	rng := newRNG(seed)
	namespaces := []string{"backup/windows", "catalog/media", "sync/profile"}
	nodes := []blockpool.SampleNode{
		{Namespace: "backup/windows", Path: "C:/Users/alice/Documents/report.docx@42"},
		{Namespace: "backup/windows", Path: "C:/Users/alice/Pictures/shared.bin@7"},
		{Namespace: "backup/windows", Path: "C:/ProgramData/cache/object-9"},
		{Namespace: "catalog/media", Path: "albums/2026/trip/cover.jpg@3"},
		{Namespace: "catalog/media", Path: "albums/2026/trip/raw-001.cr3"},
		{Namespace: "catalog/media", Path: "shared/chunk-0"},
		{Namespace: "sync/profile", Path: "profiles/alpha/state.db@12"},
		{Namespace: "sync/profile", Path: "profiles/beta/state.db@12"},
		{Namespace: "sync/profile", Path: "profiles/gamma/attachments/shared.bin"},
	}
	sizes := []int{17, 257, 4096, 513, 33, 8192, 128, 1024}
	blocks := make([]blockpool.SampleBlock, 0, len(sizes))
	for i, size := range sizes {
		content := bytes.Repeat([]byte{byte('A' + i)}, size)
		if size == 0 {
			content = []byte{}
		}
		sum := sha256.Sum256(content)
		retention := int64(60000 + i*1000)
		if i == 5 {
			retention = 0 // Eligible for the reclaim portion of the sample.
		}
		blocks = append(blocks, blockpool.SampleBlock{
			Digest:      hex.EncodeToString(sum[:]),
			SizeBytes:   blockpool.NonNegativeInt64(len(content)),
			RetentionMS: blockpool.NonNegativeInt64(retention),
			DataBase64:  base64.StdEncoding.EncodeToString(content),
		})
	}

	sample := blockpool.Sample{Seed: blockpool.NonNegativeInt64(seed), Namespaces: namespaces, Nodes: nodes, Blocks: blocks}
	actions := make([]blockpool.SampleAction, 0, 40)
	requestCounter := 0
	nextRequestID := func() string {
		requestCounter++
		return fmt.Sprintf("sample-%03d", requestCounter)
	}

	type leaseState struct {
		version int64
	}
	refsByNode := make([]map[int]leaseState, len(nodes))
	versions := make(map[string]int64)
	for i := range refsByNode {
		refsByNode[i] = make(map[int]leaseState)
	}

	mount := func(nodeIndex, blockIndex int) {
		key := fmt.Sprintf("%d/%d", nodeIndex, blockIndex)
		if _, exists := refsByNode[nodeIndex][blockIndex]; exists {
			actions = append(actions, blockpool.SampleAction{
				Op: "mount", Namespace: nodes[nodeIndex].Namespace, Path: nodes[nodeIndex].Path,
				Digest: blocks[blockIndex].Digest, LeaseMS: 60000, RequestID: nextRequestID(),
			})
			return
		}
		versions[key]++
		refsByNode[nodeIndex][blockIndex] = leaseState{version: versions[key]}
		actions = append(actions, blockpool.SampleAction{
			Op: "mount", Namespace: nodes[nodeIndex].Namespace, Path: nodes[nodeIndex].Path,
			Digest: blocks[blockIndex].Digest, LeaseMS: 60000, RequestID: nextRequestID(),
		})
	}
	unmount := func(nodeIndex, blockIndex int) {
		delete(refsByNode[nodeIndex], blockIndex)
		actions = append(actions, blockpool.SampleAction{
			Op: "unmount", Namespace: nodes[nodeIndex].Namespace, Path: nodes[nodeIndex].Path,
			Digest: blocks[blockIndex].Digest, RequestID: nextRequestID(),
		})
	}
	renew := func(nodeIndex, blockIndex int) {
		lease, ok := refsByNode[nodeIndex][blockIndex]
		if !ok {
			return
		}
		actions = append(actions, blockpool.SampleAction{
			Op: "renew", Namespace: nodes[nodeIndex].Namespace, Path: nodes[nodeIndex].Path,
			Digest: blocks[blockIndex].Digest, Version: blockpool.NonNegativeInt64(lease.version),
			LeaseMS: 60000, RequestID: nextRequestID(),
		})
	}
	replace := func(nodeIndex int, desired []int) {
		next := make(map[int]leaseState)
		for _, blockIndex := range desired {
			if state, ok := refsByNode[nodeIndex][blockIndex]; ok {
				next[blockIndex] = state
			} else {
				key := fmt.Sprintf("%d/%d", nodeIndex, blockIndex)
				versions[key]++
				next[blockIndex] = leaseState{version: versions[key]}
			}
		}
		refsByNode[nodeIndex] = next
		digests := make([]string, 0, len(desired))
		for _, blockIndex := range desired {
			digests = append(digests, blocks[blockIndex].Digest)
		}
		actions = append(actions, blockpool.SampleAction{
			Op: "replace", Namespace: nodes[nodeIndex].Namespace, Path: nodes[nodeIndex].Path,
			Digests: digests, LeaseMS: 60000, RequestID: nextRequestID(),
		})
	}

	// Deliberately share a few digests across namespaces before the randomized
	// interleaving starts.
	for i := 0; i < len(nodes); i++ {
		mount(i, i%len(blocks))
	}
	mount(8, 0)
	mount(5, 0)
	renew(0, 0)
	replace(3, []int{1, 2, 6})

	for step := 0; step < 26; step++ {
		nodeIndex := rng.intn(len(nodes))
		blockIndex := rng.intn(len(blocks))
		switch rng.intn(10) {
		case 0, 1, 2, 3:
			mount(nodeIndex, blockIndex)
		case 4, 5:
			unmount(nodeIndex, blockIndex)
		case 6, 7:
			renew(nodeIndex, blockIndex)
		default:
			desired := make(map[int]struct{})
			for count := rng.intn(4); count > 0; count-- {
				desired[rng.intn(len(blocks))] = struct{}{}
			}
			indices := make([]int, 0, len(desired))
			for index := range desired {
				indices = append(indices, index)
			}
			for i := 1; i < len(indices); i++ {
				for j := i; j > 0 && indices[j] < indices[j-1]; j-- {
					indices[j], indices[j-1] = indices[j-1], indices[j]
				}
			}
			replace(nodeIndex, indices)
		}
	}

	// Force the zero-retention block to zero references, then reclaim it. This
	// produces a real, non-simulated eligible GC record.
	for nodeIndex := range nodes {
		if _, ok := refsByNode[nodeIndex][5]; ok {
			unmount(nodeIndex, 5)
		}
	}
	actions = append(actions, blockpool.SampleAction{
		Op: "reclaim", Digest: blocks[5].Digest, RequestID: nextRequestID(),
	})
	sample.Actions = actions
	return sample, nil
}
