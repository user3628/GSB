package model

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Document struct {
	DocID  int64  `json:"doc_id"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	Lang   string `json:"lang"`
	Source string `json:"source"`
}

type Corpus struct {
	Docs        []Document `json:"docs"`
	ShardDigest string     `json:"shard_digest"`
	ShardFiles  []string   `json:"shard_files"`
}

type shardHash struct {
	Name string `json:"name"`
	Hash string `json:"sha256"`
}

func LoadShards(dir string) (Corpus, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Corpus{}, fmt.Errorf("read shard directory: %w", err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	if len(names) == 0 {
		return Corpus{}, fmt.Errorf("no .jsonl shards in %s", dir)
	}
	var docs []Document
	var shards []shardHash
	seen := make(map[int64]struct{})
	for _, name := range names {
		path := filepath.Join(dir, name)
		f, err := os.Open(path)
		if err != nil {
			return Corpus{}, fmt.Errorf("open shard %s: %w", name, err)
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
		line := 0
		for scanner.Scan() {
			line++
			text := strings.TrimSpace(scanner.Text())
			if text == "" {
				continue
			}
			var doc Document
			if err := json.Unmarshal([]byte(text), &doc); err != nil {
				f.Close()
				return Corpus{}, fmt.Errorf("parse %s:%d: %w", name, line, err)
			}
			if doc.DocID < 0 {
				f.Close()
				return Corpus{}, fmt.Errorf("negative doc_id %d at %s:%d", doc.DocID, name, line)
			}
			if _, ok := seen[doc.DocID]; ok {
				f.Close()
				return Corpus{}, fmt.Errorf("duplicate doc_id %d at %s:%d", doc.DocID, name, line)
			}
			seen[doc.DocID] = struct{}{}
			docs = append(docs, doc)
		}
		if err := scanner.Err(); err != nil {
			f.Close()
			return Corpus{}, fmt.Errorf("scan shard %s: %w", name, err)
		}
		f.Close()
		raw, err := os.ReadFile(path)
		if err != nil {
			return Corpus{}, fmt.Errorf("hash shard %s: %w", name, err)
		}
		shards = append(shards, shardHash{Name: name, Hash: DigestBytes(raw)})
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].DocID < docs[j].DocID })
	aggregateJSON, _ := json.Marshal(shards)
	shardNames := make([]string, len(shards))
	for i := range shards {
		shardNames[i] = shards[i].Name
	}
	return Corpus{Docs: docs, ShardDigest: DigestBytes(aggregateJSON), ShardFiles: shardNames}, nil
}

func LoadShardsFromFiles(paths []string) (Corpus, error) {
	if len(paths) == 0 {
		return Corpus{}, fmt.Errorf("no shard paths")
	}
	tmp, err := os.MkdirTemp("", "searchlab-shards-*")
	if err != nil {
		return Corpus{}, err
	}
	defer os.RemoveAll(tmp)
	for _, path := range paths {
		in, err := os.Open(path)
		if err != nil {
			return Corpus{}, err
		}
		out, err := os.Create(filepath.Join(tmp, filepath.Base(path)))
		if err != nil {
			in.Close()
			return Corpus{}, err
		}
		_, err = io.Copy(out, in)
		in.Close()
		out.Close()
		if err != nil {
			return Corpus{}, err
		}
	}
	return LoadShards(tmp)
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
