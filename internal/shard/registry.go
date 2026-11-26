package shard

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"logagg/internal/errs"
	"logagg/internal/model"
)

type Committed struct {
	Package   *model.ShardPackage
	IndexHash string
}

type Snapshot struct {
	Current map[string]*Committed
	All     map[string]map[int64]*Committed
}

type Registry struct {
	snapshot atomic.Pointer[Snapshot]
}

func NewRegistry() *Registry {
	r := &Registry{}
	r.snapshot.Store(&Snapshot{
		Current: map[string]*Committed{},
		All:     map[string]map[int64]*Committed{},
	})
	return r
}

func Validate(pkg *model.ShardPackage) error {
	if pkg == nil {
		return invalid("shard package is nil")
	}
	manifest := pkg.Manifest
	if manifest.ShardID == "" {
		return invalid("shard_id is required")
	}
	if manifest.Version <= 0 {
		return invalid("version must be positive and monotonic, got %d", manifest.Version)
	}
	if manifest.GeneratedAt.IsZero() {
		return invalid("generated_at is required")
	}
	if len(manifest.Columns) == 0 {
		return invalid("at least one column definition is required")
	}
	if len(manifest.Blocks) != len(pkg.Blocks) {
		return invalid("manifest has %d block metas but package has %d blocks", len(manifest.Blocks), len(pkg.Blocks))
	}

	columnWidth := int64(0)
	columnNames := map[string]bool{}
	for _, column := range manifest.Columns {
		if column.Name == "" {
			return invalid("column name is required")
		}
		if columnNames[column.Name] {
			return invalid("duplicate column %q", column.Name)
		}
		columnNames[column.Name] = true
		width := column.Width
		if width <= 0 {
			width = 8
		}
		columnWidth += int64(width)
	}

	seenBlocks := map[int]bool{}
	for index, meta := range manifest.Blocks {
		if meta.BlockNo < 0 {
			return invalid("block number must be non-negative")
		}
		if seenBlocks[meta.BlockNo] {
			return invalid("duplicate block_no %d", meta.BlockNo)
		}
		seenBlocks[meta.BlockNo] = true
		if !meta.Encoding.Valid() {
			return invalid("block %d has invalid encoding %q", meta.BlockNo, meta.Encoding)
		}
		if meta.RowCount < 0 {
			return invalid("block %d has negative row count", meta.BlockNo)
		}
		if columnWidth > 0 && meta.UncompressedBytes < int64(meta.RowCount)*columnWidth {
			return invalid("block %d declared_bytes=%d below row_count*column_width=%d", meta.BlockNo, meta.UncompressedBytes, int64(meta.RowCount)*columnWidth)
		}
		block := pkg.BlockByNo(meta.BlockNo)
		if block == nil {
			return invalid("block %d missing payload", meta.BlockNo)
		}
		if block.Meta != meta {
			return invalid("block %d header does not match manifest at index %d", meta.BlockNo, index)
		}
		if err := block.VerifyDigest(); err != nil {
			return invalid("block %d digest mismatch: %v", meta.BlockNo, err)
		}
	}
	return nil
}

func invalid(format string, args ...any) error {
	return errs.New(errs.EShardInvalid, format, args...)
}

func (r *Registry) Commit(pkg *model.ShardPackage) (*Committed, error) {
	if err := Validate(pkg); err != nil {
		return nil, err
	}
	current := r.snapshot.Load()
	if existing, ok := current.Current[pkg.Manifest.ShardID]; ok {
		if pkg.Manifest.Version <= existing.Package.Manifest.Version {
			return nil, errs.New(errs.EShardInvalid,
				"version not monotonic for shard %s: committed=%d submitted=%d",
				pkg.Manifest.ShardID, existing.Package.Manifest.Version, pkg.Manifest.Version).
				WithDetail("committed_version", existing.Package.Manifest.Version).
				WithDetail("submitted_version", pkg.Manifest.Version)
		}
	}

	next := &Snapshot{
		Current: make(map[string]*Committed, len(current.Current)+1),
		All:     make(map[string]map[int64]*Committed, len(current.All)+1),
	}
	for id, committed := range current.Current {
		next.Current[id] = committed
	}
	for id, versions := range current.All {
		copied := make(map[int64]*Committed, len(versions)+1)
		for version, committed := range versions {
			copied[version] = committed
		}
		next.All[id] = copied
	}

	committed := &Committed{Package: pkg, IndexHash: indexHash(pkg)}
	versions, ok := next.All[pkg.Manifest.ShardID]
	if !ok {
		versions = map[int64]*Committed{}
		next.All[pkg.Manifest.ShardID] = versions
	}
	versions[pkg.Manifest.Version] = committed
	next.Current[pkg.Manifest.ShardID] = committed
	r.snapshot.Store(next)
	return committed, nil
}

func (r *Registry) Current(shardID string) (*Committed, bool) {
	snapshot := r.snapshot.Load()
	committed, ok := snapshot.Current[shardID]
	return committed, ok
}

func (r *Registry) Get(shardID string, version int64) (*Committed, bool) {
	snapshot := r.snapshot.Load()
	versions, ok := snapshot.All[shardID]
	if !ok {
		return nil, false
	}
	committed, ok := versions[version]
	return committed, ok
}

func (r *Registry) Snapshot() *Snapshot {
	return r.snapshot.Load()
}

func indexHash(pkg *model.ShardPackage) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d|%s", pkg.Manifest.ShardID, pkg.Manifest.Version, pkg.Manifest.GeneratedAt.UTC().Format(time.RFC3339Nano))
	metas := append([]model.BlockMeta(nil), pkg.Manifest.Blocks...)
	sort.Slice(metas, func(i, j int) bool { return metas[i].BlockNo < metas[j].BlockNo })
	for _, meta := range metas {
		fmt.Fprintf(h, "|%d:%d:%d:%s:%s", meta.BlockNo, meta.Partition, meta.RowCount, meta.Encoding, meta.Digest)
	}
	return hex.EncodeToString(h.Sum(nil))
}
