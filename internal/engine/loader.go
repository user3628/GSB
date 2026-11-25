package engine

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"ticketpipeline/internal/model"
)

// LoadShards reads every *.jsonl shard under dir together with its
// *.meta.json sidecar. A mismatch between declared and actual row count is
// reported as E_SHARD_MISMATCH (the shard id is carried in the context).
func LoadShards(dir string) ([]model.Shard, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // deterministic merge order regardless of FS ordering
	shards := make([]model.Shard, 0, len(names))
	for _, name := range names {
		shard, err := loadOne(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		shards = append(shards, shard)
	}
	return shards, nil
}

func loadOne(path string) (model.Shard, error) {
	base := strings.TrimSuffix(filepath.Base(path), ".jsonl")
	metaPath := filepath.Join(filepath.Dir(path), base+".meta.json")
	var meta model.ShardMeta
	mb, err := os.ReadFile(metaPath)
	if err != nil {
		return model.Shard{}, err
	}
	if err := json.Unmarshal(mb, &meta); err != nil {
		return model.Shard{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return model.Shard{}, err
	}
	defer f.Close()
	tickets := []model.RawTicket{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var t model.RawTicket
		if err := json.Unmarshal([]byte(line), &t); err != nil {
			return model.Shard{}, err
		}
		tickets = append(tickets, t)
	}
	if err := scanner.Err(); err != nil {
		return model.Shard{}, err
	}
	if meta.DeclaredRows != len(tickets) {
		return model.Shard{}, model.NewBizError(model.CodeShardMismatch, model.StageNormalize, "",
			"declared shard row count does not match actual rows", map[string]string{
				"shard_id":      meta.ShardID,
				"declared_rows": strconv.Itoa(meta.DeclaredRows),
				"actual_rows":   strconv.Itoa(len(tickets)),
			})
	}
	return model.Shard{Meta: meta, Tickets: tickets}, nil
}

// FlattenShards merges shards into one deterministic ticket slice, ordered by
// shard id then in-shard position. Duplicate ticket ids are rejected so a
// later merge cannot silently overwrite an earlier fact.
func FlattenShards(shards []model.Shard) ([]model.RawTicket, error) {
	sorted := append([]model.Shard(nil), shards...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Meta.ShardID < sorted[j].Meta.ShardID })
	out := make([]model.RawTicket, 0)
	seen := map[string]bool{}
	for _, sh := range sorted {
		for _, t := range sh.Tickets {
			if seen[t.TicketID] {
				return nil, model.NewBizError(model.CodeUnknown, model.StageNormalize, t.TicketID,
					"duplicate ticket id across shards", map[string]string{"shard_id": sh.Meta.ShardID})
			}
			seen[t.TicketID] = true
			out = append(out, t)
		}
	}
	return out, nil
}

// LoadPolicy reads the highest-version policy snapshot in dir.
func LoadPolicy(dir string) (model.Policy, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return model.Policy{}, err
	}
	best := ""
	bestVersion := -1
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "policy.v") || !strings.HasSuffix(name, ".json") {
			continue
		}
		v, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "policy.v"), ".json"))
		if err != nil {
			continue
		}
		if v > bestVersion {
			bestVersion, best = v, name
		}
	}
	if best == "" {
		return model.Policy{}, os.ErrNotExist
	}
	return LoadPolicyFile(filepath.Join(dir, best))
}

// LoadPolicyFile reads one policy snapshot.
func LoadPolicyFile(path string) (model.Policy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return model.Policy{}, err
	}
	var p model.Policy
	if err := json.Unmarshal(b, &p); err != nil {
		return model.Policy{}, err
	}
	return p, nil
}
