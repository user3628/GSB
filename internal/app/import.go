package app

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"csrouter/internal/domain"
	"csrouter/internal/generator"
)

type GenerateResult struct {
	Manifest *domain.Manifest `json:"manifest"`
	Root     string           `json:"root"`
}

func (a *App) Generate(seed int64) (*GenerateResult, error) {
	manifest, _, err := generator.GenerateAll(a.SampleDir, seed)
	if err != nil {
		return nil, err
	}
	return &GenerateResult{Manifest: manifest, Root: a.SampleDir}, nil
}

type ShardValidation struct {
	Path            string `json:"path"`
	DeclaredRows    int    `json:"declared_rows"`
	ActualRows      int    `json:"actual_rows"`
	DeclaredColumns int    `json:"declared_columns"`
	ActualColumns   int    `json:"actual_columns"`
	ColumnCounts    []int  `json:"column_counts,omitempty"`
	SHA256          string `json:"sha256"`
	OK              bool   `json:"ok"`
}

type BundleValidation struct {
	OK     bool                       `json:"ok"`
	Shards map[string]ShardValidation `json:"shards"`
}

func (a *App) ValidateBundle() (*BundleValidation, error) {
	manifest, err := a.LoadManifest()
	if err != nil {
		return nil, err
	}
	result := &BundleValidation{OK: true, Shards: map[string]ShardValidation{}}
	for _, tierName := range manifest.TierOrder {
		tier := manifest.Tiers[tierName]
		for _, declared := range tier.Shards {
			absPath := a.SamplePath(declared.Path)
			tickets, columns, sha, err := ReadTicketFile(absPath)
			validation := ShardValidation{
				Path:            declared.Path,
				DeclaredRows:    declared.DeclaredRows,
				ActualRows:      len(tickets),
				DeclaredColumns: declared.DeclaredCols,
				ActualColumns:   actualColumnCount(columns, declared.DeclaredCols),
				ColumnCounts:    columns,
				SHA256:          sha,
			}
			if err != nil {
				validation.OK = false
				result.OK = false
			} else {
				validation.OK = len(tickets) == declared.DeclaredRows &&
					len(tickets) == declared.Rows &&
					allColumnsMatch(columns, declared.DeclaredCols) &&
					declared.DeclaredCols == declared.Columns &&
					(declared.SHA256 == "" || declared.SHA256 == sha)
				if !validation.OK {
					result.OK = false
				}
			}
			result.Shards[declared.Path] = validation
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func allColumnsMatch(counts []int, expected int) bool {
	for _, count := range counts {
		if count != expected {
			return false
		}
	}
	return true
}

func actualColumnCount(counts []int, fallback int) int {
	if len(counts) == 0 {
		return 0
	}
	return counts[0]
}

func (a *App) SamplePath(rel string) string {
	return joinClean(a.SampleDir, rel)
}

func joinClean(base, rel string) string {
	if strings.HasPrefix(rel, "/") || strings.Contains(rel, "..") {
		return rel
	}
	return base + "/" + rel
}

func ReadTicketFile(path string) ([]domain.Ticket, []int, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	tickets := []domain.Ticket{}
	columnCounts := []int{}
	h := sha256.New()
	lineNo := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lineNo++
		if line == "" {
			continue
		}
		_, _ = h.Write([]byte(line))
		_, _ = h.Write([]byte{'\n'})
		var columns map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &columns); err != nil {
			return tickets, columnCounts, "", fmt.Errorf("line %d: %w", lineNo, err)
		}
		columnCounts = append(columnCounts, len(columns))
		var ticket domain.Ticket
		if err := json.Unmarshal([]byte(line), &ticket); err != nil {
			return tickets, columnCounts, "", fmt.Errorf("line %d: %w", lineNo, err)
		}
		tickets = append(tickets, ticket)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, "", err
	}
	return tickets, columnCounts, hex.EncodeToString(h.Sum(nil)), nil
}

type ImportResult struct {
	Code            string           `json:"code"`
	Manifest        *domain.Manifest `json:"manifest"`
	ImportedTickets int              `json:"imported_tickets"`
	Shards          int              `json:"shards"`
}

func (a *App) GenerateAndImport(seed int64) (*ImportResult, error) {
	generated, err := a.Generate(seed)
	if err != nil {
		return nil, err
	}
	validation, err := a.ValidateBundle()
	if err != nil {
		return nil, err
	}
	if !validation.OK {
		return nil, domain.NewBizError(domain.ErrShardMismatch, "", "", 0, "generated shard declaration validation failed")
	}

	imported := 0
	shards := 0
	for _, tierName := range generated.Manifest.TierOrder {
		tier := generated.Manifest.Tiers[tierName]
		policy, err := a.loadPolicyFile(tier.PolicyPath)
		if err != nil {
			return nil, err
		}
		policy.ChangeCode = domain.CodeSnapshotReplaced
		if err := a.DB.SavePolicy(policy); err != nil {
			return nil, err
		}
		for _, shard := range tier.Shards {
			tickets, _, _, err := ReadTicketFile(a.SamplePath(shard.Path))
			if err != nil {
				return nil, err
			}
			if err := a.DB.UpsertShard(shard, policy.EffectiveVersion()); err != nil {
				return nil, err
			}
			if err := a.DB.UpsertTickets(tierName, shard.Path, tickets); err != nil {
				return nil, err
			}
			imported += len(tickets)
			shards++
		}
	}
	return &ImportResult{Code: domain.CodeSnapshotReplaced, Manifest: generated.Manifest, ImportedTickets: imported, Shards: shards}, nil
}

func (a *App) loadPolicyFile(relPath string) (domain.Policy, error) {
	var policy domain.Policy
	if err := readJSONFile(a.SamplePath(relPath), &policy); err != nil {
		return domain.Policy{}, err
	}
	return policy, nil
}
