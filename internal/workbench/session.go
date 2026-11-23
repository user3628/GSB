package workbench

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type ParseConfig struct {
	MaxConcurrency int
	MaxEvents      int
}

type sourceDescriptor struct {
	AbsPath string
	RelPath string
	Format  string
}

func DiscoverSources(root string) ([]sourceDescriptor, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, NewBusinessError(CodeSourceOpen, root, 0, err.Error())
	}
	if !info.IsDir() {
		return nil, NewBusinessError(CodeSourceOpen, root, 0, "input path is not a directory")
	}
	discovered := make([]sourceDescriptor, 0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		format, ok := classifySource(path)
		if !ok {
			return nil
		}
		discovered = append(discovered, sourceDescriptor{
			AbsPath: path,
			RelPath: RelPath(root, path),
			Format:  format,
		})
		return nil
	})
	if err != nil {
		return nil, NewBusinessError(CodeSourceOpen, root, 0, err.Error())
	}
	sort.Slice(discovered, func(i, j int) bool { return discovered[i].RelPath < discovered[j].RelPath })
	if len(discovered) == 0 {
		return nil, NewBusinessError(CodeSourceOpen, root, 0, "no supported observation sources found")
	}
	return discovered, nil
}

func classifySource(path string) (string, bool) {
	extension := strings.ToLower(filepath.Ext(path))
	switch extension {
	case ".dnsbin", ".bin":
		return FormatBinary, true
	case ".jsonl", ".json":
		return FormatJSON, true
	case ".dnslog", ".txt":
		return FormatText, true
	default:
		return "", false
	}
}

// ParseWindow processes each source in an independent session. Only MaxConcurrency
// workers read files at once; the remaining jobs wait in the scheduler queue.
func ParseWindow(ctx context.Context, sources []sourceDescriptor, cfg ParseConfig) ([]SessionResult, error) {
	if cfg.MaxConcurrency <= 0 {
		return nil, fmt.Errorf("max concurrency must be positive")
	}
	if cfg.MaxEvents < 0 {
		return nil, fmt.Errorf("max events must be zero (unlimited) or positive")
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no sources to parse")
	}
	jobs := make(chan sourceJob, len(sources))
	results := make(chan SessionResult, len(sources))
	for _, source := range sources {
		jobs <- sourceJob{
			AbsPath:   source.AbsPath,
			RelPath:   source.RelPath,
			Format:    source.Format,
			MaxEvents: cfg.MaxEvents,
		}
	}
	close(jobs)
	workers := cfg.MaxConcurrency
	if workers > len(sources) {
		workers = len(sources)
	}
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					results <- parseSource(job)
				}
			}
		}()
	}
	collected := make([]SessionResult, 0, len(sources))
	for i := 0; i < len(sources); i++ {
		select {
		case <-ctx.Done():
			return collected, ctx.Err()
		case result := <-results:
			collected = append(collected, result)
		}
	}
	sort.SliceStable(collected, func(i, j int) bool { return collected[i].Source < collected[j].Source })
	return collected, nil
}

func parseSource(job sourceJob) SessionResult {
	switch job.Format {
	case FormatBinary:
		return parseBinaryFile(job)
	case FormatJSON:
		return parseJSONFile(job)
	case FormatText:
		return parseTextFile(job)
	default:
		format := job.Format
		if format == "" {
			format = "unknown"
		}
		return SessionResult{
			Source: job.RelPath,
			Format: format,
			Issues: []Issue{{
				Code:   CodeParse,
				Source: job.RelPath,
				Offset: 0,
				Detail: fmt.Sprintf("unsupported source format %q", job.Format),
			}},
			Skipped: 1,
		}
	}
}
