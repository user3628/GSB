// Package app wires parsing, scheduling, snapshots and reporting into the
// command-line pipeline and the reproducible anomaly-injection self-checks.
package app

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"injectctl/internal/model"
	"injectctl/internal/parse"
	"injectctl/internal/report"
	"injectctl/internal/samplegen"
	"injectctl/internal/scheduler"
	"injectctl/internal/store"
)

// RunConfig controls one end-to-end pipeline run.
type RunConfig struct {
	WorkDir        string
	MaxConcurrent  int
	MaxQueueDepth  int
	MaxSamples     int
	MaxEncoded     int
	CrashAfterSnap bool // crash-injection hook used by the recovery subprocess
	RecoveryMode   bool
}

// PipelineResult carries everything needed for reporting.
type PipelineResult struct {
	Results     []scheduler.BatchResult
	ParseErrors []report.ParseErrorView
	Chain       store.ChainReport
	ContextVer  string
}

// LoadInputs reads generated (or externally provided) input files.
func LoadInputs(ctxPath, payloadPath string) ([]model.Sample, []report.ParseErrorView, string, error) {
	ctxFile, err := os.Open(ctxPath)
	if err != nil {
		return nil, nil, "", err
	}
	defer ctxFile.Close()
	doc, err := parse.ParseContextFile(ctxFile)
	if err != nil {
		return nil, nil, "", err
	}

	pf, err := os.Open(payloadPath)
	if err != nil {
		return nil, nil, "", err
	}
	defer pf.Close()
	blocks, err := parse.ReadPayloadBlocks(pf)
	if err != nil {
		return nil, nil, "", err
	}

	out := parse.BuildSamples(doc, blocks)
	views := make([]report.ParseErrorView, 0, len(out.Errors))
	for _, e := range out.Errors {
		views = append(views, report.ParseErrorView{
			Code: e.Code, SourceOffset: e.SourceOffset,
			SampleID: e.SampleID, Message: e.Message,
		})
	}
	return out.Samples, views, out.ContextVersion, nil
}

// Run executes the standard three-batch deterministic pipeline.
func Run(cfg RunConfig) (PipelineResult, error) {
	seeds := samplegen.StandardSeeds()
	art, err := samplegen.Write(filepath.Join(cfg.WorkDir, "inputs"), seeds)
	if err != nil {
		return PipelineResult{}, err
	}
	samples, parseViews, ctxVersion, err := LoadInputs(art.ContextPath, art.PayloadPath)
	if err != nil {
		return PipelineResult{}, err
	}

	st, err := store.Open(filepath.Join(cfg.WorkDir, "engine.db"))
	if err != nil {
		return PipelineResult{}, err
	}
	defer st.Close()

	sched := scheduler.New(scheduler.Config{
		MaxConcurrent:      cfg.MaxConcurrent,
		MaxQueueDepth:      cfg.MaxQueueDepth,
		MaxSamplesPerBatch: cfg.MaxSamples,
		MaxEncodedBytes:    cfg.MaxEncoded,
	}, st)
	if cfg.RecoveryMode {
		sched.SetRecoveryMode(true)
	}
	sched.Start()

	jobs := splitBatches(samples)
	for _, job := range jobs {
		if err := sched.Submit(job); err != nil {
			return PipelineResult{}, err
		}
		if cfg.CrashAfterSnap {
			// Hard kill point: after the first snapshot transaction commits.
			sched.WaitIdle()
			os.Exit(9)
		}
	}
	sched.WaitIdle()
	sched.Shutdown()

	results := sched.AllResults()
	chain, _ := st.LoadChain()
	return PipelineResult{
		Results: results, ParseErrors: parseViews,
		Chain: chain, ContextVer: ctxVersion,
	}, nil
}

func splitBatches(samples []model.Sample) []scheduler.Job {
	var jobs []scheduler.Job
	batchSize := 4
	for i := 0; i < len(samples); i += batchSize {
		end := i + batchSize
		if end > len(samples) {
			end = len(samples)
		}
		chunk := append([]model.Sample(nil), samples[i:end]...)
		jobs = append(jobs, scheduler.Job{
			BatchID:        fmt.Sprintf("batch-%02d", i/batchSize+1),
			ContextVersion: samplegen.ContextVersion,
			Samples:        chunk,
		})
	}
	return jobs
}

// WriteBlock renders one length-prefixed payload block for test fixtures.
func WriteBlock(id, surface, raw string) []byte {
	body := append([]byte("id="+id+"\n"), []byte("surface="+surface+"\n")...)
	body = append(body, []byte(raw)...)
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))
	return append(prefix[:], body...)
}
