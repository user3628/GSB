package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"voiceline/internal/checkpoint"
	"voiceline/internal/diagnostics"
	"voiceline/internal/dispatch"
	"voiceline/internal/dsp"
	"voiceline/internal/httpapi"
	"voiceline/internal/model"
	"voiceline/internal/pipeline"
	"voiceline/internal/policy"
	"voiceline/internal/sample"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = runGenerate(os.Args[2:])
	case "analyze":
		err = runAnalyze(os.Args[2:])
	case "convert":
		err = runConvert(os.Args[2:])
	case "validate":
		err = runValidate(os.Args[2:])
	case "verify":
		err = runVerify(os.Args[2:])
	case "serve":
		err = runServe(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		var business *model.BusinessError
		if errors.As(err, &business) {
			b, _ := json.MarshalIndent(business, "", "  ")
			fmt.Fprintln(os.Stderr, string(b))
			os.Exit(10)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `voiceline commands:
  generate --out samples/local [--segments 4] [--seed 75772049]
  analyze --manifest samples/local/manifest.json [--segment 0] [--out -]
  convert --manifest samples/local/manifest.json [--segment -1] [--out artifacts/output] [--db artifacts/checkpoints/voiceline.db]
  validate --manifest samples/local/manifest.json [--segment -1]
  verify [--out artifacts/verify]
  serve [--addr 127.0.0.1:8091]`)
}

func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", "samples/local", "output directory")
	segments := fs.Int("segments", sample.DefaultSegmentCount, "number of WAV shards")
	seed := fs.Int64("seed", sample.DefaultSeed, "fixed deterministic seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	manifest, err := sample.Generate(sample.Options{OutDir: *out, Seed: *seed, SegmentCount: *segments})
	if err != nil {
		return err
	}
	fmt.Printf("generated %d deterministic segments in %s\n", len(manifest.Segments), *out)
	fmt.Printf("manifest: %s\n", filepath.Join(*out, "manifest.json"))
	return nil
}

func runAnalyze(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "samples/local/manifest.json", "manifest JSON path")
	segmentIndex := fs.Int("segment", 0, "segment index")
	out := fs.String("out", "-", "output JSON path or - for stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	input, err := pipeline.ReadSegmentInput(*manifestPath, *segmentIndex)
	if err != nil {
		return err
	}
	table, err := dsp.Analyze(input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		return err
	}
	if *out == "-" {
		_, err = os.Stdout.Write(append(data, '\n'))
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	return os.WriteFile(*out, append(data, '\n'), 0o644)
}

func runConvert(args []string) error {
	fs := flag.NewFlagSet("convert", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "samples/local/manifest.json", "manifest JSON path")
	segmentIndex := fs.Int("segment", -1, "segment index; -1 converts all")
	outDir := fs.String("out", "artifacts/output", "output directory")
	dbPath := fs.String("db", "artifacts/checkpoints/voiceline.db", "SQLite checkpoint path")
	parallel := fs.Bool("parallel", true, "run conversion groups in parallel")
	recycle := fs.Bool("recycle-quota", false, "recycle unused group quota")
	maxGroups := fs.Int("max-groups", 0, "override max concurrent groups")
	framesPerGroup := fs.Int("frames-per-group", 0, "override frame quota")
	budgetNanos := fs.Int64("budget-nanos", 0, "override per-round estimated time budget")
	blockDelay := fs.Duration("block-delay", 0, "deterministic work delay per block; useful for kill/recovery tests")
	workerID := fs.String("worker-id", "worker-local", "publication worker id")
	if err := fs.Parse(args); err != nil {
		return err
	}

	manifest, _, err := pipeline.LoadManifest(*manifestPath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	store, err := checkpoint.Open(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	strategy := policy.Strategy{LineageID: "cli", Version: 1}
	if *recycle {
		v := true
		strategy.RecycleQuota = &v
	}
	if *maxGroups > 0 {
		v := *maxGroups
		strategy.MaxGroups = &v
	}
	if *framesPerGroup > 0 {
		v := *framesPerGroup
		strategy.FramesPerGroup = &v
	}
	if *budgetNanos > 0 {
		v := *budgetNanos
		strategy.RoundBudgetNanos = &v
	}

	stop := pipeline.NewStopController()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signals
		fmt.Fprintf(os.Stderr, "received %s; stopping after current indivisible block\n", sig)
		stop.RequestStop()
	}()

	indices := []int{*segmentIndex}
	if *segmentIndex < 0 {
		indices = indices[:0]
		for i := range manifest.Segments {
			indices = append(indices, i)
		}
	}
	for _, index := range indices {
		input, err := pipeline.ReadSegmentInput(*manifestPath, index)
		if err != nil {
			return err
		}
		report, err := pipeline.RunSegment(ctx, input, pipeline.RunOptions{
			Store:      store,
			OutputDir:  *outDir,
			Strategy:   strategy,
			Parallel:   *parallel,
			WorkerID:   *workerID,
			BlockDelay: *blockDelay,
			Stop:       stop,
		})
		if errors.Is(err, pipeline.ErrGracefulStop) {
			fmt.Printf("graceful stop: segment=%s published_blocks=%d; progress committed\n", input.Segment.ID, report.PublishedBlocks)
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("converted %s: samples=%d clamps=%d reused_blocks=%d wav=%s\n",
			input.Segment.ID, report.Result.Summary.SampleCount, report.Result.Summary.ClampCount,
			report.ReusedBlocks, report.OutputWAVPath)
	}
	return nil
}

func runValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "samples/local/manifest.json", "manifest JSON path")
	segmentIndex := fs.Int("segment", -1, "segment index; -1 validates all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	manifest, _, err := pipeline.LoadManifest(*manifestPath)
	if err != nil {
		return err
	}
	indices := []int{*segmentIndex}
	if *segmentIndex < 0 {
		indices = indices[:0]
		for i := range manifest.Segments {
			indices = append(indices, i)
		}
	}
	reports := make([]*diagnostics.F0Report, 0, len(indices))
	for _, index := range indices {
		input, err := pipeline.ReadSegmentInput(*manifestPath, index)
		if err != nil {
			return err
		}
		table, err := dsp.Analyze(input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate, input.PCM.Samples, input.Fingerprint)
		if err != nil {
			return err
		}
		report, err := diagnostics.AlignFrameTable(input.Segment, table)
		reports = append(reports, report)
		if err != nil {
			data, _ := json.MarshalIndent(reports, "", "  ")
			fmt.Println(string(data))
			return err
		}
	}
	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8091", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	server := httpapi.NewServer(policy.NewStore(policy.Strategy{LineageID: "local", Version: 1}))
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Printf("voiceline policy API listening on http://%s\n", *addr)
	return httpServer.ListenAndServe()
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	out := fs.String("out", "artifacts/verify", "verification output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	_, err := sample.Generate(sample.Options{OutDir: filepath.Join(*out, "samples")})
	if err != nil {
		return err
	}
	input, err := pipeline.ReadSegmentInput(filepath.Join(*out, "samples", "manifest.json"), 0)
	if err != nil {
		return err
	}
	hugeBudget := time.Hour
	serial, err := pipeline.ConvertSegment(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{
			PolicyVersion: 1,
			Dispatch:      dispatchConfig(false, hugeBudget),
		})
	if err != nil {
		return err
	}
	parallelNoRecycle, err := pipeline.ConvertSegment(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{
			PolicyVersion: 1,
			Parallel:      true,
			Dispatch:      dispatchConfig(false, hugeBudget),
		})
	if err != nil {
		return err
	}
	parallelRecycle, err := pipeline.ConvertSegment(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{
			PolicyVersion: 1,
			Parallel:      true,
			Dispatch:      dispatchConfig(true, hugeBudget),
		})
	if err != nil {
		return err
	}
	for name, candidate := range map[string]*pipeline.ConversionResult{
		"parallel_no_recycle": parallelNoRecycle,
		"parallel_recycle":    parallelRecycle,
	} {
		if string(candidate.WAVBytes) != string(serial.WAVBytes) || string(candidate.SummaryJSON) != string(serial.SummaryJSON) {
			return fmt.Errorf("%s is not byte-identical to serial", name)
		}
	}

	stream, err := pipeline.NewStreamSession(ctx, input.Segment, input.Manifest.SampleRate, input.PCM.SampleRate,
		input.PCM.Samples, input.Fingerprint, pipeline.ConvertOptions{PolicyVersion: 1, Dispatch: dispatchConfig(false, hugeBudget)})
	if err != nil {
		return err
	}
	for i := 0; i < stream.BlockCount(); i++ {
		if _, err := stream.PushBlock(ctx, i); err != nil {
			return err
		}
	}
	streamResult, err := stream.Result()
	if err != nil {
		return err
	}
	if err := pipeline.VerifyConsistency(input.Segment.ID, streamResult, serial); err != nil {
		return err
	}

	report := map[string]any{
		"ok":                true,
		"segment_id":        input.Segment.ID,
		"wav_bytes":         len(serial.WAVBytes),
		"pcm_digest":        serial.Summary.PCMDigest,
		"wav_digest":        serial.Summary.WAVDigest,
		"clamp_count":       serial.Summary.ClampCount,
		"parallel_matches":  []string{"no_recycle", "recycle"},
		"stream_matches":    true,
		"observed_parallel": parallelNoRecycle.Plan.ObservedConcurrency,
		"recycle_transfers": len(parallelRecycle.Plan.Transfers),
	}
	data, _ := json.MarshalIndent(report, "", "  ")
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*out, "verify-report.json"), append(data, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func dispatchConfig(recycle bool, budget time.Duration) (cfg dispatchConfigType) {
	cfg.MaxGroups = 2
	cfg.FramesPerGroup = 6
	cfg.RoundBudgetNanos = int64(budget)
	cfg.RecycleQuota = recycle
	return cfg
}

type dispatchConfigType = dispatch.Config
