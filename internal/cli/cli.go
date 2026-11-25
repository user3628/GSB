package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"tm-sync/internal/api"
	"tm-sync/internal/service"
	"tm-sync/internal/store"
	"tm-sync/internal/tm"
)

func Main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "generate":
		err = cmdGenerate(os.Args[2:])
	case "serve":
		err = cmdServe(ctx, os.Args[2:])
	case "stop":
		err = cmdStop(os.Args[2:])
	case "import-source":
		err = cmdImportSource(ctx, os.Args[2:])
	case "import-increment":
		err = cmdImportIncrement(ctx, os.Args[2:])
	case "sync":
		err = cmdSync(ctx, os.Args[2:])
	case "stage":
		err = cmdStage(ctx, os.Args[2:])
	case "recover":
		err = cmdRecover(ctx, os.Args[2:])
	case "status":
		err = cmdStatus(ctx, os.Args[2:])
	case "suggest":
		err = cmdSuggest(ctx, os.Args[2:])
	case "export-units", "export":
		err = cmdExport(ctx, os.Args[2:])
	case "publish":
		err = cmdPublish(ctx, os.Args[2:])
	case "version-record":
		err = cmdVersionRecord(ctx, os.Args[2:])
	case "demo":
		err = cmdDemo(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		printCLIError(err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `tm-sync commands:
  generate --dir demo-data
  serve --db tm-sync.db --addr 127.0.0.1:18080
  stop --addr http://127.0.0.1:18080
  import-source --db tm-sync.db --version 1 source-001.jsonl [source-002.jsonl ...]
  import-increment --db tm-sync.db increment-001.jsonl
  sync --db tm-sync.db [--full] [--segment ID] [--crash-after N]
  stage --db tm-sync.db --segment seg-001 --stage alignment [--from-stage segmentation]
  recover --db tm-sync.db [--crash-after N]
  status --db tm-sync.db
  suggest --db tm-sync.db --text 提交 [--invalid]
  export-units --db tm-sync.db --out export.json
  publish --db tm-sync.db --segment seg-001 --seq 1 --kind unit --entity u_...
  version-record --db tm-sync.db --stream source --version 1 --digest sha256
  demo --dir demo-data --db demo-data/tm-sync.db`)
}

func printCLIError(err error) {
	var biz *tm.BizError
	if errors.As(err, &biz) {
		b, _ := tm.CanonicalJSON(map[string]any{"ok": false, "error": biz})
		fmt.Fprint(os.Stderr, string(b))
		return
	}
	fmt.Fprintf(os.Stderr, "{\"ok\":false,\"error\":{\"code\":\"E_CLI\",\"message\":%q}}\n", err.Error())
}

func openService(db string, crashAfter int64) (*store.Store, *service.Service, error) {
	st, err := store.Open(db)
	if err != nil {
		return nil, nil, err
	}
	cfg := tm.DefaultAlignConfig()
	if raw := os.Getenv("TM_ALIGN_THRESHOLD"); raw != "" {
		if threshold, err := strconv.ParseFloat(raw, 64); err == nil {
			cfg.Threshold = threshold
		}
	}
	svc := service.New(st, cfg)
	svc.CrashAfterCheckpoint = crashAfter
	return st, svc, nil
}

func printJSON(v any) error {
	b, err := tm.CanonicalJSON(v)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(b)
	return err
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	dir := fs.String("dir", "demo-data", "output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dataset, err := tm.GenerateDataset(*dir)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "dataset": dataset})
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "SQLite database path")
	addr := fs.String("addr", "127.0.0.1:18080", "listen address")
	crashAfter := fs.Int64("crash-after", 0, "test hook: SIGKILL process after N committed checkpoints")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, *crashAfter)
	if err != nil {
		return err
	}
	defer st.Close()
	if _, err := svc.Recover(ctx); err != nil {
		if biz, ok := tm.AsBizError(err); !ok || biz.Code != "E_NO_SOURCE_IMPORTED" {
			return err
		}
	}
	server := api.NewServer(svc)
	httpServer := &http.Server{Addr: *addr, Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}()
	fmt.Printf("tm-sync listening on http://%s (db=%s)\n", *addr, *db)

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		fmt.Fprintf(os.Stderr, "received %s, stopping gracefully\n", sig)
	case <-server.StopChannel():
		fmt.Fprintln(os.Stderr, "stop requested via HTTP")
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

func cmdStop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:18080", "server base URL or host:port")
	if err := fs.Parse(args); err != nil {
		return err
	}
	url := *addr
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	url = strings.TrimRight(url, "/") + "/api/v1/admin/stop"
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stop failed: HTTP %d", resp.StatusCode)
	}
	return printJSON(map[string]any{"ok": true, "stopping": true})
}

func cmdImportSource(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import-source", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	version := fs.Int64("version", 1, "source version")
	dir := fs.String("dir", "", "dataset directory containing sources/")
	if err := fs.Parse(args); err != nil {
		return err
	}
	data, err := readSourceInputs(*dir, fs.Args())
	if err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := svc.ImportSources(ctx, data, *version)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "result": out})
}

func readSourceInputs(dir string, paths []string) ([]byte, error) {
	var files []string
	if dir != "" {
		matches, err := filepath.Glob(filepath.Join(dir, "sources", "source-*.jsonl"))
		if err != nil {
			return nil, err
		}
		sort.Strings(matches)
		files = append(files, matches...)
	}
	files = append(files, paths...)
	if len(files) == 0 {
		return nil, fmt.Errorf("no source JSONL files provided")
	}
	var data []byte
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		data = append(data, b...)
		if len(b) == 0 || b[len(b)-1] != '\n' {
			data = append(data, '\n')
		}
	}
	return data, nil
}

func cmdImportIncrement(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import-increment", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: import-increment --db DB increment.jsonl")
	}
	data, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := svc.ApplyIncrement(ctx, data)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "result": out})
}

func cmdSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	full := fs.Bool("full", false, "force full stage pass while retaining transactional checkpoints")
	segment := fs.String("segment", "", "sync one segment")
	crashAfter := fs.Int64("crash-after", 0, "test hook: SIGKILL after N committed checkpoints")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, *crashAfter)
	if err != nil {
		return err
	}
	defer st.Close()
	mode := "incremental"
	if *full {
		mode = "full"
	}
	report, err := svc.Sync(ctx, service.SyncRequest{Mode: mode, SegmentID: *segment, Verify: true})
	if err != nil {
		return err
	}
	if *segment != "" && len(report.FailedSegments) > 0 {
		return report.FailedSegments[0].Error
	}
	return printJSON(map[string]any{"ok": len(report.FailedSegments) == 0, "report": report})
}

func cmdRecover(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	crashAfter := fs.Int64("crash-after", 0, "test hook: SIGKILL after N committed checkpoints")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, *crashAfter)
	if err != nil {
		return err
	}
	defer st.Close()
	report, err := svc.Recover(ctx)
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "report": report})
}

func cmdStage(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("stage", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	segment := fs.String("segment", "", "segment id")
	stage := fs.String("stage", "", "segmentation|alignment|merge|invalidation")
	from := fs.String("from-stage", "", "optional expected current stage for jump validation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := svc.RunStage(ctx, service.StageRequest{SegmentID: *segment, Stage: *stage, FromStage: *from})
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "result": out})
}

func cmdStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := svc.Status(ctx)
	if err != nil {
		return err
	}
	return printJSON(out)
}

func cmdSuggest(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("suggest", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	text := fs.String("text", "", "source text query")
	invalid := fs.Bool("invalid", true, "include invalid suggestions and reason chains")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := svc.SearchSuggestions(ctx, *text, *invalid)
	if err != nil {
		return err
	}
	return printJSON(out)
}

func cmdExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("export-units", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	out := fs.String("out", "", "output JSON path; defaults to stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	_, body, err := svc.ExportBundle(ctx)
	if err != nil {
		return err
	}
	if *out == "" {
		_, err = os.Stdout.Write(body)
		return err
	}
	return os.WriteFile(*out, body, 0o644)
}

func cmdPublish(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	segment := fs.String("segment", "", "segment publication lane")
	seq := fs.Int64("seq", 0, "segment-local sequence")
	kind := fs.String("kind", "unit", "unit or suggestion")
	entity := fs.String("entity", "", "entity id")
	checkpointSeq := fs.Int64("checkpoint-seq", 0, "required checkpoint monotonic seq")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := svc.Publish(ctx, service.PublishRequest{
		SegmentID: *segment, Seq: *seq, Kind: *kind, EntityID: *entity, CheckpointSeq: *checkpointSeq,
	})
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "result": out})
}

func cmdVersionRecord(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("version-record", flag.ContinueOnError)
	db := fs.String("db", "tm-sync.db", "database path")
	stream := fs.String("stream", "generic", "version stream")
	version := fs.Int64("version", 0, "record version")
	digest := fs.String("digest", "", "content digest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()
	out, err := svc.RecordVersion(ctx, service.VersionRecordRequest{Stream: *stream, Version: *version, ContentDigest: *digest})
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"ok": true, "result": out})
}

func cmdDemo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	dir := fs.String("dir", "demo-data", "deterministic data directory")
	db := fs.String("db", "demo-data/tm-sync.db", "demo database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dataset, err := tm.GenerateDataset(*dir)
	if err != nil {
		return err
	}
	resetDemoDB(*db)
	st, svc, err := openService(*db, 0)
	if err != nil {
		return err
	}
	defer st.Close()

	sourceData, err := readSourceInputs(*dir, nil)
	if err != nil {
		return err
	}
	imported, err := svc.ImportSources(ctx, sourceData, 1)
	if err != nil {
		return err
	}
	sync1, err := svc.Sync(ctx, service.SyncRequest{Mode: "full"})
	if err != nil {
		return err
	}
	export1, err := writePair(ctx, svc, *dir, "v1")
	if err != nil {
		return err
	}

	type stepReport struct {
		File            string                   `json:"file"`
		Import          *service.IncrementResult `json:"import"`
		Sync            *service.SyncReport      `json:"sync"`
		IncrementalFile string                   `json:"incremental_file"`
		FullFile        string                   `json:"full_file"`
		ByteIdentical   bool                     `json:"byte_identical"`
	}
	steps := []stepReport{}
	for i, incFile := range dataset.IncrementFiles {
		raw, err := os.ReadFile(incFile)
		if err != nil {
			return err
		}
		inc, err := svc.ApplyIncrement(ctx, raw)
		if err != nil {
			return err
		}
		syncReport, err := svc.Sync(ctx, service.SyncRequest{Mode: "incremental"})
		if err != nil {
			return err
		}
		name := fmt.Sprintf("v%d", i+2)
		incFileOut, fullFileOut, identical, err := writeComparison(ctx, svc, *dir, name)
		if err != nil {
			return err
		}
		steps = append(steps, stepReport{
			File: incFile, Import: inc, Sync: syncReport,
			IncrementalFile: incFileOut, FullFile: fullFileOut, ByteIdentical: identical,
		})
	}

	report := map[string]any{
		"ok": true, "seed": tm.GeneratorSeed, "dir": *dir, "db": *db,
		"source_import": imported,
		"initial_sync":  sync1,
		"export_v1":     export1,
		"steps":         steps,
		"http_examples": []string{
			"tm-sync serve --db " + *db + " --addr 127.0.0.1:18080",
			"curl 'http://127.0.0.1:18080/api/v1/suggestions?q=%E6%8F%90%E4%BA%A4&invalid=1'",
			"curl 'http://127.0.0.1:18080/api/v1/units/export?format=jsonl'",
			"curl -X POST 'http://127.0.0.1:18080/api/v1/recover'",
		},
	}
	b, _ := tm.CanonicalJSON(report)
	if err := os.WriteFile(filepath.Join(*dir, "demo-report.json"), b, 0o644); err != nil {
		return err
	}
	_, err = os.Stdout.Write(b)
	return err
}

func writePair(ctx context.Context, svc *service.Service, dir, name string) (string, error) {
	_, actual, err := svc.ExportBundle(ctx)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "export-"+name+".json")
	return path, os.WriteFile(path, actual, 0o644)
}

func writeComparison(ctx context.Context, svc *service.Service, dir, name string) (string, string, bool, error) {
	_, actual, err := svc.ExportBundle(ctx)
	if err != nil {
		return "", "", false, err
	}
	_, full, err := svc.FullRebuildExport(ctx)
	if err != nil {
		return "", "", false, err
	}
	incPath := filepath.Join(dir, "export-"+name+"-incremental.json")
	fullPath := filepath.Join(dir, "export-"+name+"-full.json")
	if err := os.WriteFile(incPath, actual, 0o644); err != nil {
		return "", "", false, err
	}
	if err := os.WriteFile(fullPath, full, 0o644); err != nil {
		return "", "", false, err
	}
	return incPath, fullPath, string(actual) == string(full), nil
}

func resetDemoDB(db string) {
	for _, suffix := range []string{"", "-wal", "-shm", ".lock"} {
		_ = os.Remove(db + suffix)
	}
}

var _ = strings.TrimSpace
