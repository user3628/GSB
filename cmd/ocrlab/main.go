package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"ocrlab/internal/apperr"
	"ocrlab/internal/canonical"
	"ocrlab/internal/httpapi"
	"ocrlab/internal/pipeline"
	"ocrlab/internal/querysvc"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = commandInit(os.Args[2:])
	case "import":
		err = commandImport(os.Args[2:])
	case "run":
		err = commandRun(os.Args[2:])
	case "query":
		err = commandQuery(os.Args[2:])
	case "export":
		err = commandExport(os.Args[2:])
	case "verify":
		err = commandVerify(os.Args[2:])
	case "serve":
		err = commandServe(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		writeError(err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `ocrlab - deterministic OCR detection/recognition diagnostic workbench

Commands:
  init    [--workspace DIR]                         generate samples, config, SQLite and import
  import  [--workspace DIR]                         re-import the generated manifest
  run     [--workspace DIR] [--groups a,b]          execute four-stage pipeline
          [--rebuild] [--stop-after-units N]
  query   [--workspace DIR] --kind KIND             query boxes|recognition|image|diagnostics
          [--image ID] [--group NAME] [--budget-ms N] [--force-recompute]
  export  [--workspace DIR] [--out FILE]            export deterministic diagnostic report
  verify  [--workspace DIR]                         compare incremental output with clean full build
  serve   [--workspace DIR] [--addr 127.0.0.1:18787]

No network or container runtime is used.`)
}

func commandInit(args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	workspaceRoot := flags.String("workspace", ".", "workspace directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	p, err := pipeline.Open(ctx, *workspaceRoot)
	if err != nil {
		return err
	}
	defer p.Close()
	manifest := p.Manifest()
	return writeOutput(struct {
		Status         string `json:"status"`
		ManifestDigest string `json:"manifest_digest"`
		ConfigVersion  string `json:"config_version"`
		Groups         int    `json:"groups"`
		Images         int    `json:"images"`
	}{Status: "ready", ManifestDigest: p.ManifestDigest(), ConfigVersion: p.Config().Version(), Groups: len(manifest.Groups), Images: len(manifest.Images)})
}

func commandImport(args []string) error {
	flags := flag.NewFlagSet("import", flag.ContinueOnError)
	workspaceRoot := flags.String("workspace", ".", "workspace directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	p, err := pipeline.Open(ctx, *workspaceRoot)
	if err != nil {
		return err
	}
	defer p.Close()
	summary, err := p.Reimport(ctx)
	if err != nil {
		return err
	}
	return writeOutput(summary)
}

func commandRun(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	workspaceRoot := flags.String("workspace", ".", "workspace directory")
	groups := flags.String("groups", "", "comma-separated groups (default all)")
	rebuild := flags.Bool("rebuild", false, "clear all checkpoints before running")
	stopAfter := flags.Int("stop-after-units", 0, "gracefully stop after N newly published stage units (test hook)")
	pretty := flags.Bool("pretty", false, "indent JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	p, err := pipeline.Open(ctx, *workspaceRoot)
	if err != nil {
		return err
	}
	defer p.Close()
	summary, err := p.Run(ctx, pipeline.RunOptions{
		Groups: splitGroups(*groups), Rebuild: *rebuild, StopAfterUnits: *stopAfter,
	})
	if err != nil {
		return err
	}
	if *pretty {
		return writePretty(summary)
	}
	return writeOutput(summary)
}

func commandQuery(args []string) error {
	flags := flag.NewFlagSet("query", flag.ContinueOnError)
	workspaceRoot := flags.String("workspace", ".", "workspace directory")
	kind := flags.String("kind", "", "boxes|recognition|image|diagnostics")
	imageID := flags.String("image", "", "image_id")
	group := flags.String("group", "", "group name for diagnostics")
	budget := flags.Int("budget-ms", -1, "latency budget override in milliseconds")
	force := flags.Bool("force-recompute", false, "bypass a warm query cache")
	pretty := flags.Bool("pretty", false, "indent JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *kind == "" {
		return apperr.New(apperr.CodeInvalidArgument, "--kind is required")
	}
	p, err := pipeline.Open(context.Background(), *workspaceRoot)
	if err != nil {
		return err
	}
	defer p.Close()
	service := querysvc.New(p)
	var budgetPtr *int
	if *budget >= 0 {
		budgetPtr = budget
	}
	response, err := service.Execute(context.Background(), querysvc.Request{
		Kind: *kind, ImageID: *imageID, Group: *group, BudgetMS: budgetPtr, ForceRecompute: *force,
	})
	if err != nil {
		return err
	}
	if *pretty {
		return writePretty(response)
	}
	return writeOutput(response)
}

func commandExport(args []string) error {
	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	workspaceRoot := flags.String("workspace", ".", "workspace directory")
	out := flags.String("out", "", "report path (default reports/report.json)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	p, err := pipeline.Open(context.Background(), *workspaceRoot)
	if err != nil {
		return err
	}
	defer p.Close()
	report, digest, err := p.Export(context.Background(), *out)
	if err != nil {
		return err
	}
	return writeOutput(struct {
		ReportDigest string `json:"report_sha256"`
		Groups       int    `json:"groups"`
		Images       int    `json:"images"`
		GroupsDetail any    `json:"groups_detail"`
	}{ReportDigest: digest, Groups: len(report.Groups), Images: len(report.Images), GroupsDetail: report.Groups})
}

func commandVerify(args []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	workspaceRoot := flags.String("workspace", ".", "workspace directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	p, err := pipeline.Open(context.Background(), *workspaceRoot)
	if err != nil {
		return err
	}
	defer p.Close()
	summary, err := p.VerifyIncremental(context.Background())
	if err != nil {
		return err
	}
	return writeOutput(summary)
}

func commandServe(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	workspaceRoot := flags.String("workspace", ".", "workspace directory")
	addr := flags.String("addr", "127.0.0.1:18787", "loopback listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(*addr)
	if err != nil {
		return apperr.Wrap(apperr.CodeInvalidArgument, "invalid listen address", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return apperr.New(apperr.CodeInvalidArgument, "HTTP API must bind to a loopback address")
	}
	if port != "0" {
		if value, err := strconv.Atoi(port); err != nil || value < 1 || value > 65535 {
			return apperr.New(apperr.CodeInvalidArgument, "invalid TCP port")
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	p, err := pipeline.Open(ctx, *workspaceRoot)
	if err != nil {
		return err
	}
	defer p.Close()
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return apperr.Wrap(apperr.CodeStorage, "listen on loopback", err)
	}
	server := &http.Server{Handler: httpapi.New(p), ReadHeaderTimeout: 5 * time.Second}
	fmt.Fprintf(os.Stderr, "ocrlab listening on http://%s\n", listener.Addr().String())
	errorChannel := make(chan error, 1)
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && serveErr != http.ErrServerClosed {
			errorChannel <- serveErr
			return
		}
		errorChannel <- nil
	}()
	select {
	case err := <-errorChannel:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func splitGroups(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func writeOutput(value any) error {
	body, err := canonical.Marshal(value)
	if err != nil {
		return err
	}
	fmt.Println(string(body))
	return nil
}

func writePretty(value any) error {
	body, err := canonical.Marshal(value)
	if err != nil {
		return err
	}
	var pretty any
	if err := jsonUnmarshal(body, &pretty); err != nil {
		return err
	}
	indented, err := jsonMarshalIndent(pretty)
	if err != nil {
		return err
	}
	fmt.Println(string(indented))
	return nil
}

func writeError(err error) {
	_ = writeOutput(struct {
		Error *apperr.Error `json:"error"`
	}{Error: apperr.From(err)})
}

// Kept as tiny wrappers so command output formatting does not affect the
// canonical bytes used by result digests.
func jsonUnmarshal(body []byte, target any) error { return json.Unmarshal(body, target) }

func jsonMarshalIndent(value any) ([]byte, error) { return json.MarshalIndent(value, "", "  ") }
