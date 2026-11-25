// Command matbench is the deterministic texture/material diagnostic workbench
// CLI and local HTTP server.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"matbench/internal/demo"
	"matbench/internal/errorsx"
	"matbench/internal/pipeline"
	"matbench/internal/recipe"
	"matbench/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "demo":
		err = cmdDemo(args)
	case "run":
		err = cmdRun(args, false)
	case "resume":
		err = cmdRun(args, true)
	case "incr":
		err = cmdIncr(args)
	case "status":
		err = cmdStatus(args)
	case "material":
		err = cmdMaterial(args)
	case "invalidations":
		err = cmdInvalidations(args)
	case "report":
		err = cmdReport(args)
	case "stop":
		err = cmdStop(args)
	case "serve":
		err = cmdServe(args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		if be := errorsx.CodeOf(err); be != "" {
			fmt.Fprintf(os.Stderr, "code: %s\n", be)
		}
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `matbench - deterministic texture/material diagnostic workbench

usage: matbench <command> [flags]
  demo     -dir WS          generate fixed-seed recipes.json and reference cards
  run      -dir WS [-workers N] [-only ID,...] [-force]
  resume   -dir WS          clear stop sentinel and resume from checkpoints
  incr     -dir WS -id MAT -field name=value[,name=value ...]
  status   -dir WS
  material -dir WS -id MAT [-stage control|noise|palette|report]
  invalidations -dir WS -id MAT
  report   -dir WS
  stop     -dir WS
  serve    -dir WS -addr 127.0.0.1:8080
`)
}

func dirFlag(fs *flag.FlagSet) *string { return fs.String("dir", "workspace", "workspace directory") }

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	dir := dirFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	book, err := demo.Setup(*dir)
	if err != nil {
		return err
	}
	fmt.Printf("workspace ready at %s: %d materials, %d cards, seed=%d\n",
		*dir, len(book.Recipes), len(book.Cards), book.GeneratorSeed)
	return nil
}

func openPipeline(dir string) (*pipeline.Pipeline, func() error, error) {
	srv, err := server.New(dir)
	if err != nil {
		return nil, nil, err
	}
	st := srv.Store()
	p := &pipeline.Pipeline{Dir: dir, Store: st}
	return p, srv.Close, nil
}

func cmdRun(args []string, resume bool) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	dir := dirFlag(fs)
	workers := fs.Int("workers", 1, "octave synthesis workers (1=single thread)")
	only := fs.String("only", "", "comma-separated material ids")
	force := fs.Bool("force", false, "ignore checkpoints and rebuild everything")
	if err := fs.Parse(args); err != nil {
		return err
	}
	stop := demo.FileStop{Path: filepath.Join(*dir, demo.StopFile)}
	if resume {
		if err := stop.ClearStop(); err != nil {
			return err
		}
	}
	p, closeFn, err := openPipeline(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	book, err := demo.Load(*dir)
	if err != nil {
		return err
	}
	opts := pipeline.RunOptions{Workers: *workers, ForceAll: *force, Stop: stop}
	if *only != "" {
		opts.OnlyIDs = strings.Split(*only, ",")
	}
	sum, err := p.RunFull(book, opts)
	if err != nil {
		return err
	}
	return printJSON(sum)
}

func cmdIncr(args []string) error {
	fs := flag.NewFlagSet("incr", flag.ContinueOnError)
	dir := dirFlag(fs)
	id := fs.String("id", "", "material id")
	field := fs.String("field", "", "name=value pairs, comma separated")
	workers := fs.Int("workers", 1, "workers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *field == "" {
		return fmt.Errorf("incr requires -id and -field")
	}
	patch, err := buildPatch(*id, *field)
	if err != nil {
		return err
	}
	p, closeFn, err := openPipeline(*dir)
	if err != nil {
		return err
	}
	defer closeFn()
	book, err := demo.Load(*dir)
	if err != nil {
		return err
	}
	sum, err := p.RunIncremental(book, []recipe.Patch{patch},
		pipeline.RunOptions{Workers: *workers})
	if err != nil {
		return err
	}
	return printJSON(sum)
}

func buildPatch(id, raw string) (recipe.Patch, error) {
	p := recipe.Patch{MaterialID: id}
	for _, pair := range strings.Split(raw, ",") {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			return p, fmt.Errorf("invalid field pair %q", pair)
		}
		name, value := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		switch name {
		case "description":
			s := value
			p.Description = &s
		case "noise_octaves":
			n, err := strconv.Atoi(value)
			if err != nil {
				return p, err
			}
			p.NoiseOctaves = &n
		case "noise_frequency":
			f, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return p, err
			}
			p.NoiseFrequency = &f
		case "target_resolution":
			n, err := strconv.Atoi(value)
			if err != nil {
				return p, err
			}
			p.TargetResolution = &n
		case "card_name":
			s := value
			p.CardName = &s
		default:
			return p, fmt.Errorf("unsupported patch field %q", name)
		}
	}
	return p, nil
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dir := dirFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	srv, err := server.New(*dir)
	if err != nil {
		return err
	}
	defer srv.Close()
	rows, err := srv.Store().AllActive()
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Println("no checkpoints yet")
		return nil
	}
	for _, c := range rows {
		fmt.Printf("%-10s %-8s seq=%d fp=%s... input=%s...\n",
			c.MaterialID, c.Stage, c.Seq, c.ArtifactFP[:12], c.RecipeFP[:12])
	}
	return nil
}

func cmdMaterial(args []string) error {
	fs := flag.NewFlagSet("material", flag.ContinueOnError)
	dir := dirFlag(fs)
	id := fs.String("id", "", "material id")
	stage := fs.String("stage", pipeline.StageReport, "stage name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	srv, err := server.New(*dir)
	if err != nil {
		return err
	}
	defer srv.Close()
	cp, err := srv.Store().Active(*id, *stage)
	if err != nil {
		return err
	}
	if cp == nil {
		return fmt.Errorf("no active checkpoint for %s/%s", *id, *stage)
	}
	var pretty any
	if err := json.Unmarshal(cp.Payload, &pretty); err != nil {
		return err
	}
	return printJSON(pretty)
}

func cmdInvalidations(args []string) error {
	fs := flag.NewFlagSet("invalidations", flag.ContinueOnError)
	dir := dirFlag(fs)
	id := fs.String("id", "", "material id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	srv, err := server.New(*dir)
	if err != nil {
		return err
	}
	defer srv.Close()
	rows, err := srv.Store().Invalidations(*id)
	if err != nil {
		return err
	}
	return printJSON(rows)
}

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dir := dirFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	srv, err := server.New(*dir)
	if err != nil {
		return err
	}
	defer srv.Close()
	book, err := demo.Load(*dir)
	if err != nil {
		return err
	}
	out := map[string]any{"materials": []any{}}
	for _, r := range book.Recipes {
		cp, err := srv.Store().Active(r.MaterialID, pipeline.StageReport)
		if err != nil || cp == nil {
			continue
		}
		var v any
		if err := json.Unmarshal(cp.Payload, &v); err == nil {
			out["materials"] = append(out["materials"].([]any), v)
		}
	}
	path := filepath.Join(*dir, demo.ReportDir, "diagnostic_report.json")
	data, _ := json.MarshalIndent(out, "", "  ")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	fmt.Println(path)
	return printJSON(out)
}

func cmdStop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	dir := dirFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return demo.FileStop{Path: filepath.Join(*dir, demo.StopFile)}.RequestStop()
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dir := dirFlag(fs)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	srv, err := server.New(*dir)
	if err != nil {
		return err
	}
	defer srv.Close()
	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}
	fmt.Printf("matbench listening on http://%s (workspace %s)\n", *addr, *dir)
	return httpSrv.ListenAndServe()
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
