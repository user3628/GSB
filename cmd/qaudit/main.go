// Command qaudit is the CLI + local HTTP entry point for the layered
// quantization calibration and error-propagation audit workbench.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"qaudit/internal/engine"
	"qaudit/internal/generator"
	"qaudit/internal/model"
	"qaudit/internal/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}
	cmd := args[0]
	rest := args[1:]
	switch cmd {
	case "generate":
		dataDir := flagValue(rest, "data", "data")
		return generate(dataDir)
	case "run":
		dataDir, dbPath, extra := commonFlags(rest)
		return runPipeline(dataDir, dbPath, extra)
	case "recover":
		dataDir, dbPath, _ := commonFlags(rest)
		return recoverPipeline(dataDir, dbPath)
	case "status":
		dataDir, dbPath, _ := commonFlags(rest)
		return status(dataDir, dbPath)
	case "audit":
		dataDir, dbPath, _ := commonFlags(rest)
		return auditCmd(dataDir, dbPath)
	case "versions":
		dataDir, dbPath, _ := commonFlags(rest)
		return versions(dataDir, dbPath)
	case "export":
		dataDir, dbPath, _ := commonFlags(rest)
		return exportCmd(dataDir, dbPath)
	case "inject":
		dataDir, dbPath, extra := commonFlags(rest)
		return injectCmd(dataDir, dbPath, extra)
	case "serve":
		dataDir, dbPath, extra := commonFlags(rest)
		return serve(dataDir, dbPath, extra)
	case "stop":
		dataDir, dbPath, _ := commonFlags(rest)
		return stopCmd(dataDir, dbPath)
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// commonFlags extracts -data/-db and leaves all remaining args for the
// subcommand's own FlagSet.
func commonFlags(args []string) (dataDir, dbPath string, rest []string) {
	dataDir = "data"
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-data":
			if i+1 < len(args) {
				dataDir = args[i+1]
				i++
			}
		case "-db":
			if i+1 < len(args) {
				dbPath = args[i+1]
				i++
			}
		default:
			rest = append(rest, args[i])
		}
	}
	if dbPath == "" {
		dbPath = filepath.Join(dataDir, "snapshots.db")
	}
	return dataDir, dbPath, rest
}

func flagValue(args []string, name, def string) string {
	for i := 0; i < len(args); i++ {
		if args[i] == "-"+name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return def
}

func usage() {
	fmt.Print(`qaudit - layered quantization calibration & error-propagation audit workbench

Usage:
  qaudit generate [-data DIR]
  qaudit run      [-data DIR] [-db PATH] [-crash-after-layer N]
  qaudit recover  [-data DIR] [-db PATH]
  qaudit status   [-data DIR] [-db PATH]
  qaudit audit    [-data DIR] [-db PATH]
  qaudit versions [-data DIR] [-db PATH]
  qaudit export   [-data DIR] [-db PATH]
  qaudit inject   [-data DIR] [-db PATH] -kind bad_bitwidth|zero_channel|nan_weight|calib_ratio ...
  qaudit serve    [-data DIR] [-db PATH] [-addr 127.0.0.1:PORT]
  qaudit stop     [-data DIR] [-db PATH]
`)
}

func generate(dataDir string) error {
	ds, err := generator.Generate(dataDir)
	if err != nil {
		return err
	}
	eng, err := engine.New(dataDir, filepath.Join(dataDir, "snapshots.db"))
	if err != nil {
		return err
	}
	defer eng.Close()
	if err := eng.SaveDataset(ds); err != nil {
		return err
	}
	fmt.Printf("generated deterministic dataset in %s (%d shards, %d layers)\n",
		dataDir, len(ds.Shards), len(ds.Network.Layers))
	return nil
}

func openEngine(dataDir, dbPath string) (*engine.Engine, error) {
	eng, err := engine.New(dataDir, dbPath)
	if err != nil {
		return nil, err
	}
	if eng.Dataset == nil {
		return nil, model.NewBizError(model.CodeState, "dataset missing; run 'qaudit generate' first", nil)
	}
	return eng, nil
}

func runPipeline(dataDir, dbPath string, extra []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	crash := fs.Int("crash-after-layer", 0, "simulate forced kill after N published layers")
	if err := fs.Parse(extra); err != nil {
		return err
	}
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	if err := eng.ClearStop(); err != nil {
		return err
	}
	eng.CrashAfterLayer = *crash
	err = eng.Run()
	printSnapshot(eng)
	return err
}

func recoverPipeline(dataDir, dbPath string) error {
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	if err := eng.ClearStop(); err != nil {
		return err
	}
	if err := eng.Recover(); err != nil {
		return err
	}
	printSnapshot(eng)
	return nil
}

func status(dataDir, dbPath string) error {
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	statuses, err := eng.LayerStatuses()
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"snapshot": eng.Snapshot(), "layers": statuses})
}

func auditCmd(dataDir, dbPath string) error {
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	return printJSON(eng.AuditView())
}

func versions(dataDir, dbPath string) error {
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	rows, err := eng.Versions()
	if err != nil {
		return err
	}
	return printJSON(map[string]any{"versions": rows})
}

func exportCmd(dataDir, dbPath string) error {
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	if err := eng.Recover(); err != nil {
		return err
	}
	rep, err := eng.ExportReport()
	if err != nil {
		return err
	}
	return printJSON(rep)
}

func injectCmd(dataDir, dbPath string, extra []string) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	kind := fs.String("kind", "", "injection kind")
	layer := fs.String("layer", "fc1", "target layer")
	channel := fs.Int("channel", 0, "target channel")
	bits := fs.Int("bitwidth", 3, "bit width for bad_bitwidth")
	ratio := fs.Float64("ratio", 9.0, "ratio for calib_ratio injection")
	if err := fs.Parse(extra); err != nil {
		return err
	}
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	// Fault injection uses a fresh isolated snapshot database so the injected
	// fingerprint never collides with any prior baseline or injection history.
	injPath := filepath.Join(os.TempDir(), fmt.Sprintf("qaudit_inject_%d.db", time.Now().UnixNano()))
	defer os.Remove(injPath)
	injEng, err := engine.New(dataDir, injPath)
	if err != nil {
		return err
	}
	defer injEng.Close()
	ds, err := eng.CloneDataset()
	if err != nil {
		return err
	}
	inj := engine.Injection{
		Kind: *kind, Layer: *layer, Channel: *channel,
		BitWidth: *bits, CalibRatio: *ratio,
	}
	if err := engine.Apply(ds, inj); err != nil {
		return err
	}
	return injEng.RunWith(ds)
}

func serve(dataDir, dbPath string, extra []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:0", "listen address")
	if err := fs.Parse(extra); err != nil {
		return err
	}
	eng, err := openEngine(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	srv := server.New(eng)
	addrUsed, ln, err := server.Listen(*addr)
	if err != nil {
		return err
	}
	fmt.Printf("qaudit HTTP listening on http://%s\n", addrUsed)
	return http.Serve(ln, srv.Handler())
}

func stopCmd(dataDir, dbPath string) error {
	eng, err := engine.New(dataDir, dbPath)
	if err != nil {
		return err
	}
	defer eng.Close()
	return eng.RequestGracefulStop()
}

func printSnapshot(eng *engine.Engine) {
	snap := eng.Snapshot()
	b, _ := json.MarshalIndent(snap, "", "  ")
	fmt.Println(string(b))
}

func printJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
