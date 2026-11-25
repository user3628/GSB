package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"sampleengine/engine"
)

type stringMap map[string]string

func (m stringMap) String() string {
	b, _ := json.Marshal(m)
	return string(b)
}

func (m stringMap) Set(v string) error {
	parts := strings.SplitN(v, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("field must be key=value, got %q", v)
	}
	m[parts[0]] = parts[1]
	return nil
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func fail(err error) {
	code := engine.ErrorCode(err)
	out := map[string]any{"ok": false, "code": code, "error": err.Error()}
	var ee *engine.Error
	if errors.As(err, &ee) {
		out["detail"] = ee
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	os.Exit(1)
}

func openEngine(db string) *engine.Engine {
	if db == "" {
		db = "sampleengine.db"
	}
	e, err := engine.OpenDB(db, engine.DefaultConfig())
	if err != nil {
		fail(err)
	}
	return e
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "_crash_worker" {
		os.Exit(engine.RunCrashWorker(os.Args[2:]))
	}
	args := os.Args[1:]
	if len(args) == 0 {
		args = []string{"demo"}
	}
	var err error
	switch args[0] {
	case "demo":
		err = cmdDemo(args[1:])
	case "featurize":
		err = cmdFeaturize(args[1:])
	case "import":
		err = cmdImport(args[1:])
	case "backfill":
		err = cmdBackfill(args[1:])
	case "score":
		err = cmdScore(args[1:])
	case "status":
		err = cmdStatus(args[1:])
	case "unit-version":
		err = cmdUnitVersion(args[1:])
	case "report":
		err = cmdReport(args[1:])
	case "rebuild":
		err = cmdRebuild(args[1:])
	case "inject":
		err = cmdInject(args[1:])
	case "publish-next":
		err = cmdPublishNext(args[1:])
	case "publish-loop":
		err = cmdPublishLoop(args[1:])
	case "version-register":
		err = cmdVersionRegister(args[1:])
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		fail(err)
	}
}

func usage() {
	fmt.Println(`sampleengine commands:
  demo [--db PATH] [--dir DIR] [--summary PATH] [--report PATH] [--keep-db]
  featurize --text TEXT [--id ID] [--field k=v]...
  import --db PATH (--demo | --file samples.json)
  backfill --db PATH --sample ID [--label 0|1] [--field k=v]... [--expected-version N]
  score --db PATH --text TEXT [--request-id ID] [--model-version N] [--field k=v]...
  status --db PATH
  unit-version --db PATH --unit N [--version N]
  report --db PATH [--id latest] [--out PATH]
  rebuild --db PATH [--reason whole_replacement]
  inject --case hash_dim_zero|illegal_feature_dim|label_non_binary|learning_rate_non_positive|numeric_nan|numeric_inf
  publish-next --db PATH --unit N [--expected N] [--new-version N]
  publish-loop --db PATH --unit N --iterations N [--worker-id ID]
  version-register --db PATH --sequence N [--generation N] [--dataset-digest H] [--config-digest H] [--replacement]`)
}

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	db := fs.String("db", "", "sqlite path")
	dir := fs.String("dir", "", "directory for deterministic demo db/report")
	summary := fs.String("summary", "", "write summary JSON")
	report := fs.String("report", "", "write rebuild report JSON")
	keep := fs.Bool("keep-db", false, "keep database when using a temporary/default directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := engine.RunDemo(engine.DemoOptions{DBPath: *db, DBDir: *dir, SummaryPath: *summary, ReportPath: *report, KeepDB: *keep})
	if err != nil {
		return err
	}
	printJSON(s)
	if !s.OK {
		os.Exit(2)
	}
	return nil
}

type importFile struct {
	Batches []engine.Batch `json:"batches"`
}

func cmdImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	useDemo := fs.Bool("demo", false, "import fixed-seed demo batches")
	file := fs.String("file", "", "JSON file containing {\"batches\":[...]}")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	var batches []engine.Batch
	if *useDemo {
		batches = engine.GenerateDemoData(engine.DefaultConfig().Seed).Batches
	} else if *file != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		var doc importFile
		if err := json.Unmarshal(b, &doc); err != nil {
			var arr []engine.Batch
			if err2 := json.Unmarshal(b, &arr); err2 != nil {
				return fmt.Errorf("parse import file: %w / %v", err, err2)
			}
			batches = arr
		} else {
			batches = doc.Batches
		}
	} else {
		return fmt.Errorf("import requires --demo or --file")
	}
	reports := make([]*engine.ImportReport, 0, len(batches))
	for _, batch := range batches {
		r, err := e.ImportBatch(batch)
		if err != nil {
			return err
		}
		reports = append(reports, r)
	}
	printJSON(map[string]any{"ok": true, "reports": reports})
	return nil
}

func cmdFeaturize(args []string) error {
	fs := flag.NewFlagSet("featurize", flag.ContinueOnError)
	text := fs.String("text", "", "text to featurize")
	id := fs.String("id", "cli-sample", "sample id")
	fields := stringMap{}
	fs.Var(fields, "field", "feature field k=v")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := engine.DefaultConfig()
	fv, err := engine.Featurize(engine.SampleInput{ID: *id, Text: *text, Fields: map[string]string(fields)}, cfg)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "vector": fv})
	return nil
}

func cmdUnitVersion(args []string) error {
	fs := flag.NewFlagSet("unit-version", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	unit := fs.Int("unit", 0, "unit id")
	version := fs.Int64("version", 0, "unit version; 0 means latest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()

	var stateJSON, updateJSON string
	var err error
	if *version > 0 {
		err = e.DB().QueryRow(`SELECT state_json,update_json FROM unit_versions
			WHERE unit_id=? AND version=? AND valid=1`, *unit, *version).Scan(&stateJSON, &updateJSON)
	} else {
		err = e.DB().QueryRow(`SELECT state_json,update_json FROM unit_versions
			WHERE unit_id=? AND valid=1 ORDER BY version DESC LIMIT 1`, *unit).Scan(&stateJSON, &updateJSON)
	}
	if err != nil {
		return err
	}
	var state, update any
	_ = json.Unmarshal([]byte(stateJSON), &state)
	_ = json.Unmarshal([]byte(updateJSON), &update)
	printJSON(map[string]any{"ok": true, "unit_id": *unit, "state": state, "update": update})
	return nil
}

func cmdBackfill(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	sample := fs.String("sample", "", "sample id")
	label := fs.Int("label", -1, "new binary label")
	expected := fs.Int64("expected-version", 0, "expected current model version")
	reportID := fs.String("report-id", "", "stable report id")
	fields := stringMap{}
	fs.Var(fields, "field", "supplemental field k=v (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *sample == "" {
		return fmt.Errorf("--sample is required")
	}
	req := engine.BackfillRequest{SampleID: *sample, Fields: map[string]string(fields), ExpectedModelVersion: *expected, ReportID: *reportID}
	if *label >= 0 {
		if *label != 0 && *label != 1 {
			return &engine.Error{Code: engine.CodeLabelInvalid, Message: fmt.Sprintf("label must be 0 or 1, got %d", *label)}
		}
		req.Label = label
	}
	e := openEngine(*db)
	defer e.Close()
	r, err := e.Backfill(req)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "report": r})
	return nil
}

func cmdScore(args []string) error {
	fs := flag.NewFlagSet("score", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	text := fs.String("text", "", "text to score")
	reqID := fs.String("request-id", "", "request id")
	version := fs.Int64("model-version", 0, "model version; 0 means current")
	fields := stringMap{}
	fs.Var(fields, "field", "feature field k=v")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	r, err := e.Score(engine.ScoreRequest{RequestID: *reqID, Text: *text, Fields: map[string]string(fields), ModelVersion: *version})
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "result": r})
	return nil
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	st, err := e.Status()
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "status": st})
	return nil
}

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	id := fs.String("id", "latest", "report id or latest")
	out := fs.String("out", "", "export file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	pathOrRaw, err := e.ExportReport(*id, *out)
	if err != nil {
		return err
	}
	if *out == "" {
		fmt.Println(pathOrRaw)
		return nil
	}
	printJSON(map[string]any{"ok": true, "path": pathOrRaw})
	return nil
}

func cmdInject(args []string) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	kind := fs.String("case", "", "injection case")
	requestID := fs.String("request-id", "inject-request", "request id for numeric faults")
	dim := fs.Int("dim", 1, "dimension for numeric/feature injection")
	if err := fs.Parse(args); err != nil {
		return err
	}
	err := engine.InjectParam(engine.ParamInjection{Kind: *kind, RequestID: *requestID, Dim: *dim})
	if err == nil {
		printJSON(map[string]any{"ok": true})
		return nil
	}
	return err
}

func cmdRebuild(args []string) error {
	fs := flag.NewFlagSet("rebuild", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	reason := fs.String("reason", "manual_full_rebuild", "version kind/reason")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	cp, err := e.FullRebuildAndPublish(*reason)
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "model_version": cp.ModelVersion, "generation": cp.Generation, "fingerprint": cp.Fingerprint})
	return nil
}

func cmdPublishNext(args []string) error {
	fs := flag.NewFlagSet("publish-next", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	unit := fs.Int("unit", 0, "unit id")
	expected := fs.Int64("expected", 0, "expected current version")
	newVersion := fs.Int64("new-version", 0, "explicit new version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	r, err := e.PublishUnit(engine.PublishUnitRequest{UnitID: *unit, ExpectedVersion: *expected, NewVersion: *newVersion, Producer: "cli"})
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "result": r})
	return nil
}

func cmdPublishLoop(args []string) error {
	fs := flag.NewFlagSet("publish-loop", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	unit := fs.Int("unit", 0, "unit id")
	iterations := fs.Int("iterations", 5, "publications by this process")
	workerID := fs.String("worker-id", "process", "producer label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	results := make([]*engine.PublishUnitResult, 0, *iterations)
	for i := 0; i < *iterations; i++ {
		r, err := e.PublishUnit(engine.PublishUnitRequest{UnitID: *unit, Producer: *workerID})
		if err != nil {
			return err
		}
		results = append(results, r)
	}
	printJSON(map[string]any{"ok": true, "worker": *workerID, "results": results})
	return nil
}

func cmdVersionRegister(args []string) error {
	fs := flag.NewFlagSet("version-register", flag.ContinueOnError)
	db := fs.String("db", "sampleengine.db", "sqlite path")
	sequence := fs.Int64("sequence", 0, "proposed sequence")
	generation := fs.Int64("generation", 0, "proposed generation")
	dataset := fs.String("dataset-digest", "", "dataset content digest")
	configDigest := fs.String("config-digest", "", "config digest")
	replacement := fs.Bool("replacement", false, "mark whole replacement")
	if err := fs.Parse(args); err != nil {
		return err
	}
	e := openEngine(*db)
	defer e.Close()
	dec, err := e.RegisterVersion(engine.VersionRequest{Sequence: *sequence, Generation: *generation, DatasetDigest: *dataset, ConfigDigest: *configDigest, Replacement: *replacement})
	if err != nil {
		return err
	}
	printJSON(map[string]any{"ok": true, "decision": dec})
	return nil
}
