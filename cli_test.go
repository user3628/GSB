package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"sampleengine/engine"
)

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func TestCLIProcessPublishAndInjection(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "sampleengine"+exeSuffix())
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	db := filepath.Join(tmp, "process.db")
	init := exec.Command(bin, "import", "--db", db, "--demo")
	if out, err := init.CombinedOutput(); err != nil {
		t.Fatalf("import demo: %v\n%s", err, out)
	}

	before, err := engine.OpenDB(db, engine.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	stBefore, err := before.Status()
	if err != nil {
		t.Fatal(err)
	}
	var oldVersion int64
	for _, u := range stBefore.Units {
		if u.UnitID == 5 {
			oldVersion = u.CurrentVersion
		}
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}

	const procs = 4
	const iterations = 3
	var wg sync.WaitGroup
	errs := make(chan error, procs)
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cmd := exec.Command(bin, "publish-loop", "--db", db, "--unit", "5", "--iterations", "3", "--worker-id", "proc")
			if out, err := cmd.CombinedOutput(); err != nil {
				errs <- &processError{id: id, err: err, out: string(out)}
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	after, err := engine.OpenDB(db, engine.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	stAfter, err := after.Status()
	if err != nil {
		t.Fatal(err)
	}
	var newVersion int64
	for _, u := range stAfter.Units {
		if u.UnitID == 5 {
			newVersion = u.CurrentVersion
		}
	}
	if want := oldVersion + procs*iterations; newVersion != want {
		t.Fatalf("unit 5 version = %d, want %d", newVersion, want)
	}
	var currentRows int
	if err := after.DB().QueryRow("SELECT COUNT(*) FROM unit_versions WHERE unit_id=5 AND is_current=1").Scan(&currentRows); err != nil {
		t.Fatal(err)
	}
	if currentRows != 1 {
		t.Fatalf("current rows = %d, want 1", currentRows)
	}

	cmd := exec.Command(bin, "inject", "--case", "label_non_binary")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected injection command failure, output=%s", out)
	}
	var payload struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(lastJSON(out), &payload); err != nil {
		t.Fatalf("parse injection output: %v\n%s", err, out)
	}
	if payload.Code != engine.CodeLabelInvalid {
		t.Fatalf("code = %q, want %s", payload.Code, engine.CodeLabelInvalid)
	}
}

type processError struct {
	id  int
	err error
	out string
}

func (e *processError) Error() string { return e.err.Error() + "\n" + e.out }

func lastJSON(b []byte) []byte {
	start := indexByte(b, '{')
	if start < 0 {
		return b
	}
	return b[start:]
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

var _ = os.TempDir
