package store

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"csrouter/internal/domain"
)

func TestPublishChildProcessHelper(t *testing.T) {
	if os.Getenv("CSROUTER_PUBLISH_CHILD") != "1" {
		t.Skip("child-process helper only")
	}
	dbPath := os.Getenv("CSROUTER_DB_PATH")
	workerID := os.Getenv("CSROUTER_WORKER_ID")
	if dbPath == "" || workerID == "" {
		t.Fatal("missing CSROUTER_DB_PATH or CSROUTER_WORKER_ID")
	}

	opened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()

	ticketID := "PROCESS-" + workerID
	if _, bizErr := opened.PublishArtifact(PublishInput{
		TicketID:          ticketID,
		Stage:             domain.StageNormalize,
		InputFingerprint:  "input-" + ticketID,
		ConfigVersion:     1,
		ConfigFingerprint: "policy-fingerprint",
		Artifact:          domain.NormalizeArtifact{TicketID: ticketID},
	}); bizErr != nil {
		t.Fatalf("worker %s: %v", workerID, bizErr)
	}
}

func TestMultipleProcessPublishersMaintainContiguousSequence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "process-csrouter.db")
	admin, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Close(); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	type workerProcess struct {
		cmd *exec.Cmd
		out *bytes.Buffer
	}
	processes := make([]workerProcess, 0, workers)
	for i := range workers {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPublishChildProcessHelper$")
		out := &bytes.Buffer{}
		cmd.Stdout = out
		cmd.Stderr = out
		cmd.Env = append(os.Environ(),
			"CSROUTER_PUBLISH_CHILD=1",
			"CSROUTER_DB_PATH="+dbPath,
			"CSROUTER_WORKER_ID="+fmt.Sprintf("%02d", i),
		)
		if err := cmd.Start(); err != nil {
			t.Fatalf("start worker %d: %v", i, err)
		}
		processes = append(processes, workerProcess{cmd: cmd, out: out})
	}
	for i, worker := range processes {
		if err := worker.cmd.Wait(); err != nil {
			t.Fatalf("worker %d failed: %v\n%s", i, err, worker.out.String())
		}
	}

	reader, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	head, err := reader.StageHead(domain.StageNormalize)
	if err != nil {
		t.Fatal(err)
	}
	if head != workers {
		t.Fatalf("stage head=%d, want %d", head, workers)
	}
	var gaps int
	if err := reader.db.QueryRow(`
		SELECT COUNT(1) FROM (
			SELECT stage_seq, ROW_NUMBER() OVER (ORDER BY stage_seq) AS expected_seq
			FROM artifacts WHERE stage=?
		) WHERE stage_seq <> expected_seq`, domain.StageNormalize).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	if gaps != 0 {
		t.Fatalf("found %d non-contiguous stage sequences", gaps)
	}
}
