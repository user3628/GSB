package snapshot_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/joinshuffle/internal/sample"
	"example.com/joinshuffle/internal/snapshot"
)

func TestDeterministicSnapshotAndChecksum(t *testing.T) {
	left, right, err := sample.GeneratePair(42, 128, 96, 1, 1, sample.DefaultGeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	first, err := snapshot.Marshal(left)
	if err != nil {
		t.Fatal(err)
	}
	second, err := snapshot.Marshal(left)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("snapshot marshal is not deterministic")
	}
	if len(first) != left.ByteCount && left.ByteCount != 0 {
		t.Fatalf("unexpected byte count: %d", left.ByteCount)
	}

	dir := t.TempDir()
	leftPath := filepath.Join(dir, "left.snapshot")
	rightPath := filepath.Join(dir, "right.snapshot")
	meta, err := snapshot.WriteFile(leftPath, left)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.WriteFile(rightPath, right); err != nil {
		t.Fatal(err)
	}
	fileBytes, err := os.ReadFile(leftPath)
	if err != nil {
		t.Fatal(err)
	}
	fileDigest := sha256.Sum256(fileBytes)
	if meta.SHA256 != hex.EncodeToString(fileDigest[:]) {
		t.Fatalf("external checksum is not the full-file SHA-256: %s", meta.SHA256)
	}
	checkBytes, err := os.ReadFile(leftPath + ".sha256")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(checkBytes), meta.SHA256+"  ") {
		t.Fatalf("unexpected checksum file: %q", checkBytes)
	}
	loaded, err := snapshot.ReadFile(leftPath, "", meta.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Meta.Rows != 128 || loaded.Meta.Version != 1 || loaded.Meta.Side != "left" {
		t.Fatalf("unexpected metadata: %+v", loaded.Meta)
	}
}

func TestSnapshotRejectsCorruption(t *testing.T) {
	s, err := snapshot.New("left", 1, time.Unix(0, 0), []int64{1, 2}, []float64{1.5, 2.5}, []bool{false, false})
	if err != nil {
		t.Fatal(err)
	}
	data, err := snapshot.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0x01
	if _, err := snapshot.Parse(data); err == nil {
		t.Fatal("expected corruption to be rejected")
	}
}

func TestChecksumFileMismatchRejected(t *testing.T) {
	s, err := snapshot.New("left", 1, time.Unix(0, 0), []int64{1}, []float64{1}, []bool{false})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "left.snapshot")
	if _, err := snapshot.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	wrong := strings.Repeat("0", 64) + "  left.snapshot\n"
	if err := os.WriteFile(path+".sha256", []byte(wrong), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.ReadFile(path, "", ""); err == nil {
		t.Fatal("expected external checksum mismatch")
	}
}
