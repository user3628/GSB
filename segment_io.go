package walgroup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

func createSegmentFile(dir string, h SegmentHeader) (string, error) {
	final := segmentPath(dir, h.SegmentNo)
	tmp := final + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	cleanup := func() {
		f.Close()
		os.Remove(tmp)
	}
	if _, err := f.WriteAt(encodeHeader(h), 0); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return "", err
	}
	syncDir(dir)
	return final, nil
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

func writeHeaderFile(f *os.File, h SegmentHeader) error {
	_, err := f.WriteAt(encodeHeader(h), 0)
	return err
}

func readHeaderFromFile(path string, expectedNo, expectedStart uint64) (SegmentHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		e := wrapError(CodeSegmentCorrupt, err, "open segment file")
		e.SegmentNo = expectedNo
		e.ExpectedStartLSN = expectedStart
		return SegmentHeader{}, e
	}
	defer f.Close()
	b := make([]byte, segmentHeaderSize)
	if _, err := io.ReadFull(f, b); err != nil {
		return SegmentHeader{}, segmentCorrupt(expectedNo, expectedStart, "read segment header: %v", err)
	}
	return decodeHeader(b, expectedNo, expectedStart)
}

func computeFileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func truncateAndRestoreHeader(path string, info SegmentInfo) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if info.ByteSize < int64(segmentHeaderSize) {
		return fmt.Errorf("invalid rollback size %d", info.ByteSize)
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() < info.ByteSize {
		return fmt.Errorf("segment is shorter (%d) than rollback checkpoint (%d)", st.Size(), info.ByteSize)
	}
	if err := f.Truncate(info.ByteSize); err != nil {
		return err
	}
	h := SegmentHeader{
		Magic:       segmentMagic,
		Version:     segmentVersion,
		HeaderSize:  segmentHeaderSize,
		SegmentNo:   info.SegmentNo,
		StartLSN:    info.StartLSN,
		PrevDigest:  info.PrevDigest,
		RecordCount: info.RecordCount,
		TxnCount:    info.TxnCount,
		LastLSN:     info.LastLSN,
	}
	if err := writeHeaderFile(f, h); err != nil {
		return err
	}
	return f.Sync()
}
