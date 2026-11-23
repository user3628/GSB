package snapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"example.com/joinshuffle/internal/model"
)

var magic = [8]byte{'C', 'L', 'S', 'N', 'P', '0', '1', 0}

const (
	headerSize      = 40
	digestSize      = sha256.Size
	maxRows         = 20_000_000
	maxSnapshotSize = 1 << 30
)

type Snapshot struct {
	Meta      model.SnapshotMeta
	Keys      []int64
	Measures  []float64
	Nulls     []bool
	Checksum  [sha256.Size]byte
	ByteCount int
}

func New(side string, version uint64, generatedAt time.Time, keys []int64, measures []float64, nulls []bool) (*Snapshot, error) {
	if len(keys) != len(measures) || len(keys) != len(nulls) {
		return nil, fmt.Errorf("column lengths differ: keys=%d measures=%d nulls=%d", len(keys), len(measures), len(nulls))
	}
	if len(keys) > maxRows {
		return nil, fmt.Errorf("too many rows: %d", len(keys))
	}
	for i := range measures {
		if !nulls[i] && (math.IsNaN(measures[i]) || math.IsInf(measures[i], 0)) {
			return nil, fmt.Errorf("non-finite measure at row %d", i)
		}
	}
	s := &Snapshot{
		Meta: model.SnapshotMeta{
			Side:        side,
			Version:     version,
			GeneratedAt: generatedAt.UTC().UnixNano(),
			Rows:        len(keys),
		},
		Keys:     append([]int64(nil), keys...),
		Measures: append([]float64(nil), measures...),
		Nulls:    append([]bool(nil), nulls...),
	}
	return s, nil
}

func (s *Snapshot) IsNull(i int) bool {
	return i < 0 || i >= len(s.Nulls) || s.Nulls[i]
}

func (s *Snapshot) ValidMeasure(i int) (float64, bool) {
	if s.IsNull(i) {
		return 0, false
	}
	return s.Measures[i], true
}

func Marshal(s *Snapshot) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("nil snapshot")
	}
	if len(s.Meta.Side) == 0 || len(s.Meta.Side) > 32 {
		return nil, fmt.Errorf("invalid side length")
	}
	if len(s.Keys) != len(s.Measures) || len(s.Keys) != len(s.Nulls) {
		return nil, fmt.Errorf("column lengths differ")
	}
	if len(s.Keys) > maxRows {
		return nil, fmt.Errorf("too many rows")
	}
	nullBytes := (len(s.Keys) + 7) / 8
	total := headerSize + len(s.Meta.Side) + len(s.Keys)*16 + nullBytes + digestSize
	if total > maxSnapshotSize {
		return nil, fmt.Errorf("snapshot too large: %d bytes", total)
	}
	buf := make([]byte, total)
	copy(buf[0:8], magic[:])
	binary.LittleEndian.PutUint64(buf[8:16], s.Meta.Version)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(s.Meta.GeneratedAt))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(len(s.Keys)))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(len(s.Meta.Side)))
	binary.LittleEndian.PutUint32(buf[36:40], 0)
	off := headerSize
	copy(buf[off:off+len(s.Meta.Side)], s.Meta.Side)
	off += len(s.Meta.Side)
	for i, key := range s.Keys {
		binary.LittleEndian.PutUint64(buf[off+i*8:off+(i+1)*8], uint64(key))
	}
	off += len(s.Keys) * 8
	for i, value := range s.Measures {
		binary.LittleEndian.PutUint64(buf[off+i*8:off+(i+1)*8], math.Float64bits(value))
	}
	off += len(s.Measures) * 8
	for i, isNull := range s.Nulls {
		if isNull {
			buf[off+i/8] |= 1 << uint(i%8)
		}
	}
	off += nullBytes
	digest := sha256.Sum256(buf[:off])
	copy(buf[off:off+digestSize], digest[:])
	return buf, nil
}

func Parse(data []byte) (*Snapshot, error) {
	if len(data) < headerSize+digestSize {
		return nil, fmt.Errorf("snapshot too short")
	}
	if len(data) > maxSnapshotSize {
		return nil, fmt.Errorf("snapshot too large")
	}
	if !bytes.Equal(data[0:8], magic[:]) {
		return nil, fmt.Errorf("bad snapshot magic")
	}
	version := binary.LittleEndian.Uint64(data[8:16])
	generatedAt := int64(binary.LittleEndian.Uint64(data[16:24]))
	rows := binary.LittleEndian.Uint64(data[24:32])
	sideLen := int(binary.LittleEndian.Uint32(data[32:36]))
	reserved := binary.LittleEndian.Uint32(data[36:40])
	if reserved != 0 {
		return nil, fmt.Errorf("unsupported snapshot flags")
	}
	if sideLen == 0 || sideLen > 32 {
		return nil, fmt.Errorf("invalid side length")
	}
	if rows > maxRows {
		return nil, fmt.Errorf("too many rows: %d", rows)
	}
	n := int(rows)
	nullBytes := (n + 7) / 8
	expected := headerSize + sideLen + n*16 + nullBytes + digestSize
	if expected != len(data) {
		return nil, fmt.Errorf("size mismatch: got %d want %d", len(data), expected)
	}
	off := headerSize
	side := string(data[off : off+sideLen])
	off += sideLen
	if side != "left" && side != "right" {
		return nil, fmt.Errorf("unsupported side %q", side)
	}
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(binary.LittleEndian.Uint64(data[off+i*8 : off+(i+1)*8]))
	}
	off += n * 8
	measures := make([]float64, n)
	for i := range measures {
		measures[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[off+i*8 : off+(i+1)*8]))
	}
	off += n * 8
	nulls := make([]bool, n)
	for i := range nulls {
		nulls[i] = data[off+i/8]&(1<<uint(i%8)) != 0
		if !nulls[i] && (math.IsNaN(measures[i]) || math.IsInf(measures[i], 0)) {
			return nil, fmt.Errorf("non-finite measure at row %d", i)
		}
		if nulls[i] {
			measures[i] = 0
		}
	}
	off += nullBytes
	storedDigest := data[off : off+digestSize]
	computed := sha256.Sum256(data[:off])
	if !bytes.Equal(storedDigest, computed[:]) {
		return nil, fmt.Errorf("embedded checksum mismatch")
	}
	var payloadChecksum [sha256.Size]byte
	copy(payloadChecksum[:], storedDigest)
	fileChecksum := sha256.Sum256(data)
	return &Snapshot{
		Meta: model.SnapshotMeta{
			Side:        side,
			Version:     version,
			GeneratedAt: generatedAt,
			Rows:        n,
			SHA256:      hex.EncodeToString(fileChecksum[:]),
		},
		Keys:      keys,
		Measures:  measures,
		Nulls:     nulls,
		Checksum:  payloadChecksum,
		ByteCount: len(data),
	}, nil
}

func ReadFile(path, checksumPath, expectedSHA string) (*Snapshot, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("snapshot path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	s, err := Parse(data)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(checksumPath) == "" {
		checksumPath = path + ".sha256"
	}
	checkBytes, err := os.ReadFile(checksumPath)
	if err != nil {
		return nil, fmt.Errorf("read checksum file: %w", err)
	}
	fileSHA, err := parseChecksumFile(checkBytes)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(fileSHA, s.Meta.SHA256) {
		return nil, fmt.Errorf("checksum file mismatch: file=%s embedded=%s", fileSHA, s.Meta.SHA256)
	}
	if expectedSHA != "" && !strings.EqualFold(strings.TrimSpace(expectedSHA), s.Meta.SHA256) {
		return nil, fmt.Errorf("requested checksum mismatch: requested=%s actual=%s", expectedSHA, s.Meta.SHA256)
	}
	s.Meta.Path = path
	s.Meta.ChecksumPath = checksumPath
	return s, nil
}

func WriteFile(path string, s *Snapshot) (model.SnapshotMeta, error) {
	buf, err := Marshal(s)
	if err != nil {
		return model.SnapshotMeta{}, err
	}
	if err := writeFileAtomic(path, buf, 0o644); err != nil {
		return model.SnapshotMeta{}, err
	}
	payloadDigest := sha256.Sum256(buf[:len(buf)-digestSize])
	fileDigest := sha256.Sum256(buf)
	hexDigest := hex.EncodeToString(fileDigest[:])
	checksumPath := path + ".sha256"
	checkLine := fmt.Sprintf("%s  %s\n", hexDigest, filepath.Base(path))
	if err := writeFileAtomic(checksumPath, []byte(checkLine), 0o644); err != nil {
		return model.SnapshotMeta{}, err
	}
	s.Meta.Path = path
	s.Meta.ChecksumPath = checksumPath
	s.Meta.SHA256 = hexDigest
	s.Checksum = payloadDigest
	s.ByteCount = len(buf)
	return s.Meta, nil
}

func parseChecksumFile(data []byte) (string, error) {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "", fmt.Errorf("empty checksum file")
	}
	field := strings.Fields(text)[0]
	field = strings.TrimPrefix(field, "*")
	if len(field) != sha256.Size*2 {
		return "", fmt.Errorf("invalid checksum length")
	}
	if _, err := hex.DecodeString(field); err != nil {
		return "", fmt.Errorf("invalid checksum encoding: %w", err)
	}
	return strings.ToLower(field), nil
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
