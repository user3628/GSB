package walgroup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// ParseSegment validates and returns one persisted segment using the catalog's
// expected segment number and starting LSN.
func (e *Engine) ParseSegment(ctx context.Context, segmentNo uint64) (*ParsedSegment, error) {
	if err := checkClosed(e); err != nil {
		return nil, err
	}
	info, err := getSegment(ctx, e.db, segmentNo)
	if err != nil {
		return nil, err
	}
	if info.State == SegmentReclaimed {
		return nil, newError(CodeNotFound, "segment %d has been physically reclaimed", segmentNo)
	}
	return ParseSegment(segmentPathByInfo(e.dir, info), info.SegmentNo, info.StartLSN)
}

func segmentCorrupt(segmentNo, expectedLSN uint64, format string, args ...any) *Error {
	e := newError(CodeSegmentCorrupt, format, args...)
	e.SegmentNo = segmentNo
	e.ExpectedStartLSN = expectedLSN
	return e
}

func segmentFileName(n uint64) string {
	return fmt.Sprintf("%s%016d%s", segmentFilePrefix, n, segmentFileSuffix)
}

func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentFilePrefix) || !strings.HasSuffix(name, segmentFileSuffix) {
		return 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, segmentFilePrefix), segmentFileSuffix)
	n, err := strconv.ParseUint(body, 10, 64)
	return n, err == nil
}

// ParseSegment strictly validates a segment file. expectedStartLSN may be zero
// to accept the value recorded in the header. Any format, checksum, LSN or
// record-count inconsistency returns E_SEGMENT_CORRUPT with SegmentNo and
// ExpectedStartLSN populated.
func ParseSegment(path string, expectedSegmentNo, expectedStartLSN uint64) (*ParsedSegment, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		e := wrapError(CodeSegmentCorrupt, err, "read segment file")
		e.SegmentNo = expectedSegmentNo
		e.ExpectedStartLSN = expectedStartLSN
		return nil, e
	}
	if len(b) < int(segmentHeaderSize) {
		return nil, segmentCorrupt(expectedSegmentNo, expectedStartLSN, "file is shorter than segment header")
	}
	h, err := decodeHeader(b[:segmentHeaderSize], expectedSegmentNo, expectedStartLSN)
	if err != nil {
		return nil, err
	}
	var recs []Record
	off := int(segmentHeaderSize)
	expectedLSN := h.StartLSN
	for off < len(b) {
		rec, n, err := decodeRecordAt(b, off, h.SegmentNo, len(recs))
		if err != nil {
			if e, ok := AsError(err); ok {
				e.ExpectedStartLSN = expectedStartLSN
			}
			return nil, err
		}
		if rec.LSN != expectedLSN {
			return nil, segmentCorrupt(h.SegmentNo, expectedStartLSN, "non-contiguous LSN at offset %d: got %d want %d", off, rec.LSN, expectedLSN)
		}
		expectedLSN++
		recs = append(recs, rec)
		off += n
	}
	if uint64(len(recs)) != h.RecordCount {
		return nil, segmentCorrupt(h.SegmentNo, expectedStartLSN, "header record count %d does not match %d decoded records", h.RecordCount, len(recs))
	}
	var last uint64
	if len(recs) != 0 {
		last = recs[len(recs)-1].LSN
	}
	if h.LastLSN != last {
		return nil, segmentCorrupt(h.SegmentNo, expectedStartLSN, "header last LSN %d does not match %d", h.LastLSN, last)
	}
	sum := digestBytes(b)
	return &ParsedSegment{Header: h, Records: recs, Digest: sum}, nil
}

func segmentPath(dir string, n uint64) string { return filepath.Join(dir, segmentFileName(n)) }

// segmentPathByInfo keeps a single source of truth for catalog file names.
func segmentPathByInfo(dir string, info SegmentInfo) string {
	if info.FileName != "" {
		return filepath.Join(dir, info.FileName)
	}
	return segmentPath(dir, info.SegmentNo)
}

func sortedSegmentInfos(in []SegmentInfo) []SegmentInfo {
	out := append([]SegmentInfo(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].SegmentNo < out[j].SegmentNo })
	return out
}

func isNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
