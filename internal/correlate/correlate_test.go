package correlate

import (
	"testing"

	"forensicworkbench/internal/codes"
)

func ev(typ codes.EventType, session string, pid, ppid int64, source, path string, off int64) codes.Event {
	nano := int64(1000 + off)
	return codes.Event{
		Type: typ, Source: source, FormatVersion: source + "/v1", SourcePath: path,
		Offset: off, EndOffset: off + 1, SessionID: session, PID: pid, PPID: ppid,
		TimeUnixNano: &nano, Code: codes.EOK,
	}
}

func TestLineageAndOrphan(t *testing.T) {
	events := []codes.Event{
		ev(codes.EvProcessCreate, "S1", 10, 0, "binary", "a.bin", 0),
		ev(codes.EvProcessCreate, "S1", 11, 10, "binary", "a.bin", 10),
		// orphan: parent 99 never created.
		ev(codes.EvProcessCreate, "S1", 12, 99, "jsonl", "e.jsonl", 20),
		ev(codes.EvFileAccess, "S1", 11, 10, "text", "x.txt", 30),
	}
	g := Build(events)
	if len(g.Order) != 1 || g.Order[0] != "S1" {
		t.Fatalf("unexpected sessions: %v", g.Order)
	}
	sg := g.Sessions["S1"]
	var parent11, orphan *Edge
	for i := range sg.Edges {
		e := &sg.Edges[i]
		if e.ChildPID == 11 {
			parent11 = e
		}
		if e.ChildPID == 12 {
			orphan = e
		}
	}
	if parent11 == nil || parent11.ParentPID != 10 || parent11.Orphan {
		t.Fatalf("normal edge wrong: %+v", parent11)
	}
	if parent11.Source != "binary" || parent11.Offset != 10 {
		t.Fatal("edge not traceable to source/offset")
	}
	if orphan == nil || !orphan.Orphan || orphan.ParentPID != VirtualRootPID {
		t.Fatalf("orphan edge wrong: %+v", orphan)
	}
	if orphan.Source != "jsonl" || orphan.Offset != 20 {
		t.Fatal("orphan edge lost source location")
	}
	hasOrphanCode := false
	for _, e := range sg.Errors {
		if e.Code == codes.EOrphanRef && e.Offset == 20 {
			hasOrphanCode = true
		}
	}
	if !hasOrphanCode {
		t.Fatal("E_ORPHAN_REF not recorded")
	}
	// File event attached to node 11.
	if len(sg.Nodes[11].EventRefs) != 2 {
		t.Fatalf("node 11 should hold create + file events, got %d", len(sg.Nodes[11].EventRefs))
	}
}

func TestSessionsNeverCrossLink(t *testing.T) {
	events := []codes.Event{
		ev(codes.EvProcessCreate, "S1", 5, 1, "binary", "a.bin", 0),
		ev(codes.EvProcessCreate, "S2", 5, 1, "jsonl", "e.jsonl", 5),
	}
	g := Build(events)
	if len(g.Sessions) != 2 {
		t.Fatalf("expected two isolated sessions, got %d", len(g.Sessions))
	}
	for _, sid := range []string{"S1", "S2"} {
		for _, e := range g.Sessions[sid].Edges {
			if e.SessionID != sid {
				t.Fatalf("edge leaked across sessions: %+v", e)
			}
		}
	}
	// Both pid 5 are orphans (parent pid 1 missing) but stay in own sessions.
	if g.Sessions["S1"].Nodes[5].SessionID != "S1" || g.Sessions["S2"].Nodes[5].SessionID != "S2" {
		t.Fatal("same pid in different sessions collapsed")
	}
}

func TestRootProcessIsNotOrphan(t *testing.T) {
	events := []codes.Event{ev(codes.EvProcessCreate, "S", 1, 0, "binary", "a.bin", 0)}
	g := Build(events)
	for _, e := range g.Sessions["S"].Edges {
		if e.Orphan {
			t.Fatalf("PPID 0 root must not be marked orphan: %+v", e)
		}
	}
	for _, er := range g.Sessions["S"].Errors {
		if er.Code == codes.EOrphanRef {
			t.Fatal("root process produced a spurious orphan error")
		}
	}
}
