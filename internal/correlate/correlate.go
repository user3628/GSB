// Package correlate builds per-session process lineage graphs from normalized
// events and attaches file/registry/network/process events to process nodes.
// Sessions are never cross-linked; a process-creation event whose parent was
// never created (and is not an explicit root with PPID 0) yields an
// E_ORPHAN_REF edge into the session's virtual root.
package correlate

import (
	"fmt"
	"sort"

	"forensicworkbench/internal/codes"
)

// VirtualRootPID is the synthetic PID of each session's virtual root.
const VirtualRootPID int64 = 0

// Node is one process in a session tree.
type Node struct {
	SessionID string `json:"session_id"`
	PID       int64  `json:"pid"`
	PPID      int64  `json:"ppid"`
	Virtual   bool   `json:"virtual"`
	Subject   string `json:"subject"`
	Object    string `json:"command_line,omitempty"`
	// EventRefs lists every event attached to this node.
	EventRefs []EventRef `json:"event_refs"`
}

// EventRef locates an attached event by source and byte offset.
type EventRef struct {
	Source     string          `json:"source"`
	SourcePath string          `json:"source_path"`
	Offset     int64           `json:"offset"`
	EndOffset  int64           `json:"end_offset"`
	Type       codes.EventType `json:"type"`
}

// Edge is a directed parent->child lineage edge, traceable to its source.
type Edge struct {
	SessionID  string `json:"session_id"`
	ParentPID  int64  `json:"parent_pid"`
	ChildPID   int64  `json:"child_pid"`
	Source     string `json:"source"`
	SourcePath string `json:"source_path"`
	Offset     int64  `json:"offset"`
	EndOffset  int64  `json:"end_offset"`
	Orphan     bool   `json:"orphan"`
}

// SessionGraph is one isolated session.
type SessionGraph struct {
	SessionID string              `json:"session_id"`
	Nodes     map[int64]*Node     `json:"-"`
	NodeOrder []int64             `json:"node_order"`
	Edges     []Edge              `json:"edges"`
	Errors    []codes.ErrorRecord `json:"errors,omitempty"`
}

// Graph is the full correlation result keyed by session.
type Graph struct {
	Sessions map[string]*SessionGraph `json:"sessions"`
	Order    []string                 `json:"session_order"`
}

type procKey struct {
	session string
	pid     int64
}

// Build constructs lineage graphs in deterministic passes:
//  1. every process-creation event registers a process node (PPID retained);
//  2. one lineage edge per created process is resolved inside its session;
//  3. all non-creation events are attached to their PID node, creating a
//     placeholder node (linked to virtual root) when no creation event exists.
func Build(events []codes.Event) *Graph {
	g := &Graph{Sessions: map[string]*SessionGraph{}}
	ensure := func(session string) *SessionGraph {
		sg, ok := g.Sessions[session]
		if !ok {
			sg = &SessionGraph{
				SessionID: session,
				Nodes:     map[int64]*Node{},
				Edges:     []Edge{},
				Errors:    []codes.ErrorRecord{},
			}
			sg.Nodes[VirtualRootPID] = &Node{SessionID: session, PID: VirtualRootPID, Virtual: true, Subject: "VIRTUAL_ROOT"}
			sg.NodeOrder = append(sg.NodeOrder, VirtualRootPID)
			g.Sessions[session] = sg
			g.Order = append(g.Order, session)
		}
		return sg
	}

	created := map[procKey]bool{}
	// Pass 1: register nodes from process-creation events.
	for _, ev := range events {
		if ev.Type != codes.EvProcessCreate {
			continue
		}
		sg := ensure(ev.SessionID)
		created[procKey{ev.SessionID, ev.PID}] = true
		if _, exists := sg.Nodes[ev.PID]; !exists {
			sg.Nodes[ev.PID] = &Node{
				SessionID: ev.SessionID, PID: ev.PID, PPID: ev.PPID,
				Subject: ev.Subject, Object: ev.Object,
			}
			sg.NodeOrder = append(sg.NodeOrder, ev.PID)
		}
		sg.Nodes[ev.PID].EventRefs = append(sg.Nodes[ev.PID].EventRefs, refOf(ev))
	}

	// Pass 2: resolve exactly one lineage edge per created process.
	edgeSeen := map[string]bool{}
	for _, ev := range events {
		if ev.Type != codes.EvProcessCreate {
			continue
		}
		sg := ensure(ev.SessionID)
		// PPID 0 means an explicit session root; attach to virtual root with
		// no orphan marker. A non-root whose parent creation is missing is an
		// orphan and is reported with E_ORPHAN_REF.
		parentKnown := ev.PPID != VirtualRootPID && created[procKey{ev.SessionID, ev.PPID}]
		edgeKey := fmt.Sprintf("%s|%d->%d", ev.SessionID, ev.PPID, ev.PID)
		if edgeSeen[edgeKey] {
			continue
		}
		edgeSeen[edgeKey] = true
		if parentKnown {
			sg.Edges = append(sg.Edges, Edge{
				SessionID: ev.SessionID, ParentPID: ev.PPID, ChildPID: ev.PID,
				Source: ev.Source, SourcePath: ev.SourcePath,
				Offset: ev.Offset, EndOffset: ev.EndOffset,
			})
			continue
		}
		sg.Edges = append(sg.Edges, Edge{
			SessionID: ev.SessionID, ParentPID: VirtualRootPID, ChildPID: ev.PID,
			Source: ev.Source, SourcePath: ev.SourcePath,
			Offset: ev.Offset, EndOffset: ev.EndOffset,
			Orphan: ev.PPID != VirtualRootPID,
		})
		if ev.PPID != VirtualRootPID {
			sg.Errors = append(sg.Errors, codes.ErrorRecord{
				SourcePath: ev.SourcePath, Source: ev.Source, Code: codes.EOrphanRef,
				Message: fmt.Sprintf("session %s pid %d references missing parent pid %d", ev.SessionID, ev.PID, ev.PPID),
				Offset:  ev.Offset, EndOffset: ev.EndOffset,
			})
		}
	}

	// Pass 3: attach non-creation events; unknown PIDs get placeholder nodes
	// and one virtual-root orphan edge each.
	placeholderEdge := map[string]bool{}
	for _, ev := range events {
		if ev.Type == codes.EvProcessCreate || ev.PID == 0 {
			continue
		}
		sg := ensure(ev.SessionID)
		node, exists := sg.Nodes[ev.PID]
		if !exists {
			node = &Node{SessionID: ev.SessionID, PID: ev.PID, PPID: ev.PPID, Subject: ev.Subject}
			sg.Nodes[ev.PID] = node
			sg.NodeOrder = append(sg.NodeOrder, ev.PID)
			key := fmt.Sprintf("%s|%d", ev.SessionID, ev.PID)
			if !placeholderEdge[key] {
				placeholderEdge[key] = true
				sg.Edges = append(sg.Edges, Edge{
					SessionID: ev.SessionID, ParentPID: VirtualRootPID, ChildPID: ev.PID,
					Source: ev.Source, SourcePath: ev.SourcePath,
					Offset: ev.Offset, EndOffset: ev.EndOffset, Orphan: true,
				})
				sg.Errors = append(sg.Errors, codes.ErrorRecord{
					SourcePath: ev.SourcePath, Source: ev.Source, Code: codes.EOrphanRef,
					Message: fmt.Sprintf("session %s pid %d active without a creation event", ev.SessionID, ev.PID),
					Offset:  ev.Offset, EndOffset: ev.EndOffset,
				})
			}
		}
		node.EventRefs = append(node.EventRefs, refOf(ev))
	}

	sort.Strings(g.Order)
	for _, id := range g.Order {
		sg := g.Sessions[id]
		sort.SliceStable(sg.NodeOrder, func(i, j int) bool { return sg.NodeOrder[i] < sg.NodeOrder[j] })
		sort.SliceStable(sg.Edges, func(i, j int) bool {
			if sg.Edges[i].ParentPID != sg.Edges[j].ParentPID {
				return sg.Edges[i].ParentPID < sg.Edges[j].ParentPID
			}
			if sg.Edges[i].ChildPID != sg.Edges[j].ChildPID {
				return sg.Edges[i].ChildPID < sg.Edges[j].ChildPID
			}
			return sg.Edges[i].Offset < sg.Edges[j].Offset
		})
		sort.SliceStable(sg.Errors, func(i, j int) bool {
			if sg.Errors[i].SourcePath != sg.Errors[j].SourcePath {
				return sg.Errors[i].SourcePath < sg.Errors[j].SourcePath
			}
			return sg.Errors[i].Offset < sg.Errors[j].Offset
		})
	}
	return g
}

func refOf(ev codes.Event) EventRef {
	return EventRef{
		Source: ev.Source, SourcePath: ev.SourcePath,
		Offset: ev.Offset, EndOffset: ev.EndOffset, Type: ev.Type,
	}
}
