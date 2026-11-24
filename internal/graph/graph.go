// Package graph builds the process parent/child and inherited-handle graph
// from declarations and events, attributes unauthorized writes to the
// handle-providing ancestor, and renders order-independent provenance traces.
package graph

import (
	"sort"

	"privescworkbench/internal/domain"
)

type edgeKind string

const (
	edgeParent edgeKind = "parent"
	edgeHandle edgeKind = "handle"
	edgeRoot   edgeKind = "virtual_root"
)

type edge struct {
	from   int64
	to     int64
	kind   edgeKind
	detail string
}

// Graph is the normalized process/handle relationship graph.
type Graph struct {
	procs  map[int64]domain.ProcessDecl
	edges  []edge
	orphan map[int64]bool
}

// Build constructs the graph. Outputs are canonicalized (sorted) so that the
// input merge order of processes and events cannot change provenance results.
func Build(procs []domain.ProcessDecl, events []domain.Event) *Graph {
	g := &Graph{
		procs:  make(map[int64]domain.ProcessDecl, len(procs)),
		orphan: map[int64]bool{},
	}
	for _, p := range procs {
		g.procs[p.PID] = p
	}
	edgeSet := map[edge]bool{}
	addEdge := func(e edge) { edgeSet[e] = true }

	// parent/child edges from declarations; unknown parents become orphans
	// attached to the virtual root.
	for _, p := range procs {
		if _, ok := g.procs[p.ParentPID]; ok {
			addEdge(edge{p.ParentPID, p.PID, edgeParent, "declared parent"})
		} else {
			g.orphan[p.PID] = true
			addEdge(edge{domain.VirtualRootPID, p.PID, edgeRoot, "missing parent create event"})
		}
		if p.InheritedFrom != 0 {
			if _, ok := g.procs[p.InheritedFrom]; ok {
				addEdge(edge{p.InheritedFrom, p.PID, edgeHandle, "inherited handle"})
			} else {
				g.orphan[p.PID] = true
				addEdge(edge{domain.VirtualRootPID, p.PID, edgeRoot, "missing handle provider"})
			}
		}
	}

	// handle edges inferred from events: the target process is the
	// antecedent that supplied a handle later held/used by the subject.
	for _, ev := range events {
		if _, ok := g.procs[ev.TargetPID]; !ok {
			continue
		}
		if ev.Op == "HANDLE_DUP" || ev.Op == "PROCESS_INJECT" {
			addEdge(edge{ev.TargetPID, ev.SubjectPID, edgeHandle, "event " + ev.Op})
		}
	}

	for e := range edgeSet {
		g.edges = append(g.edges, e)
	}
	sort.Slice(g.edges, func(i, j int) bool {
		a, b := g.edges[i], g.edges[j]
		if a.from != b.from {
			return a.from < b.from
		}
		if a.to != b.to {
			return a.to < b.to
		}
		return a.kind < b.kind
	})
	return g
}

// Orphans returns orphan PIDs in ascending order.
func (g *Graph) Orphans() []int64 {
	out := make([]int64, 0, len(g.orphan))
	for pid := range g.orphan {
		out = append(out, pid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Trace renders the provenance chain of a process hop-by-hop back to the
// virtual root. Cycles are guarded; edge order is canonical.
func (g *Graph) Trace(pid int64) []domain.TraceHop {
	hops := make([]domain.TraceHop, 0)
	visited := map[int64]bool{}
	cur := pid
	for cur != domain.VirtualRootPID {
		if visited[cur] {
			break
		}
		visited[cur] = true
		in := g.inEdges(cur)
		if len(in) == 0 {
			hops = append(hops, domain.TraceHop{
				FromPID: domain.VirtualRootPID, ToPID: cur,
				Edge: string(edgeRoot), Detail: "no incoming relation",
			})
			break
		}
		// prefer handle-providing ancestor for attribution, then parent, then
		// root; skip any edge leading back into an already visited node
		chosen := pickEdgeFiltered(in, visited)
		if chosen == nil {
			hops = append(hops, domain.TraceHop{
				FromPID: domain.VirtualRootPID, ToPID: cur,
				Edge: string(edgeRoot), Detail: "cycle broken to virtual root",
			})
			break
		}
		hops = append(hops, domain.TraceHop{
			FromPID: chosen.from, ToPID: chosen.to,
			Edge: string(chosen.kind), Detail: chosen.detail,
		})
		cur = chosen.from
	}
	// reverse so the chain reads root -> process
	for i, j := 0, len(hops)-1; i < j; i, j = i+1, j-1 {
		hops[i], hops[j] = hops[j], hops[i]
	}
	return hops
}

func (g *Graph) inEdges(pid int64) []edge {
	out := make([]edge, 0)
	for _, e := range g.edges {
		if e.to == pid {
			out = append(out, e)
		}
	}
	return out
}

func pickEdge(in []edge) edge {
	if e := pickEdgeFiltered(in, map[int64]bool{}); e != nil {
		return *e
	}
	return in[0]
}

func pickEdgeFiltered(in []edge, blocked map[int64]bool) *edge {
	var handle, parent, root *edge
	for i := range in {
		if blocked[in[i].from] {
			continue
		}
		switch in[i].kind {
		case edgeHandle:
			handle = &in[i]
		case edgeParent:
			parent = &in[i]
		case edgeRoot:
			root = &in[i]
		}
	}
	if handle != nil {
		return handle
	}
	if parent != nil {
		return parent
	}
	if root != nil {
		return root
	}
	return nil
}

// AttributingAncestor walks handle/parent edges to find the ancestor that
// supplied an inheritable handle used by an unauthorized write event.
func (g *Graph) AttributingAncestor(pid int64) int64 {
	visited := map[int64]bool{}
	cur := pid
	for {
		if visited[cur] || cur == domain.VirtualRootPID {
			return cur
		}
		visited[cur] = true
		in := g.inEdges(cur)
		if len(in) == 0 {
			return domain.VirtualRootPID
		}
		cur = pickEdge(in).from
	}
}
