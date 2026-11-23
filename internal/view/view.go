// Package view provides read-only interactive and one-shot projections over a
// rendered forensic report: session filtering and multi-format source
// comparison for a single event.
package view

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strings"

	"forensicworkbench/internal/report"
)

// Match groups timeline events that describe the same logical activity across
// formats. The correlation key is session id + pid + event type, which is
// format-independent.
type Match struct {
	Key     string                 `json:"key"`
	Sources []report.TimelineEvent `json:"sources"`
}

// FilterBySession returns timeline events belonging to one session, preserving
// the report's stable ordering.
func FilterBySession(r *report.Report, session string) []report.TimelineEvent {
	var out []report.TimelineEvent
	for _, ev := range r.Timeline {
		if ev.SessionID == session {
			out = append(out, ev)
		}
	}
	return out
}

// Sessions lists the distinct session ids in sorted order.
func Sessions(r *report.Report) []string {
	set := map[string]bool{}
	for _, ev := range r.Timeline {
		set[ev.SessionID] = true
	}
	var out []string
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func matchKey(ev report.TimelineEvent) string {
	return ev.SessionID + "|" + fmt.Sprintf("%d", ev.PID) + "|" + string(ev.Type)
}

// MultiFormatSources returns every format representation sharing one event's
// session+pid+type key, enabling side-by-side source comparison.
func MultiFormatSources(r *report.Report, eventID string) (*Match, error) {
	var anchor *report.TimelineEvent
	for i := range r.Timeline {
		if r.Timeline[i].EventID == eventID {
			anchor = &r.Timeline[i]
			break
		}
	}
	if anchor == nil {
		return nil, fmt.Errorf("event %q not found", eventID)
	}
	key := matchKey(*anchor)
	m := &Match{Key: key}
	for _, ev := range r.Timeline {
		if matchKey(ev) == key {
			m.Sources = append(m.Sources, ev)
		}
	}
	sort.SliceStable(m.Sources, func(i, j int) bool {
		if m.Sources[i].Source != m.Sources[j].Source {
			return m.Sources[i].Source < m.Sources[j].Source
		}
		return m.Sources[i].Offset < m.Sources[j].Offset
	})
	return m, nil
}

// RunREPL serves an interactive prompt. It is deterministic given the report
// and reads commands from in, writing text projections to out.
func RunREPL(r *report.Report, in io.Reader, out io.Writer) error {
	w := bufio.NewWriter(out)
	defer w.Flush()
	fmt.Fprintf(w, "forensic workbench view (snapshot v%d). Commands: sessions, session <id>, event <id>, edges, help, quit\n", r.SnapshotVersion)
	scanner := bufio.NewScanner(in)
	for {
		fmt.Fprint(w, "fw> ")
		if !scanner.Scan() {
			return nil
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		switch parts[0] {
		case "quit", "exit":
			return nil
		case "help":
			fmt.Fprintln(w, "sessions | session <id> | event <id> | edges | quit")
		case "sessions":
			for _, s := range Sessions(r) {
				fmt.Fprintln(w, s)
			}
		case "session":
			if len(parts) < 2 {
				fmt.Fprintln(w, "usage: session <id>")
				continue
			}
			for _, ev := range FilterBySession(r, parts[1]) {
				fmt.Fprintf(w, "%s  t=%v  %s  pid=%d  %s  off=%d  [%s]\n",
					ev.EventID, nanoStr(ev.TimeUnixNano), ev.Type, ev.PID, ev.Source, ev.Offset, ev.Code)
			}
		case "event":
			if len(parts) < 2 {
				fmt.Fprintln(w, "usage: event <id>")
				continue
			}
			m, err := MultiFormatSources(r, parts[1])
			if err != nil {
				fmt.Fprintln(w, "error:", err)
				continue
			}
			fmt.Fprintf(w, "correlation key %s (%d format sources):\n", m.Key, len(m.Sources))
			for _, ev := range m.Sources {
				fmt.Fprintf(w, "  %-6s %-10s off=%-4d subj=%s obj=%s path=%s\n",
					ev.Source, ev.FormatVersion, ev.Offset, ev.Subject, ev.Object, ev.SourcePath)
			}
		case "edges":
			for _, e := range r.LineageEdges {
				tag := ""
				if e.Orphan {
					tag = " ORPHAN"
				}
				fmt.Fprintf(w, "%s: %d -> %d via %s@%d%s\n", e.SessionID, e.ParentPID, e.ChildPID, e.Source, e.Offset, tag)
			}
		default:
			fmt.Fprintln(w, "unknown command; try help")
		}
	}
}

func nanoStr(p *int64) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *p)
}
