package store

import (
	"fmt"
	"math"
	"sort"

	"meshmigrate/internal/model"
)

func BuildDerived(snapshot *model.Snapshot, affected []string, rebuiltAtMs int64) (*DerivedData, error) {
	d := &DerivedData{SnapshotVersion: snapshot.SnapshotVersion, RebuiltAtMs: rebuiltAtMs}
	d.Affected = append([]string(nil), affected...)
	sort.Strings(d.Affected)
	type windowKey struct {
		service string
		start   int64
		end     int64
		window  int64
	}
	type windowAgg struct {
		limit uint64
		ids   []string
	}
	type serviceAgg struct {
		policies  int
		routes    int
		windows   int
		totalRate uint64
		circuit   int64
		ids       []string
	}
	windows := make(map[windowKey]*windowAgg)
	services := make(map[string]*serviceAgg)
	for _, p := range snapshot.Policies {
		key := p.Match.Method + "\x1f" + p.Match.PathPrefix
		d.Routes = append(d.Routes, RouteRow{
			VirtualServiceID: p.VirtualServiceID,
			RouteKey:         key,
			PolicyID:         p.PolicyID,
			Method:           p.Match.Method,
			PathPrefix:       p.Match.PathPrefix,
			Headers:          p.Match.Headers,
		})
		wk := windowKey{p.VirtualServiceID, p.RateWindowStartMs, p.RateWindowEndMs, int64(p.RateWindowMs)}
		wa := windows[wk]
		if wa == nil {
			wa = &windowAgg{}
			windows[wk] = wa
		}
		if wa.limit > math.MaxUint64-p.RateLimitBps {
			return nil, fmt.Errorf("aggregated rate limit overflows uint64 for service %s", p.VirtualServiceID)
		}
		wa.limit += p.RateLimitBps
		wa.ids = append(wa.ids, p.PolicyID)
		sa := services[p.VirtualServiceID]
		if sa == nil {
			sa = &serviceAgg{}
			services[p.VirtualServiceID] = sa
		}
		sa.policies++
		sa.routes++
		if sa.totalRate > math.MaxUint64-p.RateLimitBps {
			return nil, fmt.Errorf("total rate limit overflows uint64 for service %s", p.VirtualServiceID)
		}
		sa.totalRate += p.RateLimitBps
		if int64(p.CircuitWindowMs) > sa.circuit {
			sa.circuit = int64(p.CircuitWindowMs)
		}
		sa.ids = append(sa.ids, p.PolicyID)
	}
	for k, wa := range windows {
		sort.Strings(wa.ids)
		d.RateWindows = append(d.RateWindows, RateWindowRow{
			VirtualServiceID: k.service,
			WindowStartMs:    k.start,
			WindowEndMs:      k.end,
			WindowMs:         k.window,
			LimitBps:         wa.limit,
			PolicyCount:      len(wa.ids),
			PolicyIDs:        wa.ids,
		})
		if services[k.service] != nil {
			services[k.service].windows++
		}
	}
	for service, sa := range services {
		sort.Strings(sa.ids)
		d.Circuits = append(d.Circuits, CircuitRow{
			VirtualServiceID: service,
			State:            "closed",
			WindowMs:         sa.circuit,
			PolicyIDs:        sa.ids,
		})
		d.Summaries = append(d.Summaries, SummaryRow{
			VirtualServiceID: service,
			PolicyCount:      sa.policies,
			RouteCount:       sa.routes,
			RateWindowCount:  sa.windows,
			TotalRateBps:     sa.totalRate,
			CircuitState:     "closed",
			PolicyIDs:        sa.ids,
		})
	}
	sort.Slice(d.Routes, func(i, j int) bool {
		if d.Routes[i].VirtualServiceID != d.Routes[j].VirtualServiceID {
			return d.Routes[i].VirtualServiceID < d.Routes[j].VirtualServiceID
		}
		if d.Routes[i].RouteKey != d.Routes[j].RouteKey {
			return d.Routes[i].RouteKey < d.Routes[j].RouteKey
		}
		return d.Routes[i].PolicyID < d.Routes[j].PolicyID
	})
	sort.Slice(d.RateWindows, func(i, j int) bool {
		if d.RateWindows[i].VirtualServiceID != d.RateWindows[j].VirtualServiceID {
			return d.RateWindows[i].VirtualServiceID < d.RateWindows[j].VirtualServiceID
		}
		if d.RateWindows[i].WindowStartMs != d.RateWindows[j].WindowStartMs {
			return d.RateWindows[i].WindowStartMs < d.RateWindows[j].WindowStartMs
		}
		return d.RateWindows[i].WindowMs < d.RateWindows[j].WindowMs
	})
	sort.Slice(d.Circuits, func(i, j int) bool { return d.Circuits[i].VirtualServiceID < d.Circuits[j].VirtualServiceID })
	sort.Slice(d.Summaries, func(i, j int) bool { return d.Summaries[i].VirtualServiceID < d.Summaries[j].VirtualServiceID })
	return d, nil
}
