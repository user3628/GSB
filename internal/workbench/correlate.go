package workbench

import (
	"fmt"
	"sort"
)

type CorrelateConfig struct {
	WindowTicks     uint64
	FlipWindowTicks uint64
}

type eventGroupKey struct {
	TxID     uint32
	NameWire string
}

type eventGroup struct {
	TxID      uint32
	Name      string
	NameWire  string
	Queries   []Event
	Responses []Event
	Caches    []Event
}

func Correlate(events []Event, cfg CorrelateConfig) ([]TransactionReport, []Issue) {
	sorted := append([]Event(nil), events...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Tick != sorted[j].Tick {
			return sorted[i].Tick < sorted[j].Tick
		}
		if sorted[i].Source != sorted[j].Source {
			return sorted[i].Source < sorted[j].Source
		}
		if sorted[i].Offset != sorted[j].Offset {
			return sorted[i].Offset < sorted[j].Offset
		}
		return sorted[i].Kind < sorted[j].Kind
	})

	findings := detectNameConflicts(sorted)
	groups := buildGroups(sorted)

	nameFindings := detectNameLevelFindings(sorted, cfg)
	findings = append(findings, nameFindings...)

	for _, group := range groups {
		findings = append(findings, detectTransactionFindings(group)...)
	}
	SortIssues(findings)

	transactions := make([]TransactionReport, 0, len(groups))
	for _, group := range groups {
		tx := TransactionReport{
			TxID:     group.TxID,
			Name:     group.Name,
			NameWire: group.NameWire,
		}
		for _, event := range group.Queries {
			tx.Queries = append(tx.Queries, event.Reference())
		}
		for _, event := range group.Responses {
			tx.Responses = append(tx.Responses, event.Reference())
		}
		for _, event := range group.Caches {
			tx.Caches = append(tx.Caches, event.Reference())
		}
		SortEventRefs(tx.Queries)
		SortEventRefs(tx.Responses)
		SortEventRefs(tx.Caches)
		for _, finding := range findings {
			if finding.TxID == tx.TxID && finding.Name == tx.Name {
				tx.Verdicts = append(tx.Verdicts, finding)
			}
		}
		SortIssues(tx.Verdicts)
		transactions = append(transactions, tx)
	}
	SortTransactions(transactions)
	return transactions, findings
}

func buildGroups(events []Event) []eventGroup {
	byKey := make(map[eventGroupKey]*eventGroup)
	for _, event := range events {
		key := eventGroupKey{TxID: event.TxID, NameWire: event.NameWire}
		group := byKey[key]
		if group == nil {
			group = &eventGroup{TxID: event.TxID, Name: event.Name, NameWire: event.NameWire}
			byKey[key] = group
		}
		switch event.Kind {
		case KindQuery:
			group.Queries = append(group.Queries, event)
		case KindResponse:
			group.Responses = append(group.Responses, event)
		case KindCache:
			group.Caches = append(group.Caches, event)
		}
	}
	groups := make([]eventGroup, 0, len(byKey))
	for _, group := range byKey {
		groups = append(groups, *group)
	}
	return groups
}

func detectNameConflicts(events []Event) []Issue {
	byTxID := make(map[uint32][]Event)
	for _, event := range events {
		byTxID[event.TxID] = append(byTxID[event.TxID], event)
	}
	ids := make([]uint32, 0, len(byTxID))
	for id := range byTxID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	findings := make([]Issue, 0)
	for _, id := range ids {
		list := byTxID[id]
		for i := 0; i < len(list); i++ {
			for j := i + 1; j < len(list); j++ {
				if list[i].Source == list[j].Source || list[i].NameWire == list[j].NameWire {
					continue
				}
				issue := Issue{
					Code:          CodeNameConflict,
					Source:        list[i].Source,
					Offset:        list[i].Offset,
					Time:          list[i].Time,
					TxID:          id,
					Name:          list[i].Name,
					RelatedSource: list[j].Source,
					RelatedOffset: list[j].Offset,
					Detail:        fmt.Sprintf("txid %d maps to %q in %s and %q in %s", id, list[i].Name, list[i].Source, list[j].Name, list[j].Source),
				}
				findings = append(findings, issue)
				goto nextTxID
			}
		}
	nextTxID:
	}
	return findings
}

type addressObservation struct {
	Event  Event
	Record Record
}

func detectNameLevelFindings(events []Event, cfg CorrelateConfig) []Issue {
	byName := make(map[string][]Event)
	for _, event := range events {
		if event.Kind != KindResponse && event.Kind != KindCache {
			continue
		}
		byName[event.NameWire] = append(byName[event.NameWire], event)
	}
	nameWires := make([]string, 0, len(byName))
	for nameWire := range byName {
		nameWires = append(nameWires, nameWire)
	}
	sort.Strings(nameWires)
	findings := make([]Issue, 0)
	for _, nameWire := range nameWires {
		list := byName[nameWire]
		responses := make([]Event, 0)
		caches := make([]Event, 0)
		for _, event := range list {
			if event.Kind == KindResponse {
				responses = append(responses, event)
			} else if event.Kind == KindCache {
				caches = append(caches, event)
			}
		}
		sortEvents(responses)
		sortEvents(caches)
		if issue, ok := firstTTLMismatch(responses, caches, cfg.WindowTicks); ok {
			findings = append(findings, issue)
		}
		observations := make([]addressObservation, 0)
		for _, event := range append(append([]Event{}, responses...), caches...) {
			for _, record := range event.Answers {
				if record.Name == event.Name && IsAddressType(record.Type) && record.Address != "" {
					observations = append(observations, addressObservation{Event: event, Record: record})
					break
				}
			}
		}
		sort.SliceStable(observations, func(i, j int) bool {
			a, b := observations[i], observations[j]
			if a.Event.Tick != b.Event.Tick {
				return a.Event.Tick < b.Event.Tick
			}
			if a.Event.Source != b.Event.Source {
				return a.Event.Source < b.Event.Source
			}
			return a.Record.Offset < b.Record.Offset
		})
		for i := 1; i < len(observations); i++ {
			prev, current := observations[i-1], observations[i]
			if prev.Record.Address == current.Record.Address {
				continue
			}
			var delta uint64
			if current.Event.Tick >= prev.Event.Tick {
				delta = current.Event.Tick - prev.Event.Tick
			} else {
				continue
			}
			if cfg.FlipWindowTicks > 0 && delta > cfg.FlipWindowTicks {
				continue
			}
			findings = append(findings, Issue{
				Code:   CodeFlipFlop,
				Source: current.Event.Source,
				Offset: current.Record.Offset,
				Time:   current.Event.Time,
				TxID:   current.Event.TxID,
				Name:   current.Event.Name,
				Detail: fmt.Sprintf("answer address changed from %s to %s within %d sample ticks", prev.Record.Address, current.Record.Address, delta),
			})
			break
		}
	}
	return findings
}

func firstTTLMismatch(responses, caches []Event, window uint64) (Issue, bool) {
	for _, cache := range caches {
		var matched *Event
		for i := len(responses) - 1; i >= 0; i-- {
			response := responses[i]
			if response.Tick > cache.Tick {
				continue
			}
			delta := cache.Tick - response.Tick
			if window > 0 && delta > window {
				break
			}
			matched = &responses[i]
			break
		}
		if matched == nil {
			continue
		}
		expected := make(map[string]uint32)
		for _, record := range matched.Answers {
			if record.Name == matched.Name && IsAddressType(record.Type) && record.Address != "" {
				expected[record.Address] = record.TTL
			}
		}
		for _, record := range cache.Answers {
			if record.Name != cache.Name || !IsAddressType(record.Type) || record.Address == "" {
				continue
			}
			expectedTTL, ok := expected[record.Address]
			if !ok || expectedTTL == record.TTL {
				continue
			}
			return Issue{
				Code:   CodeTTLMismatch,
				Source: cache.Source,
				Offset: record.Offset,
				Time:   cache.Time,
				TxID:   cache.TxID,
				Name:   cache.Name,
				Detail: fmt.Sprintf("address %s TTL expected %d from response %s:%d, observed %d", record.Address, expectedTTL, matched.Source, matched.Offset, record.TTL),
			}, true
		}
	}
	return Issue{}, false
}

func detectTransactionFindings(group eventGroup) []Issue {
	findings := make([]Issue, 0)
	sortEvents(group.Responses)
	for _, response := range group.Responses {
		if len(group.Queries) == 0 {
			findings = append(findings, Issue{
				Code:   CodeUnsolicited,
				Source: response.Source,
				Offset: response.Offset,
				Time:   response.Time,
				TxID:   response.TxID,
				Name:   response.Name,
				Detail: "response has no matching query for the same txid and question name",
			})
			break
		}
	}
	for _, response := range group.Responses {
		for _, record := range response.Additionals {
			if !IsAddressType(record.Type) || record.Address == "" || record.Name == response.Name {
				continue
			}
			findings = append(findings, Issue{
				Code:   CodeAdditionalInject,
				Source: response.Source,
				Offset: record.Offset,
				Time:   response.Time,
				TxID:   response.TxID,
				Name:   response.Name,
				Detail: fmt.Sprintf("additional section address %s belongs to unrelated name %q", record.Address, record.Name),
			})
			break
		}
	}
	return findings
}

func sortEvents(events []Event) {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Tick != events[j].Tick {
			return events[i].Tick < events[j].Tick
		}
		if events[i].Source != events[j].Source {
			return events[i].Source < events[j].Source
		}
		return events[i].Offset < events[j].Offset
	})
}
