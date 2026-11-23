package dnsdetect

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

type WindowProgress struct {
	Index        int64  `json:"index"`
	StartTicks   int64  `json:"start_ticks"`
	EndTicks     int64  `json:"end_ticks"`
	EventCount   int    `json:"event_count"`
	PairCount    int    `json:"pair_count"`
	FindingCount int    `json:"finding_count"`
	EventDigest  string `json:"event_digest"`
}

type Adjudication struct {
	Pairs     []Pair
	Findings  []Finding
	Uncertain []UncertainAssociation
	Windows   []WindowProgress
}

type eventGroup struct {
	window      Window
	txid        uint32
	question    string
	questionKey string
	events      []Event
}

// Adjudicate pairs events by fixed window, transaction id, and canonical
// question name, then evaluates the four poisoning indicators. It is pure and
// keeps merge-phase ordering deterministic regardless of source completion
// order.
func Adjudicate(events []Event, config Config) (*Adjudication, error) {
	config = config.WithDefaults()
	if len(events) == 0 {
		return &Adjudication{}, nil
	}
	normalized := append([]Event(nil), events...)
	for index := range normalized {
		display, key, err := NormalizeName(normalized[index].Question)
		if err != nil {
			return nil, wrapOffsetError(err, normalized[index].SourcePath, normalized[index].Offset)
		}
		normalized[index].Question = display
		normalized[index].QuestionKey = key
		if normalized[index].SourcePriority == 0 && normalized[index].SourceKind != "" {
			normalized[index].SourcePriority = config.Priority(normalized[index].SourceKind)
		}
		if bound := TimeErrorTicks(normalized[index].TimeErrorNS); bound > normalized[index].TimeErrorTicks {
			normalized[index].TimeErrorTicks = bound
		}
	}
	sort.SliceStable(normalized, func(i, j int) bool {
		a := config.WindowForTicks(normalized[i].TimeTicks)
		b := config.WindowForTicks(normalized[j].TimeTicks)
		if a.Index != b.Index {
			return a.Index < b.Index
		}
		if normalized[i].TXID != normalized[j].TXID {
			return normalized[i].TXID < normalized[j].TXID
		}
		if normalized[i].QuestionKey != normalized[j].QuestionKey {
			return normalized[i].QuestionKey < normalized[j].QuestionKey
		}
		if normalized[i].TimeTicks != normalized[j].TimeTicks {
			return normalized[i].TimeTicks < normalized[j].TimeTicks
		}
		if normalized[i].SourcePriority != normalized[j].SourcePriority {
			return normalized[i].SourcePriority > normalized[j].SourcePriority
		}
		if normalized[i].SourcePath != normalized[j].SourcePath {
			return normalized[i].SourcePath < normalized[j].SourcePath
		}
		return normalized[i].Offset < normalized[j].Offset
	})

	if config.StrictTXIDQName {
		seen := make(map[int64]map[uint32]map[string]bool)
		for _, event := range normalized {
			window := config.WindowForTicks(event.TimeTicks)
			names := seen[window.Index]
			if names == nil {
				names = make(map[uint32]map[string]bool)
				seen[window.Index] = names
			}
			questions := names[event.TXID]
			if questions == nil {
				questions = make(map[string]bool)
				names[event.TXID] = questions
			}
			questions[event.QuestionKey] = true
			if len(questions) > 1 {
				return nil, newBusinessError(CodeTXIDQNameConflict, event.SourcePath, event.Offset,
					fmt.Sprintf("txid %d maps to multiple question names in window %d", event.TXID, window.Index), nil)
			}
		}
	}

	groups := make([]*eventGroup, 0)
	var current *eventGroup
	for _, event := range normalized {
		window := config.WindowForTicks(event.TimeTicks)
		if current == nil || current.window.Index != window.Index || current.txid != event.TXID || current.questionKey != event.QuestionKey {
			current = &eventGroup{
				window:      window,
				txid:        event.TXID,
				question:    event.Question,
				questionKey: event.QuestionKey,
			}
			groups = append(groups, current)
		}
		current.events = append(current.events, event)
	}

	result := &Adjudication{
		Pairs:     make([]Pair, 0, len(groups)),
		Findings:  make([]Finding, 0),
		Uncertain: make([]UncertainAssociation, 0),
		Windows:   make([]WindowProgress, 0),
	}
	windowByIndex := make(map[int64]*WindowProgress)
	for _, group := range groups {
		pair := makePair(*group, config)
		result.Pairs = append(result.Pairs, pair)
		findings, uncertain := adjudicateGroup(*group, config)
		result.Findings = append(result.Findings, findings...)
		result.Uncertain = append(result.Uncertain, uncertain...)

		progress, ok := windowByIndex[group.window.Index]
		if !ok {
			progress = &WindowProgress{Index: group.window.Index, StartTicks: group.window.StartTicks, EndTicks: group.window.EndTicks}
			windowByIndex[group.window.Index] = progress
			result.Windows = append(result.Windows, WindowProgress{})
		}
		digest := digestGroupEvents(*group)
		if progress.EventDigest != "" && progress.EventDigest != digest {
			progress.EventDigest = digestOfStrings(progress.EventDigest, digest)
		} else {
			progress.EventDigest = digest
		}
		progress.EventCount += len(group.events)
		progress.PairCount++
		progress.FindingCount += len(findings)
	}
	// Fill structs copied above before pointers were updated.
	for index := range result.Windows {
		progress := windowByIndex[result.Windows[index].Index]
		if progress != nil {
			result.Windows[index] = *progress
		}
	}
	sort.Slice(result.Findings, func(i, j int) bool {
		a, b := result.Findings[i], result.Findings[j]
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		if a.TXID != b.TXID {
			return a.TXID < b.TXID
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if a.Offset != b.Offset {
			return a.Offset < b.Offset
		}
		if a.Question != b.Question {
			return a.Question < b.Question
		}
		if joinedOwners(a.Owners) != joinedOwners(b.Owners) {
			return joinedOwners(a.Owners) < joinedOwners(b.Owners)
		}
		if joinedAddresses(a.Addresses) != joinedAddresses(b.Addresses) {
			return joinedAddresses(a.Addresses) < joinedAddresses(b.Addresses)
		}
		return a.Detail < b.Detail
	})
	sort.Slice(result.Uncertain, func(i, j int) bool {
		a, b := result.Uncertain[i], result.Uncertain[j]
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		if a.Offset != b.Offset {
			return a.Offset < b.Offset
		}
		if a.TXID != b.TXID {
			return a.TXID < b.TXID
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if a.Query.SourcePath != b.Query.SourcePath {
			return a.Query.SourcePath < b.Query.SourcePath
		}
		return a.Query.Offset < b.Query.Offset
	})
	sort.Slice(result.Pairs, func(i, j int) bool {
		a, b := result.Pairs[i], result.Pairs[j]
		if a.Window.Index != b.Window.Index {
			return a.Window.Index < b.Window.Index
		}
		if a.TXID != b.TXID {
			return a.TXID < b.TXID
		}
		return a.QuestionKey < b.QuestionKey
	})
	sort.Slice(result.Windows, func(i, j int) bool { return result.Windows[i].Index < result.Windows[j].Index })
	return result, nil
}

func makePair(group eventGroup, config Config) Pair {
	events := append([]Event(nil), group.events...)
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].SourcePriority != events[j].SourcePriority {
			return events[i].SourcePriority > events[j].SourcePriority
		}
		if events[i].SourcePath != events[j].SourcePath {
			return events[i].SourcePath < events[j].SourcePath
		}
		return events[i].Offset < events[j].Offset
	})
	pair := Pair{
		Window:      group.window,
		TXID:        group.txid,
		Question:    group.question,
		QuestionKey: hex.EncodeToString([]byte(group.questionKey)),
		Primary:     events[0].Ref(),
	}
	for _, event := range events[1:] {
		pair.Evidence = append(pair.Evidence, event.Ref())
	}
	sortEventRefs(pair.Evidence)
	return pair
}

type associationClass int

const (
	associationOutside associationClass = iota
	associationUncertain
	associationDefinite
)

type queryMatch struct {
	query    Event
	class    associationClass
	lower    int64
	upper    int64
	distance uint64
}

func adjudicateGroup(group eventGroup, config Config) ([]Finding, []UncertainAssociation) {
	var queries, responses, caches []Event
	for _, event := range group.events {
		switch event.Role {
		case RoleQuery:
			queries = append(queries, event)
		case RoleResponse:
			responses = append(responses, event)
		case RoleCache:
			caches = append(caches, event)
		}
	}
	if len(queries) == 0 {
		if len(responses) == 0 {
			return nil, nil
		}
		representative := representativeEvent(responses)
		related := refsForEvents(responses)
		return []Finding{{
			Code:       CodeUnsolicitedResponse,
			Window:     group.window,
			TXID:       group.txid,
			Question:   group.question,
			SourcePath: representative.SourcePath,
			Offset:     representative.Offset,
			TimeTicks:  representative.TimeTicks,
			Related:    related,
			Detail:     "response has no query event in the same fixed window",
		}}, nil
	}

	associatedResponses := make([]Event, 0, len(responses))
	unsolicitedResponses := make([]Event, 0)
	uncertain := make([]UncertainAssociation, 0)
	for _, response := range responses {
		match := bestQueryMatch(queries, response, config.AssociationWindowTicks)
		switch match.class {
		case associationDefinite:
			associatedResponses = append(associatedResponses, response)
		case associationUncertain:
			uncertain = append(uncertain, UncertainAssociation{
				Code:       CodeUncertainAssociation,
				Window:     group.window,
				TXID:       group.txid,
				Question:   group.question,
				SourcePath: response.SourcePath,
				Offset:     response.Offset,
				TimeTicks:  response.TimeTicks,
				Query:      match.query.Ref(),
				Response:   response.Ref(),
				LowerTicks: match.lower,
				UpperTicks: match.upper,
				Detail:     "response error interval crosses the configured association boundary; no poisoning verdict is emitted",
			})
		default:
			unsolicitedResponses = append(unsolicitedResponses, response)
		}
	}
	associatedCaches := make([]Event, 0, len(caches))
	for _, cache := range caches {
		match := bestQueryMatch(queries, cache, config.AssociationWindowTicks)
		switch match.class {
		case associationDefinite:
			associatedCaches = append(associatedCaches, cache)
		case associationUncertain:
			uncertain = append(uncertain, UncertainAssociation{
				Code:       CodeUncertainAssociation,
				Window:     group.window,
				TXID:       group.txid,
				Question:   group.question,
				SourcePath: cache.SourcePath,
				Offset:     cache.Offset,
				TimeTicks:  cache.TimeTicks,
				Query:      match.query.Ref(),
				Response:   cache.Ref(),
				LowerTicks: match.lower,
				UpperTicks: match.upper,
				Detail:     "cache observation error interval crosses the configured association boundary; no poisoning verdict is emitted",
			})
		}
	}

	findings := make([]Finding, 0)
	if len(unsolicitedResponses) > 0 {
		representative := representativeEvent(unsolicitedResponses)
		findings = append(findings, Finding{
			Code:       CodeUnsolicitedResponse,
			Window:     group.window,
			TXID:       group.txid,
			Question:   group.question,
			SourcePath: representative.SourcePath,
			Offset:     representative.Offset,
			TimeTicks:  representative.TimeTicks,
			Related:    refsForEvents(unsolicitedResponses),
			Detail:     "response lies outside every query association window",
		})
	}

	type additionalKey struct {
		owner string
	}
	additionalGroups := make(map[additionalKey][]recordWithEvent)
	addressGroups := make(map[string][]recordWithEvent)
	for _, response := range associatedResponses {
		for _, record := range response.Records {
			if record.Type != TypeA && record.Type != TypeAAAA {
				continue
			}
			if record.Section == SectionAdditional && !RelatedName(group.question, record.Name) {
				key := additionalKey{owner: record.Name}
				additionalGroups[key] = append(additionalGroups[key], recordWithEvent{record: record, event: response})
			}
			if record.Section == SectionAnswer {
				addressGroups[record.Name] = append(addressGroups[record.Name], recordWithEvent{record: record, event: response})
			}
		}
	}
	for _, cache := range associatedCaches {
		for _, record := range cache.Records {
			if record.Section == SectionCache && (record.Type == TypeA || record.Type == TypeAAAA) {
				addressGroups[record.Name] = append(addressGroups[record.Name], recordWithEvent{record: record, event: cache})
			}
		}
	}
	for key, records := range additionalGroups {
		representative := representativeRecord(records)
		findings = append(findings, Finding{
			Code:       CodeIrrelevantAdditional,
			Window:     group.window,
			TXID:       group.txid,
			Question:   group.question,
			SourcePath: representative.event.SourcePath,
			Offset:     representative.record.Offset,
			TimeTicks:  representative.event.TimeTicks,
			Related:    refsForRecords(records),
			Owners:     []string{key.owner},
			Addresses:  uniqueSortedAddresses(records),
			Detail:     "additional address record is outside the queried name subtree",
		})
	}

	ttlGroups := make(map[string][]recordWithEvent)
	for _, response := range associatedResponses {
		for _, answer := range response.Records {
			if answer.Section != SectionAnswer || (answer.Type != TypeA && answer.Type != TypeAAAA) {
				continue
			}
			for _, cache := range associatedCaches {
				for _, cached := range cache.Records {
					if cached.Section != SectionCache || cached.Name != answer.Name || cached.Type != answer.Type {
						continue
					}
					if cached.TTL == answer.TTL {
						continue
					}
					key := fmt.Sprintf("%s|%d|%d|%d", answer.Name, answer.Type, answer.TTL, cached.TTL)
					ttlGroups[key] = append(ttlGroups[key], recordWithEvent{record: cached, event: cache})
					ttlGroups[key] = append(ttlGroups[key], recordWithEvent{record: answer, event: response})
				}
			}
		}
	}
	for key, records := range ttlGroups {
		representative := representativeRecord(records)
		parts := strings.Split(key, "|")
		findings = append(findings, Finding{
			Code:       CodeTTLMismatch,
			Window:     group.window,
			TXID:       group.txid,
			Question:   group.question,
			SourcePath: representative.event.SourcePath,
			Offset:     representative.record.Offset,
			TimeTicks:  representative.event.TimeTicks,
			Related:    refsForRecords(records),
			Owners:     []string{parts[0]},
			Detail:     fmt.Sprintf("answer TTL %s differs from cache TTL %s for the same name", parts[2], parts[3]),
		})
	}

	for owner, records := range addressGroups {
		addresses := uniqueSortedAddresses(records)
		if len(addresses) < 2 {
			continue
		}
		representative := representativeRecord(records)
		findings = append(findings, Finding{
			Code:       CodeMultiAddress,
			Window:     group.window,
			TXID:       group.txid,
			Question:   group.question,
			SourcePath: representative.event.SourcePath,
			Offset:     representative.record.Offset,
			TimeTicks:  representative.event.TimeTicks,
			Related:    refsForRecords(records),
			Owners:     []string{owner},
			Addresses:  addresses,
			Detail:     "multiple distinct addresses were observed for the same normalized owner name",
		})
	}
	return findings, uncertain
}

type recordWithEvent struct {
	record ResourceRecord
	event  Event
}

func bestQueryMatch(queries []Event, candidate Event, associationTicks int64) queryMatch {
	best := queryMatch{class: associationOutside}
	bestSet := false
	for _, query := range queries {
		class, lower, upper := association(query, candidate, associationTicks)
		distance := tickDistance(candidate.TimeTicks, query.TimeTicks)
		if class == associationDefinite {
			if !bestSet || best.class != associationDefinite || distance < best.distance {
				best = queryMatch{query: query, class: class, lower: lower, upper: upper, distance: distance}
				bestSet = true
			}
			continue
		}
		if class == associationUncertain && (!bestSet || best.class == associationOutside) {
			best = queryMatch{query: query, class: class, lower: lower, upper: upper, distance: distance}
			bestSet = true
		}
	}
	return best
}

func association(query, candidate Event, associationTicks int64) (associationClass, int64, int64) {
	queryLow, okLow := checkedAdd(query.TimeTicks, -query.TimeErrorTicks)
	queryHigh, okHigh := checkedAdd(query.TimeTicks, query.TimeErrorTicks)
	if !okLow {
		queryLow = query.TimeTicks
	}
	if !okHigh {
		queryHigh = query.TimeTicks
	}
	lower, ok := checkedAdd(queryLow, -associationTicks)
	if !ok {
		lower = queryLow
	}
	upper, ok := checkedAdd(queryHigh, associationTicks)
	if !ok {
		upper = queryHigh
	}
	candidateLow, okLow := checkedAdd(candidate.TimeTicks, -candidate.TimeErrorTicks)
	candidateHigh, okHigh := checkedAdd(candidate.TimeTicks, candidate.TimeErrorTicks)
	if !okLow {
		candidateLow = candidate.TimeTicks
	}
	if !okHigh {
		candidateHigh = candidate.TimeTicks
	}
	if candidateHigh < lower || candidateLow > upper {
		return associationOutside, lower, upper
	}
	if candidateLow >= lower && candidateHigh <= upper {
		return associationDefinite, lower, upper
	}
	return associationUncertain, lower, upper
}

func representativeEvent(events []Event) Event {
	copied := append([]Event(nil), events...)
	sort.SliceStable(copied, func(i, j int) bool {
		if copied[i].SourcePriority != copied[j].SourcePriority {
			return copied[i].SourcePriority > copied[j].SourcePriority
		}
		if copied[i].SourcePath != copied[j].SourcePath {
			return copied[i].SourcePath < copied[j].SourcePath
		}
		return copied[i].Offset < copied[j].Offset
	})
	return copied[0]
}

func representativeRecord(records []recordWithEvent) recordWithEvent {
	copied := append([]recordWithEvent(nil), records...)
	sort.SliceStable(copied, func(i, j int) bool {
		if copied[i].event.SourcePriority != copied[j].event.SourcePriority {
			return copied[i].event.SourcePriority > copied[j].event.SourcePriority
		}
		if copied[i].event.SourcePath != copied[j].event.SourcePath {
			return copied[i].event.SourcePath < copied[j].event.SourcePath
		}
		if copied[i].record.Offset != copied[j].record.Offset {
			return copied[i].record.Offset < copied[j].record.Offset
		}
		return copied[i].record.Address < copied[j].record.Address
	})
	return copied[0]
}

func refsForEvents(events []Event) []EventRef {
	refs := make([]EventRef, 0, len(events))
	seen := make(map[string]bool)
	for _, event := range events {
		ref := event.Ref()
		key := fmt.Sprintf("%s|%d|%s", ref.SourcePath, ref.Offset, ref.Role)
		if !seen[key] {
			refs = append(refs, ref)
			seen[key] = true
		}
	}
	sortEventRefs(refs)
	return refs
}

func refsForRecords(records []recordWithEvent) []EventRef {
	events := make([]Event, 0, len(records))
	for _, record := range records {
		events = append(events, record.event)
	}
	return refsForEvents(events)
}

func uniqueSortedAddresses(records []recordWithEvent) []string {
	set := make(map[string]bool)
	for _, record := range records {
		if record.record.Address != "" {
			set[record.record.Address] = true
		}
	}
	addresses := make([]string, 0, len(set))
	for address := range set {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	return addresses
}

func joinedOwners(values []string) string { return strings.Join(values, "\x00") }

func joinedAddresses(values []string) string { return strings.Join(values, "\x00") }

func digestGroupEvents(group eventGroup) string {
	hash := sha256.New()
	for _, event := range group.events {
		fmt.Fprintf(hash, "%s|%d|%d|%d|%s|%s|%s|", event.SourcePath, event.Offset, event.TXID, event.TimeTicks, event.QuestionKey, event.Role, event.SourceKind)
		for _, record := range event.Records {
			fmt.Fprintf(hash, "%s|%d|%d|%s|%s;", record.Name, record.Type, record.TTL, record.Address, record.Section)
		}
		hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func digestOfStrings(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		hash.Write([]byte(value))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
