package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/errs"
)

type CachePayload struct {
	RCode       dnsmsg.RCode `json:"rcode"`
	Answers     []dnsmsg.RR  `json:"answers,omitempty"`
	Authorities []dnsmsg.RR  `json:"authorities,omitempty"`
	Additionals []dnsmsg.RR  `json:"additionals,omitempty"`
}

type CacheWrite struct {
	QueryID     string
	QName       string
	QType       dnsmsg.Type
	SourceType  dnsmsg.Type
	Zone        string
	ZoneVersion int64
	TTLMillis   int64
	// ExpectedZone/ExpectedZoneVersion are used by the resolver to prevent a
	// response fetched before a zone commit from being inserted after that commit.
	ExpectedZone        string
	ExpectedZoneVersion int64
	ExpectedZoneEpoch   int64
	RequireZoneStable   bool
	RequireEpochStable  bool
	Payload             CachePayload
}

type CacheWriteResult struct {
	CacheID    int64  `json:"cache_id"`
	CacheKey   string `json:"cache_key"`
	Status     string `json:"status"`
	TTLMillis  int64  `json:"ttl_ms"`
	WrittenMS  int64  `json:"written_ms"`
	Idempotent bool   `json:"idempotent"`
}

type CacheEntry struct {
	CacheID     int64        `json:"cache_id"`
	QName       string       `json:"qname"`
	QType       dnsmsg.Type  `json:"qtype"`
	Status      string       `json:"status"`
	SourceType  dnsmsg.Type  `json:"source_type"`
	Zone        string       `json:"zone,omitempty"`
	ZoneVersion int64        `json:"zone_version,omitempty"`
	WrittenMS   int64        `json:"written_ms"`
	TTLMillis   int64        `json:"ttl_ms"`
	RemainingMS int64        `json:"remaining_ms"`
	Payload     CachePayload `json:"payload"`
}

type CacheLookup struct {
	Hit    bool        `json:"hit"`
	Entry  *CacheEntry `json:"entry,omitempty"`
	Reason string      `json:"reason,omitempty"`
}

type RRset struct {
	Owner   string         `json:"owner"`
	Type    dnsmsg.Type    `json:"type"`
	TTL     uint32         `json:"ttl"`
	Records []dnsmsg.RData `json:"records"`
}

type RRsetChange struct {
	Owner   string         `json:"owner"`
	Type    dnsmsg.Type    `json:"type"`
	TTL     uint32         `json:"ttl,omitempty"`
	Records []dnsmsg.RData `json:"records,omitempty"`
	Delete  bool           `json:"delete,omitempty"`
}

type ZoneSnapshot struct {
	Zone          string  `json:"zone"`
	Version       int64   `json:"version"`
	GeneratedAtMS int64   `json:"generated_at_ms"`
	Checksum      string  `json:"checksum"`
	RRsets        []RRset `json:"rrsets"`
}

type ZoneDelta struct {
	DeltaID       string        `json:"delta_id,omitempty"`
	Zone          string        `json:"zone"`
	BaseVersion   int64         `json:"base_version"`
	Version       int64         `json:"version"`
	GeneratedAtMS int64         `json:"generated_at_ms"`
	Checksum      string        `json:"checksum"`
	Changes       []RRsetChange `json:"changes"`
}

type ZoneCommitResult struct {
	BatchID                 string `json:"batch_id"`
	Zone                    string `json:"zone"`
	Kind                    string `json:"kind"`
	PreviousVersion         int64  `json:"previous_version"`
	Version                 int64  `json:"version"`
	ChangedRRSets           int    `json:"changed_rrsets"`
	InvalidatedCacheEntries int64  `json:"invalidated_cache_entries"`
	DerivedRows             int    `json:"derived_rows"`
	Idempotent              bool   `json:"idempotent"`
}

type ZoneState struct {
	Zone          string  `json:"zone"`
	Version       int64   `json:"version"`
	GeneratedAtMS int64   `json:"generated_at_ms"`
	Checksum      string  `json:"checksum"`
	UpdatedAtMS   int64   `json:"updated_at_ms"`
	RRsets        []RRset `json:"rrsets"`
}

type DerivedRow struct {
	Kind        string      `json:"kind"`
	Key         string      `json:"key"`
	Owner       string      `json:"owner"`
	Type        dnsmsg.Type `json:"type"`
	SourceOwner string      `json:"source_owner"`
	SourceType  dnsmsg.Type `json:"source_type"`
	Refs        string      `json:"refs"`
	Version     int64       `json:"version"`
}

type RecoveryReport struct {
	StartedAtMS            int64    `json:"started_at_ms"`
	FinishedAtMS           int64    `json:"finished_at_ms"`
	PendingIntents         int      `json:"pending_intents"`
	CacheEntriesRolledBack int      `json:"cache_entries_rolled_back"`
	ZoneBatchesAborted     int      `json:"zone_batches_aborted"`
	ZoneBatchesReplayed    int      `json:"zone_batches_replayed"`
	Details                []string `json:"details"`
	FactsOnlyNote          string   `json:"facts_only_note"`
	Limitations            []string `json:"limitations"`
}

type CacheStats struct {
	Entries        int64 `json:"entries"`
	Positive       int64 `json:"positive"`
	NXDomain       int64 `json:"nxdomain"`
	NODATA         int64 `json:"nodata"`
	Expired        int64 `json:"expired"`
	DependencyRows int64 `json:"dependency_rows"`
	Zones          int64 `json:"zones"`
	PendingIntents int64 `json:"pending_intents"`
}

func canonicalRecords(rs RRset) []string {
	out := make([]string, 0, len(rs.Records))
	for _, data := range rs.Records {
		rr := dnsmsg.RR{Name: rs.Owner, Type: rs.Type, TTL: rs.TTL, Data: data}
		out = append(out, rr.Canonical())
	}
	sort.Strings(out)
	return out
}

func normalizedRRsets(in []RRset, zone string) []RRset {
	out := make([]RRset, 0, len(in))
	for _, rs := range in {
		rs.Owner = dnsmsg.CanonicalName(rs.Owner)
		rs.Records = append([]dnsmsg.RData(nil), rs.Records...)
		for i := range rs.Records {
			rs.Records[i].Texts = append([]string(nil), rs.Records[i].Texts...)
			rs.Records[i].Raw = append([]byte(nil), rs.Records[i].Raw...)
			if rs.Records[i].SOA != nil {
				soa := *rs.Records[i].SOA
				rs.Records[i].SOA = &soa
			}
		}
		out = append(out, rs)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Owner == out[j].Owner {
			return out[i].Type < out[j].Type
		}
		return out[i].Owner < out[j].Owner
	})
	return out
}

func normalizedChanges(in []RRsetChange) []RRsetChange {
	out := make([]RRsetChange, 0, len(in))
	for _, ch := range in {
		ch.Owner = dnsmsg.CanonicalName(ch.Owner)
		ch.Records = append([]dnsmsg.RData(nil), ch.Records...)
		for i := range ch.Records {
			ch.Records[i].Texts = append([]string(nil), ch.Records[i].Texts...)
			ch.Records[i].Raw = append([]byte(nil), ch.Records[i].Raw...)
			if ch.Records[i].SOA != nil {
				soa := *ch.Records[i].SOA
				ch.Records[i].SOA = &soa
			}
		}
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Owner == out[j].Owner {
			return out[i].Type < out[j].Type
		}
		return out[i].Owner < out[j].Owner
	})
	return out
}

func ComputeSnapshotChecksum(s ZoneSnapshot) string {
	h := sha256.New()
	fmt.Fprintf(h, "snapshot\x00%s\x00%d\x00%d\x00", dnsmsg.CanonicalName(s.Zone), s.Version, s.GeneratedAtMS)
	for _, rs := range normalizedRRsets(s.RRsets, s.Zone) {
		fmt.Fprintf(h, "rrset\x00%s\x00%d\x00%d\x00", rs.Owner, rs.Type, rs.TTL)
		for _, rec := range canonicalRecords(rs) {
			fmt.Fprintf(h, "r\x00%s\x00", rec)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func ComputeDeltaChecksum(d ZoneDelta) string {
	h := sha256.New()
	fmt.Fprintf(h, "delta\x00%s\x00%d\x00%d\x00%d\x00", dnsmsg.CanonicalName(d.Zone), d.BaseVersion, d.Version, d.GeneratedAtMS)
	for _, ch := range normalizedChanges(d.Changes) {
		fmt.Fprintf(h, "change\x00%s\x00%d\x00%d\x00%t\x00", ch.Owner, ch.Type, ch.TTL, ch.Delete)
		if !ch.Delete {
			rs := RRset{Owner: ch.Owner, Type: ch.Type, TTL: ch.TTL, Records: ch.Records}
			for _, rec := range canonicalRecords(rs) {
				fmt.Fprintf(h, "r\x00%s\x00", rec)
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validateRRset(zone string, rs RRset) error {
	zone = dnsmsg.CanonicalName(zone)
	owner := dnsmsg.CanonicalName(rs.Owner)
	if !dnsmsg.NameWithin(owner, zone) {
		return errs.New(errs.EInvalidRequest, "owner %s is outside zone %s", owner, zone)
	}
	if !dnsmsg.SupportedType(rs.Type) {
		return errs.New(errs.EUnsupportedType, "RRset type %s is not supported", rs.Type)
	}
	if len(rs.Records) == 0 {
		return errs.New(errs.EInvalidRequest, "RRset %s/%s has no records", owner, rs.Type)
	}
	for i, data := range rs.Records {
		switch rs.Type {
		case dnsmsg.TypeA:
			ip := net.ParseIP(data.Address)
			if ip == nil || ip.To4() == nil || strings.Contains(data.Address, ":") {
				return errs.New(errs.EInvalidRequest, "RRset %s/%s record %d has invalid A address %q", owner, rs.Type, i, data.Address)
			}
		case dnsmsg.TypeAAAA:
			ip := net.ParseIP(data.Address)
			if ip == nil || ip.To16() == nil || !strings.Contains(data.Address, ":") {
				return errs.New(errs.EInvalidRequest, "RRset %s/%s record %d has invalid AAAA address %q", owner, rs.Type, i, data.Address)
			}
		case dnsmsg.TypeCNAME, dnsmsg.TypeNS:
			if data.Target == "" {
				return errs.New(errs.EInvalidRequest, "RRset %s/%s record %d has empty target", owner, rs.Type, i)
			}
		case dnsmsg.TypeMX:
			if data.Target == "" {
				return errs.New(errs.EInvalidRequest, "RRset %s/%s record %d has empty exchange", owner, rs.Type, i)
			}
		case dnsmsg.TypeSOA:
			if data.SOA == nil {
				return errs.New(errs.EInvalidRequest, "RRset %s/SOA record %d is missing SOA fields", owner, i)
			}
		}
	}
	return nil
}

func payloadHash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
