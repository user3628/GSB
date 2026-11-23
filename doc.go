// Package dnsdetect implements a deterministic, local DNS cache-poisoning
// detection domain component.
//
// It parses three probe formats into one event model, normalizes DNS names and
// transaction identifiers, converts all timestamps to fixed Unix 100 ns ticks,
// pairs events by window, adjudicates four poisoning indicators, commits
// immutable versioned snapshots to pure-Go SQLite, and exports byte-stable JSON
// reports.
//
// Time arithmetic is integer-only. Parsing sessions, error ledgers, and budget
// counters are isolated per source; the merge and adjudication phases are
// deliberately serial and deterministically ordered. Snapshot commits are
// atomic and idempotent by content digest. See README.md and FORMATS.md for the
// wire formats and integration contract.
package dnsdetect
