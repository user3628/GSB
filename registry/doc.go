// Package registry implements a local service-instance registry, three real
// health probe classes, a five-state health machine, atomically rebuilt derived
// views, a triple-budget probe scheduler, and crash-recoverable SQLite
// persistence.
//
// Engine serializes all mutating operations with one writer gate. Read methods
// hold a state read gate and execute in a SQLite read transaction, so every
// response comes from one complete committed revision. Network probes run under
// the writer gate but outside the state read gate; readers therefore continue
// to observe the last complete revision while a round is in flight.
//
// Registration changes use a two-phase commit: a durable intent is written,
// a prospective snapshot and derived revision are staged with STAGING status,
// then the active instance table and meta pointers are switched in one
// transaction. Probe rounds similarly persist PENDING tasks and uncommitted
// results before health and derived activation. Recover clears every unfinished
// stage at startup and reports only durable facts.
package registry
