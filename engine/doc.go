// Package engine coordinates the interactive query path and the settlement
// adjudication path over one immutable plan snapshot.
//
// Query recomputes only units intersecting the request and enforces a latency
// budget. Settle reuses committed row blocks, computes missing ones, and
// commits each completed indivisible unit. ComparePaths performs bit-level and
// digest-level adjudication and returns E_PATH_MISMATCH with the first
// differing execution row block.
package engine
