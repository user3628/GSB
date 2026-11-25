package engine

import "ticketpipeline/internal/store"

// VerifyCheckpoints is the engine-level facade reused by the CLI and HTTP
// verification tooling.
func VerifyCheckpoints(st *store.Store) error {
	return verifyStore(st)
}
