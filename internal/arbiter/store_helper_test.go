package arbiter

import "leasearbiter/internal/store"

func storeOpen(path string) (*store.Store, error) { return store.Open(path) }
