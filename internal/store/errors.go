package store

import "errors"

// ErrVersionRetired means the row version needed by an old snapshot was
// reclaimed by retention/recycling. Callers must never fall back to a newer
// version when this error is returned.
var ErrVersionRetired = errors.New("row version retired by retention policy")
