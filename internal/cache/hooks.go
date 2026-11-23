package cache

// CrashHook is deliberately tiny: an end-to-end test can make it write a
// marker and block, then terminate the process with Process.Kill. A hook does
// not alter persistence by itself.
type CrashHook func(point string)

const (
	CrashAfterWriting     = "after_write_writing"
	CrashAfterEvictIntent = "after_evict_intent"
)

func (s *Store) maybeCrash(point string) {
	if s.crashPoint == point && s.crashHook != nil {
		s.crashHook(point)
	}
}
