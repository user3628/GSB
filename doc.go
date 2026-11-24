// Package rewardpack normalizes preference annotations, derives auditable
// factorized reward scores, projects and quantizes fixed-dimension features,
// and selects pairs under token and per-source budgets.
//
// Engine.Build publishes five deterministic stages through SQLite
// transactions. Pair artifacts carry input digests for minimal incremental
// reuse, while stage checkpoints use conditionally updated monotonic sequence
// numbers and a cross-process file lock. Engine.VerifyIncremental compares the
// resulting scores, feature summaries, retention decision and report against a
// from-scratch build byte for byte.
package rewardpack
