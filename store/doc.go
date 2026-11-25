// Package store persists immutable plan versions, plan heads, per-row-block
// products, independent error reports and operator state in SQLite.
//
// Every publication takes a real cross-platform file lock and an immediate
// SQLite transaction. Plan and block sequence numbers must advance exactly by
// one; stale, duplicate or cross-plan publications return spmm.CodeStalePublish.
// ReconcilePlan validates the immutable digest chain and invalidates downstream
// products when it detects rollback or whole-record replacement.
package store
