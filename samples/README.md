# Fixed samples

- `snapshot-v1.json`: version 1 at Unix epoch minute `29453760` (2026-01-01T00:00Z).
- `snapshot-v2.json`: version 2 at minute `29453766` (00:06Z); tenant A changes
  from `standard` to `peak` and from repayment priority 1 to 5.
- `events.json`: seven synthetic borrow/return events spanning minutes 0 through 9.
- `settle.json`: deterministic settlement through 00:10Z.
- `borrow-overcommit.json`: a tenant C borrow that would push the pool farther over
  its threshold and must return `E_POOL_OVERCOMMIT`.
- `crash-settle.json`: a target settlement through 00:12Z used only by the
  fault-injection endpoint.
- `settle-after-recovery.json`: the normal settlement repeated after restart.
- `expected/`: stable settlement summaries, full expected state, rejection error,
  recovery facts and post-recovery result used by `scripts/verify.ps1` and the Go
  tests.

All quantities are integer milli-shares. Interest outputs are integer micro-shares
with nano-share rounding residuals. Snapshot digests are canonical SHA-256 values;
`ledgerctl stamp -file <snapshot>` can recompute them.
