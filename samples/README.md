# Deterministic samples

`workload-42.json` is produced by:

```sh
go run ./cmd/walgroup generate -seed 42 -out samples/workload-42.json
```

The format is stable, contains no timestamps or random host data, and uses
`math/rand` seeded with `42`. The sample covers group commits, segment
rotation, a duplicate commit, a conflicting commit, checkpoint advancement,
safe and unsafe reclaim decisions, stale reads, checkpoint lag, and replay.
