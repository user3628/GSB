# Design Notes

## 1. Data and Identity

The service keeps the unique sample archive in `samples` because both policy rebuilds and exact-path audits need reproducible replay. Reservoir collections themselves are strictly bounded by `services.<name>.reservoir_capacity`; a full reservoir never grows with later replacements.

Sample identity is the explicit `id`. Its content hash covers:

```text
timestamp_ms | service | normalized stack frames | weight
```

`channel` is intentionally excluded: one profiler observation can arrive over more than one transport channel. A repeated ID with the same hash increments `duplicate_count`/`received_count` and is not inserted into the archive or reservoir again. A repeated ID with a different hash fails the whole batch with `E_SAMPLE_CONFLICT`.

## 2. Stratified Weighted Reservoir

The key is:

```text
service=<service>|depth_<d1>=<prefix>|depth_<d2>=<prefix>
```

The reservoir table primary key is `(policy_version, reservoir_id)`, while individual reservoirs are grouped by service, channel, and layer key. For every sample arriving in a reservoir:

```text
h = SHA-256("weighted-bottom-k-sha256-v1" | sample_id | reservoir_id)
u = (first_64_bits(h) + 1) / (2^64 + 1)
priority = -ln(u) / weight
```

The reservoir retains the `k` smallest priorities. A lower priority is better. If a new candidate beats the current maximum, the old maximum sample ID is appended to the recent-replacement trail and counters are updated; otherwise the candidate is counted as rejected. Because a bottom-k set is order independent, replaying the archive after a policy change produces the same retained set even if ingestion was split into many batches.

The replacement trail is bounded by reservoir capacity. Aggregate replacement counts remain exact in `replaced_total` and `rejected_total`.

## 3. Path Selection and Audit

For each service:

```text
population = count(distinct sample IDs in retained archive)
exact       = population <= exact_limit
approximate = population > exact_limit
```

The exact path scans the archive and reports unweighted occurrence counts plus corrected weighted heat. The approximate path uses only the union of reservoir members. The response always contains:

- `path.mode = exact | approximate`
- `path.used_reservoir`
- `path.reason`
- `approximation_basis` (rule, selected/population, strata, channel rates)
- `exact_share` and `absolute_error` for every frame

Thus approximate output is never silently substituted for exact output. The archive-side exact reference is an audit/difference result; it does not feed the approximate estimate.

## 4. Bias Bound and Confidence Interval

Let:

```text
W_s = selected corrected sample weight
W_p = population corrected sample weight
r   = W_s / W_p
```

For frame `f`, let `q_f` be its share inside the selected weight and `p_f` its true full-population share. The unretained mass contributes at most `1-r`, so:

```text
|q_f - p_f| <= 1 - r
```

The service reports exactly this as `bias_upper_bound`. The deterministic confidence interval is `[max(0,q_f-bound), min(1,q_f+bound)]`, labelled with a conservative coverage of 0.95. Because the bound is deterministic, every observed `absolute_error` is checkable against it frame by frame.

The effective correction coefficient is:

```text
service.bias_correction * channel.bias_correction
```

and the corrected weight is `sample.weight * effective_coefficient`.

## 5. Stable Hotspot Ranking

Estimated shares are converted to fixed-point integers at scale `1e12`. Frames are initially sorted by fixed value descending and ID ascending. A frame joins the current tie group when:

```text
abs(anchor_fixed - candidate_fixed) * 1_000_000
  <= anchor_fixed * tolerance_ppm
```

The final output is tie group ascending, then frame ID ascending. `conclusion.top_tie_frame_ids` is the full top group in ID order, so an in-tolerance value swap cannot change the stated hotspot conclusion.

## 6. Policy Transactions and Cascade Rebuild

Every policy version owns immutable rows in `reservoirs` and `hotspot_runs`. A full snapshot or increment performs, in one write transaction:

1. Validate version/base and compute semantic policy diff.
2. Replay the unique sample archive into new reservoirs.
3. Recompute hotspots and exact-reference differences.
4. Replace reservoir rows for the new version.
5. Insert the new hotspot row and policy snapshot.
6. Write the rebuild report with superseded reservoir IDs and changed references.
7. Advance `state_meta.current_policy_version` and `commit_seq`.

Readers see either the old complete version or the new complete version. There is no window where a current policy points at new reservoirs but old hotspots. Historical version rows remain queryable and are never mutated by later rebuilds.

`reservoirs.references_json` records the policy version, stratification fingerprint, hotspot policy fingerprint, channel rate, service/channel bias, effective bias, capacity, exact limit, and replacement rule.

## 7. Concurrency and Idempotency

- A process-local writer mutex serializes all business writes.
- SQLite uses WAL, `busy_timeout=5000`, `synchronous=FULL`, and foreign keys.
- Independent read endpoints use read transactions. `/v1/versions/{version}/snapshot` loads reservoirs and hotspots in one read transaction.
- `commit_seq` is the fencing coordinate exposed in every snapshot.
- Two increments with the same base are serialized; the first advances the version, and the second sees a stale base and returns `E_STALE_BASE`.
- A batch ID is unique in `sample_batches`. Repeating the same normalized payload returns the stored response with `idempotent_replay=true`; a different payload returns `E_CONFLICTING_REQUEST`.

## 8. Intent Log and Recovery

Write protocol:

```text
1. commit intent(state=pending) in its own tiny transaction
2. BEGIN business transaction
3. mutate samples/reservoirs/hotspots/policy/batch/state_meta
4. UPDATE intent state=committed inside the same business transaction
5. COMMIT
```

Crash windows:

- Before intent: no effect.
- After intent, before business commit: SQLite rolls the business transaction back; pending intent remains.
- During business commit: SQLite is atomic.
- After business commit: intent is already committed because it was updated in the same transaction; retry is idempotent.

On startup `Recover` scans pending intents. It checks whether the target batch/policy actually exists. Complete targets repair the intent to `committed`; all others become `rolled_back`. It then deletes any state rows whose `commit_seq` is greater than the last complete `state_meta.commit_seq`, runs `PRAGMA integrity_check`, and persists a recovery report.

Deterministic fault points call `os.Exit(137)` only when `PROFILE_CRASH_STAGE` matches:

- `after_intent`: before the business transaction begins.
- `during_reservoir_replace`: after approximately half of a reservoir batch is written inside the open transaction.
- `during_hotspot_aggregation`: after the in-memory aggregation completes but before state rows are written.
- `after_reservoir_replace`: after reservoir replacement but before hotspot persistence.
- `after_hotspot_insert`: after hotspot persistence but before policy/batch/state commit.

`PROFILE_CRASH_AFTER_INTENT=1` is retained as a convenience alias for `after_intent`. All fault points are off by default.
