# Loopback Tunnel Gateway · Architecture Notes

## 1. Processes and hops

The application is deliberately local-only. One FastAPI process owns the HTTP
control plane and the in-memory scheduler. The same process also hosts the local
UDP test facilities:

```text
HTTP request
   │
   ▼
frame encoder ── per-frame UDP socket ──► carrier listener (UDP)
                                             │
                                             ▼
                                      egress decapsulator (UDP)
                                             │
                                             ▼
                                      response frame
   ◄──────────────── carrier relay ◄──────────┘
```

The snapshot's `egress_endpoints` point at the carrier listeners. The carrier is
a transparent two-hop test carrier: it forwards each datagram to the configured
`egress` decapsulator and relays the response to the gateway's original source
address. Every frame uses a fresh connected UDP socket; timeout handling closes
that exact socket in `finally`. A timeout therefore means the local waiter and
its socket were actually closed, not merely hidden behind an async sleep.

## 2. Frame format

The wire vocabulary is binary, not JSON envelopes. All integers are network byte
order.

### Request frame `VGW1`

| Field | Width | Notes |
|---|---:|---|
| magic | 4 bytes | `VGW1` |
| wire version | 1 byte | currently `1` |
| flags | 1 byte | reserved, `0` |
| channel | 2 bytes | channel slot chosen by the scheduler |
| sequence | 8 bytes | uint64, per-session contiguous |
| payload length | 4 bytes | exact payload byte count |
| session length | 2 bytes | UTF-8 session-id byte count |
| reserved | 2 bytes | `0` |
| session id | variable | UTF-8 |
| payload | variable | caller-supplied bytes |
| checksum | 32 bytes | SHA-256 of every preceding byte |

The response `VGR1` has the same shape, replacing flags with a one-byte status.
The decapsulator validates magic, version, declared lengths, payload bound and
the digest before consuming the payload. A corrupted request receives a
checksum-checked error response; the gateway validates response identity and
digest before it can become a session acknowledgement.

## 3. Serialized state and concurrent reads

`Gateway._writer_lock` is the single writer boundary for:

- channel lease grant and release,
- snapshot loading,
- route-patch quiescence and commit,
- frame admission, dispatch and finalization,
- recovery publication.

Readers never acquire the writer lock. They receive `Gateway._published`, an
immutable dictionary rebuilt only after a SQLite commit. Consequently a reader
cannot observe a half-applied route patch, a released lease combined with an
old queue publication, or a statistics generation from the previous endpoint.

The frame-id idempotency table is part of the same writer boundary. The first
submission creates one `FrameOperation`; duplicate callers await the same future.
The operation signature covers `session_id`, `sequence` and a canonical payload
SHA-256. A matching duplicate reuses the outcome; a mismatching duplicate is
rejected with `E_CONFLICTING_FRAME` before it can touch the queue.

## 4. Scheduling

Each snapshot defines:

- `channel_count`: fixed number of channel slots;
- `round_budget_ms`: per-round dispatch-time budget;
- `queue_capacity`: bounded waiting-queue capacity;
- each session's `priority`, `channel` affinity and egress binding.

A dispatch round scans the waiting list while the wall-clock budget permits and
while a bound channel is free. Eligible frames are ordered by descending session
priority, then admission index. A session with an active frame is not eligible.
Different sessions whose channel affinities are free can run in parallel.

Admission dispatches eligible frames inside the serialized admission boundary.
This prevents a burst of concurrent HTTP calls from erroneously filling the
waiting queue before any channel has had a chance to accept a frame. Once all
applicable channels are busy, further admitted frames enter the bounded queue;
the next frame is rejected with `E_CHANNEL_QUEUE_FULL` when the queue is full.

## 5. Incremental rebuild protocol

1. Validate the patch model and digest without touching state.
2. Acquire the rebuild lock and writer lock.
3. Verify `base_version == current.version`; otherwise return `E_STALE_BASE`.
4. Compute a candidate complete snapshot and affected session set.
5. Block new admissions for affected sessions.
6. Wait until their active and queued old-binding frames drain.
7. In one SQLite transaction: insert the new snapshot version, update session
   facts and rebuild affected statistics from acknowledged frame facts.
8. Publish the new snapshot and rebuilt statistics as one immutable state object.
9. Unblock affected sessions.

In-flight frames are never rebound. They finish against the binding captured at
dispatch, and the switch occurs only after the affected scope is quiescent. The
result is an atomic visible transition from old snapshot + old stats to new
snapshot + new stats.

## 6. Intent and lease persistence

SQLite runs in WAL mode with `synchronous=FULL`. Before the first outbound UDP
send, `commit_dispatch` inserts a pending `frame_dispatch` intent and updates the
frame and lease in the same transaction. The lease contains the channel,
`sent_unacked_seq`, recent activity and session id.

A successful response commits a second transaction that marks the intent
committed, the frame acknowledged, releases the channel reservation and updates
statistics. A crash can therefore leave one of these durable facts:

- queued frame with no dispatch intent → recovered as aborted;
- pending dispatch intent + dispatched frame + active lease → retransmit marker
  if the current binding still matches the intent, otherwise aborted;
- committed acknowledgement → no recovery action.

The recovery report is explicitly labelled `facts_only: true`. It does not claim
that a process killed with `SIGKILL`/`TerminateProcess` produced a graceful exit
report, and it does not infer that a remote egress did or did not process a
datagram. It only classifies committed intent/frame/lease rows.

## 7. Deliberate local scope

- No Docker, container runtime, real VPN account, remote target or external
  network is used.
- Only `localhost`, `127.0.0.1` or `::1` is accepted for snapshot endpoints.
- The demo uses fixed generated samples and real loopback UDP sockets.
- A single gateway process is the writer; SQLite is not used as a multi-process
  coordination service for live scheduling.

