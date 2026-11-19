# Drive media latency

The slot reader delivers one ordered byte stream. A large media frame must be
downloaded and CRC-checked before later inline frames can be delivered. Mux
connections sharing that stream also share its head-of-line delays.

Drive media reads use up to four independent HTTP connection pools. The head
frame normally uses up to four ranges (four 128 KiB ranges for a 512 KiB frame);
prefetched non-head frames use one range per 512 KiB, capped at four. After two
seconds without progress, a pending range may race one duplicate on the separate
bulk connection. Completed ranges are retained. Moving transfers do not trigger
duplicates. Generic slot-body reads also have a two-second hedge; Drive frame
reads use the per-range layer rather than duplicating the entire frame. Priority
description reads have a 900 ms no-progress hedge starting at dispatch, not while
queued. Speculative descriptions do not hedge. Description recovery allows one
concurrent hedge per session, one dispatch per hedge, and no hedges for one minute
after a confirmed live rate limit. Each layer has a bounded hedge budget. Recent
403/429 rate limits also suppress media hedges and reduce new frames to one range.

Heads and watermarks use a separate pooled control connection so speculative
reads and uploads do not share their HTTP connection. This adds a connection
pool, not polling or keep-warm API requests.

Range offsets, lengths and the complete frame CRC are checked before delivery
or acknowledgement. Servers that ignore Range can return a full-sized body.
For these Drive media changes, the frame and announcement formats are unchanged:
a client-only upgrade works with existing servers. The template WAL upgrade below
has a different compatibility requirement. No server setting can be increased by
changing the client.

## Template-backed WAL publication and upgrades

All template-backed WAL writers publish the unchanged `<seq>.seg` payload first,
then upload a completion sidecar `<seq>.commit`. The sidecar is exactly 48 bytes:
8-byte magic `XDWALC01`, an 8-byte big-endian payload length, and a 32-byte SHA-256
payload hash. New readers require a valid sidecar and matching payload size/hash
before delivering a segment; an incomplete publication is bounded by the existing
`holeTimeout` rather than treated as complete data. This prevents a partially
visible template PUT from becoming stream data.

**Upgrade both template endpoints.** Legacy readers can still consume the
unchanged `.seg` payload, but new readers refuse legacy writers that omit completion
markers. This is not universal backward compatibility. Template-backed WAL adds
completion-record PUTs and at least one extra completion-record GET per successfully
consumed template segment; polling and retries can add requests, with corresponding
backend/API overhead.
Local, S3 and Drive WAL do not require these sidecars. Slot-mode framing is separate
and unchanged.

## Capacity and diagnostics

For browsing, distant description lookahead backs off after repeated misses,
while head promotion and near-head admission still interrupt that wait. During
cooldown and for five seconds after a rate limit, the slot reader admits only
one metadata hunt at a time per slot instead of three overlapping hunts. These
changes reduce wasted stale-frame reads without changing framing or poll settings.
If a metadata-only lazy create is retried and the existing description already
matches, recovery skips the redundant PATCH. Media creates still update the body.
They cannot eliminate the peer's upload time; compare both peers' logs before
attributing `discover` latency to the network alone.

Google's [published limits](https://developers.google.com/workspace/drive/api/guides/limits)
(checked November 2026) are 1,000,000 units/minute/project and 325,000
units/minute/user/project. Metadata GETs cost 5 units, edits 50, lists 100, and
media GETs 200, including range requests. The latter budget alone permits about
27 media GETs/second, before other traffic. Both peers and other applications
using the same user/project contribute. Google grandfathers projects that used
the API between November 2025 and April 2026; the per-method unit model is not
necessarily their effective quota. Check the project's quotas in Cloud Console;
connection counts are not API quota settings.
Google also documents additional backend throttling and a 750 GB/day Workspace
upload limit. [Range downloads are supported by Drive](https://developers.google.com/workspace/drive/api/guides/manage-downloads#partial_download).

Parallel reads exchange extra requests and connections for lower per-frame
latency: a four-range head frame uses 800 download units instead of 200, plus any
stalled-range duplicate. They cannot remove peer upload delays or an outage
affecting every network connection.

Debug `xdrive read` lines report `discover` (time until the expected frame was
first seen), `body` (time in media downloads, including failed rounds), and
`blocked` (time a completed segment waited behind its predecessors). `fetch`
also includes prefetch admission and polling waits; it is not an HTTP timeout.
Per-minute timing lines report `requests` and estimated `units` for all sent
attempts, including retries/cancelled hedges. `n` remains the capped latency
sample count. `failed` and `canceled` count transport/body failures, while
`rate-limits` counts confirmed 429 or rate-limit 403 responses, not timeouts.
Sum units across operation types and peers when comparing usage
with the project's effective quota. These are estimates using the published
per-method model, not measurements of a grandfathered project's quota charges.

Cleanup listings and deletes share a storage-wide concurrency limit; every
cleanup DELETE attempt, including retries, is paced. Cleanup rate limits do not
cool live traffic. Live throttled 403 responses retain their bounded 45-second
retry window; 429 responses retain the eight-attempt policy. Transient OAuth
failures wait cancelably for token eligibility, with a separate three-attempt,
two-minute authentication bound; they do not spend Drive API attempts. Permanent
credential rejection fails immediately.

To verify live, compare body latency, blocking episodes, player buffer health,
and request units over the same video workload. A high `discover` with low
`body` requires peer-side timing to distinguish slow uploads from discovery.
