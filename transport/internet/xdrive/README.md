# Drive media latency

The slot reader delivers one ordered byte stream. Legacy media frames are fully
downloaded and CRC-checked before delivery. Negotiated streaming frames can
deliver verified portions sooner, but later frames still cannot overtake missing
earlier bytes. This is incremental delivery, not per-mux-stream isolation.

## Negotiated verified media streaming

New Drive peers advertise reader support in the existing ring announcement.
The accepting server acknowledges it through version/session properties on its
normal slot or watermark updates; clients never publish their own acceptance.
Markers are session-bound public file properties so different OAuth applications
can see them. Missing, stale, or unsupported acceptance leaves the writer on the
legacy format. This negotiation adds metadata bytes, not extra API calls or files.

Streaming mode (`mode=5`) adds CRC32 checksums for fixed 32 KiB blocks. Their
combined CRC must match the frame CRC before delivery starts; each block must
then pass its own checksum. Blocks are delivered in byte order, and delivered
blocks are not replayed by retries or hedges. Slot reuse/ACK waits for every range
to finish exact length and EOF validation and for all verified blocks to be queued.
Primary GET counts and existing range/hedge budgets are retained. Streamed range
boundaries are block-aligned; legacy range behavior and polling floors are unchanged.

CRC32 is corruption detection, not authentication. Existing VLESS encryption and
its byte order are unchanged. A later bad block, truncated response, or EOF error
can terminate a connection after a valid prefix has already been delivered;
those prefix bytes cannot be retracted. No all-or-nothing delivery guarantee is
claimed for streaming frames. Prefetch bounds remain in place, and consumed
block buffers are released while their deduplication bits are retained.

Use upgraded endpoints on both sides to enable streaming. Mixed-version peers
remain compatible but use legacy media. No mux-resume feature, external transport
dependency, configuration change, or polling-floor reduction is required.

For streaming read logs, `first-block-wait` measures the time until the first
verified block is queued to the connection reader. `head-wait` still describes
whole-frame completion waiting, so it is not the first-byte delay for mode 5.
The first-block field is zero/not applicable for legacy frames. Slow HTTP/body
timings include local checksum processing; they are not pure wire measurements.

## Queue and streaming-gap observations

Write logs add `buffer-episode-age`, `upload-permit-wait`, `upload-queue`, and
`prefetch-setup` before the existing `await` and `put` phases. The buffer episode
begins on an empty-to-nonempty append and persists across extracted suffixes, so
its age is an upper bound, not exact per-byte residence time. Permit waits include
reacquiring the writer mutex after a full upload window. Queue time measures
assignment to worker entry; prefetch setup covers the synchronous preparation
before reuse waiting starts, not the background watermark HTTP request duration.
These durations can overlap and must not simply be
added to infer end-to-end latency. They are logged only for successful writes.

`local-outstanding-bytes` snapshots buffered plus assigned/incomplete upload bytes
at assignment, including the selected frame. It is not remote unacknowledged data
or the exact bytes ahead of an interactive message. `buffer-bytes` is the buffer
size before extraction, not a queue position. Unavailable observations report
`-1`; none of these observations change batching, admission, or storage traffic.

Read logs separate `media-admission` (cumulative wait at the body-prefetch gate)
from HTTP retrieval. For streaming frames, `missing-block-gaps`,
`missing-block-wait`, and `missing-block-max` describe reader-observed waits for
the next required block after the first delivery. They exclude downstream
reader-channel backpressure, which is recorded separately as `enqueue-wait` and
`enqueue-max`. `completion-after-blocks` describes the remaining completion
fence after the last block was queued; it can include range EOF/validation and
worker joining, not just network EOF. Ready/prefetched blocks do not create
missing-block gaps. These are transport observations, not website benchmarks.

Split streaming reads additionally emit a debug `xdrive stream plan` with the
sequence, frame size, range count, and head/prefetch role. It measures planned
geometry, not successful requests or quota savings. The current geometry is
unchanged; the plan helps assess whether a two-range experiment is worthwhile.

### November 20 follow-up (resume.47)

The client 08:22:00–08:27:43 log contains 434 media frames, all using streaming
mode 5. In 98 frames the first block was queued more than 100 ms before full-frame
completion, with a maximum within-frame lead of 1.32 s. That is direct evidence
of earlier transport handoff, not a matched browser-speed comparison. Five timing
windows report two media hedges with no wins and 15 deferred opportunities.
Most server reuse waits are microseconds, although there are roughly one-second
outliers. The major matched inline delay is therefore not explained by a general
reuse bottleneck; local writer queue observations are needed before changing
window or ACK policies.

The writer already overrides the large-burst rule with inline fan-out when its
available window can cover the buffer (up to about 252 KiB with an empty default
12-upload window). A grown segment size does not mean every subsequent burst
uses media. A single multipart request also does not establish visibility timing
from the writer's HTTP reply alone: the reader does not wait for that reply.
Smaller tiers, faster ACKs, or a new metadata-prefix mode are not introduced on
these assumptions. A prefix would add bandwidth to repeated metadata reads and
does not by itself guarantee a complete encrypted record.

Drive media reads use up to four independent HTTP connection pools. The head
frame normally uses up to four ranges (four 128 KiB ranges for a 512 KiB frame);
prefetched non-head frames use one range per 512 KiB, capped at four. After two
seconds without progress after dispatch, a pending range may race one duplicate
on the separate bulk connection. For slot reads with promotion tracking, a
prefetched range defers this recovery until its frame becomes the delivery head.
Promotion rechecks the primary's existing stall age and active status, rather
than imposing another two-second delay. Standalone callers without a nonnil
promotion channel retain their previous recovery policy. Headers and body bytes
reset this clock;
token/permit/connection waits do not start it. Completed ranges are retained.
Moving transfers do not trigger duplicates. Generic slot-body reads also have a
two-second hedge; Drive frame
reads use the per-range layer rather than duplicating the entire frame. Priority
description reads have a 900 ms no-progress hedge starting at dispatch, not while
queued. Speculative descriptions do not hedge. Description recovery allows one
concurrent hedge per session, one dispatch per hedge, and no hedges for one minute
after a confirmed live rate limit. Each layer has a bounded hedge budget. Recent
403/429 rate limits also suppress media hedges and reduce new frames to one range.
Media duplicates likewise get one dispatch, not their own retry loop. All Drive
read duplicates recheck primary progress at dispatch admission, so known progress
or retry recovery can suppress a queued duplicate. Primary retries are unchanged,
but later retry attempts do not acquire another hedge; recovery latency after a
failed first attempt can therefore differ. A spent timer does not wait for new
hedge tokens or the end of a cooldown.
`hedge-deferrals` counts expired nonhead recovery opportunities, not saved API
calls: promotion may admit one later, and normal quota/budget checks still apply.
Primary GETs, ranges, windows, and budgets are unchanged; useful speculative
recovery can start later. Deferral does not guarantee fewer requests in every
workload, since preserved tokens can instead fund delivery-head recovery.

Heads and watermarks use a separate pooled control connection so speculative
reads and uploads do not share their HTTP connection. This adds a connection
pool, not polling or keep-warm API requests.

Range offsets, lengths and the complete frame CRC are checked before delivery
or acknowledgement. Servers that ignore Range can return a full-sized body.
The older range/hedge changes leave legacy formats unchanged. The streaming
extension above is negotiated per session; a client-only upgrade still works with
existing servers without enabling streaming. The template WAL upgrade below
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
while head promotion and near-head admission still interrupt that wait. A primary
metadata read starts immediately; extra overlapping head reads wait 1.5 times the
smoothed completed-description latency (250 ms initially, bounded to 100 ms–1 s).
Promotion and write wakeups retain an outstanding read's launch age instead of
starting more copies at each 25 ms tick. This trades more aggressive speculative
visibility for fewer discarded reads; stalled heads retain bounded overlap.
After at least three misses, a distant read that returns a valid older-lap frame
may use a 2 s backoff ceiling if the observed description frontier has not advanced
for `eagerWindow`. The existing exponential calculation remains (typically
1.2 s at the 300 ms pace floor), not a forced 2 s sleep. Absent/new slots,
invalid frames, errors, and head/near schedules are unchanged. This reduces
repeated old-frame speculation during quiet peer periods. Promotion still wakes
the wait, but frontier progress alone does not: far-future visibility and body
prefetch timing can be delayed, even though prefetch-window sizes are unchanged.
During cooldown and for five seconds after a rate limit, the slot reader admits only
one metadata hunt at a time per slot instead of three overlapping hunts. These
changes reduce wasted stale-frame reads without changing framing or ordinary
head/near poll settings.
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
`head-wait` measures the time from the ordered reader's first wait for that head
until it was ready (zero if prefetched already). It separates actual delivery
waiting from prefetch age. `overlap-delay` reports the current metadata overlap
threshold. Neither is a website response-time measurement.
`quiet-tail-backoffs` counts scheduled waits extended beyond the ordinary tail
ceiling. These waits can be interrupted; the counter is not saved requests or
quota units.
Per-minute timing lines report `requests` and estimated `units` for all sent
attempts, including retries/cancelled hedges. `n` remains the capped latency
sample count. `failed` and `canceled` count transport/body failures, while
`rate-limits` counts confirmed 429 or rate-limit 403 responses, not timeouts.
Phase-tail fields add p95/max for permit/connection waits, post-send first-byte
wait (`server`), and response-body retrieval. `send-p50/p95/max` measures dispatch
admission to the completed request write, including headers and any upload body;
it is not a pure wire-throughput measurement. Percentiles/maxima use the capped
completed-attempt reservoir, not every request counted by `requests`.
Transport/body failures generally lack phase samples; outer retry sleeps are
excluded, and internally replayed attempts retain the latest dispatch snapshot.
Debug `xdrive slow timing` lines report individual completed HTTP attempts taking
at least 2 s, even before the next minute flush, with HTTP status and outer attempt
number so retry/error responses are distinguishable. Media lines include sequence,
range (exclusive end), whole-frame and requested-range byte sizes, rescue status,
and head-marker/promotion state at dispatch and HTTP completion. The head state
is sticky once promoted, not evidence of HTTP priority or the current head after
a losing rescue outlives its primary. Head flags are diagnostic; promotion can
wake deferred media recovery but does not reprioritize or restart primary GETs.
These diagnostic details add no API calls and do not themselves improve download
speed. Unobserved phases are reported as
unavailable (`-1ns`).
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

### November 19 browsing baseline (resume.43)

The supplied 08:15:58–08:25:13 log contains nine full timing windows: 3,320
head-read dispatches, of which 1,781 were canceled; 9,769 other metadata dispatches;
and 830 media dispatches with ten duplicates and no duplicate wins. There were no
confirmed rate limits or failed attempts in those windows. Completed metadata
reads were typically around 330 ms. These observations motivate latency-aware
overlap and dispatch-aware media recovery; they do not prove a speedup for the
next changes or establish a like-for-like comparison with an older workload.

### November 19 follow-up (resume.44)

The 09:10:35–09:17:46 log contains six reported timing windows: 553 head dispatches
with 22 cancellations (4.0%, versus 53.6% in the preceding log), and 6,206 other
metadata dispatches. Inline `head-wait` is 18 ms at p50 and 762 ms at p95, but has a
7.7 s outlier; media has a 7.4 s head-wait outlier. The workloads differ, so these
are not matched throughput or website-latency benchmarks. Eleven request failures
came from one shared-socket remote reset and recovered through existing retries;
there were no confirmed rate limits. Valid old-frame speculation motivates the
quiet-frontier backoff, not more head overlap or additional media requests.
Peer-side timing is still needed to explain the long inline discovery gaps;
whole-frame body waits do not by themselves prove a stalled network transfer.

The supplied server excerpt covers only 05:46:13–05:47:13 and contains 496 XDrive
transport lines. Matching 337 downlink and 139 uplink frames bounds the constant
client-minus-server clock offset to approximately 12,632.6–12,633.8 s; simply
adding 3 h 30 min would misalign these logs. For downlink frame 1685, the server
PUT took 1.55 s with negligible credit wait, while the client body phase took
7.48 s and blocked the ordered reader for 7.43 s. Server media PUT p50/p95 were
1.36/1.67 s, with no failures or rate limits in its reported window. This points
to media retrieval for that episode, but does not distinguish local queueing,
first-byte delay, or a moving slow transfer. The excerpt does not cover the
earlier inline discovery gaps. Phase-tail diagnostics are added before changing
media scheduling, concurrency, or range counts on this evidence.

### November 19 follow-up (resume.45)

The 10:23:46–10:27:56 client log has four reported timing windows: 309 head
dispatches with four cancellations (1.3%), and 562 media dispatches with ten
duplicates and no wins. `quiet-tail-backoffs` remains zero, so this workload
does not demonstrate savings from that gate. Slow media attempts have negligible
local permit/connection waits, but first-byte waits up to 3.62 s and body retrieval
up to 2.94 s. Those phase samples do not justify changing the local permit pool.
Tracked nonhead duplicate recovery is deferred to preserve budget for delivery.
The short server excerpt overlaps only the last part of the client log and misses
its main media slowdown. For matched inline frame 1017, bounded clock alignment
places most of its 4.73 s head wait before the server PUT began, not in a four-second
metadata HTTP request. These are different workloads, not matched speed benchmarks.
