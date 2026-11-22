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

<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:e7c42c9740f8a4ae3229fb179fed77e2a361e65b68681edf2210f632898fddb9 -->

## Verified inline start for Drive media

<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:cf12cf40ac2afb9ea6fdec6d8e9f416e8dcaeea4550283534e08b962b26a514f -->

Pure inline frames deliver small messages from their metadata, without a media
GET. A downstream media frame may also carry its first 11 KiB in the metadata of
its existing multipart upload. The prefix and the media suffix each have
integrity checks and share one sequence and one slot. The reader can hand off
the prefix before the media response arrives, but reuse still requires verified
EOF of the suffix.

With progressive media and prefix support, this also applies to overlapping
media uploads from 64 to 512 KiB when at most 512 KiB of earlier payload is still
uploading. The expanded path is disabled during cooldown or for five seconds
after a rate limit. Peers that offer prefix support without progressive media
still use it only when no other uploads are outstanding.

<!-- /coderabbit-ai-generated:v1 -->

The fast start has no separate upload, slot, metadata request, or extra primary
media request. Repeated metadata polls can download the prefix more than once.
Even the largest encoded prefix descriptor stays below the existing 14 KiB
inline payload descriptor size. At 900 ms of simulated media startup, the
prefix arrives 900 ms earlier at the XDrive reader, and the last media block
arrives at the same time. This does not shorten the Drive publication delay,
let later stream bytes overtake earlier ones, or guarantee a complete HLS
segment or TLS record. Use both updated peers for downstream testing. Read and
write logs include `inline-prefix-bytes` so live samples can distinguish it
from ordinary streaming.

## Recovery for stalled ordered blocks

<!-- /coderabbit-ai-generated:v1 -->

The matched v48 session shows two different retrieval stalls: frame 730 waited
9.22 s for its first verified block, while frames 866 and 869 had later block gaps
of 5.36 s and 3.06 s. A ready 101-byte frame then spent 18.44 s behind preceding
bulk frames. The roughly 15 s admission ages of later prefetched frames were not
15 s of waiting at the delivery head; increasing permits on that evidence would
target the wrong phase.

Tracked streaming slot reads identify the next block actually awaited by the
ordered reader. Only the headed range containing that block spends scarce rescue
budget; stalled later ranges wait for frontier advancement. Recovery can consider
a two-second verified-block gap as well as the existing wire-progress clock, so
partial bytes need not indefinitely suppress recovery of a missing verified block.
The wait excludes speculative age and downstream enqueue backpressure, and cannot
start before the primary's actual dispatch. Reaching the terminal block frontier
still leaves downstream enqueue and unfinished range EOF/validation fenced.

A head waiting for rescue tokens can wake on existing budget replenishment rather
than permanently abandon recovery after one empty-budget observation. Eligibility
and stall age are checked again at dispatch, suppressing obsolete queued duplicates.
Primary GET plans, token caps/refill rates, rescue concurrency, cooldown protection,
one-rescue-per-range limits, checksums, deduplication and ACK fences remain intact.
Legacy/standalone callers without the ordered delivery signal retain wire-progress
recovery. No uploads are duplicated and no encrypted bytes are reordered.

This is not a two-second delivery deadline: quota eligibility, available tokens,
HTTP admission and the service still determine whether a rescue can help. Racing a
slow but moving transfer can consume bandwidth and one extra media GET within the
existing budget. Actual requests may differ; use matched first-block/missing-block
waits, throughput and both peers' quota units to judge the result. Shared backend or
network stalls can affect both copies, and per-mux-stream isolation is not provided.

No wire-version or configuration change is required. The improved reader works
with existing v48 streaming peers; for downstream browsing, upgrade the client
reader. Updating the server also applies the server-side batching improvements.

## Optional two-range streaming-head experiment

The production default remains four media ranges for a large initially headed
streaming frame. An explicit process environment setting can cap those MODE5
head plans at two ranges for an A/B experiment:

```powershell
$env:XRAY_XDRIVE_STREAM_HEAD_RANGES = "2"
# Start the core with your existing configuration.
```

Use `"4"` or remove the setting to return to the default, and restart the core
between runs. Only `2` and `4` are accepted; invalid values warn and retain four.
The setting is captured when the Drive storage owner is created and copied into
its sessions, not re-read during traffic. For downstream browsing, change the
client reader's environment. No configuration-schema or dependency change is needed.

This affects only initially head-marked MODE5 primary range planning. Legacy
MODE2, nonhead planning, connection pools, promotion, polling floors, permit limits,
hedge policy, and checksum/EOF/ACK requirements are unchanged. A prefetched
single-range request is not split or relaunched when it later becomes the head.
Two ranges may save quota but can worsen later block gaps, completion time, or
connection-diversity recovery. Fewer completed ranges also replenish the existing
hedge-token budget more slowly; no compensation is applied during this experiment.
It is not a claimed fix for slow first-byte responses or a new production default.

Match direction, frame size, load and connection warmth when comparing 4 versus
2. Compare actual `requests`/`units`, throughput, first-block waits, subsequent
missing-block gaps, completion and recovery—not only planned range counts. The
owner startup log and split plan's `head-limit` show the effective setting. Read
logs append the physical `slot` tag so plans and slow requests can be correlated.

## Queue and streaming-gap observations

Write logs add `buffer-episode-age`, `upload-permit-wait`, `upload-queue`, and
`prefetch-setup` before the existing `await` and `put` phases. The buffer episode
begins on an empty-to-nonempty append and persists across extracted suffixes, so
its age is an upper bound, not exact per-byte residence time. Permit waits include
reacquiring the writer mutex after a full upload window. Queue time measures
assignment to worker entry; prefetch setup covers the synchronous preparation
before reuse waiting starts, not the background watermark HTTP request duration.
<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:5d9aa8de359f722b1affdea01157cde9f4d34e286b8b2d8e3d10731a88815987 -->

These durations can overlap and must not simply be
added to infer end-to-end latency. They are logged only for successful writes.

### Reuse when the ring fills

The reuse fence still requires the previous write to finish and the reader to
acknowledge that frame. Workers now wake when either condition changes, rather
than waiting for the next local poll. The watermark GET cache and single-flight
limit still apply.

On Drive, seeing a full ring can flush already earned credit before the usual
ACK batch or timer. The reader acknowledges through its completed delivery head,
never through the furthest frame it has discovered. Pressure can trigger at most
one early flush per ACK interval, and this path is skipped during quota recovery.

Successful write logs include `await-previous-write` and `await-watermark`.
Both are measured from the start of the worker's reuse wait to its observation
of that gate opening. They can overlap. The pressure ACK log shows the previous
watermark, the sequence it acknowledged through, and the discovery frontier.

<!-- /coderabbit-ai-generated:v1 -->

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

### November 20 rerun (resume.48)

The saved client 09:39:39–09:44:51 log contains 574 streaming frames and 124
prefix handoffs over 100 ms before completion, with a maximum lead of 5.62 s.
Five media hedge races won; whole-range win counts still cannot account for every
useful early block. Local reader enqueue p95 is about 2 ms, and the maximum
completion fence after queued blocks is about 5 ms. Significant first-byte/body
outliers remain: frame 730 waited 9.54 s for headers and 2.49 s for body retrieval,
while later prefetched requests took about 15 s without local HTTP queue waits.
Long admission waits belong to bounded future-frame prefetching and do not by
themselves justify increasing that window.

Server upload-permit waits reached 1.10 s and reuse waits 1.49 s, but p95 reuse
wait is only about 47 microseconds. No window/ACK change is introduced. Split plans
contained thirty four-range and eight three-range heads; applying a two-range
cap to those same plans would remove 68 primary GETs (13,600 estimated units).
That is a planned counterfactual, not measured savings. It motivates only the
explicit opt-in experiment above, with the default unchanged.

### Browsing-oriented follow-up

Small Drive inline suffixes that stop growing can flush after two coalescing
ticks rather than waiting for all eight while other uploads are outstanding.
At the default 10 ms interval this is typically 20–30 ms instead of 80–90 ms;
growing bursts keep the existing batching. Cooldown/recent rate limits disable
the early flush. The first matched v48 client session had 600 small inline writes
with a median buffer episode age near 84 ms, which motivates this local change,
not a claimed website speedup. An earlier suffix can split later data into another inline
upload, increasing write units and polling/ACK overhead; this is not quota-neutral.

Repeated clean empty descriptions and never-written lazy-slot 404s can now use
the existing quiet distant backoff. Errors, invalid frames and 404s for previously
seen slots do not accumulate clean-absence credit. Head/near schedules and
promotion interrupts remain unchanged. This can delay far-future prefetching;
v48 recorded no extended quiet-tail backoffs, so savings need a matched rerun.
Ranges, upload windows, checksums, ordered delivery and ACK fences are unchanged.
Ready small frames can still wait behind earlier bulk data: the matched v48 log
includes a 101-byte frame blocked for 18.44 s. Head-block recovery targets retrieval
stalls, but these changes do not isolate browser/mux streams or eliminate every
remote HTTP stall.

Drive media reads use up to four independent HTTP connection pools. The head
frame normally uses up to four ranges (four 128 KiB ranges for a 512 KiB frame);
prefetched non-head frames use one range per 512 KiB, capped at four. After two
seconds without progress after dispatch, a pending range may race one duplicate
on the separate bulk connection. For slot reads with promotion tracking, a
prefetched range defers this recovery until its frame becomes the delivery head.
Promotion rechecks the primary's existing stall age and active status, rather
than imposing another two-second delay. Standalone callers without a nonnil
promotion channel retain their previous recovery policy. Headers and body bytes
reset the wire-progress clock, not an outstanding verified-block wait.
Token/permit/connection waits do not start recovery clocks. Completed ranges are retained.
Moving transfers without an actual headed verified-block gap do not trigger
duplicates. Tracked streaming heads additionally use the ordered-block recovery
signal described above. Generic slot-body reads also have a
two-second hedge; Drive frame
reads use the per-range layer rather than duplicating the entire frame. Priority
description reads have a 900 ms no-progress hedge starting at dispatch, not while
queued. Speculative descriptions do not hedge. Description recovery allows one
concurrent hedge per session, one dispatch per hedge, and no hedges for one minute
after a confirmed live rate limit. Each layer has a bounded hedge budget. Recent
403/429 rate limits also suppress media hedges and reduce new frames to one range.
<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:cf8fa32b6219364979e04b13c6b35546c9fc289ca59ea1263ca1cb09a97868fa -->

Media duplicates likewise get one dispatch, not their own retry loop. All Drive
read duplicates recheck active status and applicable progress at dispatch admission,
so a resolved head-block gap, wire progress or retry recovery can suppress a queued
duplicate. Legacy reads only hedge the first dispatch. For progressive
media, a dispatched application retry can use the frame's one rescue if it is
still unused. No recovery clock runs during retry backoff or admission, and
internal HTTP replays are still excluded. Tracked streaming heads may wait for
existing token replenishment; legacy spent timers do not. Recovery does not wait
out a quota cooldown to dispatch a duplicate.

<!-- /coderabbit-ai-generated:v1 -->
`hedge-deferrals` counts legacy timer-based nonhead recovery opportunities, not
saved API calls. Tracked streaming generation/budget waits are not included in
that counter; compare actual requests/units and block waits instead. Promotion
may admit a legacy deferred rescue later, with normal quota/budget checks.
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
or has three consecutive clean Drive absence observations may use a 2 s backoff
ceiling if the observed description frontier has not advanced for `eagerWindow`.
The existing exponential calculation remains (typically
1.2 s at the 300 ms pace floor), not a forced 2 s sleep. Invalid frames, errors,
previously-seen-slot 404s, and head/near schedules are unchanged. This reduces
repeated old-frame/empty-slot speculation during quiet peer periods. Promotion
still wakes the wait, but frontier progress alone does not: far-future visibility and body
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
wake deferred media recovery; the ordered-block signal prioritizes rescues, not
primary GETs. Primary requests are not restarted on promotion.
These diagnostic details add no API calls and do not themselves improve download
speed. Unobserved phases are reported as
unavailable (`-1ns`).
Sum units across operation types and peers when comparing usage
with the project's effective quota. These are estimates using the published
per-method model, not measurements of a grandfathered project's quota charges.

Exact-name Drive cache lookups use a direct map lookup instead of scanning the
whole shared name cache. Duplicate/missing-ID filtering is unchanged; this reduces
local work and lock contention without adding API calls.
Disposable idle connection probes use one application-level request-loop attempt.
After a transient failure, warming waits for later traffic or the next keeper tick;
foreground retries, token/ID refills, quota signaling, and transport-level replays
are unchanged.

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

### Bulk upload window (latencyWindow)

The ring is a FIFO byte stream, so a small frame completes only after every
frame written before it. Bulk frames in flight share one link (and Drive's
per-user throughput); beyond its bandwidth-delay product extra concurrency adds
no throughput and only queues, and every small frame behind pays that queue.
In the November 21 client log body latency rose from about 1.1 s to 2.7 s (max
3.4 s) at 465 frames/min, and 184 of the 194 reads blocked for over a second
sat behind a media frame.

`slot_latency_window.go` bounds bulk uploads by observed latency instead of a fixed
count: the window shrinks by a quarter
(at most once per base latency) when an operation takes more than 1.5x the
unloaded floor plus 100 ms, and grows by one when it is back within about 1.33x
of it. The floor is the 5th percentile of the last 256 samples. It starts at
its maximum, so an uncongested path behaves as before.

- Writer (`slotWriter.uplink`): the bulk upload window of `writeWindow`, steered
  by the latency of media/stream puts, never below the normal window.

What this cannot remove: a small frame still waits for the frame before it, so
behind a media frame it pays about one media latency (upload, description
discovery and a body GET, roughly 2.2 s) minus its own (about 1.1 s). Removing
that needs separate rings per flow class, which a byte-stream transport cannot
choose without knowing the sessions inside it.

### Ordered byte-credit prefetch

`slot_body_gate.go` owns a sequence-ordered media admission queue. Queued earlier
frames take priority over queued later frames, without restarting requests already
running. The scheduler also enforces the existing 20-frame body lookahead.

HTTP completion returns an active-request permit, but downloaded bytes keep their
credit until ordered handoff to the connection reader. Streamed blocks return
credit individually; legacy bodies return it at whole-body handoff. Shutdown and
failed reads reclaim remaining reservations. The head and its two successors can
borrow beyond the ordinary byte budget so prefetched tails cannot deadlock them.
Borrowing does not bypass Drive's HTTP or quota limits. Inline frames do not enter
the media scheduler, and storage ACK/reuse still requires validated completion.

The cold byte budget is 20 configured segment sizes, capped by ring depth. After
24 clean latency samples and an observed delivery interval, its target is recent
peak ordered throughput times the healthy fetch-latency floor, plus one segment
of handoff slack. The floor remains the 5th percentile of 256 samples; throughput
uses the last eight complete delivery intervals. The ordinary budget stays between
six and twenty configured segment sizes (or the smaller ring's limits).

Only validated first-primary dispatch-to-EOF timings train the Drive latency
floor. Retries, replays, dispatched rescues, and quota recovery invalidate a frame's
sample. Idle/outage intervals do not erase demonstrated delivery capacity. There
is no reader-side quarter-cut response to a single slow fetch. Generic stores use
successful retrieval duration when dispatch telemetry is unavailable.

Read logs retain `body-window` as the byte budget expressed in segment units and
add `body-active`, `body-outstanding-bytes` (reserved undelivered media payload),
`body-buffered-bytes` (received but not yet handed off), and `body-budget-bytes`.
These do not include bytes already queued in the connection reader channel, and
near-head borrowing can make outstanding bytes exceed the ordinary budget.

### Verified-progress media retries

Streaming primary retries resume at the first unverified block within their
original range. An incomplete block is never skipped. If every block arrived but
EOF validation failed, the retry re-reads a nonempty final block/suffix and must
validate its response length and EOF. Ignored Range responses are still fully
checksum-checked. Verified blocks remain deduplicated, and every original range
must complete successfully before storage credit/ACK is returned.

First-primary range geometry, retry-attempt limits, head-only adaptive hedge
deadlines, ordered-delivery gating, and the one-additional-GET-per-range hedge
budget are unchanged. Resuming reduces requested bytes where a verified prefix
exists; it does not reduce the flat 200-unit cost of a retry GET.

These changes address admission/recovery waste, not slow peer production, delayed
Drive visibility, or cross-flow blocking inside a shared mux carrier. Twitch or
other live-streaming improvement requires a matched live workload; none is claimed
from unit tests alone. No timeout or polling constants are changed.

<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:cb1130080237b200fa011794b9c50d9f224a24bff93b47e8e0076eb7916a9534 -->

### Backlog-aware inline publication

Drive writers use `slot_batch_window.go` to decide whether another buffered burst
fits inline. Admission accounts for buffered bytes, assigned/incomplete upload
bytes, and outstanding upload requests; a new input chunk does not get a fresh
window while the old work is still outstanding. An accepted inline burst retains
its byte-counted plan across later flushes, so changing pressure does not turn its
remaining suffix into media halfway through the burst.

The ordinary byte allowance is one normal upload window of inline payloads
(24 times 14 KiB by default), capped by ring depth; configured smaller segment
sizes are respected. It is resource credit, not an estimate of application demand.
Sparse control writes must not lower it: their publication rate measures how little
the application sent, not how much a full inline upload window can carry.
The separate bulk upload latency controller continues to handle measured congestion.

#### Current limit for new inline plans

The spare capacity allowance is capped at **32 frames** (448 KiB at the default
14 KiB inline payload). The 24-request inline window, pressure checks, and
committed plans still apply. A fresh 512 KiB burst uses the existing media path.
The 48-frame allowance and 512 KiB inline timings below describe older releases.

In the matched November 21 Twitch capture (client 19:21–19:24), nine inline
plans of roughly 496–512 KiB occupied 332 slots. During one such plan, reuse
wait reached 3.67 s; the following media frame waited 3.30 s for an upload
permit. On another plan a 4.58 s publication held already available frames
behind it; the largest observed wait behind earlier frames was 3.72 s.
Both sides logged zero Drive rate-limit responses. Reducing the number of writes
and ring slots limits this source of ordered stalls. It cannot remove Google's
response latency, and the media fetch can delay first bytes on healthy bursts.

Using the package's unit estimates, the nine bursts cost 18,260 units for
332 writes and one metadata read each. Nine media equivalents would cost
2,295–7,695 units with one to four primary media GETs, before polling, ACKs,
or retries. This is a request-count comparison rather than a combined quota
measurement. In a controlled 512 KiB burst with one delayed inline write, the
full burst plus a small trailing message delivered in 2.10 s versus 4.97 s.
Live playback with the new cap has not been measured.

#### Earlier tuning and measurements

<!-- /coderabbit-ai-generated:v1 -->

A lightly loaded publisher can reserve up to two normal inline upload waves,
capped by ring depth (48 times 14 KiB, or 672 KiB, by default). This requires no
more than half the normal request window and half its byte allowance already in
use. The new burst and outstanding bytes must fit that ceiling, and outstanding
requests reduce its frame credit. Inline publication has its own 24-request limit;
normal bulk admission remains 12 and its warmed adaptive ceiling remains 24. The
second inline wave waits for returned permits. A busy publisher retains its
one-wave byte allowance and request-pressure rule. Accepted plans remain
committed, but later input does not inherit unused burst credit automatically.

This targets the extra media-GET stage for modest bursts, not idle peer production
or outages. In the November 21 10:54 Twitch interval, a 192,207-byte frame waited
1.95 s for its first block while its media read took 0.93 s, with no admission wait.
The client alone cannot separate earlier production/upload from Drive visibility.
The larger inline allowance can avoid media retrieval when real producer-side
pressure permits; it does not claim to establish or remove every buffering cause.

The November 21 10:07–10:22 client log contains streamed frames around 160 KiB
whose media retrieval alone takes about one second. The previous demand-based
inline estimator could reduce the allowance to 84 KiB after sparse control traffic,
forcing an otherwise fitting burst through media. Removing that estimator preserves
the existing byte/request backlog checks and committed burst plans, without new
timeouts, polling changes, files, ranges, or recovery requests. Actual inline/media
traffic and quota units can change; replacing media with inline fan-out can increase
metadata/upload requests. Matched server logs and live playback are needed to measure
the benefit, not just the client-wide frame timings.

Admission previews the next publication class without committing a burst plan.
This lets a committed inline burst use the inline limit even after the aggregation
target has grown, but prevents a larger unplanned media upload from inheriting that
limit. This separation is necessary when input arrives in smaller chunks: enlarging
only the byte allowance would leave the old 12-upload bottleneck in place. Drive's
backend permit ceilings, range geometry, and download/recovery concurrency stay
unchanged. Other storage backends retain their original scheduling.

Unplanned backlogged bulk uses media and the full configured aggregation target,
allowing later chunks to coalesce rather than repeatedly creating inline waves.
Small suffixes remain inline; bursts of at most four inline payloads also retain
inline publication instead of spending a media GET on a short body. Existing
flush ticks, stable-suffix admission, bulk upload limits/latency window, range
plans, wire formats, and ACK/reuse fences are unchanged. Other slot stores retain
their original batching rule.

Write logs add `inline-budget-bytes` (ordinary allowance),
`inline-burst-budget-bytes` (light-load two-wave ceiling), and
`inline-planned-bytes` (committed inline bytes remaining after this assignment).
A lightly loaded committed burst can exceed the ordinary allowance. Non-Drive or
unassigned observations report `-1`. Publication completion is not proof of peer
visibility, and these are transport measurements, not browser/player deadlines.
Media conversion can increase GET units for individual bursts even when it reduces
upload fragmentation; no universal quota saving or playback improvement is claimed.

The published Drive limits were rechecked through the supplied local SOCKS proxy:
325,000 units/minute/user/project and 1,000,000 units/minute/project, with metadata
GETs costing 5, edits 50, and media GETs 200. The supplied client windows peak near
30,000 units/minute and contain no confirmed rate limits. Converting all 19
size-eligible media frames in the 95-second Twitch interval would add about 6,110
combined publication/one-discovery units under those prices at the 448 KiB ceiling:
12,100 additional server publication units minus 5,990 client retrieval units.
This is a size-only counterfactual, not actual eligibility or full accounting:
repeated metadata reads,
both peers, other applications, grandfathered quotas, and backend throttling still
matter. No per-request payload limit, backend permit ceiling, media-download
concurrency, or hedge budget is raised; only inline publication admission and
near-head metadata priming are wider.

There is no new quota limiter, persistent ledger, or quota-related admission code.
Concurrency and ring sizes do not bound requests per minute: faster responses and
additional carriers can spend more units with the same constants. A complete
client-plus-server worst-case ceiling cannot be guaranteed from this client log.
Both endpoints' dispatched units (including retries, hedges, and cleanup) and other
applications must be counted against the shared 325,000-unit limit. The existing
Drive cooldown and quota-retry handling remain unchanged.

Keep a 64-slot ring for this workload. Inline admission tops out at 24 and a fresh
two-wave plan at 48 frames; media lookahead tops out at 20 segment sizes and can
adapt lower. A deeper ring does not widen these windows or shorten Drive response
latency. It also requires more object creation/cleanup when fully used and delays
the two-lap bulk warm-up (128 full bulk frames at depth 64; 192 at depth 96).
Consider 96 only as a measured experiment if logs demonstrate sustained slot-reuse
`await` stalls while the peer drains promptly—not as a buffering fix or quota-safety
guarantee. The supported ring-depth cap remains 256; it is not a recommendation.
The earlier client capture has only four nonzero writer `await` observations, all
under one millisecond; there is no client-side ring-reuse bottleneck in this trace.

The matched resume.57 client/server capture contains 64 media frames, including
27 full 512 KiB frames: they exceed the previous 448 KiB inline-plan ceiling. Media
GET start to first verified block is about 0.88 s at p50, independently of peer
clock alignment; media admission is negligible. Inline admission is now 24, with
the same two-wave rule, so a lightly loaded 512 KiB frame can fit without changing
the 14 KiB per-request inline payload limit or media range/recovery budgets.

Near-head metadata priming now covers the head's 11 successors, matching the
existing 12-slot initial fetch window. The old three-successor setting made
already-published inline bursts drain in repeated deferred metadata waves. The
fetch horizon, polling/backoff intervals, idle behavior, and error handling remain
unchanged. A controlled 512 KiB test using 600 ms inline publication, 1.1 s media
publication, 300 ms metadata reads, and 900 ms body reads delivers first/full bytes
in 2.10/2.10 s with resume.57, 0.65/2.00 s with wider uploads alone, and
0.65/1.55 s with both changes. This is a model, not a live playback benchmark.

The endpoint timing-window peaks in that capture sum to 74,875 estimated units;
this is not a combined rolling-minute guarantee. A reconstructed producer-side
admission counterfactual converts 32 media frames (about 14 MiB), adding 50,300
server publication units and removing a net 18,370 client retrieval units over
the whole capture before changed repeated polls/retries. Actual eligibility can
change as the new pipeline runs. Importantly, server frame 2476 really receives
`userRateLimitExceeded` and takes 13.64 s to publish, blocking ordered delivery.
Increasing constants cannot eliminate a Drive rejection or promise buffer-free
playback. Verify the project's applied quotas: Google's published guide preserves
previously assigned quotas for eligible projects, and other applications share them.

### Browsing alongside fluent 360p (resume.59)

The matched November 21 client 13:02–13:10 and server 09:33–09:41 capture uses
session `fef6ba5732a9492b4176a7262f0962d8`, with all observed payload frames inline
and no recorded Drive rate-limit rejection. Server slot-reuse `await` reaches
5.40 s. Its continuously nonempty buffer episode reaches 142.67 s; this is an
episode-age upper bound, not proof that each selected byte waited that long.
The client excerpt also omits sequences 49–135, so its timestamp gap must not be
treated as a measured 36-second head stall.

A large backlogged flush could reject inline admission, wait for the bulk upload
window, then gain a fresh two-wave inline plan when outstanding requests fell below
half a window. Repeating this defeats aggregation and spends ring slots on small
frames. The bulk decision now survives that admission wait. Previously committed
inline prefixes and short suffixes remain inline; fresh lightly loaded bursts retain
the 24-upload, 48-frame two-wave allowance and 11-successor metadata priming.
No timeout, polling interval, Drive retry, range, or hedge limit changes.

A controlled 2 MiB transfer plus a browsing suffix formerly produced 148 inline
frames; the regression now requires 41 frames, including three aggregated full
segments, and verifies ordered bytes, publication modes, and the terminal frame.
The isolated 512 KiB delivery model remains 0.65/1.55 s for first/full bytes.
These are controlled transport results, not a measured website-load improvement.
Cross-flow ordering inside one mux carrier and Drive response delays still apply.
Routing remains controlled by the client configuration; this change adds no
domain exceptions or routing overrides.

Completed endpoint timing-window peaks sum to 92,435 estimated units, below the
published 325,000 units/minute reference but not a combined rolling-minute bound.
Aggregation reduces publication fan-out in the regression; actual combined usage
still includes both peers, metadata polling, media GETs, retries, and other apps.
Keep the deployed ring at 64 slots and validate browsing while 360p plays.

### Shared-owner live-ring cleanup fence

Drive slot connections pin their session in the shared storage owner through
connection teardown and worker joining. Whole-session cleanup must acquire an
exclusive fence before listing or deleting ring objects. Another listener sharing
that owner cannot collect a pinned ring, and a connection cannot bind a ring whose
cleanup has already started. A live rejection refreshes cleanup's locally observed
inactivity age without spending API requests or retry sleeps. Announcement cleanup
and unrelated sessions remain independent; released state is reclaimed.

The supplied November 21 server log matches both client sessions and shows
downstream assigned/buffered payload reaching 6 MiB. Both directions of the first
ring later report missing objects. It does not identify the deleting actor, so the
fence is not claimed to establish or fix that incident's cause. It protects only
listeners sharing one owner in one process—not other processes, external deletion,
permission loss, or a restart. Missing slots still fail closed; IDs are not blindly
recreated or changed underneath the peer.

Mux/carrier isolation remains outside this implementation. The carrier still
delivers one ordered stream, so ready small frames can remain blocked behind bulk.
For downstream batching benefits, upgrade the server writer as well as the client.

### Live segments: successor discovery during media reads

A live media segment larger than one frame is published as consecutive media
frames. Near successors normally poll at the 40 ms read-ahead pace only within
500 ms of the last *completed* frame; a verified block handoff refreshed
activity but not that clock, so during a roughly one-second media read the next
frame of the same segment was polled at the ordinary tail pace (up to 800 ms
plus one metadata request). While any media body holds a retrieval permit, the
first `slotFrontierProbeAhead` (3) successors of the head now use the read-ahead
pace. This is reader-local: no wire, configuration or integrity change. With
about 330 ms metadata reads it adds roughly five metadata GETs per second for each
connection while media downloads, about 1,600 units/minute at the published
5-unit price. Compare
`discover` for frames that follow a media head before and after.

### Single-wave inline publication

By default, an already committed inline plan of at most 32 frames can use up to
32 upload permits, instead of sending frames 25-32 after the first 24 uploads
return. Fresh plans are still admitted under the ordinary 24-upload window, and
Drive cooldown or a rate limit in the last five seconds restores that window.
The warmed bulk media window stays at 24. Request counts and frame boundaries are
unchanged; only peak concurrent PATCHes rise from 24 to 32, and their throttling
behavior on live Drive has not yet been measured. Setting
`XRAY_XDRIVE_INLINE_SINGLE_WAVE=0` before startup restores the two-wave window.
This is a writer-side change: for downstream video, upgrade the server. Compare
`upload-permit-wait`, the reader's `discover`/`blocked` timings and both peers'
`rate-limits`.

### Google Drive code layout

The Drive backend, media recovery, streaming negotiation, and telemetry live in
`drive.go`; their regression tests live in `drive_test.go`. Shared slot and
transport code remains separate because other storage backends also use it.
