# XDRIVE

xDrive carries an ordered byte stream in each direction. Use matching releases on
both peers. Drive uses slots; local, URL-template and S3 stores keep their WAL.
This repository is a review candidate based on the maintainer's fork, not a merge
of the current upstream branch.

## Drive protocol

The announcement names a table of file IDs and one reader-owned watermark for
**each direction**. IDs are allocated in batches. The first lap creates files
on first publication. Established sessions do not list files to find data:
sequence `n` maps directly to `slots[n % depth]`. If present, the announcement's
first payload is sequence zero, not an additional record.

Mode 1 carries up to 14 KiB in metadata. Mode 5 has a tagged media body and
can carry its first 14 KiB in the same publication's description. Modes 3 and
4 mean end and error. See "Transport model" below.

- **Write:** one goroutine batches caller input and assigns sequence numbers.
  Input is normally gathered for 25 ms. A small first write with no uploads
  in flight gets a 2 ms window outside cooldown. A quiet burst can use up to
  24 inline positions; during streaming the limit is four. There are at most
  32 upload workers per direction and never more than the ring depth.
  Reuse requires previous local completion and cumulative remote credit.
- **Read:** idle polling focuses on the head. After a minute without a head,
  one bounded background probe at a time rotates through other slot descriptions
  to distinguish a gap from a genuinely idle peer. During activity, up to
  24 known descriptions are prefetched, with one metadata GET per worker at a
  time. Repeated empty-slot polling reaches only four positions beyond the
  observed frontier (or head). A full inline slot allows a single earlier
  probe of each position in the rest of the 24-slot window.
  All media sizes may be prefetched; byte admission follows sequence
  order, bounded to 4 MiB per media connection and held until head promotion.
  Ordinary media has four HTTP permits per connection (capped at 64); head
  media has separate permits (up to four), including queued requests promoted
  to head. Both use the same connections; the head has its own frame allowance.
  A prefetched frame returns its local byte reservation when promoted, even if
  its body is still downloading. Completed successors retain theirs until promoted.
  Workers can finish out of order; one coordinator alone delivers bytes.
  Ordered delivery refreshes the eager timer; speculative body downloads do not.
- **Media:** frames through 1 MiB use one progressive GET; larger frames
  use up to four block ranges at once, capped by the media connection count
  and targeting at least 512 KiB of payload per range. A prefetched frame
  starts with its first two ranges; the ordered head can start all of them.
  If the first range returns a full HTTP 200 body, keep it and cancel the other
  ranges instead of downloading the full body four times. Validate every
  block's sequence tag, HTTP range/length and EOF. Resume an incomplete span
  without delivering the same bytes twice (see Fetch below). No media GET runs
  before discovering its frame description.
- **Close:** Close discards unread input, drains its writer and publishes END.
  It keeps verifying input and returning credit until it reads the peer's END,
  then flushes any remaining verified watermark. The peer may still have a full
  ring to drain after our writer finishes. Interrupt cancels instead.

Unlike v6, a unilateral graceful Close can time out if the peer never publishes
END. The one-minute watchdog tracks outbound progress until the local writer
finishes, then also counts successful inbound watermark writes during Close.
Receiving END alone does not close the outbound half.

## Transport model (Drive)

Each direction is a ring of `depth` Drive files plus one watermark file owned
by the reader. Sequence `n` always lives in `slots[n % depth]`.

1. **Gather.** The writer collects caller input for up to 25 ms or until the
   current frame size is reached. The 2 ms fast flush applies only when the
   initial write fits inline, no uploads are outstanding and Drive is not cooling down.
   With 4 or more uploads in flight the writer is streaming: it keeps
   gathering while input keeps arriving, up to 200 ms or a full frame, and
   sends once input pauses for 25 ms. Bytes that cross a full-frame boundary
   start the next gather; a small overflow does not cost an immediate extra slot.
2. **Frame size grows on demand.** It starts at `segmentBytes` (512 KiB)
   and doubles after each round that filled a whole frame, up to 4 MiB (below
   Drive's 5 MB multipart limit). Cooldown freezes growth. A full ring pauses
   the writer before it starts another upload. Delayed credit leaves the frame
   size alone rather than increasing the request count for the same payload.
3. **Publish.** Up to 14 KiB goes inline in a file description. With fewer
   than eight uploads in flight and outside cooldown, the writer splits a
   burst only if its entire run fits both free workers and known remote credit.
   Normally the limit is four (56 KiB); for a partial frame with fewer than four
   uploads and no burst over 56 KiB in the last eager window, it is 24 (336 KiB).
   A larger burst uses one media publication. Tags go directly into the multipart
   upload buffer, avoiding an intermediate copy of the entire media body.
   A media frame is one multipart create or PATCH: the description holds
   `XDRVSL04`, sequence, mode and total payload length. For media frames up to
   1 MiB it also holds the first 14 KiB of payload. The reader can deliver
   this prefix in order before the body GET returns; it costs no extra slot or
   request. The rest (all payload for larger frames) is stored in blocks of an
   8-byte sequence tag and up to 256 KiB of data. At most 32 uploads and
   4 MiB of unfinished uploads per media connection run per direction (one
   frame may exceed it alone). Media requests are charged by payload size plus
   one block of request overhead. A download chooses the lowest estimated
   completion time, rotating ties (XMUX `maxConnections`, default 4): one Google
   connection carries only a fraction of a link (a measured single-connection
   client download averaged ~0.22 MB/s). The byte bound,
   about one 4 MiB frame per connection, bounds payload bytes, not request count
   or duplicated network traffic. Head reads have separate HTTP permits, not
   extra connections. Bounded look-ahead overlaps service latency between frames
   instead of forcing one large frame to finish before starting the next.
4. **Discover.** The reader polls the head slot's description (25 ms while
   active; no duplicate hedged GET) and prefetches up to 24 descriptions. Slots
   beyond the observed frontier plus four wait without repeated polling. A full
   inline slot suggests a split burst, so each successor in the window can try
   once before the frontier reaches it. A prediction never counts as evidence of a gap.
   Bodies of all sizes download ahead within the byte budget, admitted in sequence
   order so distant successors cannot reserve it before nearer frames. The head
   bypasses that budget, and a queued media request switches to the reserved
   HTTP permits when promoted. Admission waits for the previous frame's ready
   signal, not its completed download.
   Promotion releases any existing look-ahead reservation, not remote ring credit.
   Thus a head discovered in advance no longer consumes both its own allowance
   and the successors' budget. Cancellation wakes waiters and retires the
   reader's private budget with the session.
5. **Fetch.** Media up to 1 MiB uses one streaming GET; larger frames use
   two to four block-aligned ranges with the default four connections,
   targeting at least 512 KiB per range and reaching four ranges at 2 MiB.
   The ordered head starts all its ranges immediately. A prefetched frame
   starts its first two; the others start when range zero receives its first
   tagged block or the frame becomes the head, whichever happens first. This keeps later
   ranges from taking as much bandwidth away from earlier bytes. Each block
   carries its own sequence tag. Promotion does not restart an existing GET.
   A full HTTP 200 response to the first range replaces all ranges; the others
   are canceled before any of their bytes are delivered. Bytes are delivered
   in pieces of up to 4 KiB at offset zero, then up to 32 KiB. A retry starts
   at the block containing the first undelivered byte. Interrupted body reads
   can use up to eight span attempts, stopping after two consecutive attempts
   emit no new payload. A second wrong tag, range or length still fails.
   The offset is relative to the media body, excluding any description prefix.
   Duplicate bytes are skipped; tags, declared length and exact EOF are checked.
   For a multi-range frame, the first range streams straight into the frame queue.
   Each span checks after five seconds without headers or new HTTP 206 body bytes.
   It can restart early only if its frame is the ordered head, the coordinator is
   draining this specific span, and Drive is not cooling down. There must be
   evidence of progress: a later range in this frame or a later media frame
   has verified through EOF, or a later inline frame has been read in full.
   A verified successor also covers the last range of a frame and a progressive
   GET waiting for headers. An interrupted header wait uses the existing HTTP GET retry budget;
   a partial body uses offset-aware resume. After a stall
   triggers a retry, subsequent attempts use the ordinary timeout. Any positive
   body read resets the idle clock. A full-body HTTP 200 retains the ordinary timeout.
6. **Credit.** The reader writes its cumulative watermark every `depth/6`
   frames or 1.5 s. One writer waits for cumulative credit in sequence order;
   a slot also waits for its previous local upload to finish. Each media frame
   is logged at debug level: `xdrive upload seq` (credit wait, PUT time) and
   `xdrive read seq` (time to find, admission/byte-budget wait, body time).
   `xdrive watermark seq N put P` records successful ACK publications, and
   `xdrive credit seq N ack A` records the writer observing newer credit.
   `xdrive span seq` records range status, wire bytes, time waiting for headers
   (including permit wait, retries and service latency), and time after headers.
   `xdrive span retry seq` records emitted payload bytes, attempt duration and the error that
   triggered a stream resume; successful final reads can hide this error.
   `xdrive request retry seq` exposes retries before headers or buffered success.
   `xdrive cooldown D` records the shared delay after quota/service rejections.

There are no CRCs: integrity relies on TLS, the length checks and the
per-block sequence tags, and VLESS encryption authenticates the payload. Both
peers must use the `XDRVSL04` format; upgrade them together from `.100`.

## Implementation and trade-offs

`slots.go` contains the frame codec, ring announcement and
publication, outbound writer, and inbound reader, in that order. Gathering
yields immutable chunks to the uploader. Each direction has a byte allowance;
promotion releases the head's reservation without canceling its media GET.
One reader ACK loop owns both periodic and final watermark writes. `drive.go`
owns OAuth, Drive requests, quotas, file operations, ID allocation, and media
verification. A storage profile tracks its open sessions, so a second listener
cannot collect their files while either peer still holds the profile locally.
`client.go` is shared with the URL-template and S3 transports. It retains the
configured TLS/QUIC dial path, socket and mask settings, stall timeout, HTTP/2
keepalive and XMUX leases. Drive uses distinct metadata, priority and media
pools. Metadata and priority select a lease once per session. Each media lane
keeps its assigned work until the body closes. Each complete tagged block measures
elapsed time against wire bytes read on that lane since the GET started, including
overlapping GETs and one start allowance. This updates the estimate while a slow
range is still in progress; only ordered delivery through EOF grants ring credit.
Each lane averages its estimate with the new sample. New lanes and lanes
without a sample in the last minute use the latest pool sample. Uploads
use outstanding work alone. Request/reuse/lifetime limits
still apply within each lane; the media byte budget follows the same count.
Peer-supplied IDs are checked for valid syntax and uniqueness. An existing
object must match its announced session name and configured parent on first
use. Keep the folder private to the two peers; these checks do not authenticate
a writer who already has access to the session folder.

Removed from the larger implementation: custom spare-connection probes,
background connection/token/ID keepers, eager spare rings, duplicated media
rescue and latency controllers, and detailed per-burst telemetry. A Drive ID
allocation failure now fails the dial explicitly instead of switching to WAL.
Announcements are also created with reserved IDs. After a lost reply, read back
the exact slot, watermark or inline announcement description; for a media
announcement also compare the complete body. Never replay an ambiguous write.
After an ambiguous create or PATCH the writer has up to 45 s to confirm the
exact contents across a reconnect, including data slots and watermarks. The
first check is immediate; failed checks back off from 200 ms to 2 s. There
is no added wait on a successful write. An unconfirmed write still fails the
connection. If an announcement was already deleted, a 404 is not proof of delivery. If confirmation fails,
the original write error is preserved. Graceful close reports final ACK errors. These are
behavior differences, not just source rearrangement. Historical throughput
and tail-latency measurements from the larger version are **not** measurements
of this one. Empty Drive announcements are rejected rather than opened as WAL
sessions. Uploads and OAuth refresh requests do not follow redirects.

## Failure handling and verification

A fatal upload, media-read or watermark error cancels both halves of a Drive
connection with the first error as the cause. A normal END only closes its
own direction. Readers reject an overwritten sequence; a writer verifies that
a cumulative ACK cannot cover its own unpublished sequence before reusing a slot.
Buffered GETs can retry an interrupted response body. A media announcement
is fetched by the file ID from the same listing that established its age and
uniqueness. Both admission and ambiguous-write confirmation require a whole
HTTP 200 response for that object. Streamed media resumes
by delivered offset, and an ambiguous mutation is not replayed. Listing and ID allocation
use the app-data space when `remoteFolder` is `appDataFolder`. Drive announcement
age is seeded from `createdTime` and the listing response’s `Date`, so an expired
announcement does not become fresh when a listener restarts. This is not a
persistent exactly-once claim within the TTL. An unprovable gap (no subsequent
frame) still looks like an idle connection; use a read deadline when one is required.

Transient OAuth transport and service failures can retry before any file operation
is sent. A token refresh has its own 30-second HTTP timeout; a stalled media
span cannot shorten it. Refreshes honor the shared cooldown and `Retry-After`.
All Drive file attempts and token refreshes, including retries, reserve spaced starts at
80 requests/s per shared storage instance, across sessions and lanes. Two peers
at this ceiling use about 9,600 attempts/minute against the observed 12,000
quota, leaving headroom, not a guarantee for other processes or profiles sharing
the project. Each admitted attempt reserves its own pacing turn and rechecks
cooldown before proceeding. Cancellation may leave an unused pacing turn.
Drive's HTTP/2 health check probes after 10 s of read idle and allows 45 s for
the PING reply before closing the shared connection. An explicit keepalive
period still overrides the idle interval; a negative period disables PINGs.
Drive rate-limit responses and 503 responses with `Retry-After` apply a shared
exponential cooldown (jittered 2–30 s base), reset after a minute without a new
cooldown. Concurrent rejections in one cooldown do not escalate independently;
a longer `Retry-After` still extends it. A 503 never permits mutation replay.
Quota retry logs omit the repetitive JSON error body. Cleanup excludes
folders and Google-native objects. Conflicting regular files with the exact same
announcement name are not selected or deleted as a group; resolve them manually.
The admission deadline also applies while loading or binding the announcement,
not to the resulting connection. Use a dedicated folder for one listener and its clients.

### Sustained downloads on `.105`

The matched 64-slot session carried 300.65 MiB in 143 media frames. Median
server publication took 1.42 s. Of 82 frames with at least three ranges, 42
had a third or fourth range finish before range zero. Frame 164's last range
finished 24.7 s after its first completion. Both peers kept the session and
neither logged a quota cooldown. The logs time Drive requests; they do not
measure Steam's instantaneous delivery rate.

Race-instrumented peers confirmed both ways to start the waiting ranges: after
the first tagged block while the preceding frame was still downloading, and on
promotion while that block was withheld. When a speculative range zero got a
full HTTP 200, its waiting ranges made no requests. All three 32 MiB + 329 byte
responses and their follow-ups matched the origin through EOF.

For the sustained-transfer comparison, four concurrent application downloads
each received 16 MiB + 329 bytes on one 64-slot session. The local HTTP/2
Drive service delayed media headers by 650 ms; connections were shaped at
1.1 MiB/s, with the slow connection, when present, shaped at 0.12 MiB/s.
The comparison used four batches per build and profile, reversing build order.
The last two columns are the median of each batch's slowest 1 MiB of ordered
application payload, measured after the first payload byte.

| Media connections | `.105` MiB/s | Updated MiB/s | `.105` slowest MiB | Updated slowest MiB |
|---|---:|---:|---:|---:|
| Four equal | 2.60 | 3.02 | 5.42 s | 3.61 s |
| One consistently slow | 1.51 | 1.50 | 15.20 s | 15.08 s |
| Slow connection changes every 8 s | 1.40 | 1.48 | 10.33 s | 8.87 s |

An additional 24-download run also moved the slow connection every eight seconds.
There the 95th-percentile time to receive the next MiB improved from 4.61 s
to 3.38 s, but the single slowest MiB took 10.92 s versus 10.02 s on `.105`.
Aggregate application throughput was 1.60 versus 1.63 MiB/s. These are
loopback delivery measurements, not a live bandwidth floor.

### Download throughput since `.101`

`.102` replaced `.101`'s quiet inline window with a four-slot maximum and
changed media selection from round-robin to the fewest outstanding requests.
`.103` retained those policies; `.104` changed outage and retry handling. In
the matched `.104` capture, 15 of 52 media frames took over 10 s to fetch.
Frame 101 took 1.20 s to publish and 35.99 s to fetch; frame 170 took 1.25 s
and 27.95 s. Neither peer logged a quota cooldown. Waiting to deliver an early
range also delayed credit for already downloaded successors.

On a loopback HTTP/2 Drive service with a 64-slot ring, `.101` carried an idle
192 KiB response in descriptions. `.102`, `.103` and `.104` used a media GET.
On 16 MiB responses all four releases assigned several MiB to the slowest
connection; the bulk comparison did not isolate a `.103`-specific regression.
With three connections shaped at 2.4 MiB/s and one at 0.18 MiB/s, three
consecutive 16 MiB + 329 byte responses on `.104` took 19.38, 25.08 and
31.20 s. With the current scheduler they took 24.98, 7.14 and 8.02 s. On
the first transfer there was no rate history. On the next two, the slow
connection received 0.08 and 1.08 MiB instead of 4 and 5 MiB. These are
application completion times checked byte-for-byte, not live Drive goodput.
With four equally shaped connections, alternating build order over two runs
gave median times of 5.80 s on `.104` and 5.90 s here (six responses per build).
For repeated 192 KiB responses with 220 ms metadata delay, the medians were
1.31 s and 1.33 s. The gain was on uneven connections after their rates were learned.

Race-instrumented peers also delivered two 8 MiB responses through a two-slot
ring, recovered four interrupted media reads and a lost watermark reply, and
rejected a repeated wrong block tag. Mixed `.104` and current peers transferred
16 MiB + 329 bytes and a follow-up with each version acting as the reader.

### Slow-link recovery

The paired client/server capture contained seven matching sessions. All six
client session failures reported `http2: client connection lost`: four PATCHes
(two explicitly identified as watermarks), one POST and one media read. In Go
1.27.4 this error is raised by the HTTP/2 PING health check. The server reported
`.103`; the client log did not include the release suffix. Neither logged a
Drive cooldown.

Race-instrumented peers on a loopback HTTP/2 Drive mock gave these results:

| Fault | `.103` | Slow-link change |
|---|---|---|
| Client-side traffic paused for 35 s during a write | Watermark failure and session loss at 31.5 s | Original connection and session delivered the interrupted response and a follow-up byte-for-byte |
| Committed slot write lost its reply; exact readback hidden for 20 s | Follow-up needed a new session | Confirmed at 21.2 s; follow-up used the same session. Both builds sent the mutation once. |
| Four interruptions in one media span, including a resume that emitted no new payload | Response ended early on the second cut | All 3,146,057 response bytes matched; five GETs for the interrupted span |

With a lost watermark reply and 20 s of stale readbacks, a 16 MiB + 329 byte
response and a follow-up also matched on one session, with one watermark
mutation for the affected sequence. A write that never committed failed after
45.1 s without replay. With just the original socket blackholed, the first
readback arrived over a working connection at 55 s. Two attempts with no new
payload or two wrong tags stopped the read; repeated progressing cuts stopped
on attempt eight. A 16 MiB + 329 byte response matched through EOF both when
the mock ignored Range and when a range trickled 4 KiB at a time for over 10 s.
The trickling range used one GET. These checks measure recovery, not live goodput.

### Verification of `.103`

The source inventory below uses the same counting rules as the 2,703-line
candidate and the 2,903-line `.102` release. The wire format remains `XDRVSL04`.

Race-instrumented peers transferred four concurrent reverse echoes on an
eight-slot ring. A stalled GET retried after 5.125 s with a later inline
frame; without a successor, a 6.1 s delay completed on the original GET.
A 16 MiB + 587 byte download matched after two cuts at GET wire offsets
0, 262,152, 262,152. Full HTTP 200 range fallback and readback after a lost
PATCH reply were byte-exact; a persistently wrong tag was rejected.

The first side-by-side HTTP/2 run compared the 2,703-line build with this
one, five measured responses per size per build. With 220 ms metadata delay,
the 179,017-byte median was 0.875 s and 0.781 s respectively. With one of
four media connections delayed by 550 ms and the others by 70 ms, the
32 MiB + 587 byte median was 1.657 s and 1.813 s. All 20 responses matched
the origin through EOF. These are loopback timings.

A repeat bulk comparison alternated build order across four blocks. With a
slow connection selected afresh for each response, the pooled medians (32
responses per build) were 1.7965 s and 1.7865 s. With the same connection
held slow across four consecutive responses, aggregate throughput over four
sequences per build was 17.682 and 17.379 MiB/s: the refactor was 1.7% lower
on that workload. All 96 responses in this comparison were byte-exact. Live
Google Drive throughput and browser latency were not measured.

On the final race build, an OAuth endpoint returning `Retry-After: 6` received
its next POST after 6.015 s for both 429 and 503. A seven-second refresh with
a verified successor completed once, and its token fetched the ordered head.
A matching HTTP 206 prefix neither admitted a media announcement nor confirmed
a lost write reply. Persistent 206s exhausted the 15 s confirmation window
without a second announcement POST.

Two listeners sharing a storage profile preserved active session files through
cleanup and listener replacement. A 1.84 s delayed ACK at application close
still allowed both Drive peers to publish END before collection. Local,
template and S3 WAL peers each echoed 255,540 bytes in each direction across
overlapping sessions and listener restart. No XDrive race was reported.

### Earlier release and candidate checks

The 2,703-line candidate kept `.102`'s wire format and transfer policies. Two
race-instrumented peers on a loopback HTTPS Drive mock delivered four concurrent
bidirectional payloads byte-for-byte through an eight-slot ring. With only a
later inline frame, a stalled GET retried after 5.17 s; without a successor,
a 6.1 s delay completed without retry. A 16 MiB + 587 byte download matched
after two cuts in a prefixed frame (wire offsets 0, 262,152, 262,152).
The same run exercised full HTTP 200 range fallback, exact readback after a
lost PATCH reply without replay, and rejection of a persistently wrong tag.

A separate HTTP/2 mock compared `.102` and the refactor with five measured
responses per size per version. With 220 ms metadata latency, the 179,017-byte
response took a median 0.766 s and 0.750 s respectively. With four media
connections (550 ms header delay on one, 70 ms on the others), a 32 MiB + 587
byte response took 1.735 s and 1.734 s. All 20 responses matched the origin.
These timings measure the loopback workload, not live Google Drive.

Two race-instrumented peers used a loopback HTTP/2 Drive mock. With a 220 ms
metadata delay, one 179,017-byte idle response used 14 downlink publications on
`.101` and five on `.102`; completion was 1.50 s and 0.83 s respectively.
For three 32 MiB + 587 byte responses per version, one of four media connections
delayed GET headers by 550 ms, the others by 70 ms. All payloads matched.
The delayed connection received nine GETs per transfer on `.101` and three
to four on `.102`. Median completion fell from 2.02 s to 1.83 s. This models
uneven connections; it does not measure sustained speed on live Drive.

A later inline frame alone allowed a blocked media GET to retry after 5.17 s;
a 6.1 s header delay with no successor completed on the first attempt. The
workload also covered a full HTTP 200 fallback, an ambiguous PATCH, a persistent
wrong tag, ring reuse and concurrent bidirectional transfers over Drive, local,
template and S3 stores. In a separate HTTP/1.1 run, a 14 MiB + 661 byte response
matched after two cuts in the same prefixed media frame: the three GETs began
at wire offsets 0, 262,152 and 262,152. No test files were added.

Two race-instrumented peers ran against a loopback TLS Drive mock. With a
1.2 s delay before media headers, the first 6,000 application bytes of a
110,003-byte response arrived in 0.11–0.33 s on `.101` versus 1.38–1.56 s on
`.100` (three requests per version). Completion was similar. A 32 MiB + 587
byte response matched on both: `.100` used six intervening inline slots between
successive full 4 MiB frames; `.101` used none. Elapsed time was 1.50 s and
1.56 s respectively. This checks batching; it does not establish higher speed.

A blocked progressive GET and a blocked final range retried in about 5.2 s
with a later verified frame. A six-second header delay without that evidence
and a slow full HTTP 200 fallback completed without early retry. A twice-truncated
prefixed body resumed byte-exactly; a persistent wrong tag failed after one
retry. The mock also covered a lost committed PATCH reply, concurrent echoes,
and concurrent local, template and S3 WAL transfers.

Two race-instrumented Drive peers delivered a 40,000-byte browsing tail after
bulk traffic as three inline slots, and confirmed a committed inline PATCH
after a 3.7 s readback delay without replay. A twice-truncated range resumed
at tagged wire offsets 0 and 262,152; its 20 MiB + 581 byte response matched
exactly. The same workload exercised a blocked middle range and a full HTTP
200 fallback. A persistently wrong sequence tag failed after one retry. Local,
template and S3 WAL peers also carried concurrent bidirectional requests; the
template mock observed both commit writes and matching commit reads.
With the final reader-owned byte reservations, two concurrent 21 MiB + 791 byte
downloads on one session also matched with speculative successors and two
partial-body resumes. These are correctness workloads, not a live speed test.

Two race-instrumented Go peers against a loopback TLS Drive mock delivered an
18,000-byte browsing response tail after a media upload via two inline descriptions
(14,336 and 3,841 transport bytes). Five 12 MiB + 581 byte responses on the
same session then exercised a blocked middle range before headers (one retry at
5.11 s) and after a partial body (one block-aligned resume at 5.20 s), no
verified later range, steady body progress and HTTP 200 fallback. The latter
three needed no early retry; all payloads matched. These are not live Drive timings.

Against a local TLS Drive mock, two Go peers transferred four 12 MiB + 581 byte
responses on one session. With a blocked first range and a verified successor,
the receiver issued one resume after 5.2 s, starting at the next tagged block
(wire offset 262,152). A steadily trickling first body, a silent first body
without a verified successor, and a first range answered with a full HTTP 200
all completed without a resume. All four payloads matched byte-for-byte.
This measures the injected stall, not live Drive latency.

The ACK recovery check used two Go peers and a local TLS Drive mock. A committed
watermark PATCH lost its reply; its readback was delayed 3.5 s. One session
delivered a 5 MiB + 301 byte response and 21 small responses byte-for-byte,
including a truncated media GET. A later watermark advanced without replaying
the ambiguous PATCH. Separately, an HTTP/2 mock held a PING acknowledgement
for 3.5 s during a Drive LIST; the next LIST used the same connection. Neither
check measures live speed.

The previous `.96` latency changes were built with `go build ./...` and vetted with
`go vet ./transport/internet/xdrive`. Two Go peers ran against a loopback
Drive HTTP/1.1 mock over TLS, with 161 concurrent requests: 160 small assets
and one 3 MiB + 301 byte response. All response bodies matched exactly despite
a lost committed-write reply and one media response cut after 4,119 wire bytes.
This exercises recovery and byte order, not live-Drive TTFB or throughput.

This version descends from the v7 candidate, which began as a literal merge
of v6's four slot files. The per-block CRCs, the checksum table and the
checksummed media reader are gone here. Retry safety rules remain, but media
reading, request pacing and shared cooldown now differ from v6.

The quota refinement passes `gofmt -l`, `go build ./...` and xdrive `go vet`.
A Python virtual-clock model checks pacing, cancellation, cooldown extension,
Retry-After, frontier progression and frame-size freezing; it is not a Go
HTTP/race workload or a live-Drive throughput benchmark. No test files are added.

The receive-credit refinement releases local look-ahead bytes on head promotion;
watermarks still advance only after verified ordered delivery and exact EOF.
No budgets, request limits or wire-format fields changed. Validate with slow
heads, completed successors, cancellation and unequal Close workloads under race
detection before attributing a sustained-speed improvement to this change.

The following are **Python model checks against the v7 candidate, not runs of
this Go package**:

- At ring depth two, the v6 and candidate models each delivered 1,096,163
  bytes in each direction, with separate deterministic payloads. All received
  bytes matched. The fixture crossed the inline, prefix and multi-range
  thresholds. It cut 16 model responses after a complete block plus 17 bytes;
  eight resumes simulated a server ignoring Range. No duplicated bytes.
- Seven scripted quota/status cases per version checked the retry decision for
  429, rate-limited and ordinary 403, and 500/503, including a 503 with
  Retry-After on a mutation. Delays used a virtual clock, not HTTP or Drive.
- A small exhaustive close model let the empty writer publish END before the
  opposite writer finished four data frames. Three v6 terminal states could
  not progress; none of the candidate's 45 reachable states was stuck. This
  is a bounded ordering check, not a goroutine race test.

**Release gates:** the candidate workspace had no Go 1.27 or `gofmt`, so the
model checks above did not build it. This snapshot was formatted with `gofmt`
(and the repository's `vformat`/`vprotogen` checks), vetted and built with the
`go.mod` toolchain. The loopback checks above use local mocks; they do not
measure live Drive throughput or page latency. The inventory below is the physical
post-`gofmt` count. No XDrive test file is shipped.

For deeper assurance, in a disposable checkout, run the same depth-two
transfers against v6 and the candidate with a fault-injecting Drive HTTP mock
under `go test -race`: compare both byte streams, truncate media responses,
return 429/403/503, and close both peers with unequal outstanding writes.
Also check one-sided Close, final ACK failure and Interrupt. A plain
`go test -race` without a workload is not a concurrency test. Repeat the
bidirectional transfer with a dedicated live Drive folder.

## Drive source budget (physical lines, including comments and blanks)

The cap is **2,500** physical lines.

Count all three implementation files in full, including the shared HTTP client.
For mixed files, the following inventory charges whole integration functions
where practical, including generic code within them. The remaining WAL/backend,
protocol-wide listener, configuration and generated protobuf code is shared or
unrelated, not a hidden second Drive implementation. Ranges below are inclusive
for this snapshot, relative to `transport/internet/xdrive` unless marked `infra/`.

| Source | Inclusive lines counted | Count |
|---|---|---:|
| `drive.go` | whole file | 914 |
| `slots.go` | whole file | 647 |
| `client.go` | whole file | 473 |
| `conn.go` | 8, 11, 33, 62–72, 74–80, 141–164, 166–180 | 60 |
| `xdrive.go` | 4, 7, 11, 114–178, 188–189, 197, 199–202, 222–223, 260–286, 288–333, 341–346, 366–394, 397–413, 415–434, 436–473 | 260 |
| `params.go` | 74–82 | 9 |
| `storage.go` | 9, 20–25, 29–34, 45–77 | 46 |
| `storage_lease.go` | whole file | 66 |
| `infra/conf/transport_method.go` | 1020–1027, 1037, 1045–1050 | 15 |
| `infra/conf/transport_internet.go` | 266–275 | 10 |
| **Total** | | **2500** |
