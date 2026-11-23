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

The frame magic (`XDRVSL01`), sequence and length fields, JSON keys, and modes
are unchanged. Mode 1 carries up to 14 KiB in metadata. Modes 5 and 6 carry a
body with one CRC32 per 32 KiB block; mode 6 additionally carries an 11 KiB
prefix in metadata. Modes 3 and 4 mean end and error. CRCs detect accidental
corruption, not malicious modification; VLESS encryption remains required.

- **Write:** one goroutine batches caller input and assigns sequence numbers.
  After idle, a small write has a 2 ms gather window. The planner uses up to 24
  available inline positions; larger bursts can use media. There are at most
  32 upload workers per direction and never more than the ring depth. Reuse
  requires both previous local completion and cumulative remote credit.
- **Read:** idle polling focuses on the head. After a minute without a head,
  one bounded background probe at a time rotates through other slot descriptions
  to distinguish a gap from a genuinely idle peer. During activity, up to
  12 known descriptions are prefetched. An active head may start one extra
  metadata GET after 125 ms. Media lookahead has two credits held through frame
  retirement; the ordered head cannot be blocked waiting for those credits.
  Workers can finish out of order; one coordinator alone delivers bytes.
- **Media:** frames through 512 KiB use one progressive GET; larger frames can
  use four block-aligned ranges. Validate the checksum table, each emitted
  block, HTTP range/length and EOF. One retry can resume an incomplete span
  without delivering the same bytes twice. There is no speculative media GET.
- **Close:** Close discards unread input, drains its writer and publishes END.
  It keeps verifying input and returning credit until it reads the peer's END,
  then flushes any remaining verified watermark. The peer may still have a full
  ring to drain after our writer finishes. Interrupt cancels instead.

Unlike v6, a unilateral graceful Close can time out if the peer never publishes
END. The one-minute watchdog tracks outbound progress until the local writer
finishes, then also counts successful inbound watermark writes during Close.
Receiving END alone does not close the outbound half.

## Implementation and trade-offs

`slots.go` contains the frame and checksum codec, ring announcement and
publication, outbound writer, and inbound reader, in that order. One reader
ACK loop owns both periodic and final watermark writes. `drive.go` owns OAuth,
Drive requests, quotas, object CRUD, ID allocation, and media verification.
`client.go` is shared with the URL-template and S3 transports. It retains the
configured TLS/QUIC dial path, socket and mask settings, stall timeout, HTTP/2
keepalive and XMUX leases. Drive uses distinct metadata, priority and media
pools. A lease is selected once instead of sampling and rejecting candidates.
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
If an announcement was already deleted, a 404 is not proof of delivery. If confirmation fails,
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
Buffered GETs can retry an interrupted response body. Streamed media resumes
by block, and an ambiguous mutation is not replayed. Listing and ID allocation
use the app-data space when `remoteFolder` is `appDataFolder`. Drive announcement
age is seeded from `createdTime` and the listing response’s `Date`, so an expired
announcement does not become fresh when a listener restarts. This is not a
persistent exactly-once claim within the TTL. An unprovable gap (no subsequent
frame) still looks like an idle connection; use a read deadline when one is required.

Transient OAuth transport and service failures can retry before any file operation
is sent. Drive rate-limit responses and 503 responses with `Retry-After` apply a
shared cooldown; a 503 is not permission to replay a mutation. Cleanup excludes
folders and Google-native objects. Conflicting regular files with the exact same
announcement name are not selected or deleted as a group; resolve them manually.
The admission deadline also applies while loading or binding the announcement,
not to the resulting connection. Use a dedicated folder for one listener and its clients.

### Verification of this candidate

The first step was a literal merge of v6's four slot files: every declaration
body was byte-identical before subsequent edits. The merge alone changed the
count from 2,998 to 2,972. The HTTP retry and media-read implementation in
`drive.go` remains byte-identical to v6.

The following are **Python model checks, not runs of the Go package**:

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

**Release gates still open:** this workspace lacked Go 1.27 and `gofmt`.
Neither version was compiled or vetted, run against an HTTP mock in Go,
run with `-race`, or tested against live Drive. The inventory below is the
physical count in this archive, not a certified post-`gofmt` count. No XDrive
test file is shipped.

With the `go.mod` toolchain and dependencies, format the changed Go files,
recount, then run `go build ./transport/internet/xdrive` and
`go vet ./transport/internet/xdrive`. In a disposable checkout, run the
same depth-two transfers against v6 and the candidate with a fault-injecting
Drive HTTP mock under `go test -race`: compare both byte streams, truncate
media responses, return 429/403/503, and close both peers with unequal
outstanding writes. Also check one-sided Close, final ACK failure and
Interrupt. A plain `go test -race` without a workload is not a concurrency
test. Repeat the bidirectional transfer with a dedicated live Drive folder.

## Drive source budget (physical lines, including comments and blanks)

Count all three implementation files in full, including the shared HTTP client.
For mixed files, the following inventory charges whole integration functions
where practical, including generic code within them. The remaining WAL/backend,
protocol-wide listener, configuration and generated protobuf code is shared or
unrelated, not a hidden second Drive implementation. Ranges below are inclusive
for this snapshot, relative to `transport/internet/xdrive` unless marked `infra/`.

| Source | Inclusive lines counted | Count |
|---|---|---:|
| `drive.go` | whole file | 972 |
| `slots.go` | whole file | 947 |
| `client.go` | whole file | 500 |
| `conn.go` | 65–79, 185–217, 219–244 | 74 |
| `xdrive.go` | 9, 139–223, 276, 323–413, 479–490, 492–512, 538–572, 588–670 | 329 |
| `params.go` | 74–88 | 15 |
| `storage.go` | 9, 20–24, 36–63, 65–75 | 45 |
| `storage_lease.go` | 26–32, 42–45, 47–63, 69–70, 89, 157–162 | 37 |
| `infra/conf/transport_method.go` | 1020–1027, 1037, 1045–1050 | 15 |
| `infra/conf/transport_internet.go` | 266–275 | 10 |
| **Total** | | **2944** |
