# XDRIVE

XDrive carries one ordered byte stream in each direction. The Google Drive backend
uses a ring of files; local, URL-template and S3 storage use a write-ahead log.
Use matching releases on the two peers and a private Drive folder for the listener
and its clients.

## What can Google Drive metadata do?

| Drive request | What comes back | What XDrive uses it for |
|---|---|---|
| `GET /drive/v3/files/{id}?fields=description` | A JSON string field, **without downloading the file** | Frame header and up to 14 KiB of early payload. A small frame needs no media request. |
| `GET /drive/v3/files?fields=nextPageToken,files(id,description)` | The selected metadata for every matching file on the page | Discover a session and, when it fits, receive its first bytes in the listing itself. |
| `GET /drive/v3/files/{id}?alt=media` | Binary file content, optionally a byte range | Stream the remainder of a large frame. |

Google calls `description` a “short description” and does not specify a numeric
maximum in its v3 File reference. **14 KiB is XDrive’s own raw-payload limit**,
before the header and Base64 encoding. A Drive custom `property` is different:
Google limits the key and value **combined** to 124 UTF-8 bytes. Public properties
are visible across apps with file access; `appProperties` are private to the app.
Descriptions can be read with metadata-only permissions. The bytes stored there
are encrypted transport payload, just like the bytes stored in media.

[File fields](https://developers.google.com/workspace/drive/api/reference/rest/v3/files) ·
[Field selection](https://developers.google.com/workspace/drive/api/guides/fields-parameter) ·
[Custom property limits](https://developers.google.com/workspace/drive/api/guides/properties) ·
[Multipart uploads](https://developers.google.com/workspace/drive/api/guides/manage-uploads) ·
[Range downloads](https://developers.google.com/workspace/drive/api/guides/manage-downloads)

## Transport model

1. **Open.** The client reserves Drive file IDs and announces two lists of slots
   and two watermarks (one per direction). Each peer creates its files on first
   use. Sequence `n` lives in `slots[n % depth]`, so established sessions can read
   files directly by ID. A peer-supplied existing file must match the expected
   session name, parent and regular-file type on first use.
2. **Write.** Gather short writes briefly. Small frames go entirely in the
   description. Larger ones use one multipart publication for the description
   and binary body; they cannot be updated independently by the sender. Frames
   through 1 MiB carry their first 14 KiB in the description. Media blocks carry
   the sequence number so a resumed read cannot splice bytes from an old lap.
3. **Read ahead, deliver in order.** Look up successive descriptions independently.
   A complete inline slot counts as evidence immediately, even across a hole.
   A discovered media frame can start whenever there is room in the look-ahead
   budget (4 MiB per media connection). No worker waits for the preceding
   description to reserve bytes; the head has its own allowance. Only ordered
   delivery advances credit. Large bodies use parallel ranges; check their
   sequence, range, length and exact EOF.
4. **Bound the queue.** The reader reports the last delivered sequence and the
   rate at which it delivered ordered bytes. The sender uses that rate for both
   frame size (256 KiB–4 MiB) and the outstanding-media window. It cannot reuse
   a position until its previous upload has finished locally **and** the peer has
   acknowledged it. A fast upload alone says nothing about download speed.
5. **Close.** END closes one direction. Graceful close drains outgoing bytes;
   it continues verifying incoming bytes and returning credit through peer END.
   Interrupt or a fatal error cancels the session.

### When a request stalls

- **Media:** if the ordered GET receives no new bytes for five seconds and later
  data has verified, it can resume from a tagged block on another connection.
  Already delivered bytes are skipped. A continuously progressing GET keeps its
  connection. HTTP 200 in response to a Range request means a complete body, not
  the requested slice.
- **Metadata:** later sequences can expose a stuck head lookup. For a frame over
  1 MiB the next slot can also carry its predecessor’s descriptor in `xdrivePrev`.
  The reader may then fetch the original, already scoped slot directly. It checks
  the whole body before exposing any bytes from that recovery read.
- **Lost write reply:** read back the publication. For ordinary data after
  sequence zero, a validated covering receiver watermark can also prove delivery,
  independently of a stuck readback. Neither path resends an ambiguous mutation.
  Sequence zero can arrive in the announcement; its ACK cannot prove a slot write.
- **Quota and outages:** file requests and token refreshes share request pacing
  and cooldown, including `Retry-After`. Short stall recovery is disabled during
  cooldown. The HTTP progress watchdog measures inactivity rather than total
  transfer time.

### Measurements

The [`.109` measurements and earlier comparisons](https://github.com/anonymously-hidden/Xray-core/blob/v26.10.31-resume.109/transport/internet/xdrive/README.md#removing-ordered-waits-in-109)
include paired Drive traces and the single-session page, sustained-download,
interrupted-read and lost-write fixtures. Local fault injection measures the
transport; it cannot attribute a live HTTPS document’s entire first-byte delay
to Drive or to its origin.

In a two-media-connection fixture the head GET was held for up to 12 seconds;
the two nearest successor bodies could not finish until the head did, but the
third successor was readable. `.109` delivered the four frames and EOF in
12.11 s. Admitting discovered media by available bytes allowed the third
successor to verify and the idle head to retry. The same byte stream and EOF
arrived in **5.14 s** under the race detector. Promotion leaves a healthy
metadata GET running; verified later data still permits retrying an idle head.

| Browsing during a download on one session | Tagged `.109` (two runs) | Current candidate (one run) |
|---|---:|---:|
| Flaky 500 KB/s: browsing first-byte p95 | 12.10–16.12 s | 10.95 s |
| Same workload: browsing completion p95 | 12.10–16.12 s | 17.16 s |
| Same workload: bulk goodput, including startup | 246.7–249.5 KB/s | 246.5 KB/s |
| Fast 128 MiB transfer: bulk goodput, including startup | 181.44–221.21 Mbit/s | 216.39 Mbit/s |

All 36 candidate probes overlapped the downloads and matched byte-for-byte.
These local fixture runs do not establish a live Drive latency or throughput guarantee.

Two simplifications were tried separately against `.109` and rejected:

- With just one GET per existing frame, a paused 1,310,757-byte head with no
  successor stayed silent for 20.00 s. With parallel ranges, a later range of
  the **same frame** provided evidence to resume, cutting that wait to 5.20 s.
- Retaining the announcement as the cleanup inventory took 91 more counted lines
  once a delayed client could no longer create an orphan after cleanup. Cold
  first-byte time on a 70 KiB transfer rose from 372 to 639 ms (medians of three
  loopback runs per build with 80 ms per Drive request).

## Drive source budget (physical lines, including comments and blanks)

The target is **below 2,000** physical lines after repository formatting. The
three implementation files are counted in full, including the shared HTTP client.
Mixed files count their Drive integration as below; ranges are inclusive and
relative to `transport/internet/xdrive` unless marked `infra/`.

| Source | Inclusive lines counted | Count |
|---|---|---:|
| `drive.go` | whole file | 839 |
| `slots.go` | whole file | 678 |
| `client.go` | whole file | 447 |
| `conn.go` | 8, 31, 60–65, 67–73, 134–154, 156–168 | 49 |
| `xdrive.go` | 4, 7, 11, 14, 114–175, 184–188, 194–197, 217–218, 250–302, 304–325, 327–345, 347–388 | 213 |
| `params.go` | 74–82 | 9 |
| `storage.go` | 10, 20–25, 29–34, 45–78 | 47 |
| `storage_lease.go` | whole file | 61 |
| `infra/conf/transport_method.go` | 1020–1027, 1037, 1045–1050 | 15 |
| `infra/conf/transport_internet.go` | 266–275 | 10 |
| **Total** | | **2368** |
