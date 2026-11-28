# XDrive

XDrive carries an ordered byte stream in each direction. Google Drive uses the
`XDRVGET1` media protocol. Both peers must use matching builds and have access
to the same private Drive folder. Local, URL-template and S3 storage use a
write-ahead log.

## Google Drive sessions

The client reserves IDs, creates a ring of reusable files and one acknowledgement
file (the *watermark*) per direction, then publishes a session announcement. Its
description is `XDRVGET1`; its media body is a JSON object containing the two ID
tables and the tick interval in milliseconds. The listener lists announcements
and checks the supplied IDs against the expected file names in the session's
folder before accepting the session. These are setup operations.

During the session, the receiver requests **`files.get?alt=media` directly by
ID** for sequence `n` at `slots[n % depth]`. It needs no listing or descriptor
GET to discover application data. Reads for later sequences can overlap a slow
head; only the head delivers bytes to the stream.

| Drive setting | Default | Meaning |
|---|---:|---|
| `concurrency` | 100 | Slots per direction, bounded to 2–256. |
| `flushIntervalMs` | 100 ms | Client-announced cadence in both directions, bounded to 25–1,000 ms. |
| `segmentBytes` | 2 MiB | Maximum payload per tick, up to 2 MiB. A short write need not fill the frame. |

On each writable tick, the sender publishes data, an explicit empty frame, or
FIN after its input closes. Up to 96 MiB of payload can await acknowledgement.
If the byte window or ring is full, the sender waits rather than overwriting
unread data. The receiver predicts the next GET from its own monotonic clock.
A stale generation causes a retry of that ID; consecutive misses adjust future
GET times. There is no fixed one-second wait or requirement to synchronize clocks.

## Media, acknowledgement and recovery

- A data frame consists of payload blocks of up to 16 KiB. Each has a 64-byte
  header identifying the protocol, kind, sequence, total payload size, block
  offset and CRC-32C. Verified blocks can be delivered before the GET finishes.
  INIT, EMPTY, READ and FIN have a header only. EMPTY for the expected sequence
  advances the stream; INIT or READ for an older sequence does not.
- After ordered receipt, one multipart `PATCH` sets the slot description to
  `READ:n` **and replaces the media body with the 64-byte READ marker**. Once a
  batch of contiguous clears has committed, the receiver publishes its highest
  sequence in the watermark. Reuse requires both this acknowledgement and
  completion of the sender's own previous mutation of that slot.
- An interrupted media GET resumes at the first undelivered block. A head GET
  with no progress for five seconds can retry on another media connection;
  large remaining spans use two disjoint ranges. Each returned block must match
  its generation and offset, even if Drive answers a range with a full `200`.
- Ambiguous writes are confirmed by readback, never blindly replayed. For a
  publication, a covering receiver watermark also proves receipt. This covers
  an announcement whose reply was lost after the listener removed the file.
  Requests and token refreshes share quota pacing and honor `Retry-After`.
- Graceful close drains through FIN in both directions, including final clears
  and acknowledgements. The listener sweeps abandoned session files by scope.

## Drive source budget (physical lines, including comments and blanks)

The target is below 2,000 lines after repository formatting. The inventory counts
the three implementation files in full and the integration code listed below.
Ranges are inclusive and relative to `transport/internet/xdrive` unless marked `infra/`.

| Source | Inclusive lines counted | Count |
|---|---|---:|
| `drive.go` | whole file | 652 |
| `slots.go` | whole file | 537 |
| `client.go` | whole file | 417 |
| `conn.go` | 8, 31, 59–64, 66–72, 133–153, 155–166 | 48 |
| `xdrive.go` | 4, 10, 13, 113–160, 169–173, 179–182, 202–203, 235–287, 289–312, 314–332, 334–375 | 200 |
| `params.go` | 74–83 | 10 |
| `storage.go` | 10, 20–25, 29–34, 45–78 | 47 |
| `storage_lease.go` | whole file | 61 |
| `infra/conf/transport_method.go` | 1020–1027, 1037, 1045–1050 | 15 |
| `infra/conf/transport_internet.go` | 266–275 | 10 |
| **Total** | | **1997** |
