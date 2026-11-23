# XDRIVE

xDrive carries an ordered byte stream in each direction. **Deploy v8-protocol builds to both
Drive peers.** There is no negotiation or v7 fallback. Drain old sessions and
use a fresh private Drive folder for the cutover. Local, S3 and URL-template
backends still use their existing WAL protocol.

## Drive communication model

1. **Announce.** The dialer reserves all file IDs in one batch and publishes
   the announcement before returning a connection. Its JSON has `v: 2` and
   one `ids` array. Each half contains its direction's slot IDs followed by
   the reader-owned watermark ID; uplink comes first. Depth defaults to 8,
   with a minimum of 2. `concurrency` sets depth, not the number of uploads.
   The listener checks the version, count, ID syntax and uniqueness before
   binding IDs. The announcement never contains stream bytes. Only a slot's
   first publication creates it; subsequent laps update the same file.
2. **Write.** One writer gathers small writes until `flushIntervalMs` or a
   full segment, then uploads in order. Before reusing position `n % depth`,
   it must have a cumulative ACK of at least `n - depth`. An ACK at or above
   `n` is invalid: the writer hasn't published that frame yet. This one
   number prevents overwrites; there are no per-slot or substream windows.
3. **Read.** One reader polls the expected ID, sleeping for `maxPollIntervalMs`
   after a miss or an old lap (normally 200 ms by default). It immediately
   tries the next ID after a successful frame. It neither lists for data nor
   prefetches. A not-yet-created first-lap file is normal. A valid old lap
   can also mean idle indefinitely; use a read deadline when needed. A frame
   for the wrong position, a later sequence, or an empty existing description
   is an error, not a reason to poll forever.
4. **Verify and ACK.** Frames use `XDRVSL02`, a big-endian 64-bit sequence,
   one mode byte, a 32-bit payload length and a whole-payload IEEE CRC32.
   Mode 1 puts up to 14 KiB inline; mode 2 puts larger payloads in media and
   carries only the header in metadata. Modes 3 and 4 are END and error.
   There are no block tables, prefixes or range requests. A media GET checks
   status, exact length, EOF and CRC before handing off a single byte. A
   failed body attempt may restart from zero. Each delivered frame earns one
   watermark write. CRC32 detects accidents, not malicious modification.
5. **Close.** During graceful close the reader discards unread input but
   still verifies it and returns credit. Local END does not stop the reader:
   the other writer may need another lap to finish. A peer that never sends
   END can cause Close to time out. Interrupt cancels instead. The watchdog
   tracks outgoing progress, then inbound credit once the local writer is done.

This trades peak throughput and first-byte latency on large frames for fewer
moving parts. Idle connections poll, ACK writes are more frequent, and a
broken media GET can redownload a whole segment (up to 16 MiB). Small writes
can coalesce; idle writers do not upload empty frames. There are no performance
measurements for this version.

## Implementation and failure handling

`slots.go` owns the wire codec, ring and two sequential workers. `drive.go`
handles OAuth, file operations, quota cooldowns and buffered media reads.
`client.go` is shared with the other HTTP backends; Drive uses metadata and
media pools. VLESS encryption remains required. Use a private folder dedicated
to one listener and its clients. The listener checks names and folder membership
on first access to peer-selected existing objects; these checks do not
authenticate another writer with access to the folder.

The two directions share a fatal error cause. Normal END only closes its own
direction. Explicit rate-limit rejections may retry; an ambiguous mutation
is not replayed. For a lost reply, read back the reserved object's exact
description (and complete body for a media announcement); a 404 does not prove
success. Transient token failures can retry before sending a file operation.
The shared cooldown also honors `Retry-After` on 503, without treating a 503
as proof that a mutation did not commit. Cleanup excludes native Drive objects
and does not resolve duplicate announcement names by choosing one arbitrarily.

## Cleanup and verification (v8.3)

An expired announcement is removed by its listed file ID on Drive. It no
longer triggers deletion of the entire stream. The existing periodic collector
is the only path that removes stream files: active sessions and sessions with
an observed unexpired announcement reset the inactivity timer. Where several
announcement names refer to the same session, the latest admission deadline
wins. This listener policy is shared with the WAL backends. Orphaned files may
therefore remain longer before deletion. The transfer wire format is unchanged.

Only the stream being deleted is reserved against admission. A fresh notice
observed while a different stream is being deleted invalidates the waiting
candidate; the collector checks it again under the listener lock immediately
before reserving it. A notice observed after that reservation does not undo an
in-flight deletion. The per-session Drive storage guard remains in place.

A two-candidate Python model reproduced the premature batch reservation in
v8.2. Ten interleavings of notice, admission and deletion were checked for
v8.3, including admission while an earlier delete was in flight. A separate
control confirmed that genuinely orphaned candidates still get deleted.

Offline regressions passed: 2,857,568 exact bytes each way at ring depth two,
interrupted and corrupt media, watermark recovery, quota responses, and 27
simultaneous-close scenarios. These are Python models and source-translated
codec checks, not executions of the Go package. The sandbox has neither Go
1.27 nor gofmt. Build, vet, race checks, Go HTTP-mock tests, and a live Drive
transfer remain open. No XDrive test files are shipped.

## Drive source budget (physical lines, including comments and blanks)

Count all three implementation files in full, including the shared HTTP client.
For mixed files, the following inventory charges whole integration functions
where practical, including generic code within them. The remaining WAL/backend,
protocol-wide listener, configuration and generated protobuf code is shared or
unrelated, not a hidden second Drive implementation. Ranges below are inclusive
for this snapshot, relative to `transport/internet/xdrive` unless marked `infra/`.

| Source | Inclusive lines counted | Count |
|---|---|---:|
| `drive.go` | whole file | 884 |
| `slots.go` | whole file | 501 |
| `client.go` | whole file | 500 |
| `conn.go` | 64–78, 184–216, 218–234 | 65 |
| `xdrive.go` | 9, 139–209, 211–214, 229, 231, 265, 266, 313–402, 411–425, 427–444, 446–461, 487–519, 535–616 | 334 |
| `params.go` | 73–84 | 12 |
| `storage.go` | 9, 20–25, 37–64, 66–76 | 46 |
| `storage_lease.go` | 26–32, 42–45, 47–63, 69–70, 89, 157–162 | 37 |
| `infra/conf/transport_method.go` | 1020–1027, 1037, 1045–1050 | 15 |
| `infra/conf/transport_internet.go` | 266–275 | 10 |
| **Total** | | **2404** |

This counts the archived source, not a certified post-`gofmt` result. The
queue worker is charged in full because it now passes the pinned Drive ID.
