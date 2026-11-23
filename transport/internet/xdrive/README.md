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
- **Read:** only the ordered head is polled when idle. During activity, up to
  12 known descriptions are prefetched. An active head may start one extra
  metadata GET after 125 ms. Media lookahead has two credits held through frame
  retirement; the ordered head cannot be blocked waiting for those credits.
  Workers can finish out of order; one coordinator alone delivers bytes.
- **Media:** frames through 512 KiB use one progressive GET; larger frames can
  use four block-aligned ranges. Validate the checksum table, each emitted
  block, HTTP range/length and EOF. One retry can resume an incomplete span
  without delivering the same bytes twice. There is no speculative media GET.
- **Close:** publish END after draining prior uploads. On graceful connection
  close, stop the reader, take a final verified watermark snapshot, and attempt
  its write with a live, bounded context. Interrupt cancels instead.

## Implementation and trade-offs

`slots.go` and `slot_stream.go` own the wire codec. `slot_writer.go` and
`slot_reader.go` own sequencing, scheduling, and credit. `drive.go` owns OAuth,
Drive requests, quotas, object CRUD, ID allocation, and media verification.
`client.go` is shared with the URL-template and S3 transports. It retains the
configured TLS/QUIC dial path, socket and mask settings, stall timeout, HTTP/2
keepalive and XMUX leases. Drive uses distinct metadata, priority and media
pools. A lease is selected once instead of sampling and rejecting candidates.

Removed from the larger implementation: custom spare-connection probes,
background connection/token/ID keepers, eager spare rings, duplicated media
rescue and latency controllers, and detailed per-burst telemetry. A Drive ID
allocation failure now fails the dial explicitly instead of switching to WAL.
An ambiguously completed slot or watermark write is checked by reading its exact
description back from the known ID, without replaying it. If confirmation fails,
the original write error is preserved. Graceful close reports final ACK errors. These are
behavior differences, not just source rearrangement. Historical throughput
and tail-latency measurements from the larger version are **not** measurements
of this one. This candidate has not been built or benchmarked here.

## Drive source budget (physical lines, including comments and blanks)

Count all six implementation files in full, **including the shared HTTP client**.
For mixed files, the following inventory charges whole integration functions
where practical, including generic code within them. The remaining WAL/backend,
protocol-wide listener, configuration and generated protobuf code is shared or
unrelated, not a hidden second Drive implementation. Ranges below are inclusive
for this snapshot, relative to `transport/internet/xdrive` unless marked `infra/`.

| Source | Inclusive lines counted | Count |
|---|---|---:|
| `drive.go` | whole file | 847 |
| `slots.go` | whole file | 202 |
| `slot_reader.go` | whole file | 393 |
| `slot_writer.go` | whole file | 341 |
| `slot_stream.go` | whole file | 136 |
| `client.go` | whole file | 522 |
| `conn.go` | 65–78, 184–212, 214–238 | 68 |
| `xdrive.go` | 135–226, 325–424, 490–504, 506–530, 557–589, 605–707 | 368 |
| `params.go` | 74–88 | 15 |
| `storage.go` | 34–61, 63–73 | 39 |
| `storage_lease.go` | 26–32, 42–45, 47–63, 69–70, 89, 157–162 | 37 |
| `infra/conf/transport_method.go` | 1020–1027, 1037, 1045–1050 | 15 |
| `infra/conf/transport_internet.go` | 266–275 | 10 |
| **Total** | | **2993** |
