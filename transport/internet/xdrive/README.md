# XDrive

XDrive carries an ordered byte stream in each direction. Google Drive uses a ring
of reusable files; local, URL-template and S3 storage use a write-ahead log.
Use matching builds on both peers and a private Drive folder shared by the listener
and its clients.

## What a Drive slot contains

The `description` field holds a Base64-encoded frame header and, for small frames,
payload bytes. `GET /drive/v3/files/{id}?fields=description` reads **only metadata**.
The binary file body (media) is fetched separately with `?alt=media`.

| Frame size | Payload in `description` | Payload in media |
|---|---|---|
| Up to 14 KiB | All bytes | None needed |
| Over 14 KiB through 1 MiB | First 14 KiB | Remaining bytes |
| Over 1 MiB | None | All bytes |

The 14 KiB cutoff is XDrive’s limit before Base64 encoding. Google’s
[File reference](https://developers.google.com/workspace/drive/api/reference/rest/v3/files)
does not specify a numeric limit for `description`. A [custom property](https://developers.google.com/workspace/drive/api/guides/properties)
has a separate limit of 124 UTF-8 bytes for its key and value combined. XDrive
uses one such property, `xdrivePrev`, for a recovery hint. Payload in the
description is encrypted transport data and is visible with metadata-read access.

## How a session works

1. **Open.** The client announces slot IDs and one acknowledgement file (the *watermark*) per direction. The listener can read the announcement’s description in a file listing. After that, frame `n` is found by ID at `slots[n % depth]`. On first use, an existing peer-supplied file must match the expected name, parent and file type.
2. **Send.** The writer batches short writes. When a frame needs media, it publishes the description and body together in one multipart request. Each media block carries the frame’s sequence number.
3. **Receive.** The reader looks up slots concurrently and delivers bytes in order. Prefetched media shares a budget of 4 MiB per media connection; the oldest undelivered frame has its own allowance. Large frames can use parallel range reads.
4. **Acknowledge.** The watermark reports the last delivered sequence and the ordered delivery rate. The sender uses that rate to size frames and limit unacknowledged media. It reuses a slot only after the previous upload finishes and the receiver acknowledges it.
5. **Close.** END closes one direction. A graceful close drains outgoing data and continues acknowledging incoming data through the peer’s END. An interrupt or fatal error cancels the session.

## Recovery

- **Stalled media:** If the oldest read makes no progress for five seconds and a later range or frame has verified, the reader can resume at a tagged block without delivering bytes twice. A read that is still progressing continues on its original connection.
- **Stalled metadata:** For a frame over 1 MiB, the next slot can carry its descriptor in `xdrivePrev`. The reader can then fetch the original slot if its identity has already been checked. It verifies the entire body before delivering any recovered bytes.
- **Lost write reply:** The sender reads back the publication instead of replaying a possibly committed write. For data after sequence zero, a validated watermark covering that sequence also proves delivery. Sequence zero can arrive in the announcement, so its slot write needs exact readback.
- **Quota limits:** Drive requests and token refreshes share pacing and honor `Retry-After`. Short stall retries are disabled during cooldown.

## Drive source budget (physical lines, including comments and blanks)

The target is below 2,000 lines after repository formatting. The inventory counts
the three implementation files in full and the integration code listed below.
Ranges are inclusive and relative to `transport/internet/xdrive` unless marked `infra/`.

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
