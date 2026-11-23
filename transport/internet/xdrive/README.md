# XDRIVE

XDRIVE carries one ordered byte stream per session through a storage backend:
Google Drive, S3, a URL template, or a local folder. Both peers must run the
same release. There is no compatibility with earlier XDRIVE releases or
negotiation with them.

## Sessions and slots

On Drive the client announces a session with a ring of slot files for each
direction plus one watermark file per reader. Each frame is written to slot
`seq % len(ring)`, and its description says what the slot holds:

| mode | meaning |
| --- | --- |
| 1 inline | the payload (up to 14 KiB) sits in the description, no media request |
| 3 end / 4 error | the stream ended / the peer could not store this segment |
| 5 stream | the payload is the file body; the description carries its size, CRC32 and one CRC32 per 32 KiB block |
| 6 stream + prefix | as 5, plus the first 11 KiB of the payload in the description |

Other backends (S3, template, local) use a WAL of `<seq>.seg` objects. A
template WAL writer also uploads a 48-byte `<seq>.commit` (`XDWALC01`, length,
SHA-256); readers deliver a segment only after its commit matches.

Invariants:

- Bytes are delivered in order. A later frame never overtakes a missing earlier
  one, and bytes already delivered are never replayed by a retry or a copy.
- Every block is checked against its CRC before delivery. The block table must
  match the frame CRC before the first block is delivered.
- A slot is reused only after the reader has fully verified the frame (exact
  length, EOF, every block queued) and advanced its watermark.
- CRC32 detects corruption; it is not authentication. Confidentiality and
  integrity come from VLESS encryption, which is mandatory with mux.

## Reading

- The head slot is polled every 10-25 ms while the session is active; slots
  behind it follow the observed frame pace (150 ms-2 s) and back off after
  repeated misses. Near successors (up to 11, or 23 while full inline frames
  arrive) poll at 40 ms during a burst or while a media body is downloading.
- Stream frames up to 512 KiB are read by one progressive GET that hands each
  verified block over as soon as its turn comes. Larger frames are split into
  block-aligned ranges over four media lanes (at most four ranges for the head).
- A stalled or persistently slow head download can get one alternate GET that
  starts at the first unverified block, within a per-session token budget, and
  never during cooldown or within 5 s of a rate limit.
- Body admission is bounded by bytes and by a prefetch window, so a slow
  consumer cannot make the reader buffer without limit.

## Writing

- Frames up to 14 KiB go inline; larger ones go as stream frames. During a
  burst the front of the upload queue may also carry an 11 KiB prefix (mode 6).
- A live-sized burst (up to one inline wave, 24 frames = 336 KiB) stays inline
  next to other traffic unless a sustained transfer or quota recovery is under
  way. A larger live burst sends its first wave inline and the rest as media in
  parallel.
- Upload concurrency comes from an adaptive latency window, capped at 36
  uploads. 403/429 answers trigger exponential backoff with jitter and a shared
  cooldown.

## Connections

Requests use separate HTTP/2 pools (main, priority, bulk, four media lanes,
head media). Each active pool keeps one warm spare connection, opened with an
unauthenticated HEAD to the service host. If a connection hears nothing for
1.5 s after a small request while other connections answer, it is probed with a
HEAD on that connection. If the probe gets no answer within 1 s the connection
is closed and its requests are retried elsewhere.

On both client and server, `streamSettings` configure the Drive connections:
`tlsSettings` (including `alpn` and `fingerprint`), `sockopt`, `finalmask`,
`xdriveSettings.xmux`, and `address`/`port`. If set, the address is dialed
instead of the service host; SNI stays the service host. On a client,
`streamSettings.address` takes priority over the outbound address, which is
used only when it is unset.

## Quota

Drive [limits](https://developers.google.com/workspace/drive/api/guides/limits)
(checked November 2026): 325,000 units/minute per user per project and
1,000,000 per project. A metadata read costs 5 units, an edit 50, a list 100
and a media GET 200 (ranges included). Measured live sessions used about
18,000-35,000 units/minute per peer.

Drive cannot serve a request in under about two writes plus two reads:
request-to-first-byte is about 2.0 s for inline frames and about 4 s once any
media frame is involved. Each segment of a serially fetched stream must
arrive within its own duration, so 2 s live segments cannot keep up. Segments
of 6 s or more can.

## Logs

- `xdrive write` / `xdrive read` (debug): per frame, with mode, upload or
  discovery timings, `head-wait`, `first-block-wait` and body-window state.
- `xdrive burst write` / `xdrive burst read` (info): one line per segment-sized
  burst (frames of at least 8 KiB, closed after a 600 ms gap). Fields include
  `span`, `first-put`, `last-cut`, upload p50/max, `mbps`, and `after-read` /
  `after-write`. On the client, `after-write` is a lower bound of request to
  last byte.
- `xdrive download` (debug): one line per media GET, with `ttfb`, `body`,
  `mbps`, connection id, lane, and the number of overlapping downloads.
- `xdrive spare connection <n> ready`, `xdrive connection <n> slow but alive`
  (debug), and `xdrive connection <n> answered nothing ... closing it` (info).
