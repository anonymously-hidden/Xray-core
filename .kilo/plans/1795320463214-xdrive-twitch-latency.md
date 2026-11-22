# XDrive Twitch segment latency — exhaustive overhaul revalidation

## Goal
Deliver a Twitch HLS segment (~2 s of video) through xdrive-over-Google-Drive
without exceeding the player's per-segment buffer budget. Everything below is
confined to `transport/internet/xdrive/**`.

## Scope
- In scope: `transport/internet/xdrive/**` only.
- Out of scope: Xray mux/carrier flow isolation, routing, any code outside the
  xdrive folder. Cross-flow HOL cannot be fixed here.

## Method
Read every non-test Go file in the package line-by-line (`slots.go`,
`slot_stream.go`, `slot_stream_reader.go`, `slot_body_gate.go`,
`slot_batch_window.go`, `slot_latency_window.go`, `drive.go`, `client.go`,
`xdrive.go`, `conn.go`, `storage.go`, `params.go`) plus `README.md`. Built the
full candidate list first, then revalidated each item against the code. Every
verdict below cites the code that decides it.

## What the code actually bounds (steady state)
- Writer: one multipart create/PATCH per frame; upload ≈ 1.36 s per 512 KiB.
- Reader: `readDescription` (metadata GET ≈ 330 ms) + `readFrameStream` (body
  GET ≈ 880 ms to first block). Description reads are prefetched
  `slotReadAhead = 11` frames ahead (`slots.go:73`, `1410–1418`) and the head's
  description is overlapped (`slots.go:310`, `2080`).
- Per-frame steady state = `max(1.36 s, ~1.21 s) = 1.36 s`; the reader is
  **writer-bound**. The metadata read is hidden inside the upload.
- Therefore no reader-side or wire-format change reduces the steady-state
  floor. The only remaining levers are the **tail**: first-block wait and
  within-frame block gap.

## Exhaustive candidate list and revalidation

| # | Candidate | Verdict | Code evidence |
|---|---|---|---|
| A1 | Compress payload before upload | **Invalid** | Writer sends raw bytes (`slots.go:900–907`); payload is already TLS/VLESS ciphertext. |
| A2 | Raise upload concurrency / media lanes | **Invalid (already adaptive)** | `slotWriteInflight=12`, warmed 24, steered by `latencyWindow` (`slots.go:616–638`, `slot_latency_window.go`); README: beyond BDP extra concurrency only queues. |
| A3 | Parallel resumable upload of one frame | **Invalid** | Drive resumable advances from server-acknowledged offset; no parallel chunks. |
| A4 | Reduce `segmentBytes` (frame size) | **Not a floor fix** | Default 512 KiB (`params.go:6`). Total HLS bytes unchanged, so total time is uplink-bound; only granularity changes. Helps LL-HLS/partial delivery, not standard HLS. |
| A5 | Expand inline/metadata prefix | **Already implemented** | Mode 6, `slotStreamPrefixBytes=11 KiB` (`slot_stream.go:12–14`), `useInlinePrefix` (`slots.go:798`). |
| A6 | Idempotent create-by-ID | **Already implemented** | `createSlot` sets `metadata["id"]` and falls back to `setSlot` on 4xx (`drive.go:2759–2794`). |
| A7 | Reduce per-frame CRC/encode work | **Negligible** | 4 bytes per 32 KiB block (`slot_stream.go:56–60`); CRC32 over 512 KiB is microseconds. |
| A8 | Raise inline burst budget/limit | **Already implemented** | `slotInlineBurstFrames=32`, two-wave rule (`slot_batch_window.go:7`, `slots.go:450–458`). |
| A9 | Multipart metadata+body in one request | **Already implemented** | `setSlot`/`createWithID` build multipart when body != nil (`drive.go:2126–2129`, `2735–2738`). |
| A10 | Avoid reuse/watermark wait | **Not a steady-state lever** | Ring is not full in steady state; reuse waits are µs (README resume.47/48). `prefetchWatermark` already hides the GET (`slots.go:978`). |
| B1 | Remove per-frame metadata read (self-describing body) | **Rejected (see B10)** | Reader already uses `slotID(seq)` directly as the Drive ID (`slots.go:817`, `1357`, `1591`); no `files.list` in slot path. Metadata is prefetched/hidden. |
| B2 | Increase read-ahead / fetch-ahead | **Invalid** | `slotReadAhead=11`, `slotFetchAhead=12`, `slotBodyAhead=20` (`slots.go:73`, `62`, `42`). Writer-bound; more prefetch only adds memory/quota. |
| B3 | Remove ordered body gate | **Invalid/risky** | `slot_body_gate.go` returns byte credit only on ordered handoff; removing it lets later frames burn credit while an earlier frame is missing. |
| B4 | Prioritize head body GET | **Already implemented** | `mediaHeadKey`, `mediaPromotionKey`, `priorityClient` for description; body gate admits head first (`slots.go:1714`, `drive.go:1019`, `slot_body_gate.go:127–137`). |
| B5 | Earlier head body hedge | **Already adaptive** | `mediaHedgeDelay` = 1.3×p50 clamped 900 ms–2 s (`drive.go:3844–3857`); progressive startup uses `mediaStartup` median (`drive.go:3218–3221`). |
| B6 | Overlap N+1 description with N body | **Already implemented** | Prefetch window (`slots.go:1410–1418`). |
| B7 | More head ranges (smaller `partSize`) | **Rejected** | `driveMediaLanes=4`, `driveMediaPart=128 KiB`, `streamHeadRangeLimit=4` (`drive.go:3004–3005`, `3741–3746`). Progressive forces `parts=1` (`drive.go:3594–3597`); more ranges multiply the 200-unit media GET. |
| B8 | Smaller stream block size | **Rejected** | `slotStreamBlockBytes=32 KiB` (`slot_stream.go:15`). First-block wait is HTTP/TTFB-bound, not block-size-bound; smaller blocks add CRC/dedup work. |
| B9 | More media hedge tokens / faster refill | **VALID (conditional)** | `mediaHedgeTokens` starts 3 (`drive.go:358`, `388`), cap `maxReadHedgeTokens=6` (`drive.go:2200`), accrues every 20 media ranges (`drive.go:2156–2164`, `2202–2210`), spent in tracked head rescue (`drive.go:3417–3425`), waited on in `takeMediaHedgeTokenOrWait` (`drive.go:2188`). Targets the 5.36 s gap **if** it is budget-limited. |
| B10 | Self-describing progressive body / body-first protocol | **Rejected** | Mode 5 body is raw data (`slots.go:903–904`); description carries header+CRC (`slots.go:121–143`). The description GET is the **cheap discovery** (5 units); body GETs cost 200. Body-first polling is quota-prohibitive, and the description is already prefetched/hidden. No critical-path round-trip is removed. |
| B11 | Larger connection/stream pools | **Invalid** | h2 one connection (`client.go:359`), `defaultPoolConnections=4` (`client.go:666`), 4 media pools (`drive.go:361–365`); README: local permit/connection waits negligible. |
| C1 | Adaptive frame size for live | **Not a floor fix** | Same as A4; `segmentBytes` is already configurable and `inlineRampSegments` grows it adaptively (`slots.go:450–458`). |
| C2 | Reduce `flushInterval` / coalescing | **Invalid** | 20 ms (`params.go:7`); small stable suffixes already flush after 2 ticks (20–30 ms) (`slots.go:583–598`). |
| D1 | Reserved media lane for head body GET | **VALID but low value (measure)** | `readFrameStream` does not set `laneKey{}` (`drive.go:3576`), so head and prefetch share `mediaRanges` (48 permits, `drive.go:844–845`). But body gate caps inflight at 20 (`slot_body_gate.go:129`), so the 48-permit pool is not saturated. |
| D2 | Let inline frames bypass ordering | **Invalid** | Would reorder the `net.Conn` byte stream; `run()` must deliver `head` in order (`slots.go:1427–1475`). |
| D3 | Separate rings per flow class | **Out of scope** | Needs mux-layer knowledge of sessions. |
| D4 | Cap prefetch to starve head less | **Invalid** | Body gate already bounds and prioritizes; README shows admission ≈ 0. |
| E1 | More h2 connections / pools | **Invalid** | See B11. |
| E2 | Default media to HTTP/3 | **EXPERIMENTAL** | h3 is opt-in (`client.go:361`). Avoids TCP HOL but is UDP, network-dependent, and may be blocked. |
| E3 | Tune h2 ping/idle | **Already tuned** | `h2Idle=1500 ms`, `h2PingWait=2500 ms` (`client.go:243`); detects dead conns without penalizing bulk. |
| E4 | Tune HTTP/2 buffers/concurrency | **VALID to measure** | Only `SendPingTimeout`/`PingTimeout` are set (`client.go:355`); `MaxReceiveBufferPer*` / `MaxConcurrentStreams` are left at defaults. If a window is below the frame size, throughput is capped. |
| F1 | Quota-aware prefetch reduction | **Already implemented** | `quotaRecovery()` (`slots.go:2010`), cooldown suppression (`drive.go:3595`), 5 s post-rate-limit rule. |
| G1 | Lower `driveTimeout` / stall detection | **Invalid** | `driveTimeout=60 s` (`drive.go:47`) is the last resort; media hedge fires at 0.9–2 s. Lowering kills slow-but-moving transfers. |
| G2 | More rescue attempts per range | **VALID but costly (part of B9)** | One rescue per range (`drive.go:3202`, `3445–3450`); each is a 200-unit media GET. Only justified under measured budget starvation. |
| H1 | Move CRC table into body header | **Same as B10 — rejected** | Adds bytes; does not remove the description round-trip unless discovery also moves to body polling (quota-prohibitive). |
| H2 | Compress CRC table | **Negligible** | 64 bytes for 512 KiB. |
| I1 | Reduce announce latency | **Not per-segment** | 25 ms one-time (`xdrive.go:563`); first frame is already included in the announce (`xdrive.go:568–575`). |
| J1 | Deliver later blocks before earlier | **Invalid** | Violates ordered `net.Conn`. |
| J2 | Split a prefetched single-range frame on promotion | **Rejected** | Progressive is deliberately one range (`drive.go:3594–3597`); promotion only defers recovery. Splitting cancels a partial GET and adds ranges; the existing suffix rescue already covers a stalled range. |
| K1 | Speculative head body GET | **Same as B10 — rejected** | Needs a self-describing body; otherwise the size/CRC are unknown and the GET is wasted. |
| L1 | Reduce writer's `awaitReuse` fence | **Invalid** | Correctness fence (previous write + watermark); reuse waits are µs in steady state. |
| M1 | Redundant upload to two files / race two GETs | **Rejected** | Doubles upload quota (50/frame) and storage; the stall is network-wide, so both copies stall. |
| N1 | Larger delivery channel buffer | **Invalid** | `ch` buffer = concurrency (`slots.go:1296`); enqueue p95 ≈ 2 ms (README resume.48). |
| O1 | Faster CRC verification | **Negligible** | CRC32 is already microseconds. |
| P1 | Increase `maxSegmentBytes` | **Not a floor fix** | 16 MiB cap (`params.go:15`); larger frames reduce request count but not total bytes. |

## Corrected shortlist (survivors only)

### S1 (primary). Media hedge token budget and refill for head-block recovery
**Why it survives:** the 5.36 s within-frame gap is post-verification. The
tracked head rescue fires after `mediaHedgeDelay` since the last verified block
(`drive.go:3405–3426`), but it needs a token from a pool that starts at 3 and
refills only every 20 completed media ranges (`drive.go:358`, `2156–2164`,
`2200–2210`). With one range per progressive frame, a token arrives every ~20
frames; several concurrent stalls can exhaust the pool and leave later head
blocks waiting.

**Change (bounded):**
- Raise the initial `mediaHedgeTokens` and/or the cap, and accrue faster (e.g.
  every 10 ranges instead of 20) — optionally give media its own cap instead of
  sharing `maxReadHedgeTokens` with description hedges.
- Keep the one-rescue-per-range and cooldown/rate-limit fences unchanged.

**Cost:** up to +1 media GET (200 units) per additional rescue, only under
stall. No change on healthy frames.

**Validation:** instrument tokens-at-stall; compare `missing-block-max`,
`hedges`, `won`, and units on a matched Twitch window. If the stall is
network-wide (both copies stall), the extra rescue will not help — measure
before keeping.

### S2 (measure first). Reserved media lane for the head body GET
`readFrameStream` (`drive.go:3576`) does not set `laneKey{}`, so the head body
GET shares the 48-permit `mediaRanges` pool with prefetch. Expected value is
low because the body gate caps inflight at 20 (`slot_body_gate.go:129`), but if
logs show head `wait-pool`/`wait-conn` > 0 while prefetch is active, give the
head a small reserved media permit pool (mirroring `reserved` for descriptions,
`drive.go:838–839`).

### S3 (measure first). HTTP/2 buffer/concurrency tuning
Only pings are configured (`client.go:355`). If `MaxReceiveBufferPerConnection`
or `MaxReceiveBufferPerStream` defaults below the frame size, or
`MaxConcurrentStreams` below the writer window, throughput is capped. Measure
`body` p50/p95 and `send` p50/p95 first; raise only the field the data
implicates.

### S4 (experiment only). HTTP/3 for media
h3 is opt-in (`client.go:361`). It avoids TCP-level HOL on packet loss, which
could reduce tail latency, but it is UDP and may be blocked. Keep as an A/B
experiment, not a default.

### S5 (not a floor fix). Frame size / aggregation policy
`segmentBytes` is configurable (`params.go:6`) and `inlineRampSegments` already
grows it adaptively (`slots.go:450–458`). Changing it alters granularity, not
the uplink-bound total. Only relevant for LL-HLS or partial delivery.

## Rejected and why (compact)
- **B1/B10/K1/H1 self-describing body / body-first:** description is the cheap
  discovery (5 units) and is already prefetched; body GETs are 200 units, so
  body-first polling is quota-prohibitive. No critical-path round-trip removed.
- **B7 more ranges / J2 split-on-promotion:** progressive is deliberately one
  range; more ranges multiply quota and cancel partial GETs. Existing suffix
  rescue already covers a stalled range.
- **A4/C1/P1 frame size:** total bytes unchanged; uplink-bound.
- **A2/B2/B11/E1 more concurrency/pools:** writer/uplink-bound; connection
  waits negligible.
- **B3 remove body gate / D2 reorder / J1 out-of-order:** violate ordering or
  byte-credit invariants.
- **M1 redundant upload:** doubles quota; network-wide stalls affect both.
- **E4/G1 timeout changes:** 60 s is the last resort; the 0.9–2 s hedge is the
  real recovery path.

## Ordered tasks
1. **Measure** a matched Twitch client+server window with existing logs
   (`xdrive read`/`xdrive write`: `discover`, `body`, `head-wait`, `blocked`,
   `first-block-wait`, `missing-block-*`, `upload-permit-wait`,
   `wait-pool`, `wait-conn`, `hedges`, `won`). Classify each stall as
   writer-upload vs metadata vs body vs head-gate vs within-frame gap.
2. **If** head-block rescues are token-starved (S1): implement the bounded
   token/refill change and add a test that a stalled head block gets a rescue
   while a healthy stream spends none.
3. **If** logs show head `wait-pool`/`wait-conn` > 0 with prefetch active:
   implement S2 (reserved media permits for the head) and measure.
4. **If** body/send p95 scales with frame size: test S3 (HTTP/2 buffers) as an
   A/B.
5. Keep S4 as an optional h3 experiment; keep S5 as a config note only.
6. Do **not** build B1/B10, B7/J2, A4/C1, A2/B2/B11/E1, B3/D2/J1, or M1.

## Validation
- `go test ./transport/internet/xdrive/...` (focused).
- `synctest` model: metadata 330 ms, body 880 ms, injected late-after-halfway
  block; assert the within-frame gap shrinks by the suffix rescue and no extra
  GET on healthy frames.
- Mixed-version: new client + old server and vice versa on the legacy path.

## Out of scope / honest limits
- Cross-flow HOL (mux) — needs mux-layer changes.
- Reducing Drive's per-request latency — Google-side.
- Eliminating inter-frame HOL — `net.Conn` ordered-stream contract.
- A hard 2 s deadline — if the uplink is the bottleneck, total bytes / uplink
  rate is the floor and no xdrive change can beat it. The code is already
  near-optimal in steady state; only bounded tail recovery (S1) and
  measure-gated transport tuning (S2–S4) remain.