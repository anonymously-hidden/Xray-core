# XDrive Twitch segment latency — honest assessment + plan

## Goal
Deliver a Twitch HLS segment (~2 s of video) through xdrive-over-Google-Drive
within the player's ~2 s per-segment budget, without adding head-of-line
blocking or per-request latency. Everything below is confined to
`transport/internet/xdrive/**`.

## Scope
- In scope: `transport/internet/xdrive/**` only (transport, slots, streaming,
  drive backend, batching/latency windows, tests).
- Out of scope (confirmed with user): Xray mux/carrier flow isolation, routing,
  any config or code outside the xdrive folder.

## Honest verdict (read this first)

1. **There is no "uniform packet method" that both avoids HOL and minimizes
   latency.** The inline/media split is Pareto-optimal:
   - **All-inline** — impossible past the 14 KiB inline cap. A 512 KiB burst
     becomes ~37 frames; the package's own Twitch capture shows nine ~512 KiB
     inline bursts occupying **332 ring slots**, with reuse waits up to
     **3.67 s** and a **4.58 s** publication. More inline packets = *more* HOL
     and quota, not less.
   - **All-media** — every tiny control frame pays the full ~2.2 s media
     round-trip instead of ~330 ms; browsing/control latency collapses.
   - **Uniform small frames** — request-count and ring-occupancy explosion;
     the ~880 ms media first-block cost is latency-bound, not size-bound, so
     per-frame latency does not drop.
   So uniformity only trades one latency for another. The current adaptive
   inline/stream split is the local optimum.

2. **The ordered byte stream is a hard contract.** Frame `N+1` cannot be
   delivered before frame `N` (`slotReader.run()` delivers `head` then
   increments; `Conn.Read` is an ordered `net.Conn`). Inter-frame HOL is
   structural and cannot be removed inside xdrive without breaking the
   transport contract. Intra-frame HOL is already solved by mode 5/6 streaming
   (verified 32 KiB blocks delivered incrementally).

3. **The Google Drive floor is a hard limit.** Measured in this package:
   metadata GET ≈ 330 ms, media PUT ≈ 1.36 s, media GET first block ≈ 0.88 s.
   xdrive cannot make Drive answer faster; it can only reduce the number of
   round-trips and hide latency behind other work.

4. **Cross-flow HOL is a mux-layer issue** (Twitch blocked behind browsing on
   the same Conn). Acknowledged out of scope by the user.

## Where the time actually goes (one media/stream segment)

```
writer receives segment from proxy  ── overlaps with Twitch download
writer PUT to Drive                 ── ~1.36 s per 512 KiB, atomic visibility
reader metadata GET (discover)      ── ~330 ms
reader media GET (body)             ── ~880 ms to first verified block, then streams
ordered handoff to player
```

Critical path per segment ≈ `max(Twitch, upload) + metadata + body`, plus tail
stalls. The README's own logs show the **tail**, not the median, is what makes
the player buffer: 9.22 s first-block waits, 18.44 s behind bulk frames, 3.67 s
reuse waits, 4.58 s publications. Median body latency is ~0.9–1.1 s.

## Solution space (ranked by expected impact / risk)

### A. Combined self-describing streaming mode — highest impact, moderate risk
Today a progressive/stream frame costs `metadata GET (≈330 ms) → body GET
(≈880 ms)`; the description is a prerequisite because the reader needs the
block CRC table before it can verify blocks. Make the **body self-describing**
so the reader can start the body GET without waiting for the metadata round-trip:

- New mode (e.g. `slotStreamDirect`): body = `[slotFrame header] [block CRC
  table] [payload]`; description = minimal (seq, mode, size, crc).
- Writer: prepend header + block CRCs to the body (same single multipart PATCH
  as today; only the description payload shrinks).
- Reader: for the head (and eligible contiguous bursts), start the ranged body
  GET speculatively — extend the existing `startDirectProbe` /
  `huntSlotDescription` hook — read the header from the body, verify `seq` and
  the per-block CRCs, and stream blocks. Fall back to the description path when
  the probe returns a stale/previous-lap body.
- Expected: per-frame fetch drops from `≈1210 ms` to `≈880 ms` (the metadata
  round-trip is hidden behind the media TTFB). This is the only structural
  lever that shortens *every* frame's critical path, not just the head.
- Negotiation: add a flag to `ring`/`slotTable`; mixed-version peers keep the
  current path (same pattern as `StreamVersion`/`StreamPrefix` today).

### B. Head-frame prioritization — low risk, moderate impact
The code already has `mediaHeadKey`, `slotExpectedSeqKey`, and the ordered-block
recovery signal. Verify they actually prioritize the head's description + first
block over prefetch/hedge work under load, and add a dedicated head lane if not.
This shrinks the "ready small frame stuck behind bulk" tail.

### C. Upload parallelization for the first frames of a segment — moderate impact, moderate risk
The writer currently flushes a warm 512 KiB segment as one frame (one PUT,
≈1.36 s). Emitting the first 1–2 frames smaller (e.g. 128 KiB) lets them upload
in parallel (window ≥ 12) so the reader can start sooner. Trade-off: more PUTs
(quota) and ring slots; gate by the existing uplink window and
`slot_latency_window` so it backs off under congestion. This mainly improves
*time-to-first-byte*, not total transfer time (the last frame is still bounded by
the writer's receive time).

### D. Frame-size × range-count sweep — low risk, uncertain impact
Sweep `segment_bytes` and the head-range count against a matched Twitch
workload. The current 512 KiB + 4 ranges is a reasonable middle; a data-driven
point may be better for a specific bitrate.

## Recommended path (ordered)

0. **Measure first.** The package is already heavily instrumented (`xdrive read`
   / `xdrive write` logs: `discover`, `body`, `head-wait`, `blocked`,
   `first-block-wait`, `missing-block-*`, `upload-permit-wait`, `buffer-episode-age`).
   Capture a matched Twitch client+server window and classify each stall as
   writer-upload vs metadata vs body vs HOL before changing anything.
1. **Low-risk tuning (D + B):** adjust `segment_bytes` / head ranges and confirm
   head prioritization; re-measure.
2. **Combined mode (A):** implement `slotStreamDirect` behind negotiation; this
   is the main structural win.
3. **Upload parallelization (C):** only if Phase 0 shows the writer's single
   large PUT is on the critical path.

## Implementation tasks (for the recommended path)

1. `slot_stream.go`: add `slotStreamDirect` encode/decode/validate + tests
   (header in body, block CRC table in body, corruption/truncation cases).
2. `slots.go` writer: select the new mode for eligible frames in
   `upload`/`write`, gated by the negotiated flag; keep the description minimal.
3. `slots.go` reader: extend `startDirectProbe`/`huntSlotDescription`/`fetch` to
   start the body GET before the description, read the header from the body,
   verify `seq` + block CRCs, and stream; keep the description path as fallback.
4. `xdrive.go` + `slots.go`: add the negotiation flag to `ring`/`slotTable` and
   wire it in `Dial`/`newSessionConn` (mirror `StreamVersion`/`StreamPrefix`).
5. `slots_test.go` / `slot_stream_test.go`: mixed-version round-trip; stale-probe
   fallback; ordered-bytes and checksum preservation.
6. A `testing/synctest` benchmark with a simulated Drive (metadata 330 ms, body
   880 ms) asserting the head first-block time drops by ≈ the metadata RTT.
7. `README.md`: document the new mode, negotiation, and measured impact.

## Validation
- `go test ./transport/internet/xdrive/...` (focused, within the command
  timeout).
- The `synctest` benchmark above.
- Mixed-version test: new client + old server and vice versa still round-trip on
  the legacy path.

## Out of scope / honest limits
- Cross-flow HOL (mux) — needs mux-layer changes.
- Reducing Drive's per-request latency — Google-side.
- Eliminating inter-frame HOL — `net.Conn` ordered-stream contract.
- Guaranteeing a hard 2 s deadline — Drive tail latency (9 s+ stalls observed)
  cannot be bounded by xdrive; the best xdrive can do is reduce the median,
  shrink the blast radius, and recover faster.