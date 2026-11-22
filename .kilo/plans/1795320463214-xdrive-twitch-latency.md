# XDrive Twitch segment latency — honest assessment + plan

## Goal
Deliver a Twitch HLS segment (~2 s of video) through xdrive-over-Google-Drive
without adding head-of-line blocking or per-request latency. Everything below is
confined to `transport/internet/xdrive/**`.

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
   Uniformity only trades one latency for another. The current adaptive
   inline/stream split is the local optimum.

2. **The ordered byte stream is a hard contract.** Frame `N+1` cannot be
   delivered before frame `N` (`slotReader.run()` delivers `head` then
   increments; `Conn.Read` is an ordered `net.Conn`). Inter-frame HOL is
   structural and cannot be removed inside xdrive without breaking the
   transport contract. Intra-frame HOL is already solved by mode 5/6 streaming
   (32 KiB blocks delivered incrementally).

3. **Drive timings are observations, not fixed limits.** Metadata GET ≈ 330 ms,
   media PUT ≈ 1.36 s, media GET first block ≈ 880 ms. They can be overlapped,
   parallelized, or retried — but naive overlap has measured costs:
   speculative metadata racing turns a hit into a **2.30 s miss** (vs 1.40 s
   today) and can spend an extra ~200 quota units, and a mandatory frontier
   adds a barrier (see Rejected E). So "hide the latency" must be done without
   adding a serial dependency.

4. **The target is the tail, and there are two distinct stalls — do not conflate
   them.** What makes the player buffer: **9.22 s** first-block waits,
   **18.44 s** ready frame behind bulk, **3.67 s** reuse waits, **4.58 s**
   inline-burst publication. Two of these are the actionable ones and they live
   at different layers:
   - **Frame prefetch gate (~187 ms measured):** the head waits at the *frame*
     prefetch gate, not for an exhausted HTTP pool. The head-priority patch
     already protects the ordered head inside the existing 48-permit pool.
   - **Within-frame block gap (~5.36 s measured):** this happened *after*
     verified bytes had already arrived — it is a late/stalled block inside a
     frame, not a download-permit wait. The current gap timer can miss a frame
     that is still making progress but on pace to finish late.
   The 187 ms and the 5.36 s are different problems with different fixes.

5. **Cross-flow HOL is a mux-layer issue** (Twitch blocked behind browsing on
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
stalls. Body reads already overlap across frames (`slots.go:1393` starts
successor fetches before delivery). The remaining actionable levers are: (a) stop
the head waiting at the *frame prefetch gate* (L), and (b) recover a frame whose
blocks are arriving but on pace to finish late (bounded M). Read-ahead is already
sufficient (see N below).

## Solution space (ranked)

### L. Keep reserved capacity; don't cancel active prefetch

The local head-priority patch (`drive.go` head lane within the existing
48-permit pool) already protects the ordered head, and `slot_body_gate.go` gives
queued earlier frames priority over later ones **without restarting requests
already running** (README.md:519–544). Keep that.

- **Keep** the reserved head capacity and the head's two-successor byte
  borrowing (`slot_body_gate.go`), so prefetched tails cannot deadlock the head.
- **Do not** cancel an active prefetch to free a permit: that spends the GET and
  may require another one. Cancel only helps if the download has *stopped
  delivering*, which the existing suffix recovery already handles.
- The measured **187 ms** at the frame prefetch gate is not evidence of an
  exhausted HTTP pool; it is queueing at the gate. Prioritizing queued work (not
  canceling running work) is the correct response.

### M (bounded). One optional suffix GET for a head frame on pace to finish late

Broad N-way parallel ranges are not the answer. A dispatch-to-last-block model
(512 KiB frame, 880 ms to first block, 30 ms between blocks, optimistic
independent lanes — *not* Twitch segment timings):

| Modeled case | One primary GET | Two primary ranges | Four primary ranges |
|---|---:|---:|---:|
| Healthy | 1.33 s | 1.09 s | 0.97 s |
| Isolated stall after halfway | 3.08 s | 1.09 s | 0.97 s |
| Isolated stall in the first few blocks | 3.08 s | 2.84 s | 2.72 s |

Parallel ranges help **only when the stall is after halfway**; they barely help
when the stall is in the first few blocks. And at 1.5 Drive frames/s, two and
four primary ranges add **18,000** and **54,000 units/min** respectively, before
retries. The capture does not establish which block stalled or whether another
request would have escaped it.

**Bounded proposal:** issue **one optional suffix GET** for a head frame that is
**still making progress but on pace to finish late**, using the existing
**one-rescue budget**. This targets the "stall after halfway" case the current
gap timer can miss, without doubling GETs on healthy frames.

### N. Dropped — read-ahead already works

All **nine** slow progressive frames in the newer capture had successors ready
before them; frame **2241** was ready about **8.7 s** before frame **2240**
finished. A deeper window would fetch more bytes that still could not be
delivered. Keep the existing adaptive byte budget and 20-frame lookahead
(`slot_body_gate.go`, README.md:519–544). No change.

### Gating experiments (run before building bounded M)

- **B-saturated** — shared pool + queued prefetch + slow head: confirm the head
  is stuck at the *frame prefetch gate* (queueing), not at an exhausted HTTP
  pool. Decides whether any L change is needed beyond keeping the patch.
- **D-narrowed** — 256 vs 512 KiB with **one primary GET**, measuring first
  byte, last byte, and quota together. (Changing the head-range setting alone
  won't exercise the progressive path — `drive.go:3597` forces one primary GET.)
- **M-probe (bounded)** — inject a late block after halfway and verify a single
  optional suffix GET (within the one-rescue budget) escapes it; confirm no
  extra GET is spent on healthy frames.

### Experiments only — not the live protocol

- **Advisory publication hint** — an in-flight fetch carries proof that the
  exact next frame was published, so the reader can start its verified media GET
  sooner; if the hint is absent the existing path continues immediately, with no
  barrier. Replay found only **2 of 78** matched frames where such a hint could
  arrive first, and **none of the nine slow frames**. Keep as an experiment;
  do not make it load-bearing.

### Rejected / deprioritized

- **E. Group frontier + pointer chain** — rejected as the primary fix. A
  frontier reduces repeated discovery reads but adds a barrier: the fastest
  chunk waits for the slowest upload and then for another Drive write, which
  works against getting live bytes moving sooner. Measured quota (one media GET
  per chunk): current ring with one successful description read per frame =
  **2,040** units; group frontier (one frontier write + one read) = **2,055**;
  frontier plus one extra delete per immutable chunk = **2,455**. The eight
  expensive media GETs remain either way, and batching cleanup does not turn
  multiple deletes into one quota request. Also: the ring already hands the
  reader the next slot ID, and deterministic names do **not** avoid lookups
  (`files.get` needs a real ID; names need not be unique), so a next-chunk
  pointer alone does not fix discovery. Body reads already overlap
  (`slots.go:1393`); in seven overlapping eight-frame windows, waiting for all
  uploads held the first frame **295 ms at the median and up to 759 ms**, before
  any frontier write or polling. Leave the frontier and pointer chain out of the
  live protocol.
- **Broad M (N-way parallel ranges)** — modeled to help only on after-halfway
  stalls; costs **18,000–54,000 units/min**; does not help first-block stalls.
  Superseded by the bounded single suffix GET above.
- **A. Speculative metadata racing** — prototype only. Saved ~300 ms on a hit,
  but a miss took **2.30 s** vs **1.40 s** today, and racing can dispatch two
  media GETs (~extra 200 units). Not worth the serial miss penalty.
- **C. Split the first uploads throughout playback** — rejected beyond startup.
  Smaller writes can move first bytes sooner, but each extra frame adds a write,
  discovery, and media read; only **one of the nine** slow progressive frames had
  a matching upload that itself took over two seconds. At most a startup-only
  connection-ramp tweak.

## Recommended path (ordered)

0. **Measure first.** Existing instrumentation (`xdrive read` / `xdrive write`
   logs: `discover`, `body`, `head-wait`, `blocked`, `first-block-wait`,
   `missing-block-*`, `upload-permit-wait`, `buffer-episode-age`). Capture a
   matched Twitch client+server window; classify each stall as writer-upload vs
   metadata vs body vs frame-prefetch-gate vs within-frame-block-gap.
1. **Run the three gating experiments** (B-saturated, D-narrowed, M-probe
   bounded). These decide whether bounded M is worth building and set the frame
   size.
2. **Keep L as-is** (reserved capacity, no prefetch cancellation) unless
   B-saturated shows the gate itself is the problem.
3. **Build bounded M** if M-probe shows a single optional suffix GET escapes a
   late-after-halfway stall without spending extra GETs on healthy frames.
4. Keep A and C as prototypes; keep the advisory hint as an experiment.

## Implementation tasks (conditional on the gating experiments)

1. `drive.go`: add a **single optional suffix GET** for a head frame that is
   making progress but projected to finish late, bounded by the existing
   one-rescue budget; do not add broad N-way parallel ranges.
2. `slots.go` / `slot_body_gate.go`: confirm the head-priority patch and
   head/successor byte borrowing are retained; ensure queued work is prioritized
   without canceling in-flight prefetch.
3. `slots_test.go` / `drive_stream_test.go`: late-after-halfway suffix-GET
   test; healthy-frame no-extra-GET test; ordered-bytes and checksum
   preservation; prefetch-not-canceled test.
4. A `testing/synctest` benchmark with a simulated Drive (metadata 330 ms, body
   880 ms, one injected late-after-halfway block) asserting the within-frame gap
   shrinks by ~the suffix GET, with no extra GET on healthy frames.
5. `README.md`: document the retained L behavior, the bounded M suffix GET, the
   gating experiments, and the rejected frontier / broad-M / A / C options with
   their measured costs.

## Validation
- `go test ./transport/internet/xdrive/...` (focused, within the command
  timeout).
- The `synctest` benchmark above.
- Mixed-version test: new client + old server and vice versa still round-trip
  on the legacy path (L is local; bounded M must be negotiation-safe or purely
  local).

## Out of scope / honest limits
- Cross-flow HOL (mux) — needs mux-layer changes.
- Reducing Drive's per-request latency — Google-side; xdrive can only overlap,
  parallelize, and retry.
- Eliminating inter-frame HOL — `net.Conn` ordered-stream contract.
- A hard 2 s deadline — Drive tail latency (9 s+ stalls observed) cannot be
  bounded by xdrive; the best xdrive can do is shrink the median, keep the
  buffer full, and recover a late frame faster.