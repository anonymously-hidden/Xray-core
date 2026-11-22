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
   media PUT ≈ 1.36 s, media GET first block ≈ 0.88 ms. They can be overlapped,
   parallelized, or retried — but naive overlap has measured costs:
   speculative metadata racing turns a hit into a **2.30 s miss** (vs 1.40 s
   today) and can spend an extra ~200 quota units, and a mandatory frontier
   adds a barrier (see Rejected E). So "hide the latency" must be done without
   adding a serial dependency.

4. **The target is the tail, not the median.** Median body latency is
   ~0.9–1.1 s. What makes the player buffer: **9.22 s** first-block waits,
   **18.44 s** ready frame behind bulk, **3.67 s** reuse waits, **4.58 s**
   inline-burst publication, and a measured **5.36 s** gap between verified
   blocks while media admission topped out at only **187 ms**. That last point
   is the key signal: the head is stuck *behind queued prefetch in a shared
   pool* (`drive.go` media pool shared with prefetch) and the progressive path
   is forced through a single primary GET (`drive.go:3597`). This is a
   scheduling problem, not a Drive-speed problem.

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
successor fetches before delivery), so the remaining levers are: (a) stop the
head queuing behind prefetch, (b) fetch a frame's blocks in parallel instead of
one primary GET, (c) keep a read-ahead buffer full so per-frame latency hides
behind playback.

## Solution space (ranked)

### Primary overhaul candidates — scheduling, wire-format-neutral

**L. Two-lane GET scheduler (reserved head lane, preemptible bulk).**
Today the primary media GET shares its permit pool with prefetch. Split it:
- **Head lane**: a small reserved set of permits prefetch can never occupy; the
  head's description + first block always has a free permit.
- **Bulk lane**: the existing `mediaRanges` pool for prefetch, free to saturate
  without ever blocking the head.
- Directly targets the measured **5.36 s** gap. No wire-format change, no extra
  requests, no quota increase — same requests, different admission.

**M. Parallel block ranges in the progressive path.**
The progressive path forces **one primary GET** per frame
(`drive.go:3597`). One slow/stalled block then stalls the whole frame even when
later blocks are fetchable. Issue **N concurrent ranged GETs** per frame (N from
the existing head-range limit) and deliver blocks in seq order as each completes.
- A stalled block no longer blocks successors from being *fetched*; they buffer
  and deliver the instant the gap fills.
- Most direct fix for "waits for the next missing block"; composes with L
  (head block gets priority).

**N. Read-ahead pipeline (keep the buffer full).**
If the reader-side stalls dominate (as the 5.36 s / 187 ms data suggests),
stop being frame-synchronous. Reader maintains a **K-frame lookahead** and
refills whenever the buffer drops below target, walking the deterministic seq
order. Steady-state per-frame latency is then hidden behind the 2 s segment
duration **as long as average throughput ≥ bitrate**; tail stalls drain the
buffer instead of stalling the player instantly. Does not help if the *writer*
upload tail is the bottleneck.

### Gating experiments (run before building L/M/N)

- **B-saturated** — shared pool + queued prefetch + slow head: does the head
  actually get stuck behind prefetch? If yes → build L. Wire-format-neutral,
  quota-neutral.
- **D-narrowed** — 256 vs 512 KiB with **one primary GET**, measuring first
  byte, last byte, and quota together. (Changing the head-range setting alone
  won't exercise the progressive path — `drive.go:3597` forces one primary GET.)
- **M-probe** — force N concurrent ranges on the progressive path: does it close
  the 5.36 s block gap? If yes → M is the cheapest overhaul.

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
   metadata vs body vs head-behind-prefetch vs missing-block.
1. **Run the three gating experiments** (B-saturated, D-narrowed, M-probe).
   These decide whether L and/or M are worth building and set the frame size.
2. **Build L** (two-lane scheduler) if B-saturated shows the head stuck behind
   prefetch; **build M** (parallel ranges) if M-probe closes the block gap.
3. **Build N** (read-ahead) if reader-side stalls dominate after L/M.
4. Keep A and C as prototypes; keep the advisory hint as an experiment.

## Implementation tasks (conditional on the gating experiments)

1. `drive.go`: split media GET admission into a reserved head lane + bulk
   prefetch lane; head may take the next free permit without waiting on the
   bulk queue.
2. `drive.go`: allow N concurrent ranged GETs per progressive frame (bounded by
   the existing head-range limit), delivering blocks in seq order.
3. `slots.go` reader: add a K-frame read-ahead window that refills below a
   target buffer depth, preserving ordered delivery.
4. `slots_test.go` / `drive_stream_test.go`: saturated-prefetch head-lane test;
   concurrent-range block-gap test; read-ahead ordering + checksum preservation.
5. A `testing/synctest` benchmark with a simulated Drive (metadata 330 ms, body
   880 ms, one injected 5 s slow block) asserting the head first-block and the
   block gap shrink under L/M/N.
6. `README.md`: document the scheduling changes, the gating experiments, and
   the rejected frontier/A/C options with their measured costs.

## Validation
- `go test ./transport/internet/xdrive/...` (focused, within the command
  timeout).
- The `synctest` benchmark above.
- Mixed-version test: new client + old server and vice versa still round-trip
  on the legacy path (L/M/N must be negotiation-safe or purely local).

## Out of scope / honest limits
- Cross-flow HOL (mux) — needs mux-layer changes.
- Reducing Drive's per-request latency — Google-side; xdrive can only overlap,
  parallelize, and retry.
- Eliminating inter-frame HOL — `net.Conn` ordered-stream contract.
- A hard 2 s deadline — Drive tail latency (9 s+ stalls observed) cannot be
  bounded by xdrive; the best xdrive can do is shrink the median, keep the
  buffer full, and recover faster.