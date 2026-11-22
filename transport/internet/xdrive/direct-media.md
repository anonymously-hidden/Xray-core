<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:e20e02b308daf41306b9b3bd95a904e51801aa19bc923d9ddf87d3a1392da01a -->

# Progressive media delivery

Updated Drive peers default to progressive delivery for downstream media frames
up to 512 KiB. The client offers the preference in its session announcement.
The server uses the existing verified streaming format (mode 5 or 6). The client
starts one media GET after the slot description arrives, and forwards each
verified 32 KiB block as soon as its turn comes. It does not pace delivery or
hold verified bytes to build a jitter buffer. Larger frames keep their existing
range policy; small inline frames keep their existing format.

The four media lanes share the work across frames. If the ordered reader needs
a block and verified delivery on that download stops, the existing recovery
budget permits one alternate GET. It starts at the first unverified block;
already verified bytes cannot be delivered twice. For progressive reads, the
startup timer learns from dispatch to the first verified block on successful
primary requests. After two blocks on a dispatch, their spacing informs the
timer for the next block. Partial unverified data does not reset the timer.
A dispatched application retry can use the frame's one rescue if it is still
unused. A queued rescue that never dispatches returns its token to the session
budget. Waiting for dispatch or retry backoff does not start a recovery clock.
The slot still cannot be reused until the transfer
passes the response length and EOF checks and all verified data is queued.

For frames near 512 KiB the ordered reader also checks for sustained slow
delivery. After four verified blocks on one dispatch, it projects the remaining
time using the fastest of the last three block intervals. The same single
alternate GET is eligible if the projected finish is past two seconds from
dispatch and the remaining work exceeds both 900 ms and 1.5 times the session's
median full download time for comparable frames. This needs at least 12 timing
samples and a median no greater than two seconds. It is a local scheduling
decision, not a browser segment deadline.

Both peers need the new version and a fresh connection for the new default.
A release that only understands direct media continues to use mode 7 with a
new peer. Mode 7 still verifies the complete object before delivering it and
can race a speculative GET against discovery. Progressive delivery keeps the
mode 5/6 metadata dependency to verify blocks, so a slow slot description can
still delay the first byte. Setting `XRAY_XDRIVE_DIRECT_MEDIA=0` on the client
continues to disable the newer media preferences.

Each verified handoff keeps successor discovery active even during a long
frame. It does not change the measured cadence of completed frames or the
polling limits. Drive upload and visibility delays, shared throttling, and
ordered delivery across frames can still create gaps. Compare browser segment
timings, the reader's block waits, and actual Drive requests.

## Connection isolation

The reader uses a separate connection pool by default for progressive primary
GETs at the ordered head after HTTP permit admission. A queued prefetch that becomes the
head can use this pool. Downloads already started continue on their original
connection. The same GET streams the whole frame or its media suffix, with
the existing request and permit limits. The existing backup GET still uses
the bulk pool.

The isolated connection may need a fresh handshake. Slow request logs report
`isolated-head` to distinguish pool selection from a frame promoted during
the request. Isolation is enabled by default, with no configuration needed.
It has not been benchmarked on live Drive playback. Set `XRAY_XDRIVE_ISOLATE_MEDIA_HEAD=0`
before startup to disable it.

<!-- /coderabbit-ai-generated:v1 -->