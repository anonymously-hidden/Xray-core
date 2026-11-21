<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:20d3834609de25508b5de0342545450bee5e6b23460cc27c576a939bbace114d -->

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


<!-- /coderabbit-ai-generated:v1 -->