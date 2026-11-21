<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:92353bfc366b39f8ea8e7a6818a7e3764d0f70170698de6f0b41f76ae8e7eba2 -->

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
already verified bytes cannot be delivered twice. A quiet period is measured
from dispatch or the most recent verified block. Unverified partial bytes
cannot reset that clock. If the first request fails, an application retry
can use the frame's unused rescue once it dispatches and stalls. A retry
does not reset the rescue budget, and waiting before dispatch never starts
the recovery clock. The slot still cannot be reused until the transfer
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
ordered delivery across frames can still create gaps. Compare browser segment timings, the
reader's first and subsequent block waits, and actual Drive requests.


<!-- /coderabbit-ai-generated:v1 -->