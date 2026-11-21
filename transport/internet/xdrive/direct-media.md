<!-- coderabbit-ai-generated:v1 -->

<!-- coderabbit-ai-generated-section:v1:0c3694501a8b2d5ab8aa73a22e008a4b297858fa3840da5a990580fac9d4fae3 -->

# Direct media

Direct media is enabled by default. Use updated binaries at both ends and
start a new connection. The client advertises support in the ring offer;
older servers ignore it and upgraded servers keep the existing format for
unadvertised clients. It applies to downstream frames already selected for
media, up to 512 KiB. Inline frames and larger media frames use their existing
formats.

An eligible object carries its sequence, size and CRC alongside the payload.
Only a complete object with the expected sequence, size and CRC can enter the
ordered stream. After a recent direct media frame, the client can issue one
bounded, single-attempt download for the next delivery head while metadata is
pending. A valid response can win before the metadata finishes. If matching
metadata comes first, the client reuses the same download. If the direct read
misses, the normal metadata path is still available. The first frame after idle
still needs discovery. Speculation is disabled during quota recovery.

This mode buffers eligible frames until validation and does not divide them
across media ranges. An unsuccessful speculation can cost an extra media GET.
Compare tail latency and Drive quota. To restore the previous streaming mode,
set `XRAY_XDRIVE_DIRECT_MEDIA=0` on the client and reconnect.


<!-- /coderabbit-ai-generated:v1 -->