<!-- coderabbit-ai-generated:v1 -->

# Optional direct media experiment

Set `XRAY_XDRIVE_DIRECT_MEDIA=1` on the **client** process and reconnect. Use
updated binaries at both ends. The option advertises support in the ring offer;
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
Compare tail latency and Drive quota before enabling it broadly. The option is
off by default; unset it and reconnect to restore existing media behavior.
