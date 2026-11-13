# Drive media latency

The slot reader delivers one ordered byte stream. A large media frame must be
downloaded and CRC-checked before later inline frames can be delivered. Mux
connections sharing that stream also share its head-of-line delays.

Drive media reads use up to four independent HTTP connection pools. A default
512 KiB frame is downloaded as four 128 KiB ranges; larger frames still use at
most four ranges. After one second without progress, a pending range may race
one duplicate on the separate bulk connection. Completed ranges are retained.
Moving transfers do not trigger duplicates. Recent 403/429 rate limits suppress
hedges and reduce newly started frames to one range.

Range offsets, lengths and the complete frame CRC are checked before delivery
or acknowledgement. Servers that ignore Range can return a full-sized body.
The frame and announcement formats are unchanged: a client-only upgrade works
with existing servers. No server setting can be increased by changing the client.

## Capacity and diagnostics

Google's [published limits](https://developers.google.com/workspace/drive/api/guides/limits)
(checked November 2026) are 1,000,000 units/minute/project and 325,000
units/minute/user/project. Metadata GETs cost 5 units, edits 50, lists 100, and
media GETs 200, including range requests. The latter budget alone permits about
27 media GETs/second, before other traffic. Both peers and other applications
using the same user/project contribute. Actual project quotas can differ and
must be checked in Cloud Console; connection counts are not API quota settings.
Google also documents additional backend throttling and a 750 GB/day Workspace
upload limit. [Range downloads are supported by Drive](https://developers.google.com/workspace/drive/api/guides/manage-downloads#partial_download).

Parallel reads exchange extra requests and connections for lower per-frame
latency: a default full frame uses 800 download units instead of 200, plus any
stalled-range duplicate. They cannot remove peer upload delays or an outage
affecting every network connection.

Debug `xdrive read` lines report `discover` (time until the expected frame was
first seen), `body` (time in media downloads, including failed rounds), and
`blocked` (time a completed segment waited behind its predecessors). `fetch`
also includes prefetch admission and polling waits; it is not an HTTP timeout.
Per-minute timing lines report `requests` and estimated `units` for all sent
attempts, including retries/cancelled hedges. `n` remains the capped latency
sample count. Sum units across operation types and peers when comparing usage
with the project's effective quota.

To verify live, compare body latency, blocking episodes, player buffer health,
and request units over the same video workload. A high `discover` with low
`body` requires peer-side timing to distinguish slow uploads from discovery.
