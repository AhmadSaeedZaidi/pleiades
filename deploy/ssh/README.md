# Pinned SSH host keys

`deploy/systemd/youtube-proxy.service` runs with `StrictHostKeyChecking=yes` and
points `UserKnownHostsFile` at `deploy/ssh/known_hosts`.

All YouTube traffic — including the authenticated session cookie jar referenced by
`YOUTUBE_COOKIES_PATH` — is tunnelled through that SOCKS socket, and the unit has
`Restart=always`. With host-key checking disabled, the tunnel reconnected forever
while trusting whatever answered on the far end of the /24: a standing
man-in-the-middle on authenticated YouTube traffic.

## Populating the file

    ssh-keyscan -H -p 22 10.0.0.22 > deploy/ssh/known_hosts

**Verify the fingerprint out of band before trusting it.** `ssh-keyscan` is subject
to the same MITM it is meant to defeat, so confirm the result against a channel you
already trust (console access to the control-plane VPS, a provider API, or a
previously recorded fingerprint).

## If the host key legitimately changes

A changed key means either the VPS was rebuilt or someone is intercepting the
connection. Do not silently re-scan. Find out which, then replace the file
deliberately and note why in the commit message.
