# Trash Tactics host download

Extract the Windows x64 ZIP and double-click **Start Trash Tactics Host.cmd**.
Keep the window open until the game opens in your browser. Create a lobby
and share its invitation. Guests can also join from the Catnmice lobby browser
without installing the host. Keep the hosting computer awake and press
**Ctrl+C** in the host window when you're finished.

A community match runs on the player's computer. It uses a temporary
Cloudflare address and is casual. Ranked matches use the Catnmice server,
which holds the Firebase credentials for profile updates. Cloudflare describes
Quick Tunnels as temporary testing connections with changing addresses and no
uptime guarantee. See [Cloudflare's Quick Tunnel documentation](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/).

## Run the host from source

Install Node.js 24 and cloudflared, then from `game/trash-tactics` run
`npm ci` and `node hosting/launch.mjs`. Set `TRASH_TACTICS_CLOUDFLARED`
to the executable path when cloudflared is outside your PATH. The launcher
starts the game on an available loopback port, opens the tunnel, verifies that
the public address reaches the same instance and then opens the local host view.
Set `TRASH_TACTICS_DIRECTORY_URL` when testing against a separate lobby directory.

For the managed Catnmice service, set up a named Cloudflare Tunnel at
`trash-tactics.catnmice.com` and route it to the central match server's
loopback listener. Keep the tunnel token on the service machine. The downloaded
community host and the official match service run independently.

## Build a Windows release

The [Windows host workflow](https://github.com/Sean-Pereksta/Magic/actions/workflows/trash-tactics-host.yml)
builds the bundle and its SHA-256 checksum. Run it manually to obtain a test
artifact. A `trash-tactics-host-v*` release tag publishes the same two files
to that tag's GitHub release, which the game download page reads.

To build locally, provide the Windows x64 binaries for
[Node.js 24.19.0](https://nodejs.org/en/blog/release/v24.19.0/)
and [cloudflared 2026.11.0](https://github.com/cloudflare/cloudflared/releases/tag/2026.11.0).
Use the [Bannerlands runtime notices](https://github.com/Sean-Pereksta/Truth/tree/232b8658273a93eb234a858fb9fb876654508c3a/games/bannerlands/source/hosting/licenses)
for those exact binaries. The builder verifies the binary hashes and stages
only the game, production npm dependencies, hosting components and notices.

```powershell
node hosting/build-windows.mjs --output C:\path\to\out --node C:\path\to\node.exe --cloudflared C:\path\to\cloudflared.exe --notices C:\path\to\hosting\licenses
```
