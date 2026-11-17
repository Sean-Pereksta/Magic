# Cat & Mouse Local Edition preview

## Playing

Install the Windows x64 setup or extract the portable ZIP, then open **Cat & Mouse Local Edition** or **Play Cat Mouse.vbs**. Choose **Singleplayer** for an offline match. The bundled game opens on your computer and stores your settings and progression in your browser.

Use WASD, the arrow keys, or tap the battlefield to walk. Place a wall beside another wall to extend, turn, or branch the line. The placement preview shows which sides will join.

Gather cheese and use the opening waves to build up income and firepower. Sappers threaten groups of buildings, and Menders heal nearby rats. Leave room to dodge the Cat's marked attacks. Hard adds a tail sweep, and Nightmare adds a charge across the battlefield.

To host, choose **Multiplayer → Host Lobby** and set the room, battlefield, difficulty, seat count, and visibility. A public room appears under **Live Lobbies** in the Arcade after its connection verifies. For an invite-only room, copy the guest invitation and share it with the people you want to play with. Guests can use their regular browser.

Use **Start** when all configured seats are filled and ready, or confirm **Force Start** to include the connected players and fill unfinished loadouts. The server gives players 20 seconds to load before a three-second countdown. A forced start can continue with the players who finish loading, provided the host finishes. Keep the host computer awake and online while playing.

## Hosting help

- If the public connection fails, choose **Retry**. If your connection blocks Cloudflare Tunnel traffic, try a different network or ask its administrator about outbound Cloudflare Tunnel connections. Quick Tunnels are a preview hosting option.
- If the directory is unavailable, copy the guest invitation after the public connection is ready. Join directly with that invitation while the room remains reachable.
- If a browser disconnects during a match, return to the same tab within two minutes to reclaim that seat. If the room filled or started before you joined, return to **Live Lobbies** to choose another room.
- Use **Stop Hosting** to close the room and remove its listing. Closing or minimizing the browser tab leaves the local service running.

## Build and deployment

From this edition's directory on a Windows x64 build machine, run `desktop/package-win-x64.ps1` with Inno Setup 6 installed. The script verifies the pinned Node and cloudflared downloads, includes their licenses and the WebSocket library license, checks that the packaged launcher serves the painted artwork locally, and writes the installer, portable ZIP, and SHA-256 checksums to `dist/`. The GitHub Actions workflow provides the same build as downloadable artifacts. The preview artifacts are unsigned.

Deploy the Cloudflare Worker from `directory/` with Wrangler and a Cloudflare account. Use its HTTPS origin in `directory-config.mjs` for the Arcade and browser game. Set the same origin as the repository variable `CAT_MOUSE_DIRECTORY_URL` for the Windows build, or pass it as `-DirectoryUrl` when packaging. The launcher also accepts `CAT_MOUSE_DIRECTORY_URL` as an environment override.

The directory accepts verified Quick Tunnel origins, retains public records for up to 65 seconds, and renews them from a running host every 20 seconds. For a managed tunnel rollout, set `MANAGED_HOST_SUFFIX` to the approved parent domain and verify the exact origin and WebSocket route before enabling public discovery.
