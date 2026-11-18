# Online PvP

Choose **Multiplayer** in Trash Tactics or **Browse Lobbies** in the Catnmice Game Hub. Select **Host Lobby** or join an open lobby. On Catnmice, players with a verified profile can also select **Create ranked 1v1 lobby**. The host starts the match when both seats are filled. Both players finish loading the artwork before the match timer begins. Guests play in their browsers.

## Host on Windows

Download and extract the Windows x64 host ZIP, then open **Start Trash Tactics Host.cmd**. Keep the launcher open. It starts the game locally and opens a temporary Cloudflare URL. Select **Host Lobby** in the browser window the launcher opens, then share the public invite. The lobby also appears in the Catnmice browser while a seat is open and the directory is reachable. Stopping the launcher closes the tunnel and match. Community matches do not change Elo.

From source, run `npm ci` and `npm start` in this game directory for browser play at `http://127.0.0.1:57291/`. Use two browser windows to test a casual lobby. The launcher offers public hosting using a temporary Quick Tunnel. The GitHub Actions **Trash Tactics Windows host** workflow builds the portable ZIP. A tag matching `trash-tactics-host-v*` publishes the ZIP and checksum to GitHub Releases for the in-game download page.

## Run the Catnmice service

Run the trusted service from the Catnmice repository with the root server dependencies (including Firebase Admin) and this game's dependencies installed. Provide Application Default Credentials for the existing `bible-game-246c0` project through the deployment environment. Start the service with:

```sh
node game/trash-tactics/server.mjs --trusted --host 127.0.0.1 --port 57291
```

Publish `trash-tactics.catnmice.com` through a named Cloudflare Tunnel with service URL `http://127.0.0.1:57291`. The WebSocket endpoint is `/api/play`. The deployment uses a single service instance because the lobby directory and active matches are held in memory. `/api/health`, `/api/lobbies`, and `/api/leaderboard` can be checked before directing players to the service. `--trusted --casual-only` starts the public directory and casual lobbies without Firebase credentials.

Deploy the repository's Firestore rules to the Catnmice project before enabling Ranked. Players open Ranked from the Game Hub on the same Catnmice origin where their account is signed in. Ranked reuses the existing verified account bindings administered by `server/faction-forge/migrate-account.cjs`. A legacy display name or unlinked browser session alone cannot enter Ranked. Results update `users/{username}.gameStats.trashTactics` with Elo and a win, loss, and draw record. The Firestore transaction uses a match receipt to prevent duplicate results. An early disconnect before 90 seconds results in a casual forfeit with no Elo change. Reconnecting within 30 seconds restores the player's seat.

Cloudflare's temporary host and fixed Catnmice service use the same game and lobby protocol. See the [Quick Tunnel instructions](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/) and [named tunnel setup](https://developers.cloudflare.com/tunnel/get-started/).
