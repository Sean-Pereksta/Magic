# Catnmice official play service

The existing Trash Tactics host owns its rooms, Elo and profiles. The launcher reads its published `/api/lobbies` and `/api/leaderboard` endpoints. The new operator service runs Bannerlands ranked 1v1, the Cat & Mouse Ranked Survival Challenge and the hub's lifetime-player database. Run it on a managed server, with players connecting from the website or desktop through the same named tunnel.

## Website changes

Apply `integration/magic.patch` followed by `integration/magic-controls.patch` to Magic revision `5ab7ad915bb42a6c3064f0490911f28277edcafc`. Apply `integration/truth.patch` followed by `integration/truth-controls.patch` to Truth revision `796d19963f96000e55d71813d5559d3956008038`. Use `git apply --check <absolute-patch-path>` followed by `git apply <absolute-patch-path>` for each patch in order in the corresponding checkout. The changes cover 71 Magic entry pages, eight additional inner game pages and all six cataloged Truth games.

The official Catnmice page `/hub/ranked.html?game=bannerlands` or `?game=catandmouse` opens from the corresponding game's menu. It acquires a Firebase token through the existing website session. The service checks revocation and the existing verified name and account bindings through Magic's Trash Tactics implementation. The same verified account can enter either queue on either client. The original guest, invitation and community-room flows stay available.

## Operating the service

Use Node 24. Install Magic's pinned root server dependencies for Firebase Admin and the pinned Cat & Mouse edition dependency `ws`. Prepare a read/write deployment copy of both source revisions together with the hub release. From the hub's root, generate the official Cat & Mouse engine once.

```sh
node services/prepare-cat.mjs "$MAGIC_ROOT/game/catandmouse/edition"
```

Provide the existing Catnmice Firebase project credentials using Application Default Credentials on the operator machine. Set these environment variables before starting `node services/server.mjs`.

| Variable | Value |
| --- | --- |
| `MAGIC_ROOT` | Absolute path to the deployed Magic source tree |
| `TRUTH_ROOT` | Absolute path to the deployed Truth source tree |
| `CATALOG_PATH` | Absolute path to the hub release's `content/catalog.json` |
| `HUB_DATABASE` | Absolute path to a durable SQLite file in an operator-owned data directory |
| `PORT` | Loopback listening port, default 57292 |

Run one instance with a supervised process. Back up SQLite with its online backup facility or stop the process before copying the database and journal files. Active rooms run in memory. A service restart abandons incomplete matches and retains committed receipts, ratings and counts.

Publish `hub.catnmice.com` as a Cloudflare Tunnel published application with origin `http://127.0.0.1:57292`. Forward HTTP and WebSocket traffic. The gateway routes each player to a loopback-only game engine. The existing Trash Tactics published application uses `trash-tactics.catnmice.com` and its own service on port 57291. These settings require the Catnmice domain and service operator. No tunnel credential belongs in a game pack. Consult the [Cloudflare published application guide](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/routing-to-tunnel/) and [Trash Tactics deployment guide](https://github.com/Sean-Pereksta/Magic/blob/main/game/trash-tactics/ONLINE.md).

## Ranking and player counts

A ranked season follows UTC calendar quarters. Ratings start at 1,000 with K=32. The first five matches are placements. Bronze starts below 950, Silver at 950, Gold at 1,100, Platinum at 1,250, Diamond at 1,400 and Crown at 1,600. A match records its season when paired. SQLite commits the receipt, both ratings and both history entries in one transaction.

Bannerlands assigns exactly two human houses and reads victory from the authoritative game. Ranked Survival runs two isolated one-player simulations on Flour Mill at Hard with the same server-selected seed. It counts fully cleared waves from the individual enemies and scheduled spawns belonging to each wave. The server compares waves first, then simulated survival time in milliseconds. Equal results are draws. Each client receives the signed-in player's change on the result overlay. The verified website page shows recent matches and the seasonal leaderboard.

The browser modules record actual match entry for Bannerlands, Trash Tactics and Cat & Mouse Local Edition. Other instrumented pages recognize trusted input on a board, hand, play area or visible canvas. A game with a custom control can tag its actual play area `data-gameplay-surface` or mark an engine-confirmed entry on the page. These signals can miss play in custom interfaces, and historic launch counts cannot establish unique players. Release verification should cover each game's controls and supported modes.

The original profile that played owns each queued activity record. A verified username and title count once. A guest uses a persistent random secret, and the database stores its digest. Direct Truth-repository and community Quick Tunnel visitors use an origin-local guest profile. A new temporary hostname can therefore count a returning guest again. The official arcade shell receives signals only from its active Truth or supported community frame and its selected title. Players can explicitly link an official-site guest's play from their profile page. Linking migrates all title records transactionally and deduplicates outstanding retry batches. A remembered account without a verified binding keeps its pending activity locally. Visible counts carry the service's last snapshot time.

## Checks

```sh
node --test services/service.test.mjs
node --test services/engine.test.mjs
node --test services/complete-match.test.mjs
```

For the engine tests, set `BANNERLANDS_ROOT` to `$TRUTH_ROOT/games/bannerlands` and `CAT_EDITION_ROOT` to the prepared Cat & Mouse edition directory. Set `HUB_LONG_MATCH=1` to run real-time protocol matches to natural completion using local test accounts. Production acceptance also requires two networks, real verified account bindings, a live tunnel, the installed Windows client and a browser client. The upstream Cat & Mouse catalog directory currently needs operator configuration for public co-op discovery.
