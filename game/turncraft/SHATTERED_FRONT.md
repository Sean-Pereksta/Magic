# TurnCraft — Shattered Front

The game entry point remains `game/turncraft.html`. This expansion adds a command center around the existing RTS simulation, its three factions, economy, technologies, abilities, controls, and animated artwork. It requires no frontend build step.

## Playing

- **Campaign:** six authored regions with four operations each. Missions combine preparation, finite enemy waves, protected targets, escorts, captures, rescues, and destroyable installations. One star opens the next mission. Completed operations can be replayed; better stars and optional objectives are retained.
- **Endless Survival:** six different defensive layouts, unlocked through campaign victories. Seeded waves adapt to aircraft and fortifications, add modifiers every five waves, and include elite assaults every ten. Reinforcement queues and active-wave counts are bounded; there is no final wave.
- **Single Player:** choose one or two opposing factions, then use the existing graphical map gallery, AI difficulty, temperament, and three victory conditions. Existing standard starting resources remain in force. The established engine uses free-for-all combat; this update does not pretend it supports allied teams or naval units.
- **Multiplayer:** the dedicated `game/turncraft-lobby.html` supports public rooms, private invitations, host configuration, faction selection, readiness, direct joins, and reconnects. Ranked play is balanced human 1v1 using the actual RTS engine on the server.
- **Armory / Profile:** inspect the real faction rosters, best mission stars, cosmetic achievements, personal survival records, and confirmed ranked rating. Players without a rated game appear as Unranked.

Use **Menu** during a campaign or survival battle to pause and save. **Resume saved battle** restores entities, resources, orders, production, effects, objectives, the wave reinforcement queue, and timers. The browser writes a checkpoint every 20 simulated seconds, between waves, and when leaving. Local checkpoints remain available when the connection fails. Reloading pauses elapsed simulation time rather than completing every production timer immediately.

## Modules

`shattered-front/campaign.mjs` holds the 24 mission scripts, six region layouts, and arena definitions. `waves.mjs` generates deterministic, bounded enemy plans. `engine.mjs` coordinates mission objectives, checkpoints, waves, and result events through a narrow adapter in the original HTML engine.

`menu.mjs`, `menu.css`, `progress.mjs`, and `assets.mjs` implement the command center, graphical faction armory, progression merge, and configurable artwork loading. `profile.mjs` and `lobby.mjs` use the existing Firebase authentication and the TurnCraft API. `server/turncraft/` provides authenticated lobby operations, personal cloud saves, the headless production RTS engine, ranked queue, compressed match checkpoints, fog-filtered client snapshots, and transactional Elo receipts.

The Cloudflare unit, structure, terrain, and animation paths remain intact. Missing static roster images can recover an isolated real frame from the established production atlases, including measured crop repairs. The fallback shares the bounded texture cache instead of adding large images to GitHub.

## Artwork upload

Unzip `TurnCraft_ShatteredFront_Assets.zip` into the existing R2 bucket under **`shattered-front/`**, preserving the archive's directories. The expected first object is:

```text
shattered-front/ui/main_menu/battlefield.webp
```

The default public prefix is:

```text
https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/shattered-front/
```

For another host, set `window.TURNCRAFT_EXPANSION_ASSET_BASE` before loading the game, or save the public prefix in **Settings → Expansion artwork location**. The equivalent device preference is `localStorage['turncraft.expansionAssetBase']`. The prefix must end in `/`. Serve WebP as `image/webp`, PNG as `image/png`, and JSON as `application/json`. Configure public GET access, suitable cache headers, and CORS for the CatnMice origin. Existing graphics stay at their original locations.

`shattered-front-assets.json` maps logical IDs to relative paths, image dimensions, transparency, hashes, provenance, and exact animation frames. Art is loaded as needed. The original faction illustrations keep menus usable while the new assets are being uploaded.

## Cloud deployment and verification

See [the server deployment guide](../../docs/turncraft-multiplayer-deployment.md) for the API origin, existing CatnMice account binding, Admin credentials, and Firebase rules rollout. Local single-player modes work before that rollout. A missing backend produces a connection message and keeps local progress; it does not award guessed rating.

Personal campaign and survival saves are authenticated and validated for shape and progression order. They do not award MMR or enter public competitive records. Global survival rankings remain disabled until a trustworthy survival verifier is added. Ranked rating uses only the server's terminal simulation or an authenticated forfeit, never a client winner claim. Other games' shared profile fields are preserved.

## Checks

Install the root package and the `game/turncraft` package, then install Playwright Chromium in the game package. The following checks are available:

```sh
npm --prefix game/turncraft test
npm --prefix game/turncraft run test:warfare
npm --prefix game/turncraft run test:expansion
node --test server/turncraft/tests/*.test.cjs
npx firebase emulators:exec --project demo-turncraft --config firebase.turncraft-test.json --only firestore "node game/turncraft/tests/turncraft-rules.emulator.cjs"
```

`CHROMIUM_EXECUTABLE_PATH` selects an installed Chromium browser. `TURNCRAFT_ASSET_CACHE` points to the verified existing Cloudflare artwork cache. `TURNCRAFT_EXPANSION_ART_DIR` can point to the unpacked new art during menu screenshot checks; `TURNCRAFT_SCREENSHOT_DIR` saves the desktop and phone previews.

Tests exercise the actual RTS code, all faction/region deployments, movement and combat, a complete mission, an extended survival run, fresh-page checkpoint restoration, responsive menus and HUD, authoritative ranked commands/results, duplicate rating receipts, private/public lobby admission, and Firestore authority boundaries. Automated accelerated wave-clearing is used to test long progression runs; it is not a human difficulty or balance playtest. Production two-browser sessions, real account provisioning, live backend throughput, and physical phone performance must still be checked after deployment.
