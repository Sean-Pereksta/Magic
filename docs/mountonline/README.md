# Mountain Blade Online — The Living Realm

The Online game now uses a shared deterministic simulation instead of separate world, local-battle, and siege implementations. The default route remains `/game/mountonline.html`; the Cat'N Mice lobby supports both local campaign and online-realm entry. The separate `/game/mountainblade.html` skirmish game and Village Wars' shared `/game/battle.html` remain intact.

## Playing

- Explore a 128 × 88 realm with roads, physical trees and cliffs, rivers, bridges, settlements, caravans, outlaws, and faction patrols. Tap ground to march or an entity to inspect it. WASD travels on the world map.
- Recruit at settlements, promote all 14 original troop classes, rotate reserves, buy supplies and standards, complete contracts, and develop leadership, logistics, and tactics.
- Use Move, Attack, Charge, Hold, Defend, Follow, Retreat, Patrol, and Auto commands. Attack on ground attacks enemies on the way to that location. Hold keeps the current position. Follow accepts an allied unit; including that leader in the selection holds the leader while followers converge. Patrol guards a route and returns after engagements.
- Infantry counters charges, cavalry flanks toward missile troops, crossbows pierce armor, mages deal splash damage, and healers follow wounded allies. Shield wall and square formations protect defending troops. Commander rally, volleys, and firestorms have server-validated cooldowns.
- Breach a castle gate before entering. A nearby, available owner controls the castle's actual garrison. Garrison casualties never duplicate or consume the owner's separate field company. An absent or occupied owner has AI defenders.
- Castles generate income every 120 seconds of active simulation. Supply consumption and modest upkeep make logistics meaningful. Hostile patrols can recapture neglected holdings; development, garrison strength, interception, and peace negotiations protect them.
- Online alliances support founding, membership requests, founder approval, succession, and leaving. Members cannot attack each other's companies or castles.

Desktop: click/drag to select, Shift to add, right-click to move/attack, mouse wheel to zoom, Space-drag or arrow keys to pan. A/M/C/H/D/F/R/P/G select orders. 1–5 select roles; Ctrl+1–5 saves control groups. Touch: tap troops or role groups, tap enemies/ground, use Add to selection, hold then drag for a selection box, pinch to zoom, or enable Pan. The mini-map moves the camera.

## Architecture

| File | Responsibility |
| --- | --- |
| `data.mjs` | Shared troop definitions, profile schema, progression constants, legacy conversion |
| `navigation.mjs` | Bounded A*, route cache, physical obstacles, spatial hash |
| `world.mjs` | Seeded terrain, connected roads/bridges, NPC companies and routes |
| `battle.mjs` | 20 Hz deterministic combat, roles, orders, formations, sieges, projectiles, abilities |
| `progression.mjs` | Validated economy, training, reserves, contracts, inventory, skills, settlements |
| `realm.mjs` | World simulation, encounters, territory, garrison control, battle settlement |
| `renderer.mjs` | Interpolated sprite animation, culled terrain chunks, mini-map, order graphics |
| `assets.mjs` | Essential art preload and versioned Cache API reuse |
| `storage.mjs` | Validated local campaign import/export, previous-save recovery, battle resume |
| `network.mjs` | Existing Firebase identity, authenticated HTTP, Socket.IO, safe request retries |
| `app.mjs`, `style.css` | Responsive menu, management screens, selection, desktop/touch input |
| `server/mountonline/` | Firebase Admin authority, receipts, checkpoints, leases, HTTP and Socket.IO |

Rendering runs independently of the 20 Hz simulation. Static terrain uses a bounded 21-chunk cache; the renderer culls off-screen sprites and interpolates movement. NPC decisions and unit targeting are staggered. Path replanning has a shared budget. Each army supports 600 troops; the server admits up to 4,800 simultaneous battle troops and 16 retained battle sessions to bound its workload. Limits are admission controls, not silent troop deletion.

## Firebase deployment

The code is included in this PR; production hosting and rules deployment are separate operations.

1. Deploy the Node code with the existing `bible-game-246c0` service account. `server.js` mounts `/api/mountonline` and the `/mountonline` Socket.IO namespace using its existing Admin connection. An optional `npm run start:mountonline` host serves this game and artwork directly and handles graceful shutdown.
2. Use one active replica for this realm, with persistent Firebase access. A Firestore lease prevents a second process from settling a competing timeline. Configure `MOUNTONLINE_ALLOWED_ORIGINS` with the exact frontend origin(s). Defaults are `https://catnmice.com` and `https://www.catnmice.com`.
3. When the frontend and Node host have different origins, set `API_BASE` in `game/mountonline/config.mjs` to the Node host's HTTPS `/api/mountonline` URL. The default is same-origin. Backend addresses are deliberately never accepted from query parameters or local campaign files because they receive Firebase tokens.
4. Deploy `firestore.rules` to the existing project after backing up the deployed rules and confirming the frontend/backend configuration. New saves, matches, receipts, names, and realm state are server-written. Clients may only get their own new profile. The old `games_rogueduel/mountandblade_online` namespace becomes read-only; other Rogue Duel games retain their existing permissions. No production rules were deployed by creating this PR.
5. Check `/api/mountonline/health`, then sign in from the real frontend with two accounts. Verify a company save, movement, battle, garrison defense, alliance, castle capture, and reconnect before promoting the release.

The existing publicly readable display-name profile/password scheme is not treated as proof of ownership. A URL username cannot import or overwrite another player's company. Verified `mountOnlineAccounts` bindings are accepted; existing verified `factionForgeAccounts` plus matching `factionForgeNames` bindings are reused. Unlinked Firebase identities can immediately play and save their own new online company under a unique Wanderer name.

For an older lobby account without a trusted binding, an operator must independently verify the owner and bind its exact legacy name to its Firebase UID. The helper is server-only and defaults to a dry run:

```bash
node server/mountonline/bind-account.mjs --username Captain --uid FIREBASE_UID
node server/mountonline/bind-account.mjs --username Captain --uid FIREBASE_UID --commit
```

Run it in the trusted server environment with the existing `FIREBASE_SERVICE_ACCOUNT`, never in a browser or a public endpoint. The next fresh realm join imports the old company. If that identity already has a live unlinked session, let it leave the realm before migration. Bindings are not silently reassigned across identities.

Migration retains gold, level, XP, every known troop class, archived original inventory, and owned legacy castles. Forces beyond the field cap remain in reserves. Legacy castles retain their identities and garrisons at safe coordinates on the rebuilt map. Known active standards carry over; unknown banner payloads and allegiance references are archived rather than being interpreted as new authority or silently discarded. Old display-name battle URLs return to the rebuilt realm; they cannot grant a result or replay an old reward. Existing records remain available for rollback/review.

## Durability and authority

Clients send intentions, never troop losses, gold gains, castle owners, or winners. Firebase ID tokens are verified by Admin; player identifiers come from the verified token and protected bindings. The server validates proximity, troop ownership, inventory, costs, target allegiance, and capacity. Both players and castle ownership settle in one Firestore transaction.

Each durable action has a permanent per-player receipt. Retrying the same action after an uncertain response reuses its request ID; a committed purchase or reward is not applied twice. Commander orders and movement are ephemeral intentions. Progress actions, battle entry, and results checkpoint before acknowledgement/broadcast. In-flight battles checkpoint every two seconds; world/profiles checkpoint every ten seconds. An abrupt process loss can therefore roll back at most the last two seconds of battle movement or ten seconds of world movement, while acknowledged progression actions remain committed. A lost authority or failed checkpoint pauses mutation, reloads the persisted state, and retries ownership recovery.

Snapshots are sent at 5 Hz over Socket.IO with nearby public companies and NPCs; full map artwork and terrain are not networked. If the WebSocket client cannot load or reconnect, the browser polls the server's cached state at 1.5-second intervals and stops polling in hidden tabs. Firestore is not polled by each player and is never used as a per-frame transport. HTTP and socket commands have separate per-identity limits and origin validation.

## Local campaigns

Local campaigns do not require Firebase or the realm backend. They save every five seconds and on page hide, retain the previous checkpoint, and support JSON export/import. An invalid current save can recover from the previous save. Imports restore campaigns and unfinished battles locally; they cannot enter the online economy. Practice battles and sieges do not modify a saved campaign.

## Art

All assets are in `graphics/mountonline/`. Two optimized RGBA WebP atlases contain 112 fixed-cell troop poses across 14 classes, with idle, two walk, two attack, brace, fall, and fallen poses. Breathing, marching, projectiles, impacts, banner feedback, tree sway, and water highlights animate them in the engine. The world atlas contains eight ground textures and eight cutout props; native SVG icons provide consistent command graphics. The existing Mountain Blade lobby artwork is reused as the loading/menu/result backdrop.

The three atlases plus hero total approximately 1.7 MB. Essential bitmap artwork is decoded before gameplay and cached under a game-specific version. `manifest.json` documents crop cells with an inset that prevents neighboring sprites bleeding into the frame. Re-encode new source atlases with `scripts/build-mountonline-assets.cjs`; bump the art cache version whenever changing deployed art. Asset provenance and generation specifications are in `ART.md`.

## Validation

```bash
npm run test:mountonline
npm run bench:mountonline
npx playwright install chromium
npm run test:mountonline:browser
npm run test:mountonline:rules
```

The emulator suite uses only `demo-mountonline`, refuses to run without `FIRESTORE_EMULATOR_HOST`, and tests production rules plus the real Admin service and two browser clients. Browser tests serve the actual game, injecting inspection hooks only into the test HTTP response. The two-player test substitutes Firebase's token verifier/browser Auth SDK; it exercises real HTTP, Socket.IO, shared simulation, Firestore transactions, reconnects, and result settlement. No fixture authentication is installed in production code. See `QA.md` for measured results and the distinction between browser emulation and physical devices.
