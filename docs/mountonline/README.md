# Mountain Blade Online — The Living Realm

The Online game now uses a shared deterministic simulation instead of separate world, local-battle, and siege implementations. The default route remains `/game/mountonline.html`; the Cat'N Mice lobby supports both local campaign and online-realm entry. The separate `/game/mountainblade.html` skirmish game and Village Wars' shared `/game/battle.html` remain intact.

## Playing

- Explore a 128 × 88 realm with roads, physical trees and cliffs, rivers, bridges, settlements, caravans, outlaws, and faction patrols. Tap ground to march or an entity to inspect it. WASD travels on the world map.
- Recruit at settlements, promote all 14 original troop classes, rotate reserves, buy supplies and standards, complete contracts, and develop leadership, logistics, and tactics.
- Use Move, Attack, Charge, Hold, Defend, Follow, Retreat, Patrol, and Auto commands. Attack on ground attacks enemies on the way to that location. Hold keeps the current position. Follow accepts an allied unit; including that leader in the selection holds the leader while followers converge. Patrol guards a route and returns after engagements.
- Infantry counters charges, cavalry flanks toward missile troops, crossbows pierce armor, mages deal splash damage, and healers follow wounded allies. Shield wall and square formations protect defending troops. Commander rally, volleys, and firestorms have validated cooldowns.
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
| `renderer.mjs`, `motion.mjs` | Persistent online display poses, sprite animation, culled terrain chunks, mini-map, order graphics |
| `assets.mjs` | Essential art preload and versioned Cache API reuse |
| `storage.mjs` | Validated local campaign import/export, previous-save recovery, battle resume |
| `network.mjs`, `firebase.mjs` | Existing Firebase identity, direct Firestore listeners, orders, and safe retries |
| `app.mjs`, `style.css` | Responsive menu, management screens, selection, desktop/touch input |
| `firestore-realm.mjs`, `codec.mjs` | Elected browser simulation, compressed snapshots/checkpoints, transactional results, host handover |

Rendering runs independently of the 20 Hz simulation. Static terrain uses a bounded 21-chunk cache; the renderer culls off-screen sprites and interpolates movement. NPC decisions and unit targeting are staggered. Path replanning has a shared budget. Each army supports 600 troops; the shared simulation admits up to 4,800 simultaneous battle troops and 16 commanders to bound its workload. Limits are admission controls, not silent troop deletion.

## Firebase deployment — Firestore only

Mountain Blade Online is a static browser game using the existing `bible-game-246c0` Firebase project and its existing Firebase Authentication session, including anonymous sign-in. Multiplayer, saves, commands, ownership, and recovery use **Cloud Firestore directly**. There is no Node game service, Cloud Function, Realtime Database, service-account key, API endpoint configuration, or additional hosting requirement. Publish the game files through the existing Cat'N Mice/GitHub hosting.

1. Back up your deployed Firestore rules.
2. For the rules supplied on October 16, publish the **complete replacement** in [`firestore-complete.rules`](firestore-complete.rules) using Firebase Console → Firestore Database → Rules. It retains all supplied rules for other games. The repository's aggregate `firestore.rules` contains the same Mountain Blade section alongside its other game rules.
3. Publish this matching static game update. Check online entry with two accounts, movement, a battle, saving, and reconnecting. The game creates its own documents; no collections need to be pre-created and no custom indexes are required.

For a manual merge into a newer rules version, insert [`firestore-additions.rules`](firestore-additions.rules) inside `match /databases/{database}/documents`, after `isAuthed()`. Also change **both** write allowances in the existing `games_rogueduel/{gameId}` block to:

```rules
allow read: if isAuthed();
allow write: if isAuthed() && gameId != 'mountandblade_online';
```

The exception must appear on both the parent and recursive child matches: overlapping allows cannot be restricted by another deny. This preserves old Mountain Blade records for migration and stops old clients from overwriting them. Other Rogue Duel games retain their permissions. Publish the matching rules and game version together. No production rules or records are changed by opening a pull request.

### Older company migration, using only the Firebase Console

New companies save immediately under their Firebase UID. A lobby display name selects the displayed name; it never grants access to another player's save. Show/copy the Firebase UID from the game settings when setting up a legacy binding.

For an older name-based save, independently verify the owner and add these two documents using Firestore's **Data** tab (no script or service account):

| Document | Fields |
| --- | --- |
| `mountOnlineAccounts/FIREBASE_UID` | `username` (string): exact old name; `verified` (boolean): `true` |
| `mountOnlineNames/EXACT_OLD_NAME` | `uid` (string): the same Firebase UID |

Then have that player leave the online realm and reconnect after finishing any active battle. The browser host imports the old company and castles once. Late bindings retain newer skills, contracts, achievements, and progression. Gold, level, XP, all original troop classes, excess troops in reserve, archived inventory, original castle identities and garrisons survive migration. Both mappings must agree. Browser clients cannot create or edit these mappings. Do not reassign an existing binding to another person.

Anonymous Firebase identity persists in that browser's storage. Reconnecting with the same UID resumes its save. Clearing browser identity or moving to another device does not authenticate the previous UID; use the site's existing linked Firebase sign-in when cross-device identity is needed. A username in a URL is insufficient proof of ownership.

## Synchronization and recovery

An active browser acquires a 20-second Firestore lease through a transaction. That browser runs the same 20 Hz world and battle simulation as local play. Other players submit UID-owned command documents and listen to their private view. Each write checks the current host and lease epoch. After an abrupt host disconnect, another active browser claims the expired lease and restores the checkpoint; a normal leave releases it immediately. The world pauses when no player is hosting.

Movement uses separate display positions, following the same approach as Chess Warlords: each incoming view retargets the position currently drawn on screen. Early/burst updates never restart a company or troop from the previous saved square; duplicate position updates do not restart its glide. Deadline-based frame interpolation and a short easing tail smooth corrections at different frame rates. The commander, retinue, nearby players, NPCs, battle troops, camera and mini-map use these display positions. Mouse/touch selection, selection boxes and formation previews use the visible troop positions too. Walking continues until a visible move finishes. New battle/identity scopes, long reconnects, tab resume and large respawns reset stale tracks. Poses are kept in bounded maps and reused across frames. This is presentation only: simulation positions, costs, damage, orders and saves remain authoritative, and no additional Firestore writes are needed.

Lease renewals are coalesced to approximately five-second intervals. World views update at most once every 1.2 seconds; unfinished battles update every 500 ms. The renderer interpolates positions between snapshots independently of the network. Compressed native-browser payloads avoid nested-array restrictions and keep 1,200-troop battles below Firestore's document limit. Terrain and artwork are static assets and never pass through Firestore. A size guard pauses mutation rather than silently truncating a realm. Listeners replace polling, and ephemeral completed orders are removed by their owner.

Progress purchases, battle entry, battle results, both participants' profiles, territory, and the permanent command receipt commit atomically. Retrying a purchase uses the same request ID and reads its result. Completed durable receipts cannot be changed or deleted by clients. Battle checkpoints commit every two seconds; world movement checkpoints every five seconds. Abrupt host loss can roll back unacknowledged movement to that checkpoint. Online writes require a live Firestore connection; cached/offline snapshots are not presented as committed online progress. Local campaigns remain available without a network.

The rules restrict private reads, UID/session ownership, command fields, protected legacy bindings, checkpoint revisions, current-host writes, and immutable receipts. The host validates troop ownership, costs, distance, target allegiance, cooldowns, and battle settlement using the shared engine. **This is peer-hosted multiplayer:** Firestore rules cannot independently recompute a complete battle or validate compressed simulation contents. An elected host is trusted for simulation; this design does not claim protection against a malicious host. Firebase Rules documentation: https://firebase.google.com/docs/firestore/security/rules-conditions . Transactions documentation: https://firebase.google.com/docs/firestore/manage-data/transactions .

## Local campaigns

Local campaigns do not require Firebase. They save every five seconds and on page hide, retain the previous checkpoint, and support JSON export/import. An invalid current save can recover from the previous save. Imports restore campaigns and unfinished battles locally; they cannot enter the online economy. Practice battles and sieges do not modify a saved campaign.

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

The emulator suite uses only `demo-mountonline` and tests both the repository rules and the complete rules based on the supplied deployed file. Two browser clients use the real Firestore Web SDK, real listeners, transactions and security rules. Only the test HTTP response supplies emulator identity credentials; there is no production auth bypass. The HTTP server in tests only serves static assets and is not a game service or deployment dependency. See `QA.md` for results and remaining physical-device/production checks.
