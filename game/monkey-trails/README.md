# Monkey Trails · Wild Companions

Entry: `/game/monkey-trails.html`; the existing `lobby/lobby.html` card remains the launch point. Static HTML, CSS and native modules require no game build step, paid map service, or game server.

## Game systems

A permanent Sanctuary keeps individually named companions, learned techniques, bonds, equipment and discovered evolution branches. An expedition carries up to three; camps permit controlled roster changes and retain the injuries of companions already used in the expedition. A knockout requires a conscious replacement. Full-party defeat preserves ownership and permanent progress while a new expedition re-earns levels, supplies and active evolution stages.

`data.mjs` defines sixty species, their curated ability pools, eight elements, the authoritative matchup chart, items and equipment. `core.mjs` owns deterministic game rules and configurable balancing constants. Every creature retains a free Palm Strike. Hybrid attacks average their elemental damage components; mixed defensive weaknesses and resistances multiply. Final evolutions choose an elemental mastery with an explicit passive specialization; the preview shows its benefit before committing, and the discovered choice persists when a new expedition resets its active power.

Only accepted tracker distance advances discoveries, encounter milestones, distance bonuses, camps and safe alternative Ultra challenges. Discoveries roll one category at a time (60% essence, 25% two fruit, 12% utilities, 3% fragments). The 250-foot base interval cannot fall below 200 feet. Battle experience is one shared pool; training also spends a growing fruit cost. Direction favors Leaf north, Fire south, Storm east and Water west. Secondary essence rolls and resource exchange make all eight elements available from one safe route.

Ordinary encounters begin near every 2,000 feet; camps near every 6,000 feet. Encounter composition depends on depth and difficulty. Post-battle befriending normally recruits one defeated opponent, with limited attempts, visible odds and persistent failure protection. The first two ordinary victories supply guaranteed recruitment opportunities.

Ultra sightings preserve species, difficulty and identity. Their random markers are an encounter radar, with no invented roads or access claims. A normal approach needs a fresh accepted GPS fix within 100 feet, followed by a deliberate safe-stop confirmation. “Cannot safely reach this location” substitutes accepted walking distance on a route the player chooses, with the same opponent and rewards. New Ultras defeated in battle offer guaranteed recruitment. Claimed encounter IDs prevent repeat rewards.

## Art and assets

The roster uses lightweight illustrated SVG rigs with species-specific silhouettes, details and animation states. Evolution adds structures and changes proportions. Only visible creatures are rendered; inactive screens have no animation loop. Reduced-motion and battery modes limit effects. Asset configuration and the animation manifest provide a single hosting base, with local vector artwork available offline once code is loaded. See the asset packaging script and manifest for the Cloudflare upload layout.

## Tracking and migration

The existing foreground tracker retains its 10 mph cap, accuracy and drift gates, two-fix warmup, explicit pause/resume and rejection of movement gaps. Location is requested only after Start walking; backgrounding or locking the screen pauses tracking without backfilling distance. Screen wake lock is optional and never grants background tracking. References: [MDN Geolocation](https://developer.mozilla.org/en-US/docs/Web/API/Geolocation_API), [MDN Screen Wake Lock](https://developer.mozilla.org/en-US/docs/Web/API/Screen_Wake_Lock_API).

Version-one checkpoints migrate their existing monkey into slot one, preserving health, energy, evolution, training, equipment and history. The encrypted save namespace and Firebase configuration stay the same. Pinned absolute Ultra coordinates live in a separate encrypted device slot, excluded from cloud checkpoints and portable exports. A different device recalibrates the same sighting with no free approach progress. No route history is retained.

## Existing Firebase integration

`storage.mjs` uses Firebase SDK 10.7.1 and imports the existing shared config
(project `bible-game-246c0`). It waits for the existing Auth session and signs in
anonymously only if no session exists. It does not replace or sign out the lobby
identity and does not require an email/password Auth provider.

Paths:

```
games_rogueduel/monkey_trails_v1/saves/{SHA256(normalizedName+mode)}
games_rogueduel/monkey_trails_v1/scores/{saveId}_{runId}
```

These fit the existing recursive authenticated `games_rogueduel/{gameId}` rule;
no rules changes are included or required when deployed rules match this repo.
Only `mountandblade_online` is excluded by that rule. Permission errors remain
visible and device saving continues. Production rules were not deployed or
verified by this change. The leaderboard uses a single-field score ordering.

## Autosaves and limitations

Create a 3–24 character creature name and a unique password of at least 10
characters; reopen with the same credentials. Names are case-insensitive.
There is no password-reset mechanism. PBKDF2-SHA256 (310,000 iterations) derives
an AES-256-GCM key. Random salt/IV and identity-bound authenticated encryption
protect checkpoints; plaintext passwords are not stored or sent to Firestore.

Checkpoints cover movement (at most five seconds between accepted-fix saves),
rewards, battle turns, upgrades, evolution and transitions. Encrypted device
storage is the baseline; cloud writes coalesce and normally throttle to 20
seconds, with urgent actions syncing sooner. An abrupt browser/OS termination
can lose the most recent unsaved seconds; a page-exit save is only best effort.
Offline play works once game code is loaded, not as an installable offline PWA.

Revision transactions reject conflicting saves rather than overwriting a newer
cloud copy. Export/restore encrypted JSON backups and explicit cloud loading
preserve recovery copies. Death scores use stable run IDs; retries do not
multiply records. The last 25 runs also bound the offline score retry history.
Wait for the explicit Firestore-saved status before switching devices.

IMPORTANT: existing shared rules are not owner-only. Encryption protects
readable save content, not deletion/replacement by another authenticated client.
Weak passwords remain vulnerable to offline guessing. Final creature names and
scores are readable by authenticated visitors, and scores/GPS are client-reported,
unverified casual results, not authoritative competitive or fitness records.
No latitude, longitude or route history is included in any cloud checkpoint, portable export, or score. The optional single Ultra anchor is encrypted separately on its originating device.

## Verification and release status

Run from the repository root with Node 22 or newer:

```sh
npm run test:monkey-trails
npm run test:monkey-trails:browser
npm run test:monkey-trails:motion
```

Tracker, real WebCrypto, local saving, offline retry, revision-conflict, backup and practice isolation regressions remain covered. Expansion tests cover the new collection, battle, rewards and hunt rules. The v1 fixture checks migration of an evolved, injured companion with permanent equipment and completed-run history. Tutorial simulations cover all six starters, three difficulties and 48 seeds using basic attacks and forced replacements. Art checks verify distinct geometry, every animation state, evolution variants and manifest coverage.

The browser harness requires the repository's Playwright development dependency and its Chromium installation. Optional `MONKEY_TRAILS_PLAYWRIGHT` and `MONKEY_TRAILS_BROWSER_EXECUTABLE` environment variables select an existing local runtime. Routed local fixtures exercise capture, collection pagination, camp revival, forced replacement after reload and defeat/restart at 360 pixels, without production network writes. The motion harness checks real browser animations and reduced-motion behavior.

Rebuild the separate Cloudflare art ZIP with `node game/monkey-trails/build-assets.mjs --output /absolute/path/monkey-trails-assets.zip`. Preserve the ZIP's `monkey-trails/wild-companions-v2/` object prefix. The generated manifest and `HOSTING.txt` specify hosting and cache headers; the source game's finished inline vector art remains the local default.

Automated browser checks use phone-sized viewports and simulated interactions; they cannot demonstrate real phone GPS, operating-system screen locking, or production Firestore behavior. Those checks remain release gates. Do not describe the expansion as release-complete until Android Chrome and iOS Safari have demonstrated permission allow/deny, GPS interruption and resume, screen lock, asset loading, mobile layout, a full expedition, offline reopening and a two-device cloud conflict.
