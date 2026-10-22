# Monkey Trails · Wild Companions

Entry: `/game/monkey-trails.html`; the existing `lobby/lobby.html` card remains the launch point. Static HTML, CSS and native modules require no game build step, paid map service, or game server.

## Game systems

A permanent Sanctuary keeps individually named companions, learned techniques, bonds, equipment and discovered evolution branches. An expedition carries up to three; camps permit controlled roster changes and retain the injuries of companions already used in the expedition. A knockout requires a conscious replacement. Full-party defeat preserves ownership and permanent progress while a new expedition re-earns levels, supplies and active evolution stages.

`data.mjs` defines ninety species, their curated ability pools, eight elements, the authoritative matchup chart, items and equipment. Species 1–60 retain their save-stable IDs; species 61–90 add eighteen common/uncommon companions, eight rares and four Ultras. `core.mjs` owns deterministic game rules and configurable balancing constants. Every creature retains a free Palm Strike. Hybrid attacks average their elemental damage components; mixed defensive weaknesses and resistances multiply. Final evolutions choose an elemental mastery with an explicit passive specialization; the preview shows its benefit before committing, and the discovered choice persists when a new expedition resets its active power.

## Visual ability management

Team and Sanctuary views show four equipped technique cards with elemental accents, SVG icons, power or recovery values, energy costs, effects, cooldowns and use limits. Tap a card to open its species-specific technique library. Focus, hover or tap Compare to preview a replacement beside the current technique; tap an available card to equip and autosave immediately. Cancel and Escape preserve the loadout. Already-equipped techniques are marked and cannot be duplicated. Palm Strike remains required in slot one, and editing remains restricted to the trail and camp. Future evolution techniques appear in a separate locked section.

The field guide contains all 90 species with discovered status, search, element and rarity filters, and number, level, rarity and capture-order sorting. Selecting a species opens its detailed appearance, stats, evolution progress and loadout in the same view. Older saves use stable collection insertion order for capture recency. The active expedition still permits at most three companions.

Distance discoveries show their exact item/resource and quantity in a compact, nonblocking popup that fades after 3.6 seconds. Multiple finds are queued, including in practice. Opening Team, Sanctuary, Pack, Settings, abilities, regions or the compass keeps foreground walking active. GPS updates preserve menu inputs and scroll; menus refresh on the next interaction or close. Encounter milestones still pause tracking and return to Walk. Explicit pause and hidden-page behavior remain unchanged.

Only accepted tracker distance advances discoveries, encounter milestones, distance bonuses, camps and safe alternative Ultra challenges. Discoveries roll one category at a time (60% essence, 25% two fruit, 12% utilities, 3% fragments). The 250-foot base interval cannot fall below 200 feet. Each companion upgrades using fruit plus its own combined XP balance, earned from walking, victory and actual combat damage/healing. All three party members earn 1 base XP per 100 accepted feet, including reserves; sanctuary companions do not. Levels 1–3 earn 2× XP, levels 4–5 earn 1.5×, and later levels earn normal XP. Early fruit costs are 3, 5 and 8; later costs scale as before. No automatic level is granted: spend both resources from Team. Fractional XP survives reloads. Conscious party members also recover exactly 1% of equipped maximum HP per 100 accepted feet, smoothly across GPS samples; this caps at full HP, never revives, earns no combat XP and gives no stored recovery while full.

Victory XP is still split once across the party before each member’s level bonus. Additional combat XP credits the acting monkey for actual HP damage, effective self/team/drain healing, counters and attributed burn/poison. Overkill, overhealing, shields, item healing and passive walking recovery do not award contribution XP. Contributions accumulate at 1 base XP per 10 HP with a per-monkey encounter cap to bound repeat-healing rewards. All balances retain the existing per-monkey `training` save field.

In a ready battle, one successful supply use per shared round is free alongside an attack, ability or switch. It does not advance rounds, trigger enemies or tick statuses/cooldowns. Supplies may target any of the three teammates, including a fallen reserve for revival. Invalid or unneeded supplies do not consume the allowance; the allowance survives reloads and resets naturally on the next round. Switching still consumes the combat action. Direction favors Leaf north, Fire south, Storm east and Water west. Secondary essence rolls and resource exchange make all eight elements available from one safe route.

Ordinary encounters begin near every 2,000 feet; camps near every 6,000 feet. Encounter composition depends on depth and difficulty. Post-battle befriending normally recruits one defeated opponent, with limited attempts, visible odds and persistent failure protection. The first two ordinary victories supply guaranteed recruitment opportunities.

Ultra sightings preserve species, difficulty and identity. Their random markers are an encounter radar, with no invented roads or access claims. A normal approach needs a fresh accepted GPS fix within 100 feet, followed by a deliberate safe-stop confirmation. “Cannot safely reach this location” substitutes accepted walking distance on a route the player chooses, with the same opponent and rewards. New Ultras defeated in battle offer guaranteed recruitment. Claimed encounter IDs prevent repeat rewards.

## Art and assets

The Walk screen follows your monkey across a north-up terrain map. Accepted
GPS displacement moves east/right, west/left, north/up and south/down through
the selected land; the camera follows your saved position. Nearby rivers,
paths, bridges, trees, rocks and landmarks are deterministic world features,
so returning to a position shows the same terrain. Only nearby terrain is drawn.
A short exploration trail and the facing marker show where you have walked.
Zoom changes the view without changing distance or position.

The small compass stays on the map. Tap it to inspect the existing encounter
radar and Ultra direction. **Regions** opens a popup containing all ten original
land illustrations and previews, from Emerald Jungle to Ancient Monkey Kingdom.
There is no region gallery on the front screen. Unlocked lands can be revisited
between encounters. Accepted walking adds
to a persistent total (500 m, 1.5 km, 3 km, 5 km, 8 km, 12 km, 18 km, 25 km and
35 km unlocks), independent of expedition restarts. Practice uses its separate
save. Existing saves retain their furthest recorded expedition as a conservative
starting total; older versions did not record lifetime walking distance.

`terrain.mjs` renders local vector terrain around the player's coordinates.
`lands-art.mjs` preserves the ten illustrated landscapes in the region picker.
Reduced motion, battery saving, hidden-page state and the system motion setting
stop ambient motion. `lands.mjs` stores only relative fantasy-world positions
and a bounded virtual trail; it contains no absolute GPS coordinates. Fantasy
terrain never blocks accepted physical movement. The compass retains the existing
encounter direction and Ultra approach rules. Region-specific habitats and
encounter tables are a separate gameplay change.

Run `node --test game/monkey-trails/lands-tests.mjs` for progression checks or
`npm run test:monkey-trails:lands-browser` for phone layouts, previews, travel,
reloads, four-direction movement, popup selection, terrain and motion preferences. The
browser check accepts the same Playwright/browser overrides as existing checks.

All 90 companions use original lightweight SVG rigs, with bespoke silhouettes, faces and details. The 30 new species each have individually authored body proportions; the four new Ultras have exclusive animated signature effects. The package contains 654 animation scenes (idle, walk, attack, cast, hit, knockout and victory, plus Ultra arrival/signature scenes), 270 evolution appearances, environments and interface art. Only visible creatures are rendered; reduced-motion and battery modes stop articulated motion.

The actual SVG files and complete manifest are saved in `game/monkey-trails/assets/` in GitHub. The reproducible Cloudflare upload ZIP is also committed at `game/monkey-trails/art-pack/monkey-trails-wild-companions-v3.zip` (about 2.8 MiB). No external art service or asset build is needed to play. Inline rigs always provide the first render. When `window.MONKEY_TRAILS_ASSET_BASE` is configured before the game module loads, mounted portraits load the corresponding hosted SVG, fall back to the committed local file if the host fails, and keep the inline rig if neither file loads. Non-native elemental evolution branches and active evolved battle poses retain their correctly typed modular inline art; reduced-motion and battery modes retain inline art so those preferences remain effective.

## Tracking and migration

The existing foreground tracker retains its 10 mph cap, accuracy and drift gates, two-fix warmup, explicit pause/resume and rejection of movement gaps. Location is requested only after Start walking; backgrounding or locking the screen pauses tracking without backfilling distance. Screen wake lock is optional and never grants background tracking. References: [MDN Geolocation](https://developer.mozilla.org/en-US/docs/Web/API/Geolocation_API), [MDN Screen Wake Lock](https://developer.mozilla.org/en-US/docs/Web/API/Screen_Wake_Lock_API).

Version-one checkpoints migrate their existing monkey into slot one, preserving health, energy, evolution, training, equipment and history. The encrypted save namespace and Firebase configuration stay the same. Pinned absolute Ultra coordinates live in a separate encrypted device slot, excluded from cloud checkpoints and portable exports. A different device recalibrates the same sighting with no free approach progress. Only relative virtual positions and a short exploration trail are retained.

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
No latitude or longitude is included in cloud checkpoints, portable exports or scores. Encrypted checkpoints and exports include relative land positions and a bounded exploration trail. The optional single Ultra anchor is encrypted separately on its originating device.

## Verification and release status

Run from the repository root with Node 22 or newer:

```sh
npm run test:monkey-trails
npm run test:monkey-trails:browser
npm run test:monkey-trails:motion
npm run test:monkey-trails:abilities-browser
npm run test:monkey-trails:hosted-art
```

Tracker, real WebCrypto, local saving, offline retry, revision-conflict, backup and practice isolation regressions remain covered. Expansion tests cover the new collection, battle, rewards and hunt rules. The v1 fixture checks migration of an evolved, injured companion with permanent equipment and completed-run history. Tutorial simulations cover all six starters, three difficulties and 48 seeds using basic attacks and forced replacements. Art checks verify distinct geometry, every animation state, evolution variants and manifest coverage.

The browser harness requires the repository's Playwright development dependency and its Chromium installation. Optional `MONKEY_TRAILS_PLAYWRIGHT` and `MONKEY_TRAILS_BROWSER_EXECUTABLE` environment variables select an existing local runtime. Routed local fixtures exercise capture, collection pagination, camp revival, forced replacement after reload and defeat/restart at 360 pixels, without production network writes. The ability harness checks replacement, comparison, duplicate/Palm Strike protection, encrypted reopening, all 90 field-guide entries, filters and sorting at 320, 390 and 1280 pixels, plus the outgoing Ultra relay animation and single-round resolution. The hosted-art harness checks actual image loading, CDN/local failures, reduced motion and typed evolution branches. The motion harness checks browser animations, including all four new Ultra signatures.

Rebuild the committed local art and Cloudflare ZIP with `node game/monkey-trails/build-assets.mjs` (optional `--output /absolute/path/package.zip` selects a different ZIP destination). Preserve the ZIP's `monkey-trails/wild-companions-v3/` object prefix and set `window.MONKEY_TRAILS_ASSET_BASE` to your public bucket base followed by that prefix. The generated manifest and `HOSTING.txt` specify hosting and cache headers. Asset tests verify that every manifest object exists locally and has identical bytes in the upload ZIP.

Automated browser checks use phone-sized viewports and simulated interactions; they cannot demonstrate real phone GPS, operating-system screen locking, or production Firestore behavior. Those checks remain release gates. Do not describe the expansion as release-complete until Android Chrome and iOS Safari have demonstrated permission allow/deny, GPS interruption and resume, screen lock, asset loading, mobile layout, a full expedition, offline reopening and a two-device cloud conflict.

Run `npm run test:monkey-trails:progression-browser` to exercise walking XP, exact recovery, fruit/XP upgrades, saved levels, item-then-attack, reserve revival and the saved per-round supply allowance on mobile.
