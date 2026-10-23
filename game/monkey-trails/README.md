# Monkey Trails · Wild Companions

Entry: `/game/monkey-trails.html`; the existing `lobby/lobby.html` card remains the launch point. Static HTML, CSS and native modules require no game build step, paid map service, or game server.

## Game systems

A permanent Sanctuary keeps individually named companions, learned techniques, bonds, equipment and discovered evolution branches. An expedition carries up to three; camps permit controlled roster changes and retain the injuries of companions already used in the expedition. A knockout requires a conscious replacement. Full-party defeat preserves ownership and permanent progress while a new expedition re-earns levels, supplies and active evolution stages.

`data.mjs` defines ninety species, their curated ability pools, eight elements, the authoritative matchup chart, items and equipment. Species 1–60 retain their save-stable IDs; species 61–90 add eighteen common/uncommon companions, eight rares and four Ultras. `core.mjs` owns deterministic game rules and configurable balancing constants. Every creature retains a free Palm Strike. Hybrid attacks average their elemental damage components; mixed defensive weaknesses and resistances multiply. Final evolutions choose an elemental mastery with an explicit passive specialization; the preview shows its benefit before committing, and the discovered choice persists when a new expedition resets its active power.

## Visual ability management

Team and Sanctuary views show four equipped technique cards with elemental accents, SVG icons, power or recovery values, energy costs, effects, cooldowns and use limits. Tap a card to open its species-specific technique library. Focus, hover or tap Compare to preview a replacement beside the current technique; tap an available card to equip and autosave immediately. Cancel and Escape preserve the loadout. Already-equipped techniques are marked and cannot be duplicated. Palm Strike remains required in slot one, and editing remains restricted to the trail and camp. Future evolution techniques appear in a separate locked section.

The field guide contains all 90 species with discovered status, search, element and rarity filters, and number, level, rarity and capture-order sorting. Selecting a species opens its detailed appearance, stats, evolution progress and loadout in the same view. Older saves use stable collection insertion order for capture recency. The active expedition still permits at most three companions.

Distance discoveries show their exact item/resource and quantity in a compact, nonblocking popup that fades after 3.6 seconds. Multiple finds are queued, including in practice. Opening Team, Sanctuary, Pack, Settings, abilities, regions or the compass keeps foreground walking active. GPS updates preserve menu inputs and scroll; menus refresh on the next interaction or close. Encounter milestones still pause tracking and return to Walk. Explicit pause and hidden-page behavior remain unchanged.

Only accepted tracker distance advances discoveries, encounter milestones, distance bonuses, camps and safe alternative Ultra challenges. Discoveries roll one category at a time (60% essence, 25% two fruit, 12% utilities, 3% fragments). The 250-foot base interval cannot fall below 200 feet. Each companion upgrades using fruit plus its own combined XP balance, earned from walking, victory and actual combat damage/healing. All three party members earn 1 base XP per 100 accepted feet, including reserves; sanctuary companions do not. Levels 1–3 earn 2× XP, levels 4–5 earn 1.5×, and later levels earn normal XP. Early fruit costs are 3, 5 and 8; later costs scale as before. No automatic level is granted: spend both resources from Team. Team displays separate XP and fruit progress bars, exact current/required amounts, percentages and enough/needed labels. Each bar is red below its requirement and green at or above it; surplus percentages remain visible while the fill stops at 100%. GPS progress updates these bars in place while the Team menu stays open, preserving other inputs. Maximum-level companions show their completion state instead of an unreachable next-level goal. Fractional XP survives reloads. Conscious party members also recover exactly 2% of equipped maximum HP per 100 accepted feet, smoothly across GPS samples; this caps at full HP, never revives, earns no combat XP and gives no stored recovery while full.

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
to a persistent total (2 km, 6 km, 12 km, 20 km, 32 km, 48 km, 72 km, 100 km and
140 km unlocks), independent of expedition restarts. Practice uses its separate
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

## Tribes, shared bosses and friendly player battles

Open **Tribe** in a regular saved trail. Create a named tribe with a colored
shield, circle or diamond decal and one of six symbols, or join with its code.
A tribe has at most ten members; the owner can update its emblem. Gifts and
mutually accepted item trades use a synchronized tribe pouch. Offers escrow
what the sender gives, and cancellation refunds the escrow exactly once.
Move supplies between the expedition pack and pouch with the transfer controls.
The pouch survives expedition restarts.

Each member can make a ten-turn boss attempt once per hour. Attempts use healthy
copies of their chosen three companions; the boss never attacks. Completed
attempts are replayed deterministically before applying damage in a transaction.
Damage accumulates on the tribe's boss. A defeat advances its level and health,
and awards fruit, XP and supplies to every member in the defeat's membership
snapshot. Fruit/supplies are credited to their pouch, and XP is delivered to the
local save. Closeable notifications arrive live or on the member's next login.
A saved receipt is applied locally once before its notification can be dismissed.

Challenge an online, available tribe member, or explicitly opt into nearby
players. Nearby discovery uses a coarse location fix, expires after 90 seconds,
and conservatively accounts for location uncertainty at the one-mile boundary.
It never displays coordinates, routes, or a navigation marker. Turning it off
removes the shared point. Hidden/closed games are not available for challenges;
a foreground presence heartbeat runs every 25 seconds, and opted-in location is refreshed about every 45 seconds even while not walking. No location is requested
by multiplayer until the player explicitly enables nearby discovery.

Player duels alternate one action at a time, with an expected revision checked in
a Firestore transaction. Both browsers reconstruct the canonical state from the
initial team snapshots and action log. A stale/double click, an out-of-turn move
or a second simultaneous match is rejected. Players can surrender, and a waiting
player can claim a turn after 90 seconds of inactivity. A maximum action count
ends prolonged matches. Both players earn XP. Each player's fruit stake is capped
at the smaller of ten fruit, twice their average team level rounded down, and
available fruit. The stakes are shown with the invitation and held in escrow.
The winner takes the loser's stake; a draw refunds both. Returning transfers the
player's payout back into their expedition pack. There is no PvP cooldown.

Duels and boss attempts never write their combat HP, energy, status effects,
knockouts, loadouts or inventory usage into the expedition. A player battle
cannot end a run or lose a companion. New challenges remain unavailable until
the player returns from the result screen. Existing expedition encounters also
mark the player busy. Opening the ordinary Tribe menu keeps foreground walking
active, like the other menus.

### Hosting and Firestore setup

Gameplay remains static browser HTML/CSS/native modules plus Firebase Auth and
Firestore. **There is no Node server, Cloud Function, paid map API or game build
step.** The existing `lobby/firebase-config.mjs` project and current Firebase Auth
session are reused. A social player is bound to that Auth UID and one saved trail;
opening another trail cannot silently take over its wallet or membership. Use the
same authenticated identity when returning on another device. Anonymous Firebase
sessions retain their identity on the original browser; a creature password alone
does not authenticate a different device as that Firebase user.

Publish the repository's updated `firestore.rules` to the existing project before
using multiplayer. The new records are isolated under
`monkeyTrailsSocial/v1/{players,presence,tribes,offers,challenges,duels,raids,operations,receipts}`.
The broad legacy `games_rogueduel` rules cannot grant access to this namespace.
The app shows permission/connection failures and continues saving the solo trail;
it does not substitute fake local opponents for online players. Production rules
are not deployed by this source change.

The shared economy is casual browser gameplay: rules enforce ownership,
participant boundaries, escrow arithmetic and transaction ordering, while clients
replay combat. Firestore rules cannot independently prove a browser's solo-earned
inventory or execute the complete combat simulator. This is not an anti-cheat
server; no such server is introduced here.

Encrypted local deposits retain a debit intent until their idempotent remote
operation succeeds. Ambiguous network failures are retried with the same ID.
Withdrawal/reward receipts are applied once and checkpointed before dismissal.
No absolute GPS location is written into the encrypted save or portable backup.

Validation commands (Node is used only for development tests):

- `npm run test:monkey-trails`
- `npm run test:monkey-trails:social-browser`
- `npm run test:monkey-trails:social-rules`

The social browser harness uses two isolated browser contexts and a shared
transactional test adapter, without touching production multiplayer records.
The rules harness runs the actual Firestore adapter against the local emulator.

## Tribe events and crew power

The tribe roster shows every crew’s ordered starting three with portraits, names,
levels, elements and individual power, including empty slots. Crew power is the
sum of the selected companions. The tribe list sorts by crew power; tied crews
share a competition rank (1, 1, 3). Nearby crew rankings compare only the currently
eligible nearby players and do not represent a global leaderboard. Legacy profiles
without a shared team show a missing-lineup state until their next update.

`power.mjs` computes the same estimate in Team, Sanctuary and multiplayer:
maximum HP + 4 × attack + 4 × defense + 2 × speed + 8 × maximum energy. It includes
equipped gear tiers, evolution and permanent mastery. Injuries, current energy and
temporary battle modifiers do not change the rating. Power is a comparison of
permanent stats, not a prediction of victory or an Elo rating; abilities, strategy
and elemental matchups still matter. No power score is trusted from saved data.

Team displays the selected crew total, and collected companions display individual
power and their rank among all owned individuals. Sanctuary’s Highest power sort
uses the strongest individual of each species. Uncollected field-guide entries
receive no personal rank. Profile updates publish changed lineups, levels and gear
through existing Firestore documents; tribe and nearby profile subscriptions keep
the roster current without changing battle snapshots.

The shared boss event has an illustrated opponent, live shared health, the actual
next reward preview and the existing per-player hourly readiness. Attempts show
ten-turn progress and protected crew health. All prior trades, challenges, rewards
and closeable notices remain available. No new runtime server or Firestore schema
is required.

## Rarity, elemental tactics and evolution balance

Common, uncommon, rare and Ultra companions receive permanent HP/attack/defense
multipliers of 1, 1.25, 1.5 and 1.85. Their speed multipliers are 1, 1.03, 1.06 and
1.10, with 0, 1, 2 and 3 extra maximum energy. These bonuses affect growth as well
as initial stats, apply before equipment and mastery, and feed the existing power
rankings. Species roles and curated ability pools remain distinct.

Elemental strengths deal 2.25× damage and resistances reduce damage to 0.4×. Each
attacking element multiplies its defensive matchups, capped to 0.25×–4×; split
attacks average their component results. Matching a monkey’s own element adds a
1.25× affinity bonus. Physical attacks remain neutral. Combat previews and hit
feedback show the actual multiplier, Very effective!/Not effective labels and
resolved damage; support moves receive no damage-effectiveness badge.

Walking recovery is 2% of equipped maximum HP per 100 accepted feet for conscious
party members, without reviving knockouts or banking healing at full health.
First evolution costs 6 essence of each required element instead of 12; later
costs stay unchanged. Every evolution increases HP, attack, defense, speed and
maximum energy. Evolution cards show current/required essence, progress bars and
stat gains before committing.

New social battles use version-two balance. Existing duel and raid snapshots retain
version-one combat calculations when replayed, including their prior stats and
matchups. This prevents an app refresh from rewriting an ongoing fight’s history.

## Regional challenges

Later regions now require 2, 6, 12, 20, 32, 48, 72, 100 and 140 cumulative accepted
walking kilometres. Migration preserves every region earned under the earlier
thresholds, together with positions and exploration distance. Practice remains
separate and expedition restarts do not erase unlocked regions.

Each of the ten regions has three permanent challenges for walking within that
region, finding discoveries and winning wild encounters there. Players can track
a challenge to keep its progress bar on Walk and manually claim its exact fruit
and consumable reward after completion. Claims are recorded permanently and
cannot be repeated after reload or a new expedition. Event counts start when this
update is installed; historical exploration distance remains valid. Claims are
available on the trail or at camp, preserving battle inventories.

## Regional adventures and companion progression

The Walk screen and five navigation tabs remain in place. Ordinary rivals now become a single saved **Encounter ready** invitation. Foreground walking continues to earn the existing discoveries, walking XP, and 2% HP per 100 accepted feet for conscious companions. Begin the fight when safely stopped, or pass without battle rewards. Opponents, difficulty, region, and habitat stay fixed across reloads and region changes; no fight backlog accumulates. Actual battles still stop walking. Camps and Ultra safe-route sightings remain separate, and a waiting ordinary rival survives them.

All ten regions now have real species pools, terrain habitat preferences, seven connected adventure steps, guarded landmarks, regional objects, and one-time keepsake rewards. The field guide's **Find this monkey** shows valid homes, unlocks and expedition-depth requirements. Pinned research advances only through accepted walking in an eligible region and eventually reveals the chosen encounter; capture rules still apply. All 78 ordinary species and 12 Ultras have tested encounter paths. Compass direction gently biases eligible species; it never requires a particular physical route. Existing region unlocks and challenge claims are retained.

Team management groups actions into **Train · Abilities · Evolve · Equipment**, with names and compact adventure memories under Details. Four named lineups use individual identities and the existing camp, injury and equipment rules. Evolution requirements, research, adventure steps, and existing challenges share one player-selected pinned goal. Permanent friendship uses existing Bond: accepted kilometers and expedition victories add progress, while one species material provides exactly four Bond. Named tiers unlock poses, titles and decorative details. Conversion previews explain that a newly recruited duplicate becomes three species materials, and protect favorites, equipment and saved lineups. A lightweight Sanctuary scene features up to three favorites and stops idle motion offscreen or under reduced-motion/battery preferences.

Battle cards show main effects, energy, current effectiveness, availability reasons, cooldowns/uses and damage estimates with separate Details buttons. Previews never change state or consume RNG. Regional guardians telegraph charges, heavy attacks, shielding and recovery, with a lower-health pattern change; selected new tribe-boss attempts add telegraphed wards without changing old in-progress attempts. Friendly battles continue to use protected copies.

New Adventure and Continue use the existing encrypted credentials, backups and recovery workflow. Contextual tips introduce useful actions. Recruitment and evolution moments are saved receipts that can be skipped or replayed from Trail activity without spending resources again. Ordinary finds remain brief nonblocking notices; important rewards and invitations also remain in saved activity. **Finish walk · view recap** closes only the walk session: the expedition, waiting encounter, injuries, supplies, permanent progress and chosen goal stay saved.

Optional weekly tribe adventures accept multiple contribution types and use Firestore transactions, stable weekly member records and immutable claim records. See [TRIBE_ADVENTURES.md](./TRIBE_ADVENTURES.md) for reward details and deployment. **Publish the updated complete root `firestore.rules` before serving the updated client.** No production Firebase configuration, server or Cloud Functions are added. New presentation reuses existing local artwork and CSS/SVG scenes; no new Cloudflare asset upload is required.

### Verification for this update

- `npm run test:monkey-trails` includes the full existing and new module suites: migration, ecology/reachability, reward idempotence, pending encounters, exact combat previews, friendship, lineups, sessions and social protection.
- `npm run test:monkey-trails:social-rules` includes weekly membership, contribution delta, immutable claims, reconnect/offline-week recovery and existing trades/duels/boss payouts against the Firestore emulator.
- `npm run test:monkey-trails:adventures-browser` adds six routed browser flows for onboarding, pending encounter reload/pass, recap persistence, evolution replay, lineups and essence discovery notifications. Existing browser fixtures now explicitly begin waiting encounters and navigate the companion sections.

The implementation was checked with 240 passing module tests, 104 Firestore emulator assertions and a DOM integration exercise of encrypted practice, onboarding, companion controls, lineups, evolution, hunts, walking, recap and battle previews. The environment could not install Chromium, so full browser layout/animation assertions and physical-phone GPS, lock-screen, outdoor readability and production multi-account verification remain to be run. The DOM exercise did not validate SVG rendering or phone layout. No production data or rules were changed during verification.


### Choose the opening monkey

Before a wild, researched, or regional guardian battle starts, **Who goes out first?** lets you choose any conscious member of the current team. Ultra confirmations and friendly challenge/boss confirmations include the same choice. Selecting the starter costs no action and preserves HP, energy, status, equipment and the three party slots. The current active companion is the default; knocked-out companions require revival first.

After confirming the start, changing monkeys uses the existing combat switch action. Reloading a paused fight cannot reopen the free opening choice. Older active battles retain their current fighter and normal switch rules. Friendly duels and tribe bosses put the chosen starter first in protected battle snapshots; existing battles and raid attempts retain their saved order. No Firestore rule or asset changes are required.

`battle-lead-tests.mjs` covers no-cost choice, ineligible companions, save/reload restrictions, actual opening actions and protected social snapshots. The routed `browser-adventures.cjs` suite includes lead selection, disabled knocked-out choices and normal switch costs after reload.
