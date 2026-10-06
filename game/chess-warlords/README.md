# Chess Warlords modernization

The online entry point is `game/chess_warlord.html`. `core.mjs` contains pure snapshot, ranking, selection, and cosmetic-budget helpers. Existing movement rules and faction abilities are preserved.

## Strategic victory modes

Solo matches default to **Grand War** in both `chess_warlord.html` and `chess_warlord_solo.html`. Choose Grand War, Dominion, Capital Conquest, or classic Total War / Regicide before starting. Multiplayer retains Regicide.

Grand War ends when a realm reaches 120 Dominion points, holds four enemy capitals (or every enemy capital in smaller matches), or becomes the last surviving faction. Dominion uses points and regicide; capitals still weaken rivals. Capital Conquest uses simultaneous enemy-capital ownership and regicide.

Seven permanent banners are placed on reachable land away from starting capitals. Capture takes six seconds in a two-tile radius; the center awards two points every five seconds and other banners award one. Hostile presence denies scoring, multiple factions freeze capture progress, and ownership persists without a garrison. More troops do not shorten capture time. A nearby defending tower multiplies capture time by 1.5; a general multiplies it by 4/3. Together they turn six seconds into twelve. King conquest transfers the defeated realm's objectives and awards 15 points once.

Capitals remain at starting locations and take twelve seconds to occupy or retake. Occupation halves token generation and timed troop production and reveals the original realm's king, including through Shadow fog. Recapture restores production and concealment. Capital counts exclude one's own home capital. The HUD and minimap show ownership, contests, capture progress, and victory pressure; HUD markers focus the camera. Finished matches freeze gameplay and show the winning condition and capture statistics.

`objectives.mjs` owns match-time capture/scoring and a shared personality-weighted planner. It preserves king guards, home defenders, frontier units, and reserves; caps normal offensive squads at 30% of movable troops per target; evaluates leader pressure, travel routes, local strength, and losses; stages viable attacks and retreats or abandons hopeless attacks. Routes are cached and bounded, and plans refresh on important changes or after 2.5 seconds. Tide's existing standalone personality is supported without reintroducing Tide to the modern faction picker.

Objective state is included in cumulative checkpoints and cloned before publishing or hydrating to prevent an in-flight snapshot from changing with the live simulation.

Additional verification:

- `npm run test:chess-warlords`: objective rules, planner scenarios, reversible production penalties, and snapshot isolation alongside existing tests.
- `npm run test:chess-warlords:objectives-browser`: both entry points at desktop/mobile sizes; capture, denial, recapture, paused/final scoring, and Dominion/Imperial finish screens. Firebase imports are stubbed.
- `npm run bench:chess-warlords:objectives`: seeded, real-production AI matches with 2, 6, and 8 realms plus a six-realm capital match, each with a twenty-minute simulation budget. Asserts victory, reserves, home defense, offensive squad limits, and bounded route caches. Uses the same Playwright/Chromium setup as the browser smoke test.

## Synchronization

Protocol 2 sends cumulative deltas against `chess_warlord/full_state`, identified by `baseVersion`, map seed, and controller epoch. A client reconstructs from that checkpoint each time; it does not assume Firestore delivers every intermediate revision. This also handles a unit or territory cell returning to its original checkpoint value.

The controller atomically commits the current state, optional checkpoint, and command acknowledgments after checking its lease in the same transaction. In-flight simulation changes remain dirty. Checkpoints roll after 24 seconds of active updates or when a delta approaches the full snapshot size; idle scenes do not force periodic writes. Recovery reads the checkpoint and latest packet from one consistent transaction. Host takeover hydrates before processing commands. Commands bind to the authenticated presence occupying their slot, and a bounded receipt history prevents replay after takeover.

Presence uses one shared collection listener and a 12-second heartbeat, plus meaningful readiness/faction changes. The host lease renews every 10 seconds and expires after 30 seconds. A defeated host keeps simulating the surviving factions.

Use freshly loaded clients in new rooms when evaluating protocol 2. Previously deployed clients do not understand cumulative checkpoint deltas. This retains the existing client-hosted Firebase trust model, not a server-authoritative anti-cheat service.

## Competitive progression

Ratings keep the existing 100 ELO starting value. Bronze, Silver, Gold, Diamond, Masters, and Grandmaster have visible divisions where applicable. Pairwise expected results are averaged across opponents, using match-start ratings and final placement. Capturing kings adds a bounded bonus of at most 3 ELO. The winner and eliminated players receive separate, idempotent result transactions; disconnected players can be settled by the controller.

Profiles use the hub's lowercase `users` collection. The old uppercase `Users` and lobby profile paths are read as migration fallbacks. Result receipts live in the existing lobby subcollection namespace, so no rules deployment is required with the checked-in rules.

Leaderboards query at most 50 entries and fetch the viewer's own position with a count query. Friends is a personal list of up to 30 exact usernames; it sends no invitations. Quarterly season points reward placement (field size minus placement plus one); faction standings use wins with that faction. ELO, lifetime statistics, and the latest eight season records are stored at match settlement, not on each move.

## Rendering and controls

Ground-cell priority and enlarged pointer targets resolve overlapping pieces. Legal moves, selection, feedback, and predicted movement remain local. The compact unit strip exposes cooldown, HP, ability state, and a camera button. Minimap navigation eases toward its target; selection, kings, objectives, and recent visible battles are marked without exposing hidden Shadow units.

Terrain caches survive territory and wall updates. Only affected chunks become dirty. Auto quality samples normal frame intervals, even without debug mode, and removes ambient particles, decorative animation, effect density, shadow complexity, then terrain detail before reducing raster resolution. Selection and tactical indicators remain visible. `?perf=1` includes sent-payload versus full-state byte estimates and recovery counts (JSON estimates, not Firebase billing measurements).

## Verification

- `npm run test:chess-warlords`: pure helper tests plus extracted production-function tests for in-flight writes, lease fencing, idempotent placements, terrain invalidation, and existing faction rules.
- `npm run test:chess-warlords:browser`: requires Playwright and Chromium. Optionally set `CHESS_TEST_CHROMIUM` to a compatible headless executable. Tests desktop and mobile viewports, keyboard faction selection, match entry, board picking, unit details, minimap movement, missed-state reconstruction, and prediction rollback.

The browser harness intercepts Firebase imports and uses missing-asset fallbacks; it never writes to production Firebase. Real multi-device latency, deployed permissions, and imported-art appearance still need a staging match before merge.
