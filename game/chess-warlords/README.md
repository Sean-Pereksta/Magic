# Chess Warlords modernization

The online entry point is `game/chess_warlord.html`. `core.mjs` contains pure snapshot, ranking, selection, and cosmetic-budget helpers. Existing movement rules and faction abilities are preserved.

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
