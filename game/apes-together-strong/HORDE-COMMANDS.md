# Horde commands and crossing navigation

## Controls

| Input | Result | Command API |
| --- | --- | --- |
| Q | Call wild, idle, and settled apes within 340 world units into the active horde. | `call` |
| R tap | Recall recruited field apes within 260 world units. | `recall` |
| R hold | Recall all recruited field apes, regardless of distance. | `recallField` |
| T tap | Recall all recruited field apes, regardless of distance. | `recallField` |
| T hold | Mobilize every recruited ape, including settlement residents, defenders, workers, and young. | `recallAll` |

R and T use a 600 ms hold threshold. A tap commits on release; a hold commits once at the threshold and release does not repeat it. Keyboard repeat is ignored. The same gestures work on the recall arrows in the touch command palette. A visible progress bar distinguishes T's settlement mobilization from its ordinary field recall. Pause, blur, hidden tabs, canceled pointers, and lost pointer capture cancel unfinished gestures. The former T attack-nearest action remains available in the expanded command menu; directional E tap/hold orders remain available.

Q and nearby R use the ape spatial grid. Q mobilizes nearby residents and settlement scouts, including young, and recruits idle apes. Existing active field followers and their selected-species or division orders remain unchanged. Locked cages and unrescued prison cohorts retain their existing rescue requirements. Command pulses are bounded to one effect per order; feedback reports the actual affected population and mobilized resident count.

## Ownership, settlement duties, and saved games

`hordeOwner` records recruitment independently of transient movement state. New followers, settlement residents, and rescued prison followers belong to the King; free apes remain unowned until recruited. Existing version-1 saves derive this field once from their previous follower/resident state. A recruited ape temporarily marked free can rejoin through Q if it has no active field order; it also remains eligible for field recall. Apes belonging to another owner are excluded from Q.

R and tapped T exclude `settlementId`, including residents temporarily fighting or traveling on a settlement mission. Q within its radius and held T globally clear that active assignment and remove work, tower, training, caravan, and division orders. Each preserves the former assignment in `previousSettlementAssignment`, including its settlement, role, and home coordinates. Existing entities walk from their current positions; recall neither creates apes nor teleports them. Settlements do not automatically take those apes back. The existing settle, station, or reassignment actions can assign them again.

Young retain their age and health, travel without acquiring adult combat behavior, and mature under the existing 35-second rule. Ownership, recall metadata, and previous settlement assignments survive the ordinary save format. Movement caches remain transient and are reconstructed after loading.

New direct attack, movement, hold, or settlement orders clear `recallOrder` and reset the actor's navigation commitment. Recall is an ordinary following movement order, not a periodic process that overwrites later player commands.

## Navigation and performance

Procedural crossing definitions share their actual deck bounds with water collision and rendering. Wood, stone, military, and ford crossings provide broad walkable corridors and clear approaches. Infantry use stable lanes within the deck. Vehicles select only compatible crossings. A committed crossing retains its entrance and exit while the King moves, then resumes following on the far bank.

The navigation system shares regional streaming requests and local recovery paths, staggers queued searches, and retains distant followers' coarse simulation cadence. Stuck detection retries blocked approaches with a cooldown. Negative fortress routes temporarily reject unreachable targets; opening or destroying a gate invalidates those results. These routes retain ordinary wall, water, and vehicle-clearance rules.

## Validation

- Full integration run on 2026-10-08: `node --test tests/*.test.cjs` passed all 477 tests, with zero failures, in 64.61 seconds. This includes prison-control climbing and the new crossing, command, vehicle, structure, held-item, target-selection and local navigation-invalidation checks.
- `node --test tests/horde-commands.test.cjs` covers ownership exclusions, selected-species independence, exact nearby radius, field/global eligibility, resident and child mobilization, no duplication/teleportation, direct-order precedence, version-1 migration, bounded effects, and tap/hold cancellation.
- `node tests/horde-commands-browser.cjs` exercises the built game with real Chrome keyboard and CDP touch input. `--source` runs the same controls against a temporary source harness for iteration. Playwright and a Chromium installation are required; `CHROMIUM_PATH` can select the browser.
- Source-harness and built-bundle browser checks passed for Q, R, and T taps/holds; repeat suppression; pause/blur cancellation; and real touch tap, hold, and cancellation. Each gesture issued exactly one command. The existing visual-overhaul browser suite also passed against the integrated bundle, including local atlas decoding, all four graphics presets, keyboard/touch control, mobile pixel density, reduced motion, and missing-art fallback, with no browser errors.
- In isolated local regression runs on 2026-10-08, mobilizing 1,000 apes took approximately 6–12 ms, issued one pulse, and performed zero synchronous path searches. A concurrent full-suite run measured approximately 16 ms. These are command-dispatch timings, not whole-game frame-rate claims.
- Crossing regression tests exercise moving targets, 240 apes using three lanes over stepping stones, distant recalls, existing-save scenery repair, blocked-approach alternatives, fortress target rejection, destroyed gates, and shared requests for 1,000 recalled apes.

