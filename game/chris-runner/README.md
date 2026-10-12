# Chris Runner — realms and chases

The existing `game/chrisrunner.html` entry point loads `upgrade.js`. No build step or new runtime dependency is required; both files must be deployed together. The current Three.js 0.160.0 dependency remains unchanged.

## Controls and characters

Movement and the upright follow camera are retained. Player 1 uses Space for the selected character's special and Shift for sprint. Player 2 uses O for special and Enter for sprint. On touch screens, tap the existing sprint button in under 180 ms for the special; hold it to sprint. Cancellation, lost pointer capture, focus loss, and hidden tabs release sprint. Low energy uses a vignette and a recovery threshold to avoid alternating run/sprint speeds each frame.

| Character | Ability | Energy | Tradeoff |
| --- | --- | --- | --- |
| Chris | Repulse: push and briefly stun nearby enemies | 30 | Efficient endurance; short reach |
| Dawit | Dash: fast, collision-checked movement | 30 | Fastest runner; higher sprint drain |
| Jay | Sidestep: directional dodge with a brief shield | 20 | Excellent lateral movement; slower forward pace |
| Anthony | Parkour: animated vault over marked obstacles | 50 | Athletic, agile; expensive ability |
| Kevon | Smash: destroy marked terrain and surge through | 25 | Heavy build and afro; slowest runner |

Amber braces mark breakable/vaultable obstacles. Solid capped obstacles remain impassable. Invalid targets or blocked landings spend no energy. Low hurdles in optional parkour routes automatically vault for every runner; Anthony traverses faster and can clear taller marked barriers. Specials activate once per press, with per-player cooldowns.

## Sections and environments

The active `planRunnerSection` / `addRunnerSection` pipeline carries each exit into the next entry. Corridors can travel through the left, center, or right. The planner reserves the complete swept route, including two runners at ±1.3 units and a 1-unit collision radius, across neighboring obstacle rows and chunk boundaries. It supports parkour, slalom, pursuit encounters, narrowing passages, shortcuts, S-curves, zigzags, alternating gates, splits, rare gauntlets, sprinting stretches, and recovery. Around half of challenge sections block the center. Complexity increases gradually without raising enemy caps. Consecutive challenge kinds differ.

Optional edge routes offer gold behind obstacle rows; the wider cyan corridor remains usable by ordinary movement. Amber obstacles retain Smash, Parkour, and automatic low-hurdle traversal. Recovery occurs at least every seventh section, offers energy, and spawns no new enemies. Pursuit waves warn on approach. Existing pursuers can still follow into a recovery section.

All ten original environments remain, with six additional realms:

| Realm | Scenery and situation |
| --- | --- |
| Haunted Carnival | Broken booths, carousel platforms, occasional Ferris wheels, warm eerie lights, swinging panels, ambushers |
| Volcanic Rift | Dark crags, lava fissures, rising embers, amber warning rings before vent eruptions, heavy/ranged threats |
| Crystal Caverns | Faceted crystals, overhead cavern ribs, narrow routes, warning-before-collapse shards, jumpers |
| Neon Grid | Circuit roads, holographic towers, pulsing energy gates, fast cyber-styled pursuers |
| Storm Harbor | Dock containers, cranes, pier debris, slanting rain and wind, shifting cargo, heavy blockers |
| Clockwork Citadel | Brass towers, rotating gears, timed piston doors, mechanical pursuers |

Realm transitions align to six chunks (252 yards), with open entrance arches and blended sky, fog, and lighting. The five-realm introduction reaches Dream at 1,008 yards. Seeded selection then visits the remaining realms in varied order before using absence-weighted selection with a four-realm repeat exclusion. The schedule is independent of query order and cosmetic animation. Every realm appears in the first sixteen visits.

New moving situations remain inside conservative collision envelopes outside the reserved route. Energy gates and piston doors stay open while either runner occupies their clearance area. Dream eruption placement also excludes the entire swept corridor and skips a hazard when no fair placement exists. The safe route guarantees terrain clearance, not immunity from pursuing enemies or voluntarily entering hazards.

## Models and animation

Enemy classes use distinct equipment, proportions, clothing, and silhouettes: layered jackets/backpacks, athletic sprinters, armored heavies, hooded ambushers, shield blockers, padded jumpers, equipped throwers, and caped elites. Seeded position variants alter hair, skin, height, stripes, and accessories. Realm accessories add carnival hats and clockwork/cyber details. Existing dogs, police, desert stalkers, and chase conversion remain.

Lightweight knee/elbow rigs use alternating stance/return phases and two-bone leg positioning. Cadence follows measured movement, with blended speed, turning, torso lean, exhaustion, jump preparation, airborne poses, landing compression, and recovery. Heavy, fast, and elite poses differ. Canines blend diagonal trot pairs into front/rear gallop coordination with spine, head, and tail motion.

Dream dogs use opaque contrasting bodies, pulsing eyes, restrained trails, and icy-cyan inverted-hull outlines on the torso, chest, head, snout, ears, tail, and every leg. The outline bypasses atmospheric fog, retains depth testing, and stays enabled on mobile. A subtle outer violet layer is desktop-only. Solid obstacles continue to occlude the dog.

## Resource management

Scenery and static model details use instancing; model materials and limb geometry are shared within each resource owner. Removed chunks, enemies, controlled chasers, projectiles, debris, and temporary effects dispose GPU resources. Distant animation updates accumulate elapsed time, small facial details and shadows reduce at distance, and fully fogged chunks stop submitting geometry. Mobile retains reduced pixel ratio, 512px shadows, fewer particles, and fewer scenery effects. Dog outlines and all gameplay generation remain independent of graphics detail. Visual animation consumes no gameplay randomness.

Replay format version is now 2 and the generation build is `chris-runner-realms-v5`. Older files are explicitly rejected. New seeded starts reproduce terrain and realm order. The existing wall-clock/snapshot replay mechanism remains; exact simulation replay at every frame rate is not claimed.

## Validation

Dependency-free syntax and 10,000-section route tests:

```sh
node --test game/chris-runner/upgrade.test.cjs
```

Full integration tests use the same Three.js browser build as the game:

```sh
curl -fL https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js -o /tmp/chris-runner-three.js
THREE_SOURCE=/tmp/chris-runner-three.js node --test game/chris-runner/upgrade.test.cjs
```

The fifteen tests cover 10,000 connected sections with both players' full collision clearance, center-blocking frequency, seeded realm variety, abilities, blocked landings, automatic hurdles, touch cancellation, enemy behavior/models, dog outline materials, animation transitions, all hazard phases, occupied-door safety, two-player chase/tag swapping, seeded restart, and 300 chunks / 12,600 yards of bounded cleanup. Integration checks use real Three.js scene objects with a simulated DOM.

Optional browser tooling (no runtime dependency):

```sh
THREE_SOURCE=/tmp/chris-runner-three.js node game/chris-runner/browser-smoke.cjs
```

Set `PLAYWRIGHT_PATH` if Playwright is outside the project's dependency path, `CHROME_PATH` to use an installed Chromium browser, and `RUNNER_CAPTURE_DIR` to save screenshots and rendering metrics. `RUNNER_TEST_REALMS=dream,carnival` limits a targeted rerun. `RUNNER_TEST_ROOT` can point to an unmodified checkout for the forest/Dream comparison.

Headless Chromium with desktop and 430×860 mobile viewports rendered all sixteen realms without page errors, then exercised real keyboard input and all five character selections. Captures were visually inspected. Sample render draw counts decreased from 1,017 to 813 (forest desktop), 2,665 to 1,881 (Dream desktop), 804 to 548 (forest mobile), and 2,280 to 1,398 (Dream mobile). These are software-renderer samples with different generated layouts, not a controlled hardware frame-rate benchmark. Physical-phone frame rate and hands-on long-run play remain manual QA before release.
