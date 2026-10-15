# Commands, army navigation and the connected frontier

This update extends the existing game. Tribe selection, population and equipment progression, settlements, prisons, vehicles, alarms, saves, fixed simulation steps and touch controls retain their existing systems.

## Command presentation

Charge, Follow, Hold, Defend, Retreat, Attack and Movement use cloth standards, claw scratches, shield emblems and short isometric footprint trails. Attack markers track the live target without covering its sprite. Selected troops show their actual current order, including mixed groups, and the command dock highlights matching orders. Recruit-radius adjustment and combat hazard warnings remain functional.

Order-driven presentation adds a brief charge response, faster strides and forward lean, defensive bracing, shield hand positions, heavier gorilla footfalls and withdrawal poses. These read current gameplay state and never delay or move a unit. Interrupted orders change immediately; combat, injury, climbing and death clips take precedence. Reduced motion retains essential poses and disables command dust.

### Reusable artwork manifest

No external graphics or new hosting paths are required. `command-visuals.js` creates shared Canvas bitmap frames. Existing raster artwork, offline embedding and Cloudflare asset references remain compatible.

| Asset | Frames | Dimensions | Anchor | Timing |
| --- | ---: | --- | --- | --- |
| charge/follow/hold/defend/retreat/attack/movement standards | 4 per style | 96 × 112 | 48, 96 | 9 Hz |
| footprint, target scratch, dust | 1 each | 96 × 112 | 48, 96 | renderer fade/footfall phase |
| command dock emblems | 1 per button | 48 × 48 | centered | static |

The shared cache is capped at 31 frames (about 1.34 MiB of RGBA pixels). Each frame paints at most 8 command markers, at most 4 persistent selected destinations, and at most 24 spatially shared dust stamps. Low quality limits dust to 8; reduced motion uses none. Selection footmarks sample at most 24 troops. Route footprints acknowledge only the first 90 world units rather than suggesting a straight route through defenses. New command confirmations are protected within the existing hard effects cap during heavy combat.

## Navigation and performance

Crowd separation reuses local live geometry and a dry-terrain proof, while still intersecting exact trunks and thin walls. Formation slots contract when scenery or a wall blocks the space beside the leader. Distant followers first join the shared route; locally inaccessible recovery starts refine the route grid only where needed. Attackers choose reachable frontage lanes instead of one structure-center point. Barrier scans are throttled and invalidated by geometry changes.

Recovery uses ordinary collision-checked movement. There is no recovery teleport, and closed prison gates remain solid. Shared A* retains its per-tick caps of 3 search starts and 192 expansions. Existing spatial grids, terrain/sprite caches, offscreen culling, pooled effects, distant simulation and adaptive graphics remain active.

## Facilities, activity and consequences

Four blueprints join the ten existing families:

| Facility | Layout and role | Consequence |
| --- | --- | --- |
| Logging operation | Machinery, timber barricades, parked trucks, captives and work shelter | Destroyed supply machinery reduces nearby infantry replenishment |
| Communications station | Tall radio, security building, generators, watchtowers and captives | Destroyed radio delays nearby raid and resupply coordination |
| Frontier town | Housing blocks, police garrison, warehouse and separate rescue cells | Automatic military targeting excludes civilian housing |
| Expedition camp | Authored tent sprites, portable radio, transport and small rescue objective | Supports connected reconnaissance and prisoner operations |

Existing research facilities, checkpoints, prisons, armored depots, regional commands and layered fortresses remain available. New layouts keep intersecting routes and cage approaches clear for a 16-unit gorilla radius in all three layout variants. Tents, timber stacks, police posts and generators reuse authored environment sprites; other structures retain existing damage, lighting and destruction artwork.

The bounded encounter director dispatches connected patrols and prisoner transports using real finite personnel/vehicle/captive stock. Destroyed transports leave a rescue cage at their actual location. Unloaded or obstructed rescue locations retain deferred cargo until that exact location is available; no offset search crosses defenses. Arrival, interception, expiry and reload resolve cargo once. Regional facility effects persist when chunks sleep. Facility guards return to their posts, while operation forces retain their missions. Engineers repair surviving defenses only when uncontested.

Exploration adds sheltered caves, ruins, fruit groves, lookouts and lost ape refuges, with procedural clearings and one-time rewards. New facility distribution preserves the original opening rescue and small-tribe progression. Activity records and discoveries have compatible defaults in old saves.

## Verification and measured limits

The full automated suite passed 733 tests. Additional browser checks passed desktop/mobile menus, movement, rescue, settlement construction, settings, saving/reloading, combat rendering, hidden-tab pause, frame catch-up limits and permadeath; real touch gestures including tap/double-tap/hold, recall scopes, cancellation, routes and pinch; and all seven command states with 500 troops, rapid replacement, effect limits and reduced motion.

Navigation regressions send 25/100/250/500 actors through trunks, boulders, a narrow settlement lane and a prison barrier that is destroyed during travel. They assert every accessible actor arrives, exact movement distance bounds, no closed-gate crossing and shared-search budgets. Tests also verify human/tank melee, civilian targeting, regional sabotage, actual blueprint clearance, prisoner stock conservation, deferred rescues and serialized operation identity.

Matched simulation profiling used real actors, a deterministic seed/random stream, 180 ticks per scenario and identical inclusive timing instrumentation on Windows/Node 24 with a Ryzen 7 5825U. These costs exclude rendering and include cold-start/cache effects.

| Workload | Before mean ms/tick | After mean ms/tick |
| --- | ---: | ---: |
| 50 followers | 3.36 | 2.49 |
| 100 followers | 6.13 | 4.66 |
| 250 followers | 15.44 | 13.20 |
| 500 followers | 24.16 | 20.27 |
| 1,000 followers | 46.65 | 39.15 |
| 250 apes / 150 humans | 22.53 | 19.39 |
| 500 apes / 250 humans / armor / explosives | 37.89 | 31.12 |

500-follow mean simulation work improved about 16%; the combined battle improved about 18%. Command timings are recorded only for accepted commands; the combined-battle king was recovering from a blast, so rejected commands are not reported as fast responses. Measurements vary between runs. A 1,000-follower workload still costs around 39 ms per tick on this machine and is not a 60 fps guarantee. Full-artwork browser reports separately measure simulation, rendering, frame intervals and heap observations; maximum-detail combined battles remain expensive. Touch viewport testing uses desktop Chrome emulation, not physical handset certification. Longer device playtesting and balance tuning remain useful.

## Reproduce

```sh
npm run build:apes
npm run test:apes
npm run test:apes:browser
npm run test:apes:commands
node game/apes-together-strong/tests/mobile-commands-browser.cjs
npm run bench:apes:army -- --baseline /path/to/original/game/source --frames 180 --output army-comparison.json
node game/apes-together-strong/tests/visual-overhaul-performance.cjs --scenario GIJ --frames 120 --quality high --output browser-performance.json
```

Browser scripts require Playwright and Chrome/Chromium. Set `CHROMIUM_PATH` when using an installed browser and `QA_ARTIFACT_DIR` to save screenshots. Build the generated standalone HTML before opening or testing it. Existing art and audio are embedded by the normal build; no external asset installation is needed.
