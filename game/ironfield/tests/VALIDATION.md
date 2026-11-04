# IRONFIELD 2.7 validation

- Capacity: 40,000 actual soldiers, 20,000 per side. Standard and Colossal deployment checks require bodies to remain in bounds, off solid terrain, and separated. Large-army tests verify every soldier moves once per step, cooldowns and body caches advance correctly, certified marches preserve internal distances, and fresh orders and impacts restore individual movement.
- Interface: 40,000 soldiers and all saved offsets survive Save and actual file import. All five new terrain brushes, six castle choices, four building tools, material names, Undo, road overlays, and secondary gate-ground checks pass. The atlas contains 30 authored fields, 32 authored scenarios, and 30 historical scenarios.
- Navigation: actual infantry and cavalry cross all five new fortress styles, both concentric rings, and all four moat causeways. Closed-to-open gates and breached earthworks release existing movement orders without changing their destinations. Houses, watchtowers, and barricades cause real detours. Sampled movement checks enforce solid-terrain clearance, body separation, and bounded step distances.
- Browser: source and standalone entry points pass desktop and 390px checks with no browser errors. All 30 historical and eight new authored deployments are bounded and clear of solid terrain. The real castle brush places the selected star-fort material.
- Ramparts: runtime checks cover physical climbing and descent, intermediate height, support and body separation, connected wall sections, larger companies, adjustable slots, elevated shots and synchronized musket ranks. Incoming-shot samples compare the same wall target from outside and from the courtyard. Breached decks cause real falls, adjacent gate breaches order intact guards down, and accepted ground orders resume after descent. UI checks cover projected wall and troop picking, pure previews, saved plans, custom stair sides, gate carving, and restored ground controls.
- The full authored Four Winds garrison retains all 3,258 battle participants and physically fills all 320 ranged guard posts by 387.733 simulated seconds. Both 160-soldier companies keep 100% cohesion and stay supported, finite, and separated, with a minimum nearby-body ratio of 0.96. Finite stair queues and distant posts take several simulated minutes to fill.
- Command checks cover disjoint previews, physical transfers between separate castles, melee defense against a real deployed assault platform, safe Retreat, and the loss of a garrison's only stair. In the last case, four guards drop from an intact stranded wall, take fall damage, and reach the courtyard without hovering or retaining a stuck guard order.
- Graphics: 40 checks cover troop animation, new terrain and structure art, elevated guards and projectiles, overview representation of every visible soldier, detailed sprites at close zoom, and terrain-cache invalidation. Cached terrain and direct drawing have identical compared pixels.

## Large-army performance measurements

The repeatable mixed 40,000-soldier open-field march improved from about **321 ms to 39.5 ms median simulation step time** in Node, approximately 8.1 times faster. The optimized default deployment gives companies real space, state storage avoids slow dictionary objects, unnecessary navigation work is skipped, and geometrically clear distant companies can share marching calculations. Contacts, turns, terrain, and nearby formations use the individual solver.

The actual Chromium run retained all 40,000 living visible soldiers with finite in-bounds coordinates. At a full-field view it measured **67.7 ms median simulation**, **21.4 ms rendering**, and **88.4 ms combined**, about **11.3 updates per second**. The close view kept 3,716 detailed sprites and measured about 10 updates per second. These are measurements on this machine; the maximum-size case remains CPU-heavy. Renderer-only speedups are not claims of total game frame rate.

Append-only deployment repair skips candidate positions already proved occupied. Complete before/after state hashes match for Standard and Colossal 40,000-soldier deployments, dense explicit imports, and an extreme malformed all-types roster. The last case improved from 122.58 to 55.87 seconds; that impossible-density import remains expensive. Ordinary mixed 40,000-soldier construction measured about 0.2–0.25 seconds.

Run `node tests/grand-performance-bench.cjs` for the local simulation benchmark. Run `node tests/engine-test.cjs` for the full simulation, capacity, terrain, and fortress regressions; `node tests/expansion-ui-test.cjs` checks the new editor and large file roundtrip. The command, menu, graphics, and animation tests listed below remain applicable.

## IRONFIELD 2.6 validation

- The company-volley fixture fires 36 musketeers together in both a three-rank line and a three-file column. Individual reloads last at least 24 seconds. With 32 formed musketeers and four distant scouts, seven in range hold fire and eight release the volley. Movement orders suspend fire, and close combat interrupts it until the company reforms.
- Automatic companies are limited to 90 musketeers. Explicit authored battalions retain their count. Historical named sectors survive capture and load.
- An isolated impact fixture records approximately 47 world units of displacement from a catapult, 69 from a cannon, and 79 from a trebuchet. Actual results depend on target mass, terrain, and neighboring bodies.
- All 20 historical opening deployments have complete explicit soldier counts, clear terrain, and separated bodies. Characteristic depth, wings, reserves, and army-relative flank orientation have dedicated assertions.
- Menu checks cover historical search, eras, sources, preparing the selected scenario, army scaling, formation selection, saved formation styles, and returning to the battlefield collection. Browser checks cover desktop, 390-pixel mobile width, all 20 deployments, and both source and offline HTML entry points.
- Graphics checks cover all 23 complete portraits, seven distinct biome detail sets, movement/fighting poses, musket melee transitions, and bounded terrain animation that freezes with simulation time and respects Reduced animations.

Reproduce with `node tests/engine-test.cjs`, `node tests/command-ui-test.cjs`, `node tests/menu-world-ui-test.cjs`, `node tests/historical-menu-test.cjs`, `node tests/graphics-test.cjs`, and `node tests/animation-corpses-test.cjs` from the game directory. Build the standalone game using `python build.py`.

## Previous release: IRONFIELD 2.5.1 reload balance

Two wings of 64 Banner Lancers charge 128 musketeers backed by eight field cannons on open ground. The defenders hold out of range for 32 simulated seconds before the charge order, so both versions begin with loaded weapons. The table gives results at 25 seconds after the charge order.

| Deployment seed | Build | Riders remaining | Riders entering the musket line | Riders damaging a cannon |
| --- | --- | ---: | ---: | ---: |
| 47319 | 2.5.0 | 65 | 67 | 2 |
| 47319 | 2.5.1 | 108 | 88 | 9 |
| 94 | 2.5.0 | 62 | 80 | 0 |
| 94 | 2.5.1 | 106 | 86 | 10 |

In seed 47319, the defenders fired all 128 muskets and all eight cannons before the first rider landed a melee hit. By 25 seconds they had fired 212 musket shots and eight cannonballs, compared with 454 and 24 in 2.5.0. The firing-interval tests follow three successive projectiles per weapon against troops and three cannon rounds against a wall, including a gun knocked back during its reload.

```sh
node tests/gunline-charge-test.cjs
node tests/ordnance-test.cjs
```

## Previous release

# IRONFIELD 2.5.0 weapon checks

Run the following from the extracted game directory.

```sh
node tests/ordnance-test.cjs
node tests/club-sweep-test.cjs
node tests/graphics-test.cjs
node tests/command-ui-test.cjs
```

The musket test fires at a soldier screened by a second enemy and watches the front soldier take damage and move backward. The cannon tests track a survivor and a fatal casualty from the shot through landing. Closed walls stop low shots and shield troops from a blast on the other side. The trebuchet test sends a boulder over the same wall to reach a soldier beyond it. Explicit bombardment tests follow the projectile until it reduces the target structure’s health.

In an open-ground charge, a single club swing hit three levies. All three moved about 12.6 world units in the next 0.3 seconds. A rock immediately behind them limited displacement to about 2.5 units. Targets outside the sweep and friendly troops received no club damage.

The troop picker tests cover all 23 types, team switching, placement choices, preservation of hidden recruitment counts, keyboard navigation, and the live reload display.

## Previous release

# IRONFIELD 2.4.0 battlefield checks

The new regression scenarios measure soldier positions on successive simulation steps.

| Situation | Observed behavior |
| --- | --- |
| 24 swordsmen recalled during melee | All 24 stepped away on the first 1/30-second simulation step under either Move or Retreat. |
| Scattered formation on open ground | The soldiers kept their lateral positions for the first second of marching. After arrival their width decreased from 260 to about 113 units as they formed up. |
| Stonekeep north gate | A ram physically reached and breached the gate. All 128 foot soldiers rounded the northwest corner and crossed through the opening. |
| Closed castle gate | 24 attacking infantry and, in a separate trial, 24 defending cavalry waited outside a closed gate. Once it opened, all 24 completed the crossing under their existing Move order. |
| Three connected defending companies | Sustained contact gradually displaced each company by about 11 units over four seconds. The disconnected control stayed in place. |
| Morale collapse | A 48-soldier regiment with 12 survivors held together at very low morale. With nine survivors it routed in the same combat conditions. |

The Castle editor test exercises drag and click placement, gate ownership, terrain collisions, Undo, and saved corner towers. Battlefield controls cover mouse and touch operation of the gates. The pressure tests check regimental contacts, cancellation, and inherited motion in nearby supporting ranks.

## Previous release

# IRONFIELD 2.3.2 lancer movement

The included `examples/lancer-convoy.ironfield.json` places 96 lancers in two companies with alternating ranks. The same simulation issued one group Charge on the same field in each build. Forward pace comes from changes in each rider’s position during seconds 3–10.

| Measurement | Uploaded 2.3.1 | 2.3.2 |
| --- | ---: | ---: |
| Average forward speed, world units per second | 87.105 | 151.800 |
| Slowest rider’s average forward speed | 80.091 | 151.800 |
| Rider-frames below one world unit per second | 587 of 20,160 | 0 of 20,160 |

In 2.3.2, both companies reached the enemy and landed hits within 17 simulated seconds. Two isolated lancers given opposing Move orders also passed at 132 units per second with 22.87 units of sideways clearance. A second scenario sends two companies of 48 lancers from neighboring positions toward one enemy. Their slower riders sustained 151.8 units per second during the measured approach, and both companies engaged the defenders.

The collision checks measure the space between bodies over each whole simulation step. A rider can still lose ground speed during a tight turn, when a crowded crossing leaves no clear lane, or after an enemy impact. Run the two focused suites from the extracted game directory.

```sh
node tests/friendly-mounted-test.cjs
node tests/mounted-traffic-test.cjs
```

## Previous release

# IRONFIELD 2.3.1 validation

## Release scope

This release fixes friendly mounted congestion. Banner Lancers, Royal Cavalry, and Steppe Outriders can open small real lateral gaps while keeping their forward acceleration through a progressing friendly brush. Clear charging files advance their front riders before their followers, and densely packed charges retain their captured approach lanes until the enemy front is near. Assigned crossing routes and deployed combat files keep control of their own positions.

The exact shipped 2.3.0 archive is the before-change comparison. Its SHA-256 is `0a5d0601012c5fa572f7f50ad2809023f6db357dfa373327033d946bdd8d6e29`. Base troop speeds, mounted acceleration rates, damage and impact formulas, and counter rules are unchanged. Actual outcomes can change because bodies reach contact in different positions.

No browser or HTML preview was opened. Rendering checks use the instrumented Canvas harness. Simulation timing is measured separately in Node and is not browser FPS.

## Final correctness gates

| Suite | Passed | Failed |
| --- | ---: | ---: |
| Simulation, formations, commands, terrain, counters, impacts, siege, strategies, and scale | 119 | 0 |
| Command interface | 31 | 0 |
| Menu and battlefield-size interface | 13 | 0 |
| Canvas rendering | 25 | 0 |
| Animation and corpse rendering | 14 | 0 |

**202 checks passed, with no failures.** The separate 12,000-soldier command-scale audit also passed all five phases. The engine total includes six new friendly-mounted gates and the retained mounted-speed, formation-pressure, breakthrough, siege, strategy, and large-battle tests. Animation coverage includes one additional actual-courtesy-movement gate.

Source and harness hashes were captured before and after every complete suite. Packaging requires both snapshots to agree and the archive's source to match those exact hashes. The physical separation floor remains 0.90 times combined body radii; the per-step displacement ceiling remains `1.8 * baseSpeed * dt + .25`. Neither limit was relaxed for this release.

## Measured friendly mounted movement

The identical public deployment and command harness runs against the old and new release. The packed fixture has 48 lancers at 22.5-unit file spacing; its clear-file control uses 28-unit spacing. Interleaved companies contain 24 lancers each. Paces below are actual forward body displacement per second after the initial acceleration, not requested velocity or a displayed speed estimate.

| Encounter | 2.3.0 pace | 2.3.1 pace | Change |
| --- | ---: | ---: | ---: |
| Packed 48-rider Move: sustained pace | 94.442 | 113.609 | +20.3% |
| Interleaved 24-rider company A: sustained pace | 95.427 | 116.160 | +21.7% |
| Interleaved 24-rider company B: sustained pace | 95.624 | 116.160 | +21.5% |
| Packed 48-rider Charge: sustained approach pace | 91.628 | 151.800 | +65.7% |

Packed Move reaches **97.8%** of its clear-file control's **116.160** pace. All **48/48** riders pass the fixed finish line by 12 seconds, compared with **30/48** before. The slowest rider sustains **107.908**, so the improvement is not confined to the leading horses. Both interleaved companies get every rider across the line while retaining their cohesion and span gates.

| Packed Charge observation | 2.3.0 | 2.3.1 |
| --- | ---: | ---: |
| Maximum pre-contact formation span | 386.396 | 278.488 |
| First real damage, seconds after Charge | 17.567 | 12.667 |
| Distinct riders dealing damage | 22 | 21 |
| Actual total hostile health lost | 1,849.380 | 1,907.682 |
| Actual impact damage | 744.850 | 717.169 |

The initial Charge span is **193.552** world units. Its largest approach span is **27.9% smaller** than before. The contact gate verifies actual lost health and event accounting, and every recorded hostile body strike removes mounted speed. Friendly passage produces no impact damage or launched horses.

The six new gates pass on the final release. Against the exact 2.3.0 source, five fail: packed Move pace, the interleaved-companies gate, crowded Charge pace, held-rider courtesy clearance, and the released narrow queue. Fresh command precedence already passes the baseline and continues to pass. These failures demonstrate that the tests detect the requested change.

Friendly harness SHA-256: `074ff7cd994409f46ee32e2fcd1a5043fe855e2e66098e345ca9c27fb3d7f3ad`.

## Real clearance, command ownership, and blocked controls

Courtesy displacement uses ordinary swept-body collision and terrain clearance. It shares each soldier's existing movement allowance with voluntary movement and hostile pressure. The lateral allowance is bounded to 35% of base speed per step, cumulative offset is limited to approximately one file, and each request explores at most six bodies over two additional chain levels. Free-Charge ordering buffers are reused per regiment. No predicted vacancy is treated as empty space.

The held-rider control opens **3.919** units sideways. It records **0.000** maximum forward displacement, **0.000** charge power, **0.000** facing error, and **0.000** stale drift after passage. Pause freezes both the positions and simulation time. A held horse keeps its shifted individual anchor rather than moving back into the following files.

Fresh Hold, Face, Retreat, and reversed Move discard old courtesy authority. Their first turning or stopping interval records zero translation and zero retained charge power. The Retreat control makes **248.644** units of progress along its accepted initial withdrawal axis; the reversed Move makes **373.850**. Later regrouping remains part of ordinary Retreat behavior. Fresh receiving-side Face and Retreat turns also remain stationary despite an incoming friendly request.

A public three-rider case sends two moving horses past the same held neighbor with a 30-degree difference in their accepted directions. A recursive clearance step cannot push either rider backward along its own route. The final case records **0.000** minimum progress per frame and **242.483 / 280.331** total progress after three seconds, with no damage. This subcase fails on the unguarded courtesy implementation, passes the final guard, and also passes 2.3.0, so it catches an introduced regression without changing the baseline feature comparison.

In a 40-unit-wide rock passage, a physically blocked queue records **0.000** charge power. Once the leading horse receives Move and vacates the corridor, its follower advances **552.279** units, compared with **0.102** before. Its final measured forward pace is **100.158**. The fix finds nearby terrain clearance before the old coarse navigation search can choose an unusable detour around the wall.

The long-frame packed test uses `dt = 0.1` and measures complete frame sweeps, including courtesy movement received before or after a rider's voluntary move. Minimum separation is **0.924** times combined radii, above the unchanged 0.90 floor. Its largest complete-frame step is **16.758** world units. Solid-wall swept checks pass as well.

## Retained battle and strategy behavior

The original 2,534-soldier Broken Charge runs for 90 simulated seconds and its doubled 5,068-soldier opening runs for 40 seconds. Both pass the unchanged no-repeated-freeze checks. The original watched mounted cohorts physically advance and deliver real attacks. The final traffic correction gives a regiment's assigned passing lane priority over temporary courtesy offsets, so an old offset cannot trap it at a staging waypoint.

Mixed-width Rapid Encirclement wings retain separate physical lanes, bounded transit spans, and actual rear attacks from all four wings. The ordinary Encircle rear attacks, blocked-route replanning, world-boundary fallback, direct-order overrides, split formations, and recovery cases also pass. Shock cavalry already deployed near an enemy front keeps its supporting combat files; local enemy contact includes nearby opponents outside the currently selected target.

All retained lancer and Royal Cavalry breakthrough suites pass: real interior penetration against held levy ranks, full versus short run-up, prepared hoplites and pikes, heavy defenders, rocks, and an engaged Retreat. These gates count riders that actually damage the defenders and cross their original rear inside the frontage; going around an end does not count. Hostile contact still consumes actual forward speed, and lateral courtesy movement contributes no charge energy.

All five mounted-speed gates pass for cavalry, lancers, mounted archers, and commanders, including Hold and restart, reversed movement, wheels, Retreat, woodland, and narrow passages. All seven sustained-formation-pressure gates pass, covering connected ranks, support depth, bracing, heavy backing, solid terrain, automatic anchors, disconnected outliers, and fresh commands. Infantry, elephants, siege machines, and commanders do not gain the new mounted courtesy behavior.

## Visible movement and bounded drawing work

Substantial current-step lateral clearance selects the mounted sidestep pose and advances its phase from real travel distance. Small file corrections preserve the gallop. Attacks, impact reactions, Face, initial turns, Retreat, and Reduced Animations retain their existing priorities. A stale courtesy flag or a requested move without real displacement does not create a new pose.

All Canvas and animation/corpse gates pass, including the existing 2,130 cached action/facing clipping checks, bounded large-army drawing work, long-lived corpse caches, minimap behavior, siege artwork, and large worlds. These are instrumented drawing checks, not a visual browser inspection or measured frame rate.

## Isolated simulation timing

The final implementation rejects geometrically irrelevant neighbors before detailed readiness checks, skips redundant courtesy queries during nonparallel crossing plans, and returns early for zero goal offsets. Separate isolated trials preserve every unit's final position, health, and alive flag in the three shared benchmark scenarios. All six friendly-motion gates also retain identical physical metrics, including their impact events, after this cleanup.

The exact 2.3.0 baseline and final 2.3.1 source use the identical benchmark harness, presets, seeds, 30 simulated seconds, and 900 fixed steps per case. Seven fresh Node processes run sequentially after correctness testing, without a profiler or another simulation job. Python launches the single-case harness because Node child-process spawning is restricted in this environment.

| Scenario | Soldiers | 2.3.0 mean / p95 step (ms) | 2.3.1 mean / p95 step (ms) | Mean change |
| --- | ---: | ---: | ---: | ---: |
| charge | 2,534 | 3.814 / 8.088 | 6.471 / 11.921 | +69.7% |
| basin | 6,000 | 14.983 / 26.570 | 19.822 / 31.246 | +32.3% |
| epic | 12,000 | 26.689 / 45.332 | 34.609 / 51.624 | +29.7% |

The colossal **steppeEnvelopment** opening with **9,464 soldiers** measures **26.454 ms mean / 37.304 ms p95** per simulation step.

The new physical clearance adds simulation work relative to 2.3.0. The table shows the remaining cost after the redundant checks were removed; this release improves crowded mounted movement and does not claim a general CPU or frame-rate improvement over the previous release.

Timing includes natural movement, navigation, strategies, collision, combat, pressure, and morale; it excludes browser rendering and interface updates. Different movement changes when contact occurs and how the battle develops, so these are full-release workload comparisons, not an isolated cost measurement of the new helper. The figures describe this host and are not browser FPS promises.

## Offline artifact integrity

The standalone contains **781,891 bytes**, **182 unique DOM IDs**, all seven modules in the required order, exact embedded-source matching, and the current stylesheet. Both standalone copies are byte-identical. Every inline module passes JavaScript syntax checking; no external scripts or stylesheets remain.

Standalone SHA-256: `eab28aa160bd44fad8965e6e7e4e5e14fb60bb32fcae00fbf62eeaa5ee24fafe`.

The ZIP contains the runnable offline game, editable source, all regression harnesses, balance notes, this validation report, and example encounters. Packaging checks all **38 expected entries**, CRC integrity, and byte-for-byte agreement with the verified files. Internal bookkeeping and temporary tools are excluded.

| Simulation and interface source | SHA-256 |
| --- | --- |
| `src/data.js` | `cdcd4f06600286ca93e71987c4fd9fcad4ee07b311eba268aa71fcb0474c36fd` |
| `src/tactics.js` | `485049c6731e0606a1f31535ae5454af08c28fe2ef365eb21d80d12a409a18ad` |
| `src/impacts.js` | `9fc81f816869176be2a5d348a32b5d4145e53fc2ebb578dc654a94f253b9f254` |
| `src/siege.js` | `05fa961a54fcf97020d3c7d37538b49ee2d3492c3950be3053eecdfd7784d508` |
| `src/engine.js` | `ca3ee2996c8a1eaee065e814b0cf37991c49d89924795f08fcc4c8c33fdd3995` |
| `src/graphics.js` | `d4a2aebde791cd6fc6bc39f331431c8e7eda28e93d60760981ab89b9fe1900ea` |
| `src/app.js` | `06c9a75148e40d7e78f4a47a18cb17312d3b2c7d9ca061828593b208a598c131` |

## Reproduce the checks

Run from the extracted `IRONFIELD` directory with Python 3 and Node installed:

```sh
node tests/engine-test.cjs
node tests/command-ui-test.cjs
node tests/menu-world-ui-test.cjs
node tests/graphics-test.cjs
node tests/animation-corpses-test.cjs
node tests/command-scale-test.cjs
python3 build.py
```

The mounted checks can also run individually:

```sh
node tests/friendly-mounted-test.cjs
node tests/mounted-push-test.cjs
node tests/lancer-breakthrough-test.cjs
node tests/mounted-speed-test.cjs
node tests/formation-pressure-test.cjs
```

Run the performance harness separately from other tests and simulations:

```sh
node tests/performance-test.cjs . 30
```
