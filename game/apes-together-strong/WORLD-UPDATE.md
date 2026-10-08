# Living world, construction and horde navigation update

This continuation replaces the remaining vehicle/aircraft artwork, expands the settlement artwork, introduces real construction strips, and improves river/fortress movement and Q/R/T commands. The standalone `../apes-together-strong.html` embeds the code, music and all runtime artwork for offline play.

## What changes in play

- Spear towers, batteries and ballistas launch visible traveling shafts at 900, 1,000 and 1,150 world units respectively, beyond current infantry and tank gun ranges. Flight time reaches the full advertised range. Height-aware collision clears low palisades but respects tall cover, and friendly vehicles are excluded. Existing staffing and shared projectile budgets remain in effect.
- New settlement plots use stable irregular angles and wider gaps: ordinary huts have at least 104 units between centers, with more room around larger buildings. Existing saved buildings retain their positions.
- Completed housing sets the local population limit (six lodge places plus intact completed homes). Terrain capacity, food reserves and safety no longer silently pause births. Available adults, completed buildings and lodge upgrades increase family progress; nursery/rally and sanctuary bonuses still apply. Empty villages need residents, and the shared 1,000-ape cap remains. The village council and map show housing and growth status/rate.
- Ground vehicles use original illustrated chassis, eight facing directions, independently facing turrets, moving wheel highlights/suspension, exhaust/dust, headlights/brake lamps, damage states and persistent world wrecks. Every existing chassis and turret variant is covered. Helicopter bodies have independent animated main/tail rotors, banking, hovering, altitude shadows, startup/departure presentation, damage and crash artwork. Detection beams still use the existing light sources and budgets.
- Human barracks, supply buildings, repair garages, prison-site buildings and command centers have new artwork. Existing watchtowers, cages, connected walls, gates, relays and alarms retain the previous original art and state synchronization. The supplied helipad sprite is reusable library art; no fictitious landing facility was added to gameplay.
- Ape homes, developed lodges, workshops, stores, towers, batteries, ballistas, training/nursery/rally facilities, gardens, orchards and cooking areas use illustrated sprites. Actual settlement progression chooses lodge development. Residents keep existing simulation jobs and idle animations.
- Held equipment uses 41 original item sprites with separate rear/body/front drawing. All existing human roles, champion gear, crafted spears/torches/cuffs, log shields, carried food/wood and worker tools are mapped from real state. Scouts wear front/rear sash artwork. Seven firearm silhouettes replace baked weapons on 64 clean soldier body poses. Hand anchors follow the selected animation frame; crowded loadouts move retained gear to a free hand or back strap, and the body hides far-side equipment. This does not change combat, inventory or protection.
- **75 construction frames in 15 five-frame strips** show foundations/materials, frames, partial construction, finishing and completed structures. The simulation's stored stage chooses the frame; completed buildings use the final frame from the same strip. Paused construction does not visually finish. Mid-project saves restore the same artwork. Existing human buildings do not have a player construction mechanic.
- Generated crossings now include wood (104 world units), stone (128), military (152) and natural stepping-stone fords (112). The floor artwork is clipped to actual navigable bounds. These are roughly 3.7–5.4 of the navigation system's 28-unit cells before actor clearance. Three horde lanes fit the crossings. Vehicles use compatible major-road crossings. Natural gaps are decorative above a continuous shallow stone corridor.
- Apes commit to a bank approach, crossing and exit, then update their final goal. Moving the King cannot pull followers sideways out of an unfinished crossing. Formation slots remain on the King's riverbank. Distant followers use the same route connections and streamed corridors.
- Fortress target planning rejects proven unreachable targets, respects closed gates and melee obstruction, recognizes breaches, and preserves specialized climbing. A bounded shared fine-grid fallback handles narrow legal corridors between prison objects and walls.
- Nearest-target orders score at most six candidates using cached path length and river-crossing detours, so an apparently close enemy across a river does not automatically outrank a reachable same-bank target. Scoring adds no path searches or collision scans; incomplete knowledge retains straight-line estimates until a shared route resolves.

## Controls

| Gesture | Action |
|---|---|
| E tap | Directional charge against nearby humans, vehicles and hostile structures |
| E hold 280 ms | Directional charge against humans and vehicles, including active or parked tanks; ignore structures |
| Q | Call nearby wild, idle, and settled apes into your horde; preserve active field orders |
| R tap | Recall nearby recruited field apes |
| R hold 600 ms | Recall all recruited field apes |
| T tap | Recall all recruited field apes |
| T hold 600 ms | Mobilize all recruited apes, including settlement residents and guards |

One command fires per gesture, including touch. Hold progress, a pulse and affected counts distinguish the commands. T's former nearest-enemy action remains in the command menu. Settlement assignments are suspended and recorded on absolute recall; the same apes physically travel to the King. New direct orders replace old recall routes. See `HORDE-COMMANDS.md` for save migration and ownership details.

## Budgets and collision

The river graph is deterministic and cached before terrain chunks load. Regional route requests are coalesced; recall never runs a path search for each ape at dispatch. The existing limits remain at three searches and 192 expansions per simulation frame, with a 96-test rendering light budget. Fine-grid recovery shares those limits. Failed paths and stuck recovery use cooldowns. Opening a gate invalidates local route dependencies rather than rebuilding the world graph.

Gate, tree, wall and settlement geometry changes publish bounded regions. Distant
routes, shared groups and pending search progress survive a local change. Collision
cache keys include region versions; 128 change records and 4,096 region stamps
bound memory. Unlocated legacy changes or exhausted history use a safe full reset.
Streaming new chunks refreshes collision lookups while preserving route work.

Generation and save restoration validate reserved crossing approaches and remove conflicting natural trees/rocks. They do not erase military walls, gates or player structures. This is a procedural river connection graph plus bounded local navigation, not an unbounded flood fill of an infinite world; custom maps retain bounded A*. No universal guarantee is made for arbitrary user-authored enclosures.

Crossing art uses the existing ground cache. Vehicle rotor caches cap at 32 entries, material variants at 96, with four material builds/frame and bounded smoke/lamp draws. The visual modules do not alter actor positions, health, combat eligibility, detection or collision. Original PNG sources remain in the sprite library; lossless WebP runtime copies preserve all visible RGBA values and save 9,644,189 asset bytes (approximately 12.9 MB in the embedded HTML).

## Validation

- Full engine/art regression run after the held-E vehicle update: **483/483 passed**, with zero failures (93.48 seconds on the validation machine).
- Held-E checks cover active/parked tank damage, equal infantry/vehicle target ranking, kill retargeting, range/cone limits, legacy saves and excluded structures. The real keyboard hold and equivalent touch command both select nearby armor over farther infantry without targeting a closer cage.
- Real Chrome desktop and touch checks cover taps, holds, keyboard repeats, pause/blur/pointer cancellation, menus, loading, all quality presets, mobile DPR, reduced motion and missing-art fallback. The final built bundle decodes 28 runtime atlases.
- New navigation scenarios cover moving targets, saved crossing progress, 240-ape lane traffic, 120-ape recall with actual crowd separation, 1,000-ape coalesced recall, inaccessible targets, destroyed gates, live-wall melee rejection, narrow prison passages and order/save precedence.
- A commissioned tower was advanced by its existing worker system through stages 0–4; a stage-2 save restored stage/progress and the same source frame, then completion used frame 4. All 75 authored stage rectangles were rendered in Chrome and visually inspected.
- Vehicle browser checks render 28 ground vehicles and four helicopters together, all eight directions and every class. Stationary rotor captures have over 23,000 changing color channels, with capped rotor/material caches and no browser errors. Source crops were inspected and corrected.
- Equipment checks cover all 22 human roles and 24 champion identities, all eight facings, animated hand anchors, front/rear draw order, legal mixed loadouts and thrown-item release timing. Chrome rendered 64 pose samples and a live 48-ape/22-human equipment scene for 40 frames. The scene produced 7,203 item draws without changing actor state; a direct probe confirmed exactly three expected item draws, with no legacy duplicates. Missing held-art sheets use the armed fallback renderer.
- Existing infantry, rescue/prison, settlement, combat, climbing, progression and large-population work-budget tests pass.

### Same-machine Canvas comparison

Headless Chrome, 1440×900, High, 180 measured simulation/render frames per scene, same deterministic scenarios and prior visual-release source. Times are milliseconds per frame; they are measurements, not hardware-independent promises.

| Scenario | Previous sim | Updated sim | Previous render | Updated render | Updated work p95 |
|---|---:|---:|---:|---:|---:|
| A: 100 followers | 3.56 | 3.10 | 8.72 | 6.39 | 12.90 |
| D: 200 apes / 150 humans / vehicles | 12.80 | 9.07 | 22.31 | 17.93 | 34.30 |
| G: 500 followers | 12.06 | 10.64 | 12.34 | 10.67 | 35.70 |
| K: 750 apes / 320 humans / combined arms | 26.99 | 22.69 | 36.04 | 34.50 | 73.30 |

All measured populations and navigation/light/effect cache ceilings were retained. Typical render cost did not increase in this run. The extreme battle remains below 30 FPS on this machine, and the 500-ape scene also exceeds a 60 FPS budget. Desktop/mobile input correctness is tested; performance on every mobile device is not claimed. The 1,000-ape absolute recall dispatch measured 6–12 ms with one pulse and zero synchronous searches.

The final measurement includes held equipment and local geometry invalidation.
Raw before/after reports are in `benchmarks/world-update-2026-10-08.json`.

## Reproduce and package

```sh
node game/apes-together-strong/build.cjs
node game/apes-together-strong/build.cjs --check
node --test game/apes-together-strong/tests/*.test.cjs
node game/apes-together-strong/tests/horde-commands-browser.cjs
node game/apes-together-strong/tests/visual-overhaul-browser.cjs
node game/apes-together-strong/tests/vehicle-art-browser.cjs
node game/apes-together-strong/tests/structure-art-browser.cjs
node game/apes-together-strong/tests/held-item-browser.cjs
node game/apes-together-strong/tests/world-art-browser.cjs
node game/apes-together-strong/tests/visual-overhaul-performance.cjs --scenario ADGK --frames 180
node game/apes-together-strong/package-visuals.cjs <output-directory>
node game/apes-together-strong/tests/visual-overhaul-package.cjs <output-directory>
```

Browser tests need Playwright and Chrome (`CHROMIUM_PATH` can select an installed browser). Ordinary builds and packaging use only Node built-ins. Asset crop regeneration uses `scripts/construction-manifest.cjs` or `scripts/build-vehicle-manifest.py`; run `scripts/optimize-world-art.cjs` afterward to recreate lossless runtime files (optional Sharp dependency). The built-in imagegen prompts and original transparent PNGs are included under `assets/visual/settlements` and `assets/visual/vehicles`.

Save schema and existing object IDs remain compatible. Browser storage belongs to the old game's origin: use its Export save and the new copy's Import save to transfer a campaign. New crossing geometry applies deterministically to old worlds too; approach cleanup preserves constructed defenses and settlement buildings.
