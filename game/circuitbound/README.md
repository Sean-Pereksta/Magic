# Circuitbound

A playable engineering survival sandbox: **explore → mine → craft → build → generate power → automate → defend → explore deeper**. The 220 × 84 tile world has persistent construction, real item inventories, five campaign chapters, nine enemy types and continued sandbox play after the Reactor Guardian.

## Play

Open `game/circuitbound.html` in a modern browser, or serve the repository and visit `/game/circuitbound.html`. No backend, account or mandatory server is required. Saves use browser local storage; opening the game from another origin uses another save location.

A new expedition starts at an outpost with a lit generator circuit, an incomplete motor connection, storage, a Foundry Core and a ladder into accessible copper and iron. Mine eight copper, fabricate a reinforced pickaxe or engineering hammer, operate a powered machine and restore the Core. The Journal tracks the current objective. Existing saves retain their terrain.

To repair this outpost, bridge the missing wire at **(9, 27)** and place the field-kit generator at **(11, 28)**, behind the motor. The Core and motor use separate branches so the tutorial stays within wire capacity.

The chapters are **Wake the Foundry → Into the Depths → Under Siege → The Industrial Network → The Reactor Below**. Progress rewards actual machine operation, a descent route, automated extraction, defense kills, raid survival, manufacturing, stronger generation and defeating the boss. Equipment unlocks and material costs connect these goals to exploration. Casual, Standard and Hardcore adjust damage, populations, raid timing and recoverable resource loss.

## Controls

| Action | Desktop control |
| --- | --- |
| Walk / jump | A/D or Left/Right; Space |
| Climb / descend / drop through platforms | W/S or Up/Down; Space jumps off a ladder or rope |
| Mine | Mine mode; hold left mouse on a reachable block |
| Build | Build mode; click or drag ladders, supported ropes and platforms |
| Interact / inspect / combat | Select that mode, then left mouse |
| Quick placement or configuration | Right mouse; machine configuration cycles its recipe |
| Rotate held / aimed component | R / Shift+R |
| Fabricator / Field Manual | E / H |
| Journal / Blueprints | J / B |
| Engineer's Lens / cycle lens view | C / V |
| Link Tool / cycle link mode | 0 selects Link Tool; L cycles New, Add, Remove, Inspect |
| Withdraw / insert materials | F takes contents from aimed machinery; Shift+F inserts one selected item |
| Drop held item | X |
| Attack / cycle weapon / heal | Q / Tab / Z |
| Pause / close dialog | Escape |

On-screen modes, movement and context actions support touch. Aim in the world first, then hold Mine or use the selected action. Fight mode adds compact weapon-cycle and healing buttons to the top bar. Dialogs pause gameplay and support keyboard focus navigation. The hotbar scrolls through components and materials.

Interact with item machinery to open its inspector. **Load material**, **Next recipe**, **Collect finished goods** and **Recover inputs** buttons support production without keyboard modifiers; ordinary storage has a **Take contents** button. These actions obey reach, input capacity and player inventory capacity.

Hold-to-mine shows progress and cracks. Broken blocks become physical resources, collected nearby when reachable and inventory space is available. Mining a container recovers its block, ingredients and finished output. Inventory holds 2,048 items; excess goods remain recoverable.

## Ports and power

Ports rotate with the component: **0 right, 1 down, 2 left, 3 up**. The Lens shows yellow power, cyan rotation, red signal, green item and purple structural connections, with directional arrows. Inspect machines for power, torque, recipe, ingredients, outputs, progress, efficiency and specific faults. Smart placement chooses an orientation only when compatibility is unambiguous; manual rotation remains available.

| Component facing right | Rear / left | Top | Front / right | Bottom |
| --- | --- | --- | --- | --- |
| Motor | Power in | Optional enable | Rotation out | — |
| Shafts, gears, pulley | Rotation in | — | Rotation out | — |
| Clutch | Rotation in | Required enable | Rotation out | — |
| Bevel | Rotation in | — | — | Rotation out |
| Piston, winch, bearing, hinge | Rotation in | Control | Structural action / attachment | — |
| Conveyor | Rotation + item in | Item in | Rotation + item out | Rotation in |
| Hopper | Item in | Item in | Item out | — |
| Storage / filter | Item in | — | Item out | — |
| Splitter | Item in | — | First output | Second output |
| Drill / excavator | Rotation in | Power or optional enable | Mining direction | Item out; alternate power in |
| Crusher | Item in | Rotation or optional enable | Finished output | Power in |
| Other production machines | Item in | Optional enable | Finished output | Power in |
| Turret | Ammunition or power in | Optional enable | Field of fire | Alternate power in |
| Arc / Tesla / recovery station | Power in | Optional enable | — | Alternate power in |
| Automated door | Power in | Open command | — | Alternate power in |
| Player / enemy sensor | — | — | Signal out | — |

Shared port sides are alternative connection choices because a neighboring tile holds one block. For example, feed turret rounds from the rear and power from below; feed crusher ore from the rear, rotation from above and power from below.

Unconnected optional enables default on for motors and production machinery; an attached low signal stops them. Clutches require high signals. Doors require power and a high open command, waiting for an occupied doorway to clear before closing. A winch's connected control selects raising when high and lowering when low; local configuration also supports direction and pause. Hinges select 0°/90° through their input.

Generators supply 12 power. Motors use 5, lamps 1 and a Foundry Core 6. Copper wire carries 10, copper blocks 20 and iron blocks 30; the weakest cable capacity limits a connected grid. Overload or insufficient supply stops consumers.

Batteries hold **120 energy**, charge at up to **4/second** from spare generation and discharge at up to **8 power**. Charging counts as real load and respects cable capacity. Stored energy decreases by discharge power × elapsed seconds. Brownouts cannot spend energy without powering the simulation interval, and zero-time inspection neither spends nor creates energy.

## Production and logistics

Manual mining progresses through the field tool, reinforced pickaxe, steel drill tool and energy-consuming powered mining tool. The industrial drill needs **2 power + 4 torque**, and the excavator **6 power + 10 torque**, plus a real receiver with space. Drills search six tiles forward; excavators search a three-wide, ten-tile corridor. Protected construction blocks each affected ray. Resources appear only after an actual terrain tile is removed.

Machines separate ingredients from finished goods. Recipe changes reset progress. Ingredients are deducted only when a complete batch fits; missing inputs, power, rotation or output space pauses work. Disconnected producers retain their output. Hoppers and belts may intentionally unload physical goods onto the ground.

| Machine | Input → output | Power | Drive | Seconds / batch |
| --- | --- | ---: | --- | ---: |
| Crusher | 1 iron/copper → 1 matching crushed ore | 2 | 3 torque | 1.5 |
| Smelter | 1 crushed ore → 1 matching ingot; alternatively 2 raw ore → 1 ingot | 4 | — | 2 |
| Refiner | 2 iron ingots + 1 stone → 1 steel | 5 | — | 3 |
| Assembler | 2 copper ingots + 1 crystal → 1 circuit; 2 iron ingots → 1 gearbox; 1 copper ingot → 6 wire | 3 | — | 2.5 |
| Ammunition fabricator | 1 iron ingot → 4 rounds | 3 | — | 1.5 |
| Advanced fabricator | 2 steel + 1 circuit → 1 advanced component | 6 | — | 4 |
| Repair station | 2 iron + 1 crystal → 1 repair kit | 2 | — | 3 |

The assembler's `gear` output is the existing gearbox item. Crushing then smelting needs one raw ore per ingot; hand forging costs three ore and one stone. Advanced components feed excavators, Tesla coils and late equipment. Repair dressings and manufactured kits both heal 45 health.

Input buffers hold 24 items and output buffers 16; turrets hold 48 rounds, storage 64, ordinary transport 8. Transfers occur every 0.25 seconds and each item crosses at most one component per tick. Storage exports only into a facing hopper. Splitters wait if their alternating selected branch is blocked; filters reject unmatched goods. Full buffers stop upstream work without deletion and resume when space returns. Loose stacks are capped at 512; nearby matching stacks merge.

## Working layouts

Coordinates below are offsets in cleared terrain; rotations follow the numeric convention above. Every component must come from inventory. Blueprint previews show component counts, missing materials and placement conflicts.

**Drill → hopper → driven conveyor → storage:**

| Component | (x, y) | Rotation |
| --- | --- | ---: |
| Generator | (8, 2) | 0 |
| Motor | (8, 3) | 1 |
| Drill | (8, 4) | 1 |
| Generator for drill | (9, 4) | 0 |
| Hopper | (7, 4) | 2 |
| Conveyor | (6, 4) | 2 |
| Generator for conveyor motor | (6, 2) | 0 |
| Motor for conveyor | (6, 3) | 1 |
| Storage | (5, 4) | 2 |

The drill mines downward from (8, 5) and exports left. Fill storage and transport buffers to verify a safe stop; remove goods to resume. The smaller in-game miner blueprint omits the conveyor and its drive.

**Ore → ammunition → turret:** generator (3, 1), downward motor (3, 2), right-facing crusher (3, 3), input hopper (2, 3), smelter (4, 3), ammunition fabricator (5, 3), turret (6, 3). Select crushed iron on the crusher and iron ingots on the smelter. Below, connect generator (3, 4), copper blocks (4, 4), (5, 4), (6, 4), and generator (7, 4). Feed real iron into the hopper. Shared production/defense load is 12: ordinary 10-capacity wire would overload, while copper blocks work. The top crusher motor has its own generator.

**Sensor defense:** turret (3, 2), generator (3, 3), ammunition hopper (2, 2), downward enemy sensor (3, 0), signal wire (3, 1). Load rounds into the hopper. A detection signal enables the turret; no power, no ammunition or blocked sight prevents firing. The sensor-door blueprint uses a player sensor and automated door on the same vertical signal path.

**Winch lift:** generator (3, 0), downward motor (3, 1), downward winch (3, 2), left-facing switch (4, 2), and a separately linked deck at (2, 5), (3, 5), (4, 5). Link only the deck. Toggle the switch to raise/lower, clear the shaft and provide a ladder while constructing it. Load and torque affect movement; collision checks protect riders.

**Counterweighted, rail-guided lift:** extend that lift with a counterweight block at (7, 5), linked as its own assembly, and guide rails at (1, 3) through (1, 12). A counterweight costs three iron and three stone and has mass 8. The nearby winch automatically attaches a separate weight within 12 horizontal and 50 downward tiles; another winch cannot borrow an attached weight. Deck and weight move in opposite directions in one checked operation. Net mass, friction, cargo and rider weight determine the torque required; balancing helps lift a heavier load without creating power. Rails beside the deck constrain its route, and the lift stops at a rail end. An obstacle on either side or inadequate rider headroom stops both assemblies without losing cargo.

The Blueprint panel also includes a crusher/smelter/storage line. Plans are ordinary connected components and obey inventory, power, reach and collision rules.

## Survival

Surface Scrapyards lead into Iron Caverns, Crystal Hollows, Forgotten Industrial Depths and the Reactor Abyss. Veins, caves, abandoned storage, ruined lifts and a reactor chamber reward exploration. Ladders, supported ropes, rope ladders, scaffolds, bridges, one-way platforms, anchors and rails support a return route. Drag to extend compatible structures.

Scrap Beetles, Rust Crawlers, Spark Wisps, Tunnel Burrowers, Salvage Drones, Circuit Leeches, Siege Automatons, Core Sentinels and the Reactor Guardian have distinct movement, attacks and loot. Spawning considers depth, progression, day/night, difficulty and machinery, with outpost grace and bounded populations. Raids warn before attacking. Attacks have wind-ups/cooldowns; terrain blocks movement and projectiles. Leeches add actual load to their target power grid.

Craft melee/ranged/energy weapons, armor and shields. Turrets consume power and rounds; arc towers/Tesla coils use electricity; traps need rotation. Powered recovery stations heal nearby players. Establish checkpoints near live machinery. Death retains equipment and places a difficulty-dependent fraction of common resources into a marked recovery cache; Casual retains those resources too. Salvage remains recoverable when the loose-item budget is full.

## Saves

Schema **version 3** retains `circuitbound-save-v1`. The last valid prior save is kept under `circuitbound-save-v1-backup`; loading falls back to it when the main save is invalid. World tiles, inventories, production buffers/progress, battery energy, signal memory, structures, loose cargo, survival state, checkpoints, recovery caches, progression and settings persist.

Original IDs stay stable: **door 52, ladder 53, ledge 54**; added engineering tiles begin at 55. Old linked flags migrate into connected assemblies. Existing worlds preserve terrain and inventory; generating a new world resets engineering and survival state.

## Validate

From the repository root:

```sh
node --test game/circuitbound/tests/*.test.cjs
node game/circuitbound/tests/interface-browser.cjs
node game/circuitbound/tests/gameplay-browser.cjs
node game/circuitbound/tests/performance.cjs
```

Unit/integration tests use Node's built-in runner and the actual catalog/application harness. Browser checks use Playwright and an installed Chromium, Chrome or Edge; `CIRCUITBOUND_PLAYWRIGHT` and `CIRCUITBOUND_BROWSER` can specify package/browser paths. Interface checks use a temporary local server with browser routing for restricted environments; the gameplay check routes repository files directly. Interface checks cover desktop and mobile portrait/landscape layouts, closing every dialog, restored movement, crafting, previews and settings.

The gameplay browser check first uses an unchanged, reproducibly seeded generated world: actual keyboard movement/jumping, ladder climbing, resource-debited platforms, mouse hold-mining of eight generated copper ore, useful equipment crafting and repair of the actual outpost Core/motor. It then explicitly creates a **terrain fixture**: a connected cleared quarry, solid ground and 92 real copper/iron ore tiles. This later phase is a controlled regression scenario, not a claim that the whole sequence was navigated through untouched random terrain.

The fixture grants **no inventory, campaign statistics, machine power, ammunition or enemy kills**. The same expedition must mine its deposits, craft and place every component, supply actual power/torque and move real items. It verifies full transport/storage stops the drill before terrain is consumed, draining restarts work, two deposits yield twelve actual automated ore, and a crusher/smelter line converts sixteen real ore into sixteen ingots. Four ingots craft the ammunition fabricator and the other twelve produce forty-eight rounds; with eight earned chapter rounds, all fifty-six remain accounted for as buffered, stored or fired ammunition. The ordinary raid scheduler warns and spawns enemies; actual attacks, turret ammunition consumption and repairs of raid-damaged machinery complete raid survival and unlock the Industrial Network.

A separate genuine mobile/touch browser context resumes the earned normal-world save and performs held movement, hold-mining/collection, up/down climbing, resource-debited placement, weapon cycling and healing-button conservation at full health. The final smoke check opens `game/circuitbound.html` directly through `file://`, with no server, routing, test API or injected state. Success is reported only after this unmodified startup passes. Browser screenshots are written outside the repository under `../work/circuitbound-gameplay` and `../work/circuitbound-browser`, or the `CIRCUITBOUND_SCREENSHOTS` directory when supplied.

The optional performance script checks 500 component nodes over 1,200 ticks, no unchanged-topology rebuilds, conserved cargo, 46 real enemy AIs and spatial queries against brute force. Printed timings describe the execution host, not a browser frame-rate guarantee.

## Deliberate limits

- Engineering advances in fixed 50 ms steps, indexing active components and rebuilding dirty topology after edits. Survival uses a spatial enemy index and population caps of 22/34/46. The world is finite; there is no chunk streaming.
- Mechanical ratios conserve speed × torque and divide torque between branches. Weight and approximate stress can stall or detach weak links without deleting materials. Bearings/hinges move in collision-checked grid quarter-turns, with at most 128 blocks per rotating assembly. This is not a continuous rigid-body, structural-gravity or beam solver.
- Ropes are supported climbable construction. Winches translate real linked loads; counterweights move reciprocally, and adjacent guide rails constrain the next grid position. This models balancing and clearance in grid steps, without continuous rope sag, pulley tension or free-swinging cable physics. Counterweights and rails still require the existing powered mechanical drive.
- Electrical behavior models supply, load, cable capacity and conserved battery energy. Resistance/voltage simulation, transformers, heat, fluids, steam, hydraulics, vehicles and numeric robotics remain outside this implementation.
