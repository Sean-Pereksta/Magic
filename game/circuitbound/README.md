# Circuitbound

A playable engineering survival sandbox: **explore → mine → craft → build → generate power → automate → defend → explore deeper**. The 220 × 84 tile world has persistent construction, real item inventories, five campaign chapters, nine enemy types and continued sandbox play after the Reactor Guardian.

## Play

Open `game/circuitbound.html` in a modern browser, or serve the repository and visit `/game/circuitbound.html`. No backend, account or mandatory server is required. Saves use browser local storage; opening the game from another origin uses another save location.

A new expedition starts beside the outpost. Follow the pinned objective to mine and collect 6 wood and 4 stone. Open **Craft**, make a **Field Workbench**, select it in the hotbar, place it at the highlighted spot and use it. Opening Craft within reach of the Workbench also gives access to its recipes. The Journal tracks the campaign as you explore the nearby copper and iron.

The next pinned task marks the two outpost repair tiles. Bridge the missing wire at **(9, 27)** and place the field-kit generator at **(11, 28)** behind the motor. The Core and motor use separate branches so the tutorial stays within wire capacity.

The chapters are **Wake the Foundry → Into the Depths → Under Siege → The Industrial Network → The Reactor Below**. Progress rewards actual machine operation, a descent route, automated extraction, defense kills, raid survival, manufacturing, stronger generation and defeating the boss. Equipment unlocks and material costs connect these goals to exploration. Casual, Standard and Hardcore adjust damage, populations, raid timing and recoverable resource loss.

## Controls

| Action | Desktop control |
| --- | --- |
| Walk / jump | A/D or Left/Right; Space |
| Climb / descend / drop through platforms | W/S or Up/Down; Space jumps off a ladder or rope |
| Select hotbar slot | 1 through 0, or click a slot |
| Mine | Select a pickaxe and hold the left mouse button on a reachable tile |
| Place | Select a block or component and left-click. Drag compatible pieces and release to build the valid previewed run |
| Use machine, door or switch | Right-click the target. Machine controls stay on the selected machine |
| Rotate held / aimed component | R / Shift+R |
| Switch exact / smart cursor | G |
| Craft / backpack | E / I |
| Journal / Blueprints | J / B |
| Field manual | H |
| Engineer's Lens / cycle lens view | C / V |
| Link Tool / cycle link mode | 0 selects Link Tool; L cycles New, Add, Remove, Inspect |
| Use or collect / insert materials | F collects contents from aimed storage or production equipment and uses other components. Shift+F inserts one selected item |
| Drop held item | X |
| Attack / cycle weapon / heal | Left-click with a weapon selected, or Q. Tab cycles weapons and Z heals |
| Close panel / pause | Escape |

On touch, tap the world to aim. Hold **Mine** or tap **Place**, **Attack**, or **Use**. Drag in the world to preview a run of compatible parts, then tap **Place** to commit it. With the Link Tool held, aim and tap **Link** at each corner of the selection. **Turn** rotates the current component and **Aim** switches cursor behavior. Movement and jump remain on the screen edges.

The ten hotbar slots retain their assignments when you collect items. Choose a carried item in **Pack** and use **Assign to hotbar** to move it to a slot. Open **Menu** for the Journal, Blueprints, Field Manual and Engineer’s Lens.

## Crafting and machine controls

Crafting includes hand recipes, Field Workbench recipes and powered machine batches. Search by output, ingredient or use. Turn on **Show all recipes** to read a blocked recipe’s materials, station and chapter requirements. Selecting an ingredient explains where to find it or how to make it. **Craft one** and **Craft max** use your backpack materials and report the finished quantity.

Use item machinery to open its controls. Select the desired recipe directly and choose **Load**, **Collect finished goods** or **Recover inputs**. Storage has **Take contents**. The drawer shows the machine’s operating state, next action and port connections, with detailed grid and drive readings expandable.

Hold-to-mine shows progress and cracks. Broken blocks become physical resources, collected nearby when reachable and inventory space is available. Mining a container recovers its block, ingredients and finished output. Inventory holds 2,048 items; excess goods remain recoverable.

## Ports and power

Ports rotate with the component. **0 faces right, 1 down, 2 left and 3 up.** The Lens shows yellow power, cyan rotation, red signal, green items and purple structural connections. A placement preview shows matching nearby ports and direction. Smart rotation chooses an orientation when compatibility is unambiguous. R keeps your chosen direction.

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

Coordinates below are offsets in cleared terrain. Rotations follow the numeric convention above. Blueprint previews show component counts, missing materials and placement conflicts. **Start guided build** walks through the automatic miner, sensor-operated door, winch lift and counterweighted lift one component at a time. Pause a guide and resume it from the saved site. The guide and full-assembly preview both use the actual parts in your hotbar or backpack.

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

## Industrial expansion

Recover technology plans from deep vaults and meet each recipe’s chapter requirement. Search Crafting for an ore, device, or part and enable **Show all recipes** to check the exact ingredients and station. Use a Field Workbench for assembly and powered machinery for refining and Foundry batches.

| Ore | Where to look | Extraction |
| --- | --- | --- |
| Cobalt | Lower Iron Caverns and Crystal Hollows | Steel Drill Tool or a higher-tier mining system |
| Iridium | Forgotten Industrial Depths and nearby veins | Charged Powered Mining Tool, Ion Bore, or Plasma Excavator |
| Aetherium | Reactor Abyss | Plasma Excavator |

| Device | Role |
| --- | --- |
| Induction Smelter | Draws 11 power and turns rare ores into ingots |
| Robotics Foundry | Draws 14 power and produces precision bearings, frames, servo cores, flux cells, survey lenses, diamond drill heads, superalloy, phase lenses, quantum controllers, ammunition, and Flight Cells |
| Ion Bore | Draws 10 power and 15 torque to mine a fourteen-tile line. Place an ore receiver at its output |
| Plasma Excavator | Draws 18 power and 25 torque to mine up to five lanes, including aetherium |
| High-Torque Servo Motor and Servo Shaft | The motor turns 10 power into 32 torque. The shaft carries rotation to heavy machinery |
| Compact Reactor Generator and Reactor Battery | The generator supplies 42 power. The battery buffers up to 480 energy |
| Industrial Power Bus, Superconducting Cable, and Fiber Signal Line | Carry up to 45 power, up to 85 power, or control signals, respectively |
| Cargo Silo and Alloy Rack | Provide 240-item and 96-item logistics buffers |

Use Cobalt Plate, Iridium Shell, Aetherium Panel, Tempered Glass, Refractory Brick, Honeycomb Frame, Truss Beam, and Armored Bulkhead for walls and machinery frames. Use Armored Platform, Gantry Deck, Flight Deck, Hangar Grate, Armored Scaffold, and Service Ladder around hangars and factories. Magnetic Guide Rail, Anchor Pylon, and Ballast Core support working lifts and gantries. Link the ballast separately from the lift deck.

### Drone stations

Build a Drone Chassis and its role kit at the Workbench. Power the matching dock, leave open air beside it, then use **Deploy** in the dock controls. The inspector shows the roster, charge, cargo, and next action. A powered Drone Beacon marks a work area and a Drone Charger supports longer routes.

| Drone | Dock | Working storage |
| --- | --- | --- |
| Collection | Reclaimer Dock, up to 3 drones | Salvage Collection Bin, 128 items |
| Mining | Mining Drone Dock, up to 2 drones | Ore Collection Bin, 128 items |
| Defense | Defense Drone Dock, up to 2 drones | Defense Supply Bin, 96 turret rounds |

Collection drones retrieve loose goods and accessible machine outputs. Mining drones cut exposed ordinary ore and return the real drops. Both roles can use a 160-item Drone Cargo Depot and pause when compatible storage fills or becomes unreachable. Defense drones draw ammunition from their dock and engage hostile machines in sight. Use **Take contents** on a collection bin or connect its output to factory logistics. To dismantle a dock, recall its drones, wait for them to return, and pack them from the dock controls.

### Schematic weapons

| Weapon | Combat role |
| --- | --- |
| Phase Blade | A short sweep restores energy for each enemy hit |
| Coil Lance | A bolt pierces several enemies and loses damage with each hit |
| Void Pulser | An orb bursts on impact or at full range and stuns exposed targets |
| Siege Railcannon | A heavy instant shot strikes the first exposed target and spends one Siege Slug |
| Sunspike Laser | Stand still to prime a sweeping beam. Firing spends one Sunspike Charge and 28 energy. Movement interrupts the beam and cooldown carries through saves |

### Starship flight

Recover the Starflight plan. Use Foundry components and a Workbench to assemble the hull, flight controller, two plasma thrusters, flux tank, and complete Starship. Connect 12 power to a Starship Pad and clear a two-tile-wide approach above it. Bring a Flight Cell and choose **Launch starship**. Use movement controls to steer and climb, and hold Down to land. Use the landed Starship to board or disembark, and press U or use **Refuel** beside it to load a Flight Cell. A ship that runs out of charge glides down. Pack a parked ship into your backpack or recall it from a powered pad for one Flight Cell.

## Survival

Surface Scrapyards lead into Iron Caverns, Crystal Hollows, Forgotten Industrial Depths and the Reactor Abyss. Veins, caves, abandoned storage, ruined lifts and a reactor chamber reward exploration. Ladders, supported ropes, rope ladders, scaffolds, bridges, one-way platforms, anchors and rails support a return route. Drag to extend compatible structures.

Scrap Beetles, Rust Crawlers, Spark Wisps, Tunnel Burrowers, Salvage Drones, Circuit Leeches, Siege Automatons, Core Sentinels and the Reactor Guardian have distinct movement, attacks and loot. Spawning considers depth, progression, day/night, difficulty and machinery, with outpost grace and bounded populations. Raids warn before attacking. Attacks have wind-ups/cooldowns; terrain blocks movement and projectiles. Leeches add actual load to their target power grid.

Craft melee/ranged/energy weapons, armor and shields. Turrets consume power and rounds; arc towers/Tesla coils use electricity; traps need rotation. Powered recovery stations heal nearby players. Establish checkpoints near live machinery. Death retains equipment and places a difficulty-dependent fraction of common resources into a marked recovery cache; Casual retains those resources too. Salvage remains recoverable when the loose-item budget is full.

## Saves

Schema **version 5** retains `circuitbound-save-v1`. The last valid prior save is kept under `circuitbound-save-v1-backup` and loads as a fallback. World tiles, inventories, production buffers, battery energy, signal memory, structures, loose cargo, survival state, checkpoints, recovery caches, campaign progress and settings persist alongside hotbar assignments, guided builds, rare geology, recovered schematics, commissioned drones, and the deployed Starship’s fuel and location.

Existing tile IDs stay stable. The Field Workbench uses **100**. Old linked flags migrate into connected assemblies and expeditions from before version 4 receive a Workbench in their backpack. Existing expeditions receive rare geology and vaults on eligible terrain and keep placed structures and machines.

## Art

`art.js` loads painted sprite sheets and biome scenery from the game directory. It packs the sprites into in-memory atlases and caches scaled scenery. Machines share their painted icons with crafting, blueprints and the hotbar. Active machines and moving structures have animated surfaces while the simulation retains tile-aligned placement and collision.

## Validate

From the repository root:

```sh
node --test game/circuitbound/tests/*.test.cjs
node game/circuitbound/tests/interface-browser.cjs
node game/circuitbound/tests/gameplay-browser.cjs
node game/circuitbound/tests/performance.cjs
```

Unit and integration tests use Node's built-in runner and the game harness. Browser checks use Playwright and an installed Chromium, Chrome or Edge. Use `CIRCUITBOUND_PLAYWRIGHT` and `CIRCUITBOUND_BROWSER` to specify package and browser paths. Interface checks cover desktop and mobile portrait and landscape layouts, restored movement after closing dialogs, the fixed hotbar, Backpack, recipe search, blueprint previews, guided plans and settings.

The gameplay browser check first uses an unchanged, reproducibly seeded generated world: actual keyboard movement/jumping, ladder climbing, resource-debited platforms, mouse hold-mining of eight generated copper ore, useful equipment crafting and repair of the actual outpost Core/motor. It then explicitly creates a **terrain fixture**: a connected cleared quarry, solid ground and 92 real copper/iron ore tiles. This later phase is a controlled regression scenario, not a claim that the whole sequence was navigated through untouched random terrain.

The fixture grants **no inventory, campaign statistics, machine power, ammunition or enemy kills**. The same expedition must mine its deposits, craft and place every component, supply actual power/torque and move real items. It verifies full transport/storage stops the drill before terrain is consumed, draining restarts work, two deposits yield twelve actual automated ore, and a crusher/smelter line converts sixteen real ore into sixteen ingots. Four ingots craft the ammunition fabricator and the other twelve produce forty-eight rounds; with eight earned chapter rounds, all fifty-six remain accounted for as buffered, stored or fired ammunition. The ordinary raid scheduler warns and spawns enemies; actual attacks, turret ammunition consumption and repairs of raid-damaged machinery complete raid survival and unlock the Industrial Network.

A separate genuine mobile/touch browser context resumes the earned normal-world save and performs held movement, hold-mining/collection, up/down climbing, resource-debited placement, weapon cycling and healing-button conservation at full health. The final smoke check opens `game/circuitbound.html` directly through `file://`, with no server, routing, test API or injected state. Success is reported only after this unmodified startup passes. Browser screenshots are written outside the repository under `../work/circuitbound-gameplay` and `../work/circuitbound-browser`, or the `CIRCUITBOUND_SCREENSHOTS` directory when supplied.

The optional performance script checks 500 component nodes over 1,200 ticks, no unchanged-topology rebuilds, conserved cargo, 46 real enemy AIs and spatial queries against brute force. Printed timings describe the execution host, not a browser frame-rate guarantee.

## Deliberate limits

- Engineering advances in fixed 50 ms steps, indexing active components and rebuilding dirty topology after edits. Survival uses a spatial enemy index and population caps of 22/34/46. The world is finite; there is no chunk streaming.
- Mechanical ratios conserve speed × torque and divide torque between branches. Weight and approximate stress can stall or detach weak links without deleting materials. Bearings/hinges move in collision-checked grid quarter-turns, with at most 128 blocks per rotating assembly. This is not a continuous rigid-body, structural-gravity or beam solver.
- Ropes are supported climbable construction. Winches translate real linked loads; counterweights move reciprocally, and adjacent guide rails constrain the next grid position. This models balancing and clearance in grid steps, without continuous rope sag, pulley tension or free-swinging cable physics. Counterweights and rails still require the existing powered mechanical drive.
- Electrical behavior models supply, load, cable capacity and conserved battery energy. Resistance/voltage simulation, transformers, heat, fluids, steam, hydraulics, vehicles and numeric robotics remain outside this implementation.
