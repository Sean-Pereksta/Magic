# TurnCraft presentation checks

The game remains `../turncraft.html`. `polish.css` is loaded after the legacy styles. No build step or new runtime dependency is required.

## Controls

- Drag to pan; wheel/pinch or Zoom buttons to zoom. WASD/arrows pan; F focuses selection.
- Touch taps select friendly troops. With troops selected, tap open ground to move or an enemy to attack. Gestures never issue orders.
- Hold still for 380 ms, then drag a box to select friendly troops with mouse or touch. Shift-drag selects immediately; double-click/tap selects the visible unit type. Moving immediately still pans, and a second finger switches safely to pinch zoom.
- Ability buttons display Q/E/R/T and subsequent shortcuts, including on touch layouts. The same keys activate ready abilities or enter targeting; Escape cancels. Typing, modifier keys, pause, cooldowns and open dialogs suppress ability shortcuts.
- A arms Attack (X remains available). Click an enemy to attack it, or click ground to attack the nearest visible valid enemy within four tiles; empty ground issues attack-move. Escape/Cancel Order cancels targeting. The numbered control groups retain tap/hold behavior.
- Mine uses existing automatic gathering on arrival. Stop suppresses gathering until a new mining/movement order.
- Build lists structures and lets adjacent human workers assist an unfinished building. Choose Refinery, Assimilator or Extractor and click a visible unoccupied gas vent at any distance: the worker travels to a reachable adjacent spot and constructs automatically. The cost is checked when ordering and charged once when construction begins. Replacement orders, unavailable vents, blocked routes and lost affordability cancel without a charge. Pending orders survive shared snapshots; Brood consumes its Drone only when construction begins. Ordinary buildings still require adjacent placement.

## Match setup

The faction cards use three original illustrated WebP assets in `assets/factions/`; their generation prompts and provenance are documented there. They load independently of optional battlefield sprites. The menu offers Easy, Medium, Hard and Pro buttons tied to the existing AI simulation tuning and saved preference. Solo opponent selection now creates the requested one or two opponents. Faction and difficulty controls keep keyboard focus while setup status updates.

Choose from Classic Frontier and five new battlefields. Each new map has authored routes, obstacles, starting locations, six expansion sites, conquest zones, a relic location, and its own four-material terrain atlas:

| Battlefield | Terrain | Layout |
| --- | --- | --- |
| Ember Rift | Basalt, ash and ember seams | Diagonal fault, bridge crossings and flank routes |
| Frostline Basin | Snow and blue ice | Ring ridge, central basin and outer circuit |
| Verdant Divide | Jungle earth, moss and forest rock | Winding divide with three passes |
| Shattered Dunes | Sand, sandstone and oasis clearings | Broken ridges and broad open flats |
| Orbital Foundry | Steel flooring and industrial walls | Connected rooms, halls and alternate corridors |

Map cards preview the actual terrain geometry and remember the last choice. Multiplayer readiness includes a map vote; the majority wins, with the earliest ready vote breaking ties. The controller publishes the map ID, fixed terrain seed and terrain snapshot so all clients share the same battlefield. Original Classic coordinates remain available, and older snapshots default to Classic. The simulation's changing random seed is separate from the fixed map seed. Atlas prompts and provenance are in `assets/maps/README.md`; palette terrain remains usable if a texture is unavailable.

## Checks

From this directory, run `npm install`, `npx playwright install chromium`, then `npm test`.
Set `CHROMIUM_EXECUTABLE_PATH` to use an existing Chromium executable.

The headless checks load the real game source and CSS, replacing Firebase imports and the network boot with an isolated test entry point. They cover all three construction styles, snapshot fields, hold/pan/pinch gestures, smooth camera controls, mining stop/resume and ownership, helper construction, ability hotkeys and effects, distant gas construction and cancellations, bounded particles, faction artwork, persistent difficulty choices, map geometry/resource access, all six map deployments, map voting and snapshot migration, and menu/command geometry at phone and desktop sizes. They do not contact Firebase. Set `TURNCRAFT_SCREENSHOT_DIR` to capture menu screenshots from the browser checks.

Live multi-client synchronization and physical-device frame rates require an actual lobby/device check. New construction fields travel through the existing entity snapshot; presentation events are observed locally without a second simulation.
