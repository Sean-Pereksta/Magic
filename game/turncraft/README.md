# TurnCraft presentation checks

The game remains `../turncraft.html`. `polish.css` is loaded after the legacy styles. No build step or new runtime dependency is required.

## Controls

- Drag to pan; wheel/pinch or Zoom buttons to zoom. WASD/arrows pan; F focuses selection.
- Touch taps select friendly troops. With troops selected, tap open ground to move or an enemy to attack. Gestures never issue orders.
- Hold still for 380 ms, then drag a box to select friendly troops with mouse or touch. Shift-drag immediately adds boxed friendly troops to the current selection (release Shift after starting safely); double-click/tap selects the visible unit type. Moving immediately still pans, and a second finger switches safely to pinch zoom.
- Ability buttons automatically display unique shortcuts, beginning with Q/E/R/T, including on touch layouts. Allocation skips camera, command, control-group and other visible item shortcuts; large mixed selections use additional letters and Shift chords. Selection order and cooldown changes keep assignments stable. The displayed key activates the same command as a tap. Typing, composition, key repeats, Ctrl/Alt/Meta, pause and open dialogs suppress ability shortcuts. Escape or Cancel Ability exits targeting without issuing an order.
- A arms Attack (X remains available). Click an enemy to attack it, or click ground to attack the nearest visible valid enemy within four tiles; empty ground issues attack-move. Escape/Cancel Order cancels targeting. Tap 1–9 to recall a control group; hold its number or on-screen chip for 500 ms to assign the current friendly selection. Double-tap to select and center. Ctrl+number remains available; right-click a chip to clear it. Holds cancel on blur, pause, menus and selection changes. Group membership stays local to the profile and match; campaign checkpoints use the stable campaign run ID so restored timers do not lose groups.
- Press **0**, or tap **0 — All Army** at the top, to select every living owned non-worker troop across the map, including support, air and transported/garrisoned troops. Selection issues no orders or unloading commands. Later orders affect eligible deployed troops and their transports; passengers stay inside. Groups 1–9 retain their separate assignments.
- Moving troops remain selectable at their rendered positions. A small X marks selected troops finishing a movement step; the selected troop tooltip explains when the next movement order takes effect. This only describes the existing step timer and does not change orders, cooldowns, or movement.
- M arms Move, A/X arms Attack, Shift+S stops, H holds, G arms Mine and B opens Build. Every visible command uses the same button action for its displayed shortcut. Training, construction, morphing and research cards display distinct **single-key shortcuts without Shift**; automatic ability assignment avoids their keys. WASD/arrows retain camera controls.
- The battle dock is 150 px high on desktop, 218 px on phones and 202 px in landscape. It shows the selected portrait/name/health, buttons and actual hotkeys; descriptions remain in tooltips and Help. Movement commands use a 96 px high scrolling box with 44 px targets, and production/build cards use 52 px cells. Groups occupy the top row, scrolling horizontally on narrow screens. Resource changes, cooldowns, power and queue progress update existing nodes; only selection or available-action layout changes rebuild controls. Brief feedback appears temporarily without resizing the dock.
- Clicking a friendly logical square selects that entity, including a structure footprint or a moving troop. Shift-click adds a friendly troop without removing already selected troops; visible enemy occupied squares attack only with legal selected weapons. Armed abilities, construction, mining and movement retain their explicit targeting rules.
- Mine uses existing automatic gathering on arrival. Stop suppresses gathering until a new mining/movement order.
- Build lists structures and lets adjacent human workers assist an unfinished building. Choose Refinery, Assimilator or Extractor and click a visible unoccupied gas vent at any distance: the worker travels to a reachable adjacent spot and constructs automatically. The cost is checked when ordering and charged once when construction begins. Replacement orders, unavailable vents, blocked routes and lost affordability cancel without a charge. Pending orders survive shared snapshots; Brood consumes its Drone only when construction begins. Ordinary buildings still require adjacent placement.

## Match setup

The faction cards use three original illustrated WebP assets in `assets/factions/`; their generation prompts and provenance are documented there. They load independently of optional battlefield sprites. The menu offers Easy, Medium, Hard and Pro buttons tied to the existing AI simulation tuning and saved preference. Solo opponent selection now creates the requested one or two opponents. Faction and difficulty controls keep keyboard focus while setup status updates.

Easy AI now starts its economy immediately, builds essential power and production reliably, replaces consumed or lost workers, assigns gas without starving mineral gathering, and sends scouts and small attack waves. Ascendant previously never reached its first Gateway on Easy because its small workforce did not trigger Pylon construction. These basic opening steps no longer depend on optional macro rolls. Easy retains its lower income, five-worker goal, slower major decisions and smaller armies than Medium. Lost Ascendant production power is rebuilt near the affected structure. Classic maps also receive scouting orders.

The menu retains all existing faction, map popout, opponent, difficulty, temperament and objective controls. Setup and results scroll within the viewport in phone portrait and landscape layouts. Result scenes use the local commander's faction and distinguish victory, defeat, draw and early elimination; reports and their action buttons remain part of the same accessible dialog.

The command banner, insignia, colors, and match-result screen follow the local commander's faction. Three new original cinematic scenes are packaged separately for Cloudflare under `effects/ui/{faction}-command.webp`; the existing faction illustrations appear immediately and remain available if the new scenes have not been uploaded. Exact prompts and artwork provenance are in `assets/ui/README.md`. `presentation.css` and `movement-feedback.css` require no build step.

Match snapshots now include the winner, reason, faction, finish time, and match start time. Each client opens its result dialog once, including on reconnect to a completed match; older snapshots infer a result from the objective score or remaining command bases. Losing the last base shows a defeat report immediately while surviving commanders continue. Spectate dismisses the dialog without allowing an eliminated commander to issue orders, and the captured report does not keep changing during spectating.

AI combat troops respond to raids on command bases, production structures, and construction sites independently of slow economy ticks. Nearby troops respond first, with enough compatible reinforcements recalled for larger threats. Defenders pursue visible moving attackers; damage from fog draws them to the known attacked structure. Workers retain their existing behavior, and active movement, siege, and ability timers remain authoritative.

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

## Group abilities and combat effects

Each selected ability appears once with its ready/total count. Self buffs activate every ready caster; ground attacks share the requested location. Forcefield, Shield Wall and Deploy Cover find separate nearby placements, reserving the whole barrier footprint. Guardian Link, Emergency Stabilization, Intercept and Graviton Snare retain one explicitly chosen troop and use the lowest-ID eligible caster. Targeting metadata extends the original self/tile definitions without changing AI or ability balance values.

Phase Step uses the clicked location as a formation destination. The shared preview and authoritative planner preserve relative positions where possible, clamp each jump to the existing 2.25-tile range, and reserve distinct visible landing tiles. Permanent terrain, temporary barriers, buildings, resources and nonmoving occupants remain blocked. Successful simultaneous jumps may use another successful caster's vacated tile; failed casters keep their origins reserved and spend no cooldown. Partial groups report how many Dragoons Blinked. Blue squares preview planned landings, while unavailable casters receive red indicators.

Desktop and touch send the existing `groupAbility` command through the Firestore controller. Ownership, readiness, target type, visibility, range and occupancy are validated there. Bounded command receipts travel in snapshots to prevent replay after controller migration; old saves without receipts remain readable. Individual particles and effect events remain local.

Combat presentation reuses the existing sprite and combat atlases, with procedural fallbacks. Attack families gain distinct release flashes and trails; actual HP/shield losses trigger defense-specific impact effects and bounded aftermath. Teleports show fading unit silhouettes and arrival rings, storms branch throughout their active cells, and fungal fields, bile warnings and temporary barriers follow their authoritative timers. Low/Medium/High settings bound pooled events, particles and field detail. These additions require no new artwork upload, server or build step.
