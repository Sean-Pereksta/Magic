# DwarfWorld: The Living Mountain

The playable entry point remains `game/DwarfWorld.html`. It loads the original
game inside its existing iframe and installs the Living Mountain extensions.
The direct `DwarfWorld-core.html` route retains the core game and performance
improvements; use the shell for the full fortress, art and interface experience.
No additional game server or browser runtime dependency is required.

## Building a working fortress

1. Excavate a connected area with **Build → Dig**, then designate a room on
   explored floor. A room must be 2–12 tiles per side, have a corridor connection,
   and avoid other rooms or buildings. Drag a rectangle or use the compact preset.
2. Start with a garden, kitchen, dining hall and quarters. The founders' stockpile
   contains 36 stone and 12 meals. Construction needs delivered stone and a miner,
   mason or engineer physically working in the room.
3. Assign at least one farmer, cook and hauler through **Dwarves**. Workers use
   their profession to choose jobs; haulers move ingredients between locations.
   Keep the routes open. Room cards show progress, inventory and shortages.
4. Add a smelter, smithy and armory, with smiths and soldiers. Mining leaves stone
   and ore on the mined tile. Haulers deliver ore to a smelter, ingots to a smithy,
   and equipment to an armory. An unequipped soldier must visit the armory to
   claim a weapon or armor through the existing item system.
5. Add stoneworks for construction blocks, medical quarters for medicine, and a
   library for a gradual production bonus. Upgrade a room after four blocks have
   arrived. Its floor style, furnishings and rank accents reflect the upgrade.

| Industry | Input → output per cycle | Profession | Base time |
| --- | --- | --- | --- |
| Mushroom garden | none → 4 mushrooms | Farmer | 10 s |
| Kitchen | 2 mushrooms → 4 meals | Cook | 8 s |
| Smelter | 3 ore → 2 ingots | Smith | 12 s |
| Smithy | 2 ingots → 1 equipment | Smith | 16 s |
| Stoneworks | 3 stone → 2 blocks | Mason | 8 s |
| Medical quarters | 2 mushrooms → 2 medicine | Healer | 10 s |
| Library | none → 1 knowledge | Explorer | 30 s |

Skills, relevant traits, knowledge and room level modify production. Ingredients
belong to the in-progress job until completion: cancellation returns them once.
Hauling reserves source quantities, picks them up on site, then delivers them
at the destination. Death or dismantling leaves physical supplies at that
location. Saved cargo remains attached to its carrier. The original currency,
item, relic and perk systems continue alongside these new physical materials.
Workplaces retain their own ingredient buffer instead of repeatedly borrowing
the same scarce inputs from one another. At the loose-pile capacity limit,
mining pauses and dismantling refuses to discard goods. A dead carrier or
ambushed caravan retains its load at the actual location until room opens for
the dropped supplies. Assign haulers to clear piles when that warning appears.

Stone roads speed travel; rails help loaded haulers. Depots and hauling stations
provide connected storage. Lift stations and paired legacy lifts improve travel
along an existing walkable route; neither moves a worker or cargo by teleporting.
Road and rail pieces follow the excavated route and do not create passages
through solid terrain. Rooms and roads can form satellite production areas.

## Inhabitants and defense

The ten professions are miner, hauler, mason, smith, farmer, cook, healer,
soldier, engineer and explorer. Existing armed miners remain eligible for legacy
guard assignments. Explicit civilian professions are excluded from that guard
maintenance so cooks and farmers keep doing their chosen work.

Hunger, fatigue, injury and morale affect daily decisions. Dwarves visit food
stores, rest in quarters, socialize during off-shifts, gain friendships and
record personal accomplishments. Masterworks, rescues, discoveries, construction
and major battles appear in the bounded Fortress Chronicle. Food and completed
quarters together support additional population capacity without removing
existing residents when food is scarce.

The six work priorities range from paused to high: mining, construction, food,
defense, rescue and exploration. Priorities affect relevant job selection and
supply requests. Emergency needs and already carried cargo receive precedence.
Failed destinations back off instead of triggering a fresh search every frame.

**Normal**, **Alert** and **Lockdown** control readiness. Civilians seek safety
under nearby threats. Walls, barricades and arrow slits block movement; slits
allow sight. Gates keep enemies out and close to civilians during lockdown.
Watch posts reveal nearby tunnels, traps have a cooldown, and sappers can breach
fortifications. Existing towers and guards remain part of the defense system.

## Exploration and opponents

Discovered settlements offer visible caravans carrying regional goods. A trade
costs 20 gold; delivery requires a real route to a marketplace or stockpile.
Ambushed caravans leave cargo on site. Unreachable routes cancel and refund after
five minutes. Three successful visits establish an alliance and a recruitment
opportunity through the existing visitor system. Ruins require a nearby explorer
to recover supplies. Small settlements and outposts use the same world terrain.

Existing spawners become goblin warrens, orc warbands, undead crypts, Deepwild
broods or abyssal enclaves. A scheduled ecosystem tick handles a small slice of
camps. Nearby raids are announced before spawning. Camp populations and total
enemies are capped, including splitter children and summoned units.

Scouts observe along clear sight and share remembered positions. Enemies do not
query the whole map for undiscovered player targets. Scouts and wounded enemies
can retreat, sappers attack structures, commanders support nearby allies, and
healers and ranged attacks respect solid-wall sight checks. Five named leaders
use marked attack warnings, capped summons or area attacks and one-time hoards.

## Controls

The thin HUD, collapsible minimap and command dock keep the map visible. Desktop
offers Build, Dwarves, Military, Economy, Explore, Inventory and More. Narrow
screens use five primary buttons and expanded choices. Only one context panel
is open; on phones it becomes a bottom sheet.

Tap/click selects. Drag pans the map when not designating; designation mode
draws rooms or dig areas. Pinch zooms on touch screens. Long-press opens details.
Selected dwarves show a bounded route preview without revealing unseen tiles.
The original equipment, forge, civic buildings, save and cloud controls remain
available through the compact panels.

## Implementation boundaries

| File | Responsibility |
| --- | --- |
| `living-performance.js` | Budgeted navigation, regional shared route suffixes, spatial hash, fixed clock and interpolation helpers |
| `DwarfWorld-core.html` | Fixed simulation loop, visible-entity drawing, cached terrain chunks, fog invalidation and dirty UI integration |
| `living-simulation.js` | DOM-free rooms, inventories, reservations, jobs, needs, transport, migration and supply indexes |
| `living-fortress-runtime.txt` | Existing-game adapters, physical mining drops, rendering hooks and atomic combined saves |
| `living-world-runtime.txt` | Settlements, caravans, camp ecology, tactical sight, bosses and world rendering |
| `living-art-runtime.txt` | Atlas loading, animation selection, equipment overlays and fallbacks |
| `living-interface-runtime.txt` | Compact desktop/touch interface and contextual controls |
| `fortress-command-expansion.js` | Ordered, bounded loading into the existing core closure |

Simulation runs at 30 Hz, with at most three catch-up steps per rendered frame.
Visual movement interpolates previous/current positions without modifying game
or save coordinates. Navigation uses pooled, queued searches with a shared
6,000-node step budget, failure backoff and bounded caches. Completed point routes
can be reused through 16×16 lookup regions by actors going to the same target.
Blocking changes invalidate routes. This is region-indexed route sharing, **not
a full hierarchical portal graph**. Long unexplored routes may still require
multiple search slices and retries.

Terrain is cached in visible chunks with bounded rebuilds, an LRU count cap and
a pixel-memory budget. Fog dirties only affected chunks. Ore scans use local
scratch buffers; supply queries inspect at most 128 local/rotating distant
candidates instead of scanning every pile for every worker. Combat uses spatial
indexes. Static light textures and atlas effects avoid per-frame light gradients.
Effects, rooms, infrastructure, stockpiles, factions and history have explicit
limits to prevent unlimited accumulation.

## Saves and rollout

The local key and `DWGZ2:` compressed envelope remain unchanged. The existing v2
payload gains `livingMountain.version: 1`; legacy v1 and v2 saves initialize the
new state when absent. Tile buffers and core collection shapes are checked
before loading. Unsupported future versions are rejected. A single compressed
write contains core, civic and living state, and storage-quota failure preserves
the prior local save. Navigation caches and transient routes are reconstructed.

Firebase slots, encryption and account flows are unchanged. Offline tests block
external services and never write production cloud slots. Live account/cloud
roundtrips remain a rollout check. Keep an exported pre-upgrade save until that
check and device testing are complete.

Artwork is included in `game/dwarf-world/assets/living-mountain-v1/` and loads
automatically from the same site as the game. Deploying the repository includes
the illustrated pack without additional setup. [ART.md](ART.md) documents the
asset paths and optional overrides. See [QA.md](QA.md) for reproducible tests,
measurements, and the remaining validation boundaries.

This release provides the connected fortress and living-world foundation. It does
not yet include a full quest/technology tree, elaborate rivalry storylines,
individually editable furniture placement, a multi-level elevator simulation,
or a complete hierarchical navigation graph. Artwork scope and authored-motion
limits are explicit in ART.md. Real midrange-phone performance and live cloud
roundtrips must be measured before calling the release fully verified.
