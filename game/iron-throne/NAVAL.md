# Naval and river expansion

Water is a second movement network. Ships use `sea:q,r` or `river:q,r`
positions; armies continue to use land hexes. River nodes follow the same
reciprocal connections and mouths as the geography renderer, including its
bounded legacy coast-outlet repair. Rivers retain their land terrain, roads,
resources, buildings and ownership.

## Construction

| Vessel | Resources | Population | Turns | Troop capacity | Ocean / river movement |
| --- | --- | ---: | ---: | ---: | ---: |
| War Canoe | 22 wood, 12 food | 4 | 1 | 0 | 7.5 / 7.5 |
| Transport | 35 wood | 6 | 1 | 25 | 7.5 / 7.5 |
| Warship | 55 wood, 24 iron, 12 arms | 10 | 2 | 0 | 7.5 / 7.5 |

Shipyards cost 60 wood, 35 stone, 25 iron and 55 gold, taking 3 turns.
They are standalone buildings on empty owned land with navigable ocean or river
access. No town or city is required. Earlier settlement-added Shipyards and their
existing queues remain usable and upgradeable without replacing the settlement.
Each yard builds one ship at a time; its queue holds up to 12 vessels. Resources,
crew and one construction order are paid when queued. A yard cannot recruit crew
below the existing 20-civilian floor. Cancellation refunds resources and crew
once. Captured or destroyed yards lose their queue. A finished ship launches into
adjacent navigable water, preferring a friendly stack at that distance. If all adjacent positions are occupied by other Houses,
it searches outward along connected water for the nearest empty or friendly
position. It never spawns on land or in a disconnected lake. Only a completely
occupied connected waterway leaves the finished ship waiting; it retries next round.

Fishing Docks cost 30 wood, 15 stone and 18 gold, taking 2 turns. They occupy a
land site beside navigable water and produce 14 food at level I. Fertility does
not modify their output. Both new buildings have three tiers using the existing
upgrade cost/time/production formulas.

## Orders and cargo

Select a fleet on the map or through **Your realm → Your fleets**. Its panel shows
vessel counts, individual hull/crew, troop capacity, remaining movement and orders.
Board, Move, Attack / bombard, Unload, Escort, Intercept, Blockade and Hold use
end-of-turn resolution. Adjacent armies board before sailing; distant armies march
and board automatically after land movement. Blockade requires a Warship.
Clean route lines and small red crossed swords show queued orders immediately,
even while deselected. Selecting an army adds a small numeric badge halfway along
the visible route: **0** means this resolution, **1** the next, and so on. Boarding
uses a green route; ordinary marches use blue. Badges use terrain/road/river costs,
formation and general speed, spent movement, known zones of control and planned
sailing. Attack counts mean arrival at contact, not completion of a battle. Routes
and badges render above water, terrain, fog and unit artwork; enemy orders stay private.

Every vessel has a 7.5-hex ocean/river budget: 2.5 times standard infantry on open
ground. Continuous journeys retain the half hex, sailing seven then eight hexes
over two turns. Combat and unloading consume the remaining activation.
An Unload order can target a distant legal shore: the fleet sails there over
multiple turns and attempts a landing on arrival.

Embark from friendly, accessible or unclaimed shores. Boarding reserves available
seats and displays how many troops will board and remain ashore. One Transport
can take 25 from a 70-troop force, leaving 45. Three stacked Transports show 0/75
and can take all 70. Ready friendly stacks on the same water node combine when
boarding (maximum 100 vessels), preserving the selected fleet's sailing order.
Select Army → March → click a friendly transport anywhere on a known reachable
route. The order stores the fleet ID, land route and embark position across turns,
saves and multiplayer snapshots. Partial boarding shows a compact count preview
before accepting the order; the whole army marches to the embark point and the
remainder stays ashore there. Cancel using Hold/Cancel boarding.

The route replans toward the same fleet when it moves. Lost capacity or an
unreachable shore pauses boarding with a reason in the army card and Chronicle;
it retries on the next resolution. A destroyed or foreign fleet cancels the order
and leaves troops safely on land. General replanning preserves queued boarding.
No troops teleport, duplicate or bypass transport capacity. ETA is recalculated
from current observation state; a saved estimate is never the display authority.

Boarding rechecks contact, occupancy, capacity and activation at resolution. Whole
armies keep their identity aboard; partial boarding creates one detachment and
leaves the original army and its general ashore. Per-ship manifests account for
every troop and never exceed 25. Cargo remains in strength and upkeep accounting.

An army cannot take land orders while aboard. Landing uses the existing battle
and capture rules and consumes the army's action; a repulsed army's surviving
troops remain cargo. Ships remain on their naval node. Ships cannot capture land.
Fleets cannot resolve twice within a round; combining fleets cannot reset their
movement. Fleets sharing an owner's position render as one compact stack.

## Combat and blockades

Warships fire at visible enemy fleets or land armies/structures from two hexes.
Transports and War Canoes must reach connected adjacent water. Only in-range
weapons contribute to damage or return fire. Ranged fire cannot capture land.
Archers, Veteran Archers, Crossbowmen, Catapults and legacy ranged siege engines
can target ships at two hexes; Trebuchets can fire from three. Battering Rams and
melee troops cannot shoot ships. Select a ranged army and click a visible enemy ship to fire. March → click a
friendly transport queues boarding, including partial boarding when seats are limited. Mountains
block fire and targets require current vision (scouts can spot for siege units).
Orders recheck vision, hostility, range and line of fire at resolution.

Vessel attacks account for class, hull condition, surviving crew, fleet morale,
command bonuses and the canoe's river advantage. Escorts take incoming hull
damage before Transports. Large groups can overcome stronger individual ships.
Boarding follows the naval clash only on contact, allowing carried troops to
fight crews and other embarked troops. Retreats use connected unoccupied water
positions. Interception is limited to connected water edges and one reaction per
fleet per turn.

A sunk Transport loses every troop on its pre-clash manifest. No survivors move
to other transports or become armies on water. Troops on surviving ships remain.
Battle reports record sunk vessels, boarding, troop casualties and retreats.

An on-station enemy Warship blockade suspends adjacent Fishing Dock output and
Harbor production and excludes that Harbor from coastal trade routing. Existing
overland trade alternatives remain available.

## Knowledge and AI

War Canoes and Warships see five hexes; Transports see two. Fleets create dated
last-known contacts when lost from view. Enemy projections expose vessel classes
but omit manifests, troop counts, hull/crew detail, movement orders and queues.
The owning House retains the complete fleet and cargo data. Allied vision uses
the existing trust rule; other courts receive no cargo manifests.

AI builds coastal/river Shipyards, pays for crew and vessels, adds Fishing Docks,
scouts by canoe, gathers enough Transports and protection for a whole army, attacks observed
fleets, escorts loaded convoys, intercepts near its stations, blockades hostile ports,
transports armies and lands invasions. Cargo
fleets return to safe ports when their objective disappears. Higher difficulties
prepare navies earlier and seek more escorts; ordinary war motives still apply.
All strategic targets and routes are chosen from the House's knowledge view.
Canonical state is used only by shared command handlers and encounter resolution.

## Maps, authority and saves

New boards add four rows/columns to the previous dimensions to retain expansion
space while adding at least four playable exterior water rows on every side.
Broken Coast supports substantial divided regions. Shattered Realms adds broad
channels separating several landmasses. Generation requires meaningful land
components, geographic/resource diversity and complete valid founding packages.
Existing campaigns keep their dimensions and geography.

`shipBuild`, `shipCancel`, `fleetEmbark`, `fleetOrder`, and `fleetMerge` run through
the existing authenticated, lease-fenced sequential command controller. Naval
commands require the exact current state version as well as the active House,
activation, epoch and command sequence. The controller computes all movement,
casualties, resource changes and cargo transfers. No client submits replacement
fleet state. Land ranged attacks use the existing authenticated `order` command;
this update introduces no new Firebase command types or rule changes.

Schema-3 campaigns gain `navalVersion: 1`, `fleets`, `shipQueues`, and construction
tick accounting. Import checks unique identities, owners, hull/crew bounds,
connected paths, movement budgets, manifests, cargo capacities and duplicate
armies. Both original and expanded board dimensions are supported. Older saves
without naval fields migrate to empty naval collections.

## Artwork

All paths use the existing `IRON_THRONES_ASSET_BASE` in `asset-manifest.mjs` and
preload with other artwork. No new binary art is committed:

```
ships/war_canoe.png
ships/transport.png
ships/warship.png
buildings/shipyard_1.png
buildings/shipyard_2.png
buildings/shipyard_3.png
buildings/fishing_dock_1.png
buildings/fishing_dock_2.png
buildings/fishing_dock_3.png
```

Unavailable ship images use a compact anchor symbol on the map; new buildings
reuse the existing Harbor silhouette until the supplied PNGs are available.

## Verification

```
npm run test:iron-throne
node game/iron-throne/tests/naval-browser.mjs
```

The browser check uses the optional dependencies in `tests/package.json` and
supports `IRON_THRONE_CHROMIUM`. It checks real construction/embark/unload controls,
missing-art behavior and horizontal overflow at 1280px and 390px without making
screenshots or HTML previews. Firebase emulator tests include all naval command
types, authenticated actor checks and controller fencing. They use only the
`demo-iron-thrones` project.
