# Naval and river expansion

Water is a second movement network. Ships use `sea:q,r` or `river:q,r`
positions; armies continue to use land hexes. River nodes follow the same
reciprocal connections and mouths as the geography renderer, including its
bounded legacy coast-outlet repair. Rivers retain their land terrain, roads,
resources, buildings and ownership.

## Construction

| Vessel | Resources | Population | Turns | Troop capacity | Ocean / river movement |
| --- | --- | ---: | ---: | ---: | ---: |
| War Canoe | 22 wood, 12 food | 4 | 1 | 0 | 10 / 13 |
| Transport | 35 wood | 6 | 1 | 25 | 9 / 10 |
| Warship | 55 wood, 24 iron, 12 arms | 10 | 2 | 0 | 9 / 9 |

Shipyards cost 60 wood, 35 stone, 25 iron and 55 gold, taking 3 turns.
They are settlement additions requiring navigable water beside a town or city.
Each yard builds one ship at a time; its queue holds up to 12 vessels. Resources,
crew and one construction order are paid when queued. A yard cannot recruit crew
below the existing 20-civilian floor. Cancellation refunds resources and crew
once. Captured or destroyed yards lose their queue. A finished ship waits if all
launch positions are blocked by hostile fleets.

Fishing Docks cost 30 wood, 15 stone and 18 gold, taking 2 turns. They occupy a
land site beside navigable water and produce 14 food at level I. Fertility does
not modify their output. Both new buildings have three tiers using the existing
upgrade cost/time/production formulas.

## Orders and cargo

Select a fleet on the map or through **Your realm → Your fleets**. Its panel shows
vessel counts, individual hull/crew, troop capacity, remaining movement and orders.
Move, Unload, Escort, Intercept, Blockade and Hold use the existing end-of-turn
resolution. Move into hostile vessels to attack. Blockade requires a Warship.
An Unload order can target a distant legal shore: the fleet sails there over
multiple turns and attempts a landing on arrival.

Embark from friendly, accessible or unclaimed shores. A 47-troop army requires
two Transports; a 72-troop army requires three. Whole armies load only if enough
capacity exists. Embarking removes the army object from `state.armies` and puts
that same army in exactly one fleet's `cargo`. Per-ship manifests account for
every troop and never exceed 25. General identity and bonuses travel with it.
Embarked armies remain in military-strength and upkeep accounting.

An army cannot take land orders while aboard. Landing uses the existing battle
and capture rules and consumes the army's action; a repulsed army's surviving
troops remain cargo. Ships remain on their naval node. Ships cannot capture land.
Fleets cannot resolve twice within a round; combining fleets cannot reset their
movement. Fleets sharing an owner's position render as one compact stack.

## Combat and blockades

Vessel attacks account for class, hull condition, surviving crew, fleet morale,
command bonuses and the canoe's river advantage. Escorts take incoming hull
damage before Transports. Large groups can overcome stronger individual ships.
Boarding follows the naval clash only on contact, allowing carried troops to
fight crews and other embarked troops. Retreats use connected unoccupied water
positions. Interception is limited to connected water edges and one reaction per
fleet per turn.

A sinking Transport kills 80% of its assigned troops. Survivors transfer into
spare capacity on surviving transports in their fleet or another friendly fleet
at the same naval position. Overflow dies; survivors never become water armies.
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
fleet state. Deploy the updated `firestore.rules` with the client update to admit
these command types; this PR does not deploy production rules.

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
