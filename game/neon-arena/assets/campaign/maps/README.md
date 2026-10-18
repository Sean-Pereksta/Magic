# Cats vs. Mice battlefield artwork

These six local SVG tactical diagrams are generated from the same authored
walls, ramps, roofs, scenery, headquarters and sector graph used by the game.
They are map diagrams, not screenshots. No remote asset upload is required.

Rebuild from the repository root:

```sh
node game/neon-arena/tools/build-war-map-previews.cjs
```

The source of truth is `game/neon-arena/neon-war-maps.js`. Its six layouts use
the existing arena surface atlas, physically collidable scenery prefabs, static
render batches, movement rules, smooth ramps and cached tactical navigation.
Each battlefield occupies the existing approximately 130 × 130 metre playable
area, preserving the original engine's bounds, building grid and spatial hash.

| Battlefield | Distinct playable architecture |
| --- | --- |
| Rail Yard Nine | Enterable passenger cars, signal depot, maintenance underpass, elevated freight crossing and signal tower |
| Whiskerhold Fortress | Four guarded compound gates, firing slits, barracks, prison, command rooms and two enterable rampart towers |
| Floodgate Sewers | Lower drainage passages, four pump/control buildings, central upper aqueduct and two cross galleries |
| Cold Storage District | Four warehouses and offices with multiple entrances, refrigerated machinery and overhead loading gantry |
| Clockwork Foundry | Production workshops, turbine/furnace yard, destructible assembly benches and connected maintenance platforms |
| Night Market Ruins | Six enterable shops, broken market cover, linked rooftop crossings and corner ramp access |

Doors have at least 3.4 metres of headroom. Firing windows are physical gaps
between separate solid sills, jambs and lintels. Raised walkways retain full
standing clearance beneath; their ascent ramps are visibly solid to the floor.

`NeonWarMaps.get(id)` supplies `name`, `desc`, `tag`, `tileset` and `preview`.
`build(id, adapter)` delegates all geometry to the existing arena helpers.
Its `addPlat` adapter returns the created platform so roof material and
walkable-rooftop metadata can be applied. `addNavPoint` receives logical x/z
coordinates and an eye-height y value; its host converts x/z to world units.

`worldMetadata(id)` provides world-space, eye-height headquarters, numeric
`hq` and `factionSpawns` aliases, five connected Conquest sectors, campaign
anchors, patrol route waypoints, ten named mission site sets, cover and
overwatch points. Patrol waypoints are goals for the physical pathfinder,
not permission to walk in a straight line through a wall.

Verification:

```sh
node --test game/neon-arena/tests/war-maps.test.cjs
```

The tests check the production geometry validator, actual collision-clear
doorways and firing windows, all HQ and objective positions, branching sector
links, all ten mission assignments, and real tactical paths through every
sector and campaign anchor and onto all elevated fighting surfaces. They also
enforce the existing 60,000 static-vertex and 900 navigation-node budgets and
verify every local preview path. Desktop/mobile frame rate and multiplayer
battlefield play still require real-device playtesting.
