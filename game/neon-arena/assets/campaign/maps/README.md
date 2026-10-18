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
| Rail Yard Nine | Furnished passenger cars, rail sleepers and buffer stops, suspended freight crane, signal depot and elevated crossing |
| Whiskerhold Fortress | Crenellated ramparts, exterior buttresses, guarded gates, furnished barracks/prison, command rooms and watch towers |
| Floodgate Sewers | Ribbed lower aqueduct, overhead service ducts, paired sluice controls, pump buildings and upper cross galleries |
| Cold Storage District | Refrigerated rack aisles, roof condenser banks, sheltered loading bays, four enterable buildings and overhead gantry |
| Clockwork Foundry | Traveling crane and suspended crucible, assembly conveyors, reactor chimney, workshops and maintenance platforms |
| Night Market Ruins | Lantern gates, broken clock monument, storefront awnings, six shops, rooftop chimneys and linked rooftop crossings |

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
overwatch points. It also supplies `features` and 16 reserved `combatSpaces`
across the six maps. Their centers and corner samples retain a clear standing
floor even as the surrounding geometry gains detail. Patrol waypoints are goals for the physical pathfinder,
not permission to walk in a straight line through a wall.

`worldMetadata(id).lighting` contains `ambient`, `exposure`, `sunColor`,
`sunDirection`, `fogDensity` and up to six `zones`. Zone x/z/radius are world
coordinates; y is the physical lamp height (no actor-eye offset). Each zone
also has a color, intensity and label. The unscaled authoring values live in
`createLayout(id).theme.lighting`. Cold storage uses bright cool task lights;
the foundry combines warm furnace light with white work lamps; the market
keeps a blue night sky with clear warm and purple lantern pools. The other
maps use distinct daylight, industrial and utility-light palettes.

`worldMetadata(id).minimap` is a bounded array of at most 140 world-space
`{x,z,w,d,label?}` footprints for ground-floor buildings, major scenery and
exterior cover. Small overhead trim is omitted to keep tactical silhouettes
legible. These footprints reveal architecture only; actor visibility remains
the HUD's responsibility.

Verification:

```sh
node --test game/neon-arena/tests/war-maps.test.cjs
```

The tests check the production geometry validator, actual collision-clear
doorways and firing windows, all HQ and objective positions, branching sector
links, all ten mission assignments, lighting bounds and world conversion,
minimap footprint bounds, clear combat-space grids, and real tactical paths through every
sector and campaign anchor and onto all elevated fighting surfaces. They also
enforce the existing 60,000 static-vertex and 900 navigation-node budgets and
verify every local preview path. The detail pass uses 30,900–37,188 static
vertices and 366–659 navigation nodes per battlefield. The diagrams include
the new landmarks, clear areas and light pools, and rebuilding them also
updates all ten mission briefing cards. Desktop/mobile frame rate and multiplayer
battlefield play still require real-device playtesting.
