# Strategic battlefield catalog

The six original 42×30 maps retain their IDs, spawn coordinates, resource sites, terrain generator and three-player capacity. The new authored collision layouts are separate from the campaign generator.

| ID | Commanders | Size | Geography |
| --- | ---: | --- | --- |
| `ashen-causeway` | 2 | 64×64 | Opposing calderas, central basalt bridge, narrow northern pass, long southern flank |
| `ghostwood-passage` | 2 | 64×64 | Diagonal forest clearings, three winding trails, broad central clearing, remote side expansions |
| `triad-basin` | 3 | 80×80 | Three mountain bases, open central basin, pairwise outer roads, contested ridge expansions |
| `obsidian-crown` | 3 | 80×80 | Three plateaus, narrow descents, inner circular road around a resource-rich core, outer bypass roads |
| `fourfold-divide` | 4 | 96×96 | Corner strongholds, open quarter battlefields, mountain gateways, neighboring perimeter roads |
| `rift-bastion` | 4 | 96×96 | Rectangular compounds and assembly halls, narrow inner bridges, separate secondary industrial circuit |
| `ashen-confluence` | 6 | 112×112 | Six calderas, inner command circuit, neighbor flanks, six contested outer bridgeheads |
| `rift-dominion` | 8 | 128×128 | Eight compounds, broad staging halls, inner and outer circuits, four rich inner sectors |

Every new base has a 17-tile-wide interior and exactly two openings in its surrounding wall. Both openings have five-tile corridors and an outside staging point. Neutral watchtowers sit beside staging areas rather than in their entrances. All maps include one natural expansion per commander, contested neutral expansions and independent flank roads. Start spacing is at least 33 tiles. Natural access distances are checked across all commanders; their shortest grid routes differ by at most six steps.

## Generator contract

`TurncraftMaps.definitions` includes `width`, `height`, `capacity`, `playerCount`, `category` and `legacy`. Categories are `2-player`, `3-player`, `4-player` and `expanded`. Existing maps remain `legacy: true` so their original artwork atlases can continue to be used. New maps use the existing terrain and obstacle art at their actual collision locations.

`generate(id, seed)` keeps the existing `terrain`, `starts`, `expansions`, `conquestSites`, `neutralSites` and `relic` fields. New layouts add `topologyVersion: 2` and:

- `bases`: each base has its commander index in `player`, its location, interior `radius`, and two `entrances`.
- `entrances`: each has its gate location, full corridor `width`, `main` or `flank` role, and an outside `staging` point 18 tiles from the base center.
- `chokes`: named locations with full corridor `width`, `role`, optional base `owner`, and two usable `staging` points. Base choke staging is ordered inside, outside; other choke staging points straddle the passage.
- `routes`: named `main`, `flank` and `expansion` polylines with their corridor `width` and tile-coordinate `points`. These are strategic waypoints; unit navigation still validates terrain and live obstacles.
- `forwardPositions`: open assembly and forward production sites, with `player` and `role` where applicable.
- `starts[].rotation`: explicit starting-resource orientation, directing economy into the sheltered side of the clearing.
- `expansions[].role`: `natural` or `contested`; `naturalFor` and `rich` retain their existing meanings.

All coordinates are integers. Returned objects are fresh, so callers cannot corrupt a later generation by modifying metadata. The seed controls floor and obstacle surface variation, while authored topology and collision stay fixed. No simulation random calls or mutable global random source participate in map generation.

## Validation

Run `node --test tests/strategic-maps.test.cjs tests/maps-geometry.test.cjs` from this folder. The tests cover all eight new maps and preserve all geometry checks for the five non-Classic legacy maps.

New map checks include independent-client deterministic generation at zero, negative and maximum unsigned seeds; capacity and dimensions; one connected playable terrain component; 1×1 and 2×2 terrain reachability to all starts, economy sites, strategic waypoints, objectives, gates and staging positions; unclamped and non-overlapping resource footprints; base interior room; exactly two base wall openings; balanced natural access; long spawn separation; distinct collision layouts; independent flank routes; and continued 2×2 access when either entrance is sealed with a 7×7 obstacle. This validates terrain clearance, while the navigation and match integration tests exercise live traffic and participants.
