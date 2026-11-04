# Grand battlefields and fortifications

The simulator accepts **40,000 soldiers in total, up to 20,000 per army**. Every soldier remains an individual body. Larger numbers need more room; Grand, Vast, and Colossal fields are useful when raising the army scale. Frame rate depends on the computer, battle density, effects, and camera view.

Eight new authored fields bring the battlefield browser to **30 authored fields plus Custom**. Thirty sourced historical encounters appear in the separate historical collection; their deployment notes and limitations are documented in [HISTORICAL-BATTLES.md](HISTORICAL-BATTLES.md).

| New field | Size | Distinctive ground and defenses |
| --- | --- | --- |
| The Timber Marches | Grand | Four-gate timber stockade, muddy approaches, woodland, and firm roads |
| Castle of Four Winds | Grand | Stone curtain castle, four gatehouses, clear crossing lanes |
| The Reedwater Keep | Vast | Complete moat, four actual causeways, reed marsh, stone perimeter |
| Starhaven Redoubt | Grand | Low earthworks, projecting corner bastions, and approach trenches |
| The Twin Crown Citadel | Vast | Two separate defense rings, aligned gates, wide galleries, inner reserves |
| The King’s Causeway | Grand | Village buildings, road, mud, marsh, tower, and separate barricades |
| The White Pass | Vast | Three wide mountain gaps, snow, field trenches, and watchtowers |
| The Sable Lines | Grand | Two trench depths, artillery positions, broad gaps in barricades, gate keep |

Each field has an explicit scenario containing about 3,200–6,000 soldiers before scaling. Gate reserves, missile supports, artillery, assault companies, and mounted wings have actual positions. The fortress assaults include protected rams and long-range bombardment. The concentric citadel also has a separate inner-court reserve.

The Four Winds assault demonstrates a manned curtain wall. Its archers and crossbows start in the courtyard and use the interior stairs when the battle begins. The gate reserve remains on the ground. Wall companies can change their rank count and spacing or withdraw by the stairs to reform in the court; a breached supporting wall triggers a courtyard response.

Large garrisons need several minutes of battle time to fill distant posts. Soldiers walk to finite stair entrances, climb in physical queues, and form their wall ranks in order. Ground reserves and other companies still occupy real space, so keep their approach lanes clear.

Four Winds has an additional interior stair on its lower western curtain, giving its crossbows a direct approach that clears the stationary ground defenders. This is access metadata on the existing wall; it adds no ground obstacle or passage through the masonry.

## Terrain

Road, mud, marsh, snow, and trench regions are passable surfaces. They affect movement and cover; they do not create invisible walls. Roads favor movement, mud is especially slow, marsh provides some cover, snow slows the march, and trenches provide substantial cover at a movement cost. A road does not make deep water passable. A real bridge or causeway must cover the crossing.

## Castle and structure tools

The original square keep is retained. Five additional styles are available: timber palisade, curtain castle, moated castle, bastioned star fort, and concentric citadel. The star fort uses a stepped rectangular approximation for its projecting bastions rather than exact angled ramparts.

Every wall, tower footprint, gate, and building is physical and destructible. Gates belong to their chosen defender. Open the gate, destroy it, or establish a siege passage to cross a defended perimeter. A closed gate remains a real obstacle; a decorative road through it does not bypass the door.

Moated castles have four broad causeways aligned with their four gate openings. Their moat extends outside the wall rectangle. The concentric design has four routes through each ring and broad space between them. The house, watchtower, and barricade tools create solid objects that troops route around. The gate keep creates a hollow court with two opposed entrances.

Castle walls, gatehouse tops, connected corner platforms, and gate keeps have walkable decks. Each defense ring has four interior stair accesses; concentric castles have four for each ring. Stone decks are 36 units high, timber decks 28, low earthworks 14, and gate keeps 56. Gate doors still control ground passage beneath their decks. The stairs describe a real ground-to-deck route and do not add ground obstacles. The original wall and gate footprints remain unchanged.

Ranged troops fire from the height they have actually reached. A manned parapet makes a lower outside force less accurate and reduces incoming missile damage. A shooter already in the courtyard does not receive that outside accuracy penalty. Corner platforms defend their exposed sides as well.

When arranging a field, leave room outside a castle for formations to deploy and inside it for defenders to turn. Separate buildings and barricades with broad lanes. A long line can use a three-file column to pass through a doorway and form up afterward. Very large armies can still become congested when many bodies try to use one opening at once.

## Data contracts

`GameData.TERRAIN_TYPES`, `STRUCTURE_TYPES`, and `FORTRESS_TYPES` provide names, descriptions, and placement defaults for the editor. `createFortress(kind, x, y, width, height, options)` and `createBattlefieldStructure(kind, x, y, width, height, options)` return ordinary region arrays. An invalid kind or undersized fortress returns an empty array.

Destructible regions retain `type: 'wall'` or `type: 'gate'`. Their `structureKind` identifies stone, palisade, earthwork, house, watchtower, barricade, or keep construction. Optional names remain attached during save and import. Surface regions use their own terrain type. `MAX_TERRAIN_REGIONS` is 192, and `MAX_UNITS` is 40,000.

Walkable regions carry `walkable`, `deckHeight`, `rampartSide`, `courtyardX`, `courtyardY`, and `stairs`. Each stair has `id`, `x`, `y`, `w`, `h`, `axis` (`x` or `y`), `dir` (−1 or 1 from ground toward the deck), `width`, `height`, `groundX`, `groundY`, `deckX`, and `deckY`. Its deck endpoint lies on its owning wall; its ground endpoint lies inside the clear courtyard. Connected deck rectangles use the same height. An optional formation `rampart` assignment contains `structureId`, `ranks` (1–3), and `spacing` (`compact`, `normal`, or `wide`); omitting the structure selects a nearby friendly wall. Assignments never place soldiers on a deck at initial deployment.

`createRampartWall(x, y, width, height, options)` returns one walkable wall region with one stair and the original ground footprint. `courtyardSide` chooses `east`, `west`, `north`, or `south`; the default is east of a vertical wall or south of a horizontal wall. The wall faces the opposite exterior side. It also accepts `team`, `hp`, `name`, `structureKind`, `deckHeight`, and `type: 'gate'`. Optional `worldWidth` and `worldHeight` reject access points beyond the field; invalid or narrower-than-24-unit pieces return `null`. Custom walls need clear room for their access and court response points. Existing bare wall and gate lines, including the Imperial Rampart, receive access only where ground and the stair route are clear. Their physical outlines remain unchanged.

Run `node tests/battlefield-data-test.cjs` to check catalog coverage, physical template metadata, entrance sizes, map bounds, roster totals, intended deployment centers, solid-terrain clearance, and starting body separation on all eight new fields.

Run `node tests/rampart-data-test.cjs` to check deck connectivity, stair endpoints and clear ground access on all template sizes, keep platforms, and the Four Winds garrison assignments. `node tests/fortress-navigation-test.cjs` verifies actual infantry and cavalry movement through gates, causeways, and breaches.

`node tests/rampart-runtime-test.cjs` checks physical climbing and descent on every fort style, the separate inner citadel ring, the higher gate keep, supported wall formations, previews, elevated firing, incoming missile protection, larger company traffic, courtyard responses to occupied-wall and adjacent-gate breaches, and a direct ground Move order issued while troops are on the wall. These checks track individual bodies throughout their routes rather than placing them directly on a deck.

The optional `node tests/authored-garrison-test.cjs` audit retains all 3,258 soldiers in the actual Four Winds encounter, holds the other formations, suppresses weapon fire, and requires every one of its 320 ranged guards to occupy a real deck slot within 600 simulated seconds. It checks physical support, nearby-body separation, finite positions, and company cohesion throughout. Progress messages distinguish long queues from 40-second periods without a new climber or occupied post, followed by one JSON result. This lengthy audit stays outside default CI. The validated garrison filled all posts after 387.733 simulated seconds with 100 cohesion and a minimum body-separation ratio of 0.96.
