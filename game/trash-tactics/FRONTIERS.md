# Frontiers: a larger desk war

Seven maps support every 2-, 3- and 4-player setup: **21 layouts**. Every map is **9600 × 6400**, twice the previous width and height and four times its area. Troop sizes, walking speeds, attack ranges, home economy distances and six 1,500-trash starting cans retain their scale. A sheltered natural expansion is close enough to establish early; contested centers and rich outer fields create reasons to scout beyond it.

The map dropdown replaces the card grid. Its preview draws actual obstacles, heights, lanes, expansion sites and player starts, and updates when the player count changes.

| Map | Strategic character |
| --- | --- |
| Desktop Meadow | Broad main road, high-ground shelves and long outer paper lanes. |
| Breakroom Blitz | Covered service routes, cliff ramps and napkin terraces. |
| Stapler Canyon | Northern bridge, southern crossing and destructible central Stapler Gate. |
| Conveyor Chaos | Reversible factory belts, contested consoles and long outer loops. |
| Inkspill Delta | Three broad causeways across ink rivers; exposed rich outer banks. |
| Corkboard Crossroads | A central crown with four real ramps; radial and perimeter flanks. |
| Server Stack | Dual service crossings and vertical circuit corridors; solid racks with open air overflight. |

Main roads, flanks, crossings and lookout approaches have named metadata shared by the bots and map graphics. Expansion markers identify safe naturals, contested central fields, elevated caches and rich exposed salvage. Rich fields contain more trash; gathering trips still carry five trash under standard rules. New cliff ramps and crossing clearances allow large formations to pass.

## Rival battle plans

Bots have deterministic personalities: raider, flanker, quartermaster, air marshal and siege breaker. They rotate plans instead of repeatedly sending the same force down the same road. Plans use persistent squads and waypoints so distant reinforcements keep marching rather than receiving a new order every moment.

- Scouts inspect rival approaches and resource fields. Troop choices counter currently seen or recently remembered enemy families.
- Main pushes stage before committing. Split flanks threaten two approaches; feints pull attention while another squad follows a side route.
- Raids prioritize seen workers and remembered expansions. CD drop raids load real passengers, cross the map and unload into an assault, with available airplane escorts.
- Defenders respond to visible economy threats. Workers flee nearby attackers; wounded or countered troops retreat and regroup. Specialist timing and counter kiting improve with difficulty.

Bots obey fog, collision, training times, supply and construction costs. Map camps and lane layouts are public, while live hidden enemy positions stay concealed.

| Difficulty | Style | Resource rules |
| --- | --- | --- |
| Easy | Later scouting, small forces and longer recovery windows. | Standard. |
| Normal | Expansion, scouting, rotating attacks and basic defensive reactions. | Standard. |
| Hard | Faster reactions, more scouts, stronger counter choices, worker defense, split attacks and specialist coordination. | Standard. |
| Impossible | Most aggressive planning, larger production capacity and frequent coordinated waves. | **Cheats:** each bot receives 300 extra starting trash, 2× gathering credit, and 2 trash/second up to 1,200 extra. |

The human always starts with 300 trash and receives ordinary gathering credit. Impossible bonuses are recorded separately in simulation statistics. This mode does not grant extra sight or troop health.

## Raids and knockouts

Twig workers now have **75 HP**, up from 40, at the same **25-trash** price. They survive two ordinary 30-damage melee hits, buying time to respond to a transport drop.

All eleven troop types have distinct illustrated death animations: falling twig and leaf, fractured stone, torn paper, separated scissors, shattered CD, collapsed sniper and rifle, folded airplane, fallen mage hat and staff, scattered divider sectors, heavy rock slabs, and dissolving shadow blades. Death removes the unit from combat, supply and collision immediately; its visual sequence lasts about 1.3 seconds (1.45 for Rolling Rock). The [knockout gallery](death-gallery.html) provides replay, pause and frame scrubbing.
