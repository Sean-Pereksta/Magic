# Village growth, human reconnaissance and siege defense

## Player controls and rewards

- **K / Select climbers** immediately selects eligible field chimpanzees,
  gibbons and capuchins. Residents, dead apes and heavy species are excluded.
  Normal move/attack orders inside a fort use its existing climb accesses;
  heavy species continue through gates and breaches. Invalid or removed wall
  targets no longer crash the command. On touch, use Settlement → More army
  controls → Select climbers; selection closes the panel for map orders.
- Following hordes use evenly distributed slots and a larger footprint.
  Idle separation is 42 units, with the existing eight-neighbor work limit.
  Ordered ranks use 34-unit spacing (46 when spread), without repeating the
  first eight rows. Formation destinations stay inside the fort being entered.
- Clearing tier III / IV / V forts awards **180 / 350 / 600 food**, and adds
  one training level to up to **4 / 8 / 12** nearby surviving apes, capped at
  the existing level 3. Rewards are saved and paid once. Rescue and stock
  systems retain their existing behavior.

## Growth and tiers

Small-village baseline growth is approximately half the previous rate. Parent
scaling is sublinear, development is capped at +35%, and populations above 40
have further diminishing returns. Births consume six food each, require actual
housing and respect the global population cap. Low food reduces growth; food
shortages and full homes pause it. There is at most one birth per settlement
tick, with no accumulated burst when a paused village resumes.

Completed, living nurseries grant +35%, then +20%, then +12%. Additional
nurseries add +2% each up to a total +75%. Rally groves add +10% each, capped
at +25%. These bonuses apply to the new baseline. Existing residents remain.

| Residents | Tier | Reconnaissance interval before activity modifiers |
| --- | --- | --- |
| 1–15 | I — Outpost | 600 s, with an 80% chance to skip |
| 16–39 | II — Hamlet | 300 s |
| 40–79 | III — Village | 210 s |
| 80–149 | IV — Township | 150 s |
| 150+ | V — Stronghold | 110 s |

Upgrades are immediate. Downgrades require 30 seconds below 85% of the tier's
threshold. Completed facilities and defenses increase attention. Newly surveyed
building rings are about 15–20% closer together; existing buildings do not move.

## Discovery and invasions

Human scouts leave surviving installations in parties of two to four and
search a coarse 900-unit region. Sight requires facing, range and budgeted
line of sight to a real village structure. An eyewitness carries the report;
shouting alone cannot reveal every nearby settlement. Radio communication
takes four uninterrupted seconds, or a courier must physically return to an
installation or reach a radio carrier. Killing the scout can prevent reporting.
Searches, reports and missions expire; scouts retreat from overwhelming forces.

Villages display Undetected, Suspected, Discovered, Targeted or Recovering.
Confirmed intelligence expires after 900 seconds unless an attack/recovery is
in progress. Reports provide 45 seconds of preparation before a targeted raid.
Tier-scaled infantry caps are 4 / 7 / 14 / 26 / 48; vehicles require Township
or Stronghold. Existing military personnel, vehicle and global force budgets
still apply. A regional scheduler runs every eight seconds with at most two
regional operations. A village has one active invasion and then 150 seconds
of recovery, or 240 seconds at the upper two tiers.

Raid sources and infantry staging positions must be outside settlement
bounds. Attackers route toward openings or gates, damage intact defenses and
use breaches. Ordinary infantry cannot freely climb settlement palisades.
Workers shelter or repair while local defenders protect entrances. Existing
village defense priority and army orders remain available. Notifications name
the village and are throttled.

## Complete Palisade Perimeter

The main hut surveys an outer enclosure around current buildings, projects
and defenses, without an upfront charge. Four visibly braced gates admit
friendly apes while blocking humans. Walls overlap at the corners and are
solid for both sides. Existing defenses and building locations are preserved.

The saved blueprint queues at most six material projects at once. Workers
physically clear and build sections: eight timber per wall, twelve per gate.
An unfinished blueprint waits for more wood; repairs use the normal worker
queue. Newly finished or repaired walls release overlapping actors onto a
valid nearby side. New building surveys reserve the planned wall footprint.
After completion, another survey can enclose subsequent village expansion.

Survey work is bounded to 32 segments / 96 footprint samples per call. Trees
and rocks become clearing tasks. Water or permanent obstructions trigger an
outward resurvey, with at most 16 attempts. If no valid enclosure is found,
the menu reports blocked terrain and spends no timber. This remains a square
enclosure planner, not an arbitrary coastline-following wall editor; very
constrained terrain may require a different settlement location or clearing.

## Validation and performance

The gameplay suite covers growth, nursery returns, housing/food pauses, tier
hysteresis, physical scouting and reporting, expired intelligence, bounded
raids, recovery, paid construction, full collision enclosures, gate travel,
wall repairs, save compatibility, fort rewards and real move/attack climbing.
Browser checks exercise actual desktop and touch controls, the current mobile
wheel, council information, construction payments and saved orders. The
standalone bundle and lobby integration are checked separately.

Paired simulation benchmarks on this Windows/Node 24 machine used the same
FOREST-A seed, 360 steps at 1/60 second and two runs in opposite order. Means
below average the two runs; they include cold starts and exclude rendering.

| Scenario | Before | After |
| --- | ---: | ---: |
| 180 residents / 100 attackers | 11.10 ms | 11.41 ms |
| 500 moving followers | 20.63 ms | 20.73 ms |
| 250 working residents | 15.57 ms | 16.82 ms |

The resident scenario has a measurable ~8% simulation cost increase; the
500-follower result is nearly unchanged. These measurements are not a claim
of 60 FPS on all devices. Existing pathfinding, perception, separation and
distant-actor budgets remain in place. New threats run on scheduled village
ticks; wall geometry is calculated only for an active survey.
