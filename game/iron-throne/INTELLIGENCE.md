# Living diplomacy, intelligence and structure warfare

The local deterministic simulation remains authoritative. Gemini adds dialogue
only; the systems below run with scripted diplomacy and without network calls.

## Structure orders

Select an army, then a hostile building. **Structure Targets** lists every
completed primary building, road and settlement add-on, with durability and
**Attack** / **Bombard** actions. An attack can march to its target; an attack on
the army's current tile remains an attack. Move on the current tile still holds.

Catapults and legacy engines bombard up to two hexes; trebuchets reach three.
Rams operate on the occupied tile. Mountains block ranged fire. Only the siege
units that reach the target contribute damage. Orders recheck ownership, war,
range, equipment and target existence when resolved. Enemy armies protect a
structure until defeated; existing fortified approaches retain siege and terrain
bonuses. Bombardment against a guarded structure waits for defenders to be cleared.
Each army damages a structure at most once per turn.

Economic structures have 60 / 90 / 120 durability; forts and settlement cores
have 150 / 240 / 330. Wall durability uses the existing wall-strength values.
Fort breach strength remains distinct from structural durability, preserving
ordinary siege-and-capture campaigns. Siege weapons deal substantially more
structure damage than occupying troops.

Destruction removes the target's level and production, cancels an upgrade of that
same structure, and preserves deposits, roads, other add-ons and unrelated tile
state. Destroying a city downgrades it to a town. Ordinary tile attacks retain
capture behavior; explicit structure attacks destroy their selected building.

## Political attitudes and plans

Every directed pair of Houses derives a readable attitude and current tone from
its existing political values and real treaties. Ordinary label changes require
two consecutive rounds; war updates immediately. Reasons explain trust,
grievances, troop concentrations, trade dependence and intelligence incidents.

`plans.mjs` stores at most four active objectives per House. Plans carry immutable
objective and target fields, requirements, execution intentions, assigned army
IDs, status, allies and cancellation reasons. Councils use them to prepare
recruitment and siege works, negotiate alliances / access / trade / embargoes,
defend frontiers and issue actual military orders. Military plans stop for peace,
treaty protection, an endangered capital, destroyed armies, depleted reserves,
changed ownership or overwhelming enemy strength. Stalled preparations expire
with an explanation after 24 turns. Merging armies transfers plan assignments.

Infrastructure targets are ranked by their actual production, defensive value,
distance and the enemy's mounted-army dependence. A primary nearby siege campaign
keeps its field army at the muster point while equipment is built. Secondary
raids cannot pull that army away from the preparing siege.

A bounded internal audit retains creation, paid preparations, army assignments,
discoveries, execution and abandonment. This audit and private army orders are
not exposed in the public rival-turn panel. Public construction remains visible
on the existing public map. This update does not add terrain or unit fog of war.

## Spies and reports

Build a Government **Whisper Office** in a city or town. Its three levels allow
1 / 3 / 5 living agents; level II enables counterintelligence. Captives retain
their slot. Recruitment costs 60 gold and one ordinary order. Each living agent
costs 2 gold per turn; diplomacy / military missions cost 1 additional gold,
and strategic-plan missions cost 3. Missing mission funds slow the network.

Named agents travel before embedding, build networks, gain experience, and risk
compromise or capture. Assignment, recall and missions are available under
**Intel**. AI agents use these same costs and mechanics and choose targets from
their council's plans, fears and trade dependence. Counterintelligence raises
foreign detection risk. Captors can expel, imprison, ransom, exchange or execute
agents. AI rulers consider honor and wartime conditions; a player's ransom is
paid only through the player's explicit action.

Network thresholds:

| Network | Access |
| --- | --- |
| 0–19 | Court observations; early network development |
| 20–39 | Economy and construction |
| 40–59 | Diplomacy and partial strategic-plan information |
| 60–79 | Military composition and more precise plan type |
| 80–100 | Objectives, locations, assigned armies and intended timing |

Reports store dated observations. Plan reports must reference a retained,
authoritative plan object; weaker reports omit fields instead of inventing facts.
Fresh reports can discover completion or abandonment, including a cancellation
reason at extensive detail. Discovering an offensive plan against one's own House
causes a single diplomatic reaction and can prompt AI frontier defense.

Each House retains up to 24 reports, within a total serialized report budget.
Plans retain up to 120 current and historical records; pruning removes linked
reports together. Save import validates source references, disclosed detail,
statuses, numerical bounds and report shapes. Older version-3 saves acquire empty
plan and intelligence state; prior version migrations continue to work.

The Houses panel shows public treaties and wars. Private attitudes toward third
Houses require a stationed ambassador or a dated spy report. Gemini and scripted
replies can refer to discovered plans only to their disclosed detail and date;
neither creates authoritative plans or claims their future execution is certain.

## Verification

```sh
npm run test:iron-throne
npm run test:iron-throne:browser
npm run test:iron-throne:intelligence-browser
```

`tests/intrigue.test.mjs` covers structure commands and catalog-wide destruction,
range / line of fire / defender protection, real AI preparation and execution,
plan cancellation, opinion stability, spy progression and detection, prisoner
choices, report integrity and privacy, save migration and deterministic replay.
The browser check exercises structure attacks and the Intel controls on desktop,
mobile portrait and mobile landscape without generating previews or screenshots.
