# Rival councils and the player turn

Every surviving rival House takes one deterministic strategy turn whenever the
player presses **End Turn**. The simulation runs locally, including when Gemini
is disabled or unavailable. `strategy.mjs` owns the planner; `diplomacy.mjs`
owns the round sequence; `core.mjs` remains the authority for actions and costs. Persistent objectives and intelligence are described in [INTELLIGENCE.md](INTELLIGENCE.md).

The table below describes local play. Online campaigns use the saved House
activation sequence in [MULTIPLAYER.md](MULTIPLAYER.md), calling the same planner
for exactly one active AI House at a time.

## One complete round

| Phase | What happens |
| --- | --- |
| Player orders | House Ashen chooses construction, recruitment, taxes, army destinations, formations and diplomatic agreements. Construction and recruitment spend available orders and materials immediately. Army orders are queued. |
| Political assessment | Expired treaties are removed and political observations update. Existing rival diplomacy and resource trading run on their scheduled turns. |
| Rival councils | Each living rival assesses its own economy and legitimate current/dated observations, chooses priorities, spends its remaining orders and issues army orders. The first acting House rotates each round. |
| Military resolution | Every army, including the player's, uses the same movement, borders, terrain, formation, siege and combat rules. Initiative alternates. |
| Economy | Projects advance one turn, finished tiers activate, territory updates, production and upkeep resolve, and each House receives its next order allowance. Ambassadors travel. |
| Next round | Supply contracts, pledges, relations and trade proposals update; dispatch allowances reset and victory is checked. |

Calling the rival planner again in the same round does nothing. Defeated Houses
receive an explicit inactive report. The player retains control of House Ashen;
the rival planner never changes its spending or queued orders.

## How a council decides

1. Check enemy armies near settlements and any ratified military obligations.
2. Forecast food and treasury needs, including army upkeep and two deliveries of
   existing supply commitments. Reserve a minimum operating balance.
3. Rank feasible construction and recruitment together. A blocked upgrade can
   remain a trade goal while the House performs an affordable alternative.
4. Reassess after every paid action, until the normal order allowance is spent
   or saving resources is the better choice. Buildings under construction do
   not grant their next-tier benefits early.
5. Combine armies that meet, select formations, then issue defense, recovery,
   pledge, campaign, reinforcement or garrison orders.

The planner uses normal `build`, `recruit`, `orderArmy`, `mergeArmies` and
`declareWar` actions. It pays catalog costs, obeys local military prerequisites,
uses population, respects construction time and suffers ordinary upkeep and
casualties. It receives no extra income, units, orders or completed buildings.

Food shortages favor farms; material shortages favor suitable deposits and
their quality. Forecast production prevents repeatedly building an industry
whose incoming supply already covers the need. Workshops and armories support
advanced recruitment. Markets, outposts, coastal harbors and connected roads
support commerce. Population, security and reserves govern new settlements.
Tax policy responds to treasury pressure and happiness.

Military recruitment considers the current force mix, enemy mounted troops,
the strength of defended targets and the cost of supporting another regiment.
Siege equipment has its own requirement, so a full field army can still recruit
the engines needed to open a walled campaign. Peacetime recruiting starts at
one action per round; war or immediate danger permits two. Completed city
upgrades raise those limits by their extra orders, still within the kingdom's
eight-order maximum, civilian floor, desired army size and economic reserves.
Healthy AI Houses also consider paid city upgrades from turn 12 onward.

## Difficulty and expansion

`difficulty.mjs` changes decision thresholds only. Existing saves default to
Medium. No setting changes income, troop statistics, starting packages or fog.

| Setting | Decision emphasis |
| --- | --- |
| Easy | Later, cautious opportunities; forgiving negotiation; simpler coordination. |
| Medium | Balanced development and prepared expansion. |
| Hard | Preserve civilians and save for legal settlements; build a recruiting economy before discretionary conquest; coordinate credible allied campaigns. |
| Insane | Greater land ambitions and earlier strategic opportunities, more coordinated operations and firmer bargaining, subject to the same economic and information limits. |

Settlement ambition considers known usable land, world size per House,
population, projected food and elapsed rounds. There is no fixed six-town
founding ceiling. Captured territory has never been restricted by that founding
threshold. A worthwhile invasion creates a preparation plan before the required
army exists; recruitment and siege construction address its actual shortfall.
Harder councils can save for a town instead of continuously spending its reserve
on cheaper regiments. Persistent assigned-army objectives resist small ranking
changes; battle survivors regroup before returning.

Run the full-engine comparison with `npm run test:iron-throne:difficulty`.
`IRON_BENCHMARK_SEED` and `IRON_BENCHMARK_DIFFICULTY` select individual runs. It
compares identical worlds on Heartlands, Great Divide and Highland Crown and
records founded settlements, captures, completed campaigns, shortage House-rounds
and old active objectives. All Houses are AI so the measurement includes bot
versus bot conquest; stronger opposing defenses can reduce capture counts on an
individual seed. See [COMMANDS.md](COMMANDS.md) for the verification record.

## Different Houses

| House | Priorities |
| --- | --- |
| Wintermere | Timber, agriculture, ranged troops and cautious, honorable diplomacy. |
| Thornwall | Stone, iron, infantry development and strong defenses. |
| Sunspire | Coastal revenue, trade and mounted troops. |
| Vesper | Manufacturing, infantry, ambitious expansion and opportunistic diplomacy. |
| Redharbor | Horse production, mounted armies and aggressive opportunities. |

Regional production, ruler personality, current resources and actual enemies
all affect these choices. An invasion or food shortage can override a House's
usual development plan.

## Campaigns and hostilities

- Rival Houses consider every House as a possible opponent. They defend against
  whoever attacks them and can fight, capture territory from and make peace with
  other rivals.
- New discretionary wars require sufficient troops and reserves, a hostile or
  allied-defense motive, a viable route and a favorable public strength estimate.
  The planner avoids opening a second discretionary war and honors active peace,
  non-aggression, alliance and vassalage agreements. Existing low-honor betrayal
  behavior still applies the game's treaty-breaking consequences. AI vassals
  additionally require the explicit history, warning and opportunity safeguards
  described in [COMMANDS.md](COMMANDS.md), independent of difficulty.
- Armies prioritize immediate defense and ratified military orders, recover low
  morale, and favor targets they can plausibly defeat. Reinforcements gather at
  safe forward settlements. Co-located armies combine through the ordinary merge
  action.
- Offensive routing avoids defending forces whose strength and projected losses
  make an assault unsafe; empty fortifications never block a route. These are ordinary legal paths, resolved by the same
  movement rules the player uses. The planner cannot pass through closed neutral
  borders or teleport to a target. Fortified campaigns compare immediate assaults
  with bombardment and reinforcement, reassessing after integrity or troop changes.
- A prolonged exhausted or inactive rival-versus-rival war can end in an
  eight-turn truce. Peace involving the player continues to require ratification.
- Safe holding and saving are valid choices. A House does not need to spend every
  order or launch a losing attack to count as having taken its turn.

## What the player sees

**Realm → Rival Turns** stores the latest six rounds, with a report for every
rival. Public reports show visible construction, war declarations and truces. Private
priorities, recruitment decisions and future army orders remain in the internal
audit; discover strategic intentions through ambassadors and spies. Battle details remain in the battle reports.
Foreign treasury balances stay private. **How to rule** explains the shared
round sequence and how to prepare and supply the player's own campaigns.

Reports are bounded and validated on import. Version 1, 2 and existing version 3
saves without the new report field resume with an empty report history. Saved
campaigns continue deterministically; the planner does not use model calls,
wall-clock time or unseeded randomness.

## Verification

`tests/strategy.test.mjs` verifies every rival's recurring turns, real costs,
order limits, idempotence, player control, resource saving, affordable fallback
construction, actual bot-versus-bot capture, defensive holding, siege support,
treaty constraints, rival peace, save continuation, bounded history and public
report privacy. The existing long campaign, diplomacy and pledge tests continue
to apply. Browser checks inspect the first rival round on desktop and mobile.

```sh
npm run test:iron-throne
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs
npm run test:iron-throne:browser
```
