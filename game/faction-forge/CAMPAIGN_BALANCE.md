# Campaign balance and deployment validation

The campaign prepares both armies before the normal three-card draft. An empty
player roster against an enemy's scripted lineup plus its draft cards produced
large opening disadvantages: in the initial three-seed sample, Vanir lost every
full match in missions 6, 18, 24 and 30. This was an army-budget issue rather than
a reason to increase player health or weaken every enemy's statistics.

`playerStartingRoster(faction, mission)` now supplies faction-specific tank,
ranged, support and specialist units. Prepared **Battle Ranks** increase with
chapters; permanent **Forge Tiers** continue to come only from the collection.
The tutorial and first chapter boss have three prepared troop groups. Later
chapters add support and specialists. Helions receive their already-unlocked
Siphon Leech and Hive Herald in the last chapter to support their sustain and
swarm engines. Campaign AI drafts newly introduced unlockables rather than
receiving access to all six during the first mission.

## Reproduce the sample

Run from the repository root:

```sh
node game/faction-forge/campaign-benchmark.cjs --seeds 3
node game/faction-forge/campaign-benchmark.cjs --faction Aesir --missions 1,6,30 --seeds 3
node game/faction-forge/campaign-benchmark.cjs --faction Helions --missions 1,6,30 --seeds 3
```

The benchmark runs complete four-life matches with three actual issued draft
cards per round and the loser bonus. It executes the same checked-in simulation
as the authoritative service. The simulated player chooses drafts at skill 0.9;
the opponent uses the mission's difficulty. The player has Recruit Forge Tiers
and only unlocks from prior completed missions. Match seeds and outcomes are
repeatable. `--tier 2` or `--tier 3` samples an invested collection.

## Final three-seed sample

| Faction | Mission | Player match wins | Opening round wins |
| --- | ---: | ---: | ---: |
| Vanir | 1 | 3/3 | 3/3 |
| Vanir | 6 | 3/3 | 3/3 |
| Vanir | 12 | 2/3 | 0/3 |
| Vanir | 18 | 3/3 | 2/3 |
| Vanir | 24 | 2/3 | 1/3 |
| Vanir | 30 | 2/3 | 0/3 |
| Aesir | 1 | 3/3 | 3/3 |
| Aesir | 6 | 2/3 | 3/3 |
| Aesir | 30 | 2/3 | 0/3 |
| Helions | 1 | 3/3 | 2/3 |
| Helions | 6 | 1/3 | 1/3 |
| Helions | 30 | 0/3 | 0/3 |

These are small deterministic samples, not measured human win rates. Draft
availability, counters and faction matchups produce different outcomes, so
difficulty is not strictly monotonic in three seeds. The introductory mission is
now winnable for every faction in this sample. The final elite swarm is much
harder in the Helions mirror; Vanir artillery and Aesir chain damage offer better
counters. The campaign allows a different faction on each mission. This mirror
matchup warrants further player balance feedback rather than a claim of equal
faction win rates.

## Board capacity

Starting deployment reserves only a side's half of the board and checks entire
2×2 footprints. When that half has no suitable free space, `spawnSlot` returns
`null`; remaining roster copies remain reserves for future rebuilt battles.
Formation placement reserves legal footprints before committing positions and
falls back to the previous formation if a complete legal plan cannot be found.
No unit is spawned on an occupied fallback tile. Summons also have live-unit,
team, parent and decoy caps.

The automated capacity scenario uses 30 Golems plus 100 small-unit troop groups
per side and verifies every initial footprint is inside its half, inside the
board and disjoint. The unit/campaign suite also verifies rank separation,
ranked normalization, ability stacking limits, healing bounds, all unlock
milestones, ordinary card effects and declared endless-pressure hazards.
