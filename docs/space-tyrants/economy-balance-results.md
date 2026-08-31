# Economy balance results

Three deterministic campaigns ran for 1,800 simulation seconds each at a 0.5-second step. All used the complete shipping runtime, with ordinary AI, diplomacy, colony missions, production, freighters and combat. The player controller only prioritizes an existing project every 20 seconds; it does not issue tactical fleet orders or accept every trade/governor proposal.

No stock grants, population grants, technology boosts or accelerated completion were applied. The harness stops with an error if any owned planet has a non-finite or materially negative stock value. All three campaigns completed that validation.

| Seed | Final player worlds | Final AI world range | Completed projects | Mean observed completion time | Peak ship entities | AI stations T1 / T2 / T3 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 937145 | 3 | 4–5 | 158 | 368s | 124 | 11 / 0 / 0 |
| 1776 | 6 | 4–8 | 183 | 362s | 150 | 10 / 2 / 0 |
| 20260831 | 0 | 4–8 | 177 | 342s | 136 | 8 / 0 / 0 |

The campaigns completed 518 projects in total. Visible ship counts stayed below the existing technical bound of 280. AI factions expanded, maintained fleets and built independent stations using paid projects. Two AI stations reached Tier 2 in seed 1776; Tier 3 did not emerge spontaneously in these 30-minute campaigns, but the behavioral suite verifies both successive physical upgrades through a System Citadel.

## What the balancing pass changed

An earlier diagnostic had routine factory upgrades bypassing an Imperial priority. Those projects could continuously consume the same Components/Equipment before a donor could dispatch them. The exemption now applies only to essential first production/training facilities. Priority-related manufacturing inputs can also draw deeper reserves. AI production choices persist instead of being overwritten by the older trade planner.

A separate controlled test verifies that a .005 crew shortage finishes by converting actual civilian population and Equipment, then transporting personnel. Material and crew tails retain their supply orders, including .01 material and .000001 crew deficits. Empty input stores produce no emergency Equipment or orbital Components. Destroyed station freight reopens its resource demand.

## Limits and risks

- Scarce Components and Equipment remain the main pressures. Some factions end with very low discretionary inventory while real production continues; unused raw stock is not interchangeable with industrial output.
- Lower-priority projects can remain open for more than 1,500 simulation seconds. A long-open project is not necessarily stalled throughout that time. These campaigns do not establish a universal completion deadline or prove every possible shortage can be resolved.
- A priority still needs accessible producers, population, Equipment and usable routes. Wars, exhausted deposits, failed deliveries and lost sponsors can delay or prevent completion. Controlled tests cover feasible shortages, not physically impossible cases.
- The passive player is conquered in seed 20260831. The simulation continues to observe rival economies. Player/AI fleet-power differences here are not a fair skill-balanced matchup; the player controller does not defend intelligently.
- Tier 3 balance, very large player empires, prolonged blockades and repeated major-fleet losses need human playtesting. Large stations create continuing upkeep, so the economic benefit of upgrading depends on location and use.
- CPU timings are headless measurements and exclude browser rendering. Canvas geometry and inspector generation execute in the harness, but no visual browser preview was generated.

## Final faction snapshot

Faction 0 is the player. Crew is shown as personnel, while saved values use population units in millions.

| Seed | Faction | Worlds | Fleet power | Industry | Components | Equipment | Crew | Oldest active project |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 937145 | 0 | 3 | 13.7 | 6.59 | 69.9 | 145.7 | 5900 | 64s |
| 937145 | 1 | 5 | 103.9 | 6.06 | 73.6 | 36.5 | 123 | 291s |
| 937145 | 2 | 5 | 71.1 | 5.88 | 21.3 | 13.3 | 2673 | 834s |
| 937145 | 3 | 5 | 90.9 | 3.45 | 0.0 | 0.0 | 121 | 1009s |
| 937145 | 4 | 5 | 78.9 | 1.38 | 0.0 | 0.0 | 120 | 1072s |
| 937145 | 5 | 4 | 56.1 | 2.09 | 0.0 | 0.0 | 24 | 948s |
| 937145 | 6 | 5 | 65.4 | 4.78 | 0.1 | 7.5 | 15 | 449s |
| 1776 | 0 | 6 | 29.2 | 1.53 | 2.1 | 2.3 | 480 | 1527s |
| 1776 | 1 | 5 | 112.7 | 11.55 | 43.4 | 21.7 | 3931 | 366s |
| 1776 | 2 | 4 | 87.7 | 6.33 | 89.5 | 7.8 | 134 | 659s |
| 1776 | 3 | 8 | 68.1 | 2.99 | 0.1 | 0.1 | 456 | 1170s |
| 1776 | 4 | 4 | 75.9 | 6.30 | 0.1 | 34.6 | 5 | 807s |
| 1776 | 5 | 5 | 92.3 | 3.11 | 66.2 | 22.5 | 120 | 715s |
| 1776 | 6 | 7 | 115.9 | 8.17 | 13.7 | 18.8 | 18 | 743s |
| 20260831 | 0 | 0 | 3.0 | 0.00 | 0.0 | 0.0 | 0 | 0s |
| 20260831 | 1 | 5 | 78.5 | 3.07 | 0.1 | 0.0 | 120 | 808s |
| 20260831 | 2 | 8 | 174.4 | 8.16 | 2.5 | 53.5 | 6799 | 1254s |
| 20260831 | 3 | 4 | 83.9 | 2.72 | 3.8 | 37.6 | 0 | 429s |
| 20260831 | 4 | 5 | 126.5 | 7.11 | 50.7 | 28.6 | 5306 | 336s |
| 20260831 | 5 | 5 | 58.6 | 3.34 | 0.0 | 0.0 | 3 | 491s |
| 20260831 | 6 | 5 | 228.8 | 6.64 | 0.1 | 0.0 | 7442 | 780s |

## Reproduction and verification

- `npm run test:space-tyrants` — 123 passing tests, including 43 economy/fortress behavioral cases.
- `node game/space-tyrants/economy-campaigns.mjs 1800 docs/space-tyrants/economy-campaign-results.json`
- `git diff --check`

The [raw campaign data](economy-campaign-results.json) includes 120-second snapshots, per-resource stock, bottlenecks, priority ages, industrial output, freight capacity and consumption pressure. Absolute wall-clock timing varies by machine. These results are balance diagnostics, not a promise that every strategy is equally effective.
