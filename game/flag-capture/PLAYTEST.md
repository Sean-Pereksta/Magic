# Riftbound gameplay checks

Run from the repository root:

```sh
node --test game/flag-capture/engine.test.mjs
node game/flag-capture/playtest.mjs
```

The playtest prints JSON summaries and individual results. `--seeds=6` increases the sample. `--engine=path/to/engine-before.mjs` runs the same policy against a saved earlier engine.

The default sample has 48 deterministic bot matches: four modes, all six directed faction pairings, three maps, two seeds, and four affordable roster styles (balanced, scouts, brutes, and mystics). Both sides issue one legal command every 1.8 seconds; who acts first alternates by seed. The player-side bot uses a mirrored, side-swapped view of the same state. These diagnostics test rules and AI decisions; human play and browser performance need separate checks.

Compared with the engine at `b552fc3`, the revised engine produced these results on the same sample:

| Mode | Mean duration before → after | Reached 180-second limit before → after |
| --- | --- | --- |
| Beacon Rush | 74.9s → 75.3s | 0 → 0 |
| Zone Control | 76.0s → 75.7s | 0 → 0 |
| Last Army Standing | 113.4s → 112.1s | 3 → 2 |
| Reinforcement Clash | 89.0s → 88.5s | 0 → 0 |

All 48 revised matches resolved with legal commands. Two healing/shield battles in Last Army Standing used the existing health tiebreak. Match times are broadly stable; the sample does not establish overall faction balance or human enjoyment.

Concrete improvements:

- Full-shield guard orders with no effect fell from 16 to zero. An unavailable guard no longer consumes a troop's recovery.
- Zone Control movement that reduces the acting side's number of owned zones fell from 22 to 8. This is a diagnostic, since retreat can be sensible. Targeted scenarios also verify that the AI preserves sole defenders and wins contested zones from adjacent squares.
- Pursuit follows traversable routes around walls and toward shoreline enemies. Flying troops on water can be attacked; water still blocks attacks passing through it. Judgment Ray damages the hovering occupant and stops there.
- Route distances are shared within each AI decision. Sequential runs on this Node host measured median/p95 decision time of 0.78/5.44ms before and 0.56/1.79ms after. Timing varies by machine and does not measure browser rendering.
- Moving or replacing drafted troops preserves the preview's seeded terrain and RNG. Passable groves remain under deployed troops, allowing a deliberate Brood root-network opening. Regression coverage checks all 27 map/size/density combinations.

The 63 engine tests cover faction movement, all 21 specials, independent cooldowns, five difficulty schedules, gentle opening play, all modes, drafting/deployment, summoning limits, and purchases restricted to Reinforcement Clash. Troop stats, faction identities, mode goals, and difficulty intervals remain unchanged.

## Browser controls comparison

On the same desktop browser/viewport, a paused default 9 × 7 local Flag Capture match with its commander selected received four repetitions of Move → Attack → special 1 → special 2 → special 3. Measurements cover the synchronous render function, including HTML generation, DOM updates and animation bookkeeping; they are not end-to-end input latency or FPS.

| Twenty command selections | Before | After |
| --- | --- | --- |
| Board replacements | 20 | 0 |
| Median render CPU time | 9.5ms | 6.2ms |
| p95 render CPU time | 12.6ms | 8.4ms |

The persistent renderer preserves creature/control identity and skips unchanged artwork. The app records the last 200 render durations and board mounts in the invisible app element's data-render-stats attribute for repeatable diagnostics. Timings vary with device and load; this small sample does not prove universal frame rate.

Actual UI checks covered buying an extra scout, two-player placement, stable terrain while arranging, the preserved formation after countdown, auto arrangement, campaign preparation, movement into a contested zone, red attack targets, a strike reducing a scout from 6 to 4 HP, a separate healing special restoring it to 6 HP with green targets and effects, pause/menu, and both local command docks at 390 × 844. No browser warnings or errors appeared during these checks. Physical simultaneous two-finger hardware was unavailable; independent multi-pointer ownership, drop validation, click suppression and cleanup were exercised in actual-handler integration tests.

## Tactical expansion

The tactical update adds nine troops (seven main choices per faction), eighteen abilities (39 total), three formation rules, and six kinds of temporary ground effects. Formation bonuses and field effects are actual engine rules; the UI reads the same definitions.

Run the focused rules and expanded bot exercise from the repository root:

```sh
node --test game/flag-capture/engine.test.mjs game/flag-capture/tactical-engine.test.mjs
node game/flag-capture/tactical-playtest.mjs
node game/flag-capture/playtest.mjs
```

The rules suites pass 83 checks. The 20 new tactical scenarios cover formation geometry and breakage; ranged defense and pincer damage; weighted movement through hostile fields; every new ability; three-second healing, damage and shield ticks; expiry; repainting without stacking; cleansing and detonating ground; fields persisting after a caster falls; and entry effects during ordinary moves, forced swaps, summons and recruitment. Existing Story/Relaxed pacing and simple opening-plan winnability regressions still pass.

The expanded exercise runs 24 deterministic matches: four modes and all six directed faction pairings, across three map themes and balanced/wild terrain. Both sides field all seven main roles at a 40-coin budget and issue one command every 1.8 seconds. The mirrored player-side policy also mirrors field positions and ownership, so both bots evaluate the same ground rules.

| Exercise | Legally resolved | Finished before the time limit |
| --- | --- | --- |
| Classic 48-match sample | 48 / 48 | 46 / 48 |
| Expanded seven-role sample | 24 / 24 | 21 / 24 |

All 39 abilities, all six field kinds, and every troop role were used in the expanded matches. Orders issued while a formation was active included 354 Brood, 27 Carnival, and 376 Choir orders. This confirms that the different formation requirements occur during play; it does not imply equal frequency or faction balance. Five matches across both samples used the normal time-limit tiebreak.

On the Node test host, the final expanded sample measured median/p95 AI decision times of 0.86/2.02ms; the classic sample measured 0.70/1.92ms. These figures include the new field and formation decisions but do not measure browser drawing or input latency. They vary with the machine and concurrent work.

At Balanced and higher difficulty, the AI selects deterministic five-troop compositions containing guardians, rangers or shapers, each costing 21 starting coins. Story and Relaxed keep the familiar gentle starting roster; players can purchase every new role at any difficulty. Mid-battle buying remains exclusive to Reinforcement Clash, where both players and AI may recruit the expanded roles.

Earlier sections record the preceding game-feel release's comparison and timing evidence. These new diagnostics establish legality and exercise the expanded mechanics, rather than proving human enjoyment or universal faction balance.
