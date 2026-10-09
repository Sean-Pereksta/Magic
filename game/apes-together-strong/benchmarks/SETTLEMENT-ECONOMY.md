# Settlement economy and engine measurements — 2026-10-08

The baseline is an unmodified copy of main commit `70e52681616bf7b7e5d9ee271bdf5a2effdabf6c`, copied before editing the game. The final measurements use the integrated settlement, expedition, rendering, and navigation changes. Diagnostic profiles are separate from the timing comparisons.

## Method

- AMD Ryzen 7 5825U with Radeon Graphics, 16 logical processors; Windows 10.0.26200; Node v24.19.0; headless Microsoft Edge 154.0.4258.62.
- Seed `FOREST-A`; deterministic random seed `0x923cd91`; fixed simulation step of 1/60 second.
- Simulation runs: 240 updates per scenario, including the cold simulation state. Node VM timings measure simulation only and are not browser frame rates.
- Node comparisons use the same A–L/P–S order. Baseline E was rerun separately after the fixture explicitly suppressed births in both sources; its replacement row and provenance are retained in the JSON. It therefore has a separate process warmup context.
- Browser runs: 180 animation frames, 1440×900, device pixel ratio 1, high graphics, automatic quality reduction held at level 0. No warmup frames are discarded. Image decoding and scenario construction precede timing; initial ground/sprite cache creation is included.
- The browser harness loads the complete production build order and packaged character, equipment, terrain, structure, and vehicle artwork. It does not use the older procedural-only renderer benchmark as a substitute.
- Combat benchmarks retain their declared populations with high health while running real human/ape AI, collision, weapon fire, explosive effects, and vehicles. Additional population scheduling and births are excluded from the fixed-population workloads. No agents are removed or hidden to improve the comparison.
- A–L are the existing forest, combat, exploration, settlement raid, 500/750/1,000-ape and combined-arms scenarios. P adds 250 followers; Q adds 250 working residents with nine gardens and three workshops; R adds 120 genuine building commissions; S adds three villages with 240 total residents running frontier expeditions.
- S has no equivalent expedition implementation on main. Its final result is an after-only feature stress measurement, not a claimed expedition speedup. Q/R retain the same scene setup, but their improved economic behavior is intentionally different.
- These short samples measure frame cost, cold-cache behavior and work budgets. Long-term economic correctness, delivery, shortages, navigation and save restoration are exercised by regression tests rather than inferred from a four-second benchmark.

## Profile findings and changes

Initial CPU profiles identified crowd separation, local segment clearance and ordinary ape updates as dominant simulation costs. The full-art extreme-combat profile identified ape and human drawing as the largest render costs. Inclusive method times overlap; their sum is not total frame time. Initial exploratory profiles marked `diagnosticOnly` were taken alongside validation and must not be used for before/after timing claims.

1. Spatial bucket arrays are reused, and the ape grid accepts the King separately instead of allocating a `[king, ...apes]` array for each rebuild. Crowd pressure uses transient actor fields and fixed scratch buffers, preserving the same nearest eight eligible neighbors and every-frame collision-safe movement.
2. Crowd neighbors share broad-phase obstacle candidates per 64-unit cell for each separation pass. Every pair still receives exact wall/tree intersection and water clearance checks. Candidate sets refresh before the next pass; destroyed objects are checked live.
3. Native procedural rivers have a mathematically bounded envelope: their two sine offsets total at most 175 units and half-width is at most 45. Terrain outside the periodic 220-unit envelopes cannot contain native river water. Navigation and point collision skip repeated river sampling only in those proven dry bounds. Banks, bridges, fords, and modified terrain retain the original exact checks.
4. Distant local residents use existing bounded shared route recovery around obstructions and an eight-unit station arrival tolerance. Workers reach actual material delivery/work positions while offscreen. Mission and follower arrival rules remain separate.
5. The ordinary three-line grass decorations are removed from both ground renderers. Soil textures, bitmap vegetation, crops, reeds, water, roads, rocks, bridges and structures remain. The ordinary-terrain overlay exits before redundant canvas transforms.
6. Render entry buffers and settlement structure/hut membership sets are reused. Construction rendering no longer searches every hut for each queued project.
7. Authored character frame geometry, crop destinations and contact-shadow measurements are shared in a bounded 512-entry cache. Facing descriptors and animation lookup tables are shared. All original body frames, directions, shadows and animations still draw.
8. Held-item attachment coordinates are shared across rear, front and hand-occlusion passes in a bounded 512-entry immutable cache. Caches invalidate when the artwork registry or character manifest is replaced; shared coat images also invalidate when their source images change. No per-actor image cache is introduced.
9. Settlement task/project boards, construction placement budgets and event-driven building synchronization are implemented in the accompanying economy changes. Jobs continue to account for real workers and physical arrival; lower cost does not come from skipping deliveries or reducing production.

## Memory and bounds

The browser report records initial, peak and final JavaScript heap, drops greater than 1 MiB, and the largest sampled drop. These are allocation/collection observations, not direct GC pause attribution. A slow frame cannot be labeled a GC pause solely from these samples. Canvas pixel budgets and shared sprite cache counts are recorded separately; JavaScript heap does not include all decoded image/GPU memory.

Regression tests verify exact nearest-neighbor results, one broad-phase query serving repeated nearby crowd pairs, live wall destruction, river/custom-terrain safety, frame/socket cache sharing and invalidation, offscreen station arrival, and absence of ordinary-terrain grass strokes while legitimate details remain. Existing budgets remain three new A* searches and 192 expansions per update, queue size at most 384, 32 AI decisions, 96 perception checks, and eight selected separation neighbors per ape.

## Reproduction

Run from the repository root. `BASELINE_SOURCE` below is the `game/apes-together-strong` directory in a separate checkout of the baseline commit. Provide Playwright and an installed Chromium-family browser through the same environment used by the existing browser tests.

```text
node game/apes-together-strong/tests/performance-bench.cjs --source BASELINE_SOURCE --frames 240 --output before.json
node game/apes-together-strong/tests/performance-bench.cjs --frames 240 --output after.json
node game/apes-together-strong/tests/visual-overhaul-performance.cjs --source BASELINE_SOURCE --scenario ABGHIJKPQRS --frames 180 --fixed-quality --output browser-before.json
node game/apes-together-strong/tests/visual-overhaul-performance.cjs --scenario ABGHIJKPQRS --frames 180 --fixed-quality --output browser-after.json
node --trace-gc-nvp game/apes-together-strong/tests/performance-bench.cjs --source BASELINE_SOURCE --scenario GKQR --frames 120 > gc-before.log
node --trace-gc-nvp game/apes-together-strong/tests/performance-bench.cjs --scenario GKQR --frames 120 > gc-after.log
```

Use `--profile` for inclusive subsystem timing, in separate runs from the main frame-cost comparison. Keep other validation processes stopped during comparable measurements.


## Results

All paired browser rows use the same complete scenario order. Positive reductions mean less work. Milliseconds are per update/frame. Raw JSON also records maximums, over-50-ms frames, actor counts, navigation budgets and canvas/heap counters.

### Simulation only — 240 updates

| Scenario | Mean before | Mean after | p95 before | p95 after | Mean reduction |
|---|---:|---:|---:|---:|---:|
| A: 100 followers | 7.19 | 4.05 | 10.64 | 5.84 | 43.7% |
| B: 200 followers | 12.95 | 8.89 | 15.30 | 10.72 | 31.4% |
| C: 150 apes / 80 humans | 11.34 | 9.45 | 16.31 | 13.29 | 16.7% |
| D: 200 apes / 150 humans + vehicles | 17.30 | 12.87 | 23.92 | 15.15 | 25.6% |
| E: 180-resident raid | 14.86 | 10.65 | 16.94 | 12.68 | 28.3% |
| F: Procedural exploration | 4.00 | 3.09 | 8.57 | 5.33 | 22.8% |
| G: 500 followers | 27.76 | 20.30 | 32.44 | 23.73 | 26.9% |
| H: 750 followers | 37.47 | 26.51 | 47.22 | 33.24 | 29.2% |
| I: 1,000 apes (650 followers / 350 residents) | 37.77 | 27.99 | 46.07 | 33.63 | 25.9% |
| J: 500 apes / 250 humans + combined arms | 34.47 | 25.69 | 47.77 | 32.16 | 25.5% |
| K: 750 apes / 320 humans + combined arms | 51.74 | 36.82 | 67.28 | 41.99 | 28.8% |
| L: 300 apes / 100 humans + combined arms | 19.61 | 14.90 | 28.48 | 19.60 | 24.0% |
| P: 250 followers | 15.84 | 10.78 | 18.32 | 12.58 | 31.9% |
| Q: 250 working residents | 24.09 | 14.91 | 27.95 | 17.71 | 38.1% |
| R: 250 residents + 120 commissions | 23.37 | 15.50 | 30.20 | 18.12 | 33.7% |
| S: 240 residents / 3 expedition villages | — | 7.01 | — | 8.90 | After-only |

### Full production artwork — 180 browser frames, fixed high quality

| Scenario | Simulation before → after | Render before → after | Total before → after | Total p95 before → after | Mean total reduction |
|---|---:|---:|---:|---:|---:|
| A: 100 followers | 3.03 → 2.63 | 5.71 → 5.51 | 8.74 → 8.14 | 11.60 → 10.30 | 7.0% |
| B: 200 followers | 4.85 → 4.20 | 5.37 → 4.98 | 10.22 → 9.18 | 13.40 → 11.40 | 10.2% |
| G: 500 followers | 9.40 → 8.16 | 8.39 → 8.01 | 17.79 → 16.17 | 23.30 → 20.70 | 9.1% |
| H: 750 followers | 11.20 → 9.99 | 8.29 → 8.31 | 19.49 → 18.30 | 25.20 → 27.60 | 6.1% |
| I: 1,000 apes (650 followers / 350 residents) | 12.17 → 11.51 | 8.44 → 8.46 | 20.61 → 19.97 | 26.60 → 25.30 | 3.1% |
| J: 500 apes / 250 humans + combined arms | 16.19 → 14.95 | 26.32 → 25.77 | 42.51 → 40.72 | 49.80 → 46.90 | 4.2% |
| K: 750 apes / 320 humans + combined arms | 21.90 → 19.83 | 28.92 → 27.93 | 50.82 → 47.77 | 59.50 → 55.50 | 6.0% |
| P: 250 followers | 5.99 → 5.15 | 6.07 → 5.90 | 12.05 → 11.05 | 14.70 → 13.30 | 8.3% |
| Q: 250 working residents | 6.92 → 6.07 | 8.90 → 7.90 | 15.81 → 13.96 | 19.30 → 17.10 | 11.7% |
| R: 250 residents + 120 commissions | 8.56 → 7.64 | 9.84 → 9.20 | 18.40 → 16.84 | 21.60 → 19.30 | 8.5% |
| S: 240 residents / 3 expedition villages | — → 3.67 | — → 4.15 | — → 7.83 | — → 10.70 | After-only |

The main 750-follower browser sample improved its mean but had a worse p95 (25.20 → 27.60 ms). A separate fresh-browser H/H repeat measured mean 21.71 → 20.55 ms and p95 26.70 → 24.90 ms. Both measurements are retained; short-run tail latency varies and is not uniformly improved.

The 25–35% working target is reached in several simulation-only workloads, including large hordes and settlement queues. It is **not reached for combined full-art browser work**. Those paired mean reductions are approximately 3–12%. The fixed-high extreme battle also **does not reach 30 FPS** on this headless test configuration. The separate animation-frame intervals in the JSON include browser scheduling; CPU work reciprocals are not presented as measured FPS. The remaining dominant cost is drawing the full detailed combat population.

The expedition stress scene ran 3 real simultaneous parties while retaining all 240 residents. Longer journey, gathering, deposit, interruption and save/load behavior is covered by the expedition tests.

### Clean subsystem profiles — inclusive milliseconds per update

These separate 120-update runs add timing wrappers and are not substitutes for the uninstrumented timing tables. Costs overlap (for example, ape update contains navigation work).

| Scenario | Subsystem | Before | After |
|---|---|---:|---:|
| G | Crowd separation | 11.358 | 7.613 |
| G | Segment clearance | 9.948 | 4.823 |
| G | Movement/navigation | 8.459 | 4.994 |
| G | Incremental A* scheduler | 1.731 | 1.672 |
| G | Ape updates | 9.325 | 6.099 |
| K | Crowd separation | 24.485 | 16.943 |
| K | Segment clearance | 21.131 | 9.906 |
| K | Movement/navigation | 20.533 | 12.179 |
| K | Incremental A* scheduler | 1.792 | 1.756 |
| K | Ape updates | 17.288 | 11.792 |
| K | Human updates | 7.767 | 5.237 |
| K | Human perception | 1.317 | 1.273 |
| Q | Crowd separation | 15.933 | 8.886 |
| Q | Segment clearance | 14.580 | 6.585 |
| Q | Movement/navigation | 6.017 | 4.270 |
| Q | Incremental A* scheduler | 0.946 | 1.098 |
| Q | Ape updates | 5.874 | 4.831 |
| Q | Worker target scheduling | 0.502 | 0.608 |
| Q | Construction planning | 0.013 | 0.010 |
| Q | Construction/resource work | 0.004 | 0.002 |
| Q | Job assignment | 0.001 | 0.006 |
| R | Crowd separation | 14.801 | 8.351 |
| R | Segment clearance | 13.618 | 6.244 |
| R | Movement/navigation | 6.377 | 4.647 |
| R | Incremental A* scheduler | 1.678 | 1.665 |
| R | Ape updates | 6.334 | 5.255 |
| R | Worker target scheduling | 0.592 | 0.683 |
| R | Construction planning | 0.001 | 0.001 |
| R | Construction/resource work | 0.004 | 0.006 |
| R | Job assignment | 0.001 | 0.004 |

### Actual garbage-collection diagnostics

Node/V8 was run with `--trace-gc-nvp` for G, K, Q and R, 120 updates each, in separate processes after the frame benchmarks. These values are direct V8 GC pause observations, including loading and scene setup; they do not attribute browser rendering spikes to GC.

| Metric | Before | After |
|---|---:|---:|
| GC events | 224 | 165 |
| Minor/scavenge events | 221 | 162 |
| Other/major events | 3 | 3 |
| Total observed GC pause (ms) | 120.50 | 105.90 |
| Largest observed GC pause (ms) | 2.70 | 2.10 |
| GC event p95 pause (ms) | 1.10 | 1.20 |

### Browser heap observations

| Scenario | Peak heap before → after (MiB) | Sampled drops >1 MiB before → after |
|---|---:|---:|
| G | 163.06 → 160.35 | 54 → 39 |
| K | 178.95 → 178.56 | 58 → 53 |
| Q | 179.36 → 176.02 | 30 → 24 |
| R | 178.04 → 178.17 | 28 → 27 |

Heap peaks are not uniformly lower: scene history, collection timing, navigation caches and the additional real economy affect live memory. Bounded reusable caches trade some retained metadata for fewer temporary objects. No universal memory-reduction claim is made.

### Validation and raw data

The integrated validation run passed 550 automated tests and all 22 browser scripts (21 existing scripts plus the new settlement economy coverage). Browser matrix runs completed without page errors, retained declared ape/human counts and preserved the existing navigation, lighting and graphics-cache ceilings.

- [Simulation before](settlement-economy-before.json) / [after](settlement-economy-after.json)
- [Full-art browser before](settlement-economy-browser-before.json) / [after](settlement-economy-browser-after.json)
- [Subsystem profile before](settlement-economy-profile-before.json) / [after](settlement-economy-profile-after.json)
- [Direct GC summary](settlement-economy-gc.json), [before trace](settlement-economy-gc-before.log), [after trace](settlement-economy-gc-after.log)
- [750-follower repeat before](settlement-economy-browser-h-repeat-before.json) / [after](settlement-economy-browser-h-repeat-after.json)
