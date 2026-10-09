# Settlement economy, expeditions and performance

This update extends the existing game, preserves its illustrated artwork and military systems, and rebuilds `game/apes-together-strong.html` for the existing Magic lobby route. The baseline for comparison is main commit `70e52681616bf7b7e5d9ee271bdf5a2effdabf6c`.

## 1. Resource expeditions

Visit a main hut and press **B**, or open the army command dock and choose **Village buildings** on touch. The council's **Resource Expeditions** section provides separate food and lumber ranges: Local, Extended (1,600 world units), and Frontier (2,800). Choose Balanced, Food First, or Lumber First priority. The council shows available adults, active parties, cargo, and current trip or shortage status. Orders save with the village.

Shortages dispatch actual parties of 3–8 adult residents. Expeditions use at most 30% of adults, preserve essential staff and defenders, and share the existing limit of four traveling kingdom groups per village. Workers follow shared navigation corridors, harvest genuine resources, carry up to 16 units each, return physically, and deposit only near their home settlement. Local collection retains the existing nearby gathering system.

Selection considers productive supply, reservations, known threats, and route cost. Cached path lengths are used when available; otherwise known crossings and distance provide an estimate that actual movement must validate. Resource discovery and scoring proceed in bounded batches. Unsafe nearby candidates cannot permanently hide more distant safe ones. Competing villages cannot reserve the same expedition resource, and local harvesters respect those reservations.

Trips react to attacks and exhausted resources. Stalled outward trips time out and temporarily reject their destination. Blocked return routes retain real workers and cargo and retry after 20 seconds; they do not teleport across obstacles. Changing range affects later departures while current trips finish. Q, main-hut recruitment and holding T to mobilize residents clear the former mission but retain carried cargo. R and tapping T retain their existing field-recall scope. Cargo can be deposited only after returning to its source village. Full storage leaves excess cargo with the carrier; a killed carrier loses that carrier's share.

## 2. Renewable production and balance

Only completed, living structures produce resources. The existing scheduler accounts for elapsed simulation time at every distance, and storage limits still apply.

| Source | Production per game second |
| --- | --- |
| Garden | `0.7 + 0.25 × fertility + 0.12 if watered + 0.2 if tended`, multiplied by existing food specialization/support bonuses. Fertility is clamped to 0–2. |
| Orchard | `3.8 + 0.6 × fertility + 0.36 if watered`, multiplied by the same food bonuses. |
| Staffed workshop | `0.2 / (1 + 0.08 × workshop index)` timber; indices start at zero. Forge/workshop specialization multiplies this by 1.2. |
| Resident consumption | `0.06 × population` food; an operational cooking structure reduces it by 6%. |

A workshop needs an eligible adult worker within 85 units; staffing pauses under attack. Workers walk to their assigned workshop and visibly saw timber. Gardens retain their minimum output without a gardener; the tending bonus requires actual nearby staff. Destroyed or unfinished structures provide no income, and remote workers cannot count as staff.

At the minimum garden yield, four gardens cover a population of 40, nine cover 100, and 22 cover 250 before growth or other spending. Good terrain, staff and specialization reduce those infrastructure requirements. Three staffed general workshops produce about 0.557 timber per second together, giving a useful but gradual construction income. Tree expeditions offer larger deliveries at the cost of travel, labor and exposure.

Equipment, armor, training, gear and siege-material features remain available. Automatic forge crafting requires at least 40 timber, respects the essential-building safeguard, and requires food reserves before spending its existing 12 timber and 6 food. This prevents optional crafting from draining an impoverished village. Automatic construction reserves timber for missing food and workshop infrastructure and prioritizes urgent food production and damaged facilities before optional expansion. There is no unconditional free-resource grant.

## 3. Productive, stable workers

Jobs are reconsidered periodically, normally every eight seconds, or after meaningful changes. Allocation uses real buildings, available resources, shortages, repairs and pending projects. Useful existing assignments and committed builder deliveries survive reassignment. Children remain outside the ordinary adult workforce; active missions, cargo carriers and other occupied workers cannot also produce locally.

Gardeners and woodworkers receive specific completed buildings. Builders share a cached project board. Obsolete work releases workers for useful tasks; workers are no longer assigned phantom hauling or gardening activity where no applicable work exists. Defense receives a larger share during attacks, and legitimate guarding, care, training and recovery remain supported.

Construction and obstacle clearing require physical arrival at every simulation detail level. Distant residents move through the existing navigation scheduler; lower detail skips invisible animation stamps, not material delivery or arrival checks.

## 4. Reliable construction

Affordable repeatable buildings can be commissioned without a queue-size, builder-count, attack, population or immediate-plot restriction. Genuine unique lodge upgrades remain unique. Costs are reserved exactly once on acceptance. A successful order immediately appears in the queue; when a plot is unavailable it displays its search status and continues searching later.

Plot discovery is incremental: one waiting commission per village tick, at most 96 candidate positions per survey. A local plot index and cached project board avoid nested full-queue scans for each resident. Up to six construction sites and two lumber tasks can be active while the remaining paid queue waits. Completion is idempotent, and save/restore preserves paid costs, pending surveys and finished records.

Real mouse and touch tests purchase every affordable catalog entry and queue 121 projects while the fixture has one adult, an attack, and no immediately available plot. They check charges, visible feedback, unique-upgrade limits and save/restore. Separate navigation tests follow real builders to clearing and construction sites.

## 5. Warlord at 200

King of the Jungle remains at 100 living apes. Warlord now unlocks permanently at **200**, including the settlement-tier fallback, equipment access and displayed instructions. Crown, music, ceremony and rewards remain in place. Existing saves are evaluated against the new threshold while preserving acknowledged ceremonies and permanent ranks after population losses. Unrelated military escalation thresholds were not changed.

## 6. Ground rendering

Ordinary decorative grass strokes were removed from both the base ground renderer and its detail overlay. Existing terrain color, texture artwork, crops, reeds, bushes, trees, rocks and navigation geometry remain. Character and equipment artwork retains the existing detailed rendering paths.

## 7. Profiled optimizations

Profiling identified local separation, repeated navigation clearance/water checks, settlement planning and detailed actor rendering as major costs. Changes include:

- Pooled spatial buckets, reusable nearest-eight neighbor buffers and stamped pressure accumulation for horde separation, retaining the existing actor coverage and cadence.
- Shared local collision candidates with exact per-segment collision tests, plus a conservative native river-envelope shortcut. Custom terrain and positions near water still use the original exact checks.
- Cached settlement task information, one-time initialization/migration checks, bounded plot discovery, and fewer nested project/structure scans during rendering.
- Shared immutable character frame/contact geometry, fixed direction records, and bounded held-item socket caches reused across rendering passes. Artwork replacement invalidates these caches. Sprite detail, actor counts and combat work were not reduced.
- Removal of repeated grass-line drawing and avoidance of invisible distant work-animation updates.

## 8. Measured performance

See [the paired performance report](benchmarks/SETTLEMENT-ECONOMY.md) for the hardware, identical scenario settings, mean/p95 results, profiling breakdown, memory observations and reproduction commands. Raw before/after data is committed beside that report. The report distinguishes simulation-only timings from full-art browser work and labels the new expedition scenario as after-only because the baseline has no expedition feature.

In the matched full-art browser runs, mean work improved by **3–12%** across the comparable scenes: 500 followers went from **17.79 to 16.17 ms**, 250 residents from **15.81 to 13.96 ms**, 120 commissions from **18.40 to 16.84 ms**, and extreme combat from **50.82 to 47.77 ms**. Most p95 results improved, but the 750-follower scene varied between runs and does not support a consistent tail-latency claim. Simulation-only Node improvements were larger: about **26–38%** in the key large-population and settlement cases.

The **25–35% total browser-cost target and 30 FPS extreme-battle target were not achieved** on this hardware. These measurements are CPU work timings on one machine, not a guarantee of displayed FPS. Extreme scenes retain all military units and detailed artwork. Node GC diagnostics observed fewer collections and a lower maximum pause, with a slightly worse GC p95; the full report gives those limits and does not attribute browser stalls to GC from heap samples alone.

## 9. Validation

**550 automated tests passed, with zero failures, skips or cancellations** (58.2 seconds). **All 22 browser regression scripts passed** in headless Microsoft Edge, including their desktop and touch/mobile cases. After the last two accounting/arrival fixes, the affected smoke, village expansion, living-kingdom and new economy browser checks were rerun and passed. The standalone build consistency check passed, and the lobby's existing single-player launch/download path was verified.

Focused checks cover low-fertility food balance, staffed timber output, destruction and repair, offscreen accounting and physical work, competing resource reservations, physical river crossing, recalls and storage overflow, mid-trip saves, real construction clicks, Warlord migration, artwork cache invalidation, and bounded navigation/planning work. The new foraging regression invokes the actual population refresh to ensure that its legacy worker estimate cannot credit absent expedition members or cargo carriers with extra local production. Eighteen new settlement-economy tests and fifteen expedition tests extend the existing coverage.

Reproduce automated and browser checks from the repository root:

```sh
node --test game/apes-together-strong/tests/*.test.cjs
node game/apes-together-strong/build.cjs --check
node game/apes-together-strong/tests/settlement-economy-browser.cjs
```

Browser scripts use Playwright and can use an installed Chromium-family browser through `CHROMIUM_PATH`. Existing `*browser*.cjs` regression scripts are also run; the separate performance browser tools are measurements described in the linked report.

## 10. Save compatibility and limits

The existing campaign save format remains supported. Expedition settings, missions, member identities, exploration/search progress and cargo are serializable. Transient indexes and caches are rebuilt. Old production counters migrate into real structures only for legacy settlements that predate living structures; loading a modern village cannot resurrect destroyed buildings or grant replacement income. Existing construction and inventory records remain authoritative.

Economies still require appropriate infrastructure and living workers. A village with no resources, no working production and no accessible gatherable supply has no artificial bailout. Threat information is limited to what the kingdom knows. Routes may become blocked after departure; workers wait or return safely where possible, and a permanently closed route can leave cargo awaiting a real reopening or recruitment. Site surveys can remain pending if terrain truly has no valid space. Existing range choices do not recall a party midway through a valid trip.

The standalone build and the existing single-player lobby URL remain the delivery path; this change does not deploy or merge itself.
