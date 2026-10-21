# Living Mountain verification

Validation date: 2026-10-21. This is a draft implementation with measured desktop
browser results, not a certified mobile performance release.

## Automated checks

Run from the repository root with a recent Node.js version:

```sh
npm run test:dwarfworld
npm run test:dwarfworld:browser
npm run bench:dwarfworld
npm run bench:dwarfworld:browser
```

Node.js and Playwright are development/test tools only. The shipped game runs
as static HTML and JavaScript. Unit tests require no third-party packages.
Browser tests use the existing Playwright development dependency. Set
`PLAYWRIGHT_CHANNEL=chrome` to select installed Chrome where appropriate;
the integration and interface scripts also accept `PLAYWRIGHT_MODULE_PATH`
for an externally installed Playwright package. The stress harness uses ordinary
Node module resolution.

The unit suite passes **59 tests** across existing save/expansion tests and new
simulation, world, art, loader and navigation tests. The existing GitHub Actions
workflow now runs the entire DwarfWorld suite on relevant pull requests and main
updates. A passing local run does not imply a completed hosted CI run.

Covered behaviors include:

- Physical ore → ingot → equipment → soldier and mushroom → meal → diner chains.
- Construction after delivery, blocked routes, task reservations and all-material
  conservation under ten competing haulers, cancellation and reservation restore.
- Bounded source queries with 8,192 piles and eventual discovery of remote ore;
  workshop input buffers cannot be endlessly traded between understaffed rooms.
- Last fractional movement steps, returned ingredients on interrupted work,
  cargo preservation and dismantled-room/depot supplies remaining on site.
- Full-pile backpressure preserves cargo and unfinished ingredients; dismantling
  is refused until the supplies can remain on site. Ambushed caravans retain
  their loads if there is no room for a new pile.
- Needs, off-shift friendships, masterwork celebrations, gates and mine claims.
- Fixed-step equivalence across render rates, bounded catch-up, queued search,
  failure backoff, obstacle invalidation, spatial queries, render interpolation,
  shared routes and rejection of incomplete shared-route suffixes.
- Boss hoards awarded once, splitter population cap, wall-blocked explosions,
  sight-based enemy memory, and physically rerouted/restored caravans.
- Atlas rectangles and animation references, disabled or failed asset origins,
  request coalescing, essential preload and timeout fallback.
- Complete runtime injection under LF/CRLF line endings and missing-file failure.

## Browser integration and controls

`living-integration.browser.mjs` runs the full shell in an isolated context and
intercepts requests with local repository fixtures. It verifies compressed and
plain legacy v2 saves, preservation of relic equipment, perks, levels and civic
buildings, and a carrying haul-job roundtrip. Storage quota failure preserves the
previous saved bytes. Unsupported future versions and malformed top-level
collections are rejected before mutation.

Fourteen additional malformed nested cases compare gold, a tile sample, camera,
dwarves and all Living Mountain state before and after a rejected load: primitive
living state, missing/out-of-bounds job goals, unknown jobs, invalid references,
bad cargo, invalid production ingredients/workplace, missing stockpile inventory,
invalid caravans/factions/surveys and invalid item names. These checks do not
claim exhaustive validation of every numeric legacy combat field. No real user
save was supplied; fixtures exercise the shipped serializer and migration paths.

The browser also verifies armed miners and soldiers remain eligible for legacy
guard posts while explicit civilians keep their weapons and chosen professions.
A guard-assigned miner can still take a designated dig job.
Managed workers and legacy miners receive passive armor/civic healing and timer
updates exactly once. Designated mining grants levels and perk choices through
the original progression system. A teleport relic leaves physical materials at
the departure tile. At the 8,192-pile cap, a dead carrier's inert cargo record
survives save/load and releases its supplies at its original position when a
pile slot becomes available.

`living-interface.browser.mjs` passed at 1440×900 and touch-enabled 390×844 with
zero page errors. It uses real mouse/touch actions to designate rooms, place and
inspect infrastructure, dismantle it, change professions and priorities, reopen
legacy inventory/forge controls, save/load room state and long-press a selection.
Layout checks confirm narrow sheets and command controls remain within the
viewport. The mobile test is browser emulation, not a physical phone test.

External Firebase requests are blocked in these fixtures. Existing cloud code
and its encryption/chunking tests remain unchanged; no production slot was read
or written. A live encrypted cloud save/load cycle remains a release check.

## Stress methodology

`living-performance.stress.mjs` runs two sequential 30-second workloads at
1280×800 and 390×844. The narrow context enables mobile and coarse-pointer/touch
behavior. Each creates 100 dwarves, 100 enemies, 20 rooms across food/metal/stone
industries, two unfinished construction projects, designated mining and physical
hauling. Synthetic health protection keeps the actor counts stable. These are
deliberately artificial workload fixtures, not a balance or survival test.

The harness loads the real unpacked WebP pack through an intercepted local
route. Set `DWARF_ART_DIR` to the unpacked pack directory; the local workspace
default points at the supplied output pack. `REQUIRE_DWARF_ART=1` requires actual
loaded images. `DWARF_STRESS_MS` changes the sample duration. Optional component
timing and screenshot flags are documented in the harness itself.

Simulation, render, frame-interval and navigation measurements are separated.
The first 20 timing samples are excluded. Panel responsiveness is programmatic
click-to-second-animation-frame, not hardware touch latency. JavaScript heap
excludes native decoded images, canvas surfaces and GPU memory. Instrumentation
adds overhead, and the samples are not controlled before/after comparisons.

Final measurements used Chrome 155.0.8059.85 on Windows x64 with component
profiling enabled. The exact samples and metadata are in
[benchmark.json](benchmark.json).

| Metric | Desktop 1280×800 | Narrow touch 390×844 |
| --- | ---: | ---: |
| Frame interval, median / p95 | 16.7 / 18.6 ms | 16.7 / 18.7 ms |
| Simulation step, median / p95 | 1.1 / 1.9 ms | 1.1 / 1.9 ms |
| Render, median / p95 | 3.7 / 6.6 ms | 2.5 / 3.9 ms |
| Click → second animation frame | 60.3 ms | 44.2 ms |
| JavaScript heap | 40.0 MiB | 42.4 MiB |
| Regional shared-route joins | 171 | 170 |
| Queued paths at end | 0 | 0 |
| Completed production cycles | 6 | 6 |
| Material units hauled | 176 | 176 |

Both retained 100 dwarves and 100 enemies and finished all 20 rooms, including
the two initial construction projects. Eleven relevant atlases loaded with no
failures. Each viewport stayed within the terrain/effects/navigation bounds and
reported no page errors. The median frame interval is approximately 60 FPS;
the p95 results are slower and this is not a guaranteed frame rate. Effects
reached their 180-entry cap without growing further. Component timing identified
terrain rebuilds as the largest intermittent render cost (p95 3.4 / 1.8 ms).

The stress screenshots intentionally show an artificial open test cavern. Its
terrain setup clears the fortress tile as part of the fixture, so its central
legacy icon is not a missing fortress atlas in a normally generated world.
These results are not a controlled causal comparison with earlier builds.

## Release checks still required

- Upload the packaged images and set the real R2 public origin in `config.js`;
  verify CORS and image loading on the actual deployed game origin.
- Run the large-fortress workload and normal gameplay on supported midrange
  phones, including pinch/pan, thermal throttling and a prolonged session. The
  target remains 60 FPS desktop and at least 30 FPS supported mobile; browser
  emulation cannot establish the phone guarantee.
- Exercise a live encrypted Firebase slot roundtrip and representative real
  exported legacy saves before production rollout.
- Continue game balancing and longer campaign play. The current scope uses
  region-indexed route reuse rather than a hierarchical portal graph, compact
  room presets rather than arbitrary furniture editing, and lightweight
  settlement/alliance mechanics rather than a full quest/technology system.

The PR is intentionally a draft while those release checks remain. See
[LIVING-MOUNTAIN.md](LIVING-MOUNTAIN.md) for gameplay and implementation boundaries
and [ART.md](ART.md) for exact asset upload paths and artistic scope.
