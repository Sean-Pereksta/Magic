# Jesus Runner — Exploration and Sealed Passages

A static browser platformer retaining the original artwork, powers and guardians while adding the uploaded moving-platform and hazard graphics. Journey follows six compact worlds, with automatic checkpoint saves and one guardian trial at each world's end. Endless changes biome after each 6–12 chunk segment and adds a guardian every third segment. Challenge revisits use three hearts.

| Journey world | Generated width | Permanent reward | Guardian |
| --- | ---: | --- | --- |
| Galilee Countryside | 54,400 px (17 chunks) | Sandals | War Beast |
| Judean Wilderness | 60,800 px (19 chunks) | Shield | Pit Lord |
| Ancient City | 76,800 px (24 chunks) | Holy Light | War Beast |
| Mountain Pass | 80,000 px (25 chunks) | Angel Wings | Celestial Trial |
| Ancient Ruins | 83,200 px (26 chunks) | Bread | Temple Judge |
| Storm Highlands | 86,400 px (27 chunks) | Fire Rain | Celestial Trial |

Worlds combine the original 36 traversal modules with 12 new authored encounters: two for each biome. Required ferries, vertical lifts, opposite-phase transfers, pivoting bridges and restoring platforms interrupt the static route. Critical routes work with base movement; powers open optional rewards and shortcuts. Pendulums, crushers, retracting spikes, rotating beams and timed gates join the existing environmental hazards. Broad combat landings, pressure switches, recovering collapsing bridges, secrets and calm checkpoint approaches vary the rhythm. Sixteen enemy species and all four three-phase guardians are retained.

Journey length budgets are halved again; time-record targets are now 225, 263, 300, 338, 363 and 388 seconds. Checkpoints occur every five chunks and at the guardian approach. Existing records and unlocks retain their storage keys. Endless starts at six chunks, adds one chunk every three completed segments, and caps at twelve; difficulty also grows through mechanics and timing. Duration still depends on exploration, waiting, combat, retries and player skill.

New runs interleave running and moving-platform sections with two spaced sealed-passage encounters per Journey region. Eight chamber designs include rising ledges, paired elevators, wall-jump shafts, recovering collapse platforms, guardian balconies, descending caves, suspended platforms and alternate routes. All mandatory movement uses the ordinary controls, including wall jumps, without requiring earned powers. Approximately 60% of encounters receive a regional elite guardian; some hold the item until defeated, while others can be avoided. Keys, relics, pressure plates and switches activate only their matching passage, with occasional multiple mechanisms.

Mandatory gates are continuous solid colliders joined to a stone ceiling and floor. Their collision remains sealed throughout the opening animation; stone slabs, boulders, temple doors, fortress doors, rubble, cave seals and iron bars retract or crumble after activation. Matching symbols, a directional light trail and a brief approach message lead toward each mechanism. Camera anticipation reveals upper landings and lower caves without adding controls.

Endless displays a live, centered meter counter. Ten world units equal one meter, consistent with historical scores. Each segment and guardian arena starts tracking from its actual spawn coordinate; only new forward progress adds distance. Backtracking, biome changes and arena transfers add no artificial distance. The result screen includes total meters, regions reached, enemies defeated, collectibles, previous best and a personal-best indicator. Best distance is stored in the existing Journey records, and old local Endless scores remain available and seed the previous best.

Each authored dynamic route is replayed using the shipped fixed-step controller at four initial mechanism phases, including waiting, riding, hazards and transfers. Removing movers must break its route even when a basic dash is available. Generation selects among validated modules; stable shared seams connect them. Controller/geometry-fingerprinted proof data avoids running long simulations while a biome loads; changed content falls back to live validation. Rebuild that development-only catalog with `node tests/build-route-proofs.mjs` after changing physics or rooms, then run the suites below.

**Controls**

| Action | Default keyboard | Touch |
| --- | --- | --- |
| Move | A / D or left / right arrows | Left movement pad |
| Jump | Space, W, or up arrow | Jump button |
| Dash | Shift or X | Dash button |
| Blessing | Q or E | One contextual blessing button |
| Look below | Hold S / down arrow | Drag the movement pad down |
| Drop through a ledge | Hold S / down arrow and jump | Drag pad down and jump |
| Pause | Escape / P or pause button | Pause button |
| Debug overlay | F1 | — |

Keyboard bindings can be changed in Controls. Choose **Arrange touch controls** there (also available from Pause → Controls) to drag the move pad, jump, dash, and blessing controls and resize each one. Done saves separate portrait/landscape layouts; Cancel keeps the previous layout, and Reset layout restores the current orientation. Controls remain within safe screen edges and do not overlap. Each finger holds its own control, including two fingers on one button; releasing or cancelling one touch does not release the others. Jump and dash can be combined without losing upward momentum. Hold jump for greater height; release for a shorter jump. The camera frames lower landing surfaces before a drop; hold Down to look farther while keeping the player visible. Moving platforms travel 400 px horizontally or 280 px vertically from end to end. Push against a wall to slide, then jump away. Wings add a second jump and let held jump slow a descent. Stomp enemies or use a blessing during an opening. Light regenerates, shields recharge after blocking, and bread heals immediately and enables slow recovery. The single blessing action uses collected Fire Rain charges first, then returns to regenerating Holy Light. Out-of-range casts preserve charges. Touch input supports simultaneous movement and action pointers, and clears held actions on cancellation, blur, or pause.

Water pickups grant one extra air jump for 45 seconds (in addition to Wings, if owned). Wind sandals boost run and dash speed by 20% and shorten dash recovery for 30 seconds. Fire pickups refill three casts. Each chunk supplies a movement pickup and fire on the main route, alongside the original health, light, and secret rewards. A small status line shows remaining boosts and fire charges; no controls are added. Timers pause with play, refresh without stacking, and restore with the checkpoint. An available water jump remains usable until landing if its timer runs out in midair.

**Artwork and streaming**

The public image base is [Cloudflare R2](https://pub-3219e1df016149619783f45bf63280c8.r2.dev). The original catalog retains 263 assets, 1,429 frames and 301 clips. `js/assets/dynamic-catalog.js` adds all 15 uploaded PNGs and 88 explicitly cropped frames with measured origins/contact anchors. The six biome moving-platform PNGs are **directly under `platforms/`**, alongside collapsing stone and unstable wooden bridge sheets; there is no `platforms/moving/` URL. Other new sheets remain in `hazards/`, `effects/` and `ui/`. Native source sizes are preserved, including the 2,171-pixel-wide mountain sheet; frame widths must not be inferred from a generic grid.

New hazard frames are selected from the same simulation state that drives warnings, motion and damage. Platform placement uses its actual standing plane, including pivot endpoints. Original artwork remains available as a fallback if an upgrade image cannot load. The new sheets load in the background and cannot trap the player on an artwork-loading screen. Runtime uses local metadata rather than fetching a cross-origin JSON manifest.

The exploration upgrade adds no required image files or remote URL changes. Gates, chamber lighting, lock symbols and elite accents use bounded procedural drawing alongside the existing R2 terrain, enemy, collectible and effect sheets. There is no additional graphics ZIP to upload and no asset switch required before deployment.

The loader requests the current biome, its enemy pool, required actions/effects, and applicable hazards. Guardian art is prefetched near the final approach and loaded for its arena. New pickup actions are prepared as chunks enter residency. Loading screens report progress and allow retries after image failures. Shared player/UI/power images are retained; previous biome and unused enemy images are released during level changes. The renderer culls distant objects, caches a bounded set of terrain cells, and adjusts visual quality to frame cost. Four world chunks remain resident regardless of logical level length. Checkpoint shrines, ground pickups, and props use actual surface heights and source alpha bounds. Stone, bridge, moving, and collapsing platform artwork meets the landing plane with a small contact inset; bridges and rear supports dress ledges without changing collision routes.

**Run and deploy**

Serve this directory over HTTP using any static server. No installation, build step, application server, or new backend is required. For example, with Python available:

```sh
python -m http.server 8080
```

Open `http://localhost:8080/`. For production, publish this directory's `index.html`, `styles.css`, and `js/` tree together on a static host, preserving relative paths and JavaScript module MIME types. The graphics URL must remain publicly reachable. The default game stores progress and records in the browser; the existing score adapter can use Firebase only if a host already provides an initialized Firebase instance. Firebase is not loaded or required by this application.

Journey checkpoints preserve the seeded world, checkpoint player resources, collected pickup IDs, defeated enemy IDs, activated switches/checkpoints, mechanism clock, collapse/recovery states and run statistics. Gate mechanisms share those scoped activated IDs, so collecting one key cannot affect another gate. Streamed movers are sampled at that clock without a first-frame teleport. Collapsed paths recover while unloaded. Layout versions 1 and 2 retain their original lengths, generation, item identities and geometry for existing checkpoints; new runs use exploration layout 3. Death returns to the saved checkpoint. Browser storage still uses `jesusRunnerJourneyV3`; existing `jesusRunJourneyV2` progress and recognized permanent powers migrate to the six-world campaign. Old V2 checkpoint geometry is discarded during that migration. Key bindings and Endless scores retain their separate keys. Saves remain local to the browser and origin.

**Verification**

With a recent Node.js installation, run the dependency-free regression suites:

```sh
npm test
```

- `validate-content.mjs`: authored routes, generated region/tier combinations, guardian phases and exposure, preserved attack targets.
- `dynamic-modules.mjs`: all twelve new routes at four initial phases, rejection of static dash bypasses, calm seams, combat landings, pressure gate and recovering wooden bridge.
- `dynamic-platforming.mjs`: required platform riding, pivot contact, timed solidity, swept collisions, hazard state/damage synchronization and controller regressions.
- `pacing-streaming.mjs`: Journey/Endless lengths, proof-catalog fingerprints, late legacy checkpoint support, saved clocks, streamed mover continuity and unloaded collapse recovery.
- `world-platforming.mjs`: real-controller reachability, ability availability, all six world lengths, deterministic IDs, 1,440 biome/seed/difficulty/power combinations including final arena joins, bounded residency, secret totals, support/ground alignment, checkpoint serialization, movement and environmental mechanics.
- `assets-runtime.mjs`: exact frame geometry/timing, alpha-ground anchors, biome-selective plans, request deduplication/retry/cancellation, texture retention, rendered source rectangles, and visible culling. When Stage One source files exist in the authoring workspace, it additionally compares manifests and verifies PNG alpha measurements using the bundled Sharp runtime; these optional authoring files are not deployment dependencies.
- `combat-overhaul.mjs`: all enemy species, telegraphs, weaknesses, projectiles, blessings, guardian attack routines, and vulnerability behavior.
- `boss-simulation.mjs`: the original completable-guardian and movement regression simulations.
- `progress-save.mjs`: progression, records, legacy migration, corrupt/blocked storage, and configurable key conflicts.
- `touch-controls.mjs`: independent simultaneous pointers, partial release/cancellation, keyboard/touch coexistence, jump/dash combinations, safe layout bounds, and portrait/landscape preferences.
- `pickup-playstyles.mjs`: real double/triple jumps, boost refresh and expiry, wind movement and dash changes, fire-to-light switching, main-route supplies, asset plans, and checkpoint persistence.
- `endurance.mjs`: 144,000 shipped fixed steps, totaling 20 simulated minutes across six biomes, with bounded live entities/projectile pools/effects and collection/death restoration checks.

The additional exploration suites exercise sealed barriers, required mechanisms, gate-specific identity, complete chamber routes, moving lifts, checkpoint restoration and Endless distance/record behavior. `legacy-layouts.mjs` compares 24 full streamed-level geometry fingerprints captured from the previous main commit, covering both prior layouts in all six biomes. GitHub Actions runs the game suites for changes under this directory.

The endurance harness deliberately relocates an invulnerable scripted actor to visit chunks and encounters. It exercises sustained simulation and persistence; it does not demonstrate a human completing the campaign or establish a device frame-rate guarantee. Browser layout, touch behavior, graphics delivery, and performance should also be checked on the intended hosting origin and target devices.
