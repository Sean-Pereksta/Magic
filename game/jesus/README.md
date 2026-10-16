# Jesus Runner — Stage Two

A static browser platformer using the completed Stage One artwork. Journey follows six long worlds, with automatic checkpoint saves and one guardian trial at each world's end. Endless rotates the same biomes through shorter, growing runs and adds a guardian every third segment. Challenge revisits use three hearts. The menus and HUD keep the active controls, health, collectibles, and progress visible.

| Journey world | Generated width | Permanent reward | Guardian |
| --- | ---: | --- | --- |
| Galilee Countryside | 217,600 px | Sandals | War Beast |
| Judean Wilderness | 243,200 px | Shield | Pit Lord |
| Ancient City | 307,200 px | Holy Light | War Beast |
| Mountain Pass | 320,000 px | Angel Wings | Celestial Trial |
| Ancient Ruins | 332,800 px | Bread | Temple Judge |
| Storm Highlands | 345,600 px | Fire Rain | Celestial Trial |

Worlds combine 36 authored traversal modules with deterministic seeded selection. Critical routes work with base movement; powers open optional opportunities. Modules include upper branches, lower secret chambers with return stairs, combat courts, pressure switches and reward gates, slopes, moving lifts, recovering collapse platforms, and chase sections. Biome hazards include brambles, water, flame jets, descending rocks, wind, and lightning. Sixteen enemy species use detection, patrol/pursuit, readable attack tells, projectiles, and differing weaknesses. The four guardians have three phases and guaranteed vulnerability windows.

The length budget aims for roughly 10–15 minutes in early worlds and 15–25 minutes later. These are design estimates, not measured human completion times; exploration, upgrades, combat, retries, and player skill change duration. Record time targets are configured separately in `js/world/journey.js`.

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

The public image base is [Cloudflare R2](https://pub-3219e1df016149619783f45bf63280c8.r2.dev). `js/assets/catalog.js` contains compact local metadata for all 263 Stage One assets, including 1,429 exact source frames, 301 animation clips, timings, pivots, and colliders. `art-anchors.js` and `terrain-textures.js` contain measured source content/placement data. Runtime images come from R2; local metadata avoids fetching cross-origin manifest JSON. Preserve the uploaded directory structure when changing `ASSET_BASE_URL`.

The loader requests the current biome, its enemy pool, required actions/effects, and applicable hazards. Guardian art is prefetched near the final approach and loaded for its arena. New pickup actions are prepared as chunks enter residency. Loading screens report progress and allow retries after image failures. Shared player/UI/power images are retained; previous biome and unused enemy images are released during level changes. The renderer culls distant objects, caches a bounded set of terrain cells, and adjusts visual quality to frame cost. Four world chunks remain resident regardless of logical level length. Checkpoint shrines, ground pickups, and props use actual surface heights and source alpha bounds. Stone, bridge, moving, and collapsing platform artwork meets the landing plane with a small contact inset; bridges and rear supports dress ledges without changing collision routes.

**Run and deploy**

Serve this directory over HTTP using any static server. No installation, build step, application server, or new backend is required. For example, with Python available:

```sh
python -m http.server 8080
```

Open `http://localhost:8080/`. For production, publish this directory's `index.html`, `styles.css`, and `js/` tree together on a static host, preserving relative paths and JavaScript module MIME types. The graphics URL must remain publicly reachable. The default game stores progress and records in the browser; the existing score adapter can use Firebase only if a host already provides an initialized Firebase instance. Firebase is not loaded or required by this application.

Journey checkpoints preserve the seeded world, checkpoint player resources, collected pickup IDs, defeated enemy IDs, activated switches/checkpoints, and run statistics. Death returns to the saved checkpoint. Browser storage uses `jesusRunnerJourneyV3`; existing `jesusRunJourneyV2` progress and recognized permanent powers migrate to the six-world campaign. Old checkpoint geometry is discarded during migration. Key bindings and Endless scores use separate local storage keys. Saves are local to the browser and origin; blocked or corrupt storage falls back safely, with no account synchronization.

**Verification**

With a recent Node.js installation, run all nine dependency-free core suites:

```sh
npm test
```

- `validate-content.mjs`: authored routes, generated region/tier combinations, guardian phases and exposure, preserved attack targets.
- `world-platforming.mjs`: real-controller reachability, ability availability, all six world lengths, deterministic IDs, 1,440 biome/seed/difficulty/power combinations including final arena joins, bounded residency, secret totals, support/ground alignment, checkpoint serialization, movement and environmental mechanics.
- `assets-runtime.mjs`: exact frame geometry/timing, alpha-ground anchors, biome-selective plans, request deduplication/retry/cancellation, texture retention, rendered source rectangles, and visible culling. When Stage One source files exist in the authoring workspace, it additionally compares manifests and verifies PNG alpha measurements using the bundled Sharp runtime; these optional authoring files are not deployment dependencies.
- `combat-overhaul.mjs`: all enemy species, telegraphs, weaknesses, projectiles, blessings, guardian attack routines, and vulnerability behavior.
- `boss-simulation.mjs`: the original completable-guardian and movement regression simulations.
- `progress-save.mjs`: progression, records, legacy migration, corrupt/blocked storage, and configurable key conflicts.
- `touch-controls.mjs`: independent simultaneous pointers, partial release/cancellation, keyboard/touch coexistence, jump/dash combinations, safe layout bounds, and portrait/landscape preferences.
- `pickup-playstyles.mjs`: real double/triple jumps, boost refresh and expiry, wind movement and dash changes, fire-to-light switching, main-route supplies, asset plans, and checkpoint persistence.
- `endurance.mjs`: 144,000 shipped fixed steps, totaling 20 simulated minutes across six biomes, with bounded live entities/projectile pools/effects and collection/death restoration checks.

The endurance harness deliberately relocates an invulnerable scripted actor to visit chunks and encounters. It exercises sustained simulation and persistence; it does not demonstrate a human completing the campaign or establish a device frame-rate guarantee. Browser layout, touch behavior, graphics delivery, and performance should also be checked on the intended hosting origin and target devices.
