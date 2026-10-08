# Illustrated visual overhaul

The standalone game now consumes original PNG sprite atlases for the crowned
King, every existing ape species, human soldiers, forests, vegetation, terrain,
military structures, equipment and combat effects. The build embeds those same
files, so the downloaded game runs offline with its existing music and saves.

## Architecture and preserved behavior

The existing Canvas 2D renderer, isometric projection, depth sort, viewport
culling, bounded navigation and procedural world were retained. The new
`character-art.js`, `environment-art.js` and `equipment-art.js` extend the
presentation layer. `visual-assets.js` loads embedded artwork and provides a
recoverable earlier-art fallback if an image cannot decode. `graphics.js` owns
the four presentation presets.

No movement, collision footprint, damage value, AI rule, population limit,
reinforcement budget, input binding, procedural layout or save version was
changed for the overhaul. Three muzzle-effect creation sites now include the
existing firing direction and elevation as visual metadata. Combat and projectile
timing are unchanged. The source remains compatible with version-1 saves;
use Export/Import when moving a run between a hosted origin and the local HTML.

## Visible changes

- Real frame-based King animation in eight directions through five authored
  views and mirroring. Laurel artwork is part of each King frame. Reign-tier
  jewels remain visible without detaching the crown from the head.
- Six distinct ape families retain their own body proportions, gait cadence,
  fur palettes and roles. Shared coat atlases, individual phases and occasional
  scars introduce variation without one texture per actor.
- The character library contains 172 individually cropped authored figures
  across seven original atlases. Gait playback switches physical limb poses,
  including advancing combat soldiers. Planted-foot anchors and tight contact
  shadows prevent a detached floating silhouette; breathing keeps the feet fixed.
- Human patrol, rifle, heavy and scout art families share pose sheets; existing
  specialist roles retain equipment and awareness overlays. Muzzle direction,
  elevated guards, recoil and fallen bodies use the existing combat state.
- Forest trees, moss rocks, bushes, ferns, litter, terrain materials, buildings,
  cages, towers and rubble use original illustrated art. Foreground trees fade
  over the King and a bounded group of selected apes.
- Cage occupants stand on the illustrated interior floor, are clipped beneath
  the roof and inside the walls, and render behind the front bars and door.
  Prison padlocks align with that door rather than covering a second cage frame.
- Walls connect along both isometric axes. World-aligned repeating timber and
  stone materials continue across adjacent sections; exposed ends receive caps
  and intersecting corners share a post inside their actual footprint. Gate
  posts follow the correct axis. Open and destroyed gates retain their gap, and
  real service lanes are never visually bridged. Damage stages and a fixed pool
  of short-lived debris respond to real structure damage.
- Searchlight scattering is clipped to the existing obstruction polygons.
  Night mist, fire, smoke, sparks, flashes and dust are bounded and adjustable.
- Carried oak logs show three painted durability stages, driven by their real
  shield health. Their absorption, breaking and balance remain unchanged.
- The title scene uses the same in-game art. Settings can be opened from the
  main menu. Low, Medium, High and Ultra persist; reduced motion also suppresses
  interface transitions. Touch menu buttons retain accessible hit areas.
- Small character portraits and textured controls unify the HUD. The default
  desktop command dock presents seven frequent actions; its expander reveals
  the remaining commands and every existing hotkey still works. The village
  finder shows destination names, with the nearest village marked explicitly.
- A launch loading screen displays real per-atlas progress. Play and Continue
  wait for loading to complete; failed artwork produces a clear fallback notice.

## Production pipeline

See [the asset guide](assets/visual/README.md) for projection, anchors, manifests,
animation identifiers, original prompts and steps for adding artwork. Every
atlas is local. Generated originals and metadata are included in the sprite ZIP.

Build and create a complete offline release with Node's standard library:

```sh
node game/apes-together-strong/build.cjs
node game/apes-together-strong/build.cjs --check
node game/apes-together-strong/package-visuals.cjs /path/to/output
```

The update ZIP includes the playable HTML, source modules, original assets,
manifests, documentation, tests and packaging script. The separate sprite ZIP
contains the same original asset library for reuse.

## Verification

All 417 simulation and visual regression tests pass. Real headless Chrome checks
cover all 14 decoded local atlases, four saved quality presets, keyboard movement,
touch controls, reduced motion, failure recovery, progress-bar launch gating,
compact command expansion and named village bearings. Repeated renders and
quality changes preserve entity positions, health and collision data. Source
bounds, grounded feet, cage interior placement and front-bar layering are checked
separately. Animation evidence checks actual changes in painted limb poses across
every ape species and human family in all facing sectors.

Performance was measured in Chrome 154 at 1440×900, DPR 1, for 120 frames per
scenario, with the same deterministic stress setup before and after the change.
Combat benchmarks keep health high to preserve the declared workload; these are
stress scenes, not promises about every device or ordinary gameplay.

| High preset workload | Previous render mean | Updated render mean | Updated total work p95 |
| --- | ---: | ---: | ---: |
| King alone in forest | 3.38 ms | 3.89 ms | 6.80 ms |
| 100 moving followers | 3.65 ms | 6.36 ms | 12.90 ms |
| 500 moving followers | 4.82 ms | 8.48 ms | 22.80 ms |
| 750 apes, 320 humans and combined arms | 22.10 ms | 25.41 ms | 56.30 ms |

All 13 high-preset scenarios retain their real populations and respect the
existing navigation/searchlight budgets. Decorative debris is capped at 96
recycled entries, and coat variation uses at most four shared atlases. The new
art adds approximately 3–4 ms of rendering work in ordinary horde scenes. The
extreme battle remains below 30 FPS in this headless run (approximately 16.7 FPS
from frame intervals). Low at DPR 1 does not eliminate that bottleneck: the
extreme case measured 59.40 ms total work p95. Further performance work is needed
before claiming a stable 30 or 60 FPS at maximum combined-arms populations.

Wall connectivity uses the existing spatial index, with at most 16 layout queries
per rendered frame and invalidation on geometry changes. Wall face textures use
a separate bounded cache (128 entries, two million pixels, four new entries per
frame), so repeated sections do not repeatedly paint every material tile.
A separate 240-module fortress run retained every wall and gate, used 64 cached
textures / 153,664 pixels, and measured 5.98 ms mean rendering and 17.20 ms total
work p95. Timings vary with startup and browser state: a fresh-browser combined
arms scenario D measured 50 ms work p95, versus 33.10 ms in the earlier sequence.

The separate browser, contact-sheet, animation recording, performance and ZIP
verification scripts are included in `tests/`. They run against the real source
or standalone bundle. ZIP verification checks every decompressed CRC and exact
byte parity with the packaged source and artwork.

## Scope limits

This is a working raster-art renderer, not a complete skeletal animation editor.
Several states share authored poses and use restrained visual transforms; the
crowd uses four facing views while the King uses eight. Soldier roles reuse four
body families with equipment overlays. Existing vehicles, aircraft, some settlement
facilities and specialist siege devices keep their detailed Canvas artwork.
Lighting uses bounded cached obstruction samples rather than per-pixel shadows.
These limitations are intentional and are not presented as newly painted assets.
