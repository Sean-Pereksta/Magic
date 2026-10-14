# TurnCraft production animation assets

The runtime reads `assets/animation-manifest.json`. Its mappings use the actual
`UNITS` and `STRUCTURES` IDs in `game/turncraft.html`: 40 original, expansion and
summoned troop types; 34 faction and neutral structure types; and the separate
`larva` and `cocoon` entity kinds. Original static graphics remain compatibility
fallbacks. Production artwork is transparent WebP hosted at
`https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/` in the top-level `units/` and `structures/` folders.
Image files are excluded from this PR; the manifests and renderer code stay in GitHub.

## Troop layout

Every troop has painted directional poses mapped to this compass order:
`N, NE, E, SE, S, SW, W, NW`. North is a rear view, south a front view, and east
and west are opposite side views. Three bilateral Brood profiles use the explicit
reflection exception described below. The master images were generated using the
existing character artwork as identity references. Running legs, claw strikes,
recoil, turret action, wing/engine motion, and collapsed death poses are actual
drawn frame changes.

| State | Frames per direction | Default fps | Loop |
| --- | ---: | ---: | --- |
| Idle | 4 | 5 | Yes |
| Move | 6 | 10 | Yes |
| Attack | 5 | 10 | No |
| Hit | 2 | 12 | No |
| Death | 5 | 8 | No |
| Ability | 2 | 6 | Yes while channeling |
| Worker activity | 6 (Probe: 4) | 8 | Yes |
| Deployment, applicable units | 6 | 8 | Timer driven |

Frames are 96 by 144 pixels, or 128 by 144 for Lattice Skiff and Void Lancer.
Core artwork is split into three sheets per troop:
`<type>-locomotion.webp` (960 by 1152), `<type>-combat.webp` (672 by 1152), and
`<type>-demise.webp` (672 by 1152). Separate `<type>-work.webp` and
`<type>-deploy.webp` sheets are 576 by 1152. No production image exceeds 2048
pixels in either dimension. The two wider craft use separate smaller state
masters with larger cells to keep their full silhouettes and weapon tips intact.

Engineer and Drone have separate six-frame tool/mining activity sheets; Probe
has four actual energy-channel poses.
Siege Tank, Lurker, Forward Fabricator and Tunnel Maw have separate six-frame
progressive deployment sheets. These do not alter worker collection, deployment
or combat timers. Attack contact metadata is zero milliseconds for existing
immediate ranged firing and 270 milliseconds for the existing melee impact.
The renderer scales animation playback to the corresponding existing visual
event duration; damage timing remains simulation controlled.

## Structures and developmental biology

All 34 structures use 512 by 512 sheets, arranged as four rows of four 128 pixel
frames: idle, production, construction and destruction. Defensive attack states
use the production row's actual firing/spine/energy poses. Construction follows
the real build progress. Frontier scaffolds assemble, Ascendant parts materialize,
and Brood bodies grow. Structure orientation is the fixed three-quarter view.

Structure idle and production loops play at 3 fps, and cosmetic destruction plays
at 3.5 fps (about 1.14 seconds), half their former playback speed. Construction
still follows real build progress, defensive attacks retain the existing contact
clock, and all troop and developmental animation timings remain unchanged.

Larva and cocoon have separate 512 by 640 sheets with four idle, two hit, five
death and six biological morph frames. Their packed positions are explicit
`coordinates.S` arrays because state boundaries cross atlas rows. Cocoon morph
progress follows its existing production timer. These kinds remain distinct
from a resulting trained unit.

## Manifest fields

An asset entry provides `kind`, `faction`, `fallback`, `anchor`, and `states`.
Each state provides `sheet`, `frameWidth`, `frameHeight`, `frames`, `directions`,
`rows`, `firstColumn`, `fps`, and `loop`. `contactMs` describes attack impact
timing; `progressDriven` marks construction, deployment or morph sequences.
Explicit irregular placements use `coordinates[direction]`, an ordered array
of `{x, y, w, h}` rectangles. Sheet paths are absolute Cloudflare URLs using the top-level `units/` and
`structures/` folders. No image URL includes a `graphics/` or `animated/` prefix. Ground anchors are approximately `[0.5, 0.78]`; air units
use `[0.5, 0.5]`. Selection uses bounded rendered-body hit positions. Commands and
collision continue to use the authoritative logical grid.

For the bilateral Drone, Lurker and Tunnel Maw, `directionFlipX` on the relevant
state supplies an opposite horizontal profile by reflecting complete genuine
animated poses. Every other view uses its painted directional row. This changes
only presentation, while keeping equipment, gait, hit/death poses and gameplay
unchanged. Optional `directionRows` in source records assigns the correct compass
labels to actual generated rows without transforming the artwork.

## Adding and rebuilding art

1. Add the real game type ID and original identity reference to
   `assets/unit-artwork-roster.json`.
2. Generate actual drawn animation frames with the built-in image-generation
   tool. Core master layout is 24 columns by eight direction rows, with four
   idle, six move, five attack, two hit, five death and two ability columns.
   Use genuine transparent alpha, stable perspective/equipment and cell pivots.
3. Record the prompt, source image location and layout in a generation record.
   The committed generation records preserve the exact prompt and input mapping.
   Their source paths describe this session's transient original outputs; archive
   newly generated masters yourself and update those paths before rebuilding.
4. Run `python game/turncraft/scripts/build-unit-atlases.py --records <records>`.
   The script only crops the explicit grid, resizes uniformly, removes tiny
   disconnected boundary scraps, and packs WebP sheets. It never creates poses
   by shifting, rotating, stretching or otherwise synthesizing a static cutout.
   Upload packed output to Cloudflare under `units/` and refresh
   `assets/visual-asset-index.json` when replacing artwork.
   Source hashes and packer version skip unchanged core sheets. Pixel data and
   JSON are written to temporary siblings and replaced atomically.
5. Run `python game/turncraft/scripts/verify-animation-assets.py` and the game's
   Node/browser tests. Python packing requires Pillow, NumPy and SciPy.

Generation used the built-in image tool throughout; no external model, API key,
or runtime image-generation service is required to play. Production WebP uses
quality 88 for troop RGB (alpha is preserved) with encoder effort 4 or 6, and
lossless structure artwork.
Unused source PNGs and reference montages are excluded from the downloadable
production asset archive.

Art verification checks complete live-type coverage, all direction/state
mappings, successful WebP decoding, texture limits, frame bounds, nonempty alpha
and distinct frame pixels. Visual inspection additionally checks body motion,
weapon recoil, death silhouettes, face direction, cropping and readability.
