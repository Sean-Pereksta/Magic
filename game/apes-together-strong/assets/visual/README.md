# Apes Together Strong — original sprite library

These are the actual raster assets consumed by the playable game. The original
PNG alpha channels are retained; artwork is generated using the built-in imagegen
tool. The exact prompts are saved in each family's directory. There are no runtime
image hotlinks, purchased stock assets, or external image dependencies.

## Coordinate and material conventions

- World projection is `screenX = (x-y)*0.8`, `screenY = (x+y)*0.42-z` before camera scale.
- Artwork uses an elevated three-quarter camera, with cool light from the upper left,
  muted forest greens, silver/charcoal fur, warm orange orangutans, and worn military materials.
- Actors are anchored at the ground contact, not at the center of their image.
  Metadata contains cropped source bounds and ground anchors. Never infer bounds
  solely by dividing a generated sheet: some poses extend beyond nominal cells.
- The crowned silverback has the dominant silhouette. Other species share world
  scale while retaining different proportions, gaits and faces.
- Image bounds are presentation bounds. They must never become collision radii,
  obstacle dimensions, navigation cells or detection cones.
- Original RGBA files retain fine fur and leaf edges. RGB in nearly transparent
  pixels is not a backdrop: composite using the alpha channel.

## Files and metadata

`characters/manifest.json` identifies character sheets, individual frame rectangles,
animation sequences, directions, timings and events. Shared sequences and mirrored
views avoid duplicating every combination of species, coat, equipment and state.
`characters/prompts.json` records the source art instructions.

`environment/manifest.json` identifies tree, undergrowth, structure, terrain-material
and effect atlases. Its frame names, rectangles and anchors are consumed directly.
Forest variations are selected deterministically from world objects; decorative
vegetation does not create obstacles. Wall materials repeat from world coordinates
across both projected axes, keeping adjacent pieces aligned. Existing collision
rectangles determine straight joins, corner posts and exposed caps. Open gates
and service lanes remain gaps. Face textures are cached separately from damaged
overlays and connectivity, keeping wall tops and climb routes aligned.

`equipment/manifest.json` contains three live log durability stages and three
additional reusable effect variants. Live shields use their existing health:
intact above 65%, chipped above 30%, badly splintered until broken. Actual shield
breakage, absorption, fragments and balance remain simulation-owned.

## Adding or replacing artwork

1. Create an original transparent PNG or WebP at the established angle and scale.
   Keep the original file and prompt/provenance beside the appropriate manifest.
2. Add its unique `id`, relative `file`, decoded `width` and `height` to that
   manifest's `atlases`. Add or update the named frames and their ground anchors.
3. Register state sequences in the character animation metadata, or use existing
   named environment/equipment frames. Test front, rear, mirrored, attacking,
   climbing, wounded and fallen appearances on a contrasting background.
4. Render through `ATSVisualAssets.get(id)`. It returns only successfully loaded
   images. Preserve the current fallback path for unavailable artwork.
5. Rebuild with `node game/apes-together-strong/build.cjs`. The build embeds the
   exact local files and all metadata in the standalone game; filenames are not
   fetched during play. It rejects duplicate atlas IDs and paths outside this folder.
6. Run the visual browser suite and simulation regressions. Review an actual
   forest, a fortress, a large horde and a touch viewport, not only an atlas gallery.

## Animation, caching and quality

`character-art.js` maps existing simulation states to shared frame sequences. Its
animation clock and visual transitions cannot deal damage or move actors. Existing
combat timers remain authoritative; visual impact poses follow those timers.
Equipment and champion overlays remain outside ordinary species caches.

`environment-art.js` caches scenery and uses bounded, time-limited visual effects.
Lighting follows the gameplay light definitions and the existing obstruction work
budget. Quality reduces decorative work without changing visibility rules.

`graphics.js` defines Low, Medium, High and Ultra. Core character sheets are retained
on every preset. Animation frequency, atmosphere, decorative foliage, effect budgets
and output resolution change independently of simulation. Reduced motion suppresses
optional visual motion. Adaptive degradation can further lower visual detail.

## Export

`node game/apes-together-strong/package-visuals.cjs <output-directory>` creates:

- `apes-together-strong-sprites.zip`: this complete asset library and its manifests.
- `apes-together-strong-visual-update.zip`: the standalone playable game, source,
  assets, documentation, build/packaging scripts and tests.
- `apes-together-strong.html`: the standalone game for direct use.

Sprite PNGs are atlas sheets, not a collection of identically sized individual
files. Import the declared frame bounds into another engine to extract sprites.
All generated originals are preserved at full resolution in the sprite ZIP.
