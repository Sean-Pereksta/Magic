# Living Mountain artwork

The illustrated artwork is included in this GitHub repository and loads from
the same origin as the game by default. All 16 optimized WebP atlases are under
`game/dwarf-world/assets/living-mountain-v1/`; publish that directory with the
game's other static files. No separate artwork upload or Cloudflare setup is
required to use the illustrated graphics.

`game/dwarf-world/config.js` resolves its default `assetBase` with
`new URL('./assets/', scriptURL).href` and uses `assetPrefix: 'living-mountain-v1/'`.
This works when the repository is served from a subdirectory, including a
GitHub Pages project path. For example, `dwarves/actions.webp` resolves beside
the game as `game/dwarf-world/assets/living-mountain-v1/dwarves/actions.webp`.

## Included asset paths

These paths are relative to `game/dwarf-world/assets/living-mountain-v1/`:

| Directory | Files |
| --- | --- |
| `terrain/` | `ancient.webp`, `fungal.webp`, `crystal.webp`, `volcanic.webp`, `ruins.webp`, `abyssal.webp` |
| `dwarves/` | `actions.webp`, `professions.webp`, `equipment.webp` |
| `enemies/` | `inhabitants.webp` |
| `buildings/` | `structures.webp` |
| `interiors/` | `furniture.webp` |
| `decorations/` | `caverns.webp` |
| `effects/` | `activity.webp` |
| `ui/` | `commands.webp` |
| `lighting/` | `soft-pools.webp` |

`generation-prompts.json` and `verification.json` are also included with the
repository assets. The game reads its version-controlled manifest at
`game/dwarf-world/art-manifest.json`, which records each image's dimensions,
byte count, SHA-256 checksum and named frame rectangles.

An embedding page can instead set `window.DWARF_ART_CONFIG` **before** loading
`config.js`; supplied values override the defaults. This also supports local QA
using `assetBase: location.origin` and a locally served `assetPrefix`.

## Optional Cloudflare R2 hosting

The separate `DwarfWorld-Living-Mountain-Art-v1.zip` remains available for
optional external hosting. Extract its contents and upload the nine image
directories under the R2 object prefix `dwarf-world/living-mountain-v1/`.
For example, the dwarf atlas object key is
`dwarf-world/living-mountain-v1/dwarves/actions.webp`.

To use that optional host, override `assetBase` with the bucket's actual public
custom-domain origin or public R2 development origin, and set `assetPrefix`
to `dwarf-world/living-mountain-v1/`. The loader uses the configuration's
`assetPrefix`; the repository manifest records the default local layout.
No public R2 URL has been provisioned by this project. An explicitly empty
`assetBase`, or `enabled: false`, disables requests and retains fallback graphics.

Serve images with `Content-Type: image/webp`. Configure R2 CORS for `GET` and
`HEAD` from the game's real origin; `Access-Control-Allow-Origin: *` also works
for public, credential-free game artwork. The image loader uses anonymous CORS.
Use `Cache-Control: public, max-age=31536000, immutable` for this versioned
prefix. Change the prefix and manifest together when replacing art in future.
No new game server, Node.js runtime, or backend is required.

## Contents and quality

There are **16 WebP atlases, 248 named frames and 22 authored animation
sequences**. Every runtime frame uses a 128×128 cell, with six-column character
and building sheets and separate eight-column terrain strips. Pixel dimensions,
byte counts, frame rectangles, animation frame names, frame rates and SHA-256
checksums are recorded in the manifest. All entity and furnishing atlases retain
alpha transparency. Terrain fills its cells.

The six biomes contain floor variants, wall tops, directional edges, corners,
ore and decorative floors. Dwarves have separate walking, mining, melee,
shooting, smithing, hauling, eating, drinking, sleeping, resting, construction,
celebration, farming, cooking, healing, engineering, exploring and soldier walk
poses. Six additional armor silhouettes cover cloth through relic armor.
Sixteen equipment overlays display equipped armor, tools and weapons during
active poses; a stable cloak ribbon color distinguishes dwarf identities.
The building sheet includes every existing civic building and defensive tower,
the Forge, Depot, fortress keep, gate, barricade, rail and upgraded Great Hall.
Enemy silhouettes include all legacy types, five bosses, and faction roles.

Art was generated with OpenAI's built-in image-generation tool, visually
inspected, and repacked without duplicating action poses. Source-sheet cells
were trimmed using visible alpha bounds, given a six-pixel guard band,
normalized to 128-pixel cells and encoded as WebP quality 85. Each dwarf action
triplet shares one scale to avoid resizing between poses. This repacking is for sprite
alignment and delivery; it does not synthesize animation from repeated crops.
The exact generation prompt set is included in the ZIP.

### Current artistic limits

- Characters use one painted view with horizontal facing, not eight authored
  viewing directions. Faces and clothing differ by profession, rather than
  providing a full independently composited beard/face/gear system.
- Armor tiers change idle silhouettes and active armor/weapon overlays. Work
  and combat have dedicated tool poses, but each action does not have a fully
  repainted image for every armor and weapon permutation.
- Enemies have distinct single poses with smooth world movement and restrained
  bobbing; they do not have fully authored walk/attack/death sequences.
- The Great Hall has a separately painted upgrade. Other buildings display
  rank accents and slight scale changes rather than unique art at every tier.
- The terrain edge/corner set is intentionally compact, not a complete
  47-tile Wang transition set. Some illustrated sprites retain a mild
  three-quarter perspective while the world and navigation stay overhead.

These limitations are also machine-readable in the manifest and must not be
described as a completed combinatorial animation or equipment system.

## Runtime integration and loading

The shell loads `config.js` before the expansion loader. While the loading
screen is visible, `DwarfWorldArtLoader.preloadEssentials()` requests ancient
terrain, dwarf actions, buildings, enemies and command icons. The total
essential preload wait is bounded by `timeoutMs` (4.5 seconds by default).
The shell remains playable if any or all artwork fails.

The iframe reuses the shell's image cache. Other biome terrain is requested
only as it enters the visible terrain cache; professions, interiors, light
textures and effects stream after essential preload. Decorations stream when
a visible tile needs them. Successful loads increment the artwork revision,
which invalidates terrain chunks once so newly available art is visible.
Duplicate requests coalesce; failed atlases do not cause retry storms.

`living-art-runtime.txt` runs after the simulation/world extensions and before
the interface extension. It supplies `window.DWARF_ART` with:

- `ready`, `preloadEssentials()`, `status()` and `revision` for loading/QA;
- `drawTerrain(ctx, tileType, worldX, worldY, size, pixelX, pixelY)`;
- `drawBuilding(ctx, type, building, position, size)`;
- `drawInterior(ctx, room, position, size)`;
- `drawInfrastructure(ctx, infrastructure, position, size)`;
- `drawFrame(ctx, name, x, y, width, height)` and `decorateIcons(root)`.

Draw methods return false when art is unavailable so core graphics can render.
Dwarf and enemy sprite functions preserve their previous renderer as fallback.
Furniture follows actual room types; dwarf poses follow the living simulation's
current activity and walking route. Existing health bars and selection remain.

Lighting consists of cached painted textures, not per-light calculated
gradients. Activity effects are atlas frames with no extra particle entities.
Visible light/effect draws are capped (desktop 16/10, coarse pointer 6/4 per
approximately one render interval), and reduced-motion preferences disable
animation and activity effects. Terrain decorations are baked into chunks.

## Verification and reauthoring

Run `node --test game/dwarf-world/living-art.test.mjs` for loading and fallback
coverage. Serve the repository normally to exercise the default artwork URLs;
inspect `DWARF_ART.status()` to confirm actual illustrated assets were active
during measurement. The stress harness can also serve the unpacked ZIP at a
local route and inject an optional config override before the page loads.

`scripts/package-dwarf-world-art.py SOURCE_INDEX.json OUTPUT_DIR` repacks source
PNGs using Pillow. `SOURCE_INDEX.json` maps the twelve source names (`terrain`,
`floors`, `dwarves`, `professions`, `equipment`, `buildings`, `enemies`, `interiors`, `decorations`,
`effects`, `ui`, `lighting`) to source PNG paths. The separate calm floor sheet
replaces two columns of each original biome strip. Pillow is an authoring-only
tool; users do not install it to play. Keep large source PNGs and temporary
authoring output outside the Git checkout. Copy the final optimized WebPs to
`game/dwarf-world/assets/living-mountain-v1/` and update the codebase manifest
together. Verify the copied files against every manifest checksum before
publishing an updated pack.
