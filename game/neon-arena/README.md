# Neon Breach

Dependency-free browser arena FPS with five modes, adaptive bots, weapon upgrades,
destructible building, mobile visible-target auto-fire, and local split screen.
Serve the repository root and open `/game/neon-arena/`.

## Hosted graphics

The detailed v2 `weapons/` and `characters/` models are referenced from
`https://pub-7a87605611d24e35813d8a3e5aa78af4.r2.dev` with cache version 2.
The 19 menu/HUD images are checked into `assets/ui/` in this repository and loaded
from the same origin as the game. The R2 bucket only needs the two model folders.
Required models preload before a match starts. Missing or unsupported models stop loading with a readable retry message. Primitive actor/weapon fallbacks and legacy pickup icons/circles have been removed; health, armor and tactical gear use actual GLBs.

`neon-assets.js` uses `ASSET_BASE_URL` for all 35 models and `UI_ASSET_BASE_URL`
for menu/HUD artwork. The default UI root `./assets` resolves next to the deployed
game, including deployments beneath a repository prefix. An explicitly empty UI
root disables image requests; an omitted UI root retains the previous shared-root
behavior. Failed images leave interactive HTML/CSS and icon fallbacks available.
Increment `version` when replacing art.

`neon-models.js` reads the supplied embedded, uncompressed, opaque GLB 2.0 meshes.
It supports node matrices/TRS, indexed triangles, authored smooth/crease normals
with correct node transforms, and material colors with emissive accents. The old
pack without normals retains flat shading. It does not implement textured, skinned, compressed,
transparent or embedded-animation glTF; required unsupported files produce a loading error. Named rigid
parts become shoulder/hip/head/wing/tail or weapon pivots for lightweight runtime
animation. Surface lighting approximates the supplied materials without a general
PBR engine or an external library download.

Downloads and parsed geometry are shared across actors and the menu portrait,
with two concurrent requests, a timeout, and a delayed retry for failures. GPU
buffers are cached per WebGL context. Nearby actors have animated parts; medium
distance and distant actors share static model geometry, without primitive silhouettes.
Both split views use the existing projection, clipping and world depth buffer.
First-person models retain the original weapon effects and authoritative combat.

Alien widths retain gameplay scale. Their vertical presentation fits the existing
head/body target envelope, including the unusually low boss target bands, so the
visible head can be aimed at without changing hitboxes. Bosses consequently have
a broad, crouched posture rather than a new, taller collision hull.

Before enabling hosted graphics, check the live files:

```bash
node scripts/check-neon-assets.cjs https://pub-7a87605611d24e35813d8a3e5aa78af4.r2.dev https://catnmice.com --models-only
```

The check downloads all 35 models, validates parsing, records SHA-256 values,
checks GET CORS from the game origin, and recommends `model/gltf-binary` MIME.
`application/octet-stream` is readable through the loader's ArrayBuffer path.
Omitting `--models-only` also checks a shared-host menu/HUD pack. Local tests do
not prove production CORS or GPU rendering.

All 35 hosted models passed the asset checker with CatnMice-origin CORS on
2026-10-13. `application/octet-stream` is compatible with ArrayBuffer loading;
`model/gltf-binary` remains the recommended MIME. If hosting changes, rerun the
checker. `r2-cors.json` documents the required public read policy.

## Controls and menu

Mode and arena cards retain the underlying selectors used by match setup. Survivor
shows eight graphical arenas with cleared-wave milestone unlocks. Settings
contains difficulty, bots, length, second player, graphics and controller inputs.
Cards support arrow keys, keyboard focus and controller navigation.

Touch uses a left analog movement stick, a right swipe area, icon controls for
jump/build/swap, a smaller reload control and an upgrade indicator. Tap Build to
place the assisted valid preview. Hold Build to choose Wall or Platform; pointer
cancellation never places a structure. Auto-fire retains visibility ray checks,
aim cones, and throttled target scans, with lock feedback in the crosshair.
Touch combat information is compact; the feed, help labels and narrow-screen
minimap do not occupy the play area.

Desktop retains WASD/mouse, Space jump, R reload, F swap/pickup, Tab upgrades,
right-button build preview/place, and Q build override. Controller and Player 2
shortcuts remain available. Q/controller build toggle cycles assisted, wall,
platform, then assisted again.

## Movement and building

Gravity remains 34 and normal jump impulse 23. Keyboard, touch and controller
jumps use one rising-edge request with 0.11 second coyote time and 0.14 second
buffering. Requests consume once and reset on respawn. Dash/grapple launches
cannot turn the buffer into a second jump.

Movement uses bounded time/travel substeps, swept descending foot intersections
before horizontal pushes, separate ascending head tests, rounded wall corners,
and at most 0.10 units of downward grounding assistance. Grounding never raises
the player to a distant surface.

Assisted builds infer walls on ground, platforms on elevated sides, and stacked
walls on tops. Platform grid height is its actual standing surface; edge bridges
match the supporting top exactly. One-cell nearby assistance still runs support,
occupancy, geometry, actor overlap, range, height, charge and active-build checks.
Valid holograms use cyan/purple; invalid holograms use muted red and a short reason.
Destruction and structural collapse remain in the simulation.

## Vanguard character rebuild

All seven player/bot operators now use original, bulky armored GLBs with broad
pauldrons, reinforced limbs, heavy boots and emissive visors. Human bodies render
at 2.39 units (about 15% taller than the former 2.08); ornaments retain their
authored size. Soles, animation pivots and head/chest aim points come from each
model's embedded rig metadata. Movement/camera height stays at 1.86.

The Character Locker appears in every mode. Breacher and Sentinel are starters.
Survivor clears unlock Bulwark (5), Ronin (10), Warden (15), Revenant (25),
Praetorian (40), and Overlord (50). Fortress Defender, Sky Hunter and Boss Slayer
offer alternative unlocks for Warden, Revenant and Praetorian respectively.
Choices persist with existing progression, including backup recovery and profile
sync, and change appearance without granting passive stats.

Upload the character ZIP's `characters/v3/` folder to the existing R2 bucket,
preserving those exact object keys. The default configuration resolves rebuilt
humans and portraits under that prefix; existing weapons and aliens keep their
original keys. The same 13 GLBs and WebP portraits ship in `assets/characters/`
as a fallback for missing files, failed CORS requests and timeouts. A failed
remote request uses a fresh local request; both sources failing still reports a
loading error. Only the equipped unlock is included in match preloading.

Rebuild the geometry with Python's standard library:

```bash
python game/neon-arena/tools/build-characters.py
node --test game/neon-arena/tests/characters-*.test.cjs
```

`assets/characters/manifest.json` records each model's bounds, triangle count,
rig, size and SHA256, plus portrait checksums. The new GLBs are self-contained,
opaque and uncompressed; no external texture or skeletal animation dependency
is required. Portraits were captured through the actual game WebGL renderer.

## Validation commands

```bash
node --test tests/neon-*.test.cjs game/neon-arena/tests/model-renderer.cjs
node tests/neon-browser.cjs
```

The browser test requires Playwright and a WebGL-capable Chromium installation.
Set `CHROMIUM_PATH` to use an existing executable. Set `NEON_ASSET_FIXTURES` to an
extracted supplied model pack for browser GLB checks; UI tests use repository art.
For the dependency-free model tests use `NEON_ASSET_SOURCE` or
`--assets /path/to/pack`. Supply model fixtures for offline tests; model-only startup requires usable hosted assets.
`NEON_MOCK_WEBGL=1` is an explicit DOM/input/simulation-only fallback for environments
without GPU libraries; it does **not** validate actual shader rendering or depth.

Movement tests cover the wall/attached platform, bridge seam, corner, stacked
wall and small-gap course, all four authored maps, low FPS, jump buffer/coyote,
ceiling collisions and fast ability movement. Model tests cover parsing, caches,
failure handling, shared GPU resources and animation/depth state. Browser checks
cover modes, arenas, split actors, mobile controls, menu return and weapons.

Production CORS, real desktop/mobile GPU depth checks and physical phone/controller
performance measurements remain release checks. The asset URL is configured and
the direct-download binaries are checked independently. Headless simulation timing should
not be described as a phone FPS benchmark.

## Performance guardrails

Static world geometry remains in cached GPU buffers; the spatial hash bounds
collision, raycasting, support and collapse queries. Player builds remain capped
at 320 pieces. Adaptive resolution, effect/visibility budgets and bounded
particles/decals remain active. Split screen shares simulation and model downloads
while retaining separate camera, input, inventory, health, score and builds.

## Endless Breach 2.0

Eight authored arenas use repository-relative `assets/survival/manifest.json`,
64-cell padded atlases, mobile textures, curved prefab meshes and map blueprints.
Only the selected environment atlas is resident; destruction rebuilds affected
cached chunks. X/Z logical coordinates scale by 1.45 once; heights are already
world units, shared by rendering, collision, navigation and building.

Arenas unlock after clearing Waves 5, 10, 15, 20, 25, 30 and 40. Each stays endless
until the whole local party dies. Versioned local storage provides backup recovery
and bounded history. Optional profile sync reuses verified existing CatnMice lobby
authentication and updates only `neonSurvival`. It never creates/signs in an
account. Live authenticated synchronization is not covered by automated tests.

Squads have three to six tactical roles, legitimate visibility, nine-second
frozen memories, cached layered navigation, defense breaking and flank/intercept
goals. Six original named-part GLBs introduce Leaper (7), Stalker (12), Screecher
(18), Artillery (24), Lurker (28) and Warder (35). Bosses arrive every fifth wave
with combined encounters at high waves. Ordinary stat growth is capped; pressure
comes from composition, elites and reinforcement cadence. There are at most
25 live aliens and fewer than 148 queued enemies in a wave.

Buy temporary gear in the salvage shop. E deploys and C cycles the shared party
loadout; Player 2 keyboard uses Num3. Controller R3 deploys and View/Select cycles.
Touch provides Deploy/cycle buttons. Each tool has charges, cooldown, duration,
active count and durability limits. Persistent rewards grant choices and cosmetic
appearances without permanent passive stat advantages.

```bash
python game/neon-arena/assets/survival/common/source/build_species.py
python game/neon-arena/assets/survival/common/source/build_items.py
python scripts/generate-neon-survival-awards.py
node --test tests/neon-*.test.cjs game/neon-arena/tests/survival-maps.test.cjs
node game/neon-arena/tests/model-renderer.cjs
NEON_ASSET_FIXTURES=/path/to/models node tests/neon-survival-browser.cjs
NEON_ASSET_FIXTURES=/path/to/models node tests/neon-browser.cjs
```

Original source PNGs, prompts and crop metadata are retained in each tileset's
`source/`. Each 256-pixel atlas allocation contains a 240-pixel usable crop starting
8 pixels inside. Gutters extrude the nearest edge, and mipmaps are disabled to
prevent cross-cell mixing. Runtime UVs derive from stable tile manifest IDs.

Validation includes all eight maps' spawn/elevated-route connectivity, ramps,
destruction and UVs, 48 parsed GLBs, five modes, and real WebGL in desktop, mobile
and local split-screen browser configurations. Production AI simulations cover
Waves 50, 100, 200 and 100,000. Software WebGL is not a physical phone FPS benchmark.
