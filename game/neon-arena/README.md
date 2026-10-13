# Neon Breach

Dependency-free browser arena FPS with five modes, adaptive bots, weapon upgrades,
destructible building, mobile visible-target auto-fire, and local split screen.
Serve the repository root and open `/game/neon-arena/`.

## Hosted graphics

The detailed v2 `weapons/` and `characters/` models are referenced from
`https://pub-7a87605611d24e35813d8a3e5aa78af4.r2.dev` with cache version 2.
The 19 menu/HUD images are checked into `assets/ui/` in this repository and loaded
from the same origin as the game. The R2 bucket only needs the two model folders.
Failed, malformed or unsupported models use the original procedural graphics.

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
transparent or embedded-animation glTF; those files fall back safely. Named rigid
parts become shoulder/hip/head/wing/tail or weapon pivots for lightweight runtime
animation. Surface lighting approximates the supplied materials without a general
PBR engine or an external library download.

Downloads and parsed geometry are shared across actors and the menu portrait,
with two concurrent requests, a timeout, and a delayed retry for failures. GPU
buffers are cached per WebGL context. Nearby actors have animated parts; medium
distance actors share a single static draw; distant actors use cheap silhouettes.
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

The uploaded files currently respond to direct GETs, but the tested game-origin
requests lack `Access-Control-Allow-Origin`. In the Cloudflare bucket's Settings
→ CORS Policy → JSON, paste `r2-cors.json` from this directory. It allows public
read-only GET/HEAD requests from the game's origin and preview origins. Until
that policy is enabled, browser model fetches use procedural fallback.

## Controls and menu

Mode and arena cards retain the underlying selectors used by match setup. Survivor
shows its exclusive Breach Ring instead of an unusable arena selector. Settings
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

## Validation

```bash
node --test tests/neon-*.test.cjs game/neon-arena/tests/model-renderer.cjs
node tests/neon-browser.cjs
```

The browser test requires Playwright and a WebGL-capable Chromium installation.
Set `CHROMIUM_PATH` to use an existing executable. Set `NEON_ASSET_FIXTURES` to an
extracted supplied model pack for browser GLB checks; UI tests use repository art.
For the dependency-free model tests use `NEON_ASSET_SOURCE` or
`--assets /path/to/pack`. Without fixtures, the browser suite exercises procedural fallback.
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
