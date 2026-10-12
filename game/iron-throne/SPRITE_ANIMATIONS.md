# Sprite animation integration

This is a presentation-only integration. `sprite-animations.mjs` is the source
of truth for the 21 R2 paths, dimensions, action rows, frame counts, FPS and cell
anchors. `animation-manifest.json` is its checked export. Existing static PNG,
SVG and canvas fallbacks remain in place. Large PNGs are delivered separately
in `iron-thrones-animated-sprites.zip`, rather than committed to this repository.

## Artwork status

The separate artwork delivery contains all **21 transparent PNG sheets**:
15 troops, three ships and three construction stages, totaling 456 illustrated
frames. Each troop/ship action has eight frames; construction has one eight-frame
build loop per stage. The supplied unit artwork and existing construction art
were used as references for the built-in image generator. Export processing
isolates, registers, downsizes and packs the illustrated poses; it does not
synthesize animation by transforming a static source pose.

Troop sheets are 1280 × 480, ships 1536 × 480 and construction 1536 × 192.
The ZIP includes the exact manifest, upload instructions, validation report,
checksums and the generation prompts. An offline interactive preview is also
provided separately. Every path and filename case agrees with this manifest.

## Playback

- The renderer receives the same authorized knowledge view as before. It keeps
  local snapshots of visible entities, without modifying campaign objects.
- A changed tile/naval node triggers a one-second march/sailing cycle. A queued
  or blocked order alone stays idle, including after a selection change.
- Fresh battle, siege and structure events trigger a one-second attack for the
  recorded participant IDs. Existing battle effects continue independently.
- Naval events without participant IDs use an unambiguous recorded encounter
  location or target/order match. Ambiguous cases intentionally remain idle;
  wars alone never trigger an attack. Destroyed entities are not resurrected
  for playback. The existing battle effects still report their outcome.
- Reloads, campaign changes, turn rewinds and viewer changes seed event history
  without replaying old battles. Losing visibility removes local clocks.
- The dominant troop type, army/fleet grouping, map position, counts, ownership
  outlines, selection badges, cargo labels and hit boxes remain unchanged.
- Active visible `tile.project` progress selects the existing three stages.
  A visible shipyard's first active queue entry may show a small construction
  activity sprite. Completed or launch-blocked entries do not animate.

## Loading and performance

Optional atlases load lazily for visible entities through the shared image
cache; they do not extend startup preloading. Decode failures, 404s and invalid
atlas dimensions use the original art. Visible failures retry no more than
once per minute, through the same map scheduler, so later R2 uploads need no
code change. Reloading also makes a fresh attempt. Browser/CDN error caching
can delay availability until its cache expires.

Canvas draws crop one cell. Ownership outlines are prepared as small display
atlases and cached within a 16-million-pixel budget; playback does not recolor
full source images. The map has one shared sprite timeout, with 4 FPS idle,
8 FPS movement/attack and 6 FPS construction. Existing brief battle/selection
effects retain their own RAF behavior. Hidden tabs cancel animation updates;
reduced motion shows representative frames. Missing art does not create a
continuous animation loop.

## Validation

Run `npm run test:iron-throne` and
`node game/iron-throne/tests/sprite-animations-browser.mjs` with Playwright
available. Also run the existing art, fog and naval browser tests. The new
browser test defaults to colored synthetic PNGs to check source cropping,
loading, timing and integration. Set `IRON_THRONE_SPRITE_DIR` to an extracted
delivery's root (the directory containing `sprites/`) to use the real PNGs.
That mode additionally decodes all 21 sheets, draws every frame through the
asset cache, checks per-action frame uniqueness, transparency and clear cell
edges, and prepares all ownership outlines. Pixel inspection uses local copies
of the bytes; production drawing does not require R2 CORS changes.

The delivered-art browser run passed for all 456 frames and for both available
and missing artwork. Movement, combat expiry, construction completion, fog,
badges, reduced motion and hidden/resume checks passed. A local synthetic
500-army grouping sample averaged 2.24 ms/render; this is not a full campaign
benchmark. Source poses and the exported map-scale preview were visually
reviewed. Pixel uniqueness alone is not evidence of anatomically correct poses.

The earlier complete gameplay command reported 667 passing tests and a missing
shared Firebase config in the sparse checkout. After restoring the unchanged
config, the affected file's three tests passed, as did focused renderer checks.
The existing fog-browser width assertion (68 versus 64) also fails on the
unchanged baseline. Sprite-specific fog checks pass. Live Firebase multiplayer
was not exercised.

## R2 upload contract

Extract the artwork archive and upload its `sprites/` directory to the
root of the existing bucket. Do not nest it under an archive-name directory.
Keep exact casing from `animation-manifest.json`, including `menAtArms.png`,
`heavyInfantry.png`, `veteranArcher.png`, `lightCavalry.png` and `war_canoe.png`.
Serve as `image/png`. Keep all original `troops/`, `ships/` and `construction/`
objects for fallback. No Cloudflare account or bucket change is performed by
this integration.
