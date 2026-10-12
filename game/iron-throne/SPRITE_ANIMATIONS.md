# Sprite animation integration

This is a presentation-only integration. `sprite-animations.mjs` is the source
of truth for the 21 R2 paths, dimensions, action rows, frame counts, FPS and cell
anchors. `animation-manifest.json` is its checked export. Existing static PNG,
SVG and canvas fallbacks remain in place. No new artwork is part of this PR.

## Artwork status

The production sprite artwork is **not complete**. Two generated levy drafts
failed visual acceptance: backgrounds remained visible, walking frames repeated
similar poses, and the output did not match the requested cell dimensions.
Neither draft is shipped, and no complete sprite ZIP has been produced. All 21
production sheets still require completion and visual review before the full
artwork request can be considered delivered.

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
browser test uses colored synthetic PNGs solely to check source cropping,
loading, timing and integration. It does not validate finished artwork.

The full 21-file artwork acceptance remains pending: authentic distinct action
poses, clean transparency, stable anchors, clean loops and visual readability
at map scale must be inspected on the actual sheets before distribution.

## R2 upload contract

Extract the future artwork archive and upload its `sprites/` directory to the
root of the existing bucket. Do not nest it under an archive-name directory.
Keep exact casing from `animation-manifest.json`, including `menAtArms.png`,
`heavyInfantry.png`, `veteranArcher.png`, `lightCavalry.png` and `war_canoe.png`.
Serve as `image/png`. Keep all original `troops/`, `ships/` and `construction/`
objects for fallback. No Cloudflare account or bucket change is performed by
this integration.
