# TurnCraft visual and command upgrade

TurnCraft keeps its square-grid simulation and existing match rules. Painted
animation atlases replace static battlefield troop and structure art, and a
local presentation layer makes each successful grid movement travel smoothly
along its existing route. None of the new intermediate positions, animation
phases, texture caches, or death objects are sent in multiplayer snapshots.

The inventory covers 40 original/expansion/summoned troop types, 34 structure
types, and Brood's larva and cocoon stages. Troop atlases provide eight-direction
animation; structures and immobile developmental stages use the directions
appropriate to their presentation. The complete frame rectangles, playback
rates, contact markers, anchors, loops and original-art fallbacks are in
`assets/animation-manifest.json`.

| Input | Result |
| --- | --- |
| A or X, then click a valid enemy | Attack that exact target |
| A or X, then click ground within four tiles of an enemy | Attack the nearest visible enemy a selected weapon can hit |
| A or X, then click ground with no valid enemy nearby | Attack-move using the existing formation system |
| Touch: select troops, then tap ground / an enemy | Move / attack; keep the group selected |
| Touch: tap another friendly troop | Switch selection |
| Touch: hold still on another friendly troop | Add or remove it from the group |
| Hold, then drag | Select troops in a rectangle |
| Double-click or double-tap a troop | Select visible troops of that type |
| Arrow keys, W/S/D and `[` | Pan the camera |

Armed construction, mining, rally and ability targeting own their input. Panning
and pinch gestures never issue orders. Visible body hit regions follow rendered
troop positions, with bounded regions and separate airborne anchors so larger
art does not capture neighboring troops. Target commands still carry the
authoritative entity ID and retain ownership, fog and weapon validation.

`animation.js` derives states from the existing movement, attack, damage,
ability, construction and production timers. Direction hysteresis and short
direction crossfades prevent rapid facing changes. Movement cycles remain
continuous across consecutive route segments; attack contact frames follow the
existing projectile or melee contact clock. Death animations finish in bounded,
local objects after immediate logical removal. Hidden combat is never exposed
by an animation, corpse, projectile or impact accent.

Low, Medium and High graphics settings are available in setup and Controls.
They preserve every game rule and reduce cosmetic work only. Atlas decoding is
lazy, deduplicated and limited to two concurrent loads. The decoded animation
texture budgets are 48, 96 and 192 MiB respectively. Original-art fallback
images load only when needed and are reduced to at most 256 pixels. Selection
highlight membership and target IDs are cached once per rendering pass.

All six existing maps have terrain and transparent prop atlases. `terrain.js`
selects deterministic variations from the existing map seed, blends neighbor
transitions and caches static chunks. Resource amounts are excluded from those
chunks. See `TERRAIN_ANIMATION.md` and the terrain/effects manifests for paths,
state layouts, fog handling and extension instructions.

Validation uses the real inline simulation, pure presentation tests, asset
coverage/decoder checks, desktop and phone-sized Chromium tests, and mixed-army
stress scenarios with 600 units and 300 selected troops. Definition fingerprints
protect the complete original unit, structure, ability, research, difficulty,
faction and objective data. Physical-device performance and live multi-client
Firebase sessions still require a release playtest.

Run the existing checks with:

```sh
npm --prefix game/turncraft install
npm --prefix game/turncraft test
npm --prefix game/turncraft run test:warfare
```

Create the artwork ZIP with the exact paths consumed by the game:

```sh
python3 game/turncraft/tools/package-visual-assets.py --assets /tmp/TurnCraft-Animated-Artwork.zip
```

The archive contains production artwork, manifests and asset documentation.
Raw generation outputs, reference boards and browser screenshots are excluded.
Extract the archive at the repository root when moving the artwork to another
checkout. Keep existing static PNGs available for compatibility fallbacks.

## Cloudflare production artwork

The 176 new production images are hosted at `https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/`. The bucket contains exactly four top-level image folders: `units/` (129 sheets), `structures/` (34 sheets), `terrain/` (12 sheets), and `effects/` (one sheet). Image URLs never contain `graphics/turncraft/` or `animated/`. Animation and terrain manifests, renderer code, generation records, and frame metadata stay in GitHub. The image files are excluded from this PR. Previously shipped static compatibility art and menu illustrations remain at their existing locations.

`assets/visual-asset-index.json` records the expected dimensions, byte lengths, and SHA-256 digests of every production image. `npm test` first downloads and verifies those exact public URLs into a temporary directory outside the repository; it reuses only verified cached bytes. Set `TURNCRAFT_ASSET_CACHE` to choose another temporary directory. The browser regression harness serves the downloaded real artwork as isolated fixtures while retaining the actual Cloudflare URLs and normal browser security. Production image rendering does not require cross-origin pixel access.

Run `python3 scripts/cache-visual-assets.py --refresh` to recheck all public uploads. Run `python3 scripts/verify-animation-assets.py` to decode every production image and inspect all animation frames. The ZIP packager now writes images only into the same four top-level folders, and its optional code archive contains source and metadata without image files.

The existing PR review regression is also fixed: Spectate permits inspecting units and structures after the match ends, while contextual orders, armed orders, command submission, and authoritative command application remain disabled. The command-input regression covers selection, deselection, all armed tap paths, and post-match command rejection.
