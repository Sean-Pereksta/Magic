# Living Wilds hosted artwork

Arcane Wilds uses the initial art pack hosted at
`https://pub-982943ccc2b14477a62ef61fa83e5179.r2.dev`.
The upload has `characters/`, `enemies/`, and `terrain/` directly at its root.
The atlas catalog is bundled in `hosted-art-catalog.js`; there is no cross-origin
JSON fetch and images deliberately omit `crossOrigin`, so display works without
changing the public bucket's CORS policy. Canvas pixel export would require a
separate CORS configuration; saves do not export the canvas.

## Loading and fallback

- A room load requests the equipped armor character, its continent's four new
  creatures and ruler, and the matching terrain atlas. Shadow Realms use their
  corrupted terrain atlas.
- New Game, Continue, ordinary travel, continent portals, dungeon transitions,
  and Shadow Realm travel share the existing final `loadRoom` hook.
- Equipping another armor requests that appearance. Equipment previews request
  their own character or weapon catalog when opened.
- Three image downloads run at once. Current-region atlases and the current
  character are pinned; former regions are released on continent changes.
  Other decoded references use an LRU limit of eight atlases on touch devices
  and twelve on desktop.
- Missing files, timeouts, unexpected image dimensions, and unsupported enemy
  appearances retain the existing procedural artwork. Armor failures retry
  after a cooldown. No gameplay or save state waits on network requests.

`window.AW_ART_CONFIG`, set before `hosted-art.js`, can override `baseUrl`,
`stripAssetPrefix`, `enabled`, `maxParallel`, `maxCachedAtlases`, `timeoutMs`,
and `retryMs`. To move hosting, preserve the uploaded relative paths or set
`stripAssetPrefix:false` if the new host includes an `assets/` directory.

## Artwork and gameplay integration

The twelve `lw_*` enemies are registered with four types per continent. They
reuse existing AI, attack telegraphs, affinities, wave timing, and save formats;
they replace part of suitable regional squads and join reinforcement pools.
The three ruler appearances match campaign node IDs before their shared base
creature IDs. Other enemy types retain their own procedural drawings.

Each character appearance has four facings and two poses each for idle,
movement, attack, and death. Death sprites use the existing bounded presentation
death pool. Sprite rectangles and pivots are authoritative; compact animation
events do not replace existing hit timing or collisions. Existing shadows,
shields, auras, health bars, warnings, object occlusion, and drawing order remain.

Terrain substitutes only matching existing props and safe floor decoration.
It does not create new collision or hazards. Completed interactables and objects
without matching art keep their previous renderer and interaction prompts.

Armor is a complete baked character appearance. The source sheets include a
short staff and have no independent hand sockets. Weapon family artwork appears
in inventory and equipment comparisons; replacing the staff on the character
requires another art-authoring pass. The twenty spellbook illustrations describe
proposed spells and are retained in the catalog without mislabeling existing
spells or introducing new spell mechanics.

## Validation

Run `npm test --prefix game/arcane-wilds`.
Tests cover request deduplication and concurrency, initial region selection,
continent changes, image failures and retries, atlas bounds and pivots, renderer
fallbacks, armor changes, death pool reuse, equipment crops, terrain prompts,
new creature regional spawning, and save/boss compatibility. The existing
cloud-save and grand-update syntax tests use `fileURLToPath` so the suite also
runs on Windows.
