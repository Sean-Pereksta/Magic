# Living Wilds hosted artwork

Arcane Wilds uses the initial art pack, enemy packs 02–07, and boss pack 01 hosted at
`https://pub-982943ccc2b14477a62ef61fa83e5179.r2.dev`.
The upload has `characters/`, `enemies/`, and `terrain/` directly at its root.
The atlas catalog is bundled in `hosted-art-catalog.js`; there is no cross-origin
JSON fetch and images deliberately omit `crossOrigin`, so display works without
changing the public bucket's CORS policy. Canvas pixel export would require a
separate CORS configuration; saves do not export the canvas.

## Loading and fallback

- A room load requests the equipped armor character, the matching continent's
  terrain atlas, and the enemies actually present. Shadow Realms use their
  corrupted terrain atlas and their own creature sheets. Other continents and
  future encounters are not downloaded in advance.
- Later waves, danger encounters, and summons request their appearances when
  first drawn. Summoners also preload their summoned plants, echoes, or crystals.
- New Game, Continue, ordinary travel, continent portals, dungeon transitions,
  and Shadow Realm travel share the existing final `loadRoom` hook.
- Equipping another armor requests that appearance. Equipment previews request
  their own character or weapon catalog when opened.
- Three image downloads run at once. Current-room enemy atlases, their summon
  dependencies, terrain, and the current character are pinned; former regions
  are released on continent changes. Room changes release previous enemy pins.
  Other decoded references use an LRU limit of eight atlases on touch devices
  and twelve on desktop.
- Missing files, timeouts, unexpected image dimensions, and unsupported enemy
  appearances retain the existing procedural artwork. Failed downloads retry
  after a cooldown. No gameplay or save state waits on network requests.

`window.AW_ART_CONFIG`, set before `hosted-art.js`, can override `baseUrl`,
`stripAssetPrefix`, `enabled`, `maxParallel`, `maxCachedAtlases`, `timeoutMs`,
and `retryMs`. To move hosting, preserve the uploaded relative paths or set
`stripAssetPrefix:false` if the new host includes an `assets/` directory.

## Artwork and gameplay integration

The twelve `lw_*` enemies are registered with four types per continent. They
reuse existing AI, attack telegraphs, affinities, wave timing, and save formats;
they replace part of suitable regional squads and join reinforcement pools.
The catalog covers all 124 registered enemy and summoned-object types, plus
26 dedicated boss appearances. Forty fixed boss encounters resolve through
explicit aliases or registered names, including campaign bosses, rulers,
dungeon guardians, legacy bosses, and legendary creatures. Repeated encounters
share their matching sheet. Ten landmark dungeon guardians reuse explicitly
assigned boss sheets that match their creature or region; they do not require
additional filenames. Generated Shadow Realm bosses use their shadow creature
type. Unknown future types keep their existing renderer.

| Landmark guardian | Shared authored appearance |
| --- | --- |
| Glassmoth Grove, Crimson Rootway | The Thornheart Sovereign |
| Sapphire Well | Storm Knight Regent |
| Frozen Workshop | The Crystal Vault Guardian |
| Frostvein Mine | Ancient Frost Troll |
| Scorched Caravan, Heavens Grave | The Stonebell Colossus |
| Fallen Astrolabe | The Sunken Observatory Guardian |
| Nightmarket, Abyss Bridge | Arcane Colossus |

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
fallbacks, late waves and summon dependencies, room revisits, all registered
enemy/boss references, armor changes, death pool reuse, equipment crops, terrain
prompts, new creature regional spawning, and save/boss compatibility. The existing
cloud-save and grand-update syntax tests use `fileURLToPath` so the suite also
runs on Windows.

