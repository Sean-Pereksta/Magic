# Arcade Nexus 2.0 artwork

The effective catalog contains **68 unique entries**: 65 core entries plus the loader additions `circuitbound`, `mtgcatalog`, and `fantasyfootball`. The loader also checks `receiverwindowqb`, which already exists in the core and is counted once. Every catalog entry has an intentional graphical cover and compact thumbnail.

## Delivered collection

- 68 library covers, 640 × 360 WebP, quality 79.
- 68 compact thumbnails, 240 × 135 WebP, quality 72.
- Four cinematic flagship hero illustrations, 1280 × 720 WebP.
- One original arcade-room fallback for future or unrecognized registry entries.
- Two reusable native SVG interface assets: cat/mouse symbol silhouettes and arcade pathways.
- Synchronous JavaScript and JSON manifests, including route/mode inventory, artwork tier, source provenance, dimensions, byte counts, and SHA-256 hashes.
- Full exact generation prompts for 57 new cover/fallback assets and four flagship heroes.

The 141 WebP files total **4,700,876 bytes**; the two SVG files total **1,650 bytes**. Full-resolution generation masters are not loaded by the website.

## Artwork audit and preservation

Twelve existing illustrations were retained: the ten repository title graphics, tracked Faction Forge artwork, and the existing Cloudflare-hosted TurnCraft illustration. Their original references remain recorded in `originalImagePath` and `sourceArtwork`; the source files and Cloudflare objects were not replaced. Existing square and portrait art includes painted titles, so its library derivatives preserve the full original image on a dark landscape canvas instead of clipping lettering. The four new wide hero scenes complement the original flagship covers.

The other 56 games/tools received separate game-specific illustrations using the built-in ImageGen tool. Each illustration was requested individually; distinct games were not packed into a generated atlas. Subjects follow the registered game's description: tactical armies, magical worlds, mouse survival, sports, geography, card play, engineering, or reading tools. The new arcade-room fallback and four wide flagship heroes were also generated individually. No API-key or CLI generation fallback was used.

`lobby/assets/generation-prompts.json` records the exact cover/fallback prompts. `lobby/assets/hero-prompts.json` records the exact hero prompts. The native SVG silhouettes are original code artwork copied from the runtime's inline symbol definitions; the pathway SVG is likewise copied from the runtime. Animation uses the inline copies, avoiding an additional network request.

## Editorial curation

The manifest derives tiers from `lobby/lobby-showcase.js`: four flagships, eight premium showcase games, and twelve additional built-out experiences form the 24 strongest games. The explicitly configured five tools are separate. Entries marked Incomplete or explicitly experimental belong to the experimental tier; remaining titles default to in-production. These are editorial placement labels, not a replacement game registry. Updating a tier does not change or regenerate its artwork.

```powershell
python tooling/build-lobby-artwork.py --refresh-metadata --verify
```

## File organization

```text
lobby/assets/
  game-tiles/{existingRegistryKey}.webp
  game-thumbnails/{existingRegistryKey}.webp
  game-heroes/{flagshipRegistryKey}.webp
  game-tiles/arcade-fallback.webp
  interface/cat-mouse-sprites.svg
  interface/arcade-pathways.svg
  game-artwork.js
  game-artwork.json
  generation-prompts.json
  hero-prompts.json
```

`game-artwork.js` assigns `globalThis.GameHubArtworkManifest` synchronously. Its data matches `game-artwork.json`. The original game registry remains authoritative for gameplay and routes; the artwork manifest only describes presentation and inventories the corresponding registry entries. Known entries have `tile`, `thumb`, and `hero` paths. For non-flagship entries, `hero` points to the regular tile so future promotion remains safe.

## Verification and packaging from a clean clone

Python 3 and Pillow are needed for image verification. Generation masters and the local scratch directory are **not** needed to validate or package committed deliverables.

```powershell
python tooling/build-lobby-artwork.py --verify
python tooling/build-lobby-artwork.py --package-only --zip arcade-nexus-artwork.zip
```

Verification checks every catalog variant and the unknown-game fallback, all recorded file hashes and byte counts, WebP formats and dimensions, and valid SVG XML. Packaging runs verification before creating the ZIP. The ZIP root corresponds exactly to the configurable artwork base path; no additional `lobby/assets` nesting is included.

The build mode is intended for an art-authoring workspace containing the generation masters and retained source art:

```powershell
python tooling/build-lobby-artwork.py --source-dir work/art-source --source-map work/art-source/generated-current.json --hero-source ../hero-art
```

The source directory contains `registry.json`, the retained images named in the build script, and a source-map JSON array of `{key,path,prompt}` records. `--hero-source` points to the four optimized hero files and `hero-prompts.json`. A clean deployment uses the committed derivatives directly; it does not run art generation.

## Cloudflare R2 integration

1. Extract the artwork ZIP and upload its contents under a versioned object prefix, for example `arcade/nexus-v2/`.
2. Set `assetBase` in `lobby/lobby-showcase.js` to the public HTTPS URL for that prefix. The default `/lobby/assets` serves all committed files locally.
3. Keep `game-artwork.js` available with the application scripts. Relative paths in the manifest resolve against `assetBase`; no game routes change.
4. Serve WebP as `image/webp`, SVG as `image/svg+xml`, JSON as `application/json`, and JavaScript as `application/javascript`. A versioned prefix permits long-lived caching. If artwork changes, use a new prefix or invalidate cached stable filenames.
5. Confirm each manifest object is publicly readable before changing the base URL. Existing remote game assets retain their own URLs and are independent of this package.

## Adding another game

Add the game to the existing core registry or supported loader flow. An entry without a manifest record immediately receives the original arcade-room illustration. To give it dedicated art, generate one landscape composition with important subjects in the central 70%, export tile/thumb WebPs under its exact registry key, and add a manifest record. Flagship promotion can optionally add a hero; otherwise the tile remains a valid large-art fallback. Update both serialized manifests together and rerun the artwork verification plus the catalog tests.

## Visual QA

All delivered covers were visually inspected in labeled contact sheets after WebP conversion. Checks included identifiable game-specific subject matter, coherent full scenes, unclipped existing painted titles, central subjects, readable small previews, and no generated interface/button overlays. The four new hero scenes were separately inspected at landscape size. The implementation's browser checks cover responsive rendering and network loading; artwork verification covers the actual packaged files.

## Complete catalog inventory

All rows below have both a dedicated tile and thumbnail. “Retained” means the existing illustration was preserved; “New” means a separately generated cover. The four flagship hero illustrations are new in addition to their retained library covers.

| Registry key | Display title | Category | Modes | Tier | Cover | Route |
|---|---|---|---|---|---|---|
| apestogetherstrong | Apes Together Strong | Survival | single | flagship | Retained | /game/apes-together-strong.html |
| ironthrone | Iron Thrones | Strategy | single, multi | flagship | Retained | /game/iron-throne/index.html |
| nflradio | NFL Radio Dial | Apps | single | tool | New | /game/nfl-radio.html |
| cat | Cat & Mouse | Party | multi | flagship | Retained | /game/catandmouse.html |
| grass | Grass Realms | Strategy | multi | in-production | Retained | /game/grassrealms.html |
| warrealms | War Realms | Cards | single, multi | flagship | Retained | /game/warrealms.html |
| estate | Estate Defense (Incomplete) | Strategy | multi | experimental | New | /game/estatedefense.html |
| mountain | Mountain Blade (Incomplete) | Action | single, multi | experimental | New | /game/mountainblade.html |
| mountonline | Mountain Blade Online (Incomplete) | Action | multi | experimental | New | /game/mountonline.html |
| crownlands | Crownlands Online (Incomplete) | Strategy | multi | experimental | New | /game/crownlands.html |
| rock | Rock Paper Scissors MMO (Incomplete) | Party | multi | experimental | New | /game/rock.html |
| stockkings | Stock Kings (Incomplete) | Strategy | multi | experimental | New | /game/stockkings.html |
| dutchblitz | Dutch Blitz Arena (Incomplete) | Cards | multi | experimental | New | /game/dutchblitz.html |
| splinters | Splinters (Incomplete) | Action | multi | experimental | New | /game/Splinters.html |
| spelltower | Spell Tower | Party | multi | in-production | New | /game/spelltower.html |
| zombiezone | Zombie Zone (Incomplete) | Survival | multi | experimental | New | /game/zombiezone.html |
| biblegame | Bible Game | Faith | single, multi | built-out | New | /game/biblegame.html |
| bibleroguelike | Bible Game — Roguelike | Faith | single, multi | built-out | New | /game/biblegame.html |
| historyrush | History Rush | Party | multi | in-production | New | /game/historyrush.html |
| crowncouncil | Faction Forge | Strategy | single, multi | showcase | Retained | /game/crowncouncil.html |
| thegame | The Game  (Incomplete) | Cards | multi | experimental | New | /game/thegame.html |
| matchwars | Match Wars | Arcade | single | in-production | New | /game/matchwars.html |
| catmobile | Cat vs Mouse Mobile (Incomplete) | Party | multi | experimental | New | /game/catmousemobile.html |
| flag | Flag (Incomplete) | Action | single | experimental | New | /game/Flag.html |
| microwars | Micro Wars (Incomplete) | Strategy | single | experimental | New | /game/microwars.html |
| microwars2 | Monster Quest (Incomplete) | RPG | single | experimental | New | /game/microwaves2.html |
| swarm | Swarm Wars (Incomplete) | Strategy | single | experimental | New | /game/swarms.html |
| microwars4 | Micro Wars Nexus | Strategy | single | in-production | New | /game/microwaves4.html |
| border | Border Trials | Survival | single | in-production | New | /game/border.html |
| gateguard | The Last Five | Strategy | single | built-out | New | /game/gateguard.html |
| mobamania | MOBA Mania (Incomplete) | Action | single | experimental | New | /game/CatLanes.html |
| trials | Trials Rogue-like (Incomplete) | RPG | single | experimental | Retained | /game/trials.html |
| roguecards | Tiny Troops Emoji Battle | Cards | single | built-out | New | /game/roguecard.html |
| cardrealms | Card Realms (Incomplete) | Cards | single | experimental | New | /game/cardrealms.html |
| cardrealmsmulti | Card Realms Multiplayer (Incomplete) | Cards | multi | experimental | New | /game/cardrealmsmulti.html |
| rogueworld | Rogue World | RPG | single | in-production | New | /game/rogueworld.html |
| rogueduel | Rogue Duel (Incomplete) | Cards | multi | experimental | New | /game/rogueduel.html |
| versemem | Verse Memorizer | Faith | single | tool | New | /game/verse-memorizer.html |
| pdfreader | PDF Reader | Educational | single | tool | New | /game/pdfreader.html |
| village | Village | Builder | single | in-production | New | /game/villagelobby.html |
| jesusjumper | Jesus Run | Faith | single | built-out | New | /game/jesus/ |
| castledefense | Castle Defense (Incomplete) | Strategy | multi | experimental | New | /game/castlepanic.html |
| trumprev | Base Defense: Revenge (Incomplete) | Strategy | multi | experimental | New | /game/donrevenge.html |
| warspan | Warspan | Cards | single, multi | in-production | New | /game/WarspanOnline.html |
| balatle | Balatle (Incomplete) | Cards | multi | experimental | New | /game/balatle.html |
| balatlesolo | Balatle Solo | Cards | single | in-production | New | /game/balatlesolo.html |
| dwarfworld | DwarfWorld | Builder | single | built-out | New | /game/DwarfWorld.html |
| receiverwindowqb | Receiver Window QB | Sports | single | built-out | New | /game/receiver-window-qb.html |
| arcanewilds | Arcane Wilds | RPG | single | showcase | Retained | /game/arcane-wilds.html |
| spacetyrants | Space Tyrants: Galactic Mandate | Strategy | single | showcase | Retained | /game/space-tyrants.html |
| cheesehold | Cheesehold 3D | Survival | single | built-out | New | /game/Cheesehold_3D_Progression_Upgrade.html |
| neonbreach | Neon Breach | Action | single | showcase | New | /game/neon-arena/ |
| hearthmouse | Hearthmouse | Survival | single | showcase | New | /game/hearthmouse/ |
| stoneyrelic | Stoney's Relic | RPG | multi | in-production | New | /game/stoney.html |
| starcraft2 | HTML Craft | Strategy | single | in-production | New | /game/starcraft2.html |
| knightwarfare | World Quest | Party | single | built-out | New | /game/Knight.html |
| goofywizard | Goofy Wizard | Arcade | single | in-production | New | /game/Goofy Wizard.html |
| basketball2 | Basketball 2 | Sports | single | built-out | New | /game/Basketball 2.html |
| chrisrunner | Chris Runner | Arcade | single | showcase | Retained | /game/chrisrunner.html |
| forgeflight | Forge & Flight | Action | single | in-production | New | /game/forge&flight.html |
| onehitknight | One Hit Knight | Action | single | built-out | New | /game/onehitknight.html |
| deepsea | Deep Sea | Adventure | single | in-production | New | /game/deep_sea.html |
| turncraft | TurnCraft — Shattered Front | Strategy | single, multi | showcase | Retained | /game/turncraft.html |
| gridbound | Gridbound Realms | RPG | multi | built-out | New | /game/Gridbound_RPG_MMO.html |
| chesswarlord | Chess Warlord | Strategy | multi | showcase | Retained | /game/chess_warlord.html |
| circuitbound | Circuitbound | Builder | single | in-production | New | /game/circuitbound.html |
| mtgcatalog | MTG Catalog & Deck Builder | Apps | single | tool | New | /game/MTG_Catalog_Deck_Builder.html |
| fantasyfootball | Fantasy Football Command Center | Apps | single | tool | New | /game/Fantasy_Draft_Command_Center.html |
