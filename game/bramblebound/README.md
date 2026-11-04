# Bramblebound · The Endless Road

Standalone expedition roguelite for Cat’N Mice. Open `/game/bramblebound/` through any static HTTP server; there is no build or server dependency. `/game/bramblebound.html` is a compatibility redirect.

Guide Pip through 35 regions and repeating circuits, build a loadout from 284 elemental relics, fuse ingredients, choose 64 talents and abilities, battle group encounters and guardians, and purchase permanent Legacy upgrades after each run. Four equipment slots and a searchable 400-slot pack support mouse, keyboard, and touch controls. Space pauses combat. Menus and the title screen pause the simulation.

Profiles autosave under `bramblebound.profile.v1`. Existing standalone saves remain compatible; the profile menu exports/imports JSON, including prepared starting kits. Saves are device/browser local. Corrupt saves enter recovery without overwriting the original. No account is required.

The title menu adds original generated cover art, continue/start actions, kit preparation, the handbook, profile settings, and a library link. The library registry is `lobby/lobby-core.html`; editorial placement is `lobby/lobby-showcase.js`; responsive art variants and hashes are in `lobby/assets/game-artwork.{json,js}`. The original game source was imported from the user's Bramblebound folder with expanded elemental combat and backward-compatible profile migration.

Run `npm run test:bramblebound` for combat, progression, fusion, save validation, and library integration tests. `node game/bramblebound/tests/browser.cjs` runs browser checks with Playwright installed and Chromium or an installed Edge browser. Set `BRAMBLEBOUND_PLAYWRIGHT` to a Playwright package path if it is not installed in the repo; set `BRAMBLEBOUND_SCREENSHOTS` for screenshots.

## Expanded road

The expansion adds 15 biomes, 105 enemies, 72 relics, and 30 fusion recipes. Every element has innate combat behavior. Twelve signature weapon mechanics and eight counter types support deliberate loadouts; enemy dossiers and the searchable field guide explain them. A Dewglass Vial is included in every new starting backpack so cleansing is available immediately. Held weapons now have dedicated pixel artwork, elemental trails, rarity markings, and level sockets.

Existing profiles migrate without resetting their run, gear, discoveries, or records. A saved 8- or 20-region circuit finishes before the next circuit opens all 35 regions. New expeditions use 35 regions immediately.

## Rarity and finds

Five tiers classify sustained damage, utility and signature effects: Common, Uncommon, Rare, Epic and Mythic. Their rarity power multipliers are 1×, 1.12×, 1.28×, 1.5× and 1.8×; item details include this bonus in displayed damage. Tier changes preserve existing item IDs, levels and recipes. Rare finds receive stronger ornament and auras.

The chance of at least one relic in a normal wave is now 24% (previously 16%); guardian waves have a 48% chance (previously 37%), independent of enemy headcount. Rarity is chosen before duplicate, support or ingredient bias. At the first region with all tiers unlocked, ordinary finds are 65.7% Common, 20% Uncommon, 10% Rare, 3.5% Epic and 0.8% Mythic. Guardians and later regions improve high-tier odds, shown exactly in the atlas. Fusion remains an alternate route to strong gear. Tests include 28,000 cleared waves and verify rarity frequencies, headcount normalization and owned-Mythic independence.
