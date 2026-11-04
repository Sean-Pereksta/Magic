# Bramblebound · The Endless Road

Standalone expedition roguelite for Cat’N Mice. Open `/game/bramblebound/` through any static HTTP server; there is no build or server dependency. `/game/bramblebound.html` is a compatibility redirect.

Guide Pip through 20 regions and repeating circuits, build a loadout from 212 elemental relics, fuse ingredients, choose 64 talents and abilities, battle group encounters and guardians, and purchase permanent Legacy upgrades after each run. Four equipment slots and a searchable 400-slot pack support mouse, keyboard, and touch controls. Space pauses combat. Menus and the title screen pause the simulation.

Profiles autosave under `bramblebound.profile.v1`. Existing standalone saves remain compatible; the profile menu exports/imports JSON, including prepared starting kits. Saves are device/browser local. Corrupt saves enter recovery without overwriting the original. No account is required.

The title menu adds original generated cover art, continue/start actions, kit preparation, the handbook, profile settings, and a library link. The library registry is `lobby/lobby-core.html`; editorial placement is `lobby/lobby-showcase.js`; responsive art variants and hashes are in `lobby/assets/game-artwork.{json,js}`. The original game source was imported from the user's Bramblebound folder without changing its combat or progression rules.

Run `npm run test:bramblebound` for combat, progression, fusion, save validation, and library integration tests. `node game/bramblebound/tests/browser.cjs` runs browser checks with Playwright installed and Chromium or an installed Edge browser. Set `BRAMBLEBOUND_PLAYWRIGHT` to a Playwright package path if it is not installed in the repo; set `BRAMBLEBOUND_SCREENSHOTS` for screenshots.
