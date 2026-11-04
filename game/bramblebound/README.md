# Bramblebound · The Endless Road

Standalone expedition roguelite for Cat’N Mice. Open `/game/bramblebound/` through any static HTTP server; there is no build or server dependency. `/game/bramblebound.html` is a compatibility redirect.

Guide Pip through 35 regions and repeating circuits, build a loadout from 284 elemental relics, fuse ingredients, choose 64 talents and abilities, battle group encounters and guardians, and purchase permanent Legacy upgrades after each run. Four equipment slots and a searchable 400-slot pack support mouse, keyboard, and touch controls. Space pauses combat. Menus and the title screen pause the simulation.

Profiles autosave under `bramblebound.profile.v1`. Existing standalone saves remain compatible; the profile menu exports/imports JSON, including prepared starting kits. Saves are device/browser local. Corrupt saves enter recovery without overwriting the original. No account is required.

## Firebase cloud journeys

Use **Firebase cloud saves** on the title screen, or the cloud section of **Profile & settings**. Assign a save name and password for a new journey; on another device choose **Reopen journey**, enter the same name/password, review it, and confirm the restore. Complete expeditions, Legacy progress, discoveries, prepared starting kits, and settings are included. A restore keeps the replaced local data at `bramblebound.cloud.backup.v1`. Only the cloud save name is remembered at `bramblebound.cloud.name.v1`; the password stays in memory for this session. Once connected, changed progress syncs about every 30 seconds while the game is visible and no dialog is open. Use **Save to Firebase** for an immediate confirmed checkpoint before switching devices. Local autosaves continue independently and work offline.

The implementation matches Arcane Wilds’ current model: Firebase project `bible-game-246c0`, anonymous authentication, and encrypted named documents in the existing `lobbies` collection. Bramblebound uses the `bramblebound-save--` SHA-256 namespace, `bramblebound-cloud-save` game type, `status: active`, and an empty player list; these documents do not appear in waiting-game lists. No global rules change or Firebase rules deployment is required. Those legacy rules permit authenticated reads/writes for generic save documents; this is password-encrypted single-player storage, not owner-only server authorization or an anti-cheat authority. Do not persist passwords or rely on a lobby username as ownership.

Payloads use AES-GCM-256 with PBKDF2-SHA256 at 210,000 iterations, random per-write salts/IVs and optional gzip. Encrypted and expanded sizes are bounded. Server-only reads, validated complete profiles, transaction revision/IV/content checks, and reopen-after-uncertain-write behavior prevent silent stale-device overwrites. Existing slots must be reopened in the current session before updates. Disconnecting removes the local binding and stops autosaving without deleting the Firebase slot. Imports and fresh profiles pause cloud synchronization. `cloud-save.test.cjs` tests real encryption and transport failures; `cloud-browser.cjs` tests desktop/mobile transfers and autosave/disconnect races with an isolated transport.

The title menu adds original generated cover art, continue/start actions, kit preparation, the handbook, profile settings, and a library link. The library registry is `lobby/lobby-core.html`; editorial placement is `lobby/lobby-showcase.js`; responsive art variants and hashes are in `lobby/assets/game-artwork.{json,js}`. The original game source was imported from the user's Bramblebound folder with expanded elemental combat and backward-compatible profile migration.

Run `npm run test:bramblebound` for combat, progression, fusion, save validation, and library integration tests. `node game/bramblebound/tests/browser.cjs` runs browser checks with Playwright installed and Chromium or an installed Edge browser. Set `BRAMBLEBOUND_PLAYWRIGHT` to a Playwright package path if it is not installed in the repo; set `BRAMBLEBOUND_SCREENSHOTS` for screenshots.

## Expanded road

The expansion adds 15 biomes, 105 enemies, 72 relics, and 30 fusion recipes. Every element has innate combat behavior. Twelve signature weapon mechanics and eight counter types support deliberate loadouts; enemy dossiers and the searchable field guide explain them. A Dewglass Vial is included in every new starting backpack so cleansing is available immediately. Held weapons now have dedicated pixel artwork, elemental trails, rarity markings, and level sockets.

Existing profiles migrate without resetting their run, gear, discoveries, or records. A saved 8- or 20-region circuit finishes before the next circuit opens all 35 regions. New expeditions use 35 regions immediately.

## Rarity and finds

Five tiers classify sustained damage, utility and signature effects: Common, Uncommon, Rare, Epic and Mythic. Their rarity power multipliers are 1×, 1.12×, 1.28×, 1.5× and 1.8×; item details include this bonus in displayed damage. Tier changes preserve existing item IDs, levels and recipes. Rare finds receive stronger ornament and auras.

The chance of at least one relic in a normal wave is now 24% (previously 16%); guardian waves have a 48% chance (previously 37%), independent of enemy headcount. Rarity is chosen before duplicate, support or ingredient bias. At the first region with all tiers unlocked, ordinary finds are 65.7% Common, 20% Uncommon, 10% Rare, 3.5% Epic and 0.8% Mythic. Guardians and later regions improve high-tier odds, shown exactly in the atlas. Fusion remains an alternate route to strong gear. Tests include 28,000 cleared waves and verify rarity frequencies, headcount normalization and owned-Mythic independence.
