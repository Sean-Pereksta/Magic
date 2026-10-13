# Faction Forge: animated warfare and the War for the Forge

Faction Forge now opens directly from the Game Hub's solo entry and keeps its existing hosted multiplayer route. It includes 32 troops, 30 campaign missions in five chapters, six milestone/blueprint unlocks, a separate three-tier Forge collection, strategic bot drafting and an Endless Crucible. The original battle ranks I–III remain independent of permanent Forge tiers. Relics, tactics and formations apply their described battlefield effects.

Permanent collection stats are frozen when a validated match starts. Ranked human matches normalize them while showing earned tier artwork. Campaign, bot and casual matches use modest role-specific permanent bonuses. Practice is available without the progression service. Practice and unverified legacy/private hosted sessions do not award competitive MMR or collectible rewards.

## Artwork hosting

New graphics are delivered separately as `faction-forge-animated-assets-v2.zip`. No new image is committed to GitHub. The ZIP extracts `units/`, `terrain/`, `backgrounds/`, `effects/`, `ui/`, `qa/`, `manifest.json` and upload instructions directly at the top level.

The configured Faction Forge asset origin is `https://pub-8ce3fd69b69d4a58b3989d32f7a47e81.r2.dev`. Its `units/`, `terrain/`, `backgrounds/`, `effects/` and `ui/` folders contain the delivered artwork. Keep these folder names and filenames intact when replacing files.

Exact frame metadata is checked in as `game/faction-forge/manifest.js`; `manifest.json` does not need to be uploaded. The display-only image loader draws cross-origin artwork without requiring CORS headers. It does not read pixels or export those canvases. Optional custom remote manifests or pixel-export features require appropriate CORS configuration.

To move the artwork, set `assetBaseUrl` in `game/faction-forge/config.js` to the new public folder URL. Do not reuse another game's R2 origin. Verify a sheet such as `BASE_URL/units/rifleman-tier1.webp` loads and inspect a moving unit and its portrait badge.

Each troop has three different 768×768 WebP atlases. Each atlas has 36 exact 128×128 source rectangles: six frames each for idle, movement, attack, ability, hurt and death. The pivot is `(64,120)` with transparent safety gutters. Facing uses horizontal mirroring. Manifest timing and origins drive the loader; neighboring cells are never sampled. Artwork is loaded on demand, with a bounded image cache and legacy art/symbol fallbacks.

Render interpolation never changes the logical 8×16 board, attack targets, occupancy or network state. Death frames remain visible before removal. Whole 2×2 footprints are respected; troops beyond their starting half's physical capacity remain in the roster as reserves rather than overlapping occupied tiles.

## Progression and deployment

See [PROGRESSION_DEPLOYMENT.md](../../game/faction-forge/PROGRESSION_DEPLOYMENT.md) for the authenticated Admin service, ownership recovery, rules deployment, API contract, idempotent transactions and farming limits. This code does not deploy or change production Firebase by itself.

The existing Firebase project remains `bible-game-246c0`. The service preserves `users/{username}.crowncouncilElo` for existing leaderboard readers and stores protected progression against the verified Firebase UID. A durable verified identity is required for cross-device recovery. The old publicly readable username password cannot safely authorize migration.

A container build is provided:

```sh
docker build -f server/faction-forge/Dockerfile -t faction-forge-progression .
```

Use your server runtime's private Application Default Credentials. Configure the exact game origin allowlist and project ID, expose the service through HTTPS, then set `progressionApiUrl` in `game/faction-forge/config.js` to its `/api/faction-forge` route. Deploy the reviewed Firestore rules alongside the updated game and hub code. No credentials belong in the asset ZIP or repository.

## Verification

```sh
npm run test:faction-forge
npm run test:faction-forge:rules
npm run test:faction-forge:browser
node --experimental-vm-modules --test lobby/tests/solo-launch.test.cjs lobby/tests/lobby-library.test.cjs
# Optional check against the live public artwork host:
FACTION_FORGE_R2_URL=https://pub-8ce3fd69b69d4a58b3989d32f7a47e81.r2.dev npm run test:faction-forge:assets
```

The focused checks cover every actual troop definition, seeded replay determinism, ranked normalization, unit abilities, legal formations/footprints, campaign gating, upgrade costs, duplicate receipts, profile ownership, protected Firebase writes, cross-instance persistence, MMR, real-lobby filtering/join races, fallback identity, sprite bounds, rendering clocks, desktop/mobile controls and the full solo/secure game entry flows.

Tests use a demo Firestore emulator, mocked browser transport or local test servers. The configured R2 artwork is verified separately in a browser. Live production lobbies, verified account migration, progression API deployment and production Firestore rules still require the operator's deployment/configuration step. Until that service is configured, the game provides practice without awarding collection progress or ranked ratings. The balance sampler uses a small deterministic seed set and is a starting point for playtesting, not a claim that every faction/matchup has an equal win rate.
