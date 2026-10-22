// Firebase Auth identifies the player; Firestore owns all Faction Forge saves.
// Cloudflare serves artwork only. Progression never uses an external API.
export const FACTION_FORGE_CONFIG = Object.freeze({
  assetBaseUrl: 'https://pub-8ce3fd69b69d4a58b3989d32f7a47e81.r2.dev',
  matchmakingTimeoutMs: 30000,
  assetVersion: 'faction-forge-animated-v2'
});


