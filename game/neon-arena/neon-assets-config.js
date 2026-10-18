// Weapons/aliens use the existing pack. Rebuilt operators use characters/v3/.
// UI artwork is included in this repository under assets/ui/.
// An embedding page may override these values beforehand.
window.NEON_ASSET_CONFIG = Object.assign({
  ASSET_BASE_URL: 'https://pub-7a87605611d24e35813d8a3e5aa78af4.r2.dev',
  UI_ASSET_BASE_URL: './assets',
  CHARACTER_ASSET_VERSION: 'v3',
  CHARACTER_FALLBACK_BASE_URL: './assets',
  // Expansion assets ship in the repository. Set an uploaded CDN root only
  // after copying the manifest's exact relative object keys to that root.
  EXPANSION_ASSET_BASE_URL: './assets',
  EXPANSION_FALLBACK_BASE_URL: './assets',
  version: '2'
}, window.NEON_ASSET_CONFIG || {});
