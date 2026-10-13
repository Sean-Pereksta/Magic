// Set this to the public R2 root after uploading weapons/, characters/ and ui/.
// Keep it empty until a real host is available; the game remains playable with
// its procedural graphics. An embedding page may supply this value beforehand.
window.NEON_ASSET_CONFIG = Object.assign({
  ASSET_BASE_URL: '',
  version: '1'
}, window.NEON_ASSET_CONFIG || {});
