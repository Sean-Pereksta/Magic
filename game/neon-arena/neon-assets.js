(function (root) {
  'use strict';
  const config = root.NEON_ASSET_CONFIG || {};
  function normalizeRoot(value, relative = false) {
    try {
      if (!value) return '';
      const parsed = relative ? new URL(value, root.document?.baseURI || root.location?.href) : new URL(value);
      if (!['https:', 'http:'].includes(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash) throw new Error('Invalid asset root');
      return parsed.href.replace(/\/+$/, '');
    } catch (error) { console.warn('Neon Breach asset root is unavailable:', error.message); return ''; }
  }
  const baseURL = normalizeRoot(config.ASSET_BASE_URL);
  const uiBaseURL = Object.prototype.hasOwnProperty.call(config, 'UI_ASSET_BASE_URL') ? normalizeRoot(config.UI_ASSET_BASE_URL, true) : baseURL;
  const weapons = Object.fromEntries(['pistol','assault','sniper','smg','shotgun','burst','arc','rocket','mortar','punch','harpoon','plasma','laser','mender'].map(key => [key, 'weapons/' + key + '.glb']));
  const humanKeys = ['player_one','player_two','bot_breacher','bot_sentinel','bot_vanguard','bot_flanker','bot_demolisher','character_bulwark','character_ronin','character_warden','character_revenant','character_praetorian','character_overlord'];
  const characterKeys = [...humanKeys, ...['blue','yellow','red','green','stripe','purple','flyer','climber','rammerBoss','spitterBoss','matriarchBoss','shieldBoss','burrowerBoss','commanderBoss'].map(key => 'alien_' + key)];
  const characters = Object.fromEntries(characterKeys.map(key => [key, 'characters/' + key + '.glb']));
  const localCharacterRoot = normalizeRoot(config.CHARACTER_FALLBACK_BASE_URL || './assets', true);
  const isHumanPath = path => /^characters\/[\w]+\.(glb|webp)$/.test(path) && humanKeys.includes(path.split('/').pop().replace(/\.[^.]+$/, ''));
  function localCharacterURL(path) { return isHumanPath(path) && localCharacterRoot ? localCharacterRoot + '/' + path : ''; }
  function url(path) {
    if (typeof path !== 'string' || !/^(weapons|characters|ui)\/[\w/.-]+$/.test(path) || path.split('/').some(part => part === '..' || part === '.')) return '';
    const rootURL = path.startsWith('ui/') ? uiBaseURL : baseURL;
    if (!rootURL) return localCharacterURL(path);
    const characterVersion = String(config.CHARACTER_ASSET_VERSION || '');
    if (isHumanPath(path) && /^[\w-]+$/.test(characterVersion)) path = path.replace('characters/', 'characters/' + characterVersion + '/');
    return rootURL + '/' + path + (config.version ? '?v=' + encodeURIComponent(String(config.version)) : '');
  }
  function bindImages(container = document) {
    for (const image of container.querySelectorAll('img[data-neon-asset]')) {
      const source = url(image.dataset.neonAsset);
      if (!source) { image.hidden = true; continue; }
      image.decoding = 'async';
      image.addEventListener('error', () => { image.hidden = true; image.removeAttribute('src'); image.parentElement?.classList.remove('asset-loaded'); }, { once: true });
      image.addEventListener('load', () => { image.hidden = false; image.parentElement?.classList.add('asset-loaded'); }, { once: true });
      image.src = source;
    }
    const styles = document.documentElement.style;
    for (const [name, path] of Object.entries({
      'neon-menu-desktop':'ui/menu/background_desktop.webp',
      'neon-menu-mobile':'ui/menu/background_mobile.webp',
      'neon-panel-frame':'ui/menu/menu_panel_frame.png',
      'neon-button-primary':'ui/menu/button_primary.png',
      'neon-button-secondary':'ui/menu/button_secondary.png'
    })) {
      const source = url(path);
      styles.setProperty('--' + name, source ? 'url("' + source.replace(/"/g, '%22') + '")' : 'none');
    }
    root.dispatchEvent?.(new Event('neonassetsready'));
  }
  root.NeonAssets = Object.freeze({baseURL, uiBaseURL, weapons: Object.freeze(weapons), characters: Object.freeze(characters), url, localCharacterURL, bindImages});
  if (typeof module !== 'undefined' && module.exports) module.exports = root.NeonAssets;
  if (typeof document !== 'undefined') {
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', () => bindImages(), {once:true});
    else bindImages();
  }
})(typeof window !== 'undefined' ? window : globalThis);
