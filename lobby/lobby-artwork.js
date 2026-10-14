(function (root) {
  'use strict';
  const escape = value => String(value || '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
  function entry(game) {
    const manifest = root.GameHubArtworkManifest;
    const games = manifest?.games || {};
    const original = Object.prototype.hasOwnProperty.call(games, game?.key) ? games[game.key] : null;
    const overrides = root.GameHubShowcase?.artworkOverrides || {};
    const override = Object.prototype.hasOwnProperty.call(overrides, game?.key) ? overrides[game.key] : null;
    return original || override ? {...original,...override} : null;
  }
  function url(path) {
    if (!path) return '';
    if (/^https:\/\//.test(path)) return path;
    const base = (root.GameHubShowcase?.assetBase || '/lobby/assets').replace(/\/$/, '');
    return base + '/' + path.replace(/^\/?lobby\/assets\//, '').replace(/^\//, '');
  }
  function image(game, variant = 'tile') {
    const art = entry(game);
    return url(art?.[variant] || art?.tile) || String(game?.imagePath || '');
  }
  function visual(game, variant = 'mark') {
    const small = variant === 'tiny' || variant === 'mark';
    const source = image(game, small ? 'thumb' : variant === 'hero' ? 'hero' : 'tile');
    const art = entry(game);
    const fallback = url(root.GameHubArtworkManifest?.fallback || 'game-tiles/arcade-fallback.webp');
    const backups = [...new Set([game?.imagePath, fallback])].filter(path => path && path !== (source || fallback));
    const srcset = !small && art?.thumb && art?.tile ? ` srcset="${escape(url(art.thumb))} 240w, ${escape(url(art.tile))} 640w${art.hero && art.hero !== art.tile ? ', '+escape(url(art.hero))+' 1280w' : ''}" sizes="(max-width:350px) 92vw, (max-width:520px) 48vw, (max-width:900px) 45vw, 320px"` : '';
    return `<span class="game-visual game-visual-${variant} has-image" aria-hidden="true"><img src="${escape(source || fallback)}"${srcset} width="${small ? 240 : 640}" height="${small ? 135 : 360}" alt="" loading="lazy" decoding="async" data-fallback="${escape(backups[0])}" data-final="${escape(backups[1])}" onerror="this.removeAttribute('srcset');if(this.dataset.fallback){this.src=this.dataset.fallback;delete this.dataset.fallback}else if(this.dataset.final){this.src=this.dataset.final;delete this.dataset.final}else{this.hidden=true;this.parentElement.classList.add('image-missing')}"></span>`;
  }
  const api = Object.freeze({image, visual});
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.GameHubArtwork = api;
})(globalThis);
