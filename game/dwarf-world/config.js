/* DwarfWorld artwork ships in this repository beside the game. Resolve from
 * this script so root-domain and project-subdirectory hosting both work.
 * An explicit blank assetBase or enabled:false retains the legacy renderer. */
(() => {
  'use strict';
  const scriptURL = document.currentScript?.src || new URL('./dwarf-world/config.js', document.baseURI).href;
  const supplied = window.DWARF_ART_CONFIG || {};
  const config = window.DWARF_ART_CONFIG = Object.freeze(Object.assign({
    assetBase: new URL('./assets/', scriptURL).href,
    assetPrefix: 'living-mountain-v1/',
    timeoutMs: 4500,
    enabled: true
  }, supplied));
  const manifestURL = new URL('./art-manifest.json', scriptURL).href;
  const timeout = Math.max(500, Math.min(8000, Number(config.timeoutMs) || 4500));
  const images = new Map(), pending = new Map(), failures = new Set();
  let manifest = null, manifestPromise = null, preloadPromise = null;
  let revision = 0;
  const listeners = new Set();
  const enabled = !!config.enabled && !!String(config.assetBase).trim();
  function assetURL(path) {
    const base = String(config.assetBase).replace(/\/+$/, '') + '/';
    const prefix = String(config.assetPrefix).replace(/^\/+|\/+$/g, '');
    return new URL((prefix ? prefix + '/' : '') + path, base).href;
  }
  async function loadManifest() {
    if (!enabled) return null;
    if (manifestPromise) return manifestPromise;
    manifestPromise = (async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeout);
      try {
        const response = await fetch(manifestURL, {signal:controller.signal, cache:'no-cache'});
        if (!response.ok) throw new Error('Artwork manifest unavailable');
        const value = await response.json();
        if (value.version !== 1 || !value.atlases || !value.frames) throw new Error('Unsupported artwork manifest');
        manifest = value;
        return manifest;
      } catch (_) { failures.add('manifest'); return null; }
      finally { clearTimeout(timer); }
    })();
    return manifestPromise;
  }
  function request(id) {
    if (!enabled || failures.has(id)) return Promise.resolve(null);
    if (images.has(id)) return Promise.resolve(images.get(id));
    if (pending.has(id)) return pending.get(id);
    const task = (async () => {
      const data = await loadManifest(), def = data?.atlases[id];
      if (!def) return null;
      return new Promise(resolve => {
        const image = new Image();
        let finished = false;
        const finish = ok => {
          if (finished) return;
          finished = true;
          clearTimeout(timer);
          image.onload = image.onerror = null;
          if (ok && image.naturalWidth === def.width && image.naturalHeight === def.height) {
            images.set(id, image); revision++;
            for (const listener of listeners) { try { listener(id); } catch (_) {} }
            resolve(image);
          } else { failures.add(id); image.src = ''; resolve(null); }
        };
        const timer = setTimeout(() => finish(false), timeout);
        image.crossOrigin = 'anonymous';
        image.decoding = 'async';
        image.onload = () => finish(true);
        image.onerror = () => finish(false);
        try { image.src = assetURL(def.path); } catch (_) { finish(false); }
      });
    })();
    pending.set(id, task);
    return task;
  }
  function preloadEssentials() {
    if (preloadPromise) return preloadPromise;
    if (!enabled) return Promise.resolve({mode:'fallback', loaded:0, reason:'assetBase is not configured'});
    let timer;
    const load = loadManifest().then(data => data ? Promise.all(data.essential.map(request)) : []);
    preloadPromise = Promise.race([load, new Promise(resolve => {timer=setTimeout(resolve, timeout);})])
      .then(() => ({mode:images.size ? 'illustrated' : 'fallback',loaded:images.size,failed:[...failures]}))
      .finally(() => clearTimeout(timer));
    return preloadPromise;
  }
  window.DwarfWorldArtLoader = {
    config, enabled, images, request, preloadEssentials, assetURL,
    subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); },
    get manifest() { return manifest; },
    get revision() { return revision; },
    status() { return {enabled,loaded:[...images.keys()],failed:[...failures],revision}; }
  };
})();
