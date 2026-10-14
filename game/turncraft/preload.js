/* Download and decode the full artwork catalog before Turncraft opens. */
(function (root, factory) {
  const scriptUrl = typeof document !== 'undefined' ? document.currentScript?.src : '';
  const api = factory(root, scriptUrl);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.TurncraftPreload = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function (root, scriptUrl) {
  'use strict';

  const DEFAULT_BASE = 'https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/';
  const DEFAULT_EXPANSION_BASE = DEFAULT_BASE;
  const LEGACY_FILES = ['ground.png', 'cliff.png', 'mineral.png', 'gas.png', 'larva.png', 'cocoon.png'];
  const MENU_FILES = ['factions/frontier.webp', 'factions/ascendant.webp', 'factions/brood.webp', 'maps/ember-rift.webp', 'maps/frostline-basin.webp', 'maps/verdant-divide.webp', 'maps/shattered-dunes.webp', 'maps/orbital-foundry.webp'];
  const COMMAND_SCENE_FILES = ['effects/ui/frontier-command.webp', 'effects/ui/ascendant-command.webp', 'effects/ui/brood-command.webp'];

  function directoryUrl(value, fallback, pageUrl) {
    const url = new URL(value || fallback, pageUrl);
    if (!/^https?:$/.test(url.protocol)) throw new Error('Artwork must use an HTTP or HTTPS address.');
    return url.href.replace(/\/?$/, '/');
  }

  function collectUrls({ visualManifest, expansionManifest, animationManifest, baseUrl, graphicsBaseUrl, expansionBaseUrl, legacyFiles = [], additionalUrls = [], localMenuBaseUrl, includeCommandScenes = true, pageUrl = 'http://localhost/' }) {
    if (!visualManifest?.images || !Object.keys(visualManifest.images).length) throw new Error('The Turncraft artwork catalog is missing its images.');
    if (!expansionManifest?.assets || !Object.keys(expansionManifest.assets).length) throw new Error('The Shattered Front artwork catalog is missing its assets.');
    if (!animationManifest?.assets || !Object.keys(animationManifest.assets).length) throw new Error('The Turncraft animation catalog is missing its assets.');
    const graphicsBase = directoryUrl(graphicsBaseUrl, visualManifest.baseUrl || DEFAULT_BASE, pageUrl);
    const legacyBase = directoryUrl(baseUrl, graphicsBase, pageUrl);
    const expansionBase = directoryUrl(expansionBaseUrl, expansionManifest.defaultBaseURL || DEFAULT_EXPANSION_BASE, pageUrl);
    const urls = new Set();
    function add(path, base) {
      if (typeof path !== 'string' || !path.trim()) throw new Error('An artwork catalog entry has no image path.');
      const url = new URL(path, base);
      if (!/^https?:$/.test(url.protocol)) throw new Error('An artwork catalog contains an unsupported image address.');
      urls.add(url.href);
    }
    for (const path of Object.keys(visualManifest.images)) add(path, graphicsBase);
    for (const asset of Object.values(expansionManifest.assets)) add(typeof asset === 'string' ? asset : asset?.path, expansionBase);
    for (const asset of Object.values(animationManifest.assets)) {
      if (!asset?.states || !Object.keys(asset.states).length) throw new Error('An animation catalog entry has no states.');
      for (const state of Object.values(asset.states)) add(state?.sheet, graphicsBase);
    }
    for (const path of legacyFiles) add(path, legacyBase);
    if (includeCommandScenes) for (const path of COMMAND_SCENE_FILES) add(path, graphicsBase);
    if (localMenuBaseUrl) for (const path of MENU_FILES) add(path, localMenuBaseUrl);
    for (const url of additionalUrls) add(url, pageUrl);
    return [...urls];
  }

  function createPreloader(dependencies = {}) {
    const environment = dependencies.root || root;
    const pageUrl = dependencies.pageUrl || environment.location?.href || 'http://localhost/game/turncraft.html';
    const moduleUrl = dependencies.scriptUrl || scriptUrl || new URL('turncraft/preload.js', pageUrl).href;
    const fetchResource = dependencies.fetch || environment.fetch?.bind(environment);
    const createImage = dependencies.createImage || (() => new environment.Image());
    const manifests = new Map(), assets = new Map(), subscribers = new Set();
    let currentRun = null;
    let state = { phase: 'idle', completed: 0, total: 0, failed: 0, percent: 0, ready: false };

    function publish(patch) {
      state = { ...state, ...patch };
      for (const subscriber of subscribers) {
        try { subscriber({ ...state }); } catch { /* A display callback cannot skip required artwork. */ }
      }
    }

    function withTimeout(promise, timeoutMs, description, abort) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          abort?.();
          reject(new Error(`Timed out loading ${description}.`));
        }, timeoutMs);
        Promise.resolve(promise).then(resolve, reject).finally(() => clearTimeout(timer));
      });
    }

    async function manifest(path, timeoutMs) {
      const url = new URL(path, moduleUrl).href;
      if (manifests.has(url)) return manifests.get(url);
      if (!fetchResource) throw new Error('Artwork catalogs cannot be loaded in this browser.');
      const controller = typeof environment.AbortController === 'function' ? new environment.AbortController() : null;
      const pending = withTimeout((async () => {
        const response = await fetchResource(url, controller ? { signal: controller.signal } : undefined);
        if (!response?.ok) throw new Error(`Cannot load the artwork catalog ${path} (${response?.status || 'network error'}).`);
        return response.json();
      })(), timeoutMs, `artwork catalog ${path}`, () => controller?.abort());
      manifests.set(url, pending);
      try { return await pending; }
      catch (error) { manifests.delete(url); throw error; }
    }

    function loadImage(url, timeoutMs) {
      if (assets.get(url)?.status === 'ready') return Promise.resolve();
      if (assets.get(url)?.status === 'loading') return assets.get(url).promise;
      const entry = { status: 'loading' };
      assets.set(url, entry);
      entry.promise = new Promise((resolve, reject) => {
        let image, settled = false, decoding = false;
        const timer = setTimeout(() => finish(new Error(`Timed out loading image ${url}.`)), timeoutMs);
        function finish(error) {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          // Keep only completion metadata. Hundreds of full decoded atlases
          // would otherwise compete with the renderer's bounded texture cache.
          if (image) {
            image.onload = null;
            image.onerror = null;
            if (typeof image.removeAttribute === 'function') image.removeAttribute('src');
            else image.src = '';
            image = null;
          }
          entry.status = error ? 'failed' : 'ready';
          delete entry.promise;
          if (error) reject(error);
          else resolve();
        }
        try {
          image = createImage();
          image.decoding = 'async';
          image.onload = async () => {
            if (settled || decoding) return;
            decoding = true;
            try {
              if (!(image.naturalWidth > 0 && image.naturalHeight > 0)) throw new Error(`Image has no drawable pixels: ${url}.`);
              if (typeof image.decode === 'function') await image.decode();
              finish();
            } catch (error) { finish(new Error(`Cannot decode image ${url}: ${error.message || error}.`)); }
          };
          image.onerror = () => finish(new Error(`Cannot load image ${url}.`));
          image.src = url;
        } catch (error) { finish(error); }
      });
      return entry.promise;
    }

    function expansionOverride(options) {
      let value = options.expansionBaseUrl || environment.TURNCRAFT_EXPANSION_ASSET_BASE;
      try { value ||= environment.localStorage?.getItem('turncraft.expansionAssetBase'); }
      catch { /* Use the manifest's public asset host when storage is blocked. */ }
      if (!value) return undefined;
      try { return directoryUrl(value, null, pageUrl); }
      catch { return undefined; }
    }

    async function run(options) {
      const timeoutMs = Number.isFinite(options.timeoutMs) && options.timeoutMs > 0 ? options.timeoutMs : 30000;
      const requestedConcurrency = Number(options.concurrency);
      const concurrency = Number.isFinite(requestedConcurrency) && requestedConcurrency > 0 ? Math.max(1, Math.min(8, Math.floor(requestedConcurrency))) : 4;
      publish({ phase: 'manifests', completed: 0, total: 0, failed: 0, percent: 0, ready: false, error: null, url: null });
      try {
        const [visualManifest, expansionManifest, animationManifest] = await Promise.all([
          manifest('assets/visual-asset-index.json', timeoutMs),
          manifest('shattered-front-assets.json', timeoutMs),
          manifest('assets/animation-manifest.json', timeoutMs)
        ]);
        const localMenuBaseUrl = options.includeLocalMenuAssets === false ? null : new URL('assets/', moduleUrl).href;
        const urls = collectUrls({ ...options, visualManifest, expansionManifest, animationManifest, expansionBaseUrl: expansionOverride(options), localMenuBaseUrl, pageUrl });
        let completed = urls.filter(url => assets.get(url)?.status === 'ready').length, cursor = 0;
        const failures = [];
        publish({ phase: 'images', completed, total: urls.length, percent: Math.round(completed / urls.length * 100) });
        async function worker() {
          while (cursor < urls.length) {
            const url = urls[cursor++];
            if (assets.get(url)?.status === 'ready') continue;
            try { await loadImage(url, timeoutMs); completed++; }
            catch (error) { failures.push({ url, error }); }
            publish({ completed, failed: failures.length, percent: Math.round(completed / urls.length * 100), url });
          }
        }
        await Promise.all(Array.from({ length: Math.min(concurrency, urls.length) }, worker));
        if (failures.length) {
          const error = new Error(`${failures.length} Turncraft graphic${failures.length === 1 ? '' : 's'} could not be loaded. Check your connection and retry.`);
          error.failedUrls = failures.map(failure => failure.url);
          error.causes = failures.map(failure => failure.error);
          throw error;
        }
        publish({ phase: 'complete', completed: urls.length, failed: 0, percent: 100, ready: true, url: null });
        return { visualManifest, expansionManifest, animationManifest, urls, total: urls.length, completed: urls.length };
      } catch (error) {
        // A malformed successful response must be fetched again on retry too.
        if (state.phase === 'manifests') manifests.clear();
        publish({ phase: 'error', ready: false, error });
        throw error;
      }
    }

    function preload(options = {}) {
      const subscriber = typeof options.onProgress === 'function' ? options.onProgress : null;
      if (subscriber) subscribers.add(subscriber);
      if (!currentRun) {
        const pending = run(options);
        currentRun = pending;
        pending.then(() => { if (currentRun === pending) currentRun = null; }, () => { if (currentRun === pending) currentRun = null; });
      } else if (subscriber) {
        try { subscriber({ ...state }); } catch { /* Continue the shared preload. */ }
      }
      const pending = currentRun;
      return pending.finally(() => { if (subscriber) subscribers.delete(subscriber); });
    }

    return { preload, getState: () => ({ ...state }), getAssetState: url => assets.get(url)?.status || 'pending' };
  }

  return { ...createPreloader(), createPreloader, collectUrls, LEGACY_FILES, MENU_FILES, COMMAND_SCENE_FILES };
});
