/* Attempt every known artwork download and decode before Turncraft opens. */
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

  function artworkError(message, details) {
    const error = new Error(message);
    error.failure = { ...details, reason: message };
    return error;
  }

  function failureOf(error, url, stage) {
    return { url, stage, reason: String(error?.message || error), ...error?.failure };
  }

  function directoryUrl(value, fallback, pageUrl) {
    const url = new URL(value || fallback, pageUrl);
    if (!/^https?:$/.test(url.protocol)) throw new Error('Artwork must use an HTTP or HTTPS address.');
    return url.href.replace(/\/?$/, '/');
  }

  function collectUrls({ visualManifest, expansionManifest, animationManifest, baseUrl, graphicsBaseUrl, expansionBaseUrl, legacyFiles = [], additionalUrls = [], localMenuBaseUrl, includeCommandScenes = true, pageUrl = 'http://localhost/', allowFailures = false, onFailure, manifestUrls = {} }) {
    function problem(message, url, stage = 'catalog') {
      const error = artworkError(message, { url: url || pageUrl, stage });
      if (!allowFailures) throw error;
      onFailure?.(error.failure);
    }
    function entries(catalog, field, label, source) {
      const values = catalog?.[field];
      if (!values || typeof values !== 'object' || Array.isArray(values) || !Object.keys(values).length) {
        // A failed request is already in the report. Validate successful responses separately.
        if (catalog !== undefined || !allowFailures) problem(`The ${label} artwork catalog is missing its ${field}.`, source);
        return {};
      }
      return values;
    }
    const visualImages = entries(visualManifest, 'images', 'Turncraft', manifestUrls.visualManifest);
    const expansionAssets = entries(expansionManifest, 'assets', 'Shattered Front', manifestUrls.expansionManifest);
    const animationAssets = entries(animationManifest, 'assets', 'Turncraft animation', manifestUrls.animationManifest);
    function base(value, fallback, source) {
      try { return directoryUrl(value, fallback, pageUrl); }
      catch (error) { problem(error.message, source); return directoryUrl(null, DEFAULT_BASE, pageUrl); }
    }
    const graphicsBase = base(graphicsBaseUrl, visualManifest?.baseUrl || DEFAULT_BASE, manifestUrls.visualManifest);
    const legacyBase = base(baseUrl, graphicsBase, pageUrl);
    const expansionBase = base(expansionBaseUrl, expansionManifest?.defaultBaseURL || DEFAULT_EXPANSION_BASE, manifestUrls.expansionManifest);
    const urls = new Set();
    function add(path, base, source) {
      try {
        if (typeof path !== 'string' || !path.trim()) throw new Error('An artwork catalog entry has no image path.');
        const url = new URL(path, base);
        if (!/^https?:$/.test(url.protocol)) throw new Error('An artwork catalog contains an unsupported image address.');
        urls.add(url.href);
      } catch (error) { problem(error.message, source, 'catalog-entry'); }
    }
    for (const path of Object.keys(visualImages)) add(path, graphicsBase, manifestUrls.visualManifest);
    for (const asset of Object.values(expansionAssets)) add(typeof asset === 'string' ? asset : asset?.path, expansionBase, manifestUrls.expansionManifest);
    for (const asset of Object.values(animationAssets)) {
      if (!asset?.states || typeof asset.states !== 'object' || !Object.keys(asset.states).length) {
        problem('An animation catalog entry has no states.', manifestUrls.animationManifest, 'catalog-entry');
        continue;
      }
      for (const state of Object.values(asset.states)) add(state?.sheet, graphicsBase, manifestUrls.animationManifest);
    }
    for (const path of legacyFiles) add(path, legacyBase, pageUrl);
    if (includeCommandScenes) for (const path of COMMAND_SCENE_FILES) add(path, graphicsBase, pageUrl);
    if (localMenuBaseUrl) for (const path of MENU_FILES) add(path, localMenuBaseUrl, pageUrl);
    for (const url of additionalUrls) add(url, pageUrl, pageUrl);
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
    let state = { phase: 'idle', completed: 0, attempted: 0, total: 0, failed: 0, failureCount: 0, failures: [], percent: 0, ready: false, degraded: false };

    function snapshot() { return { ...state, failures: state.failures.map(failure => ({ ...failure })) }; }

    function publish(patch) {
      state = { ...state, ...patch };
      for (const subscriber of subscribers) {
        try { subscriber(snapshot()); } catch { /* A display callback cannot skip required artwork. */ }
      }
    }

    function withTimeout(promise, timeoutMs, description, abort, details) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          abort?.();
          reject(artworkError(`Timed out loading ${description}.`, details));
        }, timeoutMs);
        Promise.resolve(promise).then(resolve, reject).finally(() => clearTimeout(timer));
      });
    }

    async function manifest(path, timeoutMs) {
      const url = new URL(path, moduleUrl).href;
      if (manifests.has(url)) return manifests.get(url);
      if (!fetchResource) throw artworkError('Artwork catalogs cannot be loaded in this browser.', { url, stage: 'manifest-fetch' });
      const controller = typeof environment.AbortController === 'function' ? new environment.AbortController() : null;
      const pending = withTimeout((async () => {
        let response;
        try { response = await fetchResource(url, controller ? { signal: controller.signal } : undefined); }
        catch (error) { throw artworkError(`Cannot load the artwork catalog ${path}: ${error.message || error}.`, { url, stage: 'manifest-fetch' }); }
        if (!response?.ok) throw artworkError(`Cannot load the artwork catalog ${path} (${response?.status || 'network error'}).`, { url, stage: 'manifest-fetch', ...(response?.status ? { status: response.status } : {}) });
        try { return await response.json(); }
        catch (error) { throw artworkError(`Cannot read the artwork catalog ${path}: ${error.message || error}.`, { url, stage: 'manifest-parse' }); }
      })(), timeoutMs, `artwork catalog ${path}`, () => controller?.abort(), { url, stage: 'manifest-timeout' });
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
        const timer = setTimeout(() => finish(artworkError(`Timed out loading image ${url}${decoding ? ' while decoding' : ''}.`, { url, stage: decoding ? 'image-decode-timeout' : 'image-load-timeout' })), timeoutMs);
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
          if (error) {
            entry.failure = failureOf(error, url, decoding ? 'image-decode' : 'image-load');
            reject(error);
          }
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
            } catch (error) { finish(artworkError(`Cannot decode image ${url}: ${error.message || error}.`, { url, stage: 'image-decode' })); }
          };
          // Image events do not expose HTTP status. Report the exact URL without inventing one.
          image.onerror = () => finish(artworkError(`Cannot load image ${url}. The browser did not expose an HTTP status; the request may have failed because of the network, server, or image format.`, { url, stage: 'image-load' }));
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
      const failures = [];
      const catalogPaths = {
        visualManifest: 'assets/visual-asset-index.json',
        expansionManifest: 'shattered-front-assets.json',
        animationManifest: 'assets/animation-manifest.json'
      };
      const manifestUrls = Object.fromEntries(Object.entries(catalogPaths).map(([key, path]) => [key, new URL(path, moduleUrl).href]));
      publish({ phase: 'manifests', completed: 0, attempted: 0, total: 0, failed: 0, failureCount: 0, failures: [], percent: 0, ready: false, degraded: false, error: null, url: null });
      try {
        // One unavailable catalog must not discard artwork known to the other catalogs.
        const results = await Promise.allSettled(Object.values(catalogPaths).map(path => manifest(path, timeoutMs)));
        const catalogs = {};
        let catalogError;
        Object.keys(catalogPaths).forEach((key, index) => {
          const result = results[index];
          if (result.status === 'fulfilled') catalogs[key] = result.value;
          else {
            failures.push(failureOf(result.reason, manifestUrls[key], 'manifest-fetch'));
            catalogError ||= result.reason;
          }
        });
        if (catalogError && !options.allowFailures) throw catalogError;
        const { visualManifest, expansionManifest, animationManifest } = catalogs;
        const localMenuBaseUrl = options.includeLocalMenuAssets === false ? null : new URL('assets/', moduleUrl).href;
        const urls = collectUrls({ ...options, visualManifest, expansionManifest, animationManifest, expansionBaseUrl: expansionOverride(options), localMenuBaseUrl, pageUrl, manifestUrls, onFailure: failure => {
          failures.push(failure);
          // Invalid successful JSON must be fetched again, while other catalogs stay cached.
          manifests.delete(failure.url);
        } });
        let completed = urls.filter(url => assets.get(url)?.status === 'ready').length, attempted = completed, cursor = 0, failed = 0;
        const imageErrors = [];
        function progress() {
          return { completed, attempted, failed, total: urls.length, failures: failures.map(failure => ({ ...failure })), failureCount: failures.length, degraded: failures.length > 0, percent: urls.length ? Math.round(attempted / urls.length * 100) : 100 };
        }
        publish({ phase: 'images', ...progress() });
        async function worker() {
          while (cursor < urls.length) {
            const url = urls[cursor++];
            if (assets.get(url)?.status === 'ready') continue;
            try { await loadImage(url, timeoutMs); completed++; }
            catch (error) {
              failed++;
              failures.push(failureOf(error, url, 'image-load'));
              imageErrors.push(error);
            }
            attempted++;
            publish({ ...progress(), url });
          }
        }
        await Promise.all(Array.from({ length: Math.min(concurrency, urls.length) }, worker));
        if (failed && !options.allowFailures) {
          const error = new Error(`${failed} Turncraft graphic${failed === 1 ? '' : 's'} could not be loaded. Check your connection and retry.`);
          error.failedUrls = failures.filter(failure => failure.stage.startsWith('image-')).map(failure => failure.url);
          error.causes = imageErrors;
          error.failures = failures.map(failure => ({ ...failure }));
          throw error;
        }
        publish({ phase: 'complete', ...progress(), ready: true, url: null });
        return { visualManifest, expansionManifest, animationManifest, urls, ...progress(), ready: true };
      } catch (error) {
        if (error.failure && !failures.some(failure => failure.url === error.failure.url && failure.reason === error.failure.reason)) failures.push(error.failure);
        if (error.failure) manifests.delete(error.failure.url);
        publish({ phase: 'error', ready: false, error, failures: failures.map(failure => ({ ...failure })), failureCount: failures.length, degraded: failures.length > 0 });
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
        try { subscriber(snapshot()); } catch { /* Continue the shared preload. */ }
      }
      const pending = currentRun;
      return pending.finally(() => { if (subscriber) subscribers.delete(subscriber); });
    }

    async function diagnoseFailures(failures = [], options = {}) {
      const timeoutMs = Number.isFinite(options.timeoutMs) && options.timeoutMs > 0 ? options.timeoutMs : 5000;
      const requestedConcurrency = Number(options.concurrency);
      const concurrency = Number.isFinite(requestedConcurrency) && requestedConcurrency > 0 ? Math.max(1, Math.min(8, Math.floor(requestedConcurrency))) : 4;
      const reports = Array.isArray(failures) ? failures.map(failure => ({ ...failure })) : [];
      let cursor = 0;
      async function probe(failure) {
        // Decode errors already identify the failure, and Image events cannot reveal HTTP status.
        // Probe only download failures that do not already contain an observed HTTP status.
        if (!/^(?:image-(?:(?:load|download)(?:-timeout)?|timeout)|download|timeout)$/.test(failure.stage) || (Number.isFinite(failure.status) && failure.status > 0)) return failure;
        let url;
        try {
          url = new URL(failure.url);
          if (!/^https?:$/.test(url.protocol)) throw new Error('unsupported protocol');
        } catch {
          return { ...failure, probeReason: 'HTTP status unavailable: the diagnostic URL is not a valid HTTP or HTTPS address.' };
        }
        if (!fetchResource) return { ...failure, probeReason: 'HTTP status unavailable: this browser cannot make a diagnostic request.' };
        const controller = typeof environment.AbortController === 'function' ? new environment.AbortController() : null;
        try {
          const response = await withTimeout((async () => {
            const response = await fetchResource(url.href, { method: 'GET', ...(controller ? { signal: controller.signal } : {}) });
            // Only response headers are needed. Do not download the image a second time.
            try { Promise.resolve(response?.body?.cancel()).catch(() => {}); }
            catch { /* A closed or unreadable body does not invalidate observed headers. */ }
            return response;
          })(), timeoutMs, `diagnostic request ${url.href}`, () => controller?.abort(), { url: url.href, stage: 'image-probe-timeout' });
          const status = response?.status;
          if (!Number.isFinite(status) || status <= 0) return { ...failure, probeReason: 'HTTP status unavailable: the diagnostic response did not expose an HTTP status.' };
          const statusText = typeof response.statusText === 'string' ? response.statusText.trim() : '';
          let explanation = '';
          if (status === 404) explanation = ' The image was not found at this URL.';
          else if (status === 401 || status === 403) explanation = ' The server denied access to this URL.';
          else if (status === 429) explanation = ' The server is limiting requests to this URL.';
          else if (status >= 500) explanation = ' The server could not serve this URL.';
          else if (status >= 200 && status < 300) explanation = ' The endpoint responds, but the browser could not load the image.';
          return { ...failure, status, ...(statusText ? { statusText } : {}), probeReason: `Diagnostic GET returned HTTP ${status}${statusText ? ` ${statusText}` : ''}.${explanation}` };
        } catch (error) {
          const reason = error?.failure?.stage === 'image-probe-timeout' ? `the diagnostic request timed out after ${timeoutMs} ms` : `the diagnostic request could not complete (${String(error?.message || error)})`;
          return { ...failure, probeReason: `HTTP status unavailable: ${reason}.` };
        }
      }
      async function worker() {
        while (cursor < reports.length) {
          const index = cursor++;
          reports[index] = await probe(reports[index]);
        }
      }
      await Promise.all(Array.from({ length: Math.min(concurrency, reports.length) }, worker));
      return reports;
    }

    return { preload, diagnoseFailures, getState: snapshot, getAssetState: url => assets.get(url)?.status || 'pending' };
  }

  return { ...createPreloader(), createPreloader, collectUrls, LEGACY_FILES, MENU_FILES, COMMAND_SCENE_FILES };
});
