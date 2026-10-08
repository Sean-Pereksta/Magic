/* Original local artwork. The build embeds the exact files from the asset manifest. */
(() => {
  'use strict';
  const config = window.ATS_VISUAL_BUNDLE || { manifest: {}, sources: {} };
  const images = Object.create(null), failures = [], listeners = new Set();
  const entries = Object.entries(config.sources);
  const assets = window.ATSVisualAssets = {
    images, manifest: config.manifest, failures, status: 'loading',
    total: entries.length, loaded: 0, completed: 0, progress: 0,
    get(id) { return images[id] || null; },
    onProgress(listener) { listeners.add(listener); report(listener); return () => listeners.delete(listener); },
    ready: null
  };
  const report = listener => { try { listener(assets); } catch (error) { console.error('Artwork progress callback failed', error); } };
  const notify = () => { for (const listener of listeners) report(listener); };
  assets.ready = Promise.all(entries.map(([id, source]) => new Promise(resolve => {
    const image = new Image();
    let finished = false;
    const finish = ok => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      image.onload = image.onerror = null;
      if (ok && image.naturalWidth && image.naturalHeight) { images[id] = image; assets.loaded++; }
      else failures.push(id);
      assets.completed++;
      assets.progress = assets.completed / Math.max(1, assets.total);
      resolve();
      notify();
    };
    const timer = setTimeout(() => finish(false), 20000);
    image.onload = () => finish(true);
    image.onerror = () => finish(false);
    image.src = source;
  }))).then(() => {
    assets.status = failures.length ? 'degraded' : 'ready';
    assets.progress = 1;
    notify();
    // The decoded image objects retain their sources; release the extra bundle map.
    delete window.ATS_VISUAL_BUNDLE;
    return assets;
  });
})();
