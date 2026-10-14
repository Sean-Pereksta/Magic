const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { createPreloader, collectUrls, MENU_FILES, LEGACY_FILES, COMMAND_SCENE_FILES } = require('../preload.js');

const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
function catalogs() {
  return {
    visualManifest: { baseUrl: 'https://art.test/', images: { 'atlas.webp': {} } },
    expansionManifest: { defaultBaseURL: 'https://art.test/shattered-front/', assets: { background: { path: 'banner.webp' } } },
    animationManifest: { assets: { unit: { states: { idle: { sheet: 'atlas.webp' }, attack: { sheet: 'atlas.webp' } } } } }
  };
}
function harness({ data = catalogs(), respond, begin, root = {} } = {}) {
  const requests = [], images = [], starts = [], releases = [];
  const loader = createPreloader({
    root,
    pageUrl: 'https://game.test/game/turncraft.html',
    scriptUrl: 'https://game.test/game/turncraft/preload.js',
    fetch: async url => {
      requests.push(url);
      if (respond) {
        const response = respond(url);
        if (response) return response;
      }
      const key = url.endsWith('visual-asset-index.json') ? 'visualManifest' : url.endsWith('animation-manifest.json') ? 'animationManifest' : 'expansionManifest';
      return { ok: true, json: async () => data[key] };
    },
    createImage: () => {
      const image = {
        naturalWidth: 256, naturalHeight: 256,
        decode: () => Promise.resolve(),
        removeAttribute: name => {
          assert.equal(name, 'src');
          releases.push(image.address);
          image.address = '';
        },
        set src(url) {
          image.address = url;
          if (!url) return;
          starts.push(url);
          if (begin) begin(image, url);
          else queueMicrotask(() => image.onload?.());
        },
        get src() { return image.address; }
      };
      images.push(image);
      return image;
    }
  });
  return { loader, requests, images, starts, releases, options: { includeLocalMenuAssets: false, includeCommandScenes: false } };
}

test('the loading gate waits for every image load and decode, and releases decoded images', async () => {
  const decoding = [];
  const h = harness({ begin: image => { const pending = deferred(); image.decode = () => pending.promise; decoding.push(pending); } });
  const progress = [];
  let ready = false;
  const pending = h.loader.preload({ ...h.options, onProgress: update => progress.push(update) }).then(result => { ready = true; return result; });
  await tick();
  assert.equal(h.images.length, 2, 'duplicate animation sheets share the visual image URL');
  for (const image of h.images) image.onload();
  await tick();
  assert.equal(ready, false);
  assert.equal(h.loader.getState().completed, 0);
  decoding[0].resolve();
  await tick();
  assert.equal(ready, false, 'one unfinished decode still keeps the game closed');
  assert.equal(h.loader.getState().completed, 1);
  decoding[1].resolve();
  const result = await pending;
  assert.equal(result.total, 2);
  assert.equal(h.loader.getState().ready, true);
  assert.equal(progress.at(-1).percent, 100);
  assert.equal(progress.at(-1).phase, 'complete');
  assert.equal(h.releases.length, 2);
  for (const image of h.images) {
    assert.equal(image.src, '');
    assert.equal(image.onload, null);
    assert.equal(image.onerror, null);
  }
});

test('a missing image prevents launch and retry reuses only successful assets', async () => {
  let rejectBanner = true;
  const h = harness({ begin: (image, url) => queueMicrotask(() => url.endsWith('banner.webp') && rejectBanner ? image.onerror() : image.onload()) });
  await assert.rejects(h.loader.preload(h.options), error => {
    assert.deepEqual(error.failedUrls, ['https://art.test/shattered-front/banner.webp']);
    return true;
  });
  assert.equal(h.loader.getState().ready, false);
  assert.equal(h.loader.getAssetState('https://art.test/atlas.webp'), 'ready');
  assert.equal(h.loader.getAssetState('https://art.test/shattered-front/banner.webp'), 'failed');
  rejectBanner = false;
  await h.loader.preload(h.options);
  assert.equal(h.starts.filter(url => url.endsWith('atlas.webp')).length, 1);
  assert.equal(h.starts.filter(url => url.endsWith('banner.webp')).length, 2);
  assert.equal(h.requests.length, 3, 'successful manifests are reused on an image retry');
  assert.equal(h.loader.getState().completed, 2);
});

test('decode failure and a stalled decode both block launch and can be retried', async () => {
  let mode = 'reject';
  const lateDecode = deferred();
  const h = harness({ begin: image => {
    image.decode = () => mode === 'reject' ? Promise.reject(new Error('corrupt image')) : mode === 'timeout' ? lateDecode.promise : Promise.resolve();
    queueMicrotask(() => image.onload());
  } });
  await assert.rejects(h.loader.preload(h.options), error => error.causes.every(cause => /Cannot decode/.test(cause.message)));
  mode = 'timeout';
  await assert.rejects(h.loader.preload({ ...h.options, timeoutMs: 20 }), error => error.causes.every(cause => /Timed out/.test(cause.message)));
  lateDecode.resolve();
  await tick();
  assert.equal(h.loader.getState().ready, false, 'late decode completion cannot mark a timed out run ready');
  mode = 'success';
  await h.loader.preload(h.options);
  assert.equal(h.loader.getState().ready, true);
});

test('image work stays bounded through decode, including simultaneous callers', async () => {
  const decodes = [];
  let active = 0, maximum = 0;
  const h = harness({ begin: image => {
    active++;
    maximum = Math.max(maximum, active);
    const pending = deferred();
    decodes.push(() => { active--; pending.resolve(); });
    image.decode = () => pending.promise;
    queueMicrotask(() => image.onload());
  } });
  const options = { ...h.options, concurrency: 2, additionalUrls: ['https://art.test/one.webp', 'https://art.test/two.webp', 'https://art.test/three.webp'] };
  const first = h.loader.preload(options), second = h.loader.preload(options);
  await tick();
  assert.equal(h.images.length, 2);
  for (let i = 0; i < 5; i++) {
    assert.ok(decodes[i], `decode ${i} has entered the bounded queue`);
    decodes[i]();
    await tick();
  }
  const [a, b] = await Promise.all([first, second]);
  assert.equal(a.total, 5);
  assert.equal(b.total, 5);
  assert.equal(maximum, 2);
  assert.equal(h.starts.length, 5);
  assert.equal(h.requests.length, 3);
});

test('missing, malformed, or stalled catalogs keep the gate closed and are fetched again on retry', async () => {
  let missing = true;
  const h = harness({ respond: url => missing && url.endsWith('visual-asset-index.json') ? { ok: false, status: 404 } : null });
  await assert.rejects(h.loader.preload(h.options), /artwork catalog.*404/);
  assert.equal(h.images.length, 0);
  assert.equal(h.loader.getState().ready, false);
  missing = false;
  await h.loader.preload(h.options);
  assert.equal(h.requests.filter(url => url.endsWith('visual-asset-index.json')).length, 2);

  const malformed = harness({ data: { ...catalogs(), expansionManifest: { assets: { broken: {} } } } });
  await assert.rejects(malformed.loader.preload(malformed.options), /no image path/);
  assert.equal(malformed.images.length, 0);

  const stalled = harness({ respond: () => ({ ok: true, json: () => new Promise(() => {}) }) });
  await assert.rejects(stalled.loader.preload({ ...stalled.options, timeoutMs: 20 }), /Timed out loading artwork catalog/);
  assert.equal(stalled.images.length, 0);
});

test('the catalog resolves configured hosts, local menu art, and optional legacy files', async () => {
  const h = harness({ root: { TURNCRAFT_EXPANSION_ASSET_BASE: 'https://override.test/expansion', localStorage: { getItem: () => 'https://storage.test/' } } });
  const result = await h.loader.preload({ baseUrl: 'https://legacy.test/', legacyFiles: LEGACY_FILES, graphicsBaseUrl: 'https://graphics.test/' });
  assert.ok(result.urls.includes('https://override.test/expansion/banner.webp'));
  assert.ok(result.urls.includes('https://graphics.test/atlas.webp'));
  for (const file of LEGACY_FILES) assert.ok(result.urls.includes(`https://legacy.test/${file}`));
  for (const file of COMMAND_SCENE_FILES) assert.ok(result.urls.includes(`https://graphics.test/${file}`));
  for (const file of MENU_FILES) assert.ok(result.urls.includes(`https://game.test/game/turncraft/assets/${file}`));
  assert.ok(h.requests.every(url => url.startsWith('https://game.test/game/turncraft/')), 'catalogs load beside the script, regardless of artwork host');

  const stored = harness({ root: { localStorage: { getItem: () => 'https://storage.test/expansion/' } } });
  assert.ok((await stored.loader.preload(stored.options)).urls.includes('https://storage.test/expansion/banner.webp'));
  const explicit = await stored.loader.preload({ ...stored.options, expansionBaseUrl: 'https://explicit.test/' });
  assert.ok(explicit.urls.includes('https://explicit.test/banner.webp'));
  const unavailableStorage = harness({ root: { get localStorage() { throw new Error('storage unavailable'); } } });
  await unavailableStorage.loader.preload(unavailableStorage.options);
});

test('the shipped catalogs enumerate every graphics image and every animation sheet exactly once', () => {
  const read = file => JSON.parse(fs.readFileSync(path.join(__dirname, '..', file), 'utf8'));
  const visualManifest = read('assets/visual-asset-index.json');
  const expansionManifest = read('shattered-front-assets.json');
  const animationManifest = read('assets/animation-manifest.json');
  const urls = collectUrls({ visualManifest, expansionManifest, animationManifest });
  const expected = new Set([
    ...Object.keys(visualManifest.images).map(file => new URL(file, visualManifest.baseUrl).href),
    ...COMMAND_SCENE_FILES.map(file => new URL(file, visualManifest.baseUrl).href),
    ...Object.values(expansionManifest.assets).map(asset => new URL(asset.path, expansionManifest.defaultBaseURL).href),
    ...Object.values(animationManifest.assets).flatMap(asset => Object.values(asset.states).map(state => new URL(state.sheet, visualManifest.baseUrl).href))
  ]);
  assert.equal(urls.length, expected.size);
  assert.deepEqual(new Set(urls), expected);
  for (const category of ['units/', 'structures/', 'terrain/', 'effects/', 'regions/', 'characters/', 'ui/']) {
    assert.ok(urls.some(url => url.includes(`/${category}`)), `${category} artwork is required`);
  }
  assert.equal(urls.some(url => /\/(ground|larva|cocoon)\.png$/.test(url)), false, 'unpublished legacy fallbacks do not block the production animation catalog');
  assert.equal(expansionManifest.defaultBaseURL, visualManifest.baseUrl, 'expansion artwork is published at the same bucket root');
  assert.equal(urls.some(url => url.includes('.r2.dev/shattered-front/')), false, 'the unpublished expansion prefix must not be requested');
});

test('expansion menu URLs use the published bucket root and honor the same override as preloading', async () => {
  const previous = ['location', 'localStorage', 'TURNCRAFT_EXPANSION_ASSET_BASE'].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]);
  try {
    globalThis.location = { href: 'https://game.test/game/turncraft.html' };
    Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: { getItem: () => null } });
    delete globalThis.TURNCRAFT_EXPANSION_ASSET_BASE;
    const assets = await import(pathToFileURL(path.join(__dirname, '../shattered-front/assets.mjs')).href);
    const bucket = 'https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/';
    assert.equal(assets.assetBase(), bucket);
    assert.equal(assets.assetUrl('menu.background', 'ui/main_menu/battlefield.webp'), `${bucket}ui/main_menu/battlefield.webp`);
    globalThis.TURNCRAFT_EXPANSION_ASSET_BASE = 'https://alternate.test/graphics';
    assert.equal(assets.assetUrl('menu.background', 'ui/main_menu/battlefield.webp'), 'https://alternate.test/graphics/ui/main_menu/battlefield.webp');
  } finally {
    for (const [key, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else delete globalThis[key];
    }
  }
});

test('an image reporting load without drawable pixels never opens the game', async () => {
  const h = harness({ begin: image => { image.naturalWidth = 0; queueMicrotask(() => image.onload()); } });
  await assert.rejects(h.loader.preload(h.options), error => error.causes.every(cause => /no drawable pixels/.test(cause.message)));
  assert.equal(h.loader.getState().ready, false);
});
