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
  const requests = [], requestOptions = [], images = [], starts = [], releases = [];
  const loader = createPreloader({
    root,
    pageUrl: 'https://game.test/game/turncraft.html',
    scriptUrl: 'https://game.test/game/turncraft/preload.js',
    fetch: async (url, options) => {
      requests.push(url);
      requestOptions.push(options);
      if (respond) {
        const response = respond(url, options);
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
  return { loader, requests, requestOptions, images, starts, releases, options: { includeLocalMenuAssets: false, includeCommandScenes: false } };
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

test('best-effort loading finishes all known images after a failure and reports successful versus attempted graphics', async () => {
  const decoded = deferred();
  const progress = [];
  const h = harness({ begin: (image, url) => {
    if (url.endsWith('banner.webp')) queueMicrotask(() => image.onerror());
    else {
      image.decode = () => decoded.promise;
      queueMicrotask(() => image.onload());
    }
  } });
  let finished = false;
  const pending = h.loader.preload({ ...h.options, allowFailures: true, onProgress: update => progress.push(update) }).then(result => { finished = true; return result; });
  await tick();
  assert.equal(finished, false, 'one failed image cannot skip another image still decoding');
  assert.equal(h.loader.getState().completed, 0);
  assert.equal(h.loader.getState().attempted, 1);
  assert.equal(h.loader.getState().failed, 1);
  assert.equal(h.loader.getState().percent, 50);
  decoded.resolve();
  const result = await pending;
  assert.equal(result.ready, true);
  assert.equal(result.degraded, true);
  assert.equal(result.completed, 1);
  assert.equal(result.attempted, 2);
  assert.equal(result.total, 2);
  assert.equal(result.failed, 1);
  assert.equal(result.failureCount, 1);
  assert.deepEqual(result.failures, [{
    url: 'https://art.test/shattered-front/banner.webp',
    stage: 'image-load',
    reason: 'Cannot load image https://art.test/shattered-front/banner.webp. The browser did not expose an HTTP status; the request may have failed because of the network, server, or image format.'
  }]);
  assert.equal('status' in result.failures[0], false, 'Image errors cannot claim an unobserved HTTP status');
  assert.deepEqual(JSON.parse(JSON.stringify(result.failures)), result.failures);
  assert.equal(progress.at(-1).phase, 'complete');
  assert.equal(progress.at(-1).percent, 100);
  const state = h.loader.getState();
  state.failures[0].reason = 'external mutation';
  assert.equal(h.loader.getState().failures[0].reason, result.failures[0].reason, 'diagnostic snapshots cannot mutate internal state');
});

test('best-effort loading uses independent catalogs and explicit fallback URLs when one catalog is missing', async () => {
  let missing = true;
  const h = harness({ respond: url => missing && url.endsWith('visual-asset-index.json') ? { ok: false, status: 404 } : null });
  const options = { ...h.options, allowFailures: true, graphicsBaseUrl: 'https://art.test/', additionalUrls: ['https://art.test/fallback-terrain.webp'] };
  const result = await h.loader.preload(options);
  assert.equal(result.visualManifest, undefined);
  assert.ok(result.animationManifest);
  assert.ok(result.expansionManifest);
  assert.deepEqual(new Set(h.starts), new Set(['https://art.test/atlas.webp', 'https://art.test/shattered-front/banner.webp', 'https://art.test/fallback-terrain.webp']));
  assert.equal(result.completed, 3);
  assert.equal(result.failed, 0);
  assert.equal(result.failureCount, 1);
  assert.deepEqual(result.failures, [{
    url: 'https://game.test/game/turncraft/assets/visual-asset-index.json',
    stage: 'manifest-fetch',
    status: 404,
    reason: 'Cannot load the artwork catalog assets/visual-asset-index.json (404).'
  }]);
  missing = false;
  const retry = await h.loader.preload(options);
  assert.equal(retry.degraded, false);
  assert.equal(retry.failureCount, 0);
  assert.equal(h.requests.length, 4, 'retry fetches only the previously missing catalog');
  assert.equal(h.starts.length, 3, 'already decoded images stay cached after a catalog retry');
});

test('best-effort loading preserves valid entries in malformed catalogs and retries only that catalog', async () => {
  const data = catalogs();
  data.expansionManifest.assets.broken = {};
  const h = harness({ data });
  const result = await h.loader.preload({ ...h.options, allowFailures: true });
  assert.equal(result.completed, 2);
  assert.equal(result.total, 2);
  assert.equal(result.failureCount, 1);
  assert.equal(result.failures[0].stage, 'catalog-entry');
  assert.equal(result.failures[0].url, 'https://game.test/game/turncraft/shattered-front-assets.json');
  assert.match(result.failures[0].reason, /no image path/);
  delete data.expansionManifest.assets.broken;
  const retry = await h.loader.preload({ ...h.options, allowFailures: true });
  assert.equal(retry.failureCount, 0);
  assert.equal(h.requests.length, 4);
  assert.equal(h.requests.filter(url => url.endsWith('shattered-front-assets.json')).length, 2);
  assert.equal(h.starts.length, 2);
});

test('best-effort reports corrupt and stalled decodes, continues, and retries unsuccessful images', async () => {
  let mode = 'reject';
  const lateDecode = deferred();
  const h = harness({ begin: image => {
    image.decode = () => mode === 'reject' ? Promise.reject(new Error('corrupt image bytes')) : mode === 'timeout' ? lateDecode.promise : Promise.resolve();
    queueMicrotask(() => image.onload());
  } });
  const corrupt = await h.loader.preload({ ...h.options, allowFailures: true });
  assert.equal(corrupt.completed, 0);
  assert.equal(corrupt.attempted, 2);
  assert.equal(corrupt.failed, 2);
  assert.ok(corrupt.failures.every(failure => failure.stage === 'image-decode' && /corrupt image bytes/.test(failure.reason)));
  mode = 'timeout';
  const stalled = await h.loader.preload({ ...h.options, allowFailures: true, timeoutMs: 20 });
  assert.equal(stalled.ready, true);
  assert.equal(stalled.failed, 2);
  assert.ok(stalled.failures.every(failure => failure.stage === 'image-decode-timeout'));
  lateDecode.resolve();
  await tick();
  assert.equal(h.loader.getState().failed, 2, 'late decoding cannot erase failure diagnostics');
  mode = 'success';
  const retry = await h.loader.preload({ ...h.options, allowFailures: true });
  assert.equal(retry.completed, 2);
  assert.equal(retry.failed, 0);
  assert.equal(retry.degraded, false);
  assert.equal(h.starts.length, 6);
  assert.equal(h.requests.length, 3);
});

test('best-effort catalog timeouts still attempt explicit known graphics and return serializable diagnostics', async () => {
  const h = harness({ respond: () => ({ ok: true, json: () => new Promise(() => {}) }) });
  const result = await h.loader.preload({ ...h.options, allowFailures: true, timeoutMs: 20, additionalUrls: ['https://art.test/known.webp'] });
  assert.equal(result.completed, 1);
  assert.equal(result.total, 1);
  assert.equal(result.failed, 0);
  assert.equal(result.failureCount, 3);
  assert.ok(result.failures.every(failure => failure.stage === 'manifest-timeout'));
  assert.deepEqual(h.starts, ['https://art.test/known.webp']);
  assert.deepEqual(JSON.parse(JSON.stringify(h.loader.getState().failures)), result.failures);
});

test('failure diagnostics observe real HTTP statuses, cancel response bodies, and leave preload state unchanged', async () => {
  const canceled = [];
  const statuses = { 'missing.webp': [404, 'Not Found'], 'unavailable.webp': [503, 'Service Unavailable'], 'responding.webp': [200, 'OK'] };
  const h = harness({ respond: url => {
    const status = statuses[url.split('/').at(-1)];
    return status ? { status: status[0], statusText: status[1], body: { cancel: () => { canceled.push(url); return Promise.resolve(); } } } : null;
  } });
  await h.loader.preload(h.options);
  const before = h.loader.getState();
  const failures = Object.keys(statuses).map((name,index) => ({ url: `https://art.test/${name}`, stage: ['image-load','download','timeout'][index], reason: 'Image could not load.' }));
  const result = await h.loader.diagnoseFailures(failures);
  assert.deepEqual(result.map(failure => failure.status), [404, 503, 200]);
  assert.deepEqual(result.map(failure => failure.statusText), ['Not Found', 'Service Unavailable', 'OK']);
  assert.match(result[0].probeReason, /HTTP 404 Not Found.*not found/);
  assert.match(result[1].probeReason, /HTTP 503 Service Unavailable.*server could not serve/);
  assert.match(result[2].probeReason, /HTTP 200 OK.*endpoint responds.*could not load/);
  assert.deepEqual(new Set(canceled), new Set(failures.map(failure => failure.url)));
  assert.ok(h.requestOptions.slice(3).every(options => options.method === 'GET'));
  assert.deepEqual(h.loader.getState(), before, 'background diagnosis cannot change loading completion or progress');
  assert.equal(h.starts.length, 2);
  await h.loader.preload(h.options);
  assert.equal(h.starts.length, 2, 'diagnosing failures cannot evict cached graphics');
  assert.deepEqual(JSON.parse(JSON.stringify(result)), result);
  assert.equal('status' in failures[0], false, 'the original failure report is preserved');
});

test('failure diagnostics do not claim an HTTP status or a specific cause when the browser cannot expose one', async () => {
  const h = harness({ respond: url => url.endsWith('opaque.webp') ? { status: 0, type: 'opaque' } : Promise.reject(new TypeError('Failed to fetch')) });
  const failures = ['blocked.webp', 'opaque.webp'].map(name => ({ url: `https://art.test/${name}`, stage: 'image-load-timeout', reason: 'Loading timed out.' }));
  const result = await h.loader.diagnoseFailures(failures);
  assert.equal(result.length, 2);
  assert.ok(result.every(failure => !('status' in failure) && /HTTP status unavailable/.test(failure.probeReason)));
  assert.match(result[0].probeReason, /could not complete \(Failed to fetch\)/);
  assert.match(result[1].probeReason, /did not expose an HTTP status/);
  assert.equal(result.some(failure => /404|503|CORS blocked|network failure/.test(failure.probeReason)), false, 'an inaccessible response does not establish the server status or failure cause');
});

test('failure diagnostics have bounded concurrency and ignore decoding errors and already known statuses', async () => {
  let active = 0, maximum = 0, canceled = 0;
  const requests = [];
  const h = harness({ respond: url => {
    active++;
    maximum = Math.max(maximum, active);
    const response = deferred();
    requests.push(() => {
      active--;
      response.resolve({ status: 404, body: { cancel: () => { canceled++; } } });
    });
    return response.promise;
  } });
  const failures = [
    ...Array.from({ length: 5 }, (_, index) => ({ url: `https://art.test/${index}.webp`, stage: 'image-load', reason: 'Missing image.' })),
    { url: 'https://art.test/decode.webp', stage: 'image-decode', reason: 'Corrupt image.' },
    { url: 'https://art.test/decode-timeout.webp', stage: 'image-decode-timeout', reason: 'Decode timed out.' },
    { url: 'https://art.test/known.webp', stage: 'image-load', status: 403, reason: 'HTTP 403.' }
  ];
  const pending = h.loader.diagnoseFailures(failures, { concurrency: 2 });
  await tick();
  assert.equal(h.requests.length, 2);
  for (let index = 0; index < 5; index++) {
    assert.ok(requests[index], `diagnostic request ${index} has started`);
    requests[index]();
    await tick();
  }
  const result = await pending;
  assert.equal(maximum, 2);
  assert.equal(h.requests.length, 5);
  assert.equal(canceled, 5);
  assert.deepEqual(result.slice(5), failures.slice(5));
  assert.deepEqual(result.map(failure => failure.url), failures.map(failure => failure.url), 'report ordering remains stable');
});

test('failure diagnostics time out and abort stalled requests without delaying launch or revising reports later', async () => {
  let signal;
  const response = deferred();
  const h = harness({ root: { AbortController }, respond: (url, options) => { signal = options.signal; return response.promise; } });
  const failure = { url: 'https://art.test/stalled.webp', stage: 'image-load', reason: 'Image could not load.' };
  const result = await h.loader.diagnoseFailures([failure], { timeoutMs: 20 });
  assert.equal(signal.aborted, true);
  assert.equal('status' in result[0], false);
  assert.match(result[0].probeReason, /HTTP status unavailable.*timed out after 20 ms/);
  let canceled = false;
  response.resolve({ status: 404, body: { cancel: () => { canceled = true; } } });
  await tick();
  assert.equal(canceled, true, 'a late response body is still canceled');
  assert.equal('status' in result[0], false, 'a late response cannot mutate a finished report');
  assert.equal(h.loader.getState().phase, 'idle');
});
