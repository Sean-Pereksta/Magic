(() => {
  const nativeFetch = window.fetch.bind(window);
  const CACHE_NAME = 'ffcc-data-cache-v1';
  const STATE_URL = 'https://api.sleeper.app/v1/state/nfl';
  const PLAYER_URL = 'https://api.sleeper.app/v1/players/nfl?active=true';
  const DEFAULT_SEASON = new Date().getFullYear();
  const DEFAULT_TEAMS = 12;
  const pending = new Map();
  const memoryCache = new Map();
  const completed = new Set();
  let preseason = DEFAULT_SEASON;
  let preloadFinished = false;
  let appRendered = false;
  let hideTimer = null;

  const settings = (() => {
    try { return JSON.parse(localStorage.getItem('ffcc-settings-v3') || '{}') || {}; }
    catch { return {}; }
  })();
  const teams = Math.max(4, Math.min(20, parseInt(settings.teams, 10) || DEFAULT_TEAMS));
  const formats = [['ppr', 'ppr'], ['half', 'half-ppr'], ['standard', 'standard']];
  const ffcUrl = (format, year = preseason) => `https://fantasyfootballcalculator.com/api/v1/adp/${format}?teams=${teams}&year=${year}&position=all`;

  function ensureLoader() {
    if (document.getElementById('ffccBootLoader')) return;
    const style = document.createElement('style');
    style.textContent = `
      #ffccBootLoader{position:fixed;inset:0;z-index:9999;display:grid;place-items:center;padding:24px;background:rgba(7,16,29,.97);backdrop-filter:blur(10px);transition:opacity .28s ease,visibility .28s ease}
      #ffccBootLoader.done{opacity:0;visibility:hidden;pointer-events:none}
      #ffccBootLoader .bootCard{width:min(620px,94vw);padding:24px;border-radius:18px;background:linear-gradient(180deg,#0f1d2f,#0b1726);border:1px solid #20344f;box-shadow:0 24px 70px #000a}
      #ffccBootLoader .bootTitle{font-size:22px;font-weight:900;color:#eef6ff;margin-bottom:5px}
      #ffccBootLoader .bootSub{font-size:12px;color:#8fa7c1;margin-bottom:18px}
      #ffccBootLoader .bootTrack{height:14px;border-radius:999px;background:#07101d;border:1px solid #20344f;overflow:hidden;box-shadow:inset 0 1px 4px #0008}
      #ffccBootLoader .bootFill{height:100%;width:3%;border-radius:999px;background:linear-gradient(90deg,#65a9ff,#5ee6a8);transition:width .22s ease}
      #ffccBootLoader .bootMeta{display:flex;justify-content:space-between;gap:12px;margin-top:9px;color:#aac1d8;font-size:11px}
      #ffccBootLoader .bootStages{display:grid;grid-template-columns:repeat(5,1fr);gap:7px;margin-top:16px}
      #ffccBootLoader .bootStage{padding:8px 7px;border:1px solid #20344f;border-radius:9px;background:#091523;color:#819ab3;font-size:10px;text-align:center;transition:.2s ease}
      #ffccBootLoader .bootStage.active{border-color:#315c8f;color:#d8ebff;background:#10233a}
      #ffccBootLoader .bootStage.complete{border-color:#285943;color:#8bf1bc;background:#103126}
      @media(max-width:650px){#ffccBootLoader .bootStages{grid-template-columns:1fr 1fr}#ffccBootLoader .bootStage:last-child{grid-column:1/-1}}
    `;
    document.head.appendChild(style);
    const loader = document.createElement('div');
    loader.id = 'ffccBootLoader';
    loader.innerHTML = `
      <div class="bootCard" role="status" aria-live="polite">
        <div class="bootTitle">Loading Fantasy Draft Command Center</div>
        <div class="bootSub">Pulling the active player pool and composite market data up front so the board is ready when it opens.</div>
        <div class="bootTrack"><div class="bootFill" id="ffccBootFill"></div></div>
        <div class="bootMeta"><span id="ffccBootStatus">Starting data pipeline…</span><b id="ffccBootPercent">3%</b></div>
        <div class="bootStages">
          <div class="bootStage active" data-stage="season">NFL state</div>
          <div class="bootStage" data-stage="players">Active players</div>
          <div class="bootStage" data-stage="ppr">PPR ADP</div>
          <div class="bootStage" data-stage="half">Half ADP</div>
          <div class="bootStage" data-stage="standard">Standard ADP</div>
        </div>
      </div>`;
    document.body.appendChild(loader);
  }

  function progress(value, message) {
    const pct = Math.max(3, Math.min(100, Math.round(value)));
    const fill = document.getElementById('ffccBootFill');
    const percent = document.getElementById('ffccBootPercent');
    const status = document.getElementById('ffccBootStatus');
    if (fill) fill.style.width = `${pct}%`;
    if (percent) percent.textContent = `${pct}%`;
    if (status && message) status.textContent = message;
  }

  function markStage(stage, message) {
    completed.add(stage);
    const el = document.querySelector(`#ffccBootLoader [data-stage="${stage}"]`);
    if (el) { el.classList.remove('active'); el.classList.add('complete'); }
    const next = ['season','players','ppr','half','standard'].find(x => !completed.has(x));
    const nextEl = next && document.querySelector(`#ffccBootLoader [data-stage="${next}"]`);
    if (nextEl) nextEl.classList.add('active');
    const weighted = {season:12, players:38, ppr:14, half:14, standard:14};
    const pct = 3 + [...completed].reduce((sum, x) => sum + (weighted[x] || 0), 0);
    progress(Math.min(95, pct), message);
  }

  function ttlFor(url) {
    if (url.includes('/state/nfl')) return 5 * 60 * 1000;
    if (url.includes('/players/nfl')) return 6 * 60 * 60 * 1000;
    if (url.includes('fantasyfootballcalculator.com/api/v1/adp/')) return 30 * 60 * 1000;
    if (url.includes('api.sleeper.com/stats/nfl/player/')) return 7 * 24 * 60 * 60 * 1000;
    return 0;
  }

  function isManaged(url) {
    return url === STATE_URL || url.startsWith('https://api.sleeper.app/v1/players/nfl') || url.includes('fantasyfootballcalculator.com/api/v1/adp/') || url.includes('api.sleeper.com/stats/nfl/player/');
  }

  async function readPersistent(url) {
    if (!('caches' in window)) return null;
    try {
      const cache = await caches.open(CACHE_NAME);
      const hit = await cache.match(url);
      if (!hit) return null;
      const cachedAt = Number(hit.headers.get('x-ffcc-cached-at')) || 0;
      if (!cachedAt || Date.now() - cachedAt > ttlFor(url)) return null;
      return { body: await hit.text(), status: hit.status || 200, contentType: hit.headers.get('content-type') || 'application/json', cached: true };
    } catch { return null; }
  }

  async function writePersistent(url, data) {
    if (!('caches' in window) || !data || data.status < 200 || data.status >= 300) return;
    try {
      const cache = await caches.open(CACHE_NAME);
      await cache.put(url, new Response(data.body, { status: 200, headers: { 'content-type': data.contentType || 'application/json', 'x-ffcc-cached-at': String(Date.now()) } }));
    } catch {}
  }

  async function loadResource(url) {
    if (memoryCache.has(url)) return memoryCache.get(url);
    if (pending.has(url)) return pending.get(url);
    const task = (async () => {
      const persistent = await readPersistent(url);
      if (persistent) { memoryCache.set(url, persistent); return persistent; }
      const response = await nativeFetch(url, { cache: 'no-store' });
      const data = { body: await response.text(), status: response.status, contentType: response.headers.get('content-type') || 'application/json', cached: false };
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      memoryCache.set(url, data);
      writePersistent(url, data);
      return data;
    })().finally(() => pending.delete(url));
    pending.set(url, task);
    return task;
  }

  function responseFrom(data) {
    return new Response(data.body, { status: data.status || 200, headers: { 'content-type': data.contentType || 'application/json' } });
  }

  window.fetch = function ffccFetch(input, init = {}) {
    const url = typeof input === 'string' ? input : input?.url;
    const method = String(init?.method || (typeof input !== 'string' ? input?.method : 'GET') || 'GET').toUpperCase();
    if (method === 'GET' && url && isManaged(url)) return loadResource(url).then(responseFrom);
    return nativeFetch(input, init);
  };

  function maybeFinish() {
    if (!preloadFinished || !appRendered) return;
    progress(100, 'Rankings ready');
    clearTimeout(hideTimer);
    hideTimer = setTimeout(() => document.getElementById('ffccBootLoader')?.classList.add('done'), 180);
  }

  function watchForRender() {
    const rows = document.getElementById('rankingRows');
    if (!rows) { setTimeout(watchForRender, 20); return; }
    const check = () => {
      const text = rows.textContent || '';
      if (rows.children.length > 1 && !/loading rankings/i.test(text)) {
        appRendered = true;
        maybeFinish();
        observer.disconnect();
      }
    };
    const observer = new MutationObserver(check);
    observer.observe(rows, { childList: true, subtree: true });
    check();
  }

  async function warm(url, stage, message) {
    try {
      const data = await loadResource(url);
      let detail = message;
      if (stage === 'players') {
        try {
          const count = Object.keys(JSON.parse(data.body) || {}).length;
          detail = `${count.toLocaleString()} player records loaded`;
        } catch {}
      }
      markStage(stage, data.cached ? `${detail} • cache` : detail);
      return data;
    } catch {
      markStage(stage, `${message} unavailable • fallback enabled`);
      return null;
    }
  }

  async function preload() {
    ensureLoader();
    watchForRender();
    progress(5, 'Loading NFL state + active player pool in parallel…');
    const statePromise = warm(STATE_URL, 'season', 'NFL season state loaded');
    const playersPromise = warm(PLAYER_URL, 'players', 'Active player pool loaded');
    const state = await statePromise;
    try {
      const parsed = state && JSON.parse(state.body);
      const y = parseInt(parsed?.season, 10);
      if (y) preseason = y;
    } catch {}
    const adpPromises = formats.map(([stage, format]) => warm(ffcUrl(format, preseason), stage, `${stage === 'half' ? 'Half-PPR' : stage.toUpperCase()} ADP loaded`));
    await Promise.allSettled([playersPromise, ...adpPromises]);
    preloadFinished = true;
    progress(95, 'Building composite rankings…');
    maybeFinish();
    setTimeout(() => {
      if (!appRendered) {
        appRendered = true;
        maybeFinish();
      }
    }, 6000);
  }

  preload();
})();
