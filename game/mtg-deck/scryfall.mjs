/** Small, paced Scryfall client. Catalog pages are fetched only when requested. */
const API_ORIGIN = 'https://api.scryfall.com';
const SEARCH_PATH = '/cards/search';
const MIN_INTERVAL_MS = 600;
const TYPES = new Set(['creature', 'instant', 'sorcery', 'artifact', 'enchantment', 'planeswalker', 'land', 'battle']);
const RARITIES = new Set(['common', 'uncommon', 'rare', 'mythic']);

export class ScryfallError extends Error {
  constructor(message, { status = 0, retryAfterSeconds = null, code = '' } = {}) {
    super(message);
    this.name = 'ScryfallError';
    this.status = status;
    this.retryAfterSeconds = retryAfterSeconds;
    this.code = code;
  }
}

const quote = value => JSON.stringify(String(value).trim().replace(/[\u0000-\u001f]/g, ' '));

/** Price filters/sorts explicitly use nonfoil USD. Finish choices on cards are separate. */
export function buildSearchParams({ query = '', mode = 'name', color = '', type = '', rarity = '', set = '', mana = '', price = '', sort = 'name', unique = 'cards', finish = 'nonfoil' } = {}) {
  if (finish !== 'nonfoil' && (price || /^value-/.test(sort))) {
    throw new ScryfallError('Scryfall price filters and sorting use nonfoil USD. Choose nonfoil as the search price basis.');
  }
  const terms = ['game:paper'];
  const text = String(query).trim();
  if (text) {
    if (mode === 'advanced') terms.push(`(${text})`);
    else if (mode === 'text') terms.push(`oracle:${quote(text)}`);
    else terms.push(`name:${quote(text)}`);
  }
  const colorKey = String(color).toUpperCase();
  if (/^[WUBRG]{1,5}$/.test(colorKey)) terms.push(`c:${colorKey.toLowerCase()}`);
  else if (colorKey === 'C') terms.push('c:c');
  else if (colorKey === 'M') terms.push('c:m');
  const typeKey = String(type).toLowerCase();
  if (TYPES.has(typeKey)) terms.push(`t:${typeKey}`);
  const rarityKey = String(rarity).toLowerCase();
  if (RARITIES.has(rarityKey)) terms.push(`r:${rarityKey}`);
  if (String(set).trim()) terms.push(`set:${quote(set)}`);
  if (mana === '6+') terms.push('mv>=6');
  else if (String(mana).trim() !== '' && Number.isInteger(Number(mana)) && Number(mana) >= 0 && Number(mana) <= 20) terms.push(`mv=${Number(mana)}`);
  const priceTerms = { under1: 'usd<1', '1to5': 'usd>=1 usd<5', '5to20': 'usd>=5 usd<20', '20plus': 'usd>=20' };
  if (Object.hasOwn(priceTerms, price)) terms.push(priceTerms[price]);
  const sortParams = {
    name: ['name', 'asc'],
    'value-desc': ['usd', 'desc'],
    'value-asc': ['usd', 'asc'],
    'mana-asc': ['cmc', 'asc'],
    'mana-desc': ['cmc', 'desc'],
  };
  const [order, dir] = Object.hasOwn(sortParams, sort) ? sortParams[sort] : sortParams.name;
  // Unknown prices should never appear as zero-dollar bargains in a value sort.
  if (order === 'usd') terms.push('usd>=0');
  return new URLSearchParams({ q: terms.join(' '), order, dir, unique: unique === 'prints' ? 'prints' : 'cards' });
}

function abortError() {
  const error = new Error('Request canceled.');
  error.name = 'AbortError';
  return error;
}

function assertNotAborted(signal) {
  if (signal?.aborted) throw abortError();
}

function abortable(promise, signal) {
  if (!signal) return promise;
  if (signal.aborted) {
    // The queued work still owns its own rejection handler.
    Promise.resolve(promise).catch(() => {});
    return Promise.reject(abortError());
  }
  return new Promise((resolve, reject) => {
    const canceled = () => { cleanup(); reject(abortError()); };
    const cleanup = () => signal.removeEventListener('abort', canceled);
    signal.addEventListener('abort', canceled, { once: true });
    Promise.resolve(promise).then(value => { cleanup(); resolve(value); }, error => { cleanup(); reject(error); });
  });
}

function delay(ms, signal) {
  assertNotAborted(signal);
  return new Promise((resolve, reject) => {
    const canceled = () => { clearTimeout(timer); signal?.removeEventListener('abort', canceled); reject(abortError()); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', canceled); resolve(); }, ms);
    signal?.addEventListener('abort', canceled, { once: true });
  });
}

function copy(value) {
  return typeof structuredClone === 'function' ? structuredClone(value) : JSON.parse(JSON.stringify(value));
}

function paginationURL(value) {
  let url;
  try { url = new URL(value); } catch { throw new ScryfallError('Invalid catalog page. Search again to reload the catalog.'); }
  if (url.origin !== API_ORIGIN || url.pathname !== SEARCH_PATH || url.username || url.password) {
    throw new ScryfallError('Invalid catalog page. Search again to reload the catalog.');
  }
  return url.href;
}

function stampCards(payload, checkedAt) {
  if (Array.isArray(payload?.data)) return { ...payload, data: payload.data.map(card => ({ ...card, prices_checked_at: checkedAt })) };
  if (payload?.id && payload?.name) return { ...payload, prices_checked_at: checkedAt };
  throw new ScryfallError('Scryfall returned an unreadable card response. Please try again.');
}

/**
 * One queue covers searches, pagination, imports, and price refreshes. Defaults
 * use conservative pacing (600 ms between starts) and honor rate-limit cooldowns.
 * now/sleep are injectable so pacing and cancellation can be tested without delay.
 */
export function createScryfallClient({ fetchImpl = globalThis.fetch?.bind(globalThis), intervalMs = MIN_INTERVAL_MS, timeoutMs = 15000, cacheTtlMs = 60000, now = Date.now, sleep = delay } = {}) {
  if (typeof fetchImpl !== 'function') throw new Error('This browser does not support card requests.');
  const spacing = Math.max(MIN_INTERVAL_MS, Number(intervalMs) || MIN_INTERVAL_MS);
  const timeout = Math.max(1, Number(timeoutMs) || 15000);
  let queue = Promise.resolve();
  let lastStarted = -Infinity;
  let cooldownUntil = 0;
  const cache = new Map();

  function rateLimitError() {
    const remaining = Math.max(1, Math.ceil((cooldownUntil - now()) / 1000));
    return new ScryfallError(`Scryfall is receiving too many requests. Try again in ${remaining} seconds.`, { status: 429, retryAfterSeconds: remaining, code: 'rate_limit' });
  }

  async function send(url, { signal, method = 'GET', body } = {}) {
    assertNotAborted(signal);
    const cached = method === 'GET' ? cache.get(url) : null;
    if (cached && now() - cached.at < cacheTtlMs) return copy(cached.data);
    const run = async () => {
      assertNotAborted(signal);
      const queuedCache = method === 'GET' ? cache.get(url) : null;
      if (queuedCache && now() - queuedCache.at < cacheTtlMs) return copy(queuedCache.data);
      // Fail queued work promptly instead of making it wait behind a long timer.
      // A user can retry when the provider's cooldown has expired.
      if (now() < cooldownUntil) throw rateLimitError();
      const waitMs = Math.max(0, spacing - (now() - lastStarted));
      if (waitMs) await sleep(waitMs, signal);
      assertNotAborted(signal);
      const controller = new AbortController();
      const canceled = () => controller.abort();
      signal?.addEventListener('abort', canceled, { once: true });
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeout);
      lastStarted = now();
      try {
        const headers = { Accept: 'application/json' };
        if (body) headers['Content-Type'] = 'application/json';
        const response = await abortable(Promise.resolve(fetchImpl(url, { method, headers, body, signal: controller.signal })), controller.signal);
        if (response.status === 429) {
          // Honor the header even when a proxy returns a non-JSON error page.
          const retry = response.headers?.get?.('Retry-After');
          const seconds = retry ? (/^\d+(?:\.\d+)?$/.test(retry) ? Math.ceil(Number(retry)) : Math.ceil((Date.parse(retry) - now()) / 1000)) : NaN;
          cooldownUntil = Math.max(cooldownUntil, now() + Math.max(1, Number.isFinite(seconds) ? seconds : 30) * 1000);
          throw rateLimitError();
        }
        let payload;
        try { payload = await abortable(Promise.resolve(response.json()), controller.signal); }
        catch (error) {
          if (controller.signal.aborted) throw error;
          throw new ScryfallError(`Scryfall returned an unreadable response (${response.status || 'network error'}). Please try again.`, { status: response.status });
        }
        if (!response.ok) {
          const retry = response.headers?.get?.('Retry-After');
          const retryAfterSeconds = retry ? (/^\d+(?:\.\d+)?$/.test(retry) ? Math.ceil(Number(retry)) : Math.max(1, Math.ceil((Date.parse(retry) - now()) / 1000))) : null;
          const delaySeconds = Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : null;
          const message = payload?.details || `Card request failed (${response.status}). Please try again.`;
          throw new ScryfallError(message, { status: response.status, retryAfterSeconds: delaySeconds, code: payload?.code || '' });
        }
        const data = stampCards(payload, new Date(now()).toISOString());
        if (method === 'GET' && cacheTtlMs > 0) {
          cache.set(url, { at: now(), data: copy(data) });
          if (cache.size > 60) cache.delete(cache.keys().next().value);
        }
        return data;
      } catch (error) {
        if (signal?.aborted) throw abortError();
        if (timedOut) throw new ScryfallError('The card request took too long. Check your connection and try again.', { code: 'timeout' });
        if (error instanceof ScryfallError || error?.name === 'AbortError') throw error;
        throw new ScryfallError('Could not reach Scryfall. Check your connection and try again.', { code: 'network' });
      } finally {
        clearTimeout(timer);
        signal?.removeEventListener('abort', canceled);
      }
    };
    const pending = queue.then(run, run);
    queue = pending.catch(() => {});
    return abortable(pending, signal);
  }

  function requireList(data) {
    if (!Array.isArray(data?.data)) throw new ScryfallError('The catalog response was incomplete. Please search again.');
    return { ...data, total_cards: Number(data.total_cards) || data.data.length, has_more: Boolean(data.has_more), next_page: data.has_more ? data.next_page || null : null };
  }

  return {
    async search(options = {}, { signal } = {}) {
      const url = `${API_ORIGIN}${SEARCH_PATH}?${buildSearchParams(options)}`;
      try { return requireList(await send(url, { signal })); }
      catch (error) {
        if (error.status === 404) return { object: 'list', data: [], total_cards: 0, has_more: false, next_page: null };
        throw error;
      }
    },
    async nextPage(url, { signal } = {}) {
      return requireList(await send(paginationURL(url), { signal }));
    },
    async named(name, { signal } = {}) {
      if (!String(name).trim()) throw new ScryfallError('Enter a card name to import.');
      // /cards/named may choose a digital printing. Imports need a paper card.
      const params = new URLSearchParams({ q: `game:paper !${quote(name)}`, unique: 'cards', order: 'name', dir: 'asc' });
      const result = requireList(await send(`${API_ORIGIN}${SEARCH_PATH}?${params}`, { signal }));
      if (!result.data.length) throw new ScryfallError('No paper printing found for that exact card name.', { status: 404, code: 'not_found' });
      return result.data[0];
    },
    async collection(ids, { signal } = {}) {
      const uniqueIds = [...new Set((Array.isArray(ids) ? ids : []).filter(id => typeof id === 'string' && id.trim()).map(id => id.trim()))];
      const result = { data: [], not_found: [] };
      for (let offset = 0; offset < uniqueIds.length; offset += 75) {
        assertNotAborted(signal);
        const batch = uniqueIds.slice(offset, offset + 75);
        const payload = await send(`${API_ORIGIN}/cards/collection`, { signal, method: 'POST', body: JSON.stringify({ identifiers: batch.map(id => ({ id })) }) });
        if (!Array.isArray(payload.data)) throw new ScryfallError('The price refresh response was incomplete. Please try again.');
        result.data.push(...payload.data);
        result.not_found.push(...(Array.isArray(payload.not_found) ? payload.not_found : []));
      }
      return result;
    },
  };
}
