import test from 'node:test';
import assert from 'node:assert/strict';
import { buildSearchParams, createScryfallClient } from '../scryfall.mjs';

const card = (id = 'a') => ({ object: 'card', id, name: `Card ${id}`, prices: { usd: '1.25', usd_foil: null } });
const list = (cards = [card()], extras = {}) => ({ object: 'list', data: cards, total_cards: cards.length, has_more: false, ...extras });
const response = (body, status = 200, headers = {}) => ({ ok: status >= 200 && status < 300, status, headers: { get: name => headers[name] ?? null }, json: async () => body });

function harness(fetchImpl, extra = {}) {
  let time = 1700000000000;
  const starts = [];
  const sleeps = [];
  const client = createScryfallClient({
    fetchImpl: async (...args) => { starts.push(time); return fetchImpl(...args); },
    now: () => time,
    sleep: async ms => { sleeps.push(ms); time += ms; },
    ...extra,
  });
  return { client, starts, sleeps, advance: ms => { time += ms; } };
}

test('name/text filters quote input, advanced OR remains restricted to paper, mana zero is retained', () => {
  const name = buildSearchParams({ query: 'A "quote" OR game:arena', set: 'Test Set', color: 'U', type: 'Creature', mana: '0', unique: 'prints' });
  assert.equal(name.get('q'), 'game:paper name:"A \\"quote\\" OR game:arena" c:u t:creature set:"Test Set" mv=0');
  assert.equal(name.get('unique'), 'prints');
  assert.equal(buildSearchParams({ query: 'draw a card', mode: 'text' }).get('q'), 'game:paper oracle:"draw a card"');
  assert.equal(buildSearchParams({ query: 't:dragon OR t:angel', mode: 'advanced', mana: '6+' }).get('q'), 'game:paper (t:dragon OR t:angel) mv>=6');
});

test('sort/filter are server-side nonfoil USD and unpriced cards are excluded explicitly', () => {
  const params = buildSearchParams({ sort: 'value-desc', price: '20plus', unique: 'prints' });
  assert.equal(params.get('order'), 'usd');
  assert.equal(params.get('dir'), 'desc');
  assert.match(params.get('q'), /usd>=20/);
  assert.match(params.get('q'), /usd>=0/);
  assert.throws(() => buildSearchParams({ sort: 'value-desc', finish: 'foil' }), /nonfoil USD/);
  assert.equal(buildSearchParams({ sort: 'mana-desc' }).get('order'), 'cmc');
  // Default search includes paper printings with any finish, including foil-only.
  assert.doesNotMatch(buildSearchParams().get('q'), /is:nonfoil/);
});

test('search fetches one page only, stamps the lookup time, and pagination is explicit', async () => {
  const urls = [];
  const next = 'https://api.scryfall.com/cards/search?page=2&q=game%3Apaper';
  const { client } = harness(async (url, options) => {
    urls.push(url);
    assert.equal(options.headers.Accept, 'application/json');
    assert.equal(options.headers['User-Agent'], undefined);
    return response(list([card(urls.length)], { total_cards: 176, has_more: urls.length === 1, next_page: next }));
  });
  const first = await client.search({ query: 'bear' });
  assert.equal(urls.length, 1);
  assert.equal(first.total_cards, 176);
  assert.equal(first.data[0].prices_checked_at, '2023-11-14T22:13:20.000Z');
  assert.equal(first.data[0].prices.usd_foil, null);
  await client.nextPage(first.next_page);
  assert.equal(urls.length, 2);
  assert.equal(urls[1], next);
});

test('rejects untrusted or malformed next-page URLs before any network call', async () => {
  let called = 0;
  const { client } = harness(async () => { called++; return response(list()); });
  for (const url of ['https://example.com/cards/search?page=2', 'http://api.scryfall.com/cards/search', 'https://api.scryfall.com/cards/named', 'https://user:secret@api.scryfall.com/cards/search', 'bad']) {
    await assert.rejects(client.nextPage(url), /Invalid catalog page/);
  }
  assert.equal(called, 0);
});

test('one shared queue paces search, named lookup, and refresh at 600 ms or slower', async () => {
  const { client, starts } = harness(async () => response(list()), { intervalMs: 1 });
  await Promise.all([client.search({ query: 'a' }), client.named('Plains'), client.collection(['a'])]);
  assert.equal(starts.length, 3);
  assert.ok(starts[1] - starts[0] >= 600);
  assert.ok(starts[2] - starts[1] >= 600);
});

test('canceling a queued request settles immediately and it never reaches the network', async () => {
  let release;
  let entered;
  const enteredPromise = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const requests = [];
  const { client } = harness(async url => {
    requests.push(url);
    if (requests.length === 1) { entered(); await gate; }
    return response(list());
  });
  const first = client.search({ query: 'first' });
  await enteredPromise;
  const controller = new AbortController();
  const canceled = client.search({ query: 'second' }, { signal: controller.signal });
  controller.abort();
  await assert.rejects(canceled, { name: 'AbortError' });
  release();
  await first;
  await client.search({ query: 'third' });
  assert.equal(requests.length, 2);
  assert.match(requests[1], /third/);
});

test('abort signal cancels an active request and the queue remains usable', async () => {
  let entered;
  const started = new Promise(resolve => { entered = resolve; });
  let count = 0;
  const { client } = harness(async (url, { signal }) => {
    if (++count > 1) return response(list());
    entered();
    return new Promise((resolve, reject) => signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { name: 'AbortError' })), { once: true }));
  });
  const controller = new AbortController();
  const pending = client.search({ query: 'first' }, { signal: controller.signal });
  await started;
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  assert.equal((await client.search({ query: 'second' })).data.length, 1);
});

test('a request times out even if a fetch implementation fails to handle abort', async () => {
  const { client } = harness(() => new Promise(() => {}), { timeoutMs: 10 });
  await assert.rejects(client.search({ query: 'timeout' }), error => error.code === 'timeout' && /took too long/.test(error.message));
});

test('429 cooldown rejects queued/new requests promptly, then permits retry without caching failures', async () => {
  let calls = 0;
  const { client, advance } = harness(async () => ++calls === 1
    ? response({ object: 'error', code: 'rate_limit', details: 'Too many requests' }, 429, { 'Retry-After': '3' })
    : response(list()));
  const first = client.search({ query: 'same' });
  const queued = client.collection(['a']);
  await assert.rejects(first, error => error.status === 429 && error.retryAfterSeconds === 3 && /3 seconds/.test(error.message));
  await assert.rejects(queued, error => error.status === 429 && error.retryAfterSeconds === 3);
  await assert.rejects(client.search({ query: 'same' }), error => error.status === 429);
  advance(2000);
  await assert.rejects(client.search({ query: 'same' }), error => error.retryAfterSeconds === 1);
  assert.equal(calls, 1);
  advance(1000);
  assert.equal((await client.search({ query: 'same' })).data.length, 1);
  assert.equal(calls, 2);
});

test('429 without Retry-After uses a 30-second cooldown and abort still wins', async () => {
  let calls = 0;
  const { client, advance } = harness(async () => { calls++; return response({ details: 'Busy' }, 429); });
  await assert.rejects(client.search({ query: 'first' }), error => error.retryAfterSeconds === 30);
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(client.search({ query: 'second' }, { signal: controller.signal }), { name: 'AbortError' });
  advance(29999);
  await assert.rejects(client.named('Plains'), error => error.retryAfterSeconds === 1);
  assert.equal(calls, 1);
});

test('429 cooldown honors HTTP-date headers even for a non-JSON error page', async () => {
  let calls = 0;
  const { client } = harness(async () => {
    calls++;
    return { ok: false, status: 429, headers: { get: () => 'Tue, 14 Nov 2023 22:13:25 GMT' }, json: () => { throw new Error('HTML proxy response'); } };
  });
  await assert.rejects(client.search({ query: 'one' }), error => error.retryAfterSeconds === 5);
  await assert.rejects(client.search({ query: 'two' }), error => error.retryAfterSeconds === 5);
  assert.equal(calls, 1);
});

test('named import requests an exact paper name, safely quotes it, and returns a card', async () => {
  let searched;
  const { client } = harness(async url => { searched = new URL(url); return response(list([card('paper')])); });
  const found = await client.named('A "Quoted" Name');
  assert.equal(searched.pathname, '/cards/search');
  assert.equal(searched.searchParams.get('q'), 'game:paper !"A \\"Quoted\\" Name"');
  assert.equal(found.id, 'paper');
  assert.ok(found.prices_checked_at);
  const empty = harness(async () => response(list([]))).client;
  await assert.rejects(empty.named('No paper card'), error => error.status === 404);
});

test('404 search is an empty state, exact-name lookup retains its error, malformed success fails visibly', async () => {
  const { client } = harness(async url => url.includes('broken') ? response({ unexpected: true }) : response({ details: 'No cards found' }, 404));
  assert.deepEqual(await client.search({ query: 'none' }), { object: 'list', data: [], total_cards: 0, has_more: false, next_page: null });
  await assert.rejects(client.named('none'), error => error.status === 404);
  await assert.rejects(client.search({ query: 'broken' }), /unreadable card/);
});

test('GET cache reuses successful results without exposing mutable cached cards', async () => {
  let calls = 0;
  const { client } = harness(async () => { calls++; return response(list()); });
  const one = await client.search({ query: 'cached' });
  one.data[0].prices.usd = '9999';
  const two = await client.search({ query: 'cached' });
  assert.equal(calls, 1);
  assert.equal(two.data[0].prices.usd, '1.25');
});

test('price collection deduplicates IDs and batches <=75 sequentially, retaining missing records', async () => {
  const batches = [];
  const { client } = harness(async (url, options) => {
    assert.equal(url, 'https://api.scryfall.com/cards/collection');
    assert.equal(options.method, 'POST');
    assert.equal(options.headers['Content-Type'], 'application/json');
    const ids = JSON.parse(options.body).identifiers.map(x => x.id);
    batches.push(ids);
    return response(list(ids.filter(id => id !== 'missing').map(card), { not_found: ids.includes('missing') ? [{ id: 'missing' }] : [] }));
  });
  const ids = Array.from({ length: 151 }, (_, i) => `id-${i}`);
  const refreshed = await client.collection([...ids, 'id-1', 'missing', '', null]);
  assert.deepEqual(batches.map(batch => batch.length), [75, 75, 2]);
  assert.equal(refreshed.data.length, 151);
  assert.deepEqual(refreshed.not_found, [{ id: 'missing' }]);
  assert.ok(refreshed.data.every(c => c.prices_checked_at));
  assert.deepEqual(await client.collection([]), { data: [], not_found: [] });
});
