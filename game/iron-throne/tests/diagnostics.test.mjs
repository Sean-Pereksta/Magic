import test from 'node:test';
import assert from 'node:assert/strict';
import worker, { DiplomacyBudget } from '../worker/worker.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { CHECK_NAMES, diagnosticReport, makeDiagnostic, readDiagnostic } from '../diagnostics.mjs';
import { createGame } from '../core.mjs';

const origin = 'https://catnmice.com', endpoint = 'https://worker.example/diplomacy';
const reply = { reply: 'Let us discuss the price of your claim.', tone: 'guarded', intents: [] };
const verified = () => Response.json({ success: true, hostname: 'catnmice.com', action: 'iron-throne' });
class Storage {
  data = new Map();
  async get(key) { return structuredClone(this.data.get(key)); }
  async put(key, value) { this.data.set(key, structuredClone(value)); }
  async transaction(fn) { return fn(this); }
}
function environment() {
  const storage = new Storage();
  const env = { ALLOWED_ORIGINS: origin, GEMINI_API_KEY: 'private-gemini-value', TURNSTILE_SECRET: 'private-turnstile-value' };
  const object = new DiplomacyBudget({ storage }, env);
  env.BUDGET = { idFromName: name => name, get: () => ({ fetch: (url, options) => object.fetch(new Request(url, options)) }) };
  return { env, storage };
}
function clientFor(env) {
  return new DiplomacyClient({ endpoint, fetcher: (url, options) => worker.fetch(new Request(url, { ...options,
    headers: { ...options.headers, Origin: origin, 'CF-Connecting-IP': '192.0.2.42' } }), env) });
}
function report(client) { return diagnosticReport(client.lastDiagnostic, endpoint, origin, { endpoint: true, siteKey: true }); }
const unknownChecks = Object.fromEntries(CHECK_NAMES.map(name => [name, 'unknown']));

test('session 503 identifies each missing runtime setting and survives the scripted fallback', async () => {
  for (const missing of [...CHECK_NAMES.map(name => [name]), CHECK_NAMES]) {
    const { env } = environment(); for (const name of missing) delete env[name];
    const client = clientFor(env);
    assert.equal(await client.openSession('private-verification-token'), false);
    const captured = client.lastDiagnostic, state = createGame(), before = JSON.stringify(state);
    const result = await client.send(state, 'wintermere', 'private-player-message');
    assert.equal(result.source, 'scripted'); assert.equal(result.diagnostic, captured);
    assert.equal(captured.path, '/session'); assert.equal(captured.httpStatus, 503); assert.equal(captured.code, 'CONFIG_MISSING');
    for (const name of CHECK_NAMES) assert.equal(captured.checks[name], missing.includes(name) ? 'missing' : 'present');
    const text = report(client);
    for (const name of missing) assert.ok(text.includes(`${name}: Missing`));
    assert.doesNotMatch(text, /private-|192\.0\.2\.42/); assert.equal(JSON.stringify(state), before);
    assert.match(text, /No keys, tokens/);
  }
});

test('Turnstile errors distinguish rejected secret, expired token, hostname, action and upstream failure', async () => {
  const original = globalThis.fetch;
  try {
    for (const [value, code, secretState] of [
      [{ success: false, 'error-codes': ['invalid-input-secret', 'private-error-value'] }, 'TURNSTILE_SECRET_INVALID', 'rejected'],
      [{ success: false, 'error-codes': ['timeout-or-duplicate'] }, 'TURNSTILE_TOKEN_EXPIRED', 'present'],
      [{ success: false, 'error-codes': ['invalid-input-response'] }, 'TURNSTILE_REJECTED', 'present'],
      [{ success: false, 'error-codes': ['internal-error'] }, 'TURNSTILE_UNAVAILABLE', 'present'],
      [{ success: true, hostname: 'wrong.example', action: 'iron-throne' }, 'TURNSTILE_HOSTNAME', 'verified'],
      [{ success: true, hostname: 'catnmice.com', action: 'wrong' }, 'TURNSTILE_ACTION', 'verified'],
      [null, 'TURNSTILE_UNAVAILABLE', 'present']
    ]) {
      const { env } = environment(); let calls = 0;
      globalThis.fetch = async url => { assert.match(url, /siteverify$/); calls++; if (!value) throw new Error('private-network-error'); return Response.json(value); };
      const client = clientFor(env); assert.equal(await client.openSession('private-token'), false);
      assert.equal(client.lastDiagnostic.code, code); assert.equal(client.lastDiagnostic.checks.TURNSTILE_SECRET, secretState);
      assert.equal(client.lastDiagnostic.checks.BUDGET, 'verified'); assert.equal(calls, 1);
      assert.doesNotMatch(report(client), /private-/);
    }
  } finally { globalThis.fetch = original; }
});

test('a present but broken Durable Object is reported separately from missing configuration or Gemini', async () => {
  const { env } = environment(); env.BUDGET.get = () => { throw new Error('private-binding-error'); };
  const client = clientFor(env); assert.equal(await client.openSession('private-token'), false);
  assert.equal(client.lastDiagnostic.code, 'BUDGET_FAILED'); assert.equal(client.lastDiagnostic.httpStatus, 503);
  assert.equal(client.lastDiagnostic.checks.BUDGET, 'failed'); assert.equal(client.lastDiagnostic.checks.GEMINI_API_KEY, 'present');
  assert.doesNotMatch(report(client), /private-/);
});

test('Google billing, quota, key, permission and model failures reach the client without raw provider output', async () => {
  const original = globalThis.fetch;
  try {
    for (const [status, error, code] of [
      [402, { message: 'private-billing-data' }, 'GEMINI_BILLING'],
      [429, { message: 'Prepay credits exhausted; private-project-data' }, 'GEMINI_BILLING'],
      [429, { message: 'Quota exceeded for private-project' }, 'GEMINI_QUOTA'],
      [429, { message: 'You exceeded your current quota, please check your plan and billing details.' }, 'GEMINI_QUOTA'],
      [400, { message: 'private-key-data', details: [{ reason: 'API_KEY_INVALID' }] }, 'GEMINI_KEY_INVALID'],
      [403, { message: 'private-project-data' }, 'GEMINI_PERMISSION'],
      [404, { message: 'private-model-data' }, 'GEMINI_MODEL'],
      [503, { message: 'private-unavailable-data' }, 'GEMINI_UNAVAILABLE']
    ]) {
      const { env, storage } = environment(); let modelCalls = 0;
      globalThis.fetch = async url => { if (url.includes('siteverify')) return verified(); modelCalls++; return Response.json({ error }, { status }); };
      const client = clientFor(env); assert.equal(await client.openSession('private-token'), true);
      assert.equal((await client.send(createGame(), 'wintermere', 'private-message')).source, 'scripted');
      assert.equal(client.lastDiagnostic.code, code); assert.equal(client.lastDiagnostic.httpStatus, 503);
      assert.equal(client.lastDiagnostic.providerStatus, status); assert.equal(client.lastDiagnostic.checks.BUDGET, 'verified');
      assert.equal(client.lastDiagnostic.checks.GEMINI_API_KEY, code === 'GEMINI_KEY_INVALID' ? 'rejected' : 'present');
      assert.equal(modelCalls, 1); assert.equal((await storage.get('budget')).used, 1);
      assert.doesNotMatch(report(client), /private-|192\.0\.2\.42/);
      const captured = client.lastDiagnostic;
      await client.send(createGame(), 'wintermere', 'another message');
      assert.equal(client.lastDiagnostic, captured); assert.equal(modelCalls, 1, 'diagnostics and cooldown do not retry the provider');
    }
  } finally { globalThis.fetch = original; }
});

test('local budgets have distinct diagnostics and consume no model calls', async () => {
  const original = globalThis.fetch;
  try {
    for (const [setting, code] of [['DAILY_LIMIT', 'DAILY_LIMIT'], ['REQUESTS_PER_MINUTE', 'GLOBAL_RATE_LIMIT'], ['CLIENT_PER_MINUTE', 'CLIENT_RATE_LIMIT']]) {
      const { env } = environment(); env[setting] = '0';
      globalThis.fetch = async url => { assert.match(url, /siteverify$/); return verified(); };
      const client = clientFor(env); assert.equal(await client.openSession('token'), true);
      await client.send(createGame(), 'wintermere', 'My claim?');
      assert.equal(client.lastDiagnostic.code, code); assert.equal(client.lastDiagnostic.httpStatus, 429);
      assert.equal(client.lastDiagnostic.providerStatus, undefined);
      if (setting === 'DAILY_LIMIT') {
        assert.deepEqual(client.lastDiagnostic.dailyBudget, { limit: 0, used: 0 });
        assert.match(report(client), /disabled by the Worker’s DAILY_LIMIT of 0/);
      }
    }
  } finally { globalThis.fetch = original; }
});

test('daily exhaustion preserves the midnight retry through the Worker, client and report', async t => {
  let now = Date.parse('2026-09-24T14:28:40Z'), modelCalls = 0;
  const midnight = Date.parse('2026-09-25T00:00:00Z');
  t.mock.method(Date, 'now', () => now);
  const { env, storage } = environment();
  await storage.put('budget', { day: '2026-09-24', used: 20, minute: Math.floor(now / 60000), calls: 0, clients: {}, cooldownUntil: 0 });
  t.mock.method(globalThis, 'fetch', async url => {
    if (url.includes('siteverify')) return verified();
    modelCalls++; return Response.json({ candidates: [{ finishReason: 'STOP', content: { parts: [{ text: JSON.stringify(reply) }] } }] });
  });
  const client = clientFor(env), state = createGame();
  await client.openSession('token');
  assert.equal((await client.send(state, 'wintermere', 'My claim?')).source, 'scripted');
  assert.deepEqual(client.lastDiagnostic.dailyBudget, { limit: 20, used: 20 });
  assert.equal(client.cooldownUntil, midnight);
  assert.match(report(client), /Shared Worker daily limit \(DAILY_LIMIT\): 20/);
  assert.match(report(client), /Attempts used this UTC day: 20/);
  assert.match(report(client), /Retry after: 2026-09-25T00:00:00.000Z/);
  assert.equal(modelCalls, 0);
  now += 3600000;
  assert.equal((await client.send(state, 'wintermere', 'After an hour?')).source, 'scripted');
  assert.equal(client.lastDiagnostic.at, Date.parse('2026-09-24T14:28:40Z'));
  assert.equal(modelCalls, 0);
  now = midnight;
  await client.openSession('fresh-token');
  assert.equal((await client.send(state, 'wintermere', 'A new day?')).source, 'gemini');
  assert.equal(client.lastDiagnostic, null); assert.equal(modelCalls, 1);
  assert.equal((await storage.get('budget')).used, 1);
});

test('daily budget diagnostics allow only bounded counts and strip unrelated fields', () => {
  assert.deepEqual(makeDiagnostic('DAILY_LIMIT', { dailyBudget: { limit: 20, used: 20, message: 'private-message', clients: 'private-client' } }).dailyBudget, { limit: 20, used: 20 });
  for (const dailyBudget of [null, 'private-key', { limit: '20', used: 20 }, { limit: 20, used: -1 }, { limit: 10001, used: 20 }, { limit: 20, used: 1.5 }]) {
    const record = makeDiagnostic('DAILY_LIMIT', { dailyBudget });
    assert.equal(record.dailyBudget, undefined);
    assert.doesNotMatch(diagnosticReport(record), /private-|Attempts used/);
  }
  assert.equal(makeDiagnostic('GEMINI_QUOTA', { dailyBudget: { limit: 20, used: 20 } }).dailyBudget, undefined);
});

test('storage errors after Gemini succeeds are not misreported as provider errors', async () => {
  const original = globalThis.fetch;
  try {
    const { env, storage } = environment(), put = storage.put.bind(storage);
    storage.put = async (key, value) => { if (key === 'responses') throw new Error('private-storage-error'); return put(key, value); };
    globalThis.fetch = async url => url.includes('siteverify') ? verified() : Response.json({ candidates: [{ finishReason: 'STOP', content: { parts: [{ text: JSON.stringify(reply) }] } }] });
    const client = clientFor(env); await client.openSession('token'); await client.send(createGame(), 'wintermere', 'My claim?');
    assert.equal(client.lastDiagnostic.code, 'BUDGET_FAILED'); assert.equal(client.lastDiagnostic.checks.BUDGET, 'failed');
    assert.doesNotMatch(report(client), /private-/);
  } finally { globalThis.fetch = original; }
});

test('legacy, blocked and non-JSON responses leave server settings unknown rather than guessing', async () => {
  for (const [fetcher, code] of [
    [async () => new Response('<html>private-error</html>', { status: 503 }), 'WORKER_UNAVAILABLE'],
    [async () => Response.json({ message: 'AI diplomacy is not configured.' }, { status: 503 }), 'WORKER_UNAVAILABLE'],
    [() => { throw new TypeError('private-fetch-error'); }, 'NETWORK_UNREADABLE'],
    [async () => Response.json({ token: '', expires: Date.now() + 999999 }), 'SESSION_RESPONSE_INVALID']
  ]) {
    let calls = 0;
    const client = new DiplomacyClient({ endpoint, fetcher: (...args) => { calls++; return fetcher(...args); } });
    assert.equal(await client.openSession('token'), false); assert.equal(await client.openSession('fresh-token'), false);
    assert.equal(calls, 2, 'a failed synchronous fetch cannot leave the shared handshake stuck');
    assert.equal(client.lastDiagnostic.code, code); assert.deepEqual(client.lastDiagnostic.checks, unknownChecks);
    assert.doesNotMatch(report(client), /private-/);
  }
});

test('diagnostic reports reject injected fields, credentials, query strings and arbitrary error codes', () => {
  const value = { ...makeDiagnostic('CONFIG_MISSING'), checks: { GEMINI_API_KEY: 'private-key', BUDGET: 'missing' }, providerStatus: 'private-data', turnstileCodes: ['private-token'], message: 'private-message' };
  const record = { ...readDiagnostic(value), at: Date.now(), path: '/session?token=private-token', httpStatus: 'private-status' };
  const text = diagnosticReport(record, 'https://name:private-key@worker.example/diplomacy', 'https://catnmice.com/?key=private-key');
  assert.doesNotMatch(text, /private-|name:|\?key=/); assert.match(text, /GEMINI_API_KEY: Unknown/); assert.match(text, /BUDGET: Missing/);
  assert.equal(readDiagnostic({ version: 1, code: { toString: null } }), null);
  assert.equal(readDiagnostic({ version: 1, code: 'private-code' }), null);
});

test('intentional local play has no diagnostic, malformed Gemini output is diagnosed, and recovery clears it', async () => {
  let calls = 0;
  const client = new DiplomacyClient({ endpoint, fetcher: async () => { calls++; return calls === 1 ? new Response('not-json') : Response.json(reply); } });
  client.session = { token: 'private-session', expires: Date.now() + 1800000 };
  const state = createGame();
  assert.equal((await client.send(state, 'wintermere', 'Local', '', false)).diagnostic, null); assert.equal(calls, 0);
  await client.send(state, 'wintermere', 'First'); assert.equal(client.lastDiagnostic.code, 'GEMINI_RESPONSE_INVALID');
  client.cooldownUntil = 0;
  assert.equal((await client.send(state, 'wintermere', 'Second')).source, 'gemini'); assert.equal(client.lastDiagnostic, null);
});
