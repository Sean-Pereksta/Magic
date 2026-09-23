import test from 'node:test';
import assert from 'node:assert/strict';
import worker, { DiplomacyBudget } from '../worker/worker.mjs';
import { issueSession, reserveSessionBudget, verifySession } from '../worker/session.mjs';
import { DiplomacyClient } from '../chat.mjs';
import { createGame } from '../core.mjs';
import { makeContext } from '../diplomacy.mjs';

const origin = 'https://catnmice.com';
const reply = { reply: 'Your banners are close to Frostwatch. Tell me why.', tone: 'guarded', intents: [], speechAct: 'question', relationshipSignals: ['border_concern'], memoryCandidates: [] };
class Storage {
  data = new Map(); queue = Promise.resolve();
  async get(key) { return structuredClone(this.data.get(key)); }
  async put(key, value) { this.data.set(key, structuredClone(value)); }
  async transaction(fn) { const result = this.queue.then(() => fn(this)); this.queue = result.catch(() => {}); return result; }
}
function environment() {
  const storage = new Storage();
  const env = { ALLOWED_ORIGINS: origin, GEMINI_API_KEY: 'test-provider-key', TURNSTILE_SECRET: 'test-turnstile-secret', SESSION_SECRET: 'test-independent-signing-secret', DAILY_LIMIT: '20', REQUESTS_PER_MINUTE: '20', CLIENT_PER_MINUTE: '20' };
  const object = new DiplomacyBudget({ storage }, env);
  env.BUDGET = { idFromName: name => name, get: () => ({ fetch: (url, options) => object.fetch(new Request(url, options)) }) };
  return { env, storage };
}
function request(path, body, token = '', extra = {}) {
  return new Request(`https://worker.example${path}`, { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json', 'CF-Connecting-IP': '192.0.2.1', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...extra }, body: JSON.stringify(body) });
}
test('signed sessions enforce signature, origin, client binding, idle expiry and absolute lifetime', async () => {
  const env = { TURNSTILE_SECRET: 'test-session-secret' }, now = 100000000;
  const session = await issueSession(env, origin, 'client', now);
  assert.ok(await verifySession(session.token, env, origin, 'client', now + 1));
  assert.equal(await verifySession(`${session.token.slice(0, -4)}AAAA`, env, origin, 'client', now), null);
  assert.equal(await verifySession(session.token, env, 'https://other.example', 'client', now), null);
  assert.equal(await verifySession(session.token, env, origin, 'other', now), null);
  assert.equal(await verifySession(session.token, env, origin, 'client', session.expires), null);
  const claims = await verifySession(session.token, env, origin, 'client', now + 1);
  const renewed = await issueSession(env, origin, 'client', now + 20 * 60000, claims);
  assert.ok(renewed.expires > session.expires);
  const end = await issueSession(env, origin, 'client', now + 8 * 3600000, claims);
  assert.equal(await verifySession(end.token, env, origin, 'client', now + 8 * 3600000), null);
  assert.equal(await verifySession(session.token, { TURNSTILE_SECRET: 'rotated' }, origin, 'client', now), null);
});
test('one Turnstile verification authorizes multiple messages and refreshes, with no token replay', async () => {
  const { env, storage } = environment(), original = globalThis.fetch; let checks = 0, modelCalls = 0;
  try {
    globalThis.fetch = async (url, options) => {
      if (String(url).includes('siteverify')) { checks++; assert.equal(JSON.parse(options.body).response, 'single-use-token'); return Response.json({ success: true, hostname: 'catnmice.com', action: 'iron-throne' }); }
      modelCalls++; return Response.json({ candidates: [{ finishReason: 'STOP', content: { parts: [{ text: JSON.stringify(reply) }] } }] });
    };
    const sessionResponse = await worker.fetch(request('/session', { turnstileToken: 'single-use-token' }), env);
    assert.equal(sessionResponse.status, 200); const session = await sessionResponse.json(); assert.ok(session.token);
    assert.equal((await storage.get('budget')), undefined, 'session creation never consumes Gemini budget');
    for (const message of ['Hello.', 'What do you need?']) {
      const body = makeContext(createGame(), 'wintermere', message);
      const response = await worker.fetch(request('/diplomacy', body, session.token), env);
      assert.equal(response.status, 200); assert.equal((await response.json()).reply, reply.reply);
      assert.ok(response.headers.get('X-Diplomacy-Session')); assert.match(response.headers.get('Access-Control-Expose-Headers'), /X-Diplomacy-Session/);
    }
    const refresh = await worker.fetch(request('/session', {}, session.token), env); assert.equal(refresh.status, 200);
    assert.equal(checks, 1); assert.equal(modelCalls, 2); assert.equal((await storage.get('budget')).used, 2);
    const noSession = await worker.fetch(request('/diplomacy', { ...makeContext(createGame(), 'wintermere', 'again'), turnstileToken: 'single-use-token' }), env);
    assert.equal(noSession.status, 401); assert.equal(checks, 1); assert.equal(modelCalls, 2);
  } finally { globalThis.fetch = original; }
});
test('invalid sessions, disallowed origins and wrong verification hostname/action are rejected before the model', async () => {
  const { env } = environment(), original = globalThis.fetch; let calls = 0;
  try {
    globalThis.fetch = async () => { calls++; return Response.json({ success: true, hostname: 'evil.example', action: 'iron-throne' }); };
    assert.equal((await worker.fetch(request('/session', { turnstileToken: 'test' }), env)).status, 403);
    globalThis.fetch = async () => { calls++; return Response.json({ success: true, hostname: 'catnmice.com', action: 'some-other-widget' }); };
    assert.equal((await worker.fetch(request('/session', { turnstileToken: 'test' }), env)).status, 403);
    assert.equal((await worker.fetch(request('/diplomacy', makeContext(createGame(), 'wintermere', 'Hi'), 'forged-token'), env)).status, 401);
    assert.equal((await worker.fetch(request('/session', {}, 'forged-token'), env)).status, 401);
    assert.equal((await worker.fetch(request('/session', {}, '', { Origin: 'https://evil.example' }), env)).status, 403);
    assert.equal(calls, 2, 'invalid sessions never reach Turnstile or Gemini');
  } finally { globalThis.fetch = original; }
});
test('session issuance throttles independently of model budget and is safe under concurrent requests', async () => {
  const storage = new Storage(), now = 10000000;
  const results = await Promise.all(Array.from({ length: 15 }, () => reserveSessionBudget(storage, 'same-client', now)));
  assert.equal(results.filter(Boolean).length, 6); assert.equal(await storage.get('budget'), undefined);
  assert.equal(await reserveSessionBudget(storage, 'same-client', now + 60000), true);
});
test('the browser shares one session handshake, omits verification tokens from messages and falls back after expiry', async () => {
  let now = 10000000, sessions = 0, messages = 0;
  const client = new DiplomacyClient({ endpoint: 'https://worker.example', now: () => now, fetcher: async (url, options) => {
    const data = JSON.parse(options.body);
    if (url.endsWith('/session')) { sessions++; assert.equal(data.turnstileToken, 'test-verification'); return Response.json({ token: 'test-signed-session', expires: now + 1800000 }); }
    messages++; assert.equal(options.headers.Authorization, 'Bearer test-signed-session'); assert.equal(data.turnstileToken, undefined); return Response.json(reply);
  } });
  const ready = await Promise.all([client.openSession('test-verification'), client.openSession('test-verification')]); assert.deepEqual(ready, [true, true]); assert.equal(sessions, 1);
  const s = createGame(), before = JSON.stringify(s);
  assert.equal((await client.send(s, 'wintermere', 'one', '', true)).source, 'gemini');
  assert.equal((await client.send(s, 'wintermere', 'two', '', true)).source, 'gemini');
  assert.equal(messages, 2); assert.equal(sessions, 1); assert.equal(JSON.stringify(s), before);
  now += 1800001; assert.equal((await client.send(s, 'wintermere', 'three', '', true)).source, 'scripted'); assert.equal(messages, 2);
});
test('server-rejected session is cleared without automatic model retry or consumed-token reuse', async () => {
  const client = new DiplomacyClient({ endpoint: 'https://worker.example/diplomacy', fetcher: async () => new Response('{}', { status: 401 }) });
  client.session = { token: 'expired-server-side', expires: Date.now() + 1800000 };
  const out = await client.send(createGame(), 'wintermere', 'An alliance?', '', true);
  assert.equal(out.source, 'scripted'); assert.equal(client.session, null); assert.match(out.notice, /verification/);
});
