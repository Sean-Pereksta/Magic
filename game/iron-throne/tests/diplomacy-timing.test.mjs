import test from 'node:test';
import assert from 'node:assert/strict';
import { DiplomacyClient } from '../chat.mjs';
import { callGemini } from '../worker/worker.mjs';
import { makeCouncilContext } from '../alliance-council.mjs';
import { ownCouncil } from '../council-state.mjs';
import { createGame } from './fixtures/legacy-game.mjs';

const flush = () => new Promise(resolve => setImmediate(resolve));
const reply = { responses: [{ speakerHouseId: 'wintermere', message: 'Our scouts will help.' }] };
function fixture() {
  const state = createGame();
  state.treaties.push({ id: 'timing-alliance', type: 'alliance', parties: ['ashen', 'wintermere'], expires: 11 });
  const council = ownCouncil(state, 'ashen', true);
  return { state, options: { actorHouseId: 'ashen', councilId: council.id },
    context: makeCouncilContext(state, council, 'ashen', 'Help our scouts.') };
}

test('Worker allows a Council generation beyond the old deadline', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { context } = fixture(); let signal, finish;
  const pending = callGemini(context, {}, async (_url, init) => {
    signal = init.signal;
    await new Promise(resolve => { finish = resolve; });
    return Response.json({ candidates: [{ finishReason: 'STOP', content: { parts: [{ text: JSON.stringify(reply) }] } }] });
  });
  t.mock.timers.tick(45000);
  assert.equal(signal.aborted, false);
  finish(); assert.deepEqual(await pending, reply);
});

for (const [mode, deadline] of [['allianceCouncil', 60000], ['private', 12000]]) {
  test(`Worker still bounds a stalled ${mode} request`, async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const { context } = fixture();
    if (mode === 'private') { delete context.mode; context.rulerId = 'wintermere'; }
    let signal;
    const pending = callGemini(context, {}, (_url, init) => {
      signal = init.signal;
      return new Promise((_, reject) => signal.addEventListener('abort', () => reject(Error('aborted'))));
    });
    const checked = assert.rejects(pending, { diagnosticCode: 'GEMINI_TIMEOUT' });
    t.mock.timers.tick(deadline - 1); assert.equal(signal.aborted, false);
    t.mock.timers.tick(1); await checked;
  });
}

test('each queued Council request gets its own deadline after session renewal', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { state, options } = fixture(); const active = []; let renew;
  const c = new DiplomacyClient({ endpoint: 'https://worker.example/diplomacy', fetcher: async (url, init) => {
    if (url.endsWith('/session')) {
      await new Promise(resolve => { renew = resolve; });
      return Response.json({ token: 'renewed', expires: Date.now() + 1800000 });
    }
    return new Promise(resolve => active.push({ signal: init.signal, finish: () => resolve(Response.json(reply)) }));
  } });
  c.session = { token: 'expiring', expires: Date.now() + 60000 };
  const first = c.send(state, 'wintermere', 'first', '', true, options);
  const second = c.send(state, 'wintermere', 'second', '', true, options);
  await flush(); t.mock.timers.tick(9000); renew(); await flush();
  assert.equal(active.length, 1);
  t.mock.timers.tick(70000); assert.equal(active[0].signal.aborted, false);
  active[0].finish(); assert.equal((await first).source, 'gemini'); await flush();
  assert.equal(active.length, 2);
  t.mock.timers.tick(70000); assert.equal(active[1].signal.aborted, false);
  active[1].finish(); assert.equal((await second).source, 'gemini');
  assert.equal(c.controllers.size, 0);
});

test('browser timeout releases the Council queue for the next message', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { state, options } = fixture(); let calls = 0;
  const c = new DiplomacyClient({ endpoint: 'https://worker.example/diplomacy', fetcher: async (_url, { signal }) => {
    if (++calls > 1) return Response.json(reply);
    return new Promise((_, reject) => signal.addEventListener('abort', () => reject(Error('aborted'))));
  } });
  c.session = { token: 'valid', expires: Date.now() + 1800000 };
  const first = c.send(state, 'wintermere', 'first', '', true, options);
  const second = c.send(state, 'wintermere', 'second', '', true, options);
  await flush(); t.mock.timers.tick(75000);
  assert.equal((await first).diagnostic.code, 'REQUEST_TIMEOUT');
  assert.equal((await second).source, 'gemini');
  assert.equal(calls, 2); assert.equal(c.controllers.size, 0);
});
