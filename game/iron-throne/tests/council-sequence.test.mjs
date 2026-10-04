import test from 'node:test';
import assert from 'node:assert/strict';
import { createCouncilSequenceRunner, runCouncilSequence, COUNCIL_RESPONSE_GAP_MS } from '../council-sequence.mjs';

const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const tick = () => new Promise(resolve => setImmediate(resolve));

test('game sequence commits and renders each answer before its gap and next request', async () => {
  const houses = ['wintermere', 'redharbor', 'thornwall'], calls = [], history = [], rendered = [], pauses = [];
  const replies = houses.map(deferred), gaps = [deferred(), deferred()];
  let index = 0, inFlight = 0, maxInFlight = 0, finished = false;
  const running = runCouncilSequence({
    isCurrent: () => true, next: () => houses[index], consider: () => ({ ok: true }),
    request: async house => {
      maxInFlight = Math.max(maxInFlight, ++inFlight);
      calls.push({ house, history: [...history] });
      await replies[index].promise; inFlight--;
      if (house === 'redharbor') return { source: 'failed', diagnostic: { code: 'GEMINI_TIMEOUT' } };
      return { source: 'gemini', message: house };
    },
    commit: (house, response) => { if(response.source==='gemini')history.push(response.message); index++; return { ok: true }; },
    onCommit: (house, response) => rendered.push({ house, ...response }),
    pause: ms => { pauses.push(ms); return gaps[pauses.length - 1].promise; },
    finish: () => { finished = true; }
  });
  await tick(); assert.equal(calls.length, 1);
  replies[0].resolve(); await tick();
  assert.deepEqual(history, ['wintermere']); assert.equal(rendered.length, 1); assert.equal(calls.length, 1);
  gaps[0].resolve(); await tick();
  assert.deepEqual(calls[1], { house: 'redharbor', history: ['wintermere'] });
  replies[1].resolve(); await tick();
  assert.equal(rendered[1].diagnostic.code, 'GEMINI_TIMEOUT');
  gaps[1].resolve(); await tick();
  assert.deepEqual(calls[2].history, ['wintermere']);
  replies[2].resolve(); assert.deepEqual(await running, { ok: true });
  assert.equal(maxInFlight, 1); assert.equal(calls.length, 3); assert.equal(finished, true);
  assert.deepEqual(pauses, [COUNCIL_RESPONSE_GAP_MS, COUNCIL_RESPONSE_GAP_MS]);
});

test('a changed campaign during a request keeps earlier commits and cancels remaining rulers', async () => {
  const second = deferred(), rows = []; let current = true, calls = 0;
  const result = runCouncilSequence({
    isCurrent: () => current, next: () => ['wintermere', 'redharbor', 'thornwall'][rows.length],
    consider: () => ({ ok: true }), gapMs: 0,
    request: async house => { calls++; if (house === 'redharbor') await second.promise; return { message: house }; },
    commit: (house, response) => { rows.push(response.message); return { ok: true }; }
  });
  await tick(); assert.deepEqual(rows, ['wintermere']);
  current = false; second.resolve();
  assert.equal((await result).cancelled, true); assert.equal(calls, 2); assert.deepEqual(rows, ['wintermere']);
});

test('unexpected voicing failure records delivery failure and proceeds without synthetic speech or a paid retry', async () => {
  let index = 0; const requests = [], commits = [];
  await runCouncilSequence({ isCurrent: () => true, next: () => ['wintermere', 'redharbor'][index], gapMs: 0,
    consider: () => ({ ok: true }), request: house => { requests.push(house); if (!index) throw Error('network'); return { source: 'gemini' }; },
    fallback: () => ({ source: 'failed' }), commit: (house, result) => { commits.push(result.source); index++; return { ok: true }; } });
  assert.deepEqual(requests, ['wintermere', 'redharbor']); assert.deepEqual(commits, ['failed', 'gemini']);
});

test('local sequence ownership prevents duplicate processors and stale releases', () => {
  const runner = createCouncilSequenceRunner(), old = runner.acquire('council-1');
  assert.equal(runner.acquire('council-1'), null);
  assert.ok(runner.acquire('council-2'));
  runner.clear(); const next = runner.acquire('council-1'); runner.release(old);
  assert.equal(runner.busy('council-1'), true); runner.release(next); assert.equal(runner.busy('council-1'), false);
});
