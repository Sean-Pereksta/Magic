import { clickChatAction, revealChatAction } from './fixtures/chat-actions.mjs';
// Functional interaction tests only: no HTML previews, screenshots, or real Gemini calls.
import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createGame } from './fixtures/legacy-game.mjs';
import { relation } from '../core.mjs';
import { refreshKnowledge } from '../fog.mjs';
import { validateIntent } from '../diplomacy.mjs';

const require = createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES
  ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json` : import.meta.url);
const { chromium } = require('playwright');
const root = fileURLToPath(new URL('../../../', import.meta.url));
const saveKey = 'catnmice.iron-throne.v1';
const houses = ['wintermere', 'redharbor', 'thornwall'];
const server = http.createServer(async (req, res) => {
  try {
    const pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
    const file = path.resolve(root, '.' + (pathname.endsWith('/') ? pathname + 'index.html' : pathname));
    if (!file.startsWith(root)) throw Error('outside root');
    const body = await readFile(file);
    res.writeHead(200, { 'Content-Type': { '.html': 'text/html', '.mjs': 'text/javascript', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml' }[path.extname(file)] || 'application/octet-stream' });
    res.end(body);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
const saved = page => page.evaluate(key => JSON.parse(localStorage.getItem(key)), saveKey);
async function until(predicate, description) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  assert.fail(`Timed out waiting for ${description}`);
}
function fixture(formal = false) {
  const s = createGame(311);
  const roster = ['ashen', ...houses, 'sunspire', 'vesper'];
  s.kingdoms.sort((a, b) => roster.indexOf(a.id) - roster.indexOf(b.id));
  for (const h of houses) {
    s.treaties.push({ id: `ally-${h}`, type: 'alliance', parties: ['ashen', h], expires: 100 });
    for (const [a, b] of [[h, 'ashen'], ['ashen', h]]) Object.assign(relation(s, a, b), { trust: 95, opinion: 95, reliability: 95, grievance: 0 });
  }
  for (const k of s.kingdoms) Object.assign(k.resources, { food: 500, iron: 500, gold: 500 });
  if (formal) {
    s.treaties.push({ id: 'protected-sunspire', type: 'non-aggression', parties: ['redharbor', 'sunspire'], expires: 100 });
    // Refusal remains a refusal when the House cannot offer substitute support.
    relation(s, 'redharbor', 'ashen').trust = 10;
    s.pledges.push({ id: 'prior-duty', debtor: 'thornwall', creditor: 'ashen', intent: validateIntent({ type: 'POSITION', targetId: '5,6' }), created: 1, deadline: 10, status: 'pending', held: 0 });
  }
  refreshKnowledge(s);
  return s;
}
async function setup(browser, viewport, formal = false) {
  const context = await browser.newContext({ viewport });
  await context.addInitScript(({ s, saveKey }) => {
    localStorage.setItem(saveKey, JSON.stringify(s));
    let verification;
    globalThis.turnstile = {
      render: (_el, options) => { verification = options; queueMicrotask(() => options.callback('test-token')); return 1; },
      reset: () => queueMicrotask(() => verification.callback('fresh-test-token'))
    };
  }, { s: fixture(formal), saveKey });
  const page = await context.newPage(), errors = [], calls = [], active = new Set();
  let maxCouncilInFlight = 0, privateCalls = 0, failNextPrivate = false;
  page.on('pageerror', error => { errors.push(error.message); console.error(error.message); });
  await page.route('https://pub-*.r2.dev/**', route => route.fulfill({ status: 404, body: '' }));
  await page.route('**/game/iron-throne/config.json', route => route.fulfill({ json: { diplomacyEndpoint: 'https://worker.example/diplomacy', turnstileSiteKey: 'test-public-key' } }));
  await page.route('https://worker.example/session', route => route.fulfill({ json: { token: 'test-session', expires: Date.now() + 1800000 } }));
  await page.route('https://worker.example/diplomacy', async route => {
    const body = route.request().postDataJSON();
    if (body.mode !== 'allianceCouncil') {
      privateCalls++;
      if (failNextPrivate) {
        failNextPrivate = false;
        return route.fulfill({ status: 503, json: { diagnostics: { version: 1, code: 'GEMINI_TIMEOUT' } } });
      }
      return route.fulfill({ json: { reply: 'The private northern dispatch reached my court immediately.', tone: 'neutral', intents: [] } });
    }
    const speakers = body.world.participants.filter(p => p.ai).map(p => p.id);
    const call = { body, speakers, route, state: await saved(page), released: false };
    calls.push(call); active.add(call); maxCouncilInFlight = Math.max(maxCouncilInFlight, active.size);
  });
  await page.goto(`${base}/game/iron-throne/index.html`);
  await page.locator('#resume').click();
  await page.locator('[data-alliance]').first().click();
  await page.waitForFunction(() => document.getElementById('ai-status').textContent === 'Gemini council connected');
  const request = async index => { await until(() => calls.length > index, `council request ${index + 1}`); return calls[index]; };
  const release = async (call, message, timeout = false) => {
    assert.equal(call.released, false); call.released = true; active.delete(call);
    await call.route.fulfill(timeout
      ? { status: 503, json: { diagnostics: { version: 1, code: 'GEMINI_TIMEOUT', checks: { GEMINI_API_KEY: 'present', TURNSTILE_SECRET: 'verified', BUDGET: 'verified' } } } }
      : { json: { responses: [{ speakerHouseId: call.speakers[0], message }] } });
  };
  return { page, context, errors, calls, request, release, maxInFlight: () => maxCouncilInFlight, privateCalls: () => privateCalls,
    failPrivateOnce: () => { failNextPrivate = true; } };
}

let browser;
try {
  browser = await chromium.launch({ headless: true, executablePath: process.env.IRON_THRONE_CHROMIUM || undefined,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
  for (const viewport of [{ width: 1280, height: 900 }, { width: 390, height: 844 }]) {
    const test = await setup(browser, viewport), { page, request, release } = test;
    try {
      const message = 'Let each House describe the help it can provide.';
      await page.locator('#alliance-message').fill(message);
      await page.locator('.alliance-compose [type=submit]').click();
      const first = await request(0);
      assert.deepEqual(first.speakers, ['wintermere']);
      assert.equal(first.state.allianceCouncils[0].messages.at(-1).message, message, 'player anchor saved before first voice');
      assert.match(await page.locator('.alliance-notice').textContent(), /Wintermere/);
      assert.equal(await page.locator('.alliance-compose [type=submit]').isDisabled(), true);
      await release(first, 'Wintermere will listen to the rest of this council.');
      const second = await request(1);
      assert.deepEqual(second.speakers, ['redharbor']);
      assert.equal(second.state.allianceCouncils[0].messages.at(-1).message, 'Wintermere will listen to the rest of this council.', 'first answer committed before next model request');
      assert.ok(second.body.history.some(m => m.message === message));
      assert.ok(second.body.history.some(m => m.speakerHouseId === 'wintermere' && m.message.includes('listen to the rest')));
      assert.match(await page.locator('.alliance-history').textContent(), /Wintermere will listen/);
      assert.match(await page.locator('.alliance-notice').textContent(), /Redharbor/);

      // Closing this UI does not cancel the sequence or serialize private diplomacy.
      await page.locator('#alliance-council .close').click();
      await page.locator('[data-tab=council]').click();
      await page.locator('[data-talk=vesper]').click();
      test.failPrivateOnce();
      await page.locator('#chat-message').fill('A private greeting to your court.');
      await page.locator('#send-chat').click();
      await (await revealChatAction(page,'#retry-private-gemini')).waitFor({ state: 'visible' });
      const failedPrivate = await saved(page);
      assert.equal(failedPrivate.conversations.vesper.filter(m => m.role === 'ruler').length, 0, 'private failure has no local ruler substitute');
      assert.equal(failedPrivate.conversations.vesper.filter(m => m.role === 'player').length, 1);
      await clickChatAction(page,'#retry-private-gemini');
      await page.waitForFunction(() => document.getElementById('messages').textContent.includes('private northern dispatch'));
      const retriedPrivate = await saved(page);
      assert.equal(retriedPrivate.diplomacy.messages.regular, failedPrivate.diplomacy.messages.regular, 'private retry does not spend another envoy');
      assert.equal(retriedPrivate.conversations.vesper.filter(m => m.role === 'player').length, 1);
      assert.equal(retriedPrivate.conversations.vesper.find(m => m.role === 'ruler').source, 'gemini');
      assert.equal(test.privateCalls(), 2, 'explicit private Gemini retry finished while Redharbor remained in flight');
      assert.equal(second.released, false);
      await page.locator('#diplomacy .close').click();
      await page.locator('[data-alliance]').first().click();
      assert.equal(test.calls.length, 2, 'reopening does not launch duplicate council requests');
      assert.match(await page.locator('.alliance-history').textContent(), /Wintermere will listen/);
      assert.equal(await page.locator('.alliance-compose [type=submit]').isDisabled(), true);

      await release(second, '', true);
      const third = await request(2);
      assert.deepEqual(third.speakers, ['thornwall']);
      assert.equal(third.state.allianceCouncils[0].messages.at(-1).speakerHouseId, 'wintermere', 'a timeout adds no substitute ruler speech');
      assert.equal(third.state.allianceCouncils[0].messages.some(m => m.speakerHouseId === 'redharbor'), false);
      assert.ok(third.body.history.some(m => m.speakerHouseId === 'wintermere' && m.message.includes('listen to the rest')));
      assert.equal(third.body.history.some(m => m.speakerHouseId === 'redharbor'), false, 'later requests never receive fabricated fallback history');
      await release(third, 'Thornwall has heard Wintermere and will consider the supplies.');
      await page.waitForFunction(() => document.querySelector('.alliance-history').textContent.includes('Thornwall has heard Wintermere'));
      await until(async () => !(await page.locator('.alliance-compose [type=submit]').isDisabled()), 'completed council controls');
      const state = await saved(page), responses = state.allianceCouncils[0].messages.filter(m => m.speakerHouseId !== 'ashen');
      assert.deepEqual(responses.map(m => m.speakerHouseId), ['wintermere', 'thornwall']);
      assert.deepEqual(responses.map(m => m.source), ['gemini', 'gemini']);
      assert.equal(state.allianceCouncils[0].activeSequence.failed.redharbor.diagnostic.code, 'GEMINI_TIMEOUT');
      assert.doesNotMatch(await page.locator('.alliance-history').textContent(), /Local dialogue/);
      assert.match(await page.locator('.alliance-failed-replies').textContent(), /Redharbor.*Gemini/i);
      assert.equal(test.calls.length, 3, 'timeout has no paid retry'); assert.equal(test.maxInFlight(), 1);
      assert.equal(await page.locator('.alliance-diagnostics').evaluate(el=>!el.hidden), true, 'later success does not erase the middle timeout');
      await clickChatAction(page,'.alliance-diagnostics');
      assert.match(await page.locator('#diagnostics-report').inputValue(), /GEMINI_TIMEOUT/);
      await page.locator('#gemini-diagnostics-dialog .close').click();
      await clickChatAction(page,'.alliance-retry-gemini');
      const retry = await request(3);
      assert.deepEqual(retry.speakers, ['redharbor'], 'manual retry contacts the undelivered ruler only');
      assert.ok(retry.body.history.some(m => m.speakerHouseId === 'thornwall'), 'retry receives the current authoritative conversation');
      await release(retry, 'Redharbor has now received the council dispatch.');
      await until(async () => (await saved(page)).allianceCouncils[0].messages.some(m => m.speakerHouseId === 'redharbor'), 'explicit council Gemini retry');
      const retriedState = await saved(page), retriedCouncil = retriedState.allianceCouncils[0];
      assert.equal(retriedCouncil.messages.filter(m => m.speakerHouseId === 'redharbor').length, 1);
      assert.equal(retriedCouncil.messages.filter(m => m.speakerHouseId === 'ashen').length, 1, 'retry preserves the original player anchor');
      assert.ok(retriedCouncil.messages.filter(m => m.speakerHouseId !== 'ashen').every(m => m.source === 'gemini'));
      assert.equal(retriedState.diplomacy.messages.regular, state.diplomacy.messages.regular, 'retry does not charge a second envoy');
      assert.equal(test.calls.length, 4); assert.equal(test.maxInFlight(), 1);
      assert.deepEqual(test.errors, []);
      console.log(`PASS ${viewport.width}px: Gemini-only council history, isolated failures, explicit retry, close/reopen, and private independence`);
    } finally { await test.context.close(); }

    const formal = await setup(browser, viewport, true);
    try {
      const { page, request, release } = formal;
      await clickChatAction(page,'.alliance-offer-request');
      const builder = page.locator('#formal-proposal-builder');
      await builder.locator('[name=type]').selectOption('JOINT_WAR');
      await builder.locator('[name=target]').selectOption('sunspire');
      await builder.locator('[type=submit]').click();
      const expected = { wintermere: 'accepted', redharbor: 'declined', thornwall: 'alternative' };
      const spoken = [];
      for (let i = 0; i < 3; i++) {
        const call = await request(i), [house] = call.speakers;
        assert.equal(call.speakers.length, 1);
        const proposal = call.state.cooperation.formalProposals[0];
        assert.equal(proposal.responses[house].status, expected[house], 'game resolves decision before Gemini voice starts');
        assert.equal(call.body.world.formalDecision.status, expected[house]);
        for (const earlier of spoken) assert.ok(call.body.history.some(m => m.speakerHouseId === earlier.house && m.message === earlier.message.slice(0, 450)), 'later formal ruler hears earlier council voices');
        assert.equal(await page.locator('.alliance-history [data-formal-card]').count(), 1, 'one tracker stays in the conversation');
        assert.equal(await page.locator(`[data-formal-house=${house}]`).getAttribute('data-formal-status'), expected[house]);
        for (const later of proposal.requestedHouses.slice(i + 1)) assert.equal(proposal.responses[later].status, 'waiting');
        if (i === 0 && viewport.width > 700) {
          await page.locator('#alliance-council .close').click();
          await page.locator('[data-tab=council]').click();
          await page.locator('[data-talk=vesper]').click();
          await clickChatAction(page,'#private-offer-request');
          await builder.locator('[name=type]').selectOption('AID');
          await builder.locator('[name=direction]').selectOption('offer');
          await builder.locator('[name=resource0]').selectOption('food');
          await builder.locator('[name=amount0]').fill('1');
          await builder.locator('[type=submit]').click();
          await until(async () => (await saved(page)).cooperation.formalProposals.some(p => !p.councilId && p.responses.vesper?.spoken), 'independent private formal voice');
          assert.equal(formal.privateCalls(), 1);
          assert.equal(call.released, false, 'private formal voice completed without waiting for council');
          await page.locator('#diplomacy .close').click();
          await page.locator('[data-alliance]').first().click();
          assert.equal(formal.calls.length, 1, 'formal close/reopen does not duplicate the held request');
        }
        if (house === 'redharbor') await release(call, '', true);
        else await release(call, house === 'wintermere' ? 'I refuse to commit to this campaign.' : 'Thornwall can offer the recorded supplies instead.');
        await until(async () => (await saved(page)).cooperation.formalProposals[0].responses[house].voiceComplete, `${house} formal voice request settled`);
        const after = await saved(page), row = after.cooperation.formalProposals[0].responses[house];
        assert.equal(row.status, expected[house], 'voice failure or contradiction never changes the decision');
        const entries = after.allianceCouncils[0].messages.filter(m => m.formalProposalId === proposal.id && m.speakerHouseId === house && m.formalResponse);
        if (house === 'thornwall') {
          assert.equal(row.source, 'gemini'); assert.equal(entries.length, 1, 'valid Gemini formal voice is committed exactly once');
          spoken.push({ house, message: entries[0].message });
        } else {
          assert.equal(row.source, 'failed'); assert.equal(row.spoken, false);
          assert.equal(entries.length, 0, 'timeout or contradictory voice is withheld without synthetic ruler speech');
          assert.ok(!row.voice);
          assert.match(await page.locator(`[data-formal-house=${house}] .formal-voice-unavailable`).textContent(), /Gemini reply unavailable/);
        }
      }
      await until(async () => (await saved(formal.page)).cooperation.formalProposals[0].status === 'resolved', 'formal sequence completion');
      const card = await formal.page.locator('.alliance-history [data-formal-card]').textContent();
      for (const symbol of ['✓', '✕', '◐']) assert.ok(card.includes(symbol), `live tracker shows ${symbol}`);
      assert.match(card, /Alternative support/i); assert.doesNotMatch(card, /Declined · Alternative/);
      assert.equal(formal.calls.length, 3); assert.equal(formal.maxInFlight(), 1); assert.deepEqual(formal.errors, []);
      const beforeRetry = await saved(page);
      // Wintermere's schema-valid but contradictory voice was rejected. This
      // retry must reach Gemini again even though its model context is unchanged.
      await page.locator('[data-formal-retry][data-house=wintermere]').click();
      const retried = await request(3);
      assert.deepEqual(retried.speakers, ['wintermere'], 'explicit retry requests only the missing ruler');
      assert.equal(retried.body.world.formalDecision.status, 'accepted');
      await release(retried, 'I accept the recorded campaign commitment.');
      await until(async () => (await saved(page)).cooperation.formalProposals[0].responses.wintermere.spoken, 'explicit formal Gemini retry');
      const afterRetry = await saved(page), retriedProposal = afterRetry.cooperation.formalProposals[0];
      assert.equal(retriedProposal.responses.wintermere.status, 'accepted');
      assert.equal(retriedProposal.responses.wintermere.source, 'gemini');
      assert.equal(afterRetry.diplomacy.messages.regular, beforeRetry.diplomacy.messages.regular, 'voice retry does not charge another envoy');
      assert.deepEqual(afterRetry.wars, beforeRetry.wars); assert.deepEqual(afterRetry.pledges, beforeRetry.pledges);
      assert.equal(afterRetry.allianceCouncils[0].messages.filter(m => m.formalResponse && m.formalProposalId === retriedProposal.id && m.speakerHouseId === 'wintermere').length, 1);
      assert.equal(formal.calls.length, 4, 'successful Houses are never replayed during explicit retry');
      console.log(`PASS ${viewport.width}px: deterministic decisions, Gemini-only formal speech, live tracker, and explicit retry`);
    } finally { await formal.context.close(); }
  }

  const cancelled = await setup(browser, { width: 1280, height: 900 });
  try {
    const { page, request, release } = cancelled;
    await page.locator('#alliance-message').fill('Each House may report before the next turn.');
    await page.locator('.alliance-compose [type=submit]').click();
    await release(await request(0), 'Wintermere delivered this report before the turn changed.');
    const pending = await request(1), anchor = pending.state.allianceCouncils[0].activeSequence.anchorId;
    await page.locator('#alliance-council .close').click();
    assert.equal(await page.locator('#end-turn').isEnabled(), true, 'council processing does not freeze the game');
    await page.locator('#end-turn').click();
    await page.waitForFunction(key => JSON.parse(localStorage.getItem(key)).turn === 2, saveKey);
    const state = await saved(page), council = state.allianceCouncils.find(c => c.activeSequence?.anchorId === anchor);
    assert.equal(council.activeSequence.status, 'cancelled');
    assert.deepEqual(council.messages.filter(m => m.anchorMessageId === anchor).map(m => m.speakerHouseId), ['wintermere'], 'turn cancellation preserves the first committed answer');
    assert.equal(cancelled.calls.filter(c => c.body.turn === 1).length, 2, 'remaining old-turn rulers are never requested');
    assert.deepEqual(cancelled.errors, []);
    console.log('PASS end-turn cancellation: completed reply retained and remaining old-turn speakers stopped');
  } finally { await cancelled.context.close(); }
} finally {
  await browser?.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
}
