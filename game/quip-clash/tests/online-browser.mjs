import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root = fileURLToPath(new URL('../../..', import.meta.url));
const require = createRequire(process.env.QUIP_DEPENDENCIES ? path.resolve(process.env.QUIP_DEPENDENCIES, 'package.json') : path.join(root, 'package.json'));
const { chromium } = require(process.env.PLAYWRIGHT_PACKAGE || 'playwright');
const sdk = require('firebase/firestore');
const { initializeTestEnvironment } = require('@firebase/rules-unit-testing');
const base = process.env.QUIP_PREVIEW_URL || 'http://127.0.0.1:8790';
const [host = '127.0.0.1', rawPort = '8189'] = (process.env.FIRESTORE_EMULATOR_HOST || '').split(':');
const env = await initializeTestEnvironment({ projectId: 'demo-quip-clash', firestore: { host, port: Number(rawPort) } });
const browser = await chromium.launch({ headless: true, ...(process.env.QUIP_BROWSER_CHANNEL ? { channel: process.env.QUIP_BROWSER_CHANNEL } : {}) });
const errors = [], pages = [], contexts = [];
let code;
const observerDb = env.authenticatedContext('browser-test-observer').firestore();
const read = async () => (await sdk.getDocFromServer(sdk.doc(observerDb, 'lobbies', code))).data();
async function until(condition, label, timeout = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeout) { if (await condition()) return; await new Promise(resolve => setTimeout(resolve, 100)); }
  throw new Error(`Timed out: ${label}`);
}
async function expire() {
  await env.withSecurityRulesDisabled(context => sdk.updateDoc(sdk.doc(context.firestore(), 'lobbies', code), { phaseStartedAt: sdk.Timestamp.fromMillis(Date.now() - 120000) }));
}
try {
  for (let i = 0; i < 4; i++) {
    const context = await browser.newContext({ viewport: { width: i === 0 ? 390 : 320, height: 844 }, isMobile: true, hasTouch: true }); contexts.push(context);
    const page = await context.newPage(); pages.push(page);
    page.on('pageerror', error => errors.push(error.message));
    // Only the Firebase adapter is replaced: the production app, network,
    // reducer, timers, UI and SDK all run against the local emulator.
    await page.route('**/quip-clash/firebase.mjs', route => route.fulfill({ contentType: 'text/javascript', body: `
      import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js';
      import * as firestore from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js';
      export const sdk = firestore;
      export const db = firestore.getFirestore(initializeApp({ projectId: 'demo-quip-clash', apiKey: 'test-key', appId: 'test-app' }));
      firestore.connectFirestoreEmulator(db, ${JSON.stringify(host)}, ${Number(rawPort)}, { mockUserToken: { sub: 'browser-player-${i}', user_id: 'browser-player-${i}' } });
      export async function loadIdentity() { return { uid: 'browser-player-${i}', username: 'BrowserPlayer${i}', displayName: 'Funny Friend ${i + 1}' }; }
    ` }));
    await page.goto(`${base}/game/quip-clash.html?mode=browse`);
    await page.locator('form[data-form="create"]').waitFor({ timeout: 30000 });
  }
  await pages[0].locator('#room-name').fill('Browser test party');
  await pages[0].locator('form[data-form="create"] button').click();
  await pages[0].locator('.invite-code').waitFor(); code = (await pages[0].locator('.invite-code').textContent()).trim();
  for (const page of pages.slice(1)) { await page.locator('#room-code').fill(code); await page.locator('form[data-form="join"] button').click(); await page.locator('.invite-code').waitFor(); }
  await until(async () => (await read()).roster.length === 4, 'all players joined');
  await pages[0].locator('[data-action="start"]').click();
  for (let round = 1; round <= 3; round++) {
    for (let i = 0; i < pages.length; i++) {
      const page = pages[i]; await page.locator('textarea').first().waitFor();
      const inputs = page.locator('textarea'); assert.equal(await inputs.count(), round === 3 ? 1 : 2);
      for (let j = 0; j < await inputs.count(); j++) await inputs.nth(j).fill(`Friend ${i + 1}, round ${round}: ${'a ridiculous twist '.repeat(20)}`);
      await page.locator('[data-submit-answers]').click();
    }
    await until(async () => (await read()).phase === 'voting', 'automatic writing advance');
    let room = await read();
    for (let questionIndex = 0; questionIndex < room.questions.length; questionIndex++) {
      room = await read();
      assert.equal(room.questionIndex, questionIndex);
      const authors = room.questions[questionIndex].authors;
      for (let i = 0; i < pages.length; i++) {
        const page = pages[i]; await page.locator('[data-live-screen="voting"]').waitFor();
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'no horizontal overflow');
        if (round < 3 && authors.includes(`browser-player-${i}`)) { assert.equal(await page.locator('[data-action="vote"]:enabled').count(), 0); continue; }
        await page.locator('[data-action="vote"]:enabled').first().click();
      }
      await until(async () => (await read()).phase === 'reveal', 'automatic vote advance');
      const results = await read();
      assert.ok(results.roster.every(p => Number.isInteger(p.score)));
      await expire();
      await until(async () => (await read()).phase !== 'reveal', 'automatic reveal advance');
    }
    room = await read(); assert.equal(room.phase, round === 3 ? 'finished' : 'scores');
    for (const page of pages) await page.locator(`[data-live-screen="${room.phase}"]`).waitFor();
    if (round < 3) { await expire(); await until(async () => (await read()).phase === 'writing', 'next round auto advance'); }
  }
  let final = await read();
  assert.equal(Object.keys(final.receipts).length, 9);
  assert.equal(final.roster.reduce((sum, p) => sum + p.score, 0), 15000);
  await pages[2].reload(); await pages[2].locator('[data-live-screen="finished"]').waitFor();
  assert.equal((await read()).roster.length, 4);
  assert.deepEqual(errors, []);
  console.log('PASS: four mobile browsers completed all rounds with synced 15,000 total points, automatic progression, author restrictions, and final reconnect.');
} finally { for (const context of contexts) await context.close(); await browser.close(); await env.cleanup(); }
