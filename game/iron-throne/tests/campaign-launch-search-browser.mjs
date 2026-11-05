import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { createGame } from './fixtures/legacy-game.mjs';
import { ownCouncil } from '../council-state.mjs';
import { refreshKnowledge } from '../fog.mjs';

const require = createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? `${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json` : import.meta.url);
const { chromium } = require('playwright');
const root = path.resolve(fileURLToPath(new URL('../../../', import.meta.url)));
const server = http.createServer(async (req, res) => {
  try {
    const file = path.resolve(root, '.' + new URL(req.url, 'http://localhost').pathname);
    if (!file.startsWith(root + path.sep)) throw Error('Path');
    const body = await readFile(file);
    res.writeHead(200, { 'Content-Type': { '.html':'text/html', '.mjs':'text/javascript', '.css':'text/css', '.svg':'image/svg+xml', '.json':'application/json' }[path.extname(file)] || 'application/octet-stream' });
    res.end(body);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const url = `http://127.0.0.1:${server.address().port}/game/iron-throne/index.html`;
const browser = await chromium.launch({headless:true, executablePath:process.env.IRON_THRONE_CHROMIUM || undefined, args:['--no-sandbox']});
const saveKey = 'catnmice.iron-throne.v1';
try {
  for (const viewport of [{width:1280,height:850}, {width:390,height:844}, {width:844,height:390}]) {
    const context = await browser.newContext({viewport});
    // Hold every decorative image request. Interactivity must be independent.
    await context.route('https://pub-*.r2.dev/**', () => {});
    await context.route('**/config.json', route => route.fulfill({json:{}}));
    const page = await context.newPage(), errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(url, {waitUntil:'domcontentloaded'});
    await page.locator('#launch-new').waitFor({state:'visible',timeout:10000});
    assert.equal(await page.locator('#resume').isDisabled(), true);
    assert.equal(await page.locator('#launch-loading').isVisible(), false);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    assert.equal(await page.locator('#launch-menu #custom-battle').isVisible(), true);
    await page.locator('#custom-battle').click();
    await page.locator('#custom-setup').waitFor({state:'visible'});
    await page.locator('#custom-close').click();
    assert.equal(await page.locator('#launch-new').isVisible(), true);
    const crest = await page.locator('.launch-crest').boundingBox();
    await page.locator('#launch-new').click();
    assert.deepEqual(await page.locator('.launch-crest').boundingBox(), crest, 'crest remains anchored when menu changes');
    await page.locator('#game-size').selectOption('12');
    await page.locator('#difficulty').selectOption('hard');
    await page.locator('#start-game').click();
    await page.locator('#welcome').waitFor({state:'hidden'});
    assert.equal(await page.locator('#turn').textContent(), 'Founding');
    const save = await page.evaluate(key => JSON.parse(localStorage.getItem(key)), saveKey);
    assert.equal(save.kingdoms.length, 12); assert.equal(save.difficulty, 'hard');
    await page.locator('#map').focus(); await page.keyboard.press('ArrowRight');
    await page.locator('#zoom-in').click();
    await page.locator('#menu-button').click(); await page.locator('#save-now').click();
    const position = await page.evaluate(key => JSON.parse(localStorage.getItem(key)).presentation.board, saveKey);
    await page.reload({waitUntil:'domcontentloaded'});
    await page.locator('#resume').click(); await page.locator('#welcome').waitFor({state:'hidden'});
    assert.ok((await page.locator('#coordinates').textContent()).endsWith(position.selected));
    await page.locator('#menu-button').click(); await page.locator('#save-now').click();
    const restored = await page.evaluate(key => JSON.parse(localStorage.getItem(key)).presentation.board, saveKey);
    assert.deepEqual(restored, position, 'Continue restores selection, camera, zoom and panel');
    await page.locator('[data-close=menu]').click();
    const replacementGold = await page.evaluate(key => {
      const replacement = JSON.parse(localStorage.getItem(key));
      replacement.kingdoms[0].resources.gold += 7;
      localStorage.setItem(key, JSON.stringify(replacement));
      return replacement.kingdoms[0].resources.gold;
    }, saveKey);
    await page.reload({waitUntil:'domcontentloaded'});
    await page.locator('#resume').click(); await page.locator('#welcome').waitFor({state:'hidden'});
    assert.equal(await page.evaluate(key => JSON.parse(localStorage.getItem(key)).kingdoms[0].resources.gold, saveKey), replacementGold, 'closing the older page preserves an externally replaced save');
    assert.deepEqual(errors, []);
    console.log(`PASS ${viewport.width}×${viewport.height} menu with stalled artwork, new 12-House campaign and saved position`);
    await context.close();
  }
  const context = await browser.newContext({viewport:{width:1280,height:850}});
  const saved = createGame();
  saved.treaties.push({id:'alliance',type:'alliance',parties:['ashen','wintermere'],expires:100});
  saved.wars.push('sunspire:wintermere'); refreshKnowledge(saved);
  const council = ownCouncil(saved, 'ashen', true);
  await context.addInitScript(({key,saved}) => localStorage.setItem(key, JSON.stringify(saved)), {key:saveKey,saved});
  await context.route('https://pub-*.r2.dev/**', route => route.fulfill({status:404,body:''}));
  await context.route('**/config.json', route => route.fulfill({json:{}}));
  const page = await context.newPage(), errors = [];
  page.on('pageerror',error=>errors.push(error.message));
  await page.goto(url); await page.locator('#resume').click(); await page.locator('#welcome').waitFor({state:'hidden'});
  await page.locator('[data-tab=council]').click(); await page.locator('[data-talk=wintermere]').click();
  for (const mode of ['private','council']) {
    if (mode === 'council') {
      await page.locator('[data-close=diplomacy]').click();
      await page.locator(`#dispatch-bar [data-alliance="${council.id}"]`).click();
    }
    const dialog = page.locator(mode==='private'?'#diplomacy':'#alliance-council');
    const draft = dialog.locator(mode==='private'?'#chat-message':'textarea').first();
    await draft.fill('Keep this unsent chat draft.');
    const menu = dialog.locator('.chat-options');
    for (const [query, result, type, resource] of [
      ['Buy iron','Buy iron · resource exchange','EXCHANGE','iron'],
      ['Passage','Open borders / military access','ACCESS',null],
      ['Gift food','Gift food · offer aid','AID','food']
    ]) {
      await menu.locator('summary').first().click();
      const search = menu.getByPlaceholder('Search diplomatic actions…');
      assert.equal(await search.evaluate(input=>input===document.activeElement),true);
      await search.fill(query);
      await menu.getByRole('button',{name:new RegExp(result.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'))}).click();
      const form = page.locator('#formal-proposal-builder');
      assert.equal(await form.locator('[name=type]').inputValue(),type);
      if (resource) assert.equal(await form.locator('[name=resource0]').inputValue(),resource);
      if (query==='Buy iron') assert.equal(await form.locator('[name=direction]').inputValue(),'request');
      assert.equal(await draft.inputValue(),'Keep this unsent chat draft.');
      await form.locator('[data-formal-close]').first().click();
    }
    await menu.locator('summary').first().click();
    await menu.getByPlaceholder('Search diplomatic actions…').fill('Declare');
    const labels = await menu.locator('.diplomatic-search-results strong').allTextContents();
    assert.ok(labels.includes('Promise to enter a war'));
    assert.equal(labels.includes('Declare war'),mode==='private');
    assert.equal(labels.includes('Break treaties and declare war'),mode==='private');
    await menu.getByPlaceholder('Search diplomatic actions…').fill('Peace');
    assert.ok((await menu.locator('.diplomatic-search-results').textContent()).includes('Sunspire'));
    await page.keyboard.press('Escape');
    const current = await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),saveKey);
    assert.equal(current.cooperation.formalProposals?.length||0,0,'search never sends proposals');
    console.log(`PASS ${mode} action search, correct terms, declaration choices, named peace targets and draft preservation`);
  }
  assert.deepEqual(errors,[]); await context.close();
  const recovery = await browser.newContext();
  await recovery.addInitScript(key=>localStorage.setItem(key,'broken save'),saveKey);
  await recovery.route('https://pub-*.r2.dev/**', route=>route.fulfill({status:404,body:''}));
  const page2=await recovery.newPage(); await page2.goto(url);
  await page2.locator('#launch-retry').waitFor({state:'visible'});
  await page2.locator('#launch-retry').click();
  await page2.locator('#launch-return').click();
  await page2.locator('#launch-new').waitFor({state:'visible'});
  console.log('PASS damaged-save Retry and Return to Main Menu');
  await recovery.close();
} finally { await browser.close(); server.closeAllConnections(); await new Promise(resolve=>server.close(resolve)); }
