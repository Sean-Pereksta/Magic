#!/usr/bin/env node
'use strict';

// Exercise the real solo and multiplayer pages. Firebase is replaced with an
// in-memory transport; this test never signs in or writes to a live game room.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');

function playwright() {
  try { return require('playwright'); }
  catch (error) {
    if (!process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES) throw error;
    return require(path.join(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES, 'playwright'));
  }
}

const root = path.resolve(__dirname, '..');
const multiplayerHooks = `
globalThis.__cardRealmsTest = {
  reset() {
    uid = 'test-player'; roomId = 'test-room'; mySeat = 0; myName = 'Test'; isHost = false;
    state = initMatchState({seatUid0:uid, seatName0:'Test', seatUid1:'test-opponent', seatName1:'Opponent'});
    state.currentSeat = 0; state.ended = false; actionsRef = {path:'test-actions'};
    const me = state.players[0];
    me.trade = 10; me.combat = 4; me.hand = ['scout','viper','explorer','tf_courier','se_infiltrator'].map(k=>mkInst(state,k));
    me.bases = [mkInst(state,'relay')]; me.bases[0].curDefense = 2;
    me.inPlay = []; me.discard = []; me.scrap = [];
    state.players[1].bases = [mkInst(state,'tf_sanctuary')];
    state.tradeRow = ['freighter','vs_shroud','mc_junksiphon','tf_medical','sd_yard'].map(k=>mkInst(state,k));
    globalThis.__firebaseWrites = [];
    showNetOverlay(false); renderAll();
  },
  setTrade(n) { state.players[0].trade = n; renderAll(); },
  renderAll,
  showPile: () => showPile('Your Deck', state.players[0].deck),
};
`;

const firebaseStub = `
export const initializeApp = () => ({});
export const getAuth = () => ({});
export const getDatabase = () => ({});
export const signInAnonymously = async () => ({});
export const onAuthStateChanged = () => () => {};
export const ref = (_db,path) => ({path});
export const push = (parent) => ({parent,key:'test-action'});
export const set = async (target,value) => { (globalThis.__firebaseWrites ||= []).push({target,value}); };
export const update = set;
export const get = async () => ({val:()=>null,exists:()=>false});
export const remove = async () => {};
export const onValue = (target,callback) => { if(target.path==='/.info/serverTimeOffset') callback({val:()=>0}); return ()=>{}; };
export const onChildAdded = () => () => {};
export const serverTimestamp = () => 0;
export const runTransaction = async () => ({committed:false,snapshot:{val:()=>null}});
`;

function serve(req, res) {
  const pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
  const filename = path.resolve(root, '.' + pathname);
  if (!filename.startsWith(root + path.sep)) { res.writeHead(403).end(); return; }
  try {
    let data = fs.readFileSync(filename);
    if (pathname === '/game/cardrealmsmulti.html') {
      const html = data.toString();
      let injected = false;
      data = Buffer.from(html.replace(/<script([^>]*)>([\s\S]*?)<\/script>/g, (script, attributes, body) => {
        if (!attributes.includes('module') || !body.includes('function renderCardDiv(inst')) return script;
        injected = true;
        return `<script${attributes}>${body}${multiplayerHooks}</script>`;
      }));
      if (!injected) throw new Error('Could not locate the multiplayer engine module for the test fixture');
    }
    const type = ({'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.png':'image/png'})[path.extname(filename)] || 'application/octet-stream';
    res.writeHead(200, {'Content-Type':type, 'Cache-Control':'no-store'}).end(data);
  } catch { res.writeHead(404).end('Not found'); }
}

let checks = 0;
async function check(name, run) {
  await run();
  checks++;
  console.log(`ok ${checks} - ${name}`);
}

async function pageFor(browser, base, options = {}) {
  const context = await browser.newContext(options);
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    const url = route.request().url();
    if (url.startsWith('https://www.gstatic.com/firebasejs/')) {
      return route.fulfill({status:200, contentType:'text/javascript', body:firebaseStub});
    }
    if (new URL(url).pathname === '/hub/play-signal.mjs') {
      return route.fulfill({status:200, contentType:'text/javascript', body:''});
    }
    if (!url.startsWith(base)) return route.abort();
    return route.continue();
  });
  return {context, page, errors};
}

async function resetSolo(page) {
  await page.waitForFunction(() => typeof CardRealmsUI !== 'undefined' && !game.busy);
  await page.evaluate(() => {
    closeModal();
    game.current = 0; game.busy = false; game.gauntlet.cleared = false;
    game.players = [mkPlayer('You'), mkPlayer('AI')];
    game.settings.autoplay = false;
    const me = game.players[0];
    me.trade = 10; me.combat = 4;
    me.hand = ['scout','viper','explorer','tf_courier','se_infiltrator'].map(makeCardInstance);
    me.deck = ['scout','viper','scout'].map(makeCardInstance);
    me.bases = [makeCardInstance('relay')]; me.bases[0].curDefense = 2;
    game.players[1].bases = [makeCardInstance('tf_sanctuary')];
    game.tradeRow = ['freighter','vs_shroud','mc_junksiphon','tf_medical','sd_yard'].map(makeCardInstance);
    renderAll();
  });
}

async function soloSnapshot(page) {
  return page.evaluate(() => ({
    current:game.current, trade:game.players[0].trade, combat:game.players[0].combat,
    hand:game.players[0].hand.map(c=>c.uid),
    discard:game.players[0].discard.map(c=>c.uid),
    inPlay:game.players[0].inPlay.map(c=>c.uid),
    market:game.tradeRow.map(c=>c?.uid),
    enemyDefense:game.players[1].bases.map(c=>c.curDefense)
  }));
}

async function visibleInspector(page) {
  await page.locator('#cr-inspector').waitFor({state:'visible'});
  return page.locator('#cr-inspector');
}

async function closeInspector(page) {
  await page.keyboard.press('Escape');
  await page.locator('#cr-inspector').waitFor({state:'hidden'});
}

async function settleLayout(page) {
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
}

async function activateCard(card, touch = false) {
  const primary = card.locator('.cr-primary');
  const box = await primary.boundingBox();
  assert(box && box.width > 0 && box.height > 0, 'card has a visible primary control');
  const position = {x:box.width / 2, y:Math.min(35, box.height * .28)};
  if (await primary.getAttribute('aria-disabled') === 'true') {
    // Playwright deliberately refuses locator clicks on aria-disabled controls.
    // Use actual pointer input to verify the game's guard still rejects them,
    // while checking that no overlay makes this a click on the wrong target.
    const point = {x:box.x + position.x, y:box.y + position.y};
    assert(await primary.evaluate((node,point)=>node.contains(document.elementFromPoint(point.x,point.y)),point),'disabled card action is unobstructed');
    if (touch) await primary.page().touchscreen.tap(point.x,point.y);
    else await primary.page().mouse.click(point.x,point.y);
  } else if (touch) await primary.tap({position});
  else await primary.click({position});
}

async function screenshot(page, name) {
  if (!process.env.CARDREALMS_SCREENSHOTS) return;
  fs.mkdirSync(process.env.CARDREALMS_SCREENSHOTS, {recursive:true});
  await page.screenshot({path:path.join(process.env.CARDREALMS_SCREENSHOTS, name + '.png'), fullPage:true});
}

async function boardFitSnapshot(page) {
  return page.evaluate(() => {
    const rectangle = node => {
      if (!node) return null;
      const {x,y,width,height,right,bottom} = node.getBoundingClientRect();
      return {x,y,width,height,right,bottom};
    };
    const areas = ['enemyBases','tradeRow','myBases','hand','inPlay'].map(id => {
      const node = document.getElementById(id);
      return {id,rect:rectangle(node),zone:rectangle(node.closest('.cr-zone')),scrollWidth:node.scrollWidth,clientWidth:node.clientWidth,
        cards:[...node.querySelectorAll('.cr-card')].map(card => ({
          key:card.dataset.cardKey,rect:rectangle(card),name:rectangle(card.querySelector('.cr-name')),
          art:rectangle(card.querySelector('.cr-art')),effects:rectangle(card.querySelector('.cr-effects')),
          stats:[...card.querySelectorAll('.cr-effects .cr-stat')].map(rectangle),scrap:rectangle(card.querySelector('.cr-scrap'))
        }))};
    });
    return {width:innerWidth,height:innerHeight,scrollWidth:document.documentElement.scrollWidth,
      scrollHeight:document.documentElement.scrollHeight,areas,
      controls:['playAllBtn','buyExplorerBtn','attackAuthorityBtn','endTurnBtn'].map(id => {
        const node=document.getElementById(id), rect=rectangle(node);
        return {id,rect,uncovered:node.contains(document.elementFromPoint(rect.x+rect.width/2,rect.y+rect.height/2))};
      })};
  });
}

function assertFitsBoard(snapshot, {allCards = true} = {}) {
  const within = rect => rect.width > 0 && rect.height > 0 && rect.x >= -1 && rect.y >= -1 && rect.right <= snapshot.width + 1 && rect.bottom <= snapshot.height + 1;
  const contained = (inner,outer) => inner && inner.width > 0 && inner.height > 0 && inner.x >= outer.x - 1 && inner.y >= outer.y - 1 && inner.right <= outer.right + 1 && inner.bottom <= outer.bottom + 1;
  assert(snapshot.scrollWidth <= snapshot.width + 1, JSON.stringify(snapshot));
  assert(snapshot.scrollHeight <= snapshot.height + 1, JSON.stringify(snapshot));
  for (const area of snapshot.areas) {
    assert(within(area.zone), `${area.id} zone fits viewport: ${JSON.stringify(area)}`);
    assert(within(area.rect), `${area.id} card area fits viewport: ${JSON.stringify(area)}`);
    for (const card of area.cards) {
      const detail=`${area.id}/${card.key}: ${JSON.stringify(card)}`;
      assert(card.art && card.art.height >= 20, `actual card artwork retains visible height: ${detail}`);
      assert(contained(card.name,card.rect), `card name stays inside its face: ${detail}`);
      assert(contained(card.effects,card.rect), `primary actions and health stay inside the card face: ${detail}`);
      assert(card.effects.y >= card.name.bottom - 1, `primary actions do not overlap the card name: ${detail}`);
      for (const stat of card.stats) assert(contained(stat,card.rect), `each primary icon and value stays visible: ${detail}`);
      if (card.scrap) {
        assert(contained(card.scrap,card.rect), `Scrap control stays inside the card face: ${detail}`);
        assert(card.scrap.y >= card.name.bottom - 1 && card.scrap.y >= card.effects.bottom - 1, `Scrap control does not overlap the name or primary effects: ${detail}`);
      }
    }
    if (allCards) {
      for (const card of area.cards) {
        assert(within(card.rect), `${area.id}/${card.key} is on screen: ${JSON.stringify(card)}`);
        assert(card.rect.x >= area.rect.x - 1 && card.rect.y >= area.rect.y - 1 && card.rect.right <= area.rect.right + 1 && card.rect.bottom <= area.rect.bottom + 1,
          `${area.id}/${card.key} is fully visible within its own area: ${JSON.stringify({card,area:area.rect})}`);
      }
      assert(area.scrollWidth <= area.clientWidth + 1, `${area.id} does not hide cards beyond a horizontal scroller`);
    }
  }
  for (const control of snapshot.controls) {
    assert(within(control.rect), `${control.id} remains fully visible: ${JSON.stringify(control)}`);
    assert(control.uncovered, `${control.id} is not covered by another board area`);
  }
}

async function viewportChecks(browser, base) {
  const sizes = [[1366,768],[1280,720],[390,844],[375,667],[844,390]];
  for (const [width,height] of sizes) {
    const {context,page,errors} = await pageFor(browser, base, {viewport:{width,height},isMobile:width<900,hasTouch:width<900});
    await page.goto(base + '/game/cardrealms.html');
    await resetSolo(page);
    await page.evaluate(() => {
      game.players[0].inPlay = ['explorer','tf_courier','viper'].map(makeCardInstance);
      game.players[0].bases.push(makeCardInstance('sd_yard'));
      game.players[1].bases.push(makeCardInstance('vs_shroud'));
      renderAll();
    });
    await settleLayout(page);
    await check(`${width}×${height} shows the full board, five market cards, and five hand cards together`, async () => {
      const snapshot = await boardFitSnapshot(page);
      await screenshot(page, `orbital-rivals-${width}x${height}`);
      assertFitsBoard(snapshot);
      assert.equal(snapshot.areas.find(a=>a.id==='tradeRow').cards.length,5);
      assert.equal(snapshot.areas.find(a=>a.id==='hand').cards.length,5);
      const marketCards = snapshot.areas.find(a=>a.id==='tradeRow').cards;
      assert(marketCards.every(card=>card.rect.width <= (width < 600 ? 80 : 110)),JSON.stringify(marketCards));
      assert((await page.title()).includes('Orbital Rivals'));
    });
    if (width === 390 || width === 1280) {
      await check(`${width}×${height} contains a ten-card hand without pushing turn controls off screen`, async () => {
        await page.evaluate(() => {
          game.players[0].hand = Array.from({length:10},(_,i)=>makeCardInstance(['scout','viper','explorer','tf_courier','se_infiltrator'][i%5]));
          renderAll();
        });
        await settleLayout(page);
        const snapshot=await boardFitSnapshot(page);
        assertFitsBoard(snapshot,{allCards:false});
        const hand=snapshot.areas.find(area=>area.id==='hand');
        assert.equal(hand.cards.length,10);
        assert(hand.scrollWidth<=hand.clientWidth+1,'a large hand remains within its board column');
      });
    }
    assert.deepEqual(errors,[],`page errors at ${width}×${height}`);
    await context.close();
  }
}

async function desktopChecks(browser, base) {
  const {context,page,errors} = await pageFor(browser, base, {viewport:{width:1366,height:768}});
  await page.goto(base + '/game/cardrealms.html');
  await resetSolo(page);
  await settleLayout(page);

  await check('desktop cards stay compact and retain unique artwork', async () => {
    const sizes = await page.locator('#tradeRow .cr-card').evaluateAll(cards => cards.map(card => {
      const rect = card.getBoundingClientRect();
      return {width:rect.width,height:rect.height,svg:!!card.querySelector('svg')};
    }));
    assert.equal(sizes.length, 5);
    assert(sizes.every(s=>s.width<=110 && s.width>=54 && s.height<=165 && s.svg), JSON.stringify(sizes));
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  });

  await check('card faces use a successfully decoded painted artwork asset', async () => {
    const artwork = await page.evaluate(async () => {
      const nodes = [...document.querySelectorAll('#tradeRow .cr-painted-art')];
      return Promise.all(nodes.map(async node => {
        const css = getComputedStyle(node);
        const source = node.currentSrc || node.src || node.querySelector('image')?.getAttribute('href') || css.backgroundImage.match(/url\(["']?([^"')]+)["']?\)/)?.[1];
        if (!source) return {source:null};
        const image = new Image();
        image.src = source;
        await image.decode();
        return {source,width:image.naturalWidth,height:image.naturalHeight,position:node.getAttribute('viewBox') || css.backgroundPosition};
      }));
    });
    assert.equal(artwork.length,5,'every market card has painted artwork');
    assert(artwork.every(art=>art.width>=512 && art.height>=512 && !art.source.startsWith('data:image/svg')),JSON.stringify(artwork));
    assert(new Set(artwork.map(art=>`${art.source}:${art.position}`)).size>1,'different factions use different artwork cells');
  });

  await check('card actions and inspection controls have separate native button semantics', async () => {
    const cards = await page.locator('.cr-card').evaluateAll(nodes => nodes.map(node => ({
      role:node.getAttribute('role'),
      primaryTag:node.querySelector('.cr-primary')?.tagName,
      primaryDisabled:node.querySelector('.cr-primary')?.getAttribute('aria-disabled'),
      nested:!!node.querySelector('button button, [role="button"] button, button [role="button"]'),
      infoHasButtonAncestor:!!node.querySelector('.cr-info')?.parentElement.closest('button, [role="button"]')
    })));
    assert(cards.length>0);
    assert(cards.every(card=>card.role==='group' && card.primaryTag==='BUTTON' && card.primaryDisabled==='false' && !card.nested && !card.infoHasButtonAncestor),JSON.stringify(cards));
  });

  await check('hover reveals full card rules without buying or playing', async () => {
    const before = await soloSnapshot(page);
    await page.locator('#tradeRow [data-card-key="vs_shroud"]').hover();
    const inspector = await visibleInspector(page);
    const expected = await page.evaluate(() => tplByKey('vs_shroud').text);
    for (const line of expected.split('\n')) assert((await inspector.innerText()).includes(line), line);
    assert.deepEqual(await soloSnapshot(page), before);
    await closeInspector(page);
  });

  await check('hover details remain open while the pointer travels into the inspector', async () => {
    const before=await soloSnapshot(page);
    await page.mouse.move(1,1);
    await page.locator('#tradeRow [data-card-key="vs_shroud"]').hover();
    const inspector=await visibleInspector(page);
    const box=await inspector.boundingBox();
    await page.mouse.move(box.x+25,box.y+35,{steps:8});
    // Wait past the hover-leave dismissal timer to verify the inspector itself
    // keeps the details open while the user reads or scrolls them.
    await page.waitForTimeout(240);
    assert.equal(await inspector.isVisible(),true);
    assert.equal(await inspector.locator('.cr-inspector-title').innerText(),'Shroud Gate');
    assert.deepEqual(await soloSnapshot(page),before);
    await closeInspector(page);
  });

  await check('info click and keyboard info activation never trigger gameplay', async () => {
    const before = await soloSnapshot(page);
    const info = page.locator('#hand .cr-info').first();
    await info.click();
    await visibleInspector(page);
    assert.deepEqual(await soloSnapshot(page), before);
    await closeInspector(page);
    await info.focus();
    await page.keyboard.press('Enter');
    await visibleInspector(page);
    assert.deepEqual(await soloSnapshot(page), before);
    await closeInspector(page);
  });

  await check('clicking a playable market card still purchases exactly once', async () => {
    const before = await soloSnapshot(page);
    await activateCard(page.locator('#tradeRow [data-card-key="freighter"]'));
    const after = await soloSnapshot(page);
    assert.equal(after.trade, before.trade - 3);
    assert.equal(after.discard.length, before.discard.length + 1);
    assert(after.discard.includes(before.market[0]));
  });

  await check('Enter and Space on the hand card still play exactly one card', async () => {
    for (const [key,keyboardKey] of [['scout','Enter'],['viper','Space']]) {
      const before = await soloSnapshot(page);
      await page.locator(`#hand [data-card-key="${key}"] .cr-primary`).focus();
      await page.keyboard.press(keyboardKey);
      await page.waitForFunction(() => !game.busy);
      const after = await soloSnapshot(page);
      assert.equal(after.hand.length, before.hand.length - 1);
      assert.equal(after.inPlay.length, before.inPlay.length + 1);
    }
  });

  await check('unaffordable market cards remain inspectable and cannot be purchased', async () => {
    await page.evaluate(() => {game.players[0].trade=0;renderAll();});
    const before = await soloSnapshot(page);
    const card = page.locator('#tradeRow [data-card-key="vs_shroud"]');
    assert.equal(await card.locator('.cr-primary').getAttribute('aria-disabled'),'true');
    await card.locator('.cr-info').click();
    await visibleInspector(page);
    assert.deepEqual(await soloSnapshot(page), before);
    await closeInspector(page);
    await activateCard(card);
    assert.deepEqual(await soloSnapshot(page), before);
    await card.locator('.cr-primary').focus();
    await page.keyboard.press('Enter');
    assert.deepEqual(await soloSnapshot(page), before);
    await closeInspector(page);
  });

  await check('dismissing pinned details with an outside click consumes that click', async () => {
    await resetSolo(page);
    const before = await soloSnapshot(page);
    await page.locator('#tradeRow [data-card-key="freighter"] .cr-info').click();
    await visibleInspector(page);
    // Click the viewport position of another affordable card. A locator click
    // intentionally refuses to click through the modal backdrop.
    const outside = await page.locator('#tradeRow [data-card-key="sd_yard"]').boundingBox();
    await page.mouse.click(outside.x + 55, outside.y + 65);
    await page.locator('#cr-inspector').waitFor({state:'hidden'});
    assert.deepEqual(await soloSnapshot(page), before);
  });

  await check('inspecting a mandatory selection card preserves its modal and choice', async () => {
    await page.evaluate(() => {
      globalThis.__selectionDone = false;
      globalThis.__selectionPicked = null;
      showModal({title:'Choose a card',text:'Choose exactly one card.',cards:game.players[0].hand,min:1,max:1,allowCancel:false})
        .then(picked=>{globalThis.__selectionDone=true;globalThis.__selectionPicked=picked?.map(c=>c.key);});
    });
    const choose = page.locator('#modalCards [data-card-key="scout"]');
    await activateCard(choose);
    assert.equal(await page.locator('#modalConfirm').isDisabled(), false);
    await choose.locator('.cr-info').click();
    await visibleInspector(page);
    await closeInspector(page);
    assert.equal(await page.locator('#modal').isVisible(), true);
    assert.equal(await page.locator('#modalConfirm').isDisabled(), false);
    assert.equal(await page.evaluate(() => __selectionDone), false);
    await page.locator('#modalConfirm').click();
    assert.deepEqual(await page.evaluate(() => __selectionPicked), ['scout']);
  });

  await check('deployed defense uses remaining HP while pile inspection uses printed defense', async () => {
    await resetSolo(page);
    await page.locator('#myBases [data-card-key="relay"] .cr-info').click();
    let text = await (await visibleInspector(page)).innerText();
    assert(/2\s*(?:\/|of)\s*5/.test(text), text);
    await closeInspector(page);
    await page.evaluate(() => {
      const base=game.players[0].bases.pop();base.curDefense=0;
      game.players[0].discard.push(base);renderAll();showPile('Your Discard',[base]);
    });
    await page.locator('#modalCards [data-card-key="relay"] .cr-info').click();
    text = await (await visibleInspector(page)).innerText();
    assert(!/0\s*(?:\/|of)\s*5/.test(text), text);
    assert(/(?:defense[^\n]*5|5[^\n]*defense)/i.test(text), text);
    await closeInspector(page);
    await page.locator('#modalConfirm').click();
  });

  await check('the compact menu opens pile details without leaving a blocking dialog', async () => {
    await resetSolo(page);
    await page.locator('#crMenuBtn').click();
    assert.equal(await page.locator('#cr-menu').evaluate(menu=>menu.open),true);
    await page.locator('#viewDeckBtn').click();
    assert.equal(await page.locator('#cr-menu').evaluate(menu=>menu.open),false);
    assert.equal(await page.locator('#modal').isVisible(),true);
    await page.locator('#modalCards .cr-info').first().click();
    await visibleInspector(page);
    await closeInspector(page);
    await page.locator('#modalConfirm').click();
    assert.equal(await page.locator('#modal').isVisible(),false);
  });

  await resetSolo(page);
  await screenshot(page, 'cardrealms-desktop');
  assert.deepEqual(errors, [], 'solo page errors');
  await context.close();
}

async function touchChecks(browser, base) {
  const {context,page,errors} = await pageFor(browser, base, {viewport:{width:390,height:844},isMobile:true,hasTouch:true});
  await page.goto(base + '/game/cardrealms.html');
  await resetSolo(page);
  await check('390px touch layout fits the viewport and keeps inspection buttons tappable', async () => {
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    const rect = await page.locator('#tradeRow .cr-info').first().boundingBox();
    assert(rect && rect.width>=24 && rect.height>=24, JSON.stringify(rect));
    const before=await soloSnapshot(page);
    await page.locator('#tradeRow [data-card-key="vs_shroud"] .cr-info').tap();
    const inspector=await visibleInspector(page);
    const box=await inspector.boundingBox();
    assert(box && box.x>=0 && box.x+box.width<=391 && box.y>=0 && box.y+box.height<=845, JSON.stringify(box));
    assert.deepEqual(await soloSnapshot(page), before);
    await screenshot(page, 'cardrealms-mobile-inspector');
    await closeInspector(page);
  });
  await check('a direct touch on a hand card still plays it', async () => {
    const before=await soloSnapshot(page);
    await activateCard(page.locator('#hand [data-card-key="scout"]'), true);
    await page.waitForFunction(() => !game.busy);
    assert.equal((await soloSnapshot(page)).hand.length,before.hand.length-1);
  });
  await screenshot(page, 'cardrealms-mobile');
  assert.deepEqual(errors, [], 'mobile page errors');
  await context.close();
}

async function multiplayerChecks(browser, base) {
  const {context,page,errors}=await pageFor(browser,base,{viewport:{width:1280,height:720}});
  await page.goto(base + '/game/cardrealmsmulti.html');
  await page.waitForFunction(() => !!globalThis.__cardRealmsTest);
  await page.evaluate(() => __cardRealmsTest.reset());
  await check('multiplayer uses the same inspection UI without sending network actions', async () => {
    await page.locator('#tradeRow [data-card-key="vs_shroud"] .cr-info').click();
    const inspector=await visibleInspector(page);
    assert((await inspector.innerText()).includes('opponent draws 1 fewer next turn'));
    assert.equal(await page.evaluate(() => __firebaseWrites.length),0);
    await closeInspector(page);
    await page.locator('#hand [data-card-key="scout"] .cr-info').focus();
    await page.keyboard.press('Space');
    await visibleInspector(page);
    assert.equal(await page.evaluate(() => __firebaseWrites.length),0);
    await closeInspector(page);
  });
  await check('multiplayer buy and keyboard play still enqueue their original actions', async () => {
    await activateCard(page.locator('#tradeRow [data-card-key="freighter"]'));
    await page.locator('#hand [data-card-key="scout"] .cr-primary').focus();
    await page.keyboard.press('Enter');
    assert.deepEqual(await page.evaluate(() => __firebaseWrites.map(w=>w.value.type)),['BUY_ROW','PLAY_CARD']);
  });
  await check('multiplayer unavailable cards and pile cards remain inspectable', async () => {
    await page.evaluate(() => {__firebaseWrites=[];__cardRealmsTest.setTrade(0);});
    await page.locator('#tradeRow [data-card-key="vs_shroud"] .cr-info').click();
    await visibleInspector(page);await closeInspector(page);
    await activateCard(page.locator('#tradeRow [data-card-key="vs_shroud"]'));
    assert.equal(await page.evaluate(() => __firebaseWrites.length),0);
    await page.evaluate(() => __cardRealmsTest.showPile());
    await page.locator('#modalCards .cr-info').first().click();
    await visibleInspector(page);
    assert.equal(await page.evaluate(() => __firebaseWrites.length),0);
    await closeInspector(page);
    await page.locator('#modalCancel').click();
  });
  await screenshot(page, 'cardrealms-multiplayer');
  assert.deepEqual(errors, [], 'multiplayer page errors');
  await context.close();
}

(async () => {
  const server=http.createServer(serve);
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const base=`http://127.0.0.1:${server.address().port}`;
  const executablePath=process.env.CHROMIUM_PATH || (fs.existsSync('/usr/lib/chromium/chromium')?'/usr/bin/chromium':undefined);
  let browser;
  try {
    browser=await playwright().chromium.launch({headless:true,executablePath,args:['--no-sandbox']});
    if (!process.argv.includes('--interactions')) await viewportChecks(browser,base);
    if (!process.argv.includes('--layout')) {
      await desktopChecks(browser,base);
      await touchChecks(browser,base);
      await multiplayerChecks(browser,base);
    }
    console.log(`Passed ${checks} Orbital Rivals browser checks.`);
  } finally {
    if (browser) await browser.close();
    await new Promise(resolve=>server.close(resolve));
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
