#!/usr/bin/env node
'use strict';

// Exercise the real local game in Chromium. Requests are kept local: no room,
// account, telemetry, or multiplayer state is created by this suite.
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
globalThis.__orbitalTest = {
  reset() {
    uid='local-test';roomId='local-room';mySeat=0;myName='Commander';isHost=false;
    state=initMatchState({seatUid0:uid,seatName0:'Commander',seatUid1:'rival-test',seatName1:'Rival'});
    state.players=[mkPlayerState(uid,'Commander'),mkPlayerState('rival-test','Rival')];
    state.currentSeat=0;state.ended=false;state.turnCount=1;state.log=[];
    state.players[0].trade=4;state.players[0].combat=3;
    state.players[0].hand=['scout','viper'].map(key=>mkInst(state,key));
    state.players[1].hand=['blob_titan','viper','scout'].map(key=>mkInst(state,key));
    state.tradeRow=Array.from({length:5},()=>mkInst(state,'blob_titan'));state.tradeDeck=[];
    actionsRef={path:'local-actions'};globalThis.__firebaseWrites=[];
    showNetOverlay(false);renderAll();
  },
  rivalPlay() {
    state.currentSeat=1;
    playCardByUid(state,1,state.players[1].hand.find(c=>c.templateKey==='viper').uid);
    renderAll();
  },
  rivalEnd() { endTurnInternal(state);renderAll(); },
  outpost() { state.players[1].bases=[mkInst(state,'tf_sanctuary')];renderAll(); },
  offTurn() { state.currentSeat=1;renderAll(); },
  finish() { state.ended=true;state.winnerSeat=0;renderAll(); },
  guardedActions() { sendAction('BUY_EXPLORER');sendAction('ATTACK_AUTH'); },
  simulateFirebaseRoundTrip() {
    const prune=value=>{
      if(value && typeof value==='object') {
        const entries=Object.entries(value).map(([key,item])=>[key,prune(item)]).filter(([,item])=>item!==undefined);
        return entries.length ? Object.fromEntries(entries) : undefined;
      }
      return value;
    };
    const remote=prune(state);
    remote.tradeRow={0:state.tradeRow[0],3:state.tradeRow[3]};
    state=normalizeMatchState(remote);renderAll();
  },
  snapshot() { return JSON.parse(JSON.stringify(state)); },
};
`;
const firebaseStub = `
export const initializeApp=()=>({});export const getAuth=()=>({});export const getDatabase=()=>({});
export const signInAnonymously=async()=>({});export const onAuthStateChanged=()=>()=>{};
export const ref=(_db,path)=>({path});export const push=parent=>({parent,key:'local-action'});
export const set=async(target,value)=>{(globalThis.__firebaseWrites||=[]).push({target,value});};export const update=set;
export const get=async()=>({val:()=>null,exists:()=>false});export const remove=async()=>{};
export const onValue=(target,callback)=>{if(target.path==='/.info/serverTimeOffset')callback({val:()=>0});return()=>{};};
export const onChildAdded=()=>()=>{};export const serverTimestamp=()=>0;
export const runTransaction=async()=>({committed:false,snapshot:{val:()=>null}});
`;
const server = http.createServer((req, res) => {
  const filename = path.resolve(root, '.' + decodeURIComponent(new URL(req.url, 'http://localhost').pathname));
  if (!filename.startsWith(root + path.sep)) { res.writeHead(403).end(); return; }
  try {
    let data=fs.readFileSync(filename);
    if(filename.endsWith('/game/cardrealmsmulti.html')) {
      let injected=false;
      data=Buffer.from(data.toString().replace(/<script([^>]*)>([\s\S]*?)<\/script>/g,(script,attributes,body)=>{
        if(!attributes.includes('module') || !body.includes('function renderCardDiv(inst')) return script;
        injected=true;
        return `<script${attributes}>${body}${multiplayerHooks}</script>`;
      }));
      if(!injected) throw new Error('Multiplayer engine module was not found');
    }
    const type = ({'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.png':'image/png'})[path.extname(filename)] || 'application/octet-stream';
    res.writeHead(200, {'Content-Type':type, 'Cache-Control':'no-store'}).end(data);
  } catch { res.writeHead(404).end('Not found'); }
});

let checks = 0;
async function check(name, run) {
  await run();
  checks++;
  console.log(`ok ${checks} - ${name}`);
}

async function newPage(browser, base, viewport = {width:1366,height:768}, multiplayer=false) {
  const context = await browser.newContext({viewport});
  const page = await context.newPage();
  page.setDefaultTimeout(6000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    const url = route.request().url();
    if (url.startsWith('https://www.gstatic.com/firebasejs/')) return route.fulfill({status:200,contentType:'text/javascript',body:firebaseStub});
    if (new URL(url).pathname === '/hub/play-signal.mjs') return route.fulfill({status:200,contentType:'text/javascript',body:''});
    if (!url.startsWith(base)) return route.abort();
    return route.continue();
  });
  await page.goto(base + (multiplayer ? '/game/cardrealmsmulti.html' : '/game/cardrealms.html'));
  if(multiplayer) await page.waitForFunction(()=>!!globalThis.__orbitalTest);
  else await page.waitForFunction(() => typeof game !== 'undefined' && !game.busy);
  return {context,page,errors};
}

async function settle(page) {
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
}

async function ready(page) {
  await page.locator('#hotseatHandoff').waitFor({state:'visible'});
  await page.locator('#handoffReadyBtn').click();
  await page.waitForFunction(() => !game.handoff && !game.busy);
}

async function launchHotseat(page) {
  await page.locator('#crMenuBtn').click();
  await page.locator('#startHotseatBtn').click();
  await ready(page);
}

async function snapshot(page) {
  return page.evaluate(() => ({
    mode:game.mode,current:game.current,ended:game.ended,
    players:game.players.map(player => ({
      name:player.name,authority:player.authority,trade:player.trade,combat:player.combat,
      hand:player.hand.map(c=>c.uid),deck:player.deck.map(c=>c.uid),discard:player.discard.map(c=>c.uid),
      inPlay:player.inPlay.map(c=>c.uid),bases:player.bases.map(c=>[c.uid,c.curDefense]),scrap:player.scrap.map(c=>c.uid)
    })),market:game.tradeRow.map(c=>c?.uid)
  }));
}

async function clickCard(page, selector) {
  const primary=page.locator(selector).locator('.cr-primary').first();
  const rect=await primary.boundingBox();
  assert(rect && rect.width>0 && rect.height>0, `Visible action for ${selector}`);
  await primary.click({position:{x:rect.width/2,y:Math.min(30,rect.height*.25)}});
}

async function resetDuel(page) {
  await page.evaluate(() => { startDuel(); });
  await page.waitForFunction(() => !game.busy && !game.handoff);
  await page.evaluate(() => {
    game.settings.autoplay=false;
    game.players=[mkPlayer('You'),mkPlayer('AI')];
    game.current=0;
    game.players[0].hand=['scout','viper','explorer'].map(makeCardInstance);
    game.players[0].deck=Array.from({length:12},()=>makeCardInstance('scout'));
    game.players[1].hand=['viper','scout','scout','scout','scout'].map(makeCardInstance);
    game.players[1].deck=Array.from({length:12},()=>makeCardInstance('scout'));
    game.tradeRow=Array.from({length:5},()=>makeCardInstance('blob_titan'));
    renderAll();
  });
}

async function hotseatChecks(browser, base) {
  const {context,page,errors}=await newPage(browser,base);
  try {
    await check('hotseat is reachable from the fleet menu and starts with a private handoff', async () => {
      await page.locator('#crMenuBtn').click();
      await page.locator('#startHotseatBtn').click();
      await page.locator('#hotseatHandoff').waitFor({state:'visible'});
      assert.equal(await page.evaluate(()=>game.mode),'hotseat');
      assert.equal(await page.locator('#hand .cr-card').count(),0,'next player hand is absent until ready');
      assert.equal(await page.locator('#cr-menu').evaluate(node=>node.open),false,'fleet menu closes for handoff');
      await page.keyboard.press('Escape');
      assert.equal(await page.locator('#hotseatHandoff').isVisible(),true,'Escape cannot bypass handoff');
      await ready(page);
      assert.equal(await page.evaluate(()=>humanSeat()),0);
      assert.equal(await page.locator('#hand .cr-card').count(),5);
      assert.equal(await page.locator('#endTurnBtn').isEnabled(),true);
    });

    await check('handoff blocks hidden-hand inspection and game actions, then gives player two control', async () => {
      await page.evaluate(() => {
        game.players[0].hand=[makeCardInstance('tf_medical')];
        game.players[0].deck=Array.from({length:6},()=>makeCardInstance('scout'));
        game.players[1].hand=[makeCardInstance('blob_titan')];
        game.players[1].deck=Array.from({length:6},()=>makeCardInstance('viper'));
        renderAll();
      });
      await page.locator('#hand .cr-info').first().click();
      await page.locator('#cr-inspector').waitFor({state:'visible'});
      // A turn transition must also clear an already open inspector, including
      // when a non-pointer path invokes the same game action.
      await page.evaluate(() => { endTurn(); });
      await page.locator('#hotseatHandoff').waitFor({state:'visible'});
      assert.equal(await page.locator('#cr-inspector').isVisible(),false);
      assert.equal(await page.locator('#hand .cr-card').count(),0);
      assert.equal(await page.locator('#modal').isVisible(),false);
      const before=await snapshot(page);
      await page.evaluate(async () => {
        showPile('Hidden deck',game.players[game.current].deck);
        buyExplorer(); attackAuthority(); await playCardFromHand(0); endTurn();
      });
      assert.equal(await page.locator('#modal').isVisible(),false,'pile view cannot expose cards during handoff');
      assert.deepEqual(await snapshot(page),before,'no action changes either player while locked');
      await ready(page);
      assert.equal(await page.evaluate(()=>humanSeat()),1);
      assert.equal(await page.locator('#hand [data-card-key="blob_titan"]').count(),1);
      assert.equal(await page.locator('#hand [data-card-key="tf_medical"]').count(),0);
      const firstPlayer=await page.evaluate(()=>JSON.stringify(game.players[0]));
      await clickCard(page,'#hand [data-card-key="viper"]');
      await page.waitForFunction(()=>!game.busy);
      assert.equal(await page.evaluate(()=>game.players[1].combat),1,'player two owns their combat');
      assert.equal(await page.evaluate(()=>JSON.stringify(game.players[0])),firstPlayer,'player two action does not touch player one');
      assert.equal(await page.evaluate(()=>game.current),1,'second human is not replaced by AI');
    });

    await check('both hotseat players can buy Explorers, attack the portrait, and end their turn', async () => {
      for (const seat of [1,0]) {
        assert.equal(await page.evaluate(()=>game.current),seat);
        await page.evaluate(() => {
          const actor=game.players[humanSeat()],opponent=game.players[1-humanSeat()];
          actor.trade=4;actor.combat=3;opponent.bases=[];renderAll();
        });
        const before=await snapshot(page);
        await page.locator('#explorerPileBtn').click();
        let after=await snapshot(page);
        assert.equal(after.players[seat].trade,before.players[seat].trade-2);
        assert.equal(after.players[seat].discard.length,before.players[seat].discard.length+1);
        assert.equal(await page.evaluate(()=>game.players[humanSeat()].discard.at(-1).key),'explorer');
        await page.locator('#opponentPortraitBtn').click();
        after=await snapshot(page);
        assert.equal(after.players[1-seat].authority,before.players[1-seat].authority-3);
        assert.equal(after.players[seat].combat,0);
        await page.locator('#endTurnBtn').click();
        await ready(page);
        assert.equal(await page.evaluate(()=>game.current),1-seat);
      }
    });

    await check('off-turn forced discard hands privately to its owner and returns to the attacker', async () => {
      await page.evaluate(() => {
        startHotseat();
      });
      await ready(page);
      await page.evaluate(() => {
        game.players[0].hand=[makeCardInstance('se_infiltrator')];
        game.players[1].hand=['tf_medical','blob_titan'].map(makeCardInstance);
        game.players[0].bases=[];game.players[1].bases=[];
        renderAll();
      });
      await clickCard(page,'#hand [data-card-key="se_infiltrator"]');
      await page.locator('#hotseatHandoff').waitFor({state:'visible'});
      assert.equal(await page.locator('#hand .cr-card').count(),0);
      assert.equal(await page.locator('#modalCards .cr-card').count(),0,'choice cards remain absent before receiver is ready');
      await page.locator('#handoffReadyBtn').click();
      await page.locator('#modal').waitFor({state:'visible'});
      assert.equal(await page.locator('#modalCards .cr-card').count(),2);
      assert.equal(await page.locator('#modalCancel').isVisible(),false,'mandatory discard cannot be cancelled');
      assert.equal(await page.locator('#modalConfirm').isDisabled(),true);
      await clickCard(page,'#modalCards [data-card-key="tf_medical"]');
      await page.locator('#modalConfirm').click();
      await page.locator('#hotseatHandoff').waitFor({state:'visible'});
      assert.equal(await page.locator('#modal').isVisible(),false);
      assert.equal(await page.locator('#hand .cr-card').count(),0,'return handoff does not reveal either hand');
      await ready(page);
      assert.equal(await page.evaluate(()=>game.current),0,'attacker resumes original turn');
      assert.deepEqual(await page.evaluate(()=>game.players[1].hand.map(c=>c.key)),['blob_titan']);
      assert.deepEqual(await page.evaluate(()=>game.players[1].discard.map(c=>c.key)),['tf_medical']);
      assert.equal(await page.evaluate(()=>game.players[0].combat),1,'resolving discard preserves the attack effect');
      assert.equal(await page.evaluate(()=>game.busy),false);
    });

    await check('restarting during a private choice cancels the old turn without leaking or mutating the new match', async () => {
      await page.evaluate(() => {
        game.players[0].hand=[makeCardInstance('se_infiltrator')];
        game.players[1].hand=['scout','viper'].map(makeCardInstance);
        renderAll();
      });
      await clickCard(page,'#hand [data-card-key="se_infiltrator"]');
      await page.locator('#hotseatHandoff').waitFor({state:'visible'});
      await page.locator('#handoffReadyBtn').click();
      await page.locator('#modal').waitFor({state:'visible'});
      await page.evaluate(() => { startDuel(); });
      await page.waitForFunction(()=>!game.busy && game.mode==='duel');
      const fresh=await snapshot(page);
      await page.waitForTimeout(120);
      assert.deepEqual(await snapshot(page),fresh,'old asynchronous effect cannot change the new match');
      assert.equal(await page.locator('#hotseatHandoff').isVisible(),false);
      assert.equal(await page.locator('#modal').isVisible(),false);
      assert.equal(await page.locator('#hand .cr-card').count(),5);
    });

    await check('hotseat victory identifies the human winner and disables later turn actions', async () => {
      await page.evaluate(()=>{startHotseat();});
      await ready(page);
      await page.evaluate(() => {
        game.players[0].combat=4;game.players[1].authority=3;game.players[1].bases=[];renderAll();
      });
      const winner=await page.evaluate(()=>game.players[0].name);
      await page.locator('#opponentPortraitBtn').click();
      assert.equal(await page.evaluate(()=>game.ended),true);
      assert((await page.locator('#statusLine').innerText()).includes(winner));
      assert.equal(await page.locator('#endTurnBtn').isDisabled(),true);
      const final=await snapshot(page);
      await page.evaluate(async()=>{buyExplorer();attackAuthority();await playCardFromHand(0);endTurn();});
      assert.deepEqual(await snapshot(page),final);
      assert.equal(await page.locator('#hotseatHandoff').isVisible(),false);
    });
    assert.deepEqual(errors,[],'hotseat browser exceptions');
  } finally { await context.close(); }
}

async function soloChecks(browser, base) {
  const {context,page,errors}=await newPage(browser,base);
  try {
    await resetDuel(page);
    await check('Scouts and Vipers use different complete ship scenes in cards and inspectors', async()=>{
      const art=await page.evaluate(()=>{
        const scout=CardRealmsArt.artFrame(tplByKey('scout'));
        const viper=CardRealmsArt.artFrame(tplByKey('viper'));
        return {scout,viper,faces:[...document.querySelectorAll('#hand .cr-painted-art')].map(node=>({
          fit:node.getAttribute('preserveAspectRatio'),clip:!!node.querySelector('clipPath')
        }))};
      });
      assert.notDeepEqual([art.scout.x,art.scout.y,art.scout.width,art.scout.height],[art.viper.x,art.viper.y,art.viper.width,art.viper.height]);
      assert(art.faces.length>=2);
      assert(art.faces.every(face=>face.fit==='xMidYMid meet' && face.clip),'complete scene fits without atlas-neighbor bleed');
      await page.locator('#hand [data-card-key="viper"] .cr-info').click();
      await page.locator('#cr-inspector').waitFor({state:'visible'});
      const inspectorFrame=await page.locator('#cr-inspector .cr-painted-art').getAttribute('viewBox');
      assert.equal(inspectorFrame,`${art.viper.x} ${art.viper.y} ${art.viper.width} ${art.viper.height}`);
      await page.keyboard.press('Escape');
    });
    await check('Explorer supply remains available after the market empties and spends exactly two trade', async () => {
      await page.evaluate(()=>{game.players[0].trade=5;game.tradeRow=[];game.tradeDeck=[];renderAll();});
      const before=await snapshot(page);
      await page.locator('#explorerPileBtn').click();
      await page.locator('#explorerPileBtn').click();
      const after=await snapshot(page);
      assert.equal(after.players[0].trade,1);
      assert.equal(after.players[0].discard.length,before.players[0].discard.length+2);
      assert.deepEqual(await page.evaluate(()=>game.players[0].discard.map(c=>c.key)),['explorer','explorer']);
      assert.equal(await page.locator('#explorerPileBtn').isDisabled(),true);
      await page.evaluate(()=>buyExplorer());
      assert.deepEqual(await snapshot(page),after);
    });

    await check('portrait attacks respect outposts and direct base attacks use the selected base', async () => {
      await resetDuel(page);
      await page.evaluate(()=>{
        game.players[0].combat=7;
        game.players[1].bases=['tf_sanctuary','relay'].map(makeCardInstance);
        renderAll();
      });
      assert.equal(await page.locator('#opponentPortraitBtn').isDisabled(),true);
      const before=await snapshot(page);
      await page.evaluate(()=>attackAuthority());
      assert.equal((await snapshot(page)).players[1].authority,before.players[1].authority);
      await clickCard(page,'#enemyBases [data-card-key="tf_sanctuary"]');
      assert.deepEqual(await page.evaluate(()=>game.players[1].bases.map(c=>c.key)),['relay']);
      assert.equal(await page.evaluate(()=>game.players[0].combat),2);
      assert.equal(await page.locator('#opponentPortraitBtn').isEnabled(),true);
      await page.locator('#opponentPortraitBtn').click();
      assert.equal(await page.evaluate(()=>game.players[1].authority),48);
      assert.equal(await page.evaluate(()=>game.players[1].bases[0].curDefense),5,'unselected base remains intact');
      assert.equal(await page.evaluate(()=>game.players[0].combat),0);
    });

    await check('AI plays stay visible one by one while its hand stays private', async () => {
      await resetDuel(page);
      await page.evaluate(()=>{game.settings.aiDelay=180;game.players[1].hand.at(-1).name='Private future ship';});
      await page.locator('#endTurnBtn').click();
      await page.waitForFunction(()=>game.current===1 && document.querySelectorAll('#enemyInPlay .cr-card').length>0);
      const first=await page.evaluate(()=>({
        count:document.querySelectorAll('#enemyInPlay .cr-card').length,
        visibleKeys:[...document.querySelectorAll('#enemyInPlay .cr-card')].map(c=>c.dataset.cardKey)
      }));
      assert(first.count<5,'one public play is visible before the rest complete');
      assert(!(await page.locator('body').innerText()).includes('Private future ship'),'unplayed AI card is not revealed');
      assert.equal(await page.locator('#endTurnBtn').isDisabled(),true);
      assert.equal(await page.locator('#explorerPileBtn').isDisabled(),true);
      assert.equal(await page.locator('#opponentPortraitBtn').isDisabled(),true);
      await page.waitForFunction(()=>game.current===0 && !game.busy,{},{timeout:10000});
      assert.equal(await page.evaluate(()=>game.players[0].authority),49,'AI combat applies once');
      assert((await page.locator('#enemyInPlay').innerText()).includes('Viper'),'public last-turn cards remain reviewable');
      assert.equal(await page.locator('#endTurnBtn').isEnabled(),true);
    });

    await check('starting a new duel during a visible AI turn cancels all old actions', async () => {
      await resetDuel(page);
      await page.evaluate(()=>{game.settings.aiDelay=180;});
      await page.locator('#endTurnBtn').click();
      await page.waitForFunction(()=>game.current===1 && document.querySelectorAll('#enemyInPlay .cr-card').length>0);
      await page.evaluate(()=>{startDuel();});
      await page.waitForFunction(()=>game.current===0 && !game.busy);
      const fresh=await snapshot(page);
      await page.waitForTimeout(500);
      assert.deepEqual(await snapshot(page),fresh);
      assert.equal(await page.locator('#endTurnBtn').isEnabled(),true);
    });
    assert.deepEqual(errors,[],'solo browser exceptions');
  } finally { await context.close(); }
}

async function multiplayerChecks(browser,base) {
  const {context,page,errors}=await newPage(browser,base,{width:1280,height:720},true);
  try {
    await page.evaluate(()=>__orbitalTest.reset());
    await check('online Explorer supply and opponent portrait send the original host-validated actions',async()=>{
      await page.locator('#explorerPileBtn').click();
      await page.locator('#opponentPortraitBtn').click();
      assert.deepEqual(await page.evaluate(()=>__firebaseWrites.map(write=>write.value.type)),['BUY_EXPLORER','ATTACK_AUTH']);
      await page.evaluate(()=>__orbitalTest.outpost());
      assert.equal(await page.locator('#opponentPortraitBtn').isDisabled(),true);
      await clickCard(page,'#enemyBases [data-card-key="tf_sanctuary"]');
      const writes=await page.evaluate(()=>__firebaseWrites.map(write=>write.value));
      assert.equal(writes.at(-1).type,'ATTACK_BASE');
      const enemy=await page.evaluate(()=>__orbitalTest.snapshot().players[1]);
      assert.equal(writes.at(-1).payload.uid,enemy.bases[0].uid);
    });

    await check('online opponent plays are visible live and remain public in the next-turn recap',async()=>{
      await page.evaluate(()=>{__orbitalTest.reset();__orbitalTest.rivalPlay();});
      assert.equal(await page.locator('#enemyInPlay [data-card-key="viper"]').count(),1);
      assert.equal(await page.locator('#enemyInPlay [data-card-key="blob_titan"]').count(),0,'unplayed private card is absent');
      assert.match(await page.locator('#turnActivity').innerText(),/Viper/);
      assert.match(await page.locator('#cr-rival-title').innerText(),/live/i);
      assert.equal(await page.locator('#explorerPileBtn').isDisabled(),true);
      assert.equal(await page.locator('#opponentPortraitBtn').isDisabled(),true);
      await page.evaluate(()=>__orbitalTest.guardedActions());
      assert.equal(await page.evaluate(()=>__firebaseWrites.length),0,'off-turn actions never enter transport');
      await page.evaluate(()=>__orbitalTest.rivalEnd());
      const state=await page.evaluate(()=>__orbitalTest.snapshot());
      assert.equal(state.currentSeat,0);
      assert.deepEqual(state.players[1].lastTurnShips.map(card=>card.templateKey),['viper']);
      assert(!state.players[1].lastTurnShips.some(card=>card.templateKey==='blob_titan'));
      assert.equal(state.players[1].inPlay.length,0,'completed ships move to discard in the real state');
      assert.equal(await page.locator('#enemyInPlay [data-card-key="viper"]').count(),1,'public recap persists after cleanup');
      assert.match(await page.locator('#cr-rival-title').innerText(),/last turn/i);
      assert.match(await page.locator('#turnActivity').innerText(),/Viper/);
    });

    await check('online completed matches disable direct targets and reject action dispatch',async()=>{
      await page.evaluate(()=>{__orbitalTest.reset();__orbitalTest.finish();});
      assert.equal(await page.locator('#explorerPileBtn').isDisabled(),true);
      assert.equal(await page.locator('#opponentPortraitBtn').isDisabled(),true);
      await page.evaluate(()=>__orbitalTest.guardedActions());
      assert.equal(await page.evaluate(()=>__firebaseWrites.length),0);
    });
    await check('online state round trips with omitted empty arrays still render and complete turns',async()=>{
      await page.evaluate(()=>{__orbitalTest.reset();__orbitalTest.simulateFirebaseRoundTrip();});
      let state=await page.evaluate(()=>__orbitalTest.snapshot());
      assert.deepEqual(state.tradeRow.map(card=>card?.templateKey||null),['blob_titan',null,null,'blob_titan',null]);
      for(const player of state.players) {
        for(const key of ['deck','discard','hand','inPlay','bases','scrap','turnShips','lastTurnShips','lastTurnLog']) assert(Array.isArray(player[key]),key);
      }
      assert.equal(await page.locator('#tradeRow .cr-card').count(),2);
      await page.evaluate(()=>{__orbitalTest.rivalPlay();__orbitalTest.rivalEnd();});
      state=await page.evaluate(()=>__orbitalTest.snapshot());
      assert.equal(state.currentSeat,0);
      assert.equal(await page.locator('#enemyInPlay [data-card-key="viper"]').count(),1);
      assert.equal(await page.locator('#endTurnBtn').isEnabled(),true);
    });
    assert.deepEqual(errors,[],'multiplayer browser exceptions');
  } finally {await context.close();}
}

async function overflowChecks(browser, base) {
  for(const [width,height] of [[1366,768],[390,844],[844,390]]) {
    const {context,page,errors}=await newPage(browser,base,{width,height});
    try {
      await resetDuel(page);
      await check(`${width}×${height} scrolls a large base fleet inside its panel with actions still visible`,async()=>{
        await page.evaluate(()=>{
          game.players[0].bases=Array.from({length:24},()=>makeCardInstance('relay'));
          game.players[1].bases=Array.from({length:24},()=>makeCardInstance('tf_sanctuary'));
          renderAll();
        });
        await settle(page);
        for (const id of ['myBases','enemyBases']) {
          const info=await page.locator('#'+id).evaluate(node=>{
            const style=getComputedStyle(node);
            node.scrollTop=node.scrollHeight;
            return {overflow:style.overflowY,scrollHeight:node.scrollHeight,clientHeight:node.clientHeight,scrollTop:node.scrollTop};
          });
          assert(['auto','scroll'].includes(info.overflow),JSON.stringify(info));
          assert(info.scrollHeight>info.clientHeight,JSON.stringify(info));
          assert(info.scrollTop>0,'base panel actually accepts scrolling');
        }
        const end=await page.locator('#endTurnBtn').boundingBox();
        assert(end && end.x>=0 && end.y>=0 && end.x+end.width<=width+1 && end.y+end.height<=height+1,JSON.stringify(end));
        assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
        assert.equal(await page.evaluate(()=>document.documentElement.scrollHeight>innerHeight+1),false);
      });
      assert.deepEqual(errors,[],`layout browser exceptions at ${width}×${height}`);
    } finally {await context.close();}
  }
}

(async()=>{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const base=`http://127.0.0.1:${server.address().port}`;
  const executablePath=process.env.CHROMIUM_PATH || (fs.existsSync('/usr/lib/chromium/chromium')?'/usr/bin/chromium':undefined);
  const browser=await playwright().chromium.launch({headless:true,executablePath,args:['--no-sandbox']});
  try {
    const groups=new Set(process.argv.slice(2));
    for(const group of groups) assert(['hotseat','solo','multiplayer','layout'].includes(group),`Unknown test group: ${group}`);
    const wants=group=>!groups.size || groups.has(group);
    if(wants('hotseat')) await hotseatChecks(browser,base);
    if(wants('solo')) await soloChecks(browser,base);
    if(wants('multiplayer')) await multiplayerChecks(browser,base);
    if(wants('layout')) await overflowChecks(browser,base);
    console.log(`\n${checks} Orbital Rivals hotseat, combat, AI visibility, and overflow checks passed.`);
  } finally {await browser.close();server.close();}
})().catch(error=>{console.error(error);server.close();process.exitCode=1;});
