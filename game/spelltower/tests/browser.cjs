'use strict';
// Tests serve the real game. Inspection hooks and the isolated room service exist
// only in this HTTP test server; no debug API ships with the game or launcher pack.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const { execFileSync } = require('node:child_process');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..');
const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'spelltower-browser-'));
const qa = process.env.SPELLTOWER_QA_OUTPUT;
const errors = [], missing = [], external = [];
const hooks = '\nwindow.__spellTest={get game(){return game},get session(){return session},get meta(){return roomMeta},get selected(){return selected()},get effects(){return effects},setGame(next){acceptState(next,roomMeta,true)},receive(next,meta){acceptState(next,meta)},get handoff(){return handoffPending}};';
const roomPath = 'lobbies/browser-room', statePath = `${roomPath}/spelltower/state`;
let docs = {[roomPath]:{gameType:'spelltower',status:'started',players:['Aster','Bramble'],host:'Aster'}}, roomVersion = 0;
const networkFixture = `import {connectRoom} from './session.mjs';
const endpoint='/__room';
const snapshot=(path,data)=>({exists:()=>Object.hasOwn(data,path),data:()=>structuredClone(data[path]),metadata:{fromCache:false,hasPendingWrites:false}});
const sdk={doc:(_,...parts)=>parts.join('/'),async runTransaction(_,callback){for(let n=0;n<8;n++){const view=await fetch(endpoint).then(r=>r.json()),writes=[];const result=await callback({get:async path=>snapshot(path,view.docs),set:(path,value)=>writes.push({path,value,op:'set'}),update:(path,value)=>writes.push({path,value,op:'update'}),delete:path=>writes.push({path,op:'delete'})});if(!writes.length)return result;const response=await fetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({version:view.version,writes})});if(response.ok)return result;}throw Error('Room changed too often.');},onSnapshot(path,options,next,error){if(typeof options==='function'){error=next;next=options;}let closed=false,previous='';const tick=async()=>{try{const data=await fetch(endpoint).then(r=>r.json());const serialized=JSON.stringify(data.docs[path]);if(!closed&&serialized!==previous){previous=serialized;next(snapshot(path,data.docs));}}catch(e){if(!closed)error?.(e);}};tick();const timer=setInterval(tick,35);return()=>{closed=true;clearInterval(timer);};}};
export function openOnlineRoom(options){return connectRoom({...options,sdk,db:{}});}`;

function buildPack() {
  for (const file of ['build-packs.py', 'catalog.json', 'pack-index.json']) fs.copyFileSync(path.join(root, 'desktop-hub/content', file), path.join(temporary, file));
  execFileSync(process.env.PYTHON || 'python3', [path.join(temporary, 'build-packs.py'), root, root, '--only', 'spelltower'], { stdio: 'pipe' });
  const pack = path.join(temporary, 'packs/spelltower');
  assert(fs.existsSync(path.join(pack, 'game/spelltower.html')));
  for (const file of ['app.mjs','art.mjs','effects.mjs','engine.mjs','session.mjs','network.mjs','style.css','assets/glade.svg']) assert(fs.existsSync(path.join(pack, 'game/spelltower', file)), `pack missing ${file}`);
  assert(!fs.existsSync(path.join(pack,'game/spelltower/tests')), 'test fixtures never ship in a game pack');
  assert(!fs.readFileSync(path.join(pack,'game/spelltower.html'),'utf8').includes('/hub/play-signal.mjs'), 'offline game does not load hub analytics');
  return pack;
}
const pack = buildPack();
const mime = { '.html':'text/html', '.mjs':'text/javascript', '.js':'text/javascript', '.svg':'image/svg+xml', '.css':'text/css', '.json':'application/json' };
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  if (url.pathname === '/__room') {
    response.setHeader('Content-Type','application/json');
    if (request.method === 'POST') {
      const chunks=[];for await(const chunk of request)chunks.push(chunk);
      const action=JSON.parse(Buffer.concat(chunks));
      if(action.version!==roomVersion){response.writeHead(409);response.end('{}');return;}
      for(const write of action.writes){if(write.op==='delete')delete docs[write.path];else docs[write.path]=write.op==='update'?{...docs[write.path],...write.value}:write.value;}
      roomVersion++;response.end('{}');return;
    }
    response.end(JSON.stringify({version:roomVersion,docs}));return;
  }
  if (url.pathname === '/hub/play-signal.mjs') {response.setHeader('Content-Type','text/javascript');response.end('export {};');return;}
  if (url.pathname === '/favicon.ico') {response.writeHead(204);response.end();return;}
  try {
    const offline = url.pathname.startsWith('/packs/spelltower/'), base = offline ? pack : root;
    const relative = offline ? url.pathname.slice('/packs/spelltower'.length) : url.pathname;
    const file = path.resolve(base, '.' + decodeURIComponent(relative));
    if (!file.startsWith(base + path.sep)) throw Error('Outside root');
    let data = fs.readFileSync(file);
    if (path.basename(file) === 'app.mjs') data = Buffer.from(data.toString() + hooks);
    if (!offline && path.basename(file) === 'network.mjs') data = Buffer.from(networkFixture);
    response.writeHead(200, {'Content-Type':mime[path.extname(file)] || 'application/octet-stream', 'Cache-Control':'no-store'});response.end(data);
  } catch {missing.push(url.pathname);response.writeHead(404);response.end('Not found');}
});

async function inspectLayout(page) {
  const data = await page.evaluate(() => {
    const boxes=['.topbar','#board','#hand','#endTurnBtn'].map(selector=>{const r=document.querySelector(selector).getBoundingClientRect();return {selector,x:r.x,y:r.y,w:r.width,h:r.height};});
    return {w:innerWidth,h:innerHeight,boxes,bodyWidth:document.documentElement.scrollWidth,bodyHeight:document.documentElement.scrollHeight,cells:[...document.querySelectorAll('#board .cell')].map(el=>{const r=el.getBoundingClientRect();return {x:r.x,y:r.y,w:r.width,h:r.height};})};
  });
  assert(data.bodyWidth<=data.w+1,'no horizontal page scroll');assert(data.bodyHeight<=data.h+1,'the live game fits the viewport');
  for(const box of data.boxes){assert(box.w>0&&box.h>0,`${box.selector} visible`);assert(box.x>=-1&&box.y>=-1&&box.x+box.w<=data.w+1&&box.y+box.h<=data.h+1,`${box.selector} is within viewport: ${JSON.stringify(data)}`);}
  assert.equal(data.cells.length,40);
  for(const cell of data.cells)assert(cell.y>=0&&cell.y+cell.h<=data.h&&cell.w>=30&&cell.h>=12,'all battlefield tiles visible and usable');
}
async function saveScreenshot(page,name){if(qa){fs.mkdirSync(qa,{recursive:true});await page.screenshot({path:path.join(qa,name+'.png')});}}
async function waitGame(page){await page.waitForFunction(()=>window.__spellTest?.game&& !document.getElementById('game').hidden);}
async function choose(page,key){const id=await page.evaluate(key=>__spellTest.game.hands[__spellTest.game.players[__spellTest.game.activeIndex]].find(c=>c.key===key)?.id,key);assert(id,`has ${key}`);await page.locator(`[data-card-id="${id}"]`).click();}
function watch(page){page.on('pageerror',error=>errors.push(error.message));page.on('request',request=>{if(!request.url().startsWith('http://127.0.0.1:')&&!request.url().startsWith('data:'))external.push(request.url());});}

(async()=>{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin=`http://127.0.0.1:${server.address().port}`;
  const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox']});
  try {
    for(const viewport of [{width:1366,height:768},{width:1024,height:600},{width:390,height:844},{width:360,height:640},{width:844,height:390}]){
      const touch=viewport.width<500;const context=await browser.newContext({viewport,isMobile:touch,hasTouch:touch});const page=await context.newPage();watch(page);
      await page.goto(`${origin}/game/spelltower.html?mode=solo`);await page.locator('#start-btn').click();await waitGame(page);await inspectLayout(page);
      await page.evaluate(()=>{const next=structuredClone(__spellTest.game),e=next.enemies[0];Object.assign(e,{key:'orc',hp:80,maxHP:80,x:1,y:3,speed:1,damage:4});next.enemies=[e];__spellTest.setGame(next);});
      await choose(page,'water');const target=page.locator('.cell[data-x="1"][data-y="3"]');
      if(!touch){await target.hover();assert(await page.locator('.cell.targeted').count()>=4);}
      await target.click();await page.waitForFunction(()=>__spellTest.game.mana===5);assert((await page.evaluate(()=>__spellTest.game.terrain['1,3'].wet))>0);
      await choose(page,'lightning');if(!touch){await target.hover();assert.match(await page.locator('#target-hint').textContent(),/Conducted/);}
      await target.click();await page.waitForFunction(()=>__spellTest.game.mana===4);
      const shocked=await page.evaluate(()=>__spellTest.game.enemies[0]);assert(shocked.hp<74&&shocked.stun>0,'puddle shock damages and stuns');
      await saveScreenshot(page,`${viewport.width}x${viewport.height}-conduction`);
      await page.locator('#fuse-btn').click();await choose(page,'fire');await choose(page,'nature');
      await page.waitForFunction(()=>Object.values(__spellTest.game.hands)[0].some(card=>card.key==='wildfire'));
      assert.equal(await page.evaluate(()=>__spellTest.selected.key),'wildfire');
      await page.locator('#guide-btn').click();await page.locator('[data-tab="interactions"]').click();assert.equal(await page.locator('.guide-entry').count(),23);
      await page.locator('[data-tab="recipes"]').click();assert.equal(await page.locator('.guide-entry').count(),28);await page.locator('[data-close="guide-dialog"]').click();
      const before=await page.evaluate(()=>JSON.stringify(__spellTest.game));await page.reload();await page.locator('#resume-btn').click();await waitGame(page);assert.equal(await page.evaluate(()=>JSON.stringify(__spellTest.game)),before,'resume preserves full simulation');
      await inspectLayout(page);await page.locator('#pause-btn').click();await page.locator('#leave-btn').click();
      await page.locator('[data-mode="hotseat"]').click();await page.locator('#seats').selectOption('3');await page.locator('#start-btn').click();await waitGame(page);
      await page.locator('#endTurnBtn').click();await page.locator('#handoff').waitFor({state:'visible'});assert(await page.locator('#hand').isHidden());
      await page.keyboard.press('e');assert.equal(await page.evaluate(()=>__spellTest.game.turn),1,'handoff blocks keyboard actions');
      await page.locator('#ready-btn').click();assert.equal(await page.evaluate(()=>__spellTest.game.round),1);
      await page.locator('#endTurnBtn').click();await page.locator('#ready-btn').click();assert.equal(await page.evaluate(()=>__spellTest.game.round),1);
      await page.locator('#endTurnBtn').click();await page.locator('#ready-btn').click();assert.equal(await page.evaluate(()=>__spellTest.game.round),2,'enemies advance once per whole party');
      await inspectLayout(page);await saveScreenshot(page,`${viewport.width}x${viewport.height}-hotseat`);await context.close();
      console.log(`PASS ${viewport.width}x${viewport.height}: layout, terrain combo, fusion, spellbook, save/resume, hot seat`);
    }

    const context=await browser.newContext({viewport:{width:1280,height:800}}),a=await context.newPage(),b=await context.newPage();watch(a);watch(b);
    await Promise.all([a.goto(`${origin}/game/spelltower.html?gameId=browser-room&username=Aster`),b.goto(`${origin}/game/spelltower.html?gameId=browser-room&username=Bramble`)]);
    await Promise.all([waitGame(a),waitGame(b)]);await a.waitForFunction(()=>!!__spellTest.session);await b.waitForFunction(()=>!!__spellTest.session);
    assert.equal(await b.locator('#endTurnBtn').isDisabled(),true);
    await choose(a,'water');await a.locator('.cell[data-x="1"][data-y="3"]').click();await b.waitForFunction(()=>__spellTest.game.terrain['1,3']?.wet>0);
    assert.equal(await a.evaluate(()=>__spellTest.game.revision),await b.evaluate(()=>__spellTest.game.revision));
    await a.locator('#endTurnBtn').click();await b.waitForFunction(()=>!document.getElementById('endTurnBtn').disabled);
    await b.locator('#endTurnBtn').click();await a.waitForFunction(()=>__spellTest.game.round===2);
    // Both pages see defeat, then both must dismiss the result screen on the host's restart.
    docs[statePath].game.status='lost';docs[statePath].game.hp=0;docs[statePath].game.revision++;roomVersion++;
    await Promise.all([a.locator('#result-dialog').waitFor({state:'visible'}),b.locator('#result-dialog').waitFor({state:'visible'})]);
    await a.locator('#play-again-btn').click();await b.waitForFunction(()=>__spellTest.game.status==='playing'&&!document.getElementById('result-dialog').open);
    assert.equal(await b.evaluate(()=>__spellTest.game.wave),1);
    await a.locator('#pause-btn').click();await a.locator('#leave-btn').click();await b.waitForFunction(()=>__spellTest.game.players.length===1&&__spellTest.meta.host==='Bramble');
    assert.equal(await b.locator('#endTurnBtn').isDisabled(),false,'host departure leaves a playable party');
    await b.reload();await waitGame(b);await b.waitForFunction(()=>!!__spellTest.session);assert.equal(await b.evaluate(()=>__spellTest.game.players[0]),'Bramble');
    const restoredRevision=await b.evaluate(()=>__spellTest.game.revision);
    await Promise.all([b.waitForEvent('load'),b.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})))]);
    await waitGame(b);await b.waitForFunction(()=>!!__spellTest.session);
    assert.equal(await b.evaluate(()=>__spellTest.game.revision),restoredRevision,'back/forward restore reconnects without resetting the room');
    await context.close();console.log('PASS two actual browser clients: online turns, synchronized terrain, restart, host transfer and reconnect');

    const offline=await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true}),p=await offline.newPage();watch(p);
    await p.goto(`${origin}/packs/spelltower/game/spelltower.html?mode=solo&gameId=stale&username=OldRoom`);await p.locator('#start-btn').click();await waitGame(p);
    assert.equal(await p.evaluate(()=>__spellTest.game.mode),'solo');assert.equal(await p.evaluate(()=>__spellTest.session),null);
    await offline.setOffline(true);await choose(p,'water');await p.locator('.cell[data-x="1"][data-y="3"]').click();assert.equal(await p.evaluate(()=>__spellTest.game.mana),5);
    await p.locator('#endTurnBtn').click();assert.equal(await p.evaluate(()=>__spellTest.game.round),2);
    assert.equal(external.length,0,`all test play is local: ${external.join(', ')}`);await offline.close();console.log('PASS generated launcher pack: local assets and gameplay with network disabled');
    assert.deepEqual(missing,[],'all runtime files loaded');assert.deepEqual(errors,[],'no browser exceptions');
  } finally {await browser.close();await new Promise(resolve=>server.close(resolve));fs.rmSync(temporary,{recursive:true,force:true});}
})().catch(error=>{console.error(error);server.close();process.exitCode=1;});
