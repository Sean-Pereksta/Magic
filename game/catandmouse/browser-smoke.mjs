// Exercises the actual public loader/core and real DOM, with network/auth
// replaced in memory. No preview HTML, screenshots, or Firebase writes.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { resolve, extname } from 'node:path';
const require=createRequire(import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../..',import.meta.url));

const firebase={
  'firebase-app.js':'export const initializeApp=()=>({});',
  'firebase-auth.js':'export const getAuth=()=>({currentUser:{uid:"test-player"}});export const signInAnonymously=async()=>{};',
  'firebase-firestore.js':`export const getFirestore=()=>({});export const doc=(_db,path)=>({id:path.split('/').pop()});export const collection=(_db,path)=>({path});
    export const getDoc=async()=>({exists:()=>false,data:()=>({})});export const getDocs=async()=>({docs:[]});
    export const setDoc=async()=>{throw Error('unexpected Firebase write');};export const updateDoc=setDoc;export const deleteDoc=setDoc;
    export const onSnapshot=()=>()=>{};export const runTransaction=setDoc;export const writeBatch=setDoc;export const increment=x=>x;`
};
const fixture=String.raw`
  window.__battleFixture={
    seed(){
      soloMode=true;isHost=true;gameStarted=true;liveSnapshotReady=true;catPower=5;ratPower=1;catHealth=1000;maxCatHealth=1000;
      playerPos={x:9,y:10};catPos={x:16,y:12};players={
        [uid]:{uid,x:9,y:10,alive:true,displayName:'tester',facing:'east',cheese:50},
        remote:{uid:'remote',x:10,y:10,alive:true,displayName:'remote',facing:'east'}};
      for(const path of ASSET_MANIFEST)failedAssetPaths.add(path);
      const placements=[[8,11,'🐇'],[8,13,'🪳'],[10,12,'🥁'],[11,11,'🔫'],[11,13,'🚀'],[11,9,'⚡'],[10,14,'☄️'],[7,10,'🟢'],[7,12,'🕸️'],[7,14,'🪭'],[6,12,'🛡️'],[6,14,'🌰'],[13,13,'🧱'],[13,14,'💎']];
      for(const [x,y,type] of placements){grid[y][x]=type;const hp=STRUCTURE_MAX_HP[type];structureHealth[x+'_'+y]=hp;addToStructureIndex(x,y,type,hp);structures.set(x+'_'+y,{x,y,type,health:hp});}
      markBlockedGridDirty();markStructureIndexDirty();
      rats=[{id:'a',kind:'rat',x:14,y:10,health:100,createdAt:Date.now()},{id:'b',kind:'rat',x:14,y:11,health:100,createdAt:Date.now()},{id:'c',kind:'rat',x:15,y:10,health:100,createdAt:Date.now()}];
      stinkRats=[{id:'s',kind:'stinkrat',x:15,y:9,health:100,createdAt:Date.now()}];
      oxen=[{id:'o',kind:'ox',x:14,y:13,health:100,createdAt:Date.now()}];
      ratKings=[{id:'k',kind:'ratking',x:15,y:14,health:100,createdAt:Date.now()}];
      vultures=[{id:'v',kind:'vulture',x:16,y:10,health:100,createdAt:Date.now()}];
      termites=[{id:'t',kind:'termite',x:14,y:14,health:100,createdAt:Date.now()}];
      rabbits=[{id:'r',kind:'rabbit',x:8,y:9,health:5,spawnX:8,spawnY:11,createdAt:Date.now()}];fleas=[];
      renderGrid();
    },
    async tick(){
      const before=rats.map(r=>({id:r.id,x:r.x,y:r.y}));
      await hostMoveRats();
      const maxRatStep=Math.max(0,...rats.map(r=>{const p=before.find(p=>p.id===r.id);return p?Math.abs(r.x-p.x)+Math.abs(r.y-p.y):0;}));
      await hostMoveOxen();await hostMoveRatKings();await hostMoveStinkRats();await hostMoveVultures();await hostMoveTermites();
      await rabbitDenTick();await fleaNestAttackTick();await hostMoveFleas();await turretAttack();await rocketTowerAttack();await specialStructureTick();
      renderGrid();return {maxRatStep,rabbits:rabbits.length,fleas:fleas.length,invalidSpawn:[...rabbits,...fleas].some(e=>STRUCTURE_TYPES.has(grid[e.y]?.[e.x]))};
    },
    authority(){return JSON.stringify({players,catPos,rats,oxen,ratKings,vultures,termites,rabbits,fleas});},
    rendered(key){return battlePresentation.getPosition(key);},
    moveRemote(){players.remote.x++;renderGrid();},
    moveLocal(){movePlayer(1,0);},
    warn(){catAbilityState.pouncePending={x:12,y:10,executeAt:Date.now()+5000};renderGrid();},
    fx(){presentShot({x:9,y:10},{x:13,y:10},'rocket');presentShot({x:9,y:10},{x:13,y:11},'tesla');presentShot({x:9,y:10},{x:13,y:12},'web');},
    hitStructure(){structureHealth['13_13']=2;structureShields['13_13']=4;renderGrid();structureHealth['13_13']=1;structureShields['13_13']=0;renderGrid();},
    overload(){for(let i=0;i<500;i++)battlePresentation.burst({x:10,y:10},'hit',9);this.fx();},
    finish(){catAbilityState.pouncePending=null;renderGrid();},
    stats(){return window.__catMouseBattleStats();}
  };
`;
const server=createServer(async(req,res)=>{
  try {
    const path=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
    if(path==='/favicon.ico'){res.writeHead(204);res.end();return;}
    const file=resolve(root,`.${path}`);
    if(!file.startsWith(root+'/')){res.writeHead(403);res.end();return;}
    let body=await readFile(file);
    if(path==='/game/catandmouse-core.html'){
      const html=body.toString(),start=html.indexOf('  bootGame().catch(err => {'),end=html.indexOf('</script>',start);
      assert.ok(start>=0 && end>start);body=html.slice(0,start)+fixture+html.slice(end);
    }
    res.setHeader('Content-Type',extname(file)==='.mjs'?'text/javascript':extname(file)==='.css'?'text/css':'text/html');res.end(body);
  }catch{res.writeHead(404);res.end();}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
let browser;
try {
  browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_EXECUTABLE?{executablePath:process.env.CHROMIUM_EXECUTABLE}:{})});
  for(const reduced of [false,true]){
    const context=await browser.newContext({viewport:{width:1000,height:800},reducedMotion:reduced?'reduce':'no-preference'});
    const page=await context.newPage(),errors=[];
    page.on('pageerror',error=>errors.push(error.message));
    page.on('console',message=>{if(message.type()==='error')errors.push(message.text());});
    await page.route('https://www.gstatic.com/firebasejs/**',route=>{
      const name=new URL(route.request().url()).pathname.split('/').pop();
      return route.fulfill({contentType:'text/javascript',headers:{'access-control-allow-origin':'*'},body:firebase[name] || ''});
    });
    await page.route('**/graphics/**',route=>route.fulfill({contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="48" height="24"><rect width="48" height="24" fill="#68b947"/></svg>'}));
    await page.route('**/sound/**',route=>route.fulfill({status:204,body:''}));
    await page.clock.install({time:new Date('2026-10-04T07:00:00Z')});
    await page.goto(`http://127.0.0.1:${server.address().port}/game/catandmouse.html`);
    await page.waitForFunction(()=>!!window.__battleFixture);
    assert.equal(await page.evaluate(()=>window.__CATMOUSE_PERF_PATCH_VERSION),'2026-10-04-rat-difficulty-v1');
    await page.evaluate(()=>window.__battleFixture.seed());
    const first=await page.evaluate(()=>window.__battleFixture.rendered('mouse:remote'));
    await page.evaluate(()=>window.__battleFixture.moveRemote());await page.clock.runFor(55);
    const middle=await page.evaluate(()=>window.__battleFixture.rendered('mouse:remote'));
    assert.ok(middle.x>first.x && middle.x<first.x+1,'remote player has an intermediate visual position');
    await page.clock.runFor(180);assert.equal((await page.evaluate(()=>window.__battleFixture.rendered('mouse:remote'))).x,11);
    const tick=await page.evaluate(()=>window.__battleFixture.tick());assert.ok(tick.maxRatStep<=1);assert.equal(tick.invalidSpawn,false);assert.ok(tick.rabbits<=12);
    const before=await page.evaluate(()=>window.__battleFixture.authority());await page.clock.runFor(160);
    assert.equal(await page.evaluate(()=>window.__battleFixture.authority()),before,'presentation frames leave game state unchanged');
    await page.evaluate(()=>{window.__battleFixture.warn();window.__battleFixture.fx();window.__battleFixture.hitStructure();});await page.clock.runFor(40);
    assert.ok(await page.locator('.cm-marker').count());assert.ok(await page.locator('.cm-projectile').count());
    assert.ok(await page.locator('.cm-damaged').count());
    await page.evaluate(()=>window.__battleFixture.overload());await page.clock.runFor(25);
    const stats=await page.evaluate(()=>window.__battleFixture.stats());assert.ok(stats.presentation.effects<=128);assert.ok(stats.ai.peakDecisions<=5);assert.ok(stats.ai.peakPaths<=10);
    assert.equal(stats.presentation.reducedMotion,reduced);
    await page.evaluate(()=>window.__battleFixture.finish());await page.clock.runFor(6500);
    assert.equal((await page.evaluate(()=>window.__battleFixture.stats())).presentation.effects,0);
    assert.deepEqual(errors,[]);
    console.log(`PASS real optimized battlefield DOM (${reduced?'reduced':'normal'} motion): units, spawns, attacks, bounds, cleanup`);
    await context.close();
  }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
