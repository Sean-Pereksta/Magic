import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';

const require=createRequire(import.meta.url);
let chromium;
try{({chromium}=require('playwright'));}
catch{({chromium}=require(path.join(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES||'/opt/codex/runtimes/codex-primary-runtime/dependencies/node/node_modules','playwright')));}
const root=path.resolve(import.meta.dirname,'../../..');
const firebase={
  'firebase-app.js':`export const initializeApp=()=>({});export const getApp=()=>({});export const getApps=()=>[];`,
  'firebase-auth.js':`const auth={currentUser:{uid:'recruitment-user',getIdToken:async()=> 'test-token'}};export const getAuth=()=>auth;export const signInAnonymously=async()=>({user:auth.currentUser});export const signInWithEmailAndPassword=async()=>({user:auth.currentUser});export const onAuthStateChanged=(a,fn)=>{setTimeout(()=>fn(a.currentUser),0);return ()=>{}};`,
  'firebase-firestore.js':`const docs=new Map();export const getFirestore=()=>({});export const doc=(...parts)=>({path:parts.slice(1).join('/')});export const collection=doc;export const query=(...x)=>x;export const where=(...x)=>x;export const getDoc=async ref=>({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}});export const setDoc=(ref,data)=>docs.set(ref.path,data);export const updateDoc=setDoc;export const runTransaction=async(db,fn)=>fn({get:getDoc,set:setDoc,update:updateDoc});export const onSnapshot=(ref,fn)=>{setTimeout(()=>ref instanceof Array?fn({docs:[]}):fn({exists:()=>docs.has(ref.path),data:()=>docs.get(ref.path)||{}}),0);return ()=>{}};`
};

async function serve(){
  const server=http.createServer(async(req,res)=>{
    try{
      const file=path.join(root,new URL(req.url,'http://localhost').pathname);
      if(!file.startsWith(root+path.sep))throw Error('bad path');
      res.setHeader('Content-Type',file.endsWith('.html')?'text/html':file.endsWith('.css')?'text/css':'text/javascript');
      res.end(await fs.readFile(file));
    }catch{res.writeHead(404);res.end();}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  return {server,url:'http://127.0.0.1:'+server.address().port};
}

// Instrument the actual host deployment, card, snapshot, and recovery paths.
// Each side has five legal types and enough 2×2 units to outgrow the old board.
const instrumentation=`globalThis.__forgeRecruitment={
  get state(){return S},get battle(){return B},get board(){return {width:W,height:H}},
  prepare(){
    stopHostLoops();
    const troops=['golem','guardian','phaser','storm','wardmaker'];
    const deck={id:'recruitment-aesir',name:'Growing Army',faction:'Aesir',troops};
    S.decks={p0:deepClone(deck),p1:deepClone(deck)};S.factions={p0:'Aesir',p1:'Aesir'};
    S.roster={p0:troops.map(type=>({type,count:type==='golem'?20:4,lvl:1})),p1:troops.map(type=>({type,count:type==='golem'?20:4,lvl:1}))};
    S.collections={p0:{unlocked:troops},p1:{unlocked:troops}};
    S.relics={p0:[],p1:[]};S.tactics={p0:[],p1:[]};S.evolutions={p0:{},p1:{}};S.comboEnhancements={p0:[],p1:[]};S.formations={p0:'balanced',p1:'balanced'};
    S.field=fieldEmpty();S.phase='draft';
  },
  packs(){return Array.from({length:80},()=>packFor(S.roster.p0,'Aesir','p0',S))},
  recruit(){applyPickToState(S,'p0',{type:'troop',key:'golem',name:'Sentinel Golem'})},
  deploy(){rebuildFieldFromRoster(S);S.phase='battle';applyHostBattleVisual(S.field);return deepClone(S.field.units)},
  wire(){return JSON.parse(JSON.stringify(makeBattlePayload(S.field,100)))},
  apply(wire){syncBattleDimensions({width:8,height:16});applyBattleSnapshot(wire);return deepClone(B)},
  async recover(wire,fromFirestore){
    stopHostLoops();syncBattleDimensions({width:8,height:16});S.phase='battle';B=fromFirestore?null:deepClone(wire);
    if(fromFirestore)await setDoc(battleRef,deepClone(wire));
    await runBattle();const recovered=deepClone(hostBattleState);stopHostLoops();return recovered;
  },
  terrainAcrossResize(){
    const original={width:W,height:H},field={...fieldEmpty(),poison:[{x:0,y:0,until:Date.now()+60000,p:0}]};
    paint([],field);const firstCell=cellsEl.firstElementChild,before=!!firstCell.querySelector('.poison');
    syncBattleDimensions({width:original.width===8?10:8,height:original.width===8?20:16});
    paint([],field);const after=!!cellsEl.firstElementChild.querySelector('.poison'),replaced=firstCell!==cellsEl.firstElementChild;
    syncBattleDimensions(original);paint([],S.field);return {before,after,replaced};
  },
  invalidDimensions(){
    return [{width:Number.MAX_SAFE_INTEGER,height:Number.MAX_SAFE_INTEGER},{width:8,height:Number.MAX_SAFE_INTEGER},{width:Number.MAX_SAFE_INTEGER,height:16},{width:9,height:18},{width:10,height:16},{width:66,height:132},{width:6,height:12}].map(dimensions=>{
      syncBattleDimensions(dimensions);return {width:W,height:H,cells:cellsEl.children.length};
    });
  },
  impossibleArmy(){
    const state=deepClone(S),firstCell=cellsEl.firstElementChild;state.roster.p0[0].count=Number.MAX_SAFE_INTEGER;
    try{rebuildFieldFromRoster(state);return {error:null,sameGrid:firstCell===cellsEl.firstElementChild};}
    catch(error){return {error:error.message,sameGrid:firstCell===cellsEl.firstElementChild};}
  }
};\n/* Fullscreen */`;

function assertDeployment(units,board,expectedCounts){
  const ids=new Set(),occupied=new Set();
  for(const unit of units){
    assert.ok(!ids.has(unit.id),'every deployed body has a unique identity');ids.add(unit.id);
    const size=unit.size===2?2:1;
    assert.ok(unit.x>=0&&unit.y>=0&&unit.x+size<=board.width&&unit.y+size<=board.height,'every footprint fits the expanded battlefield');
    assert.ok(unit.p===0?unit.y>=board.height/2:unit.y+size<=board.height/2,'initial armies stay in their own half');
    for(let y=unit.y;y<unit.y+size;y++)for(let x=unit.x;x<unit.x+size;x++){
      const tile=x+','+y;assert.ok(!occupied.has(tile),'deployed footprints never overlap');occupied.add(tile);
    }
  }
  for(const side of [0,1])for(const [type,count] of Object.entries(expectedCounts[side]))assert.equal(units.filter(unit=>unit.p===side&&unit.type===type).length,count,`all ${side}/${type} copies deploy`);
}

test('growing five-type armies keep recruiting, deploy completely, fit the screen, and survive snapshot recovery',async()=>{
  const {server,url}=await serve();let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    const context=await browser.newContext({viewport:{width:1366,height:900}});
    await context.route(url+'/**',async route=>{
      try{
        const file=path.join(root,new URL(route.request().url()).pathname);
        if(!file.startsWith(root+path.sep))throw Error('bad path');
        await route.fulfill({contentType:file.endsWith('.css')?'text/css':'text/javascript',body:await fs.readFile(file)});
      }catch{await route.fulfill({status:404,body:''});}
    });
    await context.addInitScript(()=>localStorage.setItem('gh.username','Sean'));
    await context.route('https://www.gstatic.com/firebasejs/**',route=>route.fulfill({contentType:'text/javascript',body:firebase[new URL(route.request().url()).pathname.split('/').at(-1)]||''}));
    await context.route('**/graphics/**',route=>route.fulfill({status:404,body:''}));
    await context.route('**/faction-forge/config.js',route=>route.fulfill({contentType:'text/javascript',body:`export const FACTION_FORGE_CONFIG={assetBaseUrl:'',progressionApiUrl:'',matchmakingTimeoutMs:30000};`}));
    await context.route('**/game/crowncouncil.html*',async route=>{
      const html=await fs.readFile(path.join(root,'game/crowncouncil.html'),'utf8');
      assert.ok(html.includes('/* Fullscreen */'),'browser instrumentation anchor exists');
      await route.fulfill({contentType:'text/html',body:html.replace('/* Fullscreen */',instrumentation)});
    });
    const page=await context.newPage(),errors=[],startup=[];page.on('pageerror',error=>errors.push(error.message));
    page.on('requestfailed',request=>startup.push(request.url()+' '+request.failure()?.errorText));
    page.on('console',message=>{if(message.type()==='error')startup.push(message.text());});
    await page.goto(url+'/game/crowncouncil.html');
    await page.waitForFunction(()=>globalThis.__forgeRecruitment,null,{timeout:5000}).catch(async error=>{throw new Error('Game initialization failed: '+JSON.stringify({errors,startup,body:(await page.locator('body').innerText()).slice(0,300)}),{cause:error});});
    await page.getByRole('button',{name:'Practice against AI',exact:true}).click();
    await page.getByRole('button',{name:'Select and continue with Iron Vanguard',exact:true}).click();
    await page.waitForFunction(()=>globalThis.__forgeRecruitment?.state?.phase==='draft');
    await page.evaluate(()=>__forgeRecruitment.prepare());
    const packs=await page.evaluate(()=>__forgeRecruitment.packs());
    assert.equal(packs.length,80);
    for(const pack of packs){
      assert.equal(pack.length,3);
      assert.ok(pack.filter(card=>card.type==='troop').length>=2,'large armies still receive at least two recruitment choices');
      assert.ok(pack.filter(card=>card.type==='troop').every(card=>['golem','guardian','phaser','storm','wardmaker'].includes(card.key)),'recruits stay within the five deck types');
    }
    await page.evaluate(()=>__forgeRecruitment.recruit());
    assert.equal(await page.evaluate(()=>__forgeRecruitment.state.roster.p0.reduce((sum,row)=>sum+row.count,0)),37,'a recruitment pick adds a body beyond the old five-body cap');
    const units=await page.evaluate(()=>__forgeRecruitment.deploy());
    const board=await page.evaluate(()=>__forgeRecruitment.board);
    assert.ok(board.width*board.height>128,'the battlefield grows to fit the army');
    assert.equal(units.length,73);
    const counts=[{golem:21,guardian:4,phaser:4,storm:4,wardmaker:4},{golem:20,guardian:4,phaser:4,storm:4,wardmaker:4}];
    assertDeployment(units,board,counts);
    for(const size of [{width:1366,height:900},{width:390,height:844},{width:844,height:390}]){
      await page.setViewportSize(size);
      await page.waitForFunction(()=>{
        const board=document.getElementById('cells').getBoundingClientRect(),view=document.getElementById('viewport').getBoundingClientRect();
        return board.width>0&&board.height>0&&board.left>=view.left&&board.right<=view.right&&board.top>=view.top&&board.bottom<=view.bottom&&view.bottom<=innerHeight+1;
      });
      const layout=await page.evaluate(()=>({cells:document.querySelectorAll('#cells>.cell').length,width:document.documentElement.scrollWidth,height:document.documentElement.scrollHeight}));
      assert.equal(layout.cells,board.width*board.height,'the displayed grid matches expanded battle dimensions');
      assert.ok(layout.width<=size.width&&layout.height<=size.height,'the expanded battlefield fits the viewport');
    }
    const wire=await page.evaluate(()=>__forgeRecruitment.wire());
    assert.ok(wire.unitGroups&&typeof wire.unitGroups==='object','network snapshots group troop bodies by identity');
    const ids=units.map(unit=>unit.id).sort();
    const applied=await page.evaluate(wire=>__forgeRecruitment.apply(wire),wire);
    assert.deepEqual(applied.units.map(unit=>unit.id).sort(),ids,'receiving a grouped snapshot preserves every body');
    assert.deepEqual(await page.evaluate(()=>__forgeRecruitment.board),board,'receiving a grouped snapshot restores the expanded dimensions');
    for(const fromFirestore of [false,true]){
      const recovered=await page.evaluate(({wire,fromFirestore})=>__forgeRecruitment.recover(wire,fromFirestore),{wire,fromFirestore});
      assert.deepEqual(recovered.units.map(unit=>unit.id).sort(),ids,`${fromFirestore?'Firestore':'cached'} recovery preserves every body`);
      assert.equal(recovered.units.filter(unit=>unit.size===2).length,41,'recovery preserves the large-unit footprints');
      assert.deepEqual(await page.evaluate(()=>__forgeRecruitment.board),board,'host recovery restores the expanded dimensions');
    }
    assert.deepEqual(await page.evaluate(()=>__forgeRecruitment.terrainAcrossResize()),{before:true,after:true,replaced:true},'rebuilding the grid repaints poison at a coordinate whose terrain did not change');
    assert.deepEqual(await page.evaluate(()=>__forgeRecruitment.invalidDimensions()),Array.from({length:7},()=>({width:8,height:16,cells:128})),'malformed or oversized snapshot dimensions fall back to the small grid');
    const impossible=await page.evaluate(()=>__forgeRecruitment.impossibleArmy());
    assert.match(impossible.error,/Invalid battlefield data/);assert.equal(impossible.sameGrid,true,'an impossible army is rejected before rebuilding the DOM');
    assert.deepEqual(errors,[]);
    await context.close();
  }finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
});
