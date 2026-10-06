import {createRequire} from 'node:module';
import {readFile} from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES+'/playwright/package.json':import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../../',import.meta.url));
const exposure=`\nwindow.__objectives={run:code=>eval(code)};\n`;
const pages=new Map();
for(const file of ['chess_warlord.html','chess_warlord_solo.html'])pages.set('/game/'+file,(await readFile(root+'/game/'+file,'utf8')).replace('\n})();\n</script>',exposure+'\n})();\n</script>'));
const server=http.createServer(async(req,res)=>{
  try{
    const url=req.url.split('?')[0];
    if(pages.has(url)){res.setHeader('content-type','text/html');res.end(pages.get(url));return;}
    const data=await readFile(path.join(root,decodeURIComponent(url)));
    res.setHeader('content-type',url.endsWith('.mjs')?'text/javascript':url.endsWith('.css')?'text/css':'text/plain');res.end(data);
  }catch{res.statusCode=404;res.end('Missing test asset');}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const firestore=`export const getFirestore=()=>({});export const doc=(...a)=>({path:a.slice(1).join('/')});export const collection=doc;export const serverTimestamp=()=>0;export const getDoc=async()=>({exists:()=>false,data:()=>({})});export const getDocs=async()=>({forEach:()=>{}});export const onSnapshot=()=>()=>{};export const setDoc=async()=>{};export const updateDoc=setDoc;export const deleteDoc=setDoc;export const query=(...x)=>x;export const where=query;export const orderBy=query;export const limit=query;export const documentId=()=>'';export const getCountFromServer=async()=>({data:()=>({count:0})});export const runTransaction=async(db,fn)=>fn({get:getDoc,set:()=>{}});`;
let browser;
async function launch(){return chromium.launch({headless:true,executablePath:process.env.CHESS_TEST_CHROMIUM||undefined,args:['--no-sandbox',...(process.env.CHESS_TEST_CHROMIUM?['--single-process','--no-zygote','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']:[])]});}
async function open(file,viewport){
  if(!browser)browser=await launch();
  const page=await browser.newPage({viewport,hasTouch:viewport.width<500});const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.route('https://www.gstatic.com/**',route=>route.fulfill({headers:{'access-control-allow-origin':'*'},contentType:'text/javascript',body:route.request().url().includes('firebase-firestore')?firestore:route.request().url().includes('firebase-auth')?`export const getAuth=()=>({currentUser:null});export const signInAnonymously=async()=>{};export const onAuthStateChanged=()=>()=>{};`:`export const initializeApp=()=>({});`}));
  await page.goto(`http://127.0.0.1:${server.address().port}/game/${file}?username=ObjectiveTester`);
  await page.waitForFunction(()=>window.__objectives && window.__objectives.run('typeof graphicsReady==="undefined" || graphicsReady'));
  return {page,errors};
}

// Run the actual deployed movement, cooldown, spawning and tactical AI, with a reproducible clock.
function simulate(count,mode,seed,seconds){
  const originalRandom=Math.random,originalNow=Date.now;
  let randomState=seed>>>0,clock=1760000000000+seed*1000;
  Math.random=()=>{randomState=(Math.imul(randomState,1664525)+1013904223)>>>0;return randomState/4294967296;};
  Date.now=()=>clock;
  let metrics;
  try{
    lobbyId=null;slotHumans=[];slotNames=Array.from({length:count},(_,i)=>`AI Warlord ${i+1}`);
    slotFactionIds=Array.from({length:count},(_,i)=>DEFAULT_FACTION_IDS[i%DEFAULT_FACTION_IDS.length]);
    PLAYER=0;chosenWarMode=mode;chosenDifficultyId='normal';configureBoardSizeForSlots(count);createGame();PLAYER=-1;
    const roles=new Set(),phases=new Set();let maxArmy=0,offenseCapViolations=0,samples=0,elapsed=0;
    const started=performance.now();
    for(let step=0;step<seconds*4 && !warState.victory;step++){
      clock+=250;paused=false;gameTick(.25);elapsed=(step+1)/4;
      maxArmy=Math.max(maxArmy,pieces.filter(p=>p.alive).length);
      if(step%20===0){
        samples++;
        for(const f of factions.filter(f=>f.alive)){
          const plan=strategicPlanFor(f);if(!plan)continue;
          for(const a of plan.assignments.values())roles.add(a.role);
          for(const s of plan.squads)phases.add(s.phase);
          const n=(alivePiecesByFaction[f.idx]||[]).filter(p=>p.alive&&p.type!=='king'&&!pieceDefs[p.type]?.building).length;
          if(!plan.emergency&&plan.squads.some(s=>s.ids.length>Math.ceil(n*.3)))offenseCapViolations++;
        }
      }
    }
    metrics={count,mode,seed,elapsed,wallSeconds:(performance.now()-started)/1000,maxArmy,samples,offenseCapViolations,roles:[...roles],phases:[...phases],
      bannerCaptures:Object.values(warState.stats).reduce((s,f)=>s+f.banners,0),capitalCaptures:Object.values(warState.stats).reduce((s,f)=>s+f.capitals,0),
      kings:Object.values(warState.stats).reduce((s,f)=>s+f.kings,0),scores:warState.scores,victory:warState.victory,routes:objectivePlanner.routes.size};
  }finally{paused=true;PLAYER=0;Math.random=originalRandom;Date.now=originalNow;}
  return metrics;
}

try{
  if(process.argv.includes('--simulate')){
    const {page,errors}=await open('chess_warlord.html',{width:1440,height:900});
    for(const [count,mode,seed] of [[2,'grand',17],[6,'grand',29],[8,'grand',43],[6,'capital',71]]){
      const result=await page.evaluate(code=>window.__objectives.run(code),`(${simulate.toString()})(${count},${JSON.stringify(mode)},${seed},1200)`);
      console.log(JSON.stringify(result));
      assert.equal(result.offenseCapViolations,0);assert.ok(result.roles.includes('reserve'));assert.ok(result.roles.includes('capital_defender'));assert.ok(result.routes<=64);
      assert.ok(result.victory,'objective matches finish within the twenty-minute simulation budget');
      if(mode==='grand')assert.ok(result.bannerCaptures>0,'AI challenges banners');
      else assert.ok(result.capitalCaptures>0||result.kings>0,'capital mode produces conquest attempts');
    }
    assert.deepEqual(errors,[]);await page.close();
  }else{
    for(const file of ['chess_warlord.html','chess_warlord_solo.html'])for(const viewport of [{width:1440,height:900},{width:390,height:844}]){
      const {page,errors}=await open(file,viewport);
      assert.equal(await page.locator('#warModeSelect option').count(),4);assert.equal(await page.locator('#warModeSelect').inputValue(),'grand');
      if(file==='chess_warlord.html')await page.locator('#soloAiCountSelect').selectOption('2');
      await page.locator('#startBtn').click();
      await page.waitForFunction(()=>window.__objectives.run('gameStarted'));
      const result=await page.evaluate(()=>window.__objectives.run(`(()=>{
        paused=true;
        const modern=typeof setPieceDead==='function';
        const kill=p=>{if(modern)setPieceDead(p,'objective-test');else p.alive=false;};
        const warp=(p,x,y)=>{const ox=p.x,oy=p.y;p.x=x;p.y=y;p.px=x;p.py=y;if(modern)updatePieceTileIndex(p,ox,oy);};
        for(const p of [...pieces])if(p.type!=='king')kill(p);
        pieces.filter(p=>p.alive&&p.type==='king').forEach((p,i)=>warp(p,i,1));
        const banner=warState.objectives.find(o=>o.central),capital=warState.objectives.find(o=>o.home===0);
        const piece=newPiece(0,'pawn',banner.x,banner.y,{ready:true});
        updateWarObjectives(5.75);const before=banner.owner;updateWarObjectives(.25);const captured=banner.owner;
        updateWarObjectives(5);const points=warState.scores[0];
        const hostile=newPiece(1,'pawn',banner.x+1,banner.y,{ready:true});updateWarObjectives(5);
        const contested=banner.status==='contested'&&warState.scores[0]===points;kill(hostile);
        const initialRate=spawnRateForFaction(factions[0],100,10);
        const invader=newPiece(1,'pawn',capital.x,capital.y,{ready:true});updateWarObjectives(12);
        const occupied=capital.owner===1,penalty=spawnRateForFaction(factions[0],100,10)/initialRate;
        updateHUD();draw();const lostNotice=document.getElementById('objectiveHUD').textContent;
        kill(invader);newPiece(0,'pawn',capital.x,capital.y,{ready:true});updateWarObjectives(12);
        const recaptured=capital.owner===0,restored=spawnRateForFaction(factions[0],100,10)/initialRate;
        const pausedAt=warState.elapsed;gameTick(10);const pauseFreezes=warState.elapsed===pausedAt;
        let snapshotFrozen=true;
        if(modern){const snap=buildSelfContainedState();warState.scores[0]++;snapshotFrozen=snap.war.scores[0]!==warState.scores[0];warState.scores[0]--;}
        warState.scores[0]=120;maybeAnnounceWorldWinner();const victory=warState.victory;
        const finalAt=warState.elapsed;paused=false;gameTick(10);const finishFreezes=warState.elapsed===finalAt;
        updateHUD();draw();
        return {before,captured,points,contested,occupied,penalty,recaptured,restored,pauseFreezes,snapshotFrozen,victory,finishFreezes,lostNotice,
          placements:modern?Object.keys(matchRecord.placements).length:factions.length,realms:factions.length,banners:warState.objectives.filter(o=>o.kind==='banner').length,
          title:document.querySelector('#defeatCard h2').textContent};
      })()`));
      assert.equal(result.before,null);assert.equal(result.captured,0);assert.equal(result.points,2);assert.equal(result.contested,true);
      assert.equal(result.occupied,true);assert.equal(result.penalty,.5);assert.equal(result.recaptured,true);assert.equal(result.restored,1);
      assert.equal(result.pauseFreezes,true);assert.equal(result.finishFreezes,true);assert.equal(result.snapshotFrozen,true);assert.match(result.lostNotice,/HOME LOST/);
      assert.equal(result.victory.reason,'dominion');assert.equal(result.victory.slot,0);assert.equal(result.placements,result.realms);assert.equal(result.banners,7);assert.match(result.title,/Dominion Victory/);
      const modeChecks=await page.evaluate(()=>window.__objectives.run(`(()=>{
        chosenWarMode='regicide';createGame();paused=true;updateHUD();const classic=warState.objectives.length===0&&document.getElementById('objectiveHUD').hidden;
        chosenWarMode='capital';createGame();paused=true;
        warState.objectives.filter(o=>o.home!==0).slice(0,warState.capitalTarget).forEach(o=>o.owner=0);
        maybeAnnounceWorldWinner();updateHUD();draw();
        return {classic,imperial:warState.victory?.reason,capitalTarget:warState.capitalTarget,title:document.querySelector('#defeatCard h2').textContent};
      })()`));
      assert.equal(modeChecks.classic,true);assert.equal(modeChecks.imperial,'imperial');assert.match(modeChecks.title,/Imperial Victory/);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);assert.deepEqual(errors,[]);
      console.log(JSON.stringify({file,viewport,passed:true,realms:result.realms,capitalTarget:modeChecks.capitalTarget}));await page.close();await browser.close().catch(()=>{});browser=null;
    }
  }
}finally{await browser?.close().catch(()=>{});server.close();}
