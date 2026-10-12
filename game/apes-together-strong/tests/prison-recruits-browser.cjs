'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 const errors=[],requests=[];
 try{
  const page=await browser.newPage({viewport:{width:1440,height:1000}});
  page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(/^https?:/.test(r.url()))requests.push(r.url())});
  await page.goto(pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href);
  await page.waitForFunction(()=>ATSVisualAssets.status==='ready');await page.locator('#newRun').click();await page.keyboard.press('Escape');
  const result=await page.evaluate(async()=>{
   const g=ATS.game,w=g.world;g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g._prisons=null;
   w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w.stream=()=>{};w.terrain=()=>({biome:'forest',water:false});w.waterBlocked=()=>false;
   g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;
   let id;for(let i=0;i<1000;i++){id='banner-qa-'+i;if(ATSUtil.hash(w.seed+'|prison-champion|'+id+'|0')%100>=95)break}
   const chunk=w.chunks.get('0,0');chunk.objects=[];chunk.sites=[];
   w._buildSite(chunk,{id,x:0,y:0,name:'Champion rescue',type:'armoredDepot',prisonKind:'blacksite',military:true,tier:5,guards:0,count:44,radius:540,extentX:466,extentY:300,layout:0});
   const s=w.sites.get(id),cell=s.objects.map(id=>w.objects.get(id)).find(o=>o.type==='cage');s.known=true;s.radioDown=true;
   const expected={normal:cell.normalCount,champions:cell.championCount,total:cell.count};
   for(const key of['powerId','controlId'])g.damageObject(w.objects.get(s.prison[key]),1e6,g.king);
   for(const gate of s.gates)g.siege.openGate(w.objects.get(gate));
   g.damageObject(cell,1e6,g.king);g.syncIndexes();
   const distinctPositions=new Set(g.apes.map(a=>a.x.toFixed(2)+','+a.y.toFixed(2))).size;
   const immediate={actors:g.apes.length,champions:g.apes.filter(a=>a.champion?.prisonChampion).length,freed:g.stats.freed,allFollowers:g.apes.every(a=>a.state==='follow'),distinctPositions};
   // Exercise real commands and actor movement in an open test lane so walls
   // cannot mask a regression where released units remain passive crowd art.
   w.objects.clear();w._spatial.clear();w.sites.clear();g._prisons.active=[];
   g.king.x=g.king.y=0;g.apes.forEach((a,i)=>{a.x=60+(i%5)*18;a.y=-40+Math.floor(i/5)*18});
   g.commandCD=0;g.command('hold');const held=g.apes.every(a=>a.state==='hold');
   g.commandCD=0;g.command('recall');g.king.x=350;
   const before=g.apes.map(a=>({x:a.x,y:a.y}));
   for(let i=0;i<90;i++){g.time+=1/30;g.navigation.beginFrame(g.time);g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.humanGrid.rebuild(g.humans);g.performance.beginStep(g);g.siege.tick(1/30);for(const a of g.apes)g.updateApe(a,1/30)}
   const moving=g.apes.filter((a,i)=>Math.hypot(a.x-before[i].x,a.y-before[i].y)>8).length;
   const saved=JSON.parse(JSON.stringify(g.serialize())),loaded=ATSGame.fromJSON(saved);
   const persistent=loaded.apes.length===expected.total&&loaded.apes.filter(a=>a.champion?.prisonChampion).length===expected.champions&&new Set(loaded.apes.map(a=>a.id)).size===expected.total;
   await ATSVisualAssets.preload(g.apes.filter(a=>a.champion).map(a=>'characters-armor-'+a.species+'-tier3'));
   const cv=document.createElement('canvas');cv.id='prisonRescuePreview';cv.width=1440;cv.height=720;cv.style.cssText='width:1440px;height:720px;display:block;position:fixed;top:0;left:0;z-index:100000';
   document.body.append(cv);
   const r=new ATSRenderer(cv);cv.width=1440;cv.height=720;r.camera.zoom=1;r.quality='high';r.detailLevel=0;r.time=g.time;r._tribalGame=g;const c=cv.getContext('2d');
   c.fillStyle='#142e2b';c.fillRect(0,0,1440,720);c.fillStyle='#e8d29c';c.font='bold 30px Arial';c.fillText('A cage worth rescuing',60,65);c.font='18px Arial';c.fillStyle='#b9c8b3';c.fillText(expected.normal+' normal monkeys + '+expected.champions+' armored champions · individual followers',60,100);
   const actors=[...g.apes].sort((a,b)=>Number(!!a.champion)-Number(!!b.champion));
   actors.forEach((a,i)=>{const cols=8,x=95+(i%cols)*177,y=330+Math.floor(i/cols)*160;c.save();c.translate(x,y);c.scale(1.4,1.4);r.drawApe(c,{...a,dir:.3,visualDir:.3,moving:false},false);c.restore()});
   return{expected,immediate,held,moving,persistent,banners:r.championStandardDraws||0};
  });
  assert.ok(result.expected.normal>=10&&result.expected.normal<=20);assert.equal(result.expected.champions,2);
  assert.equal(result.immediate.actors,result.expected.total);assert.equal(result.immediate.champions,2);assert.equal(result.immediate.freed,0);
  assert.ok(result.immediate.allFollowers);assert.equal(result.immediate.distinctPositions,result.expected.total);assert.ok(result.held);assert.equal(result.moving,result.expected.total);assert.ok(result.persistent);assert.equal(result.banners,2);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.locator('#prisonRescuePreview').screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,'prison-recruits.png')})}
  assert.deepEqual(errors,[]);assert.deepEqual(requests,[]);console.log(JSON.stringify(result));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
