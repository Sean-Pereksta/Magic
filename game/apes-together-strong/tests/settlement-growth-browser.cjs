/* Render the spread-out village and real airborne tower projectiles in Chrome. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{for(const mobile of [false,true]){
  const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.goto(pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href);
  await page.waitForFunction(()=>ATSVisualAssets.status==='ready');await page.locator('#seedInput').fill('SPACIOUS-VILLAGE');await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.screen==='play');
  const initial=await page.evaluate(mobile=>{
   const g=ATS.game,r=ATS.renderer;g.update=()=>{};g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g.effects=[];
   g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.world.settlementPlot=()=>({valid:true,trees:[],blocked:[]});
   g.king.x=g.king.y=0;g.king.hp=g.king.maxHp;g.time=100;
   const s={id:'gallery',name:'Willow Village',x:0,y:0,population:40,level:4,radius:480,food:2000,wood:500,livingFounding:true};g.settlements.push(s);
   for(let i=0;i<40;i++)g.makeApe(Math.cos(i*2.399)*160,Math.sin(i*2.399)*160,'settled',s.id);g.refreshSettlements();g.colonies.init(s);s.developedRadius=s.radius=480;
   for(let i=0;i<16;i++){const p=g.colonies.queue(s,'hut');if(!p)throw Error('Missing safe hut plot');g.colonies.complete(s,p);s.projects=s.projects.filter(q=>q!==p)}
   for(const kind of ['garden','cooking','storage','workShelter','training']){const p=g.colonies.queue(s,kind);if(p){g.colonies.complete(s,p);s.projects=s.projects.filter(q=>q!==p)}}
   const tower={id:'gallery-tower',kind:'spearTower',x:350,y:0,hp:220,maxHp:220,stage:4,station:{x:308,y:24},shotAt:0};s.facilities.push(tower);g.world.syncSettlementBuildings(s,g);g.colonies.assignJobs(s,g.colonies.members(s));g.colonies.staffFacilities(s);Object.assign(g.apesById.get(tower.staffedBy),tower.station);
   const enemy=g.makeHuman(1230,0,null);enemy.role='sniper';enemy.state='combat';g.humanGrid.rebuild(g.humans);g.apeGrid.rebuild([g.king,...g.apes]);g.colonies.updateDefenses(1/60);tower.shotAt=1e9;
   window.gallery={s,tower,enemy,hp:enemy.hp};r.camera.x=200;r.camera.y=0;r.camera.zoom=mobile?.55:.85;g.viewRadius=1600;
   for(let i=0;i<55;i++){g.time+=1/60;g.colonies.updateDefenses(1/60)}
   return{huts:s.huts.length,housing:s.housing,spears:s.spears.length,range:tower.range,growth:g.colonies.growthLabel(s),inFlight:s.spears.some(p=>p.x>600&&p.z>20),hp:enemy.hp};
  },mobile);
  assert.equal(initial.huts,16);assert.equal(initial.housing,166);assert.equal(initial.spears,1);assert.equal(initial.range,900);assert.ok(initial.inFlight);assert.match(initial.growth,/Growing.*young\/min/);
  await page.evaluate(()=>document.getElementById('toastArea').replaceChildren());await page.waitForTimeout(150);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'settlement-growth-mobile.png':'settlement-growth-desktop.png')})}
  const impact=await page.evaluate(()=>{const g=ATS.game,{s,enemy,hp}=gallery;for(let i=0;i<140;i++){g.time+=1/60;g.colonies.updateDefenses(1/60)}return{hurt:enemy.hp<hp,impact:g.effects.some(e=>e.type==='spearImpact'),shafts:s.spears.length}});
  assert.equal(impact.hurt,true);assert.equal(impact.impact,true);assert.equal(impact.shafts,0);
  if(mobile){await page.locator('#armyDock .army-expand').tap();await page.locator('[data-army-command=build]').tap()}else await page.keyboard.press('b');await page.waitForFunction(()=>ATS.screen==='build');
  assert.match(await page.locator('#buildResources').innerText(),/166 housed.*Growing.*young\/min/);assert.match(await page.locator('#buildCatalog').innerText(),/900 units/);
  assert.deepEqual(errors,[]);await page.close();
 }
 console.log('PASS: desktop/mobile village layout, visible long-range spear flight and impact, housing/growth labels, no overflow or browser errors.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
