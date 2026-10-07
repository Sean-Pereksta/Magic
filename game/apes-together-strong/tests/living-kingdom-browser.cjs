/* Real canvas, mature village, construction and faction traversal checks. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{for(const mobile of [false,true]){
  const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<meta name="viewport" content="width=device-width,initial-scale=1"><style>html,body{margin:0;background:#071216}canvas{display:block;width:100vw;height:100vh}</style><canvas id="game"></canvas>');
  for(const name of ['world','navigation','settlements','forces','sim','render','render-details'])await page.addScriptTag({content:fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8')});
  const report=await page.evaluate(()=>{
   const g=new ATSGame('LIVING-KINGDOM','survival');g.king.hp=g.king.maxHp=100000;g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.world.sites.clear();g.spawnSites=()=>{};g.responseDirector=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=100000;
   const home={id:'living-home',name:'Moonroot',x:0,y:0,level:10,radius:550,developedRadius:550,population:500,food:10000,wood:600,gardens:50,livingFounding:true,known:false,age:0,birthTimer:-100000};
   g.settlements=[home];for(let i=0;i<500;i++)g.makeApe(Math.cos(i*2.399)*40,Math.sin(i*2.399)*40,'settled',home.id);g.syncIndexes();g.colonies.init(home);g.king.x=5000;
   for(let i=0;i<340;i++){g.time++;g.refreshSettlements();g.colonies.tick(home)}
   const homes=home.huts.filter(h=>h.stage===4&&h.hp>0);g.king.x=g.king.y=0;for(const a of g.apes){const activity=g.colonies.activityTarget(a,home);Object.assign(a,g.findOpen(activity.x,activity.y,10));a.moving=true;}
   const linePlot=g.findOpen(610,-440,55),human=g.world.createFortification({id:'preview-human-line',...linePlot,team:'human',w:170,h:18}),ape=home.barriers.find(b=>!b.dead);
   const engineer=g.makeHuman(200,-305,null);g.forces.assign(engineer,null,'engineer');g.forces.startEngineerJob(engineer,{x:310,y:-250});engineer.engineerJob.remaining=2;
   for(let i=0;i<18;i++){const h=g.makeHuman(160+(i%6)*34,-320-Math.floor(i/6)*38,null);g.forces.assign(h,null,['rifleman','heavy','sniper','shield','medic','leader'][i%6]);h.squadOrder='Hold Line';h.dir=Math.PI/2;}
   const stage=home.huts[homes.length-1];if(stage){g.colonies.damageHut(home,stage,60)}g.colonies.queue(home,'hut');g.time+=1;
   const r=new ATSRenderer(document.getElementById('game'));r.camera.zoom=innerWidth<650?.45:.78;r.quality='high';for(let i=0;i<12;i++)r.draw(g,0);window.livingPreview={g,r};
   return {homes:homes.length,radius:home.radius,cleared:home.clearedTrees,paths:home.paths.length,structures:home.structures.length,barriers:home.barriers.length,jobs:new Set(g.apes.map(a=>a.job)).size,humanBlocksApes:g.world.actorBlocked(human.x,human.y,10,'ape'),humanAllowsHumans:!g.world.actorBlocked(human.x,human.y,10,'human'),bulletsClear:!g.world.projectileBlocked(human.x,human.y),apeAllowsApes:ape?g.world.fortificationPassable(ape,'ape'):false,apeBlocksHumans:ape?!g.world.fortificationPassable(ape,'human'):false,caches:{huts:r.hutSprites?.size||0,props:r.villageSprites?.size||0,barriers:r.barrierSprites?.size||0}};
  });
  assert.ok(report.homes>=40,JSON.stringify(report));assert.ok(report.radius>=500);assert.ok(report.cleared>0);assert.ok(report.paths>=report.homes);assert.ok(report.structures>0);assert.ok(report.barriers>0);assert.ok(report.jobs>=8);for(const key of ['humanBlocksApes','humanAllowsHumans','bulletsClear','apeAllowsApes','apeBlocksHumans'])assert.equal(report[key],true,key);assert.ok(report.caches.huts<=20&&report.caches.props<=48&&report.caches.barriers<=72);assert.deepEqual(errors,[]);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'living-kingdom-mobile.png':'living-kingdom-desktop.png')})}
  console.log(JSON.stringify({mobile,...report}));await page.close();
 }}finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
