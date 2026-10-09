/* Real Q/Z/Shift-Z commands retain a developed village and resume family growth. */
'use strict';
const assert=require('node:assert/strict'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
const url=pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href;

function isolateVillageGame(){
 const g=ATS.game;
 // Advance the actual colony economy explicitly so wall-clock keyboard holds
 // cannot change saved resources, birth progress, or a child's age mid-check.
 g.update=()=>{};g.colonies.plan=()=>{};g.colonies.work=()=>{};
 g.humans=[];g.vehicles=[];g.helis=[];
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();
 g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];
 g.world.terrain=()=>({biome:'forest',water:false,road:false});g.world.waterBlocked=()=>false;
 g.king.x=g.king.y=0;g.king.hp=g.king.maxHp;g.commandCD=0;
 g.siege.select([]);g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;
 g.syncIndexes();g.humanGrid.rebuild([]);g.vehicleGrid.rebuild([]);g.apeGrid.rebuild(g.apes,g.king);
 for(const s of g.settlements)g.world.syncSettlementBuildings(s,g);
 window.villageCommands=[];
 const command=g.command;
 g.command=function(cmd,...args){const ok=command.call(this,cmd,...args);villageCommands.push({cmd,ok,horde:this.lastHordeCommand?{...this.lastHordeCommand}:null});return ok};
 ATS.army.update(true);
}
function villageSnapshot(id){
 const g=ATS.game,s=g.settlement(id),copy=value=>JSON.parse(JSON.stringify(value));
 const building=o=>({id:o.id,kind:o.kind||'hut',x:o.x,y:o.y,hp:o.hp,maxHp:o.maxHp,stage:o.stage,capacity:o.capacity,progress:o.progress});
 return{
  id:s.id,name:s.name,count:g.settlements.length,founded:g.stats.settlements,born:g.stats.born,
  population:s.population,children:s.children,food:s.food,wood:s.wood,birthTimer:s.birthTimer,
  familyRate:g.colonies.familyGrowth(s).rate,growthRate:s.growthRate,
  assets:{level:s.level,housing:s.housing,expansionLevel:s.expansionLevel,lodge:building(s.lodge),
   huts:s.huts.map(building),facilities:s.facilities.map(building),structures:s.structures.filter(o=>!o.activityZone).map(building),
   projects:copy(s.projects),commissions:copy(s.commissions)},
  childrenState:g.apes.filter(a=>a.hp>0&&a.state==='young').map(a=>({id:a.id,state:a.state,age:a.age,hp:a.hp,maxHp:a.maxHp,settlementId:a.settlementId,recalling:!!a.recallOrder})),
  worldBuildings:Array.from(g.world.objects.values()).filter(o=>o.type==='apeBuilding'&&o.settlementId===id).map(o=>({id:o.id,structureId:o.structureId,hp:o.hp,x:o.x,y:o.y})).sort((a,b)=>a.id.localeCompare(b.id))
 };
}
const snapshot=(page,id)=>page.evaluate(villageSnapshot,id);
function preserved(actual,expected,foodTransfer=0){
 for(const key of ['id','name','count','founded','wood','birthTimer'])assert.equal(actual[key],expected[key],key+' survives mobilization and return');
 assert.equal(actual.food,expected.food+foodTransfer,'only the normal carried-food transfer changes stored food');
 assert.deepEqual(actual.assets,expected.assets,'homes, nursery, lodge damage, and paid construction progress survive');
 assert.deepEqual(actual.worldBuildings,expected.worldBuildings,'the same physical village buildings remain');
}
async function economy(page,id,seconds){
 return page.evaluate(({id,seconds})=>{
  const g=ATS.game,s=g.settlement(id),before={born:g.stats.born,timer:s.birthTimer,population:s.population};
  let advanced=false;
  for(let i=0;i<seconds;i++){g.time++;g.refreshSettlements();const timer=s.birthTimer;g.colonies.tick(s);advanced ||= s.birthTimer>timer||g.stats.born>before.born}
  g.refreshSettlements();ATS.army.update(true);
  return{before,born:g.stats.born,timer:s.birthTimer,population:s.population,rate:s.growthRate,advanced};
 },{id,seconds});
}
async function fullRecall(page,id,expected){
 await page.evaluate(()=>{ATS.game.commandCD=0;villageCommands.length=0});
 await page.keyboard.down('q');
 await page.waitForFunction(()=>villageCommands.some(c=>c.cmd==='recallAll'&&c.ok),undefined,{timeout:5000});
 await page.keyboard.up('q');
 assert.deepEqual(await page.evaluate(()=>villageCommands.map(c=>({cmd:c.cmd,ok:c.ok,count:c.horde.count,mobilized:c.horde.mobilized}))),[
  {cmd:'recallAll',ok:true,count:expected.population,mobilized:expected.population}
 ],'a full Q hold mobilizes the village once');
 const empty=await snapshot(page,id);preserved(empty,expected);
 assert.equal(empty.population,0);assert.equal(empty.familyRate,0);
 assert.deepEqual(empty.childrenState,expected.childrenState.map(a=>({...a,settlementId:null,recalling:true})),'mobilized children stay young and keep their age and health');
 const idle=await economy(page,id,50);assert.equal(idle.born,expected.born,'an empty village cannot create children');assert.equal(idle.timer,expected.birthTimer,'empty time preserves family progress');
 preserved(await snapshot(page,id),expected);
}
async function returnToVillage(page,id,key,expected){
 await page.evaluate(()=>{const g=ATS.game;g.commandCD=0;g.food=13;villageCommands.length=0;for(const [i,a]of g.apes.entries()){a.x=35+i%4*10;a.y=Math.floor(i/4)*10}g.apeGrid.rebuild(g.apes,g.king)});
 await page.keyboard.press(key);
 assert.deepEqual(await page.evaluate(()=>villageCommands.map(c=>({cmd:c.cmd,ok:c.ok}))),[{cmd:key==='z'?'settle':'settleAll',ok:true}]);
 const returned=await snapshot(page,id);preserved(returned,expected,13);
 assert.equal(returned.population,expected.population,'all recalled residents return to their existing village');
 assert.deepEqual(returned.childrenState,expected.childrenState,'returning children keep their state, age, health and original village assignment');
 assert.equal(returned.familyRate,expected.familyRate,'the established homes and nursery support the same family growth rate');
 const growth=await economy(page,id,50);
 assert.equal(growth.advanced,true);assert.ok(growth.born>growth.before.born,'births resume after residents return');
 assert.ok(growth.population>growth.before.population);assert.equal(growth.rate,expected.familyRate);
 const grown=await snapshot(page,id);assert.deepEqual(grown.assets,expected.assets);assert.ok(grown.population<=grown.assets.housing,'births respect existing housing');assert.ok(grown.food>=0,'births respect available food');
 return grown;
}

(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox','--disable-dev-shm-usage']});
 const errors=[];
 try{
  const page=await browser.newPage({viewport:{width:1440,height:900}});page.setDefaultTimeout(10000);page.on('pageerror',error=>errors.push(error.message));
  await page.goto(url);await page.locator('#seedInput').fill('VILLAGE-REGROWTH');await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.screen==='play');
  await page.evaluate(isolateVillageGame);
  await page.evaluate(()=>{const g=ATS.game;g.apes=[];g.settlements=[];g.siege.groups={};g.time=1;g.stats.settlements=0;for(let i=0;i<8;i++)g.makeApe(35+i*5,20,'follow');g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king)});
  await page.keyboard.press('Shift+z');
  const id=await page.evaluate(()=>{
   const g=ATS.game,s=g.settlements[0];s.name='Nursery Home';s.level=2;s.food=140;s.wood=77;s.birthTimer=29.15;s.safety=1;
   s.huts=[{id:s.id+'-house-a',kind:'hut',x:-190,y:120,hp:83,maxHp:100,capacity:10,stage:4,progress:1},{id:s.id+'-house-b',kind:'hut',x:-190,y:-120,hp:55,maxHp:100,capacity:10,stage:4,progress:1}];
   s.facilities.push({id:s.id+'-nursery',kind:'nursery',x:220,y:-120,hp:175,maxHp:180,stage:4,progress:1});
   for(let i=0;i<3;i++)s.structures.push({id:s.id+'-garden-'+i,kind:'garden',x:180+i*100,y:150,hp:100,maxHp:100,stage:4,progress:1});
   s.structures.push({id:s.id+'-store',kind:'storage',x:-90,y:230,hp:95,maxHp:100,stage:4,progress:1});s.lodge.hp=355;
   const project={id:s.id+'-paid-nursery',kind:'nursery',x:330,y:-150,work:17,total:55,timber:42,createdAt:1,commissioned:true,commissionKind:'nursery',paidCost:{wood:42,food:35},done:false};
   s.projects.push(project);s.commissions.push({id:project.id,projectId:project.id,kind:'nursery',status:'queued',createdAt:1,cost:{wood:42,food:35}});
   const child=g.makeApe(-30,-20,'young',s.id,true);child.age=11.25;child.hp=17;
   g.refreshSettlements();g.colonies.init(s);s.suitability={fertility:1,capacity:100,water:false};g.world.syncSettlementBuildings(s,g);g.apeGrid.rebuild(g.apes,g.king);ATS.army.update(true);return s.id;
  });
  const baseline=await economy(page,id,50);assert.ok(baseline.born>baseline.before.born,'the established village produces children before mobilization');
  const established=await snapshot(page,id);assert.ok(established.familyRate>0);assert.equal(established.assets.housing,26);assert.equal(established.assets.facilities.filter(f=>f.kind==='nursery').length,1);
  await fullRecall(page,id,established);
  const grown=await returnToVillage(page,id,'z',established);

  await fullRecall(page,id,grown);
  const savedEmpty=await snapshot(page,id);
  await page.keyboard.press('Escape');await page.locator('#saveRun').click();await page.reload();
  // Freeze the restored game before its first playable animation frame.
  await page.evaluate(()=>{const restore=ATSGame.fromJSON;ATSGame.fromJSON=function(...args){const g=restore.apply(this,args);g.update=()=>{};return g}});
  await page.locator('#continueRun').click();await page.waitForFunction(()=>ATS.screen==='play');await page.evaluate(isolateVillageGame);
  const loadedEmpty=await snapshot(page,id);preserved(loadedEmpty,savedEmpty);assert.equal(loadedEmpty.population,0);assert.deepEqual(loadedEmpty.childrenState,savedEmpty.childrenState);
  await returnToVillage(page,id,'Shift+z',grown);
  assert.deepEqual(errors,[]);
  console.log('PASS: full Q empties one established village; Z and Shift-Z reuse it with homes, nursery, damaged lodge, supplies, paid work and birth progress preserved; young retain age/health; family rates and births resume; empty save/load also resumes correctly; no browser errors.');
 }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
