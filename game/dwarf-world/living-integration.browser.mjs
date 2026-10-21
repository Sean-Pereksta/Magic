// Offline integration fixture: serves only repository files and never contacts
// Firebase or changes a production save. Optional Playwright is a dev dependency.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.PLAYWRIGHT_MODULE_PATH||'playwright');
const root=path.resolve(new URL('../../',import.meta.url).pathname.replace(/^\/([A-Za-z]:)/,'$1'));
const bridge=`
window.__livingQA={
  pause:()=>paused=true,
  snapshot:()=>({gold,dwarves:dwarves.map(d=>({id:d.id,name:d.name,level:d.level,weapon:d.weapon,pickaxe:d.pickaxe,armor:d.armor,perks:d.perks})),buildings:JSON.parse(JSON.stringify(dwx.buildings)),rooms:JSON.parse(JSON.stringify(lm.rooms))}),
  legacy:async compressed=>{await saveGameAsync();const raw=localStorage.getItem(SAVE_KEY);const s=JSON.parse(await gunzipB64ToStr(raw.slice(SAVE_MAGIC.length)));delete s.livingMountain;for(const d of s.dwarves)delete d.living;if(compressed){localStorage.setItem(SAVE_KEY,SAVE_MAGIC+await gzipStrToB64(JSON.stringify(s)));return loadGameAsync()}const get=Storage.prototype.getItem;Storage.prototype.getItem=function(k){return k===SAVE_KEY?JSON.stringify(s):get.call(this,k)};try{return await loadGameAsync()}finally{Storage.prototype.getItem=get}},
  quota:async()=>{const before=localStorage.getItem(SAVE_KEY),write=Storage.prototype.setItem;Storage.prototype.setItem=function(k,v){if(k===SAVE_KEY)throw new DOMException('full','QuotaExceededError');return write.call(this,k,v)};try{const result=await saveGameAsync();return {result,preserved:before===localStorage.getItem(SAVE_KEY)}}finally{Storage.prototype.setItem=write}},
  future:async()=>{const before=localStorage.getItem(SAVE_KEY),s=JSON.parse(await gunzipB64ToStr(before.slice(SAVE_MAGIC.length)));s.livingMountain.version=999;localStorage.setItem(SAVE_KEY,SAVE_MAGIC+await gzipStrToB64(JSON.stringify(s)));const count=dwarves.length,result=await loadGameAsync();localStorage.setItem(SAVE_KEY,before);return{result,preserved:count===dwarves.length}},
  malformed:async()=>{const before=localStorage.getItem(SAVE_KEY),s=JSON.parse(await gunzipB64ToStr(before.slice(SAVE_MAGIC.length))),snapshot=JSON.stringify(__livingQA.snapshot());s.gold=1;s.loot={bad:'not an array'};localStorage.setItem(SAVE_KEY,SAVE_MAGIC+await gzipStrToB64(JSON.stringify(s)));const result=await loadGameAsync();localStorage.setItem(SAVE_KEY,before);return{result,preserved:snapshot===JSON.stringify(__livingQA.snapshot())}},
  malformedNested:async kind=>{const before=localStorage.getItem(SAVE_KEY),s=JSON.parse(await gunzipB64ToStr(before.slice(SAVE_MAGIC.length))),fingerprint=()=>JSON.stringify({core:__livingQA.snapshot(),cam,tile:tType[0],mountain:lm,lives:dwarves.map(d=>d.living)}),snapshot=fingerprint();s.gold=1;const tiles=_b64ToU8(s.tTypeB64);tiles[0]=tiles[0]===TILE_EMPTY?TILE_ROCK:TILE_EMPTY;s.tTypeB64=_u8ToB64(tiles);
    const p=s.dwarves[0].living,point={x:fortress.x,y:fortress.y};
    if(kind==='living primitive')s.dwarves[0].living='invalid';
    if(kind==='job missing goal')p.job={kind:'haul',source:'founders-store',dest:'founders-store',resource:'ore',amount:1};
    if(kind==='job bad coordinates')p.job={kind:'sleep',dest:'founders-store',goal:{x:W,y:H}};
    if(kind==='job unknown type')p.job={kind:'unknown',dest:'founders-store',goal:point};
    if(kind==='job bad reference')p.job={kind:'sleep',dest:{id:'founders-store'},goal:point};
    if(kind==='cargo unknown material')p.cargo={resource:'invalid',amount:4};
    if(kind==='cargo negative amount')p.cargo={resource:'ore',amount:-4};
    if(kind==='production bad ingredients')p.job={kind:'produce',dest:'founders-store',goal:point,started:true,inputs:{ore:-3}};
    if(kind==='production wrong workplace')p.job={kind:'produce',dest:'founders-store',goal:point,elapsed:0};
    if(kind==='stockpile missing inventory')delete s.livingMountain.stores[0].inventory;
    if(kind==='caravan missing goal')s.livingMountain.caravans=[{id:'bad-caravan',...point,source:'a',dest:'founders-store',resource:'ore',amount:3,hp:50,speed:2}];
    if(kind==='faction null')s.livingMountain.factions=[null];
    if(kind==='survey null')s.fortressExpansion.surveys=[null];
    if(kind==='item invalid name')s.loot=[{id:'a',category:'weapon',rarity:'common',identityDone:true,name:null},{id:'b',category:'weapon',rarity:'common',identityDone:true,name:'Axe'}];
    localStorage.setItem(SAVE_KEY,SAVE_MAGIC+await gzipStrToB64(JSON.stringify(s)));try{const result=await loadGameAsync();return{result,preserved:snapshot===fingerprint()}}finally{localStorage.setItem(SAVE_KEY,before)}},
  configure:()=>{gold=1000;const d=dwarves[0];d.level=8;d.weapon=dwxMakeRelic(DWX_RELICS.find(r=>r.category==='weapon'));d.perks=['tunnel_runner'];dwx.buildings.push({id:'preserve-hall',type:'great_hall',x:fortress.x+5,y:fortress.y+5,level:2,hp:400,maxHp:400,stats:{serviced:5}});applyDwarfStats(d);},
  cargo:()=>{const d=dwarves[0],p=lmEngine.person(d);p.cargo={resource:'ore',amount:5};p.job={kind:'haul',source:'founders-store',dest:'founders-store',resource:'ore',amount:5,elapsed:0,goal:{x:fortress.x,y:fortress.y}};return d.id},
  checkCargo:id=>dwarves.find(d=>d.id===id).living.cargo,
  capacity:async()=>{dwx.buildings=[];dwarves.length=0;enemies.length=0;lm.deposits=Array.from({length:8192},(_,i)=>({id:'full-'+i,x:100+i%128,y:50+Math.floor(i/128),inventory:Object.fromEntries(DwarfLivingSimulation.MATERIALS.map(k=>[k,k==='knowledge'?1:0]))}));const d=makeDwarf();d.x=fortress.x+4;d.y=fortress.y+2;d.hp=0;lmEngine.person(d).cargo={resource:'ore',amount:5};dwarves.push(d);lmEngine.reindex();removeDead();removeDead();const held=dwarves.length===1&&d._livingRemains&&d.living.cargo.amount===5,point={x:d.x,y:d.y};const saved=await saveGameAsync(),loaded=await loadGameAsync();lm.deposits.pop();lmEngine.reindex();removeDead();const result={held:!!held,saved,loaded,removed:dwarves.length===0,ore:lm.deposits.find(p=>p.x===point.x&&p.y===point.y)?.inventory.ore};lm.deposits=[];lmEngine.reindex();return result},
  guards:()=>{towers.length=0;dwarves.length=0;enemies.length=0;spawners.length=0;for(const role of ['miner','soldier','farmer','hauler']){const d=makeDwarf();d.weapon=mkItemWeapon('common');if(role==='miner')d.pickaxe=mkItemPickaxe('common');dwarves.push(d);lmEngine.setProfession(d.id,role);applyDwarfStats(d)}addTower('guard',fortress.x+3,fortress.y);const wasPaused=paused;paused=false;tickGuardMaintenance(1);paused=wasPaused;const assignments=dwarves.filter(d=>d.guardPostId).map(d=>d.living.profession);const miner=dwarves[0],x=fortress.x+7,y=fortress.y+7;for(let yy=fortress.y;yy<=y;yy++)for(let xx=fortress.x;xx<=x;xx++){tType[idx(xx,yy)]=TILE_EMPTY;tSeen[idx(xx,yy)]=1}tType[idx(x,y)]=TILE_ROCK;tHp[idx(x,y)]=20;window.dispatchEvent(new CustomEvent('dwarfworld:terrain',{detail:{}}));lmEngine.digArea(x,y,1,1);lmEngine.begin(1/30);updateDwarf(miner,1/30);return{assignments,minerJob:miner.living.job?.kind,minerStillGuard:!!miner.guardPostId,civilianWeapons:dwarves.slice(2).every(d=>!!d.weapon)}},
  passives:()=>{const d=dwarves[0],x=fortress.x+12,y=fortress.y+12;towers.length=0;enemies.length=0;spawners.length=0;dwx.buildings=[];for(const type of ['infirmary','tavern','barracks'])dwx.buildings.push({id:'passive-'+type,type,x:x+4,y,level:1,hp:100,maxHp:100,stats:{serviced:0,goldReturned:0}});for(let yy=y-2;yy<=y+2;yy++)for(let xx=x-2;xx<=x+2;xx++){tType[idx(xx,yy)]=TILE_EMPTY;tSeen[idx(xx,yy)]=1}d.x=x;d.y=y;d.level=1;d.baseMaxHp=100;d.weapon=null;d.pickaxe=null;d.armor={...mkItemArmor('common'),bonusHp:0,defense:0,effect:'regen'};lmEngine.setProfession(d.id,'farmer');d.living.cargo=null;d.living.nextThink=0;const r=lmEngine.designate('garden',x-1,y-1,3,3).room;r.active=true;r.progress=1;lmEngine.reindex();applyDwarfStats(d);d.hp=50;d.think=5;d.atkTimer=5;d._speedBoostLeft=5;updateDwarf(d,.1);const managed={healing:d.hp-50,think:d.think,attack:d.atkTimer,boost:d._speedBoostLeft,job:d.living.job.kind,services:dwx.buildings.filter(b=>b.type!=='tavern').map(b=>b.stats.serviced)};lmEngine.setProfession(d.id,'miner');d.living.nextThink=0;d.hp=50;d.think=5;d.atkTimer=5;d._speedBoostLeft=5;lm.designations=[];updateDwarf(d,.1);return{managed,legacy:{healing:d.hp-50,think:d.think,attack:d.atkTimer,boost:d._speedBoostLeft,services:dwx.buildings.filter(b=>b.type!=='tavern').map(b=>b.stats.serviced)}}},
  teleportCargo:()=>{dwx.buildings=[];const d=dwarves[0],x=fortress.x+20,y=fortress.y+20;tType[idx(x,y)]=TILE_EMPTY;tSeen[idx(x,y)]=1;d.x=x;d.y=y;d.armor=dwxMakeRelic(DWX_RELICS.find(r=>r.id==='voidhook'));d.dwxVoidhookCd=0;lmEngine.setProfession(d.id,'farmer');d.living.cargo={resource:'ore',amount:5};d.hp=1;const before=lm.deposits.filter(p=>p.x===x&&p.y===y).reduce((n,p)=>n+p.inventory.ore,0);updateDwarf(d,1/30);return{teleported:Math.hypot(d.x-fortress.x,d.y-fortress.y)<2,onSite:lm.deposits.filter(p=>p.x===x&&p.y===y).reduce((n,p)=>n+p.inventory.ore,0)-before,cargo:d.living.cargo}},
  miningProgression:()=>{const d=dwarves[0],x=fortress.x+12,y=fortress.y+12;d.x=x;d.y=y;d.level=2;d.xp=0;d.nextXp=1;d.baseMaxHp=100;d.weapon=null;d.armor=null;d.pickaxe=mkItemPickaxe('common');applyDwarfStats(d);d.hp=d.maxHp;lmEngine.setProfession(d.id,'miner');d.living.nextThink=0;lm.designations=[];tType[idx(x,y)]=TILE_EMPTY;tType[idx(x+1,y)]=TILE_ROCK;tHp[idx(x+1,y)]=1;tSeen[idx(x+1,y)]=1;lmEngine.digArea(x+1,y,1,1);window.dispatchEvent(new CustomEvent('dwarfworld:terrain',{detail:{x:x+1,y}}));updateDwarf(d,1);return{level:d.level,perkPending:pendingPerkDwarfId===d.id,perkVisible:UI.perkModal.style.display==='flex',excavated:tType[idx(x+1,y)]===TILE_EMPTY}},
  blockingSight:()=>{const x=fortress.x+20,y=fortress.y;for(let dx=0;dx<5;dx++){tType[idx(x+dx,y)]=TILE_EMPTY;tSeen[idx(x+dx,y)]=1}tType[idx(x+2,y)]=TILE_ROCK;const blocked=lmSight(x,y,x+4,y);tType[idx(x+2,y)]=TILE_EMPTY;const clear=lmSight(x,y,x+4,y);return{blocked,clear}},
  society:()=>{const s=makeSpawner(fortress.x+7,fortress.y+7);spawners.push(s);const camp=lmCamp(s);for(let i=0;i<30;i++)lmSpawnCamp(camp,s);return{count:enemies.filter(e=>e.lmCamp===camp.id).length,all:enemies.length,origins:enemies.filter(e=>e.lmCamp===camp.id).every(e=>e.lmHome?.x===s.x)}}
};
`;
const browser=await chromium.launch({channel:process.env.PLAYWRIGHT_CHANNEL||'chrome',headless:true});
try{
  const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',async route=>{
    const url=new URL(route.request().url());if(url.hostname!=='dwarfworld.test')return route.abort();
    const file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep))return route.abort();
    try{let body=await fs.readFile(file);if(file.endsWith('DwarfWorld-core.html')){let text=body.toString();const at=text.lastIndexOf('})();');text=text.slice(0,at)+bridge+text.slice(at);body=Buffer.from(text)}const ext=path.extname(file);await route.fulfill({status:200,contentType:ext==='.html'?'text/html':ext==='.js'?'text/javascript':ext==='.json'?'application/json':'text/plain',body});}catch{await route.fulfill({status:404,body:'not found'})}
  });
  await page.goto('https://dwarfworld.test/game/DwarfWorld.html');
  await page.getByRole('button',{name:/Start New World/}).click();
  await page.frameLocator('#gameFrame').locator('#lmDock').waitFor({timeout:30000});
  const game=page.frames().find(f=>f!==page.mainFrame());await game.evaluate(()=>__livingQA.pause());await game.evaluate(()=>__livingQA.configure());
  const before=await game.evaluate(()=>__livingQA.snapshot());
  for(const compressed of [true,false]){assert.equal(await game.evaluate(c=>__livingQA.legacy(c),compressed),true);assert.deepEqual(await game.evaluate(()=>__livingQA.snapshot()),before,'legacy progress/equipment/buildings remain intact')}
  const id=await game.evaluate(()=>__livingQA.cargo());assert.equal(await game.evaluate(()=>DwarfWorldLiving.save()),true);assert.equal(await game.evaluate(()=>DwarfWorldLiving.load()),true);assert.deepEqual(await game.evaluate(id=>__livingQA.checkCargo(id),id),{resource:'ore',amount:5});
  assert.deepEqual(await game.evaluate(()=>__livingQA.quota()),{result:false,preserved:true});
  assert.deepEqual(await game.evaluate(()=>__livingQA.future()),{result:false,preserved:true});
  assert.deepEqual(await game.evaluate(()=>__livingQA.malformed()),{result:false,preserved:true});
  const malformedCases=['living primitive','job missing goal','job bad coordinates','job unknown type','job bad reference','cargo unknown material','cargo negative amount','production bad ingredients','production wrong workplace','stockpile missing inventory','caravan missing goal','faction null','survey null','item invalid name'];
  for(const kind of malformedCases)assert.deepEqual(await game.evaluate(k=>__livingQA.malformedNested(k),kind),{result:false,preserved:true},kind+' rejected before any core or expansion mutation');
  assert.deepEqual(await game.evaluate(()=>__livingQA.capacity()),{held:true,saved:true,loaded:true,removed:true,ore:5});
  assert.deepEqual(await game.evaluate(()=>__livingQA.guards()),{assignments:['miner','soldier'],minerJob:'dig',minerStillGuard:true,civilianWeapons:true});
  const passives=await game.evaluate(()=>__livingQA.passives());
  for(const state of [passives.managed,passives.legacy]){assert.ok(Math.abs(state.healing-1.39)<1e-8,'armor, infirmary and tavern healing apply exactly once');assert.equal(state.think,4.9);assert.equal(state.attack,4.9);assert.equal(state.boost,4.9)}
  assert.equal(passives.managed.job,'produce');assert.deepEqual(passives.managed.services,[1,1]);assert.deepEqual(passives.legacy.services,[2,2]);
  assert.deepEqual(await game.evaluate(()=>__livingQA.teleportCargo()),{teleported:true,onSite:5,cargo:null});
  assert.deepEqual(await game.evaluate(()=>__livingQA.miningProgression()),{level:3,perkPending:true,perkVisible:true,excavated:true});
  assert.deepEqual(await game.evaluate(()=>__livingQA.blockingSight()),{blocked:false,clear:true});
  const camp=await game.evaluate(()=>__livingQA.society());assert.ok(camp.count<=19);assert.ok(camp.all<=240);assert.equal(camp.origins,true);
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({passed:['compressed legacy migration','plain legacy migration','relics,perks,progression,buildings preserved','cargo roundtrip','quota preserves previous save','future save rejected before mutation','malformed collections rejected before mutation',...malformedCases.map(k=>k+' rejected before mutation'),'guards respect professions while miners retain designated jobs','managed and legacy passive effects apply exactly once','teleport relic leaves physical cargo at its origin','designated mining levels and offers earned perks','wall blocks sight','capped enemies retain camp origins'],camp},null,2));
}finally{await browser.close()}
