import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('hosted-art-render.js',import.meta.url),'utf8');
const catalogSource=readFileSync(new URL('hosted-art-catalog.js',import.meta.url),'utf8');
const fixedBosses=[
  ['legacy-0','The Stonebell Colossus','boss_legacy-0'],
  ['legacy-1','Morrow, Bone Regent','boss_legacy-1'],
  ['legacy-2','Vaelith the Cinder Sky','boss_legacy-2'],
  ['legacy-3','The Watching Dark','boss_legacy-3'],
  ['expansion-0','The Gilded Executioner','boss_expansion-0'],
  ['expansion-1','Myr, Stormbound Oracle','boss_expansion-1'],
  ['verdant-boss1','Briar Queen','boss_verdant-boss1'],
  ['verdant-boss2','Stonebell Colossus','boss_legacy-0'],
  ['verdant-boss3','Mire Sovereign','boss_verdant-boss3'],
  ['verdant-boss4','Ancient Wyrm','boss_verdant-boss4'],
  ['verdant-ruler','The Thornheart Sovereign','boss_verdant-ruler'],
  ['meridian-boss1','Storm Knight Regent','boss_meridian-boss1'],
  ['meridian-boss2','Rimeglass Matriarch','boss_meridian-boss2'],
  ['meridian-boss3','Vaelith the Cinder Sky','boss_legacy-2'],
  ['meridian-boss4','Shard Crown Titan','boss_meridian-boss4'],
  ['meridian-ruler','Vaelith, Tempest Dragon','boss_meridian-ruler'],
  ['gloam-boss1','Morrow, Bone Regent','boss_legacy-1'],
  ['gloam-boss2','Crimson Matriarch','boss_gloam-boss2'],
  ['gloam-boss3','Abyssal Executioner','boss_gloam-boss3'],
  ['gloam-boss4','The Last Sun Warden','boss_gloam-boss4'],
  ['gloam-ruler','The Watching Dark','boss_legacy-3'],
  ['verdant-dungeon:4','Oldroot Caverns Guardian','boss_verdant-dungeon-4'],
  ['meridian-dungeon:4','The Crystal Vault Guardian','boss_meridian-dungeon-4'],
  ['gloam-dungeon:4','The Sunken Observatory Guardian','boss_gloam-dungeon-4'],
  ['frostTroll','Ancient Frost Troll','boss_frostTroll'],
  ['wyvern','Crimson Wyvern','boss_wyvern'],
  ['colossus','Arcane Colossus','boss_colossus'],
  ['roc','Storm Roc','boss_roc'],
  ['sandWorm','Elder Sand Worm','boss_sandWorm'],
  ['guardian','Forest Guardian','boss_guardian']
];
function harness({withDeaths=false,withRequest=false}={}){
  const calls={player:0,enemy:0,aura:0,weapon:0,event:0,front:0,shadow:[],draw:[],ensure:[],request:[],canvas:[]},ready=new Set(),poses=new WeakMap();
  const ctx=new Proxy({globalAlpha:1},{get(target,key){if(key in target)return target[key];return (...args)=>calls.canvas.push({key,args});},set(target,key,value){target[key]=value;return true;}});
  const p={x:4,y:5,hp:100,shield:0,armorGear:{name:'Wayfarer Cloth'},weapon:{rarity:'Common'},facing:{x:1,y:0}};
  const context={console,Math,Map,Set,WeakMap,Number,Array,elapsed:10,game:{player:p,roomData:{}},ctx,
    worldToScreen:(x,y)=>({x:x*10,y:y*10}),shadowAt:(...args)=>calls.shadow.push(args),drawAura:()=>calls.aura++,drawWeaponModel:()=>calls.weapon++,drawEnemyBody:()=>calls.enemy++};
  context.window={AWPresentation:{camera:{zoom:.9},entities:{player:()=>calls.player++},pose(entity){if(!poses.has(entity))poses.set(entity,{state:'idle',speed:0,recoilX:0,recoilY:0,until:0});return poses.get(entity);}},
    AWArt:{get:id=>ready.has(id)?{}:null,ensure(id){calls.ensure.push(id);return Promise.resolve(null);},draw(id,canvas,x,y,options){calls.draw.push({id,x,y,...options});return ready.has(id);}}};
  if(withDeaths){
    const pool={items:[{life:0},{life:0}],cursor:0,add(data){const item=this.items.find(item=>item.life<=0)||this.items[this.cursor++%this.items.length];for(const key of Object.keys(item))delete item[key];Object.assign(item,data,{maxLife:data.life});return item;},clear(){for(const item of this.items)item.life=0;}};
    context.window.AWPresentation.fx={deaths:pool,drawFront(){calls.front++;}};
    context.window.AWPresentation.event=function(kind,data){calls.event++;if(kind==='death')pool.add({x:data.entity.x,y:data.entity.y,life:.55,color:data.entity.color});};
  }
  if(withRequest)context.window.AWArt.request=id=>{calls.request.push(id);return Promise.resolve(null);};
  vm.createContext(context);vm.runInContext(catalogSource,context);vm.runInContext(source,context);
  return {context,calls,ready,poses,p,renderer:context.window.AWArtRenderer,player:entity=>context.window.AWPresentation.entities.player(entity),enemy:entity=>context.drawEnemyBody(entity)};
}

test('armor appearances preserve base names through affixes and cover regional equipment',()=>{
  const {renderer,p}=harness();
  assert.equal(renderer.playerAssetId(p),'armor-wayfarer-cloth');
  for(const [name,id] of [['Emberguard','emberguard'],['Bone Regent','bone-regent'],['Mirrorsteel Breastplate','crystal-vindicator'],['Beastmaster Cloak','wildheart-hunter'],['Stormguard Mantle','stormcaller'],['Riftwalker Boots','riftwarden'],['Cinderplate','cinder-sovereign'],['Starbound Vestments','astral-arbiter'],['Voidheart Armor','eclipse-sovereign']]){
    assert.equal(renderer.armorId({baseName:name,name:`Stalwart ${name} of Warding`}),`armor-${id}`,name);
  }
  assert.equal(renderer.armorId({name:'Unknown future armor'}),'armor-wayfarer-cloth');
});

test('all canonical registered creatures and combat props use their own sheets',()=>{
  const {renderer,context}=harness();
  const creatures=Object.values(context.window.AWArtCatalog.entries).filter(entry=>entry.category==='enemy'&&!entry.boss&&entry.kind!=='boss');
  assert.equal(creatures.length,124);
  for(const entry of creatures){
    assert.equal(renderer.enemyAssetId({type:entry.id}),entry.id,entry.id);
    assert.equal(renderer.enemyAssetId({type:entry.id,elite:true,name:'Vampiric / Giant '+entry.name}),entry.id,entry.id+' variants');
  }
  assert.equal(renderer.enemyAssetId({type:'armor-emberguard'}),null);
  assert.equal(renderer.enemyAssetId({type:'boss_legacy-0'}),null);
  assert.equal(renderer.enemyAssetId({type:'lw_future_creature'}),null);
  assert.equal(renderer.enemyAssetId(null),null);
});

test('all 30 fixed encounters resolve to their 26 dedicated boss designs by identity and exact name',()=>{
  const {renderer}=harness();
  assert.equal(new Set(fixedBosses.map(([, ,id])=>id)).size,26);
  for(const [encounter,name,id] of fixedBosses){
    assert.equal(renderer.enemyAssetId({boss:true,artBossId:encounter}),id,encounter);
    assert.equal(renderer.enemyAssetId({boss:true,bossId:encounter}),id,encounter+' explicit boss id');
    assert.equal(renderer.enemyAssetId({boss:true,artBossId:id}),id,encounter+' canonical art id');
    assert.equal(renderer.enemyAssetId({boss:true,name}),id,name);
    assert.equal(renderer.enemyAssetId({boss:true,name:'  '+name.toUpperCase()+'  '}),id,name+' normalized case');
    assert.equal(renderer.enemyAssetId({boss:true,name:name.replace(/^The /,'')}),id,name+' optional article');
    assert.equal(renderer.enemyAssetId({boss:true,name:'The '+name.replace(/^The /,'')}),id,name+' optional leading article');
  }
});

test('campaign bosses, dungeon guardians and legendary dangers preserve encounter identity',()=>{
  const {renderer}=harness();
  assert.equal(renderer.enemyAssetId({boss:true,type:'drake'},{campaignNode:'meridian-ruler'}),'boss_meridian-ruler');
  for(const [encounter,name,id] of fixedBosses){
    if(/^(verdant|meridian|gloam)-/.test(encounter)){
      const [campaignNode,roomIndex]=encounter.split(':');
      assert.equal(renderer.enemyAssetId({boss:true},{campaignNode,campaignRoom:Number(roomIndex)||0}),id,encounter);
      assert.equal(renderer.enemyAssetId({boss:true,name},{campaignNode,campaignRoom:Number(roomIndex)||0}),id,encounter+' named');
    }else if(['frostTroll','wyvern','colossus','roc','sandWorm','guardian'].includes(encounter)){
      assert.equal(renderer.enemyAssetId({boss:true,dangerLegend:encounter,name:'Changed encounter label'},{campaignNode:'meridian-ruler'}),id,encounter);
    }
  }
  assert.equal(renderer.enemyAssetId({boss:true,name:'The Stonebell Colossus'},{campaignNode:'meridian-ruler'}),'boss_legacy-0','the enemy identity wins over its arena');
});

test('unknown bosses stay procedural and shadow bosses use their own creature appearance',()=>{
  const {renderer,context}=harness();
  for(const name of ['Unrelated boss','Elite The Watching Dark','Watching Dark Minion','Thornheart Sovereign II']){
    assert.equal(renderer.enemyAssetId({boss:true,type:'lw_lantern_wasp',name},{campaignNode:'verdant-ruler'}),null,name);
  }
  assert.equal(renderer.enemyAssetId({boss:true,type:'wolf'},{}),null,'ordinary base cannot identify a new boss');
  for(const id of context.window.AWArtCatalog.regions.shadow.filter(id=>context.window.AWArtCatalog.entries[id]?.category==='enemy')){
    assert.equal(renderer.enemyAssetId({boss:true,shadow:true,type:id,name:'Vampiric Legendary Realm Boss'},{campaignNode:'gloam-ruler'}),id,id);
  }
  assert.equal(renderer.enemyAssetId({boss:true,shadow:true,type:'missing-shadow'}),null);
});

test('world directions select four authored facings with finite defaults',()=>{
  const {renderer}=harness();
  for(const [direction,expected] of [[{x:1,y:0},'SE'],[{x:0,y:1},'SW'],[{x:-1,y:0},'NW'],[{x:0,y:-1},'NE'],[{x:1,y:1},'SE'],[{x:0,y:0},'SE'],[{x:NaN,y:1},'SE'],[null,'SE']])assert.equal(renderer.facing(direction),expected);
  assert.equal(renderer.facing({facing:{x:0,y:-1}}),'NE');
});

test('state selection prioritizes death and telegraphs over locomotion',()=>{
  const {renderer}=harness();
  assert.equal(renderer.state({hp:0},{state:'attack'}),'death');
  assert.equal(renderer.state({dead:true},{state:'run'}),'death');
  assert.equal(renderer.state({hp:10,telegraph:{}},{state:'run',speed:1}),'attack');
  assert.equal(renderer.state({hp:10},{state:'heavyCast'}),'attack');
  assert.equal(renderer.state({hp:10},{state:'dodge'}),'move');
  assert.equal(renderer.state({hp:10},{state:'idle',speed:.4}),'move');
  assert.equal(renderer.state({hp:10},{state:'heal',speed:0}),'idle');
  assert.equal(renderer.state(null),'idle');
});

test('missing art keeps procedural bodies and requests equipped armor once per change',()=>{
  const h=harness(),enemy={type:'lw_spore_hound',hp:10};
  h.player(h.p);h.player(h.p);h.enemy(enemy);
  assert.equal(h.calls.player,2);assert.equal(h.calls.enemy,1);assert.equal(h.calls.draw.length,0);
  assert.deepEqual(h.calls.ensure,['armor-wayfarer-cloth','lw_spore_hound']);
  h.p.armorGear={baseName:'Frostbound'};h.player(h.p);
  assert.deepEqual(h.calls.ensure,['armor-wayfarer-cloth','lw_spore_hound','armor-frostbound']);
  assert.equal(h.calls.player,3);
});

test('ready armor uses ground pivot, camera scale and live shield/aura without a second weapon',()=>{
  const h=harness();h.ready.add('armor-wayfarer-cloth');h.p.shield=30;h.p.invuln=1;h.player(h.p);
  assert.equal(h.calls.player,0);assert.equal(h.calls.weapon,0);assert.equal(h.calls.aura,1);assert.equal(h.calls.shadow.length,1);
  assert.equal(h.calls.draw[0].x,40);assert.equal(h.calls.draw[0].y,50);assert.equal(h.calls.draw[0].scale,.44*.9);
  assert.ok(h.calls.draw[0].alpha>=.72&&h.calls.draw[0].alpha<=1);
  assert.ok(h.calls.canvas.some(call=>call.key==='ellipse'),'shield stays visible');
});

test('enemy body replacement preserves enclosing context and keeps hidden/unknown bodies procedural',()=>{
  const h=harness(),enemy={type:'lw_spore_hound',hp:10,facing:{x:0,y:1}};h.ready.add(enemy.type);h.enemy(enemy);
  assert.equal(h.calls.enemy,0);assert.equal(h.calls.draw[0].id,enemy.type);assert.equal(h.calls.draw[0].facing,'SW');
  assert.equal(h.calls.draw[0].x,0);assert.equal(h.calls.draw[0].y,12);assert.equal(h.calls.draw[0].scale,.34);
  assert.deepEqual(h.calls.canvas.map(call=>call.key),['save','restore']);
  h.enemy({...enemy,hidden:true});h.enemy({type:'missing-future',hp:20});assert.equal(h.calls.enemy,2);
});

test('late waves and summoned creatures request once, keep fallback and switch when ready',()=>{
  const h=harness({withRequest:true}),initial={type:'wolf',hp:10},summon={type:'skeleton',hp:10};
  h.enemy(initial);h.enemy(initial);h.enemy(summon);h.enemy(summon);
  assert.deepEqual(h.calls.request,['wolf','skeleton']);
  assert.equal(h.calls.ensure.length,0,'the room-pinning request API is preferred');
  assert.equal(h.calls.enemy,4,'loading keeps procedural bodies');
  h.ready.add('skeleton');h.enemy(summon);
  assert.equal(h.calls.enemy,4);assert.equal(h.calls.draw.at(-1).id,'skeleton');
  h.enemy({...summon,type:'unknown-added-creature'});
  assert.deepEqual(h.calls.request,['wolf','skeleton'],'unknown assets do not create load attempts');
});

test('enemy retry follows loader cooldown and revisiting a room requests evicted sheets again',()=>{
  const h=harness({withRequest:true}),enemy={type:'skeleton',hp:10};let retry=false;
  h.context.window.AWArt.shouldRetry=()=>retry;
  h.context.window.AWArt.request=id=>{h.calls.request.push(id);retry=false;return Promise.resolve(null);};
  h.enemy(enemy);h.enemy(enemy);assert.equal(h.calls.request.length,1);
  retry=true;h.enemy(enemy);h.enemy(enemy);assert.equal(h.calls.request.length,2);
  const firstRoom=h.context.game.roomData;
  h.context.game.roomData={key:'other-room'};h.enemy({type:'wolf',hp:10});
  h.context.game.roomData=firstRoom;h.enemy(enemy);h.enemy(enemy);
  assert.deepEqual(h.calls.request,['skeleton','skeleton','wolf','skeleton']);
  assert.equal(h.calls.enemy,7,'procedural fallback remains available after eviction or failed loads');
});

test('room load generation reloads late summon sheets after visiting an empty town',()=>{
  const h=harness({withRequest:true}),enemy={type:'skeleton',hp:10};let roomVersion=1;
  h.context.window.AWArt.roomVersion=()=>roomVersion;
  h.enemy(enemy);h.enemy(enemy);assert.deepEqual(h.calls.request,['skeleton']);
  // The renderer sees no enemy body in the town, and the original room object
  // is reused on return. The loader's generation still invalidates its request set.
  const combatRoom=h.context.game.roomData;
  h.context.game.roomData={town:true};roomVersion++;
  h.context.game.roomData=combatRoom;roomVersion++;
  h.enemy(enemy);h.enemy(enemy);assert.deepEqual(h.calls.request,['skeleton','skeleton']);
});

test('animation clocks use simulation time, reset consecutive attacks and restart after a new journey',()=>{
  const h=harness();h.ready.add('armor-wayfarer-cloth');const pose=h.context.window.AWPresentation.pose(h.p);pose.state='attack';pose.until=100;
  h.player(h.p);assert.equal(h.calls.draw.at(-1).time,0);
  h.context.elapsed=10.1;h.player(h.p);assert.ok(Math.abs(h.calls.draw.at(-1).time-.25)<1e-9);
  h.player(h.p);assert.ok(Math.abs(h.calls.draw.at(-1).time-.25)<1e-9,'paused simulation does not advance animation');
  pose.until=200;h.player(h.p);assert.equal(h.calls.draw.at(-1).time,0,'next attack restarts');
  h.context.elapsed=0;h.player(h.p);assert.equal(h.calls.draw.at(-1).time,0,'new journey resets elapsed time');
});

test('failed current armor retries only when the loader cooldown allows it',()=>{
  const h=harness();let retry=false;
  h.context.window.AWArt.shouldRetry=()=>retry;
  h.context.window.AWArt.ensure=id=>{h.calls.ensure.push(id);retry=false;return Promise.resolve(null);};
  h.player(h.p);h.player(h.p);assert.equal(h.calls.ensure.length,1);
  retry=true;h.player(h.p);h.player(h.p);assert.equal(h.calls.ensure.length,2);
  assert.equal(h.calls.player,4,'fallback remains available during retry');
});

test('death artwork survives enemy removal through the existing bounded presentation pool',()=>{
  const h=harness({withDeaths:true}),P=h.context.window.AWPresentation,e={type:'lw_spore_hound',x:3,y:7,r:.39,facing:{x:-1,y:0}};
  h.ready.add(e.type);P.event('death',{entity:e});
  const corpse=P.fx.deaths.items.find(item=>item.life>0);assert.equal(corpse.hostedDeath.id,e.type);
  e.x=99;e.type='wolf';e.facing={x:1,y:0};corpse.life=.15;P.fx.drawFront();
  const draw=h.calls.draw.at(-1);assert.equal(draw.id,'lw_spore_hound');assert.equal(draw.state,'death');assert.equal(draw.facing,'NW');assert.equal(draw.x,30);assert.equal(draw.y,70);assert.equal(draw.scale,.39);assert.ok(Math.abs(draw.time-.4)<1e-9);assert.ok(draw.alpha<1);
  assert.equal(h.calls.front,1,'original death particles still render');assert.equal(h.calls.event,1);
  P.fx.deaths.clear();const before=h.calls.draw.length;P.fx.drawFront();assert.equal(h.calls.draw.length,before,'room reset removes corpse art');
});

test('death pool recycling clears sprite snapshots and absent art preserves procedural deaths',()=>{
  const h=harness({withDeaths:true}),P=h.context.window.AWPresentation,pool=P.fx.deaths,e={type:'lw_spore_hound',x:2,y:2,r:.39};h.ready.add(e.type);
  P.event('death',{entity:e});P.event('death',{entity:e});P.event('death',{entity:{type:'wolf',x:1,y:1}});
  assert.equal(pool.items.length,2);assert.equal(pool.items[0].hostedDeath,undefined,'a recycled procedural corpse cannot retain another enemy sheet');
  pool.clear();P.event('death',{entity:{type:'lw_lantern_wasp',x:1,y:1}});assert.equal(pool.items[0].hostedDeath,undefined,'unavailable art leaves its procedural death');
});

