const test=require('node:test');
const assert=require('node:assert/strict');
const S=require('../survival.js');
const E=require('../engineering.js');

function fixture(){
  const width=110,height=84,tile=24,T={AIR:0,GRASS:1,DIRT:2,STONE:3,WOOD:4,COPPER:5,IRON:6,CRYSTAL:7,OBSIDIAN:8,PLATFORM:9,COPPER_BLOCK:10,IRON_BLOCK:11,WIRE:12,SIGNAL:13,GENERATOR:14,SWITCH:15,SENSOR:16,MOTOR:17,SHAFT:18,GEAR:19,PISTON:20,LAMP:21,CONVEYOR:22,CORE:25,LADDER:26,STORAGE:27};
  const defs={0:{solid:false},1:{solid:true},2:{solid:true},3:{solid:true},4:{solid:true},5:{solid:true},6:{solid:true},7:{solid:true},8:{solid:true},9:{solid:true,platform:true,drop:'platform',strength:3},10:{solid:true,drop:'copperBlock',strength:5},11:{solid:true,drop:'ironBlock',strength:10},12:{solid:false,electric:true,drop:'wire'},13:{solid:false,signal:true,drop:'signal'},14:{solid:true,electric:true,source:12,drop:'generator',strength:8},17:{solid:true,electric:true,mechanical:true,drop:'motor',strength:8},20:{solid:true,mechanical:true,drop:'piston',strength:8},21:{solid:false,electric:true,drop:'lamp'},22:{solid:true,mechanical:true,drop:'conveyor'},25:{solid:true,electric:true},26:{solid:false,ladder:true,drop:'ladder'},27:{solid:true,transport:true,drop:'storage',strength:8}};
  defs[T.GENERATOR].ports=E.DIRS.map((_,side)=>({side,type:'power',mode:'out'}));
  defs[T.WIRE].ports=E.DIRS.map((_,side)=>({side,type:'power',mode:'both'}));
  defs[T.MOTOR].load=5;defs[T.MOTOR].ports=[{side:2,type:'power',mode:'in'}];
  const world=new Uint8Array(width*height),meta=new Map();for(let y=30;y<height;y++)for(let x=0;x<width;x++)world[x+y*width]=T.STONE;
  const get=(x,y)=>x>=0&&y>=0&&x<width&&y<height?world[x+y*width]:T.STONE;
  let engine;const set=(x,y,id,m)=>{world[x+y*width]=id;meta.delete(x+y*width);if(m)meta.set(x+y*width,{...m});engine?.changed(x,y);};
  engine=new E.Simulation({width,height,T,defs,get,meta:()=>meta});engine.metrics={mined:0,produced:0,shots:0,healed:0};
  const player={x:60*tile,y:30*tile-34,w:15,h:34,health:100,facing:1,vx:0,vy:0},inventory={copper:10,iron:10},stats={},messages=[];
  let randomSeed=41;const random=()=>((randomSeed=(randomSeed*1664525+1013904223)>>>0)/4294967296);
  const survival=new S.Survival({width,height,tile,T,defs,get,set,meta:()=>meta,engine,player,inventory,stats,notify:m=>messages.push(m),random});
  survival.checkpoint={x:9*tile,y:30*tile-34};
  function advance(seconds){for(let i=0;i<Math.ceil(seconds/.05);i++){engine.elapsed+=.05;survival.tick(.05);}}
  function enemy(type,dx=18,options={}){const d=S.ENEMIES[type],x=player.x+player.w/2+dx,y=30*tile-d.h/2-1;return survival.spawn(type,x,y,{ignoreDistance:true,...options});}
  return{survival,player,inventory,stats,engine,world,meta,T,defs,width,height,tile,get,set,advance,enemy,messages};
}

test('equipment catalog preserves tiles and uses connected manufactured steel',()=>{
  const f=fixture(),items=[],recipes=[],before=JSON.stringify(f.T);S.install(f.T,f.defs,items,recipes);S.install(f.T,f.defs,items,recipes);
  assert.equal(before,JSON.stringify(f.T));assert.equal(new Set(recipes.map(r=>r.key)).size,recipes.length);
  assert.equal(recipes.find(r=>r.key==='steelDrillTool').cost.steel,4);assert.equal(recipes.find(r=>r.key==='ammo').out,6);
  assert.ok(recipes.every(r=>!r.cost.alloy));assert.equal(Object.keys(S.ENEMIES).length,9);
});

test('spawn manager rejects player, terrain, machinery, unsafe outpost and crowded positions',()=>{
  const f=fixture(),s=f.survival,p=s.center(),y=30*f.tile-S.ENEMIES.beetle.h/2-1;
  assert.equal(s.spawn('beetle',p.x,p.y),null);
  assert.equal(s.spawn('beetle',80*f.tile,34*f.tile),null);
  assert.equal(s.spawn('beetle',15*f.tile,y),null);
  f.set(76,29,f.T.WIRE,{powered:true});s.scanWorld();assert.equal(s.spawn('beetle',76.5*f.tile,y),null);
  const e=s.spawn('beetle',82.5*f.tile,y);assert.ok(e);assert.equal(s.spawn('beetle',82.5*f.tile,y),null);
  assert.equal(s.blocked(e.x,e.y,e.w,e.h,false,true),false);
});

test('normal gameplay spawns real enemies after the difficulty grace period',()=>{
  const f=fixture();f.advance(109);assert.equal(f.survival.enemies.length,0);f.advance(17);
  assert.ok(f.survival.enemies.length>0);assert.ok(f.survival.enemies.every(e=>!f.survival.blocked(e.x,e.y,e.w,e.h)));
  assert.equal(f.survival.setDifficulty('invalid'),false);assert.equal(f.survival.setDifficulty('Casual'),true);assert.equal(f.survival.config().grace,180);
});

test('melee enemies telegraph, then strike, and damage does not tick continuously',()=>{
  const f=fixture(),e=f.enemy('beetle');assert.ok(e);e.cool=0;f.survival.invulnerable=0;
  f.advance(.05);assert.ok(e.wind>0);assert.equal(f.player.health,100);
  f.advance(.4);assert.equal(f.player.health,100);f.advance(.4);assert.ok(f.player.health<100);
  const health=f.player.health;f.advance(.3);assert.equal(f.player.health,health);
});

test('ground and flying movement respects solid walls; burrowers alone cross suitable terrain',()=>{
  const f=fixture(),s=f.survival;for(let y=19;y<30;y++)f.set(62,y,f.T.STONE);
  const crawler=s.spawn('crawler',64.5*f.tile,30*f.tile-S.ENEMIES.crawler.h/2-1,{ignoreDistance:true});
  const wisp=s.spawn('wisp',66.5*f.tile,25*f.tile,{ignoreDistance:true});
  assert.ok(crawler&&wisp);f.advance(6);assert.ok(crawler.x>=63*f.tile);assert.equal(s.blocked(crawler.x,crawler.y,crawler.w,crawler.h),false);assert.equal(s.blocked(wisp.x,wisp.y,wisp.w,wisp.h),false);
  const d=S.ENEMIES.burrower,e={x:62.5*f.tile,y:28*f.tile,w:d.w,h:d.h,vx:1,vy:0};
  assert.equal(s.blocked(e.x,e.y,e.w,e.h),true);assert.equal(s.blocked(e.x,e.y,e.w,e.h,true),false);
  const before=Array.from(f.world);s.move(e,-12,0,true);assert.deepEqual(Array.from(f.world),before,'burrowing cannot create or destroy resources');
});

test('melee kills yield real recoverable loot once; defenses receive kill credit',()=>{
  const f=fixture(),e=f.enemy('beetle');f.inventory.hammer=1;f.survival.selectWeapon('hammer');
  assert.equal(f.survival.attack(e.x,e.y),true);assert.ok(e.hp<=0);assert.equal(f.stats.kills,1);
  assert.equal(f.engine.drops.find(d=>d.key==='scrap').count,2);assert.equal(f.engine.drops.find(d=>d.key==='copper').count,1);
  f.survival.hit(e,100,'turret');assert.equal(f.stats.kills,1);
  f.survival.enemies=[];const other=f.enemy('crawler');f.survival.hit(other,1000,'turret');assert.equal(f.stats.defenseKills,1);
});

test('ranged combat consumes real ammunition and energy; projectiles cannot pass through cover',()=>{
  const f=fixture(),s=f.survival;f.inventory.scrapLauncher=1;s.selectWeapon('scrapLauncher');
  assert.equal(s.attack(1000,700),false);f.inventory.ammo=2;assert.equal(s.attack(1000,700),true);assert.equal(f.inventory.ammo,1);
  assert.equal(s.attack(1000,700),false,'cooldown blocks extra ammunition consumption');assert.equal(f.inventory.ammo,1);
  f.set(62,29,f.T.STONE);s.projectiles=[{x:61.5*f.tile,y:29.5*f.tile,vx:460,vy:0,life:2,owner:'player',damage:100}];f.advance(.1);assert.equal(s.projectiles.length,0);
  f.inventory.energyRifle=1;s.selectWeapon('energyRifle');s.attackCooldown=0;s.energy=6;assert.equal(s.attack(1000,700),false);s.energy=50;assert.equal(s.attack(1000,700),true);assert.equal(s.energy,43);
});

test('crafted armor, shields, healing, and powered mining have measurable costs and benefits',()=>{
  const f=fixture(),s=f.survival;s.invulnerable=0;f.inventory.miningArmor=1;s.damagePlayer(20);assert.equal(f.player.health,84);
  s.invulnerable=0;f.inventory.shieldModule=1;s.energy=100;s.damagePlayer(20);assert.equal(f.player.health,76);assert.equal(s.energy,88);
  f.inventory.repairKit=1;assert.equal(s.heal(),true);assert.equal(f.player.health,100);assert.equal(f.inventory.repairKit,0);
  f.inventory.poweredMiningTool=1;assert.equal(s.miningMultiplier(f.T.COPPER),4);s.spendToolEnergy(1);assert.equal(s.energy,84);
  s.energy=0;assert.equal(s.miningMultiplier(f.T.COPPER),1);f.inventory.reinforcedPickaxe=1;assert.equal(s.miningMultiplier(f.T.COPPER),1.7);
});

test('death retains equipment and progression; all lost resources remain recoverable exactly once',()=>{
  const f=fixture(),s=f.survival;f.inventory.copper=100;f.inventory.hammer=1;f.stats.coreReached=true;s.invulnerable=0;
  s.damagePlayer(1000);assert.equal(f.player.health,100);assert.equal(f.inventory.hammer,1);assert.equal(f.stats.coreReached,true);assert.equal(f.inventory.copper,85);assert.equal(s.caches[0].items.copper,15);
  const cache=s.caches[0];f.player.x=cache.x-f.player.w/2;f.player.y=cache.y-f.player.h/2;f.advance(.05);assert.equal(s.caches.length,0);assert.equal(f.inventory.copper,100);f.advance(.05);assert.equal(f.inventory.copper,100);
  s.setDifficulty('casual');s.invulnerable=0;s.damagePlayer(1000);assert.equal(f.inventory.copper,100);assert.equal(s.caches.length,0);
});

test('machine destruction conserves component, input, and output inventories; overload cannot erase cargo',()=>{
  const f=fixture(),s=f.survival;f.set(65,29,f.T.STORAGE,{contents:{iron:4},outputs:{steel:3},hp:1});
  assert.equal(s.damageStructure(65,29,2),true);assert.equal(f.get(65,29),f.T.AIR);
  assert.equal(f.engine.drops.find(d=>d.key==='iron').count,4);assert.equal(f.engine.drops.find(d=>d.key==='steel').count,3);assert.equal(f.engine.drops.find(d=>d.key==='storage').count,1);
  for(let i=f.engine.drops.length;i<512;i++)f.engine.drops.push({id:i+1,key:'stone',count:1,x:1,y:1});
  f.set(66,29,f.T.STORAGE,{contents:{crystal:7},outputs:{steel:2},hp:1});s.damageStructure(66,29,2);
  assert.equal(f.get(66,29),f.T.STORAGE);assert.equal(f.meta.get(66+29*f.width).contents.crystal,7);assert.equal(f.meta.get(66+29*f.width).hp,1);
});

test('full loose-item budget stores enemy salvage in a recoverable cache',()=>{
  const f=fixture(),s=f.survival;for(let i=0;i<512;i++)f.engine.drops.push({id:i+1,key:'stone',count:1,x:1,y:1});
  const e=f.enemy('crawler');s.hit(e,500);assert.equal(s.caches.length,1);assert.equal(s.caches[0].items.scrap,3);assert.equal(s.caches[0].items.iron,1);
});

test('recovery caches and chapter rewards obey inventory capacity and retain the uncollected remainder',()=>{
  const f=fixture(),s=f.survival,p=s.center();s.inventoryCapacity=22;s.caches.push({id:1,x:p.x,y:p.y,items:{copper:7}});f.advance(.05);
  assert.equal(f.inventory.copper,12);assert.equal(s.caches[0].items.copper,5);f.inventory.iron-=5;f.advance(.05);assert.equal(f.inventory.copper,17);assert.equal(s.caches.length,0);
  s.grantReward('medkit',3);assert.equal(f.inventory.medkit||0,0);assert.equal(f.engine.drops.find(d=>d.key==='medkit').count,3);
});

test('circuit leeches travel to live machines and disrupt real power rather than draining an abstract base',()=>{
  const f=fixture(),s=f.survival;f.set(58,29,f.T.GENERATOR,{powered:true});s.scanWorld();
  const d=S.ENEMIES.leech,e=s.spawn('leech',55.5*f.tile,30*f.tile-d.h/2-1,{ignoreDistance:true});assert.ok(e);f.advance(4);
  const m=f.meta.get(58+29*f.width);assert.ok(m.leechUntil>f.engine.elapsed);assert.equal(m.leechDrain,5);assert.ok(m.hp<m.maxHp);
});

test('ranged enemies warn before launching visible projectiles that can hit the player',()=>{
  const f=fixture(),s=f.survival,e=f.enemy('drone',180);assert.ok(e);e.cool=0;s.invulnerable=0;f.advance(.1);assert.ok(e.wind>0);assert.equal(s.projectiles.length,0);assert.equal(f.player.health,100);
  f.advance(1);assert.ok(s.projectiles.some(p=>p.owner==='enemy'));f.advance(1.2);assert.ok(f.player.health<100);
});

test('base attacks warn before spawning, damage walls, and award survival only after enemies are defeated',()=>{
  const f=fixture(),s=f.survival;f.set(59,28,f.T.GENERATOR,{powered:true});f.set(58,28,f.T.LAMP,{powered:true});s.scanWorld();s.elapsed=300;s.nextRaid=299;
  s.tick(.05);assert.equal(s.raid.state,'warning');assert.ok(s.raid.remaining>23);assert.equal(s.enemies.length,0);
  f.advance(24.5);assert.equal(s.raid.state,'attack');assert.ok(s.enemies.some(e=>e.raid));
  assert.equal(f.stats.raidsSurvived||0,0);for(const e of s.enemies.filter(e=>e.raid))s.hit(e,1000,'turret');f.advance(.05);
  assert.equal(s.raid.state,'quiet');assert.equal(f.stats.raidsSurvived,1);
  f.set(67,29,f.T.IRON_BLOCK,{});s.damageStructure(67,29,15);assert.equal(f.get(67,29),f.T.IRON_BLOCK);assert.ok(f.meta.get(67+29*f.width).hp>0,'reinforced structures survive multiple attacks');
});

test('walls intercept siege attacks before nearby machinery can be damaged',()=>{
  const f=fixture(),s=f.survival,e=f.enemy('siege',100);assert.ok(e);f.set(62,29,f.T.GENERATOR,{powered:true});f.set(63,29,f.T.IRON_BLOCK,{});s.scanWorld();e.raid=1;e.cool=0;
  f.advance(4);assert.equal(f.meta.get(62+29*f.width).hp,undefined,'an intervening wall protects the generator');assert.ok(f.meta.get(63+29*f.width).hp<120,'siege attacks must strike the actual wall');
});

test('raids approach the powered base while the player explores far away',()=>{
  const f=fixture(),s=f.survival;f.set(9,29,f.T.GENERATOR,{powered:true});f.set(10,29,f.T.LAMP,{powered:true});s.scanWorld();s.elapsed=300;s.nextRaid=299;f.advance(25);
  assert.equal(s.raid.state,'attack');const attackers=s.enemies.filter(e=>e.raid);assert.ok(attackers.length);assert.ok(attackers.every(e=>e.x<42*f.tile),'raid spawn center is the base, rather than the distant player');
});

test('raiders attack the powered checkpoint base instead of getting stuck under nearer abandoned machines',()=>{
  const f=fixture(),s=f.survival;f.set(62,29,f.T.GENERATOR,{powered:true});f.set(54,24,f.T.PISTON,{disabled:true,ruin:true});s.checkpoint={x:63*f.tile,y:29*f.tile};s.scanWorld();
  const d=S.ENEMIES.siege,e=s.spawn('siege',50.5*f.tile,30*f.tile-d.h/2-1,{ignoreDistance:true,raid:1});assert.ok(e);f.advance(12);
  const base=f.meta.get(62+29*f.width),ruin=f.meta.get(54+24*f.width);assert.ok(base.hp<base.maxHp);assert.equal(ruin.hp,undefined);assert.ok(e.x>58*f.tile);
});

test('chapter rewards and technology follow real mining, machines, defenses, and production without repeat grants',()=>{
  const f=fixture(),s=f.survival;f.inventory.hammer=1;f.stats.copperMined=8;f.stats.coreReached=true;s.progress();assert.equal(s.chapter,1,'an inventory and core flag alone do not prove operating machinery');
  f.set(56,29,f.T.MOTOR,{running:true,powered:true});s.scanWorld();s.progress();assert.equal(s.chapter,2);assert.equal(f.inventory.medkit,2);s.progress();assert.equal(f.inventory.medkit,2);
  for(let y=38;y<50;y++)f.set(40,y,f.T.LADDER);s.maxDepth=50;f.engine.metrics.mined=8;s.scanWorld();s.progress();assert.equal(s.chapter,3);assert.equal(s.canCraft('shieldModule'),true);assert.equal(s.canCraft('energyRifle'),false);
  f.stats.kills=6;f.stats.defenseKills=3;f.stats.raidsSurvived=1;s.progress();assert.equal(s.chapter,4);
  f.engine.metrics.produced=12;for(let i=0;i<3;i++){f.set(50+i*2,29,f.T.GENERATOR,{powered:true});f.set(51+i*2,29,f.T.WIRE,{});}f.set(45,29,f.T.PISTON,{running:true});f.set(47,29,f.T.CONVEYOR,{running:true});s.scanWorld();s.progress();assert.equal(s.chapter,5);assert.equal(s.canCraft('energyRifle'),true);
  s.maxDepth=80;f.inventory.energyRifle=1;f.stats.guardianDefeated=true;s.progress();assert.equal(s.chapter,6);assert.ok(s.getJournal().chapters.every(c=>c.complete));assert.ok(s.getJournal().unlocks.includes('Continued sandbox'));
});

test('all five regions have distinct metadata and a large abyss chamber permits a real boss spawn',()=>{
  const f=fixture(),s=f.survival;assert.equal(s.regionAt(20,42).name,'Iron Caverns');assert.equal(s.regionAt(20,54).name,'Crystal Hollows');assert.equal(s.regionAt(20,65).name,'Forgotten Industrial Depths');assert.equal(s.regionAt(20,75).name,'Reactor Abyss');
  for(let x=32;x<95;x++)for(let y=74;y<82;y++)f.set(x,y,f.T.AIR);
  f.player.x=60*f.tile;f.player.y=82*f.tile-f.player.h;s.chapter=5;s.elapsed=200;f.advance(1);assert.ok(s.enemies.some(e=>e.type==='guardian'));
  const boss=s.enemies.find(e=>e.type==='guardian');s.hit(boss,5000,'player');assert.equal(f.stats.guardianDefeated,true);assert.equal(f.engine.drops.find(d=>d.key==='reactorCell').count,1);
});

test('save migration preserves enemies, recovery, progression, raid warnings and rejects corrupt entity fields',()=>{
  const f=fixture(),s=f.survival;const e=f.enemy('drone',100);assert.ok(e);s.setDifficulty('hardcore');s.completed=[1,2];s.chapter=3;s.discoveredRegions.push('iron');s.raid={state:'warning',remaining:18,id:2,count:0};s.caches.push({id:1,x:1000,y:600,kind:'recovery',items:{copper:12}});const saved=JSON.parse(JSON.stringify(s.export()));
  const f2=fixture();f2.survival.restore(saved);assert.equal(f2.survival.chapter,3);assert.equal(f2.survival.enemies[0].type,'drone');assert.equal(f2.survival.caches[0].items.copper,12);assert.equal(f2.survival.raid.state,'warning');assert.equal(f2.survival.difficulty,'hardcore');
  f2.survival.restore({enemies:[{type:'unknown',x:10,y:20,hp:30},{type:'drone',x:'x',y:20,hp:30}],completed:[99,'1',2],chapter:99,caches:[{x:40,y:40,items:{copper:-1,iron:'bad',wood:3}}]});assert.equal(f2.survival.enemies.length,0);assert.equal(f2.survival.chapter,1);assert.deepEqual(f2.survival.caches[0].items,{wood:3});
  f2.survival.restore(null);assert.equal(f2.survival.difficulty,'standard');
});

test('spatial partition queries return only nearby live enemies',()=>{
  const grid=new S.SpatialGrid(100),entities=[{id:1,x:20,y:20,w:10,h:10,hp:3},{id:2,x:700,y:800,w:10,h:10,hp:3},{id:3,x:25,y:25,w:10,h:10,hp:0}];grid.rebuild(entities);assert.deepEqual(grid.query(25,25,30).map(e=>e.id),[1]);
});

test('a noncontiguous saved chapter history cannot permanently block later real completion',()=>{
  const f=fixture(),s=f.survival;s.restore({completed:[2,4,99,'1'],chapter:5});assert.deepEqual(s.completed,[]);assert.equal(s.chapter,1);
  f.inventory.hammer=1;f.stats.copperMined=8;f.stats.coreReached=true;f.set(56,29,f.T.MOTOR,{running:true,powered:true});s.scanWorld();s.progress();assert.equal(s.chapter,2);assert.deepEqual(s.completed,[1]);assert.equal(f.inventory.medkit,2);
  for(let y=38;y<50;y++)f.set(40,y,f.T.LADDER);s.maxDepth=50;f.engine.metrics.mined=8;s.scanWorld();s.progress();assert.equal(s.chapter,3);assert.deepEqual(s.completed,[1,2]);assert.equal(f.inventory.ammo,8,'the previously malformed chapter is earned and rewarded normally');
  const saved=s.export(),f2=fixture();f2.survival.restore({...saved,completed:[1,2,4]});assert.deepEqual(f2.survival.completed,[1,2]);assert.equal(f2.survival.chapter,3);assert.equal(f2.inventory.ammo,undefined,'restoration itself never grants duplicate rewards');
});

test('spatial queries include an overlapping body across a bucket boundary in every direction',()=>{
  const grid=new S.SpatialGrid(100),entities=[{id:1,x:101,y:50,w:20,h:20,hp:5},{id:2,x:99,y:50,w:20,h:20,hp:5},{id:3,x:50,y:101,w:20,h:20,hp:5},{id:4,x:50,y:99,w:20,h:20,hp:5}];grid.rebuild(entities);
  assert.ok(grid.query(90,50,2).some(e=>e.id===1));assert.ok(grid.query(110,50,2).some(e=>e.id===2));assert.ok(grid.query(50,90,2).some(e=>e.id===3));assert.ok(grid.query(50,110,2).some(e=>e.id===4));
  for(let x=0;x<=130;x+=3)for(let y=0;y<=130;y+=5){const expected=entities.filter(e=>Math.hypot(e.x-x,e.y-y)<=13+Math.max(e.w,e.h)/2).map(e=>e.id).sort();assert.deepEqual(grid.query(x,y,13).map(e=>e.id).sort(),expected);}
});
