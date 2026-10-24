const test=require('node:test');
const assert=require('node:assert/strict');
const {game}=require('./harness.cjs');
const {blueprints}=require('../interface.js');
function clean(g){g.api.world.fill(0);g.api.meta.clear();g.engine.reset();g.survival.reset();g.player.x=9*24;g.player.y=10*24;}
function aim(g,x,y){Object.assign(g.api.pointer,{tx:x,ty:y,wx:x*24+12,wy:y*24+12});}
function step(g,seconds){for(let t=0;t<seconds;t+=.05)g.simulationStep(.05);}
function select(g,key){g.api.selected=g.items.findIndex(i=>i.key===key);}

test('outpost repair uses a real missing wire and an isolated motor supply within cable capacity',()=>{
  const g=game();assert.equal(g.api.meta.get(g.idx(10,23)).powered,false);assert.equal(g.api.meta.get(g.idx(12,28)).running,false);
  select(g,'wire');aim(g,9,27);g.useOrPlace();assert.equal(g.api.meta.get(g.idx(10,23)).powered,true);
  select(g,'generator');aim(g,11,28);g.useOrPlace();assert.equal(g.api.meta.get(g.idx(12,28)).running,true);
  assert.equal(g.api.meta.get(g.idx(8,28)).powered,true);assert.equal(g.api.meta.get(g.idx(10,23)).overload,false);
});

test('mining nearby copper or iron immediately adds the real material without a second collection action',()=>{
  const g=game();clean(g);select(g,'stone');
  for(const [id,key] of [[g.T.COPPER,'copper'],[g.T.IRON,'iron']]){
    g.set(10,10,id);aim(g,10,10);g.api.pointer.down=true;const before=g.inventory[key]||0;
    g.mineStep(3);assert.equal(g.get(10,10),g.T.AIR);assert.equal(g.inventory[key],before+1);
  }
});

test('full inventory leaves mined resources in physical drops and pickup never passes through a wall',()=>{
  const g=game();clean(g);for(const k of Object.keys(g.inventory))delete g.inventory[k];g.inventory.stone=2048;
  g.set(10,10,g.T.COPPER);aim(g,10,10);g.api.pointer.down=true;g.mineStep(3);
  assert.equal(g.engine.drops.find(d=>d.key==='copper').count,1);assert.equal(g.inventory.copper,undefined);
  g.inventory.stone=0;g.set(10,10,g.T.STONE);g.engine.drops[0].x=11.5;g.collectDrops(true);assert.equal(g.inventory.copper,undefined);
  g.set(10,10,g.T.AIR);g.collectDrops(true);assert.equal(g.inventory.copper,1);
});

test('world collision boundaries cannot be mined into free resources',()=>{
  const g=game();clean(g);g.player.x=24;g.player.y=10*24;aim(g,-1,10);g.api.pointer.down=true;const before=g.inventory.stone;g.mineStep(5);
  assert.equal(g.inventory.stone,before);assert.equal(g.engine.drops.length,0);
});

test('reinforced barriers use their displayed durability and recover their component on destruction',()=>{
  const g=game();clean(g);g.set(15,10,g.T.BARRIER,{});g.survival.damageStructure(15,10,40);assert.equal(g.api.meta.get(g.idx(15,10)).hp,200);
  g.survival.damageStructure(15,10,200);assert.equal(g.get(15,10),g.T.AIR);assert.equal(g.engine.drops.find(d=>d.key==='barrier').count,1);
});

test('ropes require an anchor and drag construction debits only valid sections',()=>{
  const g=game();clean(g);select(g,'rope');g.inventory.rope=10;
  assert.equal(g.placementValid(10,10,g.T.ROPE),false);g.set(10,7,g.T.ANCHOR,{});
  g.player.x=10*24;g.player.y=9*24;g.api.buildDrag={x0:10,y0:8,x1:10,y1:13};
  g.placeLine();assert.equal(g.inventory.rope,4);for(let y=8;y<=13;y++)assert.equal(g.get(10,y),g.T.ROPE);
  g.player.y=12*24;g.keys.KeyW=true;const start=g.player.y;for(let i=0;i<90;i++)g.movePlayer(.016);assert.ok(g.player.y<start-100);
  g.player.y=10*24;g.keys.KeyW=false;g.keys.Space=true;g.movePlayer(.016);assert.ok(g.player.vy<0,'Jump dismounts from the rope');
});

test('a forty-section ladder provides a reliable route down and back up a mine shaft',()=>{
  const g=game();clean(g);for(let y=15;y<=55;y++)g.set(12,y,g.T.LADDER,{});
  g.player.x=12*24+4;g.player.y=16*24;g.keys.KeyS=true;for(let i=0;i<400;i++)g.movePlayer(.016);
  const bottom=g.player.y;assert.ok(bottom>45*24);g.keys.KeyS=false;g.keys.KeyW=true;for(let i=0;i<400;i++)g.movePlayer(.016);
  assert.ok(g.player.y<bottom-650);assert.equal(g.player.health,100);
});

test('ground contact remains outside solid floors so a normally walking player can establish a checkpoint',()=>{
  const g=game();clean(g);for(let x=5;x<30;x++)g.set(x,15,g.T.STONE);g.set(12,14,g.T.GENERATOR,{});
  g.player.x=14*24;g.player.y=15*24-g.player.h+.997;g.keys.KeyD=true;for(let i=0;i<40;i++)g.movePlayer(.016);g.keys.KeyD=false;
  g.survival.scanWorld();assert.ok(g.player.y+g.player.h<=15*24+.01);assert.equal(g.survival.setCheckpoint(),true);
});

test('industrial generation progress requires one connected supply and ignores disabled or isolated generators',()=>{
  const g=game();clean(g);for(const x of [5,9,13])g.set(x,10,g.T.GENERATOR,{});g.simulateNetworks();g.survival.scanWorld();assert.equal(g.survival.powerCapacity,0);
  for(let x=5;x<=13;x++)g.set(x,11,g.T.WIRE,{});g.set(10,12,g.T.MOTOR,{rot:1});g.simulateNetworks();g.survival.scanWorld();assert.equal(g.survival.powerCapacity,36);
  g.api.meta.get(g.idx(13,10)).disabled=true;g.simulateNetworks();g.survival.scanWorld();assert.equal(g.survival.powerCapacity,24);
});

test('blueprints debit real components atomically, retain clearances, and link elevator loads separately',()=>{
  const g=game();clean(g);const plan=blueprints.find(b=>b.id==='elevator');for(const [key,n] of Object.entries(plan.cost))g.inventory[key]=n;
  g.player.x=15*24;g.player.y=12*24;aim(g,10,8);g.api.blueprint=plan;
  g.set(13,8,g.T.STONE);assert.equal(g.placeBlueprint(),false);assert.equal(g.inventory.generator,1);
  g.set(13,8,g.T.AIR);assert.equal(g.placeBlueprint(),true);assert.equal(g.inventory.generator,0);
  const load=g.api.meta.get(g.idx(13,13));assert.ok(load.assembly);assert.equal(g.api.meta.get(g.idx(13,10)).assembly,undefined);
});

test('a switch reverses a powered winch and the linked deck carries its rider safely in both directions',()=>{
  const g=game();clean(g);g.player.x=14*24;g.player.y=10*24;
  g.set(12,5,g.T.GENERATOR,{});g.set(12,6,g.T.MOTOR,{rot:1});g.set(12,7,g.T.WINCH,{rot:1});g.set(13,7,g.T.SWITCH,{rot:2,on:true});
  for(let x=11;x<=13;x++)g.set(x,12,g.T.PLATFORM,{});g.engine.link([g.idx(11,12),g.idx(12,12),g.idx(13,12)]);
  g.player.x=12*24+3;g.player.y=12*24-g.player.h;const start=g.player.y;step(g,1.1);assert.ok(g.player.y<start);
  g.api.meta.get(g.idx(13,7)).on=false;const raised=g.player.y;step(g,1.1);assert.ok(g.player.y>raised);
});

test('a real powered winch uses a separately linked counterweight and guide rails to lift a heavy occupied deck',()=>{
  const g=game();clean(g);g.set(12,5,g.T.GENERATOR,{});g.set(12,6,g.T.MOTOR,{rot:1});g.set(12,7,g.T.WINCH,{rot:1});g.set(13,7,g.T.SWITCH,{rot:2,on:true});
  g.set(11,12,g.T.IRON_BLOCK,{});g.set(12,12,g.T.PLATFORM,{});g.set(13,12,g.T.IRON_BLOCK,{});g.engine.link([g.idx(11,12),g.idx(12,12),g.idx(13,12)]);
  g.player.x=12*24+3;g.player.y=12*24-g.player.h;const start=g.player.y;step(g,.55);assert.equal(g.player.y,start);
  g.set(17,10,g.T.COUNTERWEIGHT,{});g.set(17,11,g.T.COUNTERWEIGHT,{});g.engine.link([g.idx(17,10),g.idx(17,11)]);
  for(let y=10;y<=15;y++)g.set(10,y,g.T.RAIL,{});for(let y=8;y<=15;y++)g.set(18,y,g.T.RAIL,{});step(g,1.1);
  const machine=g.api.meta.get(g.idx(12,7));assert.ok(g.player.y<start);assert.equal(machine.counterbalanced,true);assert.equal(machine.guided,true);assert.equal(machine.counterweightMass,16);
  assert.equal(g.get(17,10),g.T.AIR);assert.ok(machine.requiredTorque<machine.availableTorque);
});

test('all dialogs pause simulation, release held input, and leave gameplay responsive when closed',()=>{
  const g=game();for(const id of ['craftPanel','manual','pausePanel','journalPanel','blueprintPanel','welcomePanel']){
    g.keys.KeyD=true;g.api.pointer.down=true;g.toggleModal(id,true);assert.equal(g.api.paused,true);assert.equal(g.keys.KeyD,false);assert.equal(g.api.pointer.down,false);
    const before=g.player.x;g.tick(100);assert.equal(g.player.x,before);g.toggleModal(id,false);assert.equal(g.api.paused,false);
    g.keys.KeyD=true;g.movePlayer(.016);g.keys.KeyD=false;assert.ok(g.player.x>before);
  }
});

test('a corrupt primary save recovers the valid backup, including original construction IDs',()=>{
  const g=game();g.set(30,20,g.T.DOOR,{open:true});g.set(31,20,g.T.LADDER,{});g.set(32,20,g.T.LEDGE,{});g.saveGame();g.saveGame();
  g.storage.set('circuitbound-save-v1','{broken');assert.equal(g.loadGame(),true);assert.equal(g.get(30,20),52);assert.equal(g.get(31,20),53);assert.equal(g.get(32,20),54);
  g.saveGame();assert.ok(JSON.parse(g.storage.get('circuitbound-save-v1')).world);
  const corrupt=JSON.parse(g.storage.get('circuitbound-save-v1'));corrupt.meta[0][1].rot=8;g.storage.set('circuitbound-save-v1',JSON.stringify(corrupt));assert.equal(g.loadGame(),true);assert.equal(g.get(30,20),52);
});

test('integrated fixed steps perform mining, logistics, manufacturing and powered enemy combat',()=>{
  const g=game();clean(g);const {T}=g;g.player.x=19*24;g.player.y=20*24;
  g.set(8,2,T.GENERATOR,{});g.set(8,3,T.MOTOR,{rot:1});g.set(8,4,T.DRILL,{rot:1});g.set(9,4,T.GENERATOR,{});g.set(7,4,T.HOPPER,{rot:2});g.set(6,4,T.STORAGE,{rot:2});g.set(8,5,T.IRON);
  step(g,4);assert.equal(g.get(8,5),T.AIR);assert.equal(g.api.meta.get(g.idx(6,4)).contents.iron,1);assert.equal(g.stats.automined,1);
  const ore=g.api.meta.get(g.idx(6,4)).contents;ore.iron--;g.set(13,4,T.SMELTER,{recipe:'rawIron',contents:{iron:1}});g.set(13,5,T.GENERATOR,{});step(g,3);assert.equal(g.api.meta.get(g.idx(13,4)).outputs,undefined,'Insufficient real ingredients cannot produce');
  g.api.meta.get(g.idx(13,4)).contents.iron++;step(g,3);assert.equal(g.api.meta.get(g.idx(13,4)).outputs.ironIngot,1);
  for(let x=14;x<25;x++)g.set(x,12,T.STONE);g.set(16,10,T.TURRET,{contents:{ammo:3}});g.set(16,11,T.GENERATOR,{});
  const enemy=g.survival.spawn('beetle',20*24,12*24-7,{ignoreDistance:true});assert.ok(enemy);step(g,3);
  assert.equal(enemy.hp,0);assert.equal(g.stats.defenseKills,1);assert.equal(g.engine.metrics.shots,2);assert.equal(g.api.meta.get(g.idx(16,10)).contents.ammo,1);
});

test('machine inspector provides touch-friendly material loading and finished-output collection',()=>{
  const g=game();clean(g);g.set(11,10,g.T.SMELTER,{recipe:'rawIron'});g.set(11,11,g.T.GENERATOR,{});g.inventory.iron=2;aim(g,11,10);
  g.interactAtPointer();const buttons=()=>g.ids.get('machineActions').children.flatMap(e=>e.children||[]).filter(e=>e.onclick);
  const load=buttons().find(b=>b.textContent.startsWith('Load iron'));assert.ok(load);load.onclick();assert.equal(g.inventory.iron,0);assert.equal(g.api.meta.get(g.idx(11,10)).contents.iron,2);
  step(g,3);const collect=buttons().find(b=>b.textContent==='Collect finished goods');assert.ok(collect);collect.onclick();assert.equal(g.inventory.ironIngot,1);assert.equal(g.api.meta.get(g.idx(11,10)).outputs.ironIngot,undefined);
});

test('exploration schematics unlock real fabrication early and survive save/reload without duplicate rewards',()=>{
  const g=game();clean(g);const recipe=g.recipes.find(r=>r.key==='advancedFabricator');assert.equal(g.survival.canCraft(recipe),false);
  g.set(10,10,g.T.STORAGE,{contents:{steel:4},schematic:'fabrication'});aim(g,10,10);g.interactItems();assert.equal(g.survival.canCraft(recipe),true);assert.equal(g.inventory.steel,4);
  g.interactItems();assert.equal(g.stats.schematics.length,1);g.saveGame();const restored=game(JSON.parse(g.storage.get('circuitbound-save-v1')));assert.equal(restored.survival.canCraft(restored.recipes.find(r=>r.key==='advancedFabricator')),true);
});

test('abandoned machinery repairs consume materials and leave it requiring a real drive connection',()=>{
  const g=game();clean(g);g.set(10,10,g.T.WINCH,{disabled:true,ruin:true});const m=g.api.meta.get(g.idx(10,10)),iron=g.inventory.iron,copper=g.inventory.copper;
  g.configureComponent(g.T.WINCH,g.idx(10,10),m);assert.equal(m.disabled,false);assert.equal(g.inventory.iron,iron-3);assert.equal(g.inventory.copper,copper-2);assert.equal(m.running,false);
});
