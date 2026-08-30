const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {game}=require('./harness.cjs');
function clean(g){g.api.world.fill(0);g.api.meta.clear();g.engine.reset();g.player.x=25*24;g.player.y=20*24;}
function aim(g,x,y){Object.assign(g.api.pointer,{tx:x,ty:y});g.player.x=(x-3)*24;g.player.y=y*24;}
function advance(g,n=1){for(let i=0;i<n;i++){g.simulateNetworks(.05);g.updateMachines(.05);g.engine.logistics(.05)}}
test('real game boots, renders frames, leaves manual closed and powers starter lamp',()=>{
  const g=game();assert.equal(g.ids.get('manual').classList.contains('show'),false);
  assert.equal(g.api.meta.get(g.idx(8,28)).powered,true);g.render();for(let t=0;t<1000;t+=16)g.tick(t);
  assert.ok(g.engine.rebuilds>0);g.toggleModal('manual');assert.equal(g.ids.get('manual').classList.contains('show'),true);
});
test('entrypoint loads readable source and open-modal CSS beats default ID display rules',()=>{
  const root=path.join(__dirname,'..');const html=fs.readFileSync(path.join(root,'../circuitbound.html'),'utf8'),css=fs.readFileSync(path.join(root,'styles.css'),'utf8');
  assert.doesNotMatch(html,/document\.write|part-\d|atob/);for(const file of ['engineering.js','app.js','styles.css']){assert.ok(html.includes('/game/circuitbound/'+file));assert.ok(fs.existsSync(path.join(root,file)))}
  assert.match(css,/#craftPanel\.show,#manual\.show,#pausePanel\.show\{display:block\}/);
});
test('placing and configuring an advanced part updates simulation state and inventory',()=>{
  const g=game();clean(g);const {T}=g;aim(g,8,10);g.api.selected=g.items.findIndex(i=>i.key==='timer');g.inventory.timer=1;
  g.useOrPlace();assert.equal(g.get(8,10),T.TIMER);assert.equal(g.inventory.timer,0);g.useOrPlace();assert.equal(g.api.meta.get(g.idx(8,10)).delay,2);
});
test('piston moves only its selected assembly and obeys sustained control for retraction',()=>{
  const g=game();clean(g);const {T}=g;
  g.set(1,8,T.GENERATOR,{});g.set(2,8,T.MOTOR,{});g.set(3,8,T.PISTON,{});g.set(4,8,T.PLATFORM,{});g.set(4,9,T.WOOD,{});
  g.engine.link([g.idx(4,8)]);g.engine.link([g.idx(4,9)]);g.set(3,7,T.SWITCH,{rot:1,on:true});
  advance(g);assert.equal(g.get(5,8),T.PLATFORM);assert.equal(g.get(4,9),T.WOOD);assert.equal(g.api.meta.get(g.idx(3,8)).extended,true);
  g.api.meta.get(g.idx(3,7)).on=false;advance(g);assert.equal(g.get(4,8),T.PLATFORM);assert.equal(g.get(5,8),T.AIR);
});
test('heavy load stalls until a torque gear is installed',()=>{
  const g=game();clean(g);const {T}=g;
  g.set(1,8,T.GENERATOR,{});g.set(2,8,T.MOTOR,{});g.set(3,8,T.SHAFT,{});g.set(4,8,T.PISTON,{});g.set(5,8,T.IRON_BLOCK,{});g.set(5,9,T.IRON_BLOCK,{});g.engine.link([g.idx(5,8),g.idx(5,9)]);
  advance(g);assert.equal(g.get(5,8),T.IRON_BLOCK);assert.match(g.api.meta.get(g.idx(4,8)).fault,/STALLED/);
  g.set(3,8,T.GEAR,{mode:0});advance(g);assert.equal(g.get(6,8),T.IRON_BLOCK);assert.equal(g.get(6,9),T.IRON_BLOCK);
});
test('piston collision is atomic and never crushes the player',()=>{
  const g=game();clean(g);const {T}=g;g.set(8,8,T.WOOD,{});g.set(9,8,T.STONE,{});const m={};
  assert.equal(g.moveGroup([[8,8]],1,0,12,m),false);assert.equal(g.get(8,8),T.WOOD);g.set(9,8,T.AIR);
  g.player.x=9*24;g.player.y=8*24;assert.equal(g.moveGroup([[8,8]],1,0,12,m),false);assert.match(m.fault,/PLAYER/);
});
test('moving linked platforms carry the player, but stop when rider clearance is blocked',()=>{
  const g=game();clean(g);const {T}=g;g.set(8,8,T.PLATFORM,{});g.player.x=8*24+4;g.player.y=8*24-g.player.h;
  const initial=g.player.x;assert.equal(g.moveGroup([[8,8]],1,0,12,{}),true);assert.equal(g.player.x,initial+24);
  g.set(10,7,T.STONE,{});assert.equal(g.moveGroup([[9,8]],1,0,12,{}),false);assert.equal(g.get(9,8),T.PLATFORM);
});
test('bearing rotates a separately linked load and moves its configuration intact',()=>{
  const g=game();clean(g);const {T}=g;g.set(3,8,T.BEARING,{});g.set(4,8,T.STORAGE,{rot:0,contents:{iron:5}});const id=g.engine.link([g.idx(4,8)]);
  const m=g.api.meta.get(g.idx(3,8));m.jointAssembly=id;m.torque=12;
  assert.equal(g.rotateAssembly(3,8,m,1),true);assert.equal(g.get(3,9),T.STORAGE);assert.equal(g.api.meta.get(g.idx(3,9)).rot,1);assert.equal(g.api.meta.get(g.idx(3,9)).contents.iron,5);
});
test('joint sweep detects an obstruction between start and endpoint',()=>{
  const g=game();clean(g);const {T}=g;g.set(8,8,T.BEARING,{});g.set(9,8,T.FRAME,{});g.set(10,8,T.FRAME,{});g.set(10,9,T.STONE,{});
  const id=g.engine.link([g.idx(9,8),g.idx(10,8)]),m={jointAssembly:id,torque:100};
  assert.equal(g.rotateAssembly(8,8,m,1),false);assert.match(m.fault,/SWEEP/);assert.equal(g.get(10,8),T.FRAME);
});
test('hinge swings open and closed while continuous bearing completes a full revolution',()=>{
  const g=game();clean(g);const {T}=g;g.set(1,8,T.GENERATOR,{});g.set(2,8,T.MOTOR,{});g.set(3,8,T.HINGE,{});g.set(4,8,T.FRAME,{});g.engine.link([g.idx(4,8)]);g.set(3,7,T.SWITCH,{rot:1,on:true});
  advance(g,21);assert.equal(g.get(3,9),T.FRAME);g.api.meta.get(g.idx(3,7)).on=false;advance(g,21);assert.equal(g.get(4,8),T.FRAME);
  // A motor/shaft in the sweep correctly blocks full rotation. A remote belt supplies drive behind the mount.
  clean(g);g.set(8,8,T.BEARING,{});g.set(9,8,T.FRAME,{});const id=g.engine.link([g.idx(9,8)]),m={jointAssembly:id,torque:12};
  for(let i=0;i<4;i++)assert.equal(g.rotateAssembly(8,8,m,1),true);assert.equal(g.get(9,8),T.FRAME);
});
test('mining a full container conserves component and contents as dropped entities',()=>{
  const g=game();clean(g);const {T}=g;aim(g,8,10);g.set(8,10,T.STORAGE,{contents:{iron:32,copper:20}});g.api.pointer.down=true;g.api.pointer.button=0;
  g.mineStep(2);assert.equal(g.get(8,10),T.AIR);assert.equal(g.engine.drops.find(d=>d.key==='storage').count,1);assert.equal(g.engine.drops.find(d=>d.key==='iron').count,32);assert.equal(g.engine.drops.find(d=>d.key==='copper').count,20);
});
test('mining refuses safely when drop capacity cannot preserve container cargo',()=>{
  const g=game();clean(g);aim(g,8,10);g.set(8,10,g.T.STORAGE,{contents:{iron:32,copper:20}});for(let i=0;i<510;i++)g.engine.spawn('stone',1,i,1);
  g.api.pointer.down=true;g.api.pointer.button=0;g.mineStep(2);assert.equal(g.get(8,10),g.T.STORAGE);assert.equal(g.engine.drops.length,510);
});
test('save/reload preserves memory, items, assemblies and a moved machine',()=>{
  const g=game();clean(g);const {T}=g;g.set(8,10,T.COUNTER,{count:7,target:8,prevInput:true,rot:2});g.set(9,10,T.STORAGE,{contents:{copper:11}});g.engine.link([g.idx(8,10),g.idx(9,10)]);
  assert.equal(g.moveGroup([[8,10],[9,10]],1,0,30,{}),true);g.engine.spawn('iron',3,12.5,12.5);g.saveGame();
  const saved=JSON.parse(g.storage.get('circuitbound-save-v1')),restored=game(saved);
  assert.equal(restored.get(9,10),T.COUNTER);assert.equal(restored.api.meta.get(restored.idx(9,10)).count,7);assert.equal(restored.api.meta.get(restored.idx(9,10)).rot,2);
  assert.equal(restored.api.meta.get(restored.idx(10,10)).contents.copper,11);assert.equal(restored.engine.drops[0].count,3);assert.equal(restored.engine.group(9,10).length,2);
});
test('new world clears physical items and advanced structure state',()=>{
  const g=game();clean(g);g.engine.spawn('iron',9,2,2);g.set(5,5,g.T.WOOD,{});g.engine.link([g.idx(5,5)]);g.generate();
  assert.equal(g.engine.drops.length,0);assert.equal(g.engine.assemblies.size,0);assert.equal(g.get(8,28),g.T.LAMP);
});
test('blur releases held movement and mining inputs',()=>{
  const g=game();g.keys.KeyA=true;g.api.pointer.down=true;g.listeners.blur();assert.equal(g.keys.KeyA,false);assert.equal(g.api.pointer.down,false);
});
test('belt-driven bearing completes a powered revolution without breaking its own drive path',()=>{
  const g=game();clean(g);const {T}=g;
  g.set(1,8,T.GENERATOR,{});g.set(2,8,T.MOTOR,{});g.set(3,8,T.BELT,{});g.set(8,8,T.BEARING,{});g.set(9,8,T.FRAME,{});g.engine.link([g.idx(9,8)]);
  advance(g,85);assert.equal(g.get(9,8),T.FRAME);assert.equal(g.api.meta.get(g.idx(8,8)).running,true);assert.equal(g.api.meta.get(g.idx(8,8)).jointStep,0);
});
test('a piston cannot translate a load that is still attached to an anchored joint',()=>{
  const g=game();clean(g);const {T}=g;g.set(3,8,T.HINGE,{});g.set(4,8,T.FRAME,{});const id=g.engine.link([g.idx(4,8)]);g.api.meta.get(g.idx(3,8)).jointAssembly=id;
  const m={};assert.equal(g.moveGroup([[4,8]],1,0,12,m),false);assert.match(m.fault,/ANCHORED JOINT/);
});
