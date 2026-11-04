'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {loadGame,play} = require('./deep-sea-harness.cjs');

test('opening has plentiful nearby edible fish and stays within the population cap', () => {
  for(let seed=1;seed<=16;seed++){
    const g=loadGame(seed); play(g);
    const {state:s}=g;
    assert.equal(s.fish.length,g.MAX_FISH);
    const near=s.fish.filter(f=>f.size<s.player.size*.9 && g.dist3(f.pos,s.player.pos)<85);
    assert.ok(near.length>=4,`seed ${seed}: only ${near.length} edible fish within 85 units`);
    assert.ok(near.some(f=>f.pos.z>s.player.pos.z),'prey should be ahead of the opening camera');
    for(let i=0;i<25;i++)g.spawnFish(false);
    assert.ok(s.fish.length<=g.MAX_FISH,'explicit spawns must respect global cap');
  }
});

test('refilling near the cap at abyss depth never overfills a school or predator pack', () => {
  const g=loadGame(123);play(g);const s=g.state;
  s.player.pos={x:1850,y:g.waterColumnY(1850,0,-1220,24),z:0};
  for(let i=0;i<80;i++){
    s.fish.length=g.MAX_FISH-1;g.spawnFish(false);
    assert.ok(s.fish.length<=g.MAX_FISH);
  }
  s.fish=[];for(let i=0;i<g.MAX_FISH;i++)g.spawnFish(false);
  assert.ok(s.fish.some(f=>!f.type.predator&&f.size<s.player.size),'deep water still has edible prey');
});

test('the opening swim lane reaches its first meal within three seconds without steering', () => {
  for(const seed of [1,7,19]){
    const g=loadGame(seed);play(g);
    for(let i=0;i<180;i++){
      g.state.t+=1/60;g.rebuild();g.updatePlayer(1/60);g.updateFish(1/60);g.updateBites(1/60);
    }
    assert.ok(g.state.player.fishEaten>0,`seed ${seed}: opening shoal must stay on the fish's vertical swim lane`);
    assert.equal(g.state.mode,'play');
  }
});

test('larger hunters eat smaller fish; equal size and disabled hunters cannot eat', () => {
  const g=loadGame(); play(g); const a={...g.state.fish[0],type:g.fishTypes.find(t=>t.predator),size:2,stunT:0,inkBlindT:0};
  const b={...g.state.fish[1],size:.5};
  assert.equal(g.fishCanEatFish(a,b),true);
  assert.equal(g.fishCanEatFish(b,a),false);
  assert.equal(g.fishCanEatFish(a,{...b,size:2}),false);
  assert.equal(g.fishCanEatFish({...a,stunT:1},b),false);
  assert.equal(g.fishCanEatFish({...a,inkBlindT:1},b),false);
});

test('NPC predation consumes one prey and visibly grows the hunter', () => {
  const g=loadGame();play(g); const s=g.state;
  const hunter={...s.fish[0],type:g.fishTypes.find(t=>t.predator),pos:{x:0,y:-30,z:50},dir:{x:0,y:0,z:1},size:2,stunT:0,inkBlindT:0};
  const prey={...s.fish[1],pos:{x:0,y:-30,z:52},size:.3};
  s.fish=[hunter,prey];g.rebuild();g.resolveFishPredation();
  assert.equal(s.fish.length,1);assert.equal(s.fish[0],hunter);assert.ok(hunter.size>2);
});

test('touching a smaller fish automatically feeds, grows, and replenishes sprint', () => {
  const g=loadGame();play(g);const s=g.state,p=s.player;
  const prey={...s.fish[0],pos:{...p.pos},vel:{x:0,y:0,z:0},dir:{x:0,y:0,z:1},size:.25,stunT:0};
  s.fish=[prey];p.sprintEnergy=15;g.rebuild();g.updateFish(1/60);
  assert.equal(p.fishEaten,1);assert.ok(p.size>1);assert.ok(p.sprintEnergy>15);
});

test('sprint depletes while held and recovers after releasing', () => {
  const g=loadGame();play(g);const p=g.state.player;
  g.state.obstacles=[];g.rebuild();g.keys.shift=true;
  for(let i=0;i<60;i++)g.updatePlayer(1/60);
  const drained=p.sprintEnergy;assert.ok(drained<80);assert.equal(p.sprinting,true);
  g.keys.shift=false;for(let i=0;i<120;i++)g.updatePlayer(1/60);
  assert.ok(p.sprintEnergy>drained);
});

test('a slightly larger fish cannot be automatically swallowed without a jaw evolution', () => {
  const g=loadGame();play(g);const s=g.state,p=s.player;
  const fish={...s.fish[0],pos:{...p.pos},size:1.02,vel:{x:0,y:0,z:0},stunT:0,removed:false};
  s.fish=[fish];g.rebuild();g.updateFish(1/60);assert.equal(p.fishEaten,0);
  p.mods.bitePower=.12;fish.pos={...p.pos};fish.removed=false;s.fish=[fish];g.rebuild();g.updateFish(1/60);
  assert.equal(p.fishEaten,1);
});

test('pause freezes world, sprint, and combo clock; resume does not fast-forward', () => {
  const g=loadGame();play(g);assert.equal(typeof g.togglePause,'function');
  const p=g.state.player;g.tick(1000);p.combo=3;p.comboUntil=g.state.t+4;
  g.togglePause();const time=g.state.t,pos={...p.pos},energy=p.sprintEnergy;
  g.tick(6000);assert.equal(g.state.mode,'paused');assert.equal(g.state.t,time);
  assert.deepEqual({...p.pos},pos);assert.equal(p.sprintEnergy,energy);assert.equal(p.combo,3);
  g.togglePause();g.tick(6017);assert.equal(g.state.mode,'play');assert.ok(g.state.t-time<.04);
});

test('predators telegraph, commit to a dodgeable lane, and recover before another lunge', () => {
  const g=loadGame();play(g);assert.equal(typeof g.attack,'function');
  const f={...g.state.fish[0],type:g.fishTypes.find(t=>t.predator),pos:{x:0,y:-30,z:60},dir:{x:0,y:0,z:-1},size:2,attackState:'stalk',attackT:0};
  g.state.player.pos={x:0,y:-30,z:0};g.state.player.vel={x:0,y:0,z:0};
  const args=()=>[{x:0,y:0,z:-1},'hunt',true,60];
  g.attack(f,.01,...args());assert.equal(f.attackState,'windup');
  for(let i=0;i<30;i++)g.attack(f,1/60,...args());
  assert.equal(f.attackState,'windup','half a second to see the warning');
  for(let i=0;i<11;i++)g.attack(f,1/60,...args());
  assert.equal(f.attackState,'lunge');const lane={...f.attackDir};
  g.state.player.pos.x=55;const lunging=g.attack(f,.1,{x:1,y:0,z:0},'hunt',true,60);
  assert.deepEqual({...lunging.desired},lane,'lunge cannot track a lateral dodge');
  for(let i=0;i<40;i++)g.attack(f,1/60,...args());
  assert.equal(f.attackState,'recover');
  g.attack(f,.5,...args());assert.equal(f.attackState,'recover');
  for(let i=0;i<90;i++)g.attack(f,1/60,...args());
  assert.ok(['stalk','windup'].includes(f.attackState));
});

test('windup and recovery contact are fair escape windows; an active lunge is lethal', () => {
  const g=loadGame();play(g);const s=g.state,p=s.player;
  const template={...s.fish[0]};
  for(const phase of ['windup','recover','lunge']){
    s.mode='play';s.t=20;p.spawnGraceUntil=0;
    const f={...template,type:g.fishTypes.find(t=>t.name==='Shallow Fanglet'),size:2,pos:{...p.pos},vel:{x:0,y:0,z:0},dir:{x:0,y:0,z:1},attackState:phase,attackT:.5,attackDir:{x:0,y:0,z:1},stunT:0,inkBlindT:0,removed:false};
    s.fish=[f];g.rebuild();g.updateFish(.001);
    assert.equal(s.mode,phase==='lunge'?'dying':'play',phase);
  }
});

test('spatial queries include neighbors across cell edges and ignore removed fish', () => {
  const g=loadGame();play(g);assert.equal(typeof g.nearbyFish,'function');
  const a={...g.state.fish[0],pos:{x:79,y:-30,z:79},removed:false};
  const b={...g.state.fish[1],pos:{x:81,y:-30,z:81},removed:false};
  const removed={...b,removed:true};
  g.state.fish=[a,b,removed];g.rebuild();
  const found=[...g.nearbyFish(a.pos,10)];assert.ok(found.includes(b));assert.ok(!found.includes(removed));
});

test('feeding combos expire and a restart clears the current chain', () => {
  const g=loadGame();play(g);const p=g.state.player;
  const template={...g.state.fish[0]};
  for(let i=0;i<3;i++){
    const fish={...template,type:g.fishTypes[0],size:.25,pos:{...p.pos},removed:false};g.state.fish=[fish];g.eatFish(0,fish);g.state.t+=.5;
  }
  assert.equal(p.combo,3);assert.equal(p.bestCombo,3);
  g.state.t+=5;const fish={...template,type:g.fishTypes[0],size:.25,pos:{...p.pos},removed:false};g.state.fish=[fish];g.eatFish(0,fish);
  assert.equal(p.combo,1);g.start();assert.equal(p.combo,0);assert.equal(p.bestCombo,0);
});

test('failed obstacle placement has a bounded retry budget per frame', () => {
  const g=loadGame();play(g);g.state.obstacles=[];let calls=0;
  const previous=g.setObstacleSpawner(()=>{calls++;if(calls>4)throw new Error('Unbounded non-progressing obstacle loop');});
  try{g.updateObstacles(1/60);assert.equal(calls,4);assert.equal(g.state.obstacles.length,0);}
  finally{g.setObstacleSpawner(previous);}
});

test('grazing cannot shrink a fish that has already grown beyond size twenty-four', () => {
  const g=loadGame();play(g);const p=g.state.player;p.size=25;p.mass=625;
  const f=g.forwardFromAngles(p.yaw,p.pitch),reach=4+p.size*2.3;
  const mouth={x:p.pos.x+f.x*reach,y:p.pos.y+f.y*reach,z:p.pos.z+f.z*reach};
  g.state.plants=[{base:{x:mouth.x,y:mouth.y-12,z:mouth.z},pos:{...mouth},height:12,size:1,phase:0}];
  const eaten=p.eaten;g.updatePlants(1/60);assert.equal(p.eaten,eaten+1);assert.ok(p.size>=25);assert.ok(p.size<=28);
});

test('sustained ecosystem stays finite, capped, and responsive through evolution/restart', () => {
  const g=loadGame(31);play(g);const s=g.state;s.player.mods.predatorResist=100;
  for(let i=0;i<900;i++){
    s.t+=1/60;g.rebuild();g.updatePlayer(1/60);g.updateObstacles(1/60);g.rebuild();g.updateFish(1/60);g.updateBites(1/60);
    if(s.mode==='upgrade')g.applyUpgrade(g.UPGRADE_DEFS[0]);
    assert.ok(s.fish.length<=g.MAX_FISH);
    for(const f of s.fish)for(const axis of ['x','y','z'])assert.ok(Number.isFinite(f.pos[axis]));
  }
  assert.equal(s.mode,'play');
  g.start();assert.equal(s.mode,'starter');assert.equal(s.player.size,1);assert.equal(s.player.fishEaten,0);
  assert.ok(s.fish.length<=g.MAX_FISH);assert.equal(s.player.sprintEnergy,g.sprintMax());
});
