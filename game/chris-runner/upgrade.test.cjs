const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const extension = fs.readFileSync(path.join(__dirname, 'upgrade.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '../chrisrunner.html'), 'utf8');
const main = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)]
  .map(match => match[1]).find(script => script.includes('function generateChunk'));

test('both gameplay scripts parse', () => {
  new vm.Script(main); new vm.Script(extension);
});

test('10,000 chained sections preserve swept clearance for both players and varied lanes', () => {
  const ctx = vm.createContext({}); vm.runInContext(extension, ctx);
  const plan = vm.runInContext('planRunnerSection', ctx);
  let seed = 91723; const random = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
  const kinds = new Set(), exits = new Set(); let previous = null, challenged = 0, centerBlocked = 0, transitions = 0;
  for (let i = 0; i < 10000; i++) {
    const section = plan(i, random, previous); kinds.add(section.kind); exits.add(section.exit);
    const points = section.points;
    assert.equal(section.entry, previous?.exit || 0);
    if (!section.recovery) {
      challenged++;
      if (section.rows.some(r => r.blocks.some(b => b.x === 0))) centerBlocked++;
      if (previous && previous.exit !== section.exit) transitions++;
      assert.notEqual(section.kind, previous?.kind);
    }
    for (let j = 1; j < points.length; j++) {
      const a = points[j - 1], b = points[j];
      // Kevon's lateral speed / maximum sprint speed still permits this turn.
      assert.ok(Math.abs(b.center - a.center) / (b.z - a.z) <= .5);
      for (let z = a.z; z <= b.z; z += .15) {
        const x = a.center + (b.center - a.center) * (z - a.z) / (b.z - a.z);
        for (const offset of [-1.3, 0, 1.3]) for (const row of section.rows) for (const block of row.blocks) {
          assert.ok(Math.abs(x + offset - block.x) >= 3.2 || Math.abs(z - block.z) >= 1.7, `blocked ${i}/${z}/${offset}`);
        }
      }
    }
    if (section.recovery) assert.equal(section.rows.flatMap(r => r.blocks).length, 0);
    if (section.rewardX !== undefined) assert.ok(Math.abs(section.rewardX) === 13.5);
    previous = section;
  }
  assert.equal(kinds.size, 12); assert.equal(exits.size, 3);
  assert.ok(centerBlocked / challenged >= .4 && centerBlocked / challenged <= .6, `center blocked ${centerBlocked / challenged}`);
  assert.ok(transitions > 1500);
});

test('realm selection is seeded, weighted, query-order independent and avoids repeats', () => {
  const ctx = vm.createContext({}); vm.runInContext(extension, ctx);
  const run = code => vm.runInContext(code, ctx);
  const sequence = seed => run(`resetRunnerEvolution(${seed}); Array.from({length: 500}, (_, i) => runnerRealmForIndex(i))`);
  const a = sequence(271), b = sequence(272);
  assert.equal(new Set(a).size, 16); assert.equal(a[4], 'dream'); assert.notDeepEqual(a, b);
  assert.deepEqual(a, sequence(271));
  for (let i = 1; i < a.length; i++) assert.notEqual(a[i], a[i - 1]);
  run('resetRunnerEvolution(271); runnerRealmForIndex(499);');
  for (const i of [150, 2, 99, 40, 499]) assert.equal(run(`runnerRealmForIndex(${i})`), a[i]);
});

// Optional integration layer uses the exact browser Three.js build, avoiding a
// second physics implementation in the tests. See README for the pinned fetch.
const threeSource = process.env.THREE_SOURCE;
const integration = (name, fn) => test(name, { skip: !threeSource && 'Set THREE_SOURCE to the Three.js 0.160.0 browser build' }, fn);

function runtime() {
  const elements = new Map(), timers = new Map(); let timerId = 0;
  let now = 0;
  function element() {
    const listeners = new Map();
    return { style: {}, dataset: {}, classList: { toggle() {} }, textContent: '', innerHTML: '',
      addEventListener(type, fn) { const list = listeners.get(type) || []; list.push(fn); listeners.set(type, list); },
      dispatch(type, extra = {}) { for (const fn of listeners.get(type) || []) fn({preventDefault() {}, ...extra}); },
      listenerCount(type) { return (listeners.get(type) || []).length; },
      setPointerCapture() {}, hasPointerCapture() { return true; }, setAttribute() {},
      getBoundingClientRect() { return {left:0,top:0,width:120,height:120}; }
    };
  }
  const window = Object.assign(element(), { matchMedia: () => ({ matches: true }) });
  const document = Object.assign(element(), { getElementById(id) { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); }, querySelectorAll() { return []; } });
  const ctx = vm.createContext({ console: {...console, warn() {}}, window, document, navigator: {userAgent: 'Mobile'},
    performance: {now: () => now}, localStorage: {getItem() {return null;}, setItem() {}}, sessionStorage: {getItem() {return null;}, setItem() {}},
    setTimeout(fn) { timers.set(++timerId, fn); return timerId; }, clearTimeout(id) { timers.delete(id); },
    assert
  });
  vm.runInContext(fs.readFileSync(threeSource, 'utf8'), ctx);
  vm.runInContext(extension, ctx);
  vm.runInContext(main.replace('init();\nanimate();', ''), ctx);
  const run = source => vm.runInContext(source, ctx);
  run('scene = new THREE.Scene(); player = createPlayer(); players = [player]; activeRunnerPlayers = [player]; gameMode = 1;');
  return {run, document, window, advance(ms) {now += ms;}, flushTimers() {const pending=[...timers.values()];timers.clear();pending.forEach(fn=>fn());}};
}

integration('abilities spend correct energy; Parkour preserves terrain; Smash removes collision', () => {
  const {run} = runtime();
  run(`for (const [key, cost] of Object.entries({chris:30,dawit:30,jay:20,anthony:50,kevon:25})) {
    player.characterKey=key; player.energy=100; player.abilityCooldown=0; player.traversal=null; player.rig.position.set(0,0,0); distance=0;
    for(const wall of walls)disposeRunnerObject(wall); walls=[];
    if (key==='anthony'||key==='kevon') {const wall=createRunnerObstacle('construction',true);wall.position.set(0,0,4);scene.add(wall);registerObstacle(wall);}
    assert.equal(useRunnerAbility(player,player,new THREE.Vector3(0,0,1)),true);
    assert.equal(player.energy,100-cost);
    assert.equal(useRunnerAbility(player,player,new THREE.Vector3(0,0,1)),false);
    for(let i=0;i<60;i++)updateRunnerTraversal(player,player,1/60);
    assert.equal(player.traversal,null);assert.equal(player.rig.position.y,0);
    if(key==='anthony')assert.equal(walls.length,1);
    if(key==='kevon')assert.equal(walls.length,0);
    if(key==='jay')assert.ok(player.abilityShield>0);
  }`);
});

integration('abilities reject unaffordable or blocked moves and cannot tunnel through solid walls', () => {
  const {run} = runtime();
  run(`player.characterKey='anthony';player.energy=49;
    assert.equal(useRunnerAbility(player,player,new THREE.Vector3(0,0,1)),false);
    player.energy=100; const wall=createRunnerObstacle('city',true);wall.position.set(0,0,4);scene.add(wall);registerObstacle(wall);
    const solid=createRunnerObstacle('city',false);solid.position.set(0,0,7);scene.add(solid);registerObstacle(solid);
    assert.equal(useRunnerAbility(player,player,new THREE.Vector3(0,0,1)),false);assert.equal(player.energy,100);
    player.characterKey='dawit';assert.equal(useRunnerAbility(player,player,new THREE.Vector3(0,0,1)),true);
    for(let i=0;i<60;i++)updateRunnerTraversal(player,player,1/60);
    assert.ok(player.rig.position.z<2.4);
    player.characterKey='kevon';player.energy=100;player.abilityCooldown=0;walls=[solid];player.rig.position.set(0,0,3);
    assert.equal(useRunnerAbility(player,player,new THREE.Vector3(0,0,1)),false);assert.equal(player.energy,100);`);
});

integration('all runners can automatically jump optional low hurdles', () => {
  const {run} = runtime();
  run(`for(const key of Object.keys(CHARACTER_PROFILES)){
    player.characterKey=key;player.traversal=null;player.rig.position.set(0,0,2.2);player.velocity.set(0,0,12);
    const wall=createRunnerObstacle('rooftops',true);wall.position.set(0,0,4);wall.userData.autoVault=true;walls=[wall];
    assert.equal(tryRunnerAutoVault(player,player.rig.position.clone(),wall),true);
    for(let i=0;i<60;i++)updateRunnerTraversal(player,player,1/60);
    assert.equal(player.rig.position.z,6.1);assert.equal(player.energy,100);
  }`);
});

integration('touch tap, hold, cancellation and repeated setup do not duplicate abilities or stick sprint', () => {
  const r = runtime();r.run('gameStarted=true;gameOver=false;setupMobileControls();setupMobileControls();');
  const button = r.document.getElementById('mobileSprint');
  assert.equal(button.listenerCount('pointerdown'), 1);
  button.dispatch('pointerdown',{pointerId:1});r.advance(100);button.dispatch('pointerup',{pointerId:1});
  assert.equal(r.run('mobileDodgeQueued'),true);r.run('mobileDodgeQueued=false;');
  button.dispatch('pointerdown',{pointerId:2});r.advance(220);r.flushTimers();
  assert.equal(r.run('mobileSprintDown'),true);
  button.dispatch('pointerup',{pointerId:2});assert.equal(r.run('mobileSprintDown'),false);assert.equal(r.run('mobileDodgeQueued'),false);
  button.dispatch('pointerdown',{pointerId:3});button.dispatch('pointercancel');r.flushTimers();
  assert.equal(r.run('mobileSprintDown || mobileDodgeQueued'),false);
  button.dispatch('pointerdown',{pointerId:4});r.advance(220);r.flushTimers();r.window.dispatch('blur');
  assert.equal(r.run('mobileSprintDown || keys.shift'),false);
});

integration('enemy roles have distinct behavior and survive repeated updates', () => {
  const {run} = runtime();
  run(`player.rig.position.set(0,0,0);distance=1000;
    const ambush=configureRunnerEnemy(createEnemy(0,15),800);ambush.role='ambusher';
    updateRunnerEnemy(ambush,player,.3);assert.equal(ambush.rig.position.z,15);assert.ok(!ambush.activated);
    for(let i=0;i<5;i++)updateRunnerEnemy(ambush,player,.2);assert.equal(ambush.activated,true);
    const blocker=configureRunnerEnemy(createEnemy(6,18),300);blocker.role='blocker';updateRunnerEnemy(blocker,player,.2);
    assert.ok(blocker.rig.position.x<6);assert.equal(blocker.rig.position.z,18);
    const heavy=configureRunnerEnemy(createEnemy(0,10),800);heavy.role='heavy';heavy.roleCooldown=0;
    const wall=createRunnerObstacle('construction',true);wall.position.set(0,0,12);scene.add(wall);walls=[wall];updateRunnerEnemy(heavy,player,.1);assert.equal(walls.length,0);
    for(const role of ['chaser','fast','heavy','ambusher','blocker','jumper','ranged','elite']) {
      const enemy=configureRunnerEnemy(createEnemy(5,30),800);enemy.role=role;
      for(let i=0;i<120;i++)updateRunnerEnemy(enemy,player,1/60);
      assert.ok(enemy.rig.position.toArray().every(Number.isFinite));disposeRunnerObject(enemy.rig);
    }`);
});

integration('sixteen environments generate and resources stay bounded across a long run', () => {
  const {run} = runtime();
  run(`const seen=new Set();let peak=0;
    for(let i=0;i<300;i++){
      player.rig.position.set(0,0,i*42);distance=i*42;manageWorld();
      updateTempEffects(1);updateRunnerWorld(1);seen.add(getBiomeForZ(distance));peak=Math.max(peak,worldChunks.length);
      // Exercise all existing enemy cleanup paths, without allowing catches.
      for(const enemy of getThreatActors())if(enemy.rig.position.z<distance-70)disposeRunnerObject(enemy.rig);
      enemies=enemies.filter(canEnemyDamage);dogs=dogs.filter(canEnemyDamage);policeOfficers=policeOfficers.filter(canEnemyDamage);
      assert.ok(walls.every(w=>Number.isFinite(w.position.z)));
    }
    assert.equal(seen.size,16);assert.ok(peak<=13);assert.ok(tempEffects.length<=45);assert.ok(runnerRealmSituations.length<=13);
    const geometry=new THREE.BoxGeometry(1,1,1),material=new THREE.MeshBasicMaterial();let freed=0;
    geometry.addEventListener('dispose',()=>freed++);const group=new THREE.Group();group.add(new THREE.Mesh(geometry,material));scene.add(group);
    disposeRunnerObject(group);disposeRunnerObject(group);assert.equal(freed,1);assert.equal(group.parent,null);`);
});

integration('holding special fires once; exhausted sprint waits for recovery instead of speed flicker', () => {
  const {run} = runtime();
  run(`player.characterKey='chris';keys[' ']=true;
    const controls={forwardA:'w',sprint:'shift',dodge:' '};
    updatePlayerActor(player,.01,controls);assert.ok(player.abilityCooldown>0);
    player.abilityCooldown=0;const energyBefore=player.energy;updatePlayerActor(player,.01,controls);assert.ok(player.energy>=energyBefore);
    keys[' ']=false;keys.shift=true;keys.w=true;player.energy=0;updatePlayerActor(player,.01,controls);assert.equal(player.exhausted,true);
    for(let i=0;i<20;i++)updatePlayerActor(player,.01,controls);assert.equal(player.exhausted,true);
    player.energy=25;updatePlayerActor(player,.01,controls);assert.equal(player.exhausted,false);`);
});

integration('starting and replaying a run rebuilds the same seeded terrain and removes previous players', () => {
  const {run} = runtime();
  run(`camera=new THREE.PerspectiveCamera(70,1,.1,650);clock=new THREE.Clock();
    selectedCharacterKey='anthony';startGame(1);
    assert.equal(player.characterKey,'anthony');assert.equal(player.rig.position.x,0);
    const oldRig=player.rig,seed=replayRecorder.seed;
    const signature=()=>JSON.stringify(walls.map(w=>[w.position.x,w.position.z,w.userData.breakable]));
    const original=signature();replayMode=true;setReplaySeed(seed);startGame(1);
    assert.equal(signature(),original);assert.equal(oldRig.parent,null);assert.equal(players.length,1);
    assert.equal(player.energy,100);assert.equal(player.traversal,null);
    replayMode=false;startGame(2);assert.equal(players.length,2);
    assert.equal(players[0].rig.position.x,-2.8);assert.equal(players[1].rig.position.x,2.8);
    assert.ok(Number.isFinite(camera.position.z));`);
});

integration('realm hazards stay outside the full two-player corridor in every timed phase', () => {
  const {run} = runtime();
  run(`resetRunnerEvolution(813);let situations=0,dreamCount=0;
    for(let i=0;i<96;i++)generateChunk(i*CHUNK_LENGTH);
    for(const s of runnerRealmSituations){
      situations++;const plan=s.owner.userData.plan;
      assert.ok(runnerRouteClear(plan,s.obstacle.position.x,s.obstacle.position.z-s.owner.userData.zStart));
    }
    for(const h of dreamHazards){
      dreamCount++; const owner=worldChunks.find(c=>h.group.position.z>=c.userData.zStart&&h.group.position.z<c.userData.zStart+42);
      assert.ok(runnerRouteClear(owner.userData.plan,h.group.position.x,h.group.position.z-owner.userData.zStart,h.radius,h.radius));
    }
    assert.ok(situations>=12);assert.ok(dreamCount>0);
    for(let frame=0;frame<330;frame++){
      updateRunnerWorld(1/60);
      for(const s of runnerRealmSituations) {
        const plan=s.owner.userData.plan, localZ=s.obstacle.position.z-s.owner.userData.zStart;
        const center=runnerRouteX(plan,localZ);
        for(const dx of [-1.3,1.3]) assert.ok(!getCollidingWall(new THREE.Vector3(center+dx,0,s.obstacle.position.z),1));
      }
    }
    const gate=runnerRealmSituations.find(s=>s.biome==='clockwork');
    runnerWorldTime=4-gate.phase;updateRunnerWorld(.01);assert.equal(gate.obstacle.userData.runnerOpen,true);
    player.rig.position.copy(gate.obstacle.position);runnerWorldTime=-gate.phase;
    updateRunnerWorld(.01);assert.equal(gate.obstacle.userData.runnerOpen,true);
    assert.equal(getCollidingWall(player.rig.position,1),null);
    player.rig.position.x+=5;for(let frame=0;frame<60;frame++)updateRunnerWorld(1/60);assert.equal(gate.obstacle.userData.runnerOpen,false);
    const old=runnerRealmSituations[0];disposeRunnerObject(old.owner);updateRunnerWorld(.1);
    assert.ok(!runnerRealmSituations.includes(old));`);
});

integration('enemy silhouettes have articulated rigs, equipment and variation; spirit outlines persist on mobile', () => {
  const {run} = runtime();
  run(`const signatures=new Set();
    for(const role of ['chaser','fast','heavy','ambusher','blocker','jumper','ranged','elite']) {
      const enemy=configureRunnerEnemy(createEnemy(0,800),800,role);
      assert.equal(enemy.rig.userData.role,role);assert.ok(enemy.parts.head&&enemy.parts.torso);
      for(const limb of ['leftLeg','rightLeg','leftArm','rightArm'])assert.ok(enemy.parts[limb+'Lower']);
      const geometry=[];enemy.rig.traverse(o=>{if(o.isMesh)geometry.push([o.geometry.type,o.position.toArray(),o.scale.toArray(),o.isInstancedMesh?Array.from(o.instanceMatrix.array):null]);});
      signatures.add(JSON.stringify([enemy.rig.scale.toArray(),geometry]));
      if(role==='ranged'||role==='elite')assert.ok(enemy.handTorch);
      disposeRunnerObject(enemy.rig);
    }
    assert.equal(signatures.size,8);
    for(const mobile of [false,true]) {
      isMobileDevice=mobile;const dog=createSpiritDog(0,10);let outlines=0;
      dog.rig.traverse(o=>{if(o.userData.spiritOutline){outlines++;assert.equal(o.material.depthTest,true);assert.equal(o.material.fog,false);assert.equal(o.material.side,THREE.BackSide);}});
      assert.ok(outlines>=11);assert.equal(dog.body.material.opacity,1);assert.equal(dog.body.material.transparent,false);
      assert.ok(dog.head.children.some(o=>o.userData.spiritOutline));assert.ok(dog.tail.children.some(o=>o.userData.spiritOutline));
      disposeRunnerObject(dog.rig);
    }`);
});

integration('animation follows movement, eases turns, blends exhaustion and preserves jumps', () => {
  const {run} = runtime();
  run(`animateRunner(player,1/60,false,false);const initial=player.animTime;
    for(let i=0;i<60;i++){player.rig.position.z+=13/60;animateRunner(player,1/60,true,false);}
    assert.ok(player.runnerMotion.speed>12.9);assert.equal(player.runnerMotion.mode,'running');assert.ok(player.animTime>initial+10);
    runnerTurn(player,Math.PI/2);animateRunner(player,1/60,true,false);
    assert.ok(player.rig.rotation.y>0&&player.rig.rotation.y<Math.PI/2);
    player.exhausted=true;animateRunner(player,1/60,true,false);assert.equal(player.runnerMotion.mode,'exhausted');
    player.exhausted=false;player.traversal={start:player.rig.position.clone(),end:player.rig.position.clone().add(new THREE.Vector3(0,0,7)),elapsed:0,duration:.42,height:3.6};
    animateRunner(player,.1,true,false);updateRunnerTraversal(player,player,.1);assert.ok(player.rig.position.y>1);
    for(let i=0;i<30;i++){animateRunner(player,1/60,true,false);updateRunnerTraversal(player,player,1/60);}
    animateRunner(player,1/60,false,false);assert.ok(['landing','jogging'].includes(player.runnerMotion.mode));
    assert.ok(player.parts.leftLegLower.rotation.x>=0);assert.ok(player.rig.position.toArray().every(Number.isFinite));
    const dog=createDog(2,10);for(let i=0;i<60;i++){dog.rig.position.z+=16/60;animateDog(dog,1/60,true);}
    assert.ok(dog.runnerMotion.speed>15.9);assert.notEqual(dog.legs[0].rotation.x,dog.legs[2].rotation.x);
    disposeRunnerObject(dog.rig);`);
});

integration('two-player chase conversion, movement, tag swapping and cleanup remain operational', () => {
  const {run} = runtime();
  run(`camera=new THREE.PerspectiveCamera(70,1,.1,650);clock=new THREE.Clock();startGame(2);
    const p1=players[0],p2=players[1];convertPlayerToControlledChaser(p2,'test');
    assert.equal(p2.isChaser,true);assert.equal(p2.rig.visible,false);assert.ok(p2.controlledChaser.parts.leftLegLower);
    const oldRig=p2.controlledChaser.rig;let freed=0;oldRig.traverse(o=>{if(o.geometry)o.geometry.addEventListener('dispose',()=>freed++);});
    keys.i=true;updatePlayerActor(p2,.1,{forwardA:'i',sprint:'enter',dodge:'o'});keys.i=false;
    assert.ok(p2.controlledChaser.rig.position.toArray().every(Number.isFinite));
    swapChaserAndRunner(p2,p1);assert.equal(p2.isChaser,false);assert.equal(p1.isChaser,true);
    assert.equal(oldRig.parent,null);assert.ok(freed>0);
    startNextTwoPlayerRound();assert.ok(players.every(p=>!p.isChaser&&p.rig.visible));assert.equal(players.length,2);`);
});
