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

test('10,000 sections preserve a connected player-sized route across boundaries', () => {
  const ctx = vm.createContext({}); vm.runInContext(extension, ctx);
  const plan = vm.runInContext('planRunnerSection', ctx);
  let seed = 91723; const random = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
  const kinds = new Set();
  for (let i = 0; i < 10000; i++) {
    const section = plan(i, random); kinds.add(section.kind);
    const points = [{z: 0, center: 0}, ...section.rows, {z: 42, center: 0}];
    assert.equal(section.rows[0].center, 0); assert.equal(section.rows.at(-1).center, 0);
    for (let j = 1; j < points.length; j++) {
      const a = points[j - 1], b = points[j];
      // Kevon's lateral speed / maximum sprint speed still permits this turn.
      assert.ok(Math.abs(b.center - a.center) / (b.z - a.z) < .5);
      for (let z = a.z; z <= b.z; z += .2) {
        const x = a.center + (b.center - a.center) * (z - a.z) / (b.z - a.z);
        for (const row of section.rows) for (const block of row.blocks) {
          assert.ok(Math.abs(x - block.x) >= 3.2 || Math.abs(z - block.z) >= 1.7, `blocked ${i}/${z}`);
        }
      }
    }
    if (section.recovery) assert.equal(section.rows.flatMap(r => r.blocks).length, 0);
    if (['parkour', 'shortcut'].includes(section.kind)) assert.ok(section.rows.every(r => r.blocks.some(b => b.x === 9 && b.breakable)));
  }
  assert.equal(kinds.size, 6);
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

integration('ten environments generate and resources stay bounded across a long run', () => {
  const {run} = runtime();
  run(`const seen=new Set();let peak=0;
    for(let i=0;i<300;i++){
      player.rig.position.set(0,0,i*42);distance=i*42;manageWorld();
      updateTempEffects(1);seen.add(getBiomeForZ(distance));peak=Math.max(peak,worldChunks.length);
      // Exercise all existing enemy cleanup paths, without allowing catches.
      for(const enemy of getThreatActors())if(enemy.rig.position.z<distance-70)disposeRunnerObject(enemy.rig);
      enemies=enemies.filter(canEnemyDamage);dogs=dogs.filter(canEnemyDamage);policeOfficers=policeOfficers.filter(canEnemyDamage);
      assert.ok(walls.every(w=>Number.isFinite(w.position.z)));
    }
    assert.equal(seen.size,10);assert.ok(peak<=13);assert.ok(tempEffects.length<=45);
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
