'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const C = require('../game/neon-arena/neon-campaign.js'), Q = require('../game/neon-arena/neon-conquest.js'), M = require('../game/neon-arena/neon-war-maps.js'), F = require('../game/neon-arena/neon-factions.js'), AI = require('../game/neon-arena/neon-tactical-ai.js');
const source = fs.readFileSync(path.resolve(__dirname, '../game/neon-arena/neon-arena.js'), 'utf8');
function fn(name) { const start = source.indexOf('function ' + name + '('), end = source.indexOf('\nfunction ', start + 1); assert.ok(start > 0 && end > start, name); return source.slice(start, end); }
const actor = (team = 2, human = false) => ({ entityId: human ? 'p1' : 'enemy', x: 0, y: 1.86, z: 0, team, human, hp: 100, maxHp: 100, armor: 50, maxArmor: 100, r: .5, vx: 0, vz: 0, yaw: 0, pitch: 0, dead: false, weapon: 'burrower' });
function harness(options = {}) {
  const p = actor(options.team || 2, true), selection = { faction: p.team === 1 ? 'cats' : 'mice', character: p.team === 1 ? 'cat_assault' : 'mouse_rifleman', weapon: 'burrower', mission: options.mission || 'mice_broken_convoy', budget: options.budget || 8, base: options.base || 4, perFlag: 1, resume: true };
  let progress = options.progress || C.freshProgress();
  const floor = { x: 0, y: -.5, z: 0, w: 200, d: 200, h: 1, kind: 'platform', structure: { active: true } }, blocks = [floor], notices = [];
  const context = vm.createContext({ Math, Number, Set, Map, console, NeonCampaign: C, NeonConquest: Q, PLAYER_H: 1.86, WORLD_SCALE: 1.45, ARENA_SAFE: 65,
    game: { mode: options.mode || 'conquest', started: true, over: false, difficulty: 'normal', p1Input: 'keyboard', botCount: 6, respawnDelay: 1 },
    player: p, player2: options.second ? { ...actor(3 - p.team, true), entityId: 'p2' } : null, bots: [], conquest: null, campaignRun: null, warClock: 0, warActorId: 0, warEscort: null, warInteract: false, warSightingCache: new Map(),
    warMap: M.worldMetadata(C.mission(selection.mission)?.mapId || 'cold_storage'), nav: [{ x: 0, y: 1.86, z: 0 }, { x: 8, y: 1.86, z: 8 }], edgeSpawns: [], keys: {}, pickups: [{ kind: 'weapon', type: 'assault' }, { kind: 'health' }],
    WEAPONS: Object.fromEntries([...F.weapons.map(w => w.id), 'assault', 'sniper', 'pistol'].map(id => [id, { name: id }])),
    window: { NeonFactions: F, NeonWarUI: { selection: () => selection, refreshProgress() {} } },
    survivalProgress: { getCampaign: () => JSON.parse(JSON.stringify(progress)), saveCampaign: value => { progress = C.sanitizeProgress(value); return progress; } },
    el: () => null, feed: value => notices.push(value), endMatch: reason => { context.game.over = true; notices.push(reason); },
    humanBotController: () => ({ hear() {} }), tacticalController: { reset() {} }, applyBotPersonality: a => { a.tacticalProfile = a.style; },
    equip: (a, weapon) => { a.weapon = weapon; }, resetWeaponInventory: (a, weapon) => { a.weapon = weapon; a.weaponSlots = [weapon, null]; },
    safeSpawnCandidate: (a, x, z, feet) => ({ x, z, y: feet + 1.86 }), chooseSafeSpawn: () => ({ x: 0, y: 1.86, z: 0 }),
    resetHumanJump() {}, pushOutXZ() {}, addPickup() {}, burst() {}, pick: list => list[0], rand: (a, b) => (a + b) / 2,
    clamp: (n, min, max) => Math.max(min, Math.min(n, max)), blocksAlongRay: () => blocks.filter(b => b.structure?.active !== false),
    blocksNear: () => blocks.filter(b => b.structure?.active !== false), arenaRampHeight: () => NaN
  });
  const warStart = source.indexOf('function warMode('), warEnd = source.indexOf('function warShot(', warStart);
  vm.runInContext(source.slice(warStart, warEnd) + ['spawn', 'botSeparation', 'rayBox', 'rayWorld', 'lineClear', 'actorOverSurface', 'actorStandingHeight', 'actorEyePosition', 'tacticalPointInfo', 'tacticalCanTraverse'].map(fn).join('\n'), context);
  return { c: context, p, blocks, selection, notices, progress: () => progress, tick(dt = .1) { context.warClock += dt; context.updateWar(dt); } };
}

test('real Conquest startup maps UI budget to its simulation and both factions retain correct models', () => {
  for (const budget of [8, 12, 16]) {
    const { c } = harness({ budget, base: 6, second: true }); c.initWarMatch();
    assert.equal(c.conquest.config.botBudget, budget);
    assert.ok(c.bots.filter(a => !a.human && !a.dead).length <= budget);
    assert.equal(c.player.faction, 'mice'); assert.equal(c.player2.faction, 'cats');
    for (const a of c.warActors()) assert.equal(F.characters.find(ch => ch.id === a.factionCharacter).faction, a.team === 1 ? 'cats' : 'mice');
    assert.equal(new Set(c.warActors()).size, c.warActors().length, 'split-screen player is counted once');
  }
});

test('TDM commits to faction-oriented ground until a friendly majority arrives instead of advancing on the clock alone', () => {
  for(const team of [1,2]){
    const {c}=harness({mode:'tdm',team});
    c.warMap.sectors=[{id:'A',x:-20,y:1.86,z:0},{id:'B',x:0,y:1.86,z:0},{id:'C',x:20,y:1.86,z:0}];
    const friendly=Array.from({length:4},(_,i)=>({...actor(team),entityId:'friend-'+i,x:team===1?-50:50,z:i}));
    c.bots=[...friendly,{...actor(3-team),x:team===1?-20:20}];
    const first=c.warObjective(friendly[1]);assert.equal(first.x,team===1?-20:20);assert.equal(first.strategic,true);
    c.warClock=120;assert.equal(c.warObjective(friendly[1]).x,first.x,'neither elapsed time nor an enemy at the marker advances the friendly plan');
    Object.assign(friendly[0],{x:first.x,z:0});assert.equal(c.warObjective(friendly[1]).x,first.x,'one isolated soldier cannot pull the whole squad onward');
    for(const a of friendly.slice(1,3))Object.assign(a,{x:first.x,z:0});
    const forward=c.warObjective(friendly[1]);assert.equal(forward.x,0);assert.equal(forward.intent,'capture');
    const rear=c.warObjective(friendly[0]);assert.equal(rear.x,first.x);assert.equal(rear.intent,'hold');
    const yaw=forward.approachYaw;friendly[1].x+=3;friendly[1].z+=4;
    assert.equal(c.warObjective(friendly[1]).approachYaw,yaw,'approach bearing is anchored to territory, not recomputed from a moving soldier');
  }
});

test('TDM arrival requires the objective floor and a clear route to the marker', () => {
  const {c,blocks}=harness({mode:'tdm',team:1});
  c.warMap.sectors=[{id:'A',x:10,y:1.86,z:0},{id:'B',x:30,y:1.86,z:0}];
  c.bots=Array.from({length:4},(_,i)=>({...actor(1),entityId:'friend-'+i,x:6,y:5.86}));
  c.warObjective(c.bots[1]);c.warClock=60;
  assert.equal(c.warObjective(c.bots[1]).x,10,'an upper floor cannot count as occupied ground');
  for(const a of c.bots)a.y=1.86;
  blocks.push({x:8,y:1.5,z:0,w:.3,h:3,d:8,kind:'wall',structure:{active:true}});
  assert.equal(c.warObjective(c.bots[1]).x,10,'a squad behind the barrier has not reached the marker');
  blocks.at(-1).structure.active=false;assert.equal(c.warObjective(c.bots[1]).x,30);
});

test('small TDM squads keep advancing instead of reserving the only needed attacker', () => {
  for(const count of [1,2]){
    const {c}=harness({mode:'tdm',team:1});c.warMap.sectors=[{id:'A',x:0,y:1.86,z:0},{id:'B',x:30,y:1.86,z:0}];
    c.bots=Array.from({length:count},(_,i)=>({...actor(1),entityId:'friend-'+i}));
    c.warObjective(c.bots[0]);c.warClock=10;
    for(const a of c.bots){const order=c.warObjective(a);assert.equal(order.x,30);assert.equal(order.intent,'capture');}
  }
});

test('Conquest bot casualties cost a ticket and wait for a wave before reuse', () => {
  const h = harness(), c = h.c; c.initWarMatch(); const victim = c.bots.find(a => a.team === 1), size = c.bots.length;
  victim.dead = true; c.warCasualty(victim, c.player); assert.equal(c.conquest.tickets[1], 249);
  h.tick(1); assert.equal(victim.dead, true); assert.equal(c.bots.length, size);
  for (let i = 0; i < 15; i++) h.tick(1);
  assert.equal(victim.dead, false); assert.equal(c.bots.length, size, 'existing bot slot is reused');
});

test('losing sectors lowers future replacements without removing excess living troops', () => {
  const h = harness({ budget: 16 }), c = h.c; c.initWarMatch();
  for (const s of c.conquest.sectors) s.owner = 1;
  c.conquest.nextWave = .01; h.tick(.1); const survivors = c.bots.filter(a => a.team === 1 && !a.dead);
  assert.equal(survivors.length, 9);
  for (const s of c.conquest.sectors) s.owner = 0;
  c.conquest.nextWave = .01; h.tick(.1);
  assert.ok(survivors.every(a => !a.dead && c.bots.includes(a))); assert.equal(c.bots.filter(a => a.team === 1 && !a.dead).length, survivors.length);
});

test('visible or close enemies make a forward sector unusable as a spawn', () => {
  const h = harness({ team: 1 }), c = h.c; c.initWarMatch(); const forward = c.conquest.sectors[0]; forward.owner = 1;
  const enemy = c.bots.find(a => a.team === 2); Object.assign(enemy, { x: forward.x + 2, y: forward.y, z: forward.z });
  const destination = c.chooseWarSpawn(actor(1));
  assert.ok(Math.hypot(destination.x - forward.x, destination.z - forward.z) > 16);
});

test('every authored battlefield can spawn sixteen bots and both human configurations without overlapping occupied positions', () => {
  for (const mapId of M.IDS) for (const second of [false, true]) {
    const h = harness({ budget: 16, base: 8, second }), c = h.c;
    c.warMap = M.worldMetadata(mapId);
    c.chooseSafeSpawn = () => { assert.fail(mapId + ': authored HQ candidates must accommodate the roster'); };
    c.initWarMatch();
    c.conquest.sectors[0].owner = 2;
    if (second) c.conquest.sectors[1].owner = 1;
    c.conquest.nextWave = .01; h.tick(.1);
    assert.equal(c.bots.filter(a => !a.human && !a.dead).length, 16, mapId + ': maximum active bot roster');
    const active = c.warActors().filter(a => !a.dead);
    for (let i = 0; i < active.length; i++) for (let j = i + 1; j < active.length; j++) {
      const a = active[i], b = active[j];
      if (Math.abs(a.y - b.y) < 2.5) assert.ok(Math.hypot(a.x - b.x, a.z - b.z) >= 3.1,
        mapId + ': ' + a.entityId + ' and ' + b.entityId + ' must start clear of each other');
    }
  }
});

test('war bots strongly avoid both nearby human players and count split-screen players once', () => {
  const { c } = harness({ second: true }), bot = actor(2);
  Object.assign(bot, { x: 1, z: 0 }); Object.assign(c.player2, { x: 1, z: -1 });
  c.bots = [bot, c.player2];
  for (const mode of ['tdm', 'conquest', 'campaign']) {
    c.game.mode = mode;
    const avoidance = c.botSeparation(bot);
    assert.ok(avoidance.x > 2 && avoidance.z > 2, mode + ': both humans cause strong outward movement');
    assert.equal(avoidance.x, avoidance.z, 'player two is not counted twice through the bot list');
  }
  c.player2.dead = true;
  assert.equal(c.botSeparation(bot).z, 0, 'a downed player does not repel living soldiers');
  const humanPush = c.botSeparation(bot).x; c.player.human = false;
  assert.equal(humanPush, c.botSeparation(bot).x * 4, 'human clearance gets priority over ordinary squad spacing');
});

test('war player avoidance respects separate floors and preserves the original solo-arena behavior', () => {
  const { c } = harness(), bot = { ...actor(2), x: 1 }; c.bots = [bot];
  assert.ok(c.botSeparation(bot).x > 0);
  c.player.y += 3;
  assert.equal(c.botSeparation(bot).x, 0, 'a player on the floor above cannot push soldiers sideways');
  c.player.y -= 6;
  assert.equal(c.botSeparation(bot).x, 0, 'a player on the floor below cannot push soldiers sideways');
  c.player.y = bot.y; c.game.mode = 'dm';
  assert.equal(c.botSeparation(bot).x, 0, 'legacy arena separation remains limited to its existing bot neighbors');
});

test('marksman and heavy guns lose more relative moving accuracy than the SMG while stationary spread stays unchanged', () => {
  const weapons = Object.fromEntries(F.weapons.map(w => [w.id, w])); weapons.assault = { spread: .018 };
  const c = vm.createContext({ Math, WEAPONS: weapons, rand: (min, max) => max }); vm.runInContext(fn('dirFrom'), c);
  const angle = (weapon, speed) => {
    const direction = c.dirFrom({ ...actor(), weapon, vx: speed, onGround: true }, weapons[weapon].spread);
    return Math.atan2(direction.x, direction.z);
  };
  for (const weapon of Object.keys(weapons)) assert.ok(Math.abs(angle(weapon, 0) - weapons[weapon].spread) < 1e-12,
    weapon + ': standing still adds no moving penalty');
  const relative = weapon => angle(weapon, 10) / angle(weapon, 0);
  assert.ok(relative('squeakshot') > relative('purr90'), 'the precision rifle is more sensitive to movement than the SMG');
  assert.ok(relative('thunderpaw') > relative('squeakshot'), 'the heavy machine gun is the least stable while moving');
  assert.ok(relative('purr90') > 1, 'the SMG still has a moving penalty');
  assert.equal(angle('assault', 10), angle('assault', 0), 'existing arena guns retain their original moving spread');
});

test('authored recoil recovery gives the SMG faster stabilization than marksman and heavy guns and preserves legacy defaults', () => {
  const weapons = Object.fromEntries(F.weapons.map(w => [w.id, w])); weapons.assault = { kind: 'bullet' };
  const c = vm.createContext({ Math, WEAPONS: weapons }); vm.runInContext(fn('updateWeaponTimers'), c);
  const settled = weapon => {
    const a = { ...actor(), weapon, reload: 0, cool: 0, warRecoilPitch: .04, warRecoilYaw: .02 };
    for (let i = 0; i < 10; i++) c.updateWeaponTimers(a, .1);
    assert.ok(a.warRecoilPitch > 0 && a.warRecoilPitch < .04, weapon + ': recoil decays without overshoot');
    assert.ok(Math.abs(a.warRecoilPitch - a.warRecoilYaw * 2) < 1e-12, 'both recoil axes recover consistently');
    return a.warRecoilPitch;
  };
  assert.ok(settled('purr90') < settled('squeakshot'), 'SMG stabilization is faster than the marksman rifle');
  assert.ok(settled('squeakshot') < settled('thunderpaw'), 'the heavy machine gun retains recoil longer');
  assert.ok(Math.abs(settled('assault') - .04 * Math.exp(-7)) < 1e-12, 'original guns keep the existing recovery rate');
});

test('transitioning into defense sends the queued counterattack to the new objective even with alarm disabled', () => {
  const h = harness({ mode: 'campaign', mission: 'cats_foundry_offensive', team: 1 }), c = h.c; c.initWarMatch(); c.bots = [];
  c.campaignRun.index = 1; c.campaignRun.progress = 3.95; c.campaignRun.alarm.enabled = false;
  Object.assign(c.player, c.warMap.anchors.vault); c.keys.KeyF = true; h.tick(.1);
  assert.equal(C.currentObjective(c.campaignRun).type, 'defend'); assert.ok(c.bots.length > 0);
  for (const guard of c.bots) {
    assert.ok(guard.assaultOrder, 'counterattack must have an objective order');
    assert.equal(guard.assaultOrder.x, c.warMap.anchors.defend.x); assert.equal(guard.assaultOrder.z, c.warMap.anchors.defend.z);
  }
});

test('a full campaign roster redirects existing guards for counterattack without exceeding the active cap', () => {
  const h = harness({ mode: 'campaign', mission: 'cats_foundry_offensive', team: 1 }), c = h.c; c.initWarMatch();
  while (c.bots.length < 16) c.newWarBot(2, c.bots.length);
  c.campaignRun.index = 1; c.campaignRun.progress = 3.95; c.campaignRun.alarm.enabled = false;
  Object.assign(c.player, c.warMap.anchors.vault); c.keys.KeyF = true; h.tick(.1);
  assert.equal(C.currentObjective(c.campaignRun).type, 'defend'); assert.equal(c.bots.filter(a => !a.dead).length, 16);
  const assault = c.bots.filter(a => a.assaultOrder); assert.ok(assault.length >= 4, 'the full roster must still launch a coordinated counterattack');
  for (const guard of assault) assert.equal(guard.assaultOrder.x, c.warMap.anchors.defend.x);
});

test('alarm squad orders use frozen evidence, expire, and never follow the hidden player', () => {
  const h = harness({ mode: 'campaign' }), c = h.c; c.initWarMatch(); const guard = c.bots[0];
  c.warDetection(guard, { confirmed: true, kind: 'gunfire', position: { x: 4, y: 1.86, z: 9, uncertainty: 3.5, id: 'shot' } });
  const order = c.warObjective(guard); c.player.x = 500; c.player.z = -500;
  const still = c.warObjective(guard); assert.equal(still.x, order.x); assert.equal(still.z, order.z); assert.equal(still.uncertainty, 3.5);
  c.campaignRun.elapsed = 11; assert.equal(c.warObjective(guard), null);
  assert.equal(c.warSightingCache.get(guard.team + ':shot').x, 4);
  c.warClock = 6; h.tick(.1); assert.equal(c.warSightingCache.size, 0);
});

test('extended searches discard both local memory and stale alarm orders while hidden targets keep moving', () => {
  const h = harness({ mode: 'campaign' }), c = h.c; c.initWarMatch(); const guard = c.bots[0], states = [];
  Object.assign(guard, { x: 0, z: 0, yaw: 0, faceYaw: 0 }); Object.assign(c.player, { x: 0, z: 20 });
  const controller = AI.createController({ time: () => c.campaignRun.elapsed, random: () => .5, actors: () => [guard, c.player], sameTeam: (a, b) => a.team === b.team,
    lineClear: () => false, pointInfo: () => ({ walkable: true }), canTraverse: () => true, navigation: () => c.nav, patrolRoute: () => [{ x: -12, y: 1.86, z: -12 }],
    objective: a => c.warObjective(a), onDetection: (a, event) => c.warDetection(a, event), onStateChange: (a, state) => states.push(state) });
  controller.hear({ x: 0, y: 1.86, z: 8, kind: 'gunfire', range: 48, team: c.player.team });
  let decision = controller.update(guard, .1); assert.ok(decision.memory); assert.equal(decision.enemy, null);
  const evidence = JSON.stringify(c.campaignRun.alarm.lastKnown);
  c.campaignRun.alarm.heat = 3; c.campaignRun.alarm.level = 3;
  for (let i = 0; i < 450; i++) {
    c.player.x = 100 + i; c.player.z = -100 - i; h.tick(.1); decision = controller.update(guard, .1);
    assert.equal(decision.enemy, null); assert.equal(decision.aim, null);
    assert.equal(JSON.stringify(c.campaignRun.alarm.lastKnown), evidence, 'search does not replace evidence with the live hidden player');
    if (c.campaignRun.elapsed > 11) { assert.equal(c.warObjective(guard), null); assert.equal(decision.memory, null); }
  }
  assert.ok(states.includes('investigate')); assert.ok(states.includes('search')); assert.ok(states.includes('regroup')); assert.equal(decision.guardState, 'patrol');
  assert.equal(decision.goal.x, -12); assert.equal(decision.goal.z, -12);
});

test('the legacy minimap cannot expose nearby, behind-wall or rearward enemies in any war mode', () => {
  let liveVisibilityQueries = 0;
  const c = vm.createContext({ game: { mode: 'campaign' }, player: actor(2, true), hypot2: Math.hypot, visibleToPlayer: () => { liveVisibilityQueries++; return true; } });
  vm.runInContext(fn('visibleOnMinimap'), c);
  for (const mode of ['tdm', 'conquest', 'campaign']) {
    c.game.mode = mode;
    for (const [x, z] of [[0, .5], [0, -2], [40, 40]]) assert.equal(c.visibleOnMinimap({ ...actor(1), x, z }), false, mode + ': no live enemy coordinates');
    assert.equal(c.visibleOnMinimap({ ...actor(2), x: 35, z: -25 }), true, 'friendly positions remain available');
  }
  assert.equal(liveVisibilityQueries, 0, 'legacy war radar does not independently scan enemies');
  c.game.mode = 'dm'; assert.equal(c.visibleOnMinimap(actor(1)), true); assert.equal(liveVisibilityQueries, 1, 'the original arena radar behavior is preserved');
});

test('mission interactions and optional relay require the same floor and an actual clear physical ray', () => {
  const h = harness({ mode: 'campaign' }), c = h.c; c.initWarMatch(); c.campaignRun.index = 1;
  c.warMap.anchors.intel = { x: 0, y: 1.86, z: 3 }; c.warMap.anchors.security = { x: 0, y: 1.86, z: 3 }; c.warMap.missionSites = {};
  Object.assign(c.player, { x: 0, y: 1.86, z: 0 });
  assert.equal(c.campaignInteractionAvailable(), true); assert.equal(c.campaignOptionalAvailable(), true);
  h.blocks.push({ x: 0, y: 1.5, z: 1.5, w: 4, h: 3, d: .2, kind: 'wall', structure: { active: true } });
  assert.equal(c.campaignInteractionAvailable(), false); assert.equal(c.campaignOptionalAvailable(), false);
  h.blocks.at(-1).structure.active = false; c.player.y = 5;
  assert.equal(c.campaignInteractionAvailable(), false); assert.equal(c.campaignOptionalAvailable(), false);
});

test('crouching behind low physical cover blocks interaction from the lowered eye', () => {
  const h = harness({ mode: 'campaign' }), c = h.c; c.initWarMatch(); c.campaignRun.index = 1;
  c.warMap.anchors.intel = { x: 0, y: 1.86, z: 3 }; c.warMap.anchors.security = { x: 0, y: 1.86, z: 3 }; c.warMap.missionSites = {};
  Object.assign(c.player, { x: 0, y: 1.86, z: 0 });
  h.blocks.push({ x: 0, y: .75, z: 1, w: 4, h: 1.5, d: .2, kind: 'wall', structure: { active: true } });
  assert.equal(c.campaignInteractionAvailable(), true); assert.equal(c.campaignOptionalAvailable(), true);
  c.player.crouching = true; c.player.crouchPose = 1;
  assert.equal(c.campaignInteractionAvailable(), false); assert.equal(c.campaignOptionalAvailable(), false);
  const index = c.campaignRun.index; c.keys.KeyF = true;
  for (let i = 0; i < 40; i++) h.tick(.1);
  assert.equal(c.campaignRun.index, index); assert.equal(c.campaignRun.alarm.enabled, true);
});

test('physical window gaps pass rays while frames and complete standing capsule still block passage', () => {
  const h = harness({ mode: 'campaign' }), c = h.c;
  h.blocks.push({ x: -1.1, y: 1.5, z: 1.5, w: 1.8, h: 3, d: .2, kind: 'wall', structure: { active: true } }, { x: 1.1, y: 1.5, z: 1.5, w: 1.8, h: 3, d: .2, kind: 'wall', structure: { active: true } });
  assert.equal(c.lineClear(0, 1.8, 0, 0, 1.8, 3), true);
  assert.equal(c.lineClear(.5, 1.8, 0, .5, 1.8, 3), false);
  assert.equal(c.tacticalCanTraverse(actor(1), { x: 0, y: 1.86, z: 3 }, actor(1)), false, 'a ray-sized opening cannot admit a full soldier');
});

test('checkpoint restoration restores exact zero yaw, gear and health while clearing stale posture and recoil', () => {
  const run = C.createRun('mice_broken_convoy'); run.index = 1;
  const saved = C.saveCheckpointProgress(C.freshProgress(), run, { ...actor(2, true), x: 12, z: 15, yaw: 0, hp: 47, armor: 22, weapon: 'whisker' });
  const h = harness({ mode: 'campaign', progress: saved }), c = h.c;
  Object.assign(c.player, { crouching: true, crouchPose: 1, aiming: true, aimPose: 1, warRecoilYaw: .04, warShotIndex: 12 }); c.initWarMatch();
  assert.equal(c.campaignRun.index, 1); assert.equal(c.player.x, 12); assert.equal(c.player.z, 15); assert.equal(c.player.yaw, 0);
  assert.equal(c.player.hp, 47); assert.equal(c.player.armor, 22); assert.equal(c.player.weapon, 'whisker');
  assert.equal(c.player.crouching, false); assert.equal(c.player.crouchPose, 0); assert.equal(c.player.aiming, false); assert.equal(c.player.warShotIndex, 0);
});

test('resuming a rescue checkpoint restores the escort beside the player rather than back in the prison', () => {
  const run = C.createRun('mice_whiskerhold_rescue'); run.index = 3; run.escort.rescued = true; run.escort.delivered = true;
  const saved = C.saveCheckpointProgress(C.freshProgress(), run, { ...actor(2, true), x: 10, z: 11 });
  const h = harness({ mode: 'campaign', mission: run.missionId, progress: saved }), c = h.c; c.initWarMatch(); h.tick(.1);
  assert.ok(c.warEscort); assert.equal(c.warEscort.team, c.player.team); assert.ok(Math.hypot(c.warEscort.x - c.player.x, c.warEscort.z - c.player.z) <= 3);
});
