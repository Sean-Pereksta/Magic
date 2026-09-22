import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const campaign = fs.readFileSync(new URL('./campaign.js', import.meta.url), 'utf8');
const intensity = fs.readFileSync(new URL('./intensity-combat.js', import.meta.url), 'utf8');
function section(source, start, end) {
  const a = source.indexOf(start), b = source.indexOf(end, a + start.length);
  assert.ok(a >= 0 && b > a, `Missing runtime section: ${start}`);
  return source.slice(a, b);
}
// Execute the production scheduler and clear guard, not a second implementation.
const waveRuntime = section(intensity, "'use strict';", 'function intensityQueueHazard') +
  section(intensity, 'function intensityTickEncounter(', 'function intensityTickHazards(');

function harness({type = 'danger', threat = 3, continent = 'verdant', room = 0, event} = {}) {
  const continents = ['verdant', 'meridian', 'gloam'].map(id => ({id, name: id, color: '#abc', material: 'dust'}));
  const node = {id: 'test-node', name: 'Test node', index: 1, continent, type, threat, event,
    biome: 'forest', roomCount: type === 'dungeon' ? 5 : 1, connections: [], exits: {}};
  if (type === 'town') node.town = 'test-town';
  const D = {nodes: {[node.id]: node}, continents, continent: id => continents.find(c => c.id === id),
    isTown: n => !!n.town, towns: {'test-town': {theme: 'forest'}}, items: {},
    dungeonLinks: [{N: 1}, {S: 0, W: 2, E: 3, N: 4}, {E: 1}, {W: 1}, {S: 1}]};
  const messages = [], counters = {clears: 0, hud: 0, saves: 0, materials: 0};
  const noop = () => {};
  const c = {console, ROOM_W: 18, ROOM_H: 14, TAU: Math.PI * 2, running: true, paused: false,
    roomTransition: false, modalPause: false, elapsed: 0, SPELLS: {spirits: {}},
    biomePalette: {forest: {accent: '#abc'}}, BIOME_PROP_SETS: {forest: []},
    game: {seed: 1, level: 1, gold: 0, rooms: {}, enemies: [], telegraphs: [], interactables: [],
      player: {x: 9, y: 7, unlocked: [], spellState: {}},
      campaign: {current: node.id, room, cleared: [], claimed: [], defeated: [], dungeonClears: {}, visited: [], rewards: []}},
    ENEMY_TYPES: Object.fromEntries(['wolf', 'wisp', 'archer'].map((id, i) => [id,
      {ai: i ? 'ranged' : 'melee', min: 1, biomes: ['forest']} ])),
    roomKey: (x, y) => `${x},${y}`, hash2: () => .25,
    clamp: (v, lo, hi) => Math.max(lo, Math.min(hi, v)), irnd: () => 0,
    rnd: (hi, lo = 0) => (hi + lo) / 2, dist: (a, b) => Math.hypot(a.x - b.x, a.y - b.y),
    randomEnemySpawn: () => ({x: 12, y: 7}), generateScenery: noop,
    burst: noop, fx: noop, intensityChallengeHazard: noop,
    toastMsg: message => messages.push(message), intensityRefreshHud: () => counters.hud++,
    addMaterial: () => counters.materials++, saveGame: () => counters.saves++};
  for (const name of ('getRoomData loadRoom beginWorld spawnRoomEnemies transitionRoom markRoomCleared ' +
    'buildVillagePeople openNPCPanel interact materialForEnemy playerDeath updateEnemyAI drawInteractable ' +
    'drawPlayer playerMovement damagePlayer autoAttack dodge castSpell render updateHUD renderMinimap gearCard ' +
    'questForVillage acceptQuest intensityBossAdds intensityBossPhase intensityBossHazard drawDoors ' +
    'drawFloorDetails drawAtmosphere house drawVillageNPC').split(' ')) c[name] = noop;
  c.markRoomCleared = () => {if (!c.game.roomData.cleared) {c.game.roomData.cleared = true; counters.clears++;}};
  c.spawnEnemy = (id, point, elite) => {
    assert.ok(c.ENEMY_TYPES[id], `Invalid reinforcement: ${id}`);
    const e = {type: id, ...point, elite, hp: 10, speed: 1, attack: 1, color: '#abc'};
    c.game.enemies.push(e); return e;
  };
  c.spawnBoss = () => {const e = {boss: true, hp: 100}; c.game.enemies.push(e); return e;};
  c.window = {AWCampaignData: D, AWVillagePortals: {destinations: () => [], travel: noop, open: noop}};
  vm.createContext(c);
  vm.runInContext(waveRuntime, c, {filename: 'intensity-wave-runtime.js'});
  vm.runInContext(campaign, c, {filename: 'campaign.js'});
  const point = c.window.AWCampaign.coordinates(node.id, room);
  c.game.roomData = c.getRoomData(point.x, point.y);
  if (!c.game.roomData.cleared) c.spawnRoomEnemies(c.game.roomData);
  return {c, node, messages, counters, encounter: () => c.intensityState().encounter};
}
function killWave(h) { h.c.game.enemies = []; h.c.markRoomCleared(); }
function nextWave(h) {
  const c = h.c, enc = h.encounter(), prior = enc.wave;
  c.intensityTickEncounter(2.3);
  assert.ok(enc.pending, 'remaining waves must be scheduled');
  assert.equal(enc.wave, prior, 'telegraph comes before the spawn');
  assert.ok(c.game.telegraphs.length > 0);
  c.markRoomCleared();
  assert.equal(c.game.roomData.cleared, false, 'pending waves block completion');
  c.intensityTickEncounter(.5);
  assert.equal(c.game.enemies.length, 0);
  c.intensityTickEncounter(.6);
  assert.equal(enc.wave, prior + 1);
  assert.equal(enc.pending, null);
  assert.ok(c.game.enemies.length >= 4 && c.game.enemies.length <= 8);
}

for (const [name, options, waves] of [
  ['easy road', {type: 'wildland', threat: 5}, 1],
  ['dangerous node', {type: 'danger', threat: 3}, 2],
  ['event ambush', {type: 'event', threat: 3, event: 'hunt'}, 2],
  ['dungeon entrance', {type: 'dungeon', threat: 3}, 2],
  ['dungeon elite chamber', {type: 'dungeon', threat: 3, room: 3}, 3],
  ['threat-six road', {type: 'wildland', threat: 6}, 3],
  ['threat-nine danger', {type: 'danger', threat: 9}, 3],
  ['threat-ten danger', {type: 'danger', threat: 10}, 4],
  ['later-continent road', {type: 'wildland', threat: 22, continent: 'gloam'}, 4],
  ['crossfire challenge', {type: 'event', threat: 3, continent: 'meridian'}, 3],
  ['high-threat crossfire', {type: 'event', threat: 12, continent: 'meridian'}, 4],
]) test(`${name} initializes ${waves} waves and usable scheduler fields`, () => {
  const h = harness(options), enc = h.encounter();
  assert.equal(enc.totalWaves, waves);
  assert.equal(enc.wave, 1);
  assert.ok(Number.isInteger(enc.waveSize) && enc.waveSize >= 4 && enc.waveSize <= 7);
  assert.ok(enc.spawnThreshold >= 1);
  assert.ok(enc.grace > 0);
  assert.equal(enc.roomKey, h.c.game.roomData.key);
  assert.equal(h.counters.hud, 1);
});

for (const event of ['waves', 'defend']) test(`${event} retains its dedicated three-wave controller`, () => {
  const h = harness({type: 'event', threat: 20, event});
  assert.equal(h.encounter().totalWaves, 1, 'do not stack intensity waves onto objective waves');
  h.c.game.enemies = [];
  h.c.intensityTickEncounter(5);
  assert.equal(h.encounter().pending, null);
});
for (const type of ['boss', 'ruler', 'shadowBoss', 'dungeon']) test(`${type} boss fight does not gain generic waves`, () => {
  const h = harness({type, threat: 30, room: type === 'dungeon' ? 4 : 0});
  assert.equal(h.encounter().totalWaves, 1);
  assert.equal(h.encounter().boss, true);
  assert.equal(h.c.game.enemies.length, 1);
  assert.equal(h.c.game.enemies[0].boss, true);
});
for (const type of ['town', 'shrine', 'landmark', 'mount', 'merchant', 'passage', 'resource', 'treasure', 'puzzle', 'sanctuary'])
  test(`${type} remains safe even at high threat`, () => {
    const h = harness({type, threat: 30});
    assert.equal(h.c.game.roomData.cleared, true);
    assert.equal(h.encounter(), null);
    assert.equal(h.c.game.enemies.length, 0);
  });

test('three reinforcement transitions run before clear, progression, and one-time rewards', () => {
  const h = harness({threat: 12});
  for (let wave = 1; wave < 4; wave++) {
    killWave(h);
    assert.equal(h.c.game.roomData.cleared, false);
    assert.equal(h.c.game.campaign.cleared.length, 0);
    assert.equal(h.c.game.gold, 0);
    nextWave(h);
  }
  killWave(h);
  assert.equal(h.c.game.roomData.cleared, true);
  assert.ok(h.c.game.campaign.cleared.includes(h.node.id));
  assert.equal(h.counters.clears, 1);
  assert.equal(h.c.game.gold, 25 + 12 * 5);
  const gold = h.c.game.gold;
  h.c.markRoomCleared(); h.c.intensityTickEncounter(10);
  assert.equal(h.c.game.gold, gold);
  assert.equal(h.counters.clears, 1);
  assert.equal(h.c.game.enemies.length, 0);
  assert.ok(h.messages.some(m => /wave 4 of 4/.test(m)));
});

test('scheduler honors initial grace and the low-enemy threshold', () => {
  const h = harness(), c = h.c, enc = h.encounter();
  c.intensityTickEncounter(10);
  assert.equal(enc.pending, null, 'do not reinforce a full first wave');
  c.game.enemies = c.game.enemies.slice(0, enc.spawnThreshold);
  c.intensityTickEncounter(.1);
  assert.ok(enc.pending);
  const pending = enc.pending;
  c.intensityScheduleWave(enc);
  assert.equal(enc.pending, pending, 'only one reinforcement can be pending');
  const fresh = harness(); fresh.c.game.enemies = [];
  fresh.c.intensityTickEncounter(1);
  assert.equal(fresh.encounter().pending, null, 'preserve opening grace');
});

test('dungeon relic reward and room clear wait for every wave', () => {
  const h = harness({type: 'dungeon', threat: 3, room: 2});
  killWave(h);
  assert.equal(h.c.game.campaign.dungeonClears[h.node.id], undefined);
  assert.equal(h.c.game.player.unlocked.length, 0);
  nextWave(h); killWave(h);
  assert.deepEqual(Array.from(h.c.game.campaign.dungeonClears[h.node.id]), [2]);
  assert.ok(h.c.game.player.unlocked.includes('spirits'));
  assert.equal(h.c.game.campaign.cleared.length, 0, 'guardian still required');
});

test('re-entering an unfinished room resets pending waves; cleared rooms stay cleared', () => {
  const h = harness(), c = h.c;
  killWave(h); c.intensityTickEncounter(3);
  const previous = h.encounter();
  assert.ok(previous.pending);
  c.spawnRoomEnemies(c.game.roomData);
  assert.notEqual(h.encounter(), previous);
  assert.equal(h.encounter().wave, 1);
  assert.equal(h.encounter().pending, null);
  killWave(h); nextWave(h); killWave(h);
  const r = c.game.roomData;
  delete c.game.rooms[r.key];
  assert.equal(c.getRoomData(r.x, r.y).cleared, true, 'persisted clear survives room-cache regeneration');
});

test('an old room pending wave cannot spawn after changing rooms', () => {
  const h = harness(), c = h.c;
  killWave(h); c.intensityTickEncounter(3);
  const enc = h.encounter();
  c.game.roomData = {...c.game.roomData, key: 'somewhere-else'};
  c.intensityTickEncounter(10);
  c.intensitySpawnWave(enc, enc.pending.wave, enc.pending.ids, enc.pending.points);
  assert.equal(c.game.enemies.length, 0);
});

test('non-campaign rooms continue using the original intensity spawner', () => {
  const h = harness(), c = h.c;
  c.game.roomData = {...c.game.roomData, campaignNode: undefined, difficulty: 10};
  c.game.enemies = [];
  c.spawnRoomEnemies(c.game.roomData);
  assert.equal(h.encounter().totalWaves, 4);
  assert.ok(c.game.enemies.length >= 5);
});
