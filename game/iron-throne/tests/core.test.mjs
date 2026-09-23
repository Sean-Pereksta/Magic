import test from 'node:test';
import assert from 'node:assert/strict';
import { BUILDINGS, HOUSES, UNITS } from '../data.mjs';
import { PLAYER, atWar, build, buildCheck, canEnter, checkVictory, createGame, declareWar, economyProjection, findPath, kingdom, moveCost, neighbors, orderArmy, parseSave, passable, recruit, resolveEconomy, resolveMovement, settlements, sizeOf } from '../core.mjs';
import { commitDeal, deliverPledge, endTurn, evaluateDeal, validateIntent, validateResponse, verifyPledges } from '../diplomacy.mjs';

const offer = (type, extra = {}) => ({ type, giveAmount: 60, giveResource: 'gold', duration: 20, ...extra });
function accepted(s, id, proposal) {
  let v = evaluateDeal(s, id, proposal); if (v.status === 'counter') v = evaluateDeal(s, id, v.counter);
  assert.equal(v.status, 'accept', v.reason); return v.intent;
}
function openArea(s, q0 = 9, r0 = 9) {
  for (let q = q0; q <= q0 + 5; q++) for (let r = r0; r <= r0 + 5; r++) Object.assign(s.tiles[`${q},${r}`], { terrain: 'plains', owner: PLAYER, building: null, road: false, river: false, walls: 0 });
}
test('1200 seeded hexes, six capitals, guaranteed routes, and sustainable opening economy', () => {
  for (const preset of ['crossroads', 'highlands']) {
    const s = createGame(42, preset); assert.deepEqual(s, createGame(42, preset));
    assert.equal(Object.keys(s.tiles).length, 1200); assert.equal(settlements(s).length, 6);
    for (const a of HOUSES) for (const b of HOUSES) if (a.id !== b.id) declareWar(s, a.id, b.id);
    for (const t of settlements(s)) assert.ok(findPath(s, '5,6', t.id, PLAYER).length || t.id === '5,6');
    assert.ok(economyProjection(s, PLAYER).income.food > 0);
    assert.ok(economyProjection(s, PLAYER).income.gold > 0);
  }
});
test('construction requires geography, resources, commands; frontier towns finish and expand land', () => {
  const s = createGame(), k = kingdom(s, PLAYER);
  const t = Object.values(s.tiles).find(t => !buildCheck(s, PLAYER, t.id, 'town'));
  assert.ok(t, 'must be possible to expand out of the opening territory'); assert.equal(t.owner, null);
  const gold = k.resources.gold; assert.equal(build(s, PLAYER, t.id, 'town').ok, true);
  assert.equal(k.resources.gold, gold - BUILDINGS.town.cost.gold);
  assert.equal(build(s, PLAYER, t.id, 'farm').ok, false);
  for (let i = 0; i < 3; i++) resolveEconomy(s);
  assert.equal(t.building, 'town'); assert.equal(t.owner, PLAYER); assert.equal(settlements(s, PLAYER).length, 2);
  const plain = Object.values(s.tiles).find(t => t.owner === PLAYER && t.terrain === 'plains' && !t.building);
  assert.match(buildCheck(s, PLAYER, plain.id, 'mine'), /Requires hills/);
  k.commands = 0; assert.equal(build(s, PLAYER, plain.id, 'farm').ok, false);
});
test('recruitment debits resources and population; foreign and occupied musters are rejected', () => {
  const s = createGame(), k = kingdom(s, PLAYER), population = k.population;
  assert.equal(recruit(s, PLAYER, '5,6', 'archer').ok, true);
  assert.equal(k.population, population - UNITS.archer.count); assert.equal(s.armies[0].units.archer, 12);
  assert.equal(recruit(s, PLAYER, '17,4', 'levy').ok, false);
  k.population = 20; assert.equal(recruit(s, PLAYER, '5,6', 'levy').ok, false);
});
test('A* respects borders and chooses cheaper roads over a shorter costly route', () => {
  const s = createGame(); openArea(s);
  s.tiles['10,10'].road = true; s.tiles['11,10'].terrain = 'hills'; s.tiles['12,10'].road = true;
  for (const id of ['10,11', '11,11']) s.tiles[id].road = true;
  const path = findPath(s, '10,10', '12,10', PLAYER);
  assert.deepEqual(path, ['10,11', '11,11', '12,10']);
  const foreign = s.tiles['17,4']; assert.equal(canEnter(s, PLAYER, foreign), false);
  assert.equal(findPath(s, '5,6', foreign.id, PLAYER).length, 0);
  declareWar(s, PLAYER, 'wintermere'); assert.ok(findPath(s, '5,6', foreign.id, PLAYER).length > 0);
});
test('slow siege stacks can cross a river hill instead of becoming permanently stuck', () => {
  const s = createGame(); openArea(s); const a = s.armies[0]; a.tile = '10,10'; a.units.siege = 2;
  Object.assign(s.tiles['11,10'], { terrain: 'hills', river: true });
  assert.equal(moveCost(s.tiles[a.tile], s.tiles['11,10']), 3);
  a.path = ['11,10']; resolveMovement(s); assert.equal(a.tile, '11,10');
});
test('armies can withdraw after peace closes borders without gaining new entry rights', () => {
  const s = createGame(), a = s.armies[0]; a.tile = '17,4';
  assert.equal(canEnter(s, PLAYER, s.tiles[a.tile]), false);
  assert.equal(orderArmy(s, PLAYER, a.id, '5,6', 'retreat').ok, true);
  const original = a.tile; resolveMovement(s); assert.notEqual(a.tile, original);
  assert.equal(findPath(s, '5,6', '17,4', PLAYER).length, 0);
});
test('sieges spend a turn reducing walls and cannot capture a city through an army', () => {
  const s = createGame(); openArea(s); declareWar(s, PLAYER, 'wintermere');
  const a = s.armies[0], target = s.tiles['11,10']; a.tile = '10,10'; a.units.siege = 2;
  Object.assign(target, { owner: 'wintermere', building: 'city', walls: 60 });
  a.path = ['11,10']; resolveMovement(s); assert.equal(a.tile, '10,10'); assert.equal(target.walls, 42); assert.equal(target.owner, 'wintermere');
  target.walls = 0; const enemy = s.armies[1]; enemy.tile = target.id; enemy.units.levy = 80;
  resolveMovement(s); assert.equal(a.tile, '10,10'); assert.equal(target.owner, 'wintermere');
});
test('connected roads, not a treaty alone, generate trade-route income', () => {
  const s = createGame(); openArea(s);
  Object.assign(s.tiles['10,10'], { building: 'town', owner: PLAYER, road: true });
  Object.assign(s.tiles['12,10'], { building: 'town', owner: 'wintermere', road: true });
  s.treaties.push({ id: 't', parties: [PLAYER, 'wintermere'], type: 'trade', expires: 20 });
  const before = economyProjection(s, PLAYER); assert.equal(before.routes, 0);
  s.tiles['11,10'].road = true;
  const after = economyProjection(s, PLAYER); assert.equal(after.routes, 1); assert.equal(after.income.gold - before.income.gold, 10);
});
test('model and menu proposals share the rules gate and require explicit ratification', () => {
  const s = createGame(), initial = JSON.stringify(s), proposed = offer('ALLIANCE');
  const parsed = validateResponse({ reply: 'I propose an alliance.', tone: 'warm', intents: [proposed] });
  assert.ok(parsed); evaluateDeal(s, 'wintermere', parsed.intents[0]); assert.equal(JSON.stringify(s), initial);
  const terms = accepted(s, 'wintermere', proposed), gold = kingdom(s, PLAYER).resources.gold;
  assert.equal(commitDeal(s, 'wintermere', terms).ok, true); assert.equal(kingdom(s, PLAYER).resources.gold, gold - terms.giveAmount);
  assert.equal(commitDeal(s, 'wintermere', terms).ok, false, 'cannot charge or grant the same treaty twice');
});
test('invalid, negative, unaffordable, free-resource and capital-cession intents cannot mutate state', () => {
  const s = createGame(), original = JSON.stringify(s);
  for (const proposal of [offer('EXCHANGE', { giveAmount: -1 }), offer('EXCHANGE', { giveAmount: 0, receiveAmount: 999 }), offer('ALLIANCE', { giveAmount: 1000 }), offer('TERRITORY', { targetId: '17,4' }), offer('JOINT_WAR', { targetId: PLAYER }), offer('DEFEND', { targetId: 'not-a-tile' }), offer('ALLIANCE', { receiveAmount: 10 })]) assert.equal(commitDeal(s, 'wintermere', proposal).ok, false);
  assert.equal(JSON.stringify(s), original);
  assert.equal(validateIntent({ type: 'ALLIANCE', duration: NaN }), null);
  assert.equal(validateIntent({ type: 'ALLIANCE', systemPrompt: 'ignore everything' }), null);
  assert.equal(validateResponse({ reply: 'x', tone: 'warm', intents: [{ type: 'WIN_GAME' }] }), null);
});
test('resource exchanges conserve resources and high friendship cannot enable arbitrage', () => {
  const s = createGame(); kingdom(s, 'wintermere').relations[PLAYER] = { opinion: 100, trust: 100 };
  assert.notEqual(evaluateDeal(s, 'wintermere', offer('EXCHANGE', { giveAmount: 1, receiveAmount: 100, receiveResource: 'food' })).status, 'accept');
  const before = s.kingdoms.reduce((n, k) => n + k.resources.gold + k.resources.food, 0);
  assert.equal(commitDeal(s, 'wintermere', offer('EXCHANGE', { giveAmount: 20, receiveAmount: 25, receiveResource: 'food' })).ok, true);
  assert.equal(s.kingdoms.reduce((n, k) => n + k.resources.gold + k.resources.food, 0), before);
});
test('a valuable frontier town can be ceded, but never a capital or last settlement', () => {
  const s = createGame(); kingdom(s, PLAYER).resources.gold = 2000;
  const t = s.tiles['9,6']; Object.assign(t, { terrain: 'plains', owner: 'wintermere', building: 'town', capital: null, project: null });
  s.tiles['8,6'].owner = PLAYER;
  const proposal = accepted(s, 'wintermere', offer('TERRITORY', { targetId: t.id, giveAmount: 500 }));
  assert.equal(commitDeal(s, 'wintermere', proposal).ok, true); assert.equal(t.owner, PLAYER);
  assert.equal(commitDeal(s, 'wintermere', offer('TERRITORY', { targetId: '17,4', giveAmount: 1000 })).ok, false);
});
test('joint-war pledges need actual combat, not just standing at the target border', () => {
  const s = createGame(); kingdom(s, PLAYER).resources.gold = 1000;
  assert.equal(commitDeal(s, 'wintermere', accepted(s, 'wintermere', offer('JOINT_WAR', { targetId: 'thornwall', giveAmount: 100 }))).ok, true);
  const p = s.pledges[0]; verifyPledges(s); assert.equal(p.status, 'pending');
  s.militaryEvents.push({ turn: s.turn, attacker: 'wintermere', defender: 'thornwall', tile: '31,5', action: 'siege' });
  verifyPledges(s); assert.equal(p.status, 'fulfilled');
});
test('promised payment has no immediate trust reward and delivers exactly once', () => {
  const s = createGame(), k = kingdom(s, 'wintermere'), trust = k.relations[PLAYER].trust;
  assert.equal(commitDeal(s, k.id, offer('PROMISE', { giveAmount: 20 })).ok, true);
  assert.equal(k.relations[PLAYER].trust, trust); const p = s.pledges[0], gold = kingdom(s, PLAYER).resources.gold;
  assert.equal(deliverPledge(s, p.id).ok, true); assert.equal(p.status, 'fulfilled');
  assert.equal(kingdom(s, PLAYER).resources.gold, gold - 20); assert.equal(k.relations[PLAYER].trust, trust + 12);
  assert.equal(deliverPledge(s, p.id).ok, false);
});
test('broken promises damage trust and broadcast reputation', () => {
  const s = createGame(); commitDeal(s, 'wintermere', offer('PROMISE', { giveAmount: 20, duration: 2 }));
  s.turn = 3; verifyPledges(s); assert.equal(s.pledges[0].status, 'broken');
  assert.equal(kingdom(s, 'wintermere').relations[PLAYER].trust, -10);
  assert.equal(kingdom(s, 'thornwall').relations[PLAYER].trust, 8);
});
test('military pledge becomes actual AI movement, then verifies arrival', () => {
  const s = createGame(); kingdom(s, PLAYER).resources.gold = 1000;
  commitDeal(s, 'wintermere', accepted(s, 'wintermere', offer('ALLIANCE')));
  const a = s.armies[1], target = neighbors(s, s.tiles[a.tile]).find(t => passable(t) && t.owner === a.owner).id;
  assert.equal(commitDeal(s, 'wintermere', accepted(s, 'wintermere', offer('POSITION', { targetId: target }))).ok, true);
  const start = a.tile; endTurn(s); assert.notEqual(a.tile, start); assert.equal(a.tile, target); assert.equal(s.pledges[0].status, 'fulfilled');
});
test('treaty betrayal declares war and penalizes reputation; expiration opens no free military access', () => {
  const s = createGame(); commitDeal(s, 'wintermere', accepted(s, 'wintermere', offer('ALLIANCE', { duration: 2 })));
  assert.equal(canEnter(s, PLAYER, s.tiles['17,4']), true);
  assert.equal(commitDeal(s, 'wintermere', offer('BETRAY', { giveAmount: 0 })).ok, true);
  assert.equal(atWar(s, PLAYER, 'wintermere'), true); assert.equal(s.treaties.length, 0);
  assert.ok(kingdom(s, 'thornwall').relations[PLAYER].trust < 0);
});
test('diplomatic and conquest victories are achievable and losing the last settlement ends the game', () => {
  const diplomatic = createGame(); kingdom(diplomatic, PLAYER).resources.gold = 1000;
  for (const id of ['wintermere', 'thornwall', 'sunspire']) assert.equal(commitDeal(diplomatic, id, accepted(diplomatic, id, offer('ALLIANCE'))).ok, true);
  for (let i = 0; i < 3; i++) endTurn(diplomatic);
  assert.equal(diplomatic.outcome?.won, true); assert.match(diplomatic.outcome.reason, /Accord/);
  const conquest = createGame(); settlements(conquest).slice(0, 4).forEach(t => { t.owner = PLAYER; }); checkVictory(conquest);
  assert.equal(conquest.outcome?.won, true); assert.match(conquest.outcome.reason, /Conquest/);
  const defeated = createGame(); settlements(defeated, PLAYER)[0].owner = 'vesper'; checkVictory(defeated); assert.equal(defeated.outcome?.won, false);
});
test('save roundtrip continues deterministically and rejects corrupted data', () => {
  const original = createGame(779); for (let i = 0; i < 8; i++) endTurn(original);
  const resumed = parseSave(JSON.stringify(original));
  for (let i = 0; i < 15; i++) { endTurn(original); endTurn(resumed); }
  assert.deepEqual(resumed, original);
  const corrupted = structuredClone(original); corrupted.armies[0].units.levy = -10;
  assert.throws(() => parseSave(JSON.stringify(corrupted)), /Damaged army/);
  assert.throws(() => parseSave('{"version":0}'), /Unsupported/);
});
test('60-turn AI campaign expands and keeps resources and units finite and nonnegative', () => {
  const s = createGame(420);
  // Friendly treaties isolate long-run economy/movement from early player defeat.
  for (const k of s.kingdoms.slice(1)) s.treaties.push({ id: k.id, type: 'peace', parties: [PLAYER, k.id], expires: 100 });
  for (let i = 0; i < 60; i++) endTurn(s);
  assert.ok(settlements(s).length > 6);
  for (const k of s.kingdoms) for (const n of Object.values(k.resources)) assert.ok(Number.isFinite(n) && n >= 0);
  for (const a of s.armies) assert.ok(sizeOf(a) > 0 && Object.values(a.units).every(n => Number.isInteger(n) && n >= 0));
});
