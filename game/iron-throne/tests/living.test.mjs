import { tradeRoute } from '../trade.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { HOUSES, RESOURCES, SAVE_VERSION } from '../data.mjs';
import { PLAYER, atWar, build, buildCheck, canEnter, createGame, declareWar, kingdom, parseSave, relation, resolveEconomy, resolveMovement, settlements, strategyTurn } from '../core.mjs';
import { acceptRulerMemories, commitDeal, deliverPledge, endTurn, evaluateDeal, makeContext, resolveRecurringTrade, scriptedReply, validateIntent, validateResponse, verifyPledges } from '../diplomacy.mjs';
import { ambassadorIncident, applySpeech, assignAmbassador, borderThreat, changeRelation, consumeMessage, diplomaticCapacity, economicRelationship, messageAllowance, recruitAmbassador, recordTrade, RELATION_DEFAULTS, resolveAmbassadors, stationedAmbassador, updatePoliticalState } from '../living.mjs';
import { detectPromise } from '../promises.mjs';
import { BattleEffects } from '../battle-effects.mjs';

function terms(type, extra = {}) { return validateIntent({ type, ...extra }); }
function ratify(s, id, type, extra = {}) {
  let v = evaluateDeal(s, id, terms(type, extra));
  if (v.status === 'counter') v = evaluateDeal(s, id, v.counter);
  assert.equal(v.status, 'accept', v.reason);
  assert.equal(commitDeal(s, id, v.intent).ok, true);
  return s.pledges.at(-1);
}
function embassy(s, host = 'wintermere') {
  s.tiles['5,6'].envoyOffice = true;
  const result = recruitAmbassador(s); assert.equal(result.ok, true);
  const a = s.ambassadors.find(a => a.id === result.ambassadorId);
  assert.equal(assignAmbassador(s, PLAYER, a.id, host).ok, true);
  for (let n = 0; n < 15 && a.status !== 'stationed'; n++) resolveAmbassadors(s);
  assert.equal(a.status, 'stationed'); return a;
}
test('compliments, apologies and reassurances have a small shared lifetime ceiling', () => {
  const s = createGame(), before = relation(s, 'wintermere', PLAYER).opinion;
  for (let i = 0; i < 100; i++) for (const text of ['You are amazing, my friend.', 'I apologize.', 'Trust me; I mean you no harm.']) { applySpeech(s, 'wintermere', text); s.turn++; }
  const r = relation(s, 'wintermere', PLAYER);
  assert.ok(r.opinion <= before + 4); assert.equal(r.trust, 15);
  assert.match(scriptedReply(s, 'wintermere', 'You are amazing.').reply, /terms|praised/);
});
test('nearby military strength creates wariness and retreat removes it without nice words overriding the board', () => {
  const s = createGame(), a = s.armies[0]; a.tile = '17,4'; a.units.levy = 120;
  updatePoliticalState(s); const r = relation(s, 'wintermere', PLAYER);
  assert.ok(r.wariness > 50); assert.ok(r.fear > 0);
  const opinion = r.opinion; applySpeech(s, 'wintermere', 'You are my closest friend.');
  assert.equal(r.opinion, opinion); assert.match(scriptedReply(s, 'wintermere', 'My closest friend.').reply, /soldiers|frontier/);
  a.tile = '5,6'; s.turn++; updatePoliticalState(s); assert.equal(r.wariness, 0);
  assert.ok(r.history.some(h => /withdrew/.test(h.reason)));
});
test('important imports generate dependency and are retained in bounded trade history', () => {
  const s = createGame();
  for (let i = 0; i < 8; i++) { recordTrade(s, PLAYER, 'wintermere', 'food', 100, 'recurring'); s.turn++; }
  updatePoliticalState(s);
  assert.ok(relation(s, 'wintermere', PLAYER).dependency >= 50);
  assert.equal(economicRelationship(s, 'wintermere', PLAYER).majorPartner, true);
  s.turn += 12; updatePoliticalState(s); assert.equal(relation(s, 'wintermere', PLAYER).dependency, 0);
});
test('resource promises only exist after confirmation and require a real transfer', () => {
  const s = createGame(), before = JSON.stringify(s);
  const out = scriptedReply(s, 'wintermere', "I'll send you 100 food next turn.");
  assert.equal(out.promiseDetected.type, 'PROMISE'); assert.equal(out.promiseDetected.duration, 1);
  assert.equal(s.pledges.length, 0); assert.equal(JSON.stringify(s), before);
  assert.ok(validateResponse(out));
  const trust = relation(s, 'wintermere', PLAYER).trust;
  assert.equal(commitDeal(s, 'wintermere', out.promiseDetected).ok, true);
  assert.equal(relation(s, 'wintermere', PLAYER).trust, trust);
  const food = kingdom(s, PLAYER).resources.food, received = kingdom(s, 'wintermere').resources.food;
  assert.equal(deliverPledge(s, s.pledges[0].id).ok, true);
  assert.equal(kingdom(s, PLAYER).resources.food, food - 100); assert.equal(kingdom(s, 'wintermere').resources.food, received + 100);
  assert.ok(relation(s, 'wintermere', PLAYER).trust > trust);
  assert.equal(deliverPledge(s, s.pledges[0].id).ok, false);
});
test('uncertain or negated words do not silently create an oath or declaration of war', () => {
  const s = createGame();
  for (const message of ['Maybe I will help you against Vesper.', 'I might send 100 food.', 'Would you like some aid?', "I won't send you 100 food.", 'I will help if the harvest improves.']) {
    assert.equal(detectPromise(s, 'wintermere', message), null, message);
    scriptedReply(s, 'wintermere', message); assert.equal(s.pledges.length, 0);
  }
  assert.equal(scriptedReply(s, 'wintermere', 'I will not attack you.').intents.some(i => i.type === 'WAR'), false);
});
test('conditional military oath: turn 10 pledge, turn 12 attack, turn 14 reminder, turn 16 breach remembered at turn 20', () => {
  const s = createGame(); s.turn = 10;
  const i = detectPromise(s, 'wintermere', 'If Vesper attacks you, I will join the war before Turn 16.');
  assert.equal(i.conditionHouseId, 'vesper'); assert.equal(i.duration, 6);
  const p = ratify(s, 'wintermere', i.type, i);
  assert.equal(atWar(s, PLAYER, 'vesper'), false);
  s.turn = 12; declareWar(s, 'vesper', 'wintermere'); verifyPledges(s);
  assert.equal(p.triggered, true); assert.equal(p.status, 'pending');
  assert.ok(s.conversations.wintermere.some(m => /oath is now called/.test(m.text)));
  s.turn = 14; verifyPledges(s); assert.ok(s.conversations.wintermere.some(m => /2 turns remain/.test(m.text)));
  s.turn = 16; verifyPledges(s); assert.equal(p.status, 'broken'); assert.ok(relation(s, 'wintermere', PLAYER).trust < 0);
  s.turn = 20; const c = makeContext(s, 'wintermere', 'Will you ally with me?');
  assert.equal(c.world.pledges.at(-1).status, 'broken'); assert.ok(c.memories.some(m => /broken/.test(m)));
  assert.equal(evaluateDeal(s, 'wintermere', terms('ALLIANCE', { giveAmount: 100 })).status, 'reject');
});
test('conditional guarantees that never trigger expire without farmable trust rewards', () => {
  const s = createGame(), p = ratify(s, 'wintermere', 'GUARANTEE', { targetId: 'vesper', duration: 2 });
  const trust = relation(s, 'wintermere', PLAYER).trust;
  s.turn += 2; verifyPledges(s); assert.equal(p.status, 'released'); assert.equal(relation(s, 'wintermere', PLAYER).trust, trust);
});
test('attack pledges require later actual combat at the named target, not old victories or a war declaration', () => {
  const s = createGame(); s.militaryEvents.push({ id: s.nextId++, turn: s.turn, attacker: PLAYER, defender: 'vesper', tile: '20,24', action: 'siege' });
  const p = ratify(s, 'wintermere', 'PLEDGE_ATTACK', { targetId: '20,24', duration: 3 });
  declareWar(s, PLAYER, 'vesper'); verifyPledges(s); assert.equal(p.status, 'pending');
  s.militaryEvents.push({ id: s.nextId++, turn: s.turn, attacker: PLAYER, defender: 'vesper', tile: '20,24', action: 'siege' });
  verifyPledges(s); assert.equal(p.status, 'fulfilled');
});
test('border withdrawal tracks armies and cannot be fulfilled by destroying or replacing the border stack', () => {
  const s = createGame(), a = s.armies[0]; a.tile = '17,4';
  const p = ratify(s, 'wintermere', 'PLEDGE_WITHDRAW', { duration: 3 });
  verifyPledges(s); assert.equal(p.status, 'pending');
  s.armies = s.armies.filter(b => b !== a); verifyPledges(s); assert.equal(p.status, 'pending');
  s.armies.push(a); a.tile = '5,6'; verifyPledges(s); assert.equal(p.status, 'fulfilled');
});
test('defense requires two distinct resolutions; repeated verification cannot complete it', () => {
  const s = createGame(); ratify(s, 'wintermere', 'ACCESS', { giveAmount: 80 });
  const p = ratify(s, 'wintermere', 'PLEDGE_DEFEND', { targetId: '17,4', duration: 4 }); s.armies[0].tile = '17,4';
  verifyPledges(s); verifyPledges(s); assert.equal(p.held, 1); assert.equal(p.status, 'pending');
  s.turn++; verifyPledges(s); assert.equal(p.status, 'fulfilled');
});
test('message capacity is shared, resets by turn, and only completed diplomatic buildings raise it', () => {
  const s = createGame(); assert.equal(diplomaticCapacity(s), 3);
  for (const id of ['wintermere', 'sunspire', 'vesper']) assert.equal(consumeMessage(s, id).ok, true);
  assert.equal(consumeMessage(s, 'thornwall').ok, false);
  assert.equal(build(s, PLAYER, '5,6', 'envoyOffice').ok, true); assert.equal(diplomaticCapacity(s), 3);
  resolveEconomy(s); resolveEconomy(s); assert.equal(diplomaticCapacity(s), 4);
  assert.equal(consumeMessage(s, 'thornwall').ok, true); assert.equal(consumeMessage(s, 'wintermere').ok, false);
  assert.equal(build(s, PLAYER, '5,6', 'chancery').ok, true);
  for (let i = 0; i < 3; i++) resolveEconomy(s);
  assert.equal(diplomaticCapacity(s), 5);
  s.turn++; assert.equal(messageAllowance(s, 'wintermere').remaining, 5);
  const other = createGame(); assert.match(buildCheck(other, PLAYER, '5,6', 'chancery'), /Envoy Office/);
});
test('ambassador host allowance is separate, location-bound and cannot refill through reassignment', () => {
  const s = createGame(), a = embassy(s);
  assert.equal(stationedAmbassador(s, PLAYER, 'wintermere'), true);
  for (let i = 0; i < 10; i++) assert.equal(consumeMessage(s, 'wintermere').ok, true);
  assert.equal(consumeMessage(s, 'wintermere').ok, false); assert.equal(messageAllowance(s, 'sunspire').remaining, 4);
  assert.equal(assignAmbassador(s, PLAYER, a.id).ok, true);
  assert.equal(messageAllowance(s, 'wintermere').hosted, false);
  assert.equal(assignAmbassador(s, PLAYER, a.id, 'wintermere').ok, true);
  assert.equal(messageAllowance(s, 'wintermere').remaining, 0);
  s.turn++; assert.equal(messageAllowance(s, 'wintermere').remaining, 10);
});
test('ambassadors move faster than armies, never become combat casualties and lose bonuses after capital capture', () => {
  const s = createGame(); s.tiles['5,6'].envoyOffice = true;
  const id = recruitAmbassador(s).ambassadorId; assignAmbassador(s, PLAYER, id, 'wintermere');
  const a = s.ambassadors[0], initial = a.path.length; resolveAmbassadors(s);
  assert.ok(initial - a.path.length >= 4); const tile = a.tile;
  declareWar(s, 'wintermere', PLAYER); resolveMovement(s); assert.equal(a.tile, tile); assert.notEqual(a.status, 'dead');
  a.tile = '17,4'; a.target = '17,4'; a.host = 'wintermere'; a.status = 'stationed'; a.path = [];
  assert.equal(stationedAmbassador(s, PLAYER, 'wintermere'), true);
  s.tiles['17,4'].owner = 'vesper'; assert.equal(stationedAmbassador(s, PLAYER, 'wintermere'), false);
});
test('executing an envoy requires control and explicit confirmation and causes public catastrophe', () => {
  const s = createGame(); s.tiles['17,4'].envoyOffice = true;
  const id = recruitAmbassador(s, 'wintermere').ambassadorId, a = s.ambassadors[0];
  assert.equal(ambassadorIncident(s, PLAYER, id, 'execute', true).ok, false);
  a.tile = '5,6'; const before = JSON.stringify(s);
  assert.equal(ambassadorIncident(s, PLAYER, id, 'execute').ok, false); assert.equal(JSON.stringify(s), before);
  assert.equal(ambassadorIncident(s, PLAYER, id, 'execute', true).ok, true);
  assert.equal(a.status, 'dead'); assert.equal(atWar(s, PLAYER, 'wintermere'), true);
  assert.ok(relation(s, 'wintermere', PLAYER).trust <= -75); assert.ok(relation(s, 'sunspire', PLAYER).trust < 0);
  assert.equal(kingdom(s, PLAYER).reputation.envoysKilled, 1);
  assert.equal(parseSave(JSON.stringify(s)).diplomacy.incidents.length, 1);
});
test('only the detaining House can release or execute a detained ambassador', () => {
  const s = createGame(); s.tiles['17,4'].envoyOffice = true;
  const id = recruitAmbassador(s, 'wintermere').ambassadorId, a = s.ambassadors[0]; a.tile = '5,6';
  assert.equal(ambassadorIncident(s, PLAYER, id, 'detain').ok, false);
  declareWar(s, PLAYER, 'wintermere'); assert.equal(ambassadorIncident(s, PLAYER, id, 'detain').ok, true);
  assert.equal(assignAmbassador(s, 'wintermere', id, 'sunspire').ok, false);
  assert.equal(ambassadorIncident(s, 'vesper', id, 'release').ok, false);
  assert.equal(ambassadorIncident(s, PLAYER, id, 'release').ok, true); assert.equal(a.status, 'returning');
});
test('recurring exchanges conserve resources, pay once, and fail atomically when either side cannot pay', () => {
  const s = createGame();
  for(const [id,owner] of [['6,6',PLAYER],['18,4','wintermere']]) Object.assign(s.tiles[id],{owner,building:'tradeOutpost',levels:{tradeOutpost:2,road:1},road:true});
  for(const id of tradeRoute(s,PLAYER,'wintermere').path){s.tiles[id].road=true;s.tiles[id].levels.road=1;} ratify(s, 'wintermere', 'RECURRING', { giveAmount: 20, receiveAmount: 25, duration: 4 });
  const a = kingdom(s, PLAYER), b = kingdom(s, 'wintermere'), total = () => a.resources.gold + b.resources.gold + a.resources.food + b.resources.food;
  const before = total(); s.turn++; const gold = a.resources.gold; resolveRecurringTrade(s);
  assert.equal(a.resources.gold, gold - 20); assert.equal(total(), before);
  resolveRecurringTrade(s); assert.equal(a.resources.gold, gold - 20);
  s.turn++; a.resources.gold = 0; const snapshot = [a.resources.food, b.resources.food, b.resources.gold]; resolveRecurringTrade(s);
  assert.deepEqual([a.resources.food, b.resources.food, b.resources.gold], snapshot); assert.equal(s.treaties[0].expires, s.turn);
});
test('loans defer repayment and cannot create an immediate resource arbitrage', () => {
  const s = createGame(), a = kingdom(s, PLAYER), b = kingdom(s, 'wintermere'); const before = a.resources.gold;
  const p = ratify(s, 'wintermere', 'LOAN', { giveAmount: 40, receiveAmount: 44, receiveResource: 'gold', duration: 2 });
  assert.equal(a.resources.gold, before - 40); assert.equal(p.status, 'pending');
  s.turn += 2; verifyPledges(s); assert.equal(a.resources.gold, before + 4); assert.equal(p.status, 'fulfilled');
  verifyPledges(s); assert.equal(a.resources.gold, before + 4);
});
test('an embargo has real trade consequences and military access expires normally', () => {
  const s = createGame(); kingdom(s, PLAYER).resources.gold = 1000;
  ratify(s, 'wintermere', 'ACCESS', { giveAmount: 100, duration: 2 }); assert.equal(canEnter(s, PLAYER, s.tiles['17,4']), true);
  ratify(s, 'wintermere', 'EMBARGO', { giveAmount: 150, targetId: 'vesper' });
  assert.equal(evaluateDeal(s, 'vesper', terms('EXCHANGE', { giveAmount: 20, receiveAmount: 20 })).status, 'reject');
  s.turn += 2; assert.equal(canEnter(s, PLAYER, s.tiles['17,4']), false);
});
test('AI suggestions and memory cannot mutate board state or numeric relations', () => {
  const s = createGame(), before = JSON.stringify(s);
  const response = validateResponse({ reply: 'Here are my terms.', tone: 'guarded', intents: [], promiseDetected: { type: 'PLEDGE_WAR', targetId: 'vesper', duration: 4 }, relationshipSignals: ['values_trade'], memoryCandidates: ['Player offered help.'], resources: { gold: 9999 }, trust: 100 });
  assert.ok(response); assert.equal(response.resources, undefined); assert.equal(response.trust, undefined);
  evaluateDeal(s, 'wintermere', response.promiseDetected); assert.equal(JSON.stringify(s), before);
  assert.equal(validateResponse({ ...response, counterProposal: { type: 'EXCHANGE', giveResource: 'diamonds' } }), null);
  acceptRulerMemories(s, 'wintermere', response);
  assert.equal(kingdom(s, 'wintermere').memories.at(-1).verified, false);
  const restored = parseSave(JSON.stringify(s)); assert.deepEqual(restored, s);
});
test('model snapshot contains current board, history and reasons but no private foreign treasuries or other chats', () => {
  const s = createGame(); s.conversations.vesper = [{ role: 'player', text: 'SECRET-DISCUSSION' }];
  kingdom(s, PLAYER).resources.gold = 12345; kingdom(s, 'sunspire').resources.gold = 23456;
  const context = makeContext(s, 'wintermere', 'Explain your counteroffer.', { proposal: terms('EXCHANGE', { giveAmount: 1, receiveAmount: 60 }) });
  const json = JSON.stringify(context); assert.ok(!json.includes('SECRET-DISCUSSION')); assert.ok(!json.includes('12345')); assert.ok(!json.includes('23456'));
  assert.ok(context.world.self.production.food > 0); assert.equal(context.world.negotiation.status, 'counter');
  assert.equal(validateIntent(context.world.negotiation.counter).receiveResource, 'food');
  assert.ok(new TextEncoder().encode(json).length < 22000);
});
test('large conversations and dense campaign history produce bounded prompt payloads', () => {
  const s = createGame(); s.conversations.wintermere = Array.from({ length: 50 }, () => ({ role: 'player', text: '界'.repeat(1600) }));
  for (let i = 0; i < 30; i++) kingdom(s, 'wintermere').memories.push({ text: '界'.repeat(300), turn: s.turn, importance: 8 });
  const c = makeContext(s, 'wintermere', '界'.repeat(600));
  assert.ok(new TextEncoder().encode(JSON.stringify(c)).length <= 22000);
});
test('relations remain finite and bounded and new save data survives export/import', () => {
  const s = createGame(); embassy(s); consumeMessage(s, 'wintermere');
  const changes = Object.fromEntries(['opinion', 'trust', ...Object.keys(RELATION_DEFAULTS)].map(k => [k, 10000]));
  changeRelation(s, 'wintermere', PLAYER, changes, 'Large test event');
  const r = relation(s, 'wintermere', PLAYER); for (const key of Object.keys(changes)) assert.equal(r[key], 100);
  changeRelation(s, 'wintermere', PLAYER, { trust: NaN, fear: Infinity }, 'Invalid'); assert.equal(r.trust, 100);
  s.diplomacy.offers.wintermere = [terms('EXCHANGE', { giveAmount: 20, receiveAmount: 20 })];
  const restored = parseSave(JSON.stringify(s)); assert.deepEqual(restored, s);
  const bad = structuredClone(s); bad.diplomacy.messages.hosts.wintermere = 11; assert.throws(() => parseSave(JSON.stringify(bad)), /Damaged living/);
});
test('version 1 saves migrate without changing resources, armies, existing pledges or conversations', () => {
  const old = createGame(); old.version = 1; delete old.ambassadors; delete old.diplomacy;
  for (const k of old.kingdoms) { delete k.reputation; delete k.priorities; for (const r of Object.values(k.relations)) for (const key of Object.keys(r)) if (!['opinion', 'trust'].includes(key)) delete r[key]; }
  old.conversations.wintermere = [{ role: 'player', text: 'Our original conversation.' }];
  const migrated = parseSave(JSON.stringify(old)); assert.equal(migrated.version, SAVE_VERSION);
  assert.deepEqual(migrated.armies, old.armies); assert.deepEqual(migrated.conversations, old.conversations); assert.deepEqual(migrated.kingdoms[0].resources, old.kingdoms[0].resources);
  assert.equal(messageAllowance(migrated, 'wintermere').remaining, 3); assert.deepEqual(migrated.ambassadors, []);
});
test('high fear and low trust affect actual AI fortification and military goals', () => {
  const s = createGame(); const k = kingdom(s, 'wintermere');
  Object.assign(relation(s, k.id, PLAYER), { wariness: 80, fear: 80, trust: -20 });
  strategyTurn(s);
  assert.equal(s.tiles['17,4'].project?.type, 'wall'); assert.equal(k.goal, 'GUARD_FRONTIER');
});
test('battle presentation consumes actual events without mutating the simulation and bounds its pools', () => {
  const s = createGame(), fx = new BattleEffects(); fx.ingest(s, 0);
  for (let i = 0; i < 30; i++) s.militaryEvents.push({ id: s.nextId++, turn: s.turn, attacker: PLAYER, defender: 'wintermere', tile: '17,4', action: 'battle', before: [28, 28], after: [20, 15], winner: PLAYER });
  const before = JSON.stringify(s); fx.ingest(s, 100);
  assert.equal(fx.active.length, 6); assert.ok(fx.active.reduce((n, e) => n + e.particles.length, 0) <= 72); assert.equal(JSON.stringify(s), before);
  assert.equal(fx.animating(200, true), true, 'Reduced effects retain fading casualty numbers'); fx.ingest(s, 7000); assert.equal(fx.active.length, 0); assert.equal(fx.results.length, 0);
});
test('honorable and mercantile rulers weigh the same profitable offer differently after broken trust', () => {
  const s = createGame();
  for (const id of ['wintermere', 'sunspire']) Object.assign(relation(s, id, PLAYER), { trust: -20, grievance: 25, opinion: 0 });
  const offer = terms('ALLIANCE', { giveAmount: 160 });
  assert.equal(evaluateDeal(s, 'wintermere', offer).status, 'reject');
  assert.equal(evaluateDeal(s, 'sunspire', offer).status, 'accept');
});
test('a conditional pledge is still broken immediately by attacking its beneficiary', () => {
  const s = createGame(), p = ratify(s, 'wintermere', 'GUARANTEE', { targetId: 'vesper', duration: 5 });
  declareWar(s, PLAYER, 'wintermere'); verifyPledges(s); assert.equal(p.status, 'broken');
});
test('ruler interpretations and open negotiations survive a save without becoming verified history', () => {
  const s = createGame();
  const response = validateResponse({ reply: 'I await your answer.', tone: 'guarded', intents: [], relationshipSummary: 'Ashen is negotiating food shipments.', memoryCandidates: ['Ashen offered grain.'] });
  acceptRulerMemories(s, 'wintermere', response);
  s.diplomacy.offers.wintermere = [terms('EXCHANGE', { giveAmount: 1, receiveAmount: 60 })];
  const restored = parseSave(JSON.stringify(s)), context = makeContext(restored, 'wintermere', 'Forty gold?');
  assert.match(context.world.conversationInterpretation, /Unverified/);
  assert.equal(context.world.openNegotiations[0].status, 'counter');
  assert.equal(restored.kingdoms[1].memories.at(-1).verified, false);
});
