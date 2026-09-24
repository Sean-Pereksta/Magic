import { finishPlans, recordPlayerPlans } from './plans.mjs';
import { resolveEspionage, visiblePlans } from './espionage.mjs';
import { politicalAttitude } from './politics.mjs';
import { finishStrategyRound } from './strategy.mjs';
import { HOUSES, INTENT_TYPES, RESOURCES, RESOURCE_VALUES } from './data.mjs';
import { appendConversation, applyGift, borderThreat, changeRelation, contact, diplomaticPriorities, economicRelationship, grossProduction, recordPoliticalMemory, recordTrade, resolveAmbassadors, stationedAmbassador, tradeBlocked, updatePoliticalState } from './living.mjs';
import { createPlayerPromise, detectPromise, isPlayerPromise, playerPromiseCheck, promiseProgress } from './promises.mjs';
import { PLAYER, alive, armiesOf, atWar, buildCheck, canAfford, checkVictory, declareWar, distance, findPath, kingdom, log, makePeace, pay, rebuildTerritory, relation, remember, resolveEconomy, resolveMovement, settlements, strategyTurn, strength, treaty } from './core.mjs';

export const LABELS = { ALLIANCE: 'Alliance', PEACE: 'Peace treaty', TRADE: 'Trade agreement', EXCHANGE: 'Resource exchange', AID: 'Gift / military aid', JOINT_WAR: 'Joint war', DEFEND: 'Defend a settlement', POSITION: 'Position an army', WITHDRAW: 'Withdraw troops', BUILD_DEFENSES: 'Build a fort', TERRITORY: 'Request a province', TRIBUTE: 'Demand tribute', VASSALAGE: 'Request allegiance', PROMISE: 'Promise a later payment', WAR: 'Declare war', BETRAY: 'Break treaties and declare war', RECURRING: 'Recurring resource trade', LOAN: 'Loan with repayment', NON_AGGRESSION: 'Non-aggression pact', ACCESS: 'Open borders / military access', EMBARGO: 'Embargo a third House', GUARANTEE: 'Guarantee independence', PLEDGE_WAR: 'Promise to enter a war', PLEDGE_ATTACK: 'Promise an attack', PLEDGE_DEFEND: 'Promise to defend a location', PLEDGE_WITHDRAW: 'Promise border withdrawal', PLEDGE_BUILD: 'Promise to build a fort', PLEDGE_PEACE: 'Promise not to attack' };
const VALUES = RESOURCE_VALUES;
import { aiResourceTrade, contractAnchors, contractCheck, economicNeeds, scheduleTrade, tradeRoute } from './trade.mjs';
const TYPES = new Set(INTENT_TYPES);
const FIELDS = new Set(['type', 'targetId', 'giveResource', 'giveAmount', 'receiveResource', 'receiveAmount', 'duration', 'conditionHouseId', 'tradeKind']);
export function validateIntent(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !TYPES.has(value.type) || Object.keys(value).some(k => !FIELDS.has(k))) return null;
  const i = { type: value.type, duration: value.duration ?? 10, giveResource: value.giveResource ?? 'gold', giveAmount: value.giveAmount ?? 0, receiveResource: value.receiveResource ?? 'food', receiveAmount: value.receiveAmount ?? 0, targetId: value.targetId ?? '' };
  if(value.tradeKind!==undefined){if(!['immediate','recurring','purchase','strategic','emergency','preferential'].includes(value.tradeKind))return null;i.tradeKind=value.tradeKind;}
  if (value.conditionHouseId !== undefined) i.conditionHouseId = value.conditionHouseId;
  if (i.conditionHouseId !== undefined && (typeof i.conditionHouseId !== 'string' || !HOUSES.some(h => h.id === i.conditionHouseId) || !['PLEDGE_WAR', 'GUARANTEE'].includes(i.type))) return null;
  if (!Number.isInteger(i.duration) || i.duration < (isPlayerPromise(i) ? 1 : 2) || i.duration > 20 || !RESOURCES.includes(i.giveResource) || !RESOURCES.includes(i.receiveResource) || !Number.isInteger(i.giveAmount) || i.giveAmount < 0 || i.giveAmount > 1000 || !Number.isInteger(i.receiveAmount) || i.receiveAmount < 0 || i.receiveAmount > 1000 || typeof i.targetId !== 'string' || i.targetId.length > 60) return null;
  return i;
}
export function validateResponse(raw) {
  try {
    const out = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (!out || typeof out.reply !== 'string' || !out.reply.trim() || out.reply.length > 1600 || !Array.isArray(out.intents) || out.intents.length > 3 || !['warm', 'neutral', 'cold', 'hostile', 'guarded'].includes(out.tone)) return null;
    const intents = out.intents.map(validateIntent);
    if (intents.some(i => !i)) return null;
    const result = { reply: out.reply, intents, tone: out.tone };
    for (const key of ['proposal', 'counterProposal', 'promiseDetected']) {
      if (out[key] !== undefined && out[key] !== null) {
        const i = validateIntent(out[key]);
        if (!i || (key === 'promiseDetected' && !isPlayerPromise(i))) return null;
        result[key] = i;
      }
    }
    if (out.speechAct !== undefined) {
      if (!['statement', 'question', 'accept', 'reject', 'counteroffer', 'promise', 'warning', 'gratitude'].includes(out.speechAct)) return null;
      result.speechAct = out.speechAct;
    }
    for (const key of ['relationshipSignals', 'memoryCandidates']) if (out[key] !== undefined) {
      if (!Array.isArray(out[key]) || out[key].length > 4 || out[key].some(t => typeof t !== 'string' || t.length > 180)) return null;
      result[key] = out[key];
    }
    if (out.relationshipSummary !== undefined) {
      if (typeof out.relationshipSummary !== 'string' || out.relationshipSummary.length > 360) return null;
      result.relationshipSummary = out.relationshipSummary;
    }
    // Unknown model fields are discarded. In particular, no numeric deltas or actions survive.
    return result;
  } catch { return null; }
}
export function describeIntent(i) {
  const timing = i.type === 'RECURRING' ? ' each turn' : isPlayerPromise(i) ? ' by the deadline' : ' now';
  const duration = ['EXCHANGE', 'AID', 'WAR', 'BETRAY', 'TRIBUTE', 'TERRITORY'].includes(i.type) ? 'Immediate' : `${i.duration} turn${i.duration === 1 ? '' : 's'}`;
  return `${LABELS[i.type]}${i.targetId ? ` · ${i.targetId}` : ''}${i.giveAmount ? ` · Ashen gives ${i.giveAmount} ${i.giveResource}${timing}` : ''}${i.receiveAmount ? ` · Ashen receives ${i.receiveAmount} ${i.receiveResource}${i.type === 'LOAN' ? ' on the deadline' : i.type === 'RECURRING' ? ' each turn' : ' now'}` : ''} · ${duration}${i.conditionHouseId || i.type === 'GUARANTEE' ? ` · only if ${i.conditionHouseId || i.targetId} attacks this House` : ''}`;
}
export function dealFactors(s, rulerId, i) {
  const r = relation(s, rulerId, PLAYER), k = kingdom(s, rulerId), military = borderThreat(s, rulerId, PLAYER), economic = economicRelationship(s, rulerId, PLAYER);
  const factors = [];
  if (r.trust < 0) factors.push('Broken confidence makes future obligations costly.');
  if ((r.grievance || 0) > 25) factors.push('Past hostilities have left serious grievances.');
  if (military.score >= 20) factors.push('Ashen armies are concentrated near our frontier.');
  if (k.resources[i.giveResource] < 40 && i.giveAmount) factors.push(`We need the offered ${i.giveResource}.`);
  if (k.resources[i.receiveResource] < 60 && i.receiveAmount) factors.push(`Our ${i.receiveResource} stores are too scarce to part with cheaply.`);
  if (economic.majorPartner) factors.push('Ashen supplies an important share of our imports.');
  if (military.sharedEnemies.length) factors.push('We face a shared enemy.');
  if ((r.reliability ?? 50) > 60) factors.push('Ashen has honored its previous promises.');
  if (k.greed > .8) factors.push('Profitable, immediate payment carries great weight at this court.');
  return factors.slice(0, 6);
}
export function evaluateDeal(s, rulerId, raw) {
  const intent = validateIntent(raw), k = kingdom(s, rulerId), player = kingdom(s, PLAYER);
  const factors = intent && k && rulerId !== PLAYER ? dealFactors(s, rulerId, intent) : [];
  const reject = reason => ({ status: 'reject', reason, intent, factors });
  if (s.outcome) return reject('The campaign is over.');
  if (!intent || !k || rulerId === PLAYER || !alive(s, rulerId)) return reject('The proposed terms are invalid.');
  const i = intent, r = relation(s, rulerId, PLAYER), value = i.giveAmount * VALUES[i.giveResource] - i.receiveAmount * VALUES[i.receiveResource];
  if (!isPlayerPromise(i) && !canAfford(player, { [i.giveResource]: i.giveAmount })) return reject('Your treasury cannot cover this offer.');
  if (i.type !== 'LOAN' && !canAfford(k, { [i.receiveResource]: i.receiveAmount })) return reject('That house does not possess the requested resources.');
  if (!['EXCHANGE', 'TRIBUTE', 'RECURRING', 'LOAN'].includes(i.type) && i.receiveAmount !== 0) return reject('Use an exchange, recurring trade, loan or tribute to specify received resources.');
  if (['WAR', 'BETRAY', 'WITHDRAW', 'TRIBUTE', 'PROMISE'].includes(i.type) && i.type !== 'PROMISE' && i.giveAmount !== 0) return reject('This action does not accept an upfront payment.');
  if (['ALLIANCE', 'TRADE', 'VASSALAGE', 'DEFEND', 'POSITION', 'BUILD_DEFENSES', 'TERRITORY', 'PROMISE', 'RECURRING', 'LOAN', 'NON_AGGRESSION', 'ACCESS', 'EMBARGO', 'GUARANTEE', 'PLEDGE_WAR', 'PLEDGE_ATTACK', 'PLEDGE_DEFEND', 'PLEDGE_WITHDRAW', 'PLEDGE_BUILD', 'PLEDGE_PEACE'].includes(i.type) && atWar(s, PLAYER, rulerId)) return reject('Negotiate peace before making this agreement.');
  if (isPlayerPromise(i)) {
    const error = playerPromiseCheck(s, rulerId, i);
    return error ? reject(error) : { status: 'accept', reason: `Give your word only after reviewing this oath. It is due on turn ${s.turn + i.duration}; words alone will not fulfill it.`, intent, factors };
  }
  if (['EXCHANGE', 'TRADE', 'RECURRING', 'LOAN'].includes(i.type) && tradeBlocked(s, PLAYER, rulerId)) return reject('An active embargo prevents this economic agreement.');
  if (['WAR', 'BETRAY'].includes(i.type)) {
    if (atWar(s, PLAYER, rulerId)) return reject('You are already at war.');
    return { status: 'accept', reason: treaty(s, PLAYER, rulerId) ? 'This breaks your treaties and damages your reputation with every house.' : 'Hostilities open borders for invasion. Armies still need marching orders.', intent };
  }
  if (i.type === 'PEACE' && !atWar(s, PLAYER, rulerId)) return reject('You are already at peace.');
  const treatyType = { ALLIANCE: 'alliance', TRADE: 'trade', VASSALAGE: 'vassalage', NON_AGGRESSION: 'non-aggression', ACCESS: 'access', RECURRING: 'recurring' }[i.type];
  if (treatyType && treatyType!=='recurring' && treaty(s, PLAYER, rulerId, treatyType)) return reject('This agreement is already active.');
  if (i.type === 'AID') {
    if (i.giveAmount < 10) return reject('A meaningful gift is at least 10 resources.');
    if (k.lastGiftTurn === s.turn) return reject('This ruler has already received a gift this turn.');
    return { status: 'accept', reason: 'Aid matters most during genuine need. Repeated gifts quickly lose diplomatic influence.', intent };
  }
  if (['EXCHANGE', 'RECURRING'].includes(i.type) && (!i.giveAmount || !i.receiveAmount || i.giveResource === i.receiveResource)) return reject('Offer two different resources, each with a positive amount.');
  if(i.type==='EXCHANGE'&&!tradeRoute(s,PLAYER,rulerId).safe)return reject('No safe trade route is available.');
  if(i.type==='RECURRING'&&s.treaties.some(t=>t.type==='recurring'&&t.expires>s.turn&&t.parties.includes(PLAYER)&&t.parties.includes(rulerId)&&t.intent.giveResource===i.giveResource&&t.intent.receiveResource===i.receiveResource&&t.intent.giveAmount===i.giveAmount&&t.intent.receiveAmount===i.receiveAmount))return reject('An identical supply agreement is already active.');
  if(i.type==='RECURRING'){const why=contractCheck(s,PLAYER,rulerId,i);if(why)return reject(why);}
  if(i.tradeKind==='purchase'&&![i.giveResource,i.receiveResource].includes('gold'))return reject('A purchase must include gold.');
  if(['strategic','preferential','recurring'].includes(i.tradeKind)&&i.type!=='RECURRING')return reject('These supply terms require a recurring contract.');
  if (i.type === 'LOAN' && (i.giveAmount < 10 || i.receiveResource !== i.giveResource || i.receiveAmount < i.giveAmount || i.receiveAmount > Math.floor(i.giveAmount * 1.5))) return reject('A loan needs at least 10 resources and repayment of 100–150% in the same resource.');
  if (i.type === 'LOAN' && s.pledges.some(p => p.debtor === rulerId && p.creditor === PLAYER && p.status === 'pending' && p.loan)) return reject('This House must repay its existing loan first.');
  if (i.type === 'EMBARGO') {
    if (!alive(s, i.targetId) || [PLAYER, rulerId].includes(i.targetId)) return reject('Choose a third surviving House to embargo.');
    if (treaty(s, rulerId, i.targetId, 'alliance')) return reject('We will not embargo our ally.');
    if (s.treaties.some(t => t.type === 'embargo' && t.expires > s.turn && t.parties.includes(PLAYER) && t.parties.includes(rulerId) && t.targetId === i.targetId)) return reject('This embargo is already active.');
  }
  if (i.type === 'TRIBUTE' && !i.receiveAmount) return reject('Specify the tribute you demand.');
  const playerPower = armiesOf(s, PLAYER).reduce((n, a) => n + strength(a), 0), rulerPower = armiesOf(s, rulerId).reduce((n, a) => n + strength(a), 0);
  if (i.type === 'VASSALAGE' && (playerPower < rulerPower * 1.8 || settlements(s, PLAYER).length < 2)) return reject('Allegiance requires at least two settlements and overwhelming military strength.');
  if (i.type === 'ALLIANCE' && r.trust < 0 && (k.honor >= .6 || r.trust < -60)) return reject('Rebuild trust before requesting an alliance.');
  if (i.type === 'TERRITORY') {
    const t = s.tiles[i.targetId];
    if (!t || t.owner !== rulerId || t.capital || !['city', 'town', 'fort'].includes(t.building) || t.project) return reject('Choose a completed frontier town or fort. Capitals cannot be ceded.');
    if (['city', 'town'].includes(t.building) && settlements(s, rulerId).length < 2) return reject('A ruler never sells their last settlement.');
    if (armiesOf(s, rulerId).some(a => a.tile === t.id)) return reject('The ruler will not cede an occupied garrison.');
    if (!Object.values(s.tiles).some(n => n.owner === PLAYER && distance(n, t) === 1)) return reject('The province must adjoin your land.');
  }
  if (i.type === 'JOINT_WAR') {
    if (!kingdom(s, i.targetId) || [PLAYER, rulerId].includes(i.targetId) || !alive(s, i.targetId)) return reject('Choose a third surviving house.');
    if (treaty(s, rulerId, i.targetId) || treaty(s, PLAYER, i.targetId)) return reject('Existing treaties forbid this joint invasion.');
    if (s.pledges.some(p => p.status === 'pending' && p.debtor === rulerId && p.intent.type === i.type)) return reject('This house already has a joint-war commitment.');
    if (!rulerPower) return reject('This ruler has no army to commit.');
  }
  if (['DEFEND', 'POSITION', 'BUILD_DEFENSES'].includes(i.type)) {
    const target = s.tiles[i.targetId];
    if (!target) return reject('Choose a real map tile.');
    if (!treaty(s, PLAYER, rulerId, 'alliance')) return reject('An alliance is required for coordinated military orders.');
    if (s.pledges.some(p => p.debtor === rulerId && p.status === 'pending' && ['DEFEND', 'POSITION', 'BUILD_DEFENSES'].includes(p.intent.type))) return reject('This house is already carrying out a military pledge.');
    if (i.type === 'DEFEND' && (target.owner !== PLAYER || !['city', 'town', 'fort'].includes(target.building))) return reject('Select one of your settlements or forts to defend.');
    if (i.type === 'BUILD_DEFENSES') {
      const why = buildCheck(s, rulerId, target.id, 'fort'); if (why) return reject(why);
    } else {
      const reachable = armiesOf(s, rulerId).some(a => a.tile === target.id || findPath(s, a.tile, target.id, rulerId).length > 0);
      if (!reachable) return reject('No army can legally reach that destination.');
    }
  }
  if (i.type === 'WITHDRAW' && !armiesOf(s, rulerId).some(a => s.tiles[a.tile].owner === PLAYER)) return reject('There are no troops of this house in your territory.');
  let threshold = { ALLIANCE: 85, PEACE: 30, TRADE: 32, EXCHANGE: 0, JOINT_WAR: 105, DEFEND: 45, POSITION: 40, WITHDRAW: 0, BUILD_DEFENSES: 80, TRIBUTE: 30, VASSALAGE: 120, TERRITORY: 500, NON_AGGRESSION: 35, ACCESS: 50, RECURRING: 0, LOAN: 0, EMBARGO: 70 }[i.type] ?? 9999;
  const military = borderThreat(s, rulerId, PLAYER), economic = economicRelationship(s, rulerId, PLAYER);
  const scarcity = resource => k.resources[resource] < 40 ? 1.8 : k.resources[resource] < 80 ? 1.2 : 1;
  const material = i.giveAmount * VALUES[i.giveResource] * scarcity(i.giveResource) - i.receiveAmount * VALUES[i.receiveResource] * scarcity(i.receiveResource);
  let utility = material * (.7 + k.greed * .35) + r.opinion * .5 + r.trust * (.4 + k.honor * .6) + economic.dependency * .3 - (r.grievance || 0) * k.honor * .4 - military.score * k.paranoia * .4 + ((r.reliability ?? 50) - 50) * k.honor * .35;
  utility += ((r.respect ?? 15) - 15) * .1 + (r.generosity || 0) * .1 - (r.aggression || 0) * k.paranoia * .1;
  if (military.sharedEnemies.length) utility += 12 + k.ambition * 8;
  if (i.type === 'JOINT_WAR') utility += atWar(s, rulerId, i.targetId) ? 25 : -economicRelationship(s, rulerId, i.targetId).dependency;
  if (i.type === 'EMBARGO') utility -= economicRelationship(s, rulerId, i.targetId).dependency * 2;
  if (i.type === 'ALLIANCE') utility += k.honor * 20 - k.paranoia * 12;
  if (i.type === 'PEACE') utility += (playerPower - rulerPower) * .7;
  if (i.type === 'JOINT_WAR') utility += k.aggression * 30;
  if (['TRIBUTE', 'VASSALAGE'].includes(i.type)) utility += (playerPower - rulerPower) * .7 + (r.fear || 0) * .25;
  if (['EXCHANGE', 'RECURRING'].includes(i.type)) {
    // Friendship never permits arbitrage: resource value must balance independently.
    utility = Math.min(value, material) - (i.tradeKind==='emergency'&&k.resources[i.receiveResource]<40?Math.ceil(i.receiveAmount*VALUES[i.receiveResource]*.2):0) - (r.trust < -30 ? Math.ceil(-r.trust * k.honor * (1 - k.greed) * .12) : 0); threshold = 0;
  }
  if (i.type === 'WITHDRAW' && !atWar(s, PLAYER, rulerId)) utility = 100;
  if (i.type === 'LOAN') utility = i.giveAmount - (i.receiveAmount - i.giveAmount) * (2 + k.greed);
  if (utility >= threshold) return { status: 'accept', reason: `${factors.slice(0, 2).join(' ')} The council accepts these terms; ratification makes them binding.`.trim(), intent, factors };
  if (['TRIBUTE', 'VASSALAGE', 'WITHDRAW'].includes(i.type)) return reject('Your leverage does not justify this demand.');
  const extra = Math.ceil((threshold - utility) / (VALUES[i.giveResource] * (['EXCHANGE', 'RECURRING'].includes(i.type) ? 1 : .7 + k.greed * .35))) + 2;
  const counter = { ...i, giveAmount: i.giveAmount + extra };
  if (counter.giveAmount <= 1000 && canAfford(player, { [counter.giveResource]: counter.giveAmount })) return { status: 'counter', reason: `${factors.slice(0, 2).join(' ')} We can accept ${counter.giveAmount} ${counter.giveResource}${i.type === 'RECURRING' ? ' each turn' : ' upfront'} for these obligations.`.trim(), intent, counter, factors };
  return reject('The terms are too costly for this house.');
}

function addTreaty(s, a, b, type, duration) {
  s.treaties.push({ id: `treaty-${s.nextId++}`, type, parties: [a, b], expires: s.turn + duration });
}
export function commitDeal(s, rulerId, raw) {
  const verdict = evaluateDeal(s, rulerId, raw);
  if (verdict.status !== 'accept') return { ok: false, error: verdict.reason };
  const i = verdict.intent, k = kingdom(s, rulerId), player = kingdom(s, PLAYER);
  if (!isPlayerPromise(i)) { pay(player, { [i.giveResource]: i.giveAmount }); pay(k, { [i.giveResource]: i.giveAmount }, 1); }
  if (i.receiveAmount && i.type !== 'LOAN') { pay(k, { [i.receiveResource]: i.receiveAmount }); pay(player, { [i.receiveResource]: i.receiveAmount }, 1); }
  if (['WAR', 'BETRAY'].includes(i.type)) declareWar(s, PLAYER, rulerId);
  if (i.type === 'PEACE') { makePeace(s, PLAYER, rulerId); addTreaty(s, PLAYER, rulerId, 'peace', i.duration); }
  const type = { ALLIANCE: 'alliance', TRADE: 'trade', VASSALAGE: 'vassalage', NON_AGGRESSION: 'non-aggression', ACCESS: 'access', RECURRING: 'recurring' }[i.type];
  if (type) addTreaty(s, PLAYER, rulerId, type, i.duration);
  if (i.type === 'TERRITORY') { stateTransfer(s, i.targetId, PLAYER); }
  if (i.type === 'AID') { k.lastGiftTurn = s.turn; applyGift(s, PLAYER, rulerId, i.giveResource, i.giveAmount); }
  if (!isPlayerPromise(i) && i.giveAmount) recordTrade(s, PLAYER, rulerId, i.giveResource, i.giveAmount, i.type.toLowerCase());
  if (i.receiveAmount && i.type !== 'LOAN') recordTrade(s, rulerId, PLAYER, i.receiveResource, i.receiveAmount, i.type.toLowerCase());
  if (['TRIBUTE', 'VASSALAGE'].includes(i.type)) changeRelation(s, rulerId, PLAYER, { fear: 10, trust: -5, opinion: -8, grievance: 10 }, 'Coercion secured concessions, not friendship.');
  if (isPlayerPromise(i)) createPlayerPromise(s, rulerId, i);
  if (i.type === 'RECURRING') { const route=tradeRoute(s,PLAYER,rulerId);if(i.tradeKind==='preferential')route.fee=0;Object.assign(s.treaties.at(-1), { intent: i, payer: PLAYER, lastPaid: s.turn, anchors: contractAnchors(s,PLAYER,rulerId), legacyRoute:false, routeStatus:route.status });for(const party of [player,k])pay(party,{gold:route.fee}); }
  if (i.type === 'EMBARGO') {
    addTreaty(s, PLAYER, rulerId, 'embargo', i.duration); s.treaties.at(-1).targetId = i.targetId;
    s.treaties = s.treaties.filter(t => !(['trade', 'recurring'].includes(t.type) && t.parties.includes(i.targetId) && t.parties.some(id => [PLAYER, rulerId].includes(id))));
    changeRelation(s, i.targetId, PLAYER, { opinion: -15, grievance: 12 }, 'Ashen organized a trade embargo.');
  }
  if (i.type === 'LOAN') s.pledges.push({ id: `pledge-${s.nextId++}`, debtor: rulerId, creditor: PLAYER, intent: { ...i, type: 'PROMISE', giveAmount: i.receiveAmount, giveResource: i.giveResource, receiveAmount: 0 }, created: s.turn, deadline: s.turn + i.duration, status: 'pending', delivered: false, held: 0, breached: false, loan: true });
  if (['JOINT_WAR', 'DEFEND', 'POSITION', 'WITHDRAW', 'BUILD_DEFENSES'].includes(i.type)) {
    if (i.type === 'JOINT_WAR') { declareWar(s, PLAYER, i.targetId); declareWar(s, rulerId, i.targetId); }
    s.pledges.push({ id: `pledge-${s.nextId++}`, debtor: rulerId, creditor: PLAYER, eventAfter: s.nextId - 1, intent: i, deadline: s.turn + i.duration, status: 'pending', delivered: false, created: s.turn, held: 0, breached: false });
  }
  recordPoliticalMemory(s, rulerId, PLAYER, 'agreement', `${LABELS[i.type]} agreed with ${player.name}; ${i.giveAmount} ${i.giveResource}${i.type === 'PROMISE' ? ' promised' : ' paid'}.`, 7);
  log(s, `${LABELS[i.type]} with ${k.name} ratified.`, 'diplomacy');
  return { ok: true };
}
function stateTransfer(s, tileId, owner) { s.tiles[tileId].owner = owner; rebuildTerritory(s); }
export function deliverPledge(s, id) {
  const p = s.pledges.find(p => p.id === id && p.debtor === PLAYER && p.status === 'pending');
  if (s.outcome || !p || p.intent.type !== 'PROMISE') return { ok: false, error: 'That promise is no longer pending.' };
  const cost = { [p.intent.giveResource]: p.intent.giveAmount }, player = kingdom(s, PLAYER);
  if (!canAfford(player, cost)) return { ok: false, error: 'You cannot afford this delivery yet.' };
  pay(player, cost); pay(kingdom(s, p.creditor), cost, 1); p.delivered = true;
  recordTrade(s, PLAYER, p.creditor, p.intent.giveResource, p.intent.giveAmount, 'pledge');
  finishPledge(s, p, true); return { ok: true };
}
function finishPledge(s, p, fulfilled) {
  p.status = fulfilled ? 'fulfilled' : 'broken';
  const observer = kingdom(s, p.creditor), debtor = kingdom(s, p.debtor);
  debtor.reputation[fulfilled ? 'kept' : 'broken']++;
  const trust = fulfilled ? Math.round(6 + observer.honor * 6) : -Math.ceil(13 + observer.honor * 12);
  changeRelation(s, p.creditor, p.debtor, { opinion: fulfilled ? 8 : -18, trust, respect: fulfilled ? 8 : -10, grievance: fulfilled ? -3 : 20, reliability: fulfilled ? 8 : -18 }, `${LABELS[p.intent.type]} promise ${p.status}.`);
  if (!fulfilled) for (const k of s.kingdoms.filter(k => ![p.creditor, p.debtor].includes(k.id))) changeRelation(s, k.id, p.debtor, { opinion: -5, trust: -7, reliability: -4 }, 'A public oath to another House was broken.');
  recordPoliticalMemory(s, p.creditor, p.debtor, `promise-${p.status}`, `${debtor.name} ${p.status} a promise: ${LABELS[p.intent.type]}${p.intent.targetId ? ` against/at ${p.intent.targetId}` : ''}.`, 10);
  remember(s, p.debtor, `Our ${LABELS[p.intent.type]} pledge was ${p.status}.`, 8);
  if (p.debtor === PLAYER) contact(s, p.creditor, `oath-${p.id}`, fulfilled ? 'Your word was followed by deeds. We remember who stood by us when it mattered.' : `You gave your word: ${LABELS[p.intent.type]}. The deadline passed without your help. Do not expect another courteous speech to repair this.`, 100000);
  else if (p.creditor === PLAYER) contact(s, p.debtor, `oath-${p.id}`, fulfilled ? 'Our council has fulfilled the terms we agreed. Let the record show that our word was kept.' : 'We have failed to meet our obligation. I will not pretend otherwise.', 100000);
  log(s, `${debtor.name}: ${LABELS[p.intent.type]} pledge ${p.status}.`, 'diplomacy');
}
export function verifyPledges(s) {
  for (const p of s.pledges.filter(p => p.status === 'pending')) {
    const i = p.intent, forces = armiesOf(s, p.debtor), target = s.tiles[i.targetId];
    if (p.loan && s.turn >= p.deadline && canAfford(kingdom(s, p.debtor), { [i.giveResource]: i.giveAmount })) {
      pay(kingdom(s, p.debtor), { [i.giveResource]: i.giveAmount }); pay(kingdom(s, p.creditor), { [i.giveResource]: i.giveAmount }, 1); p.delivered = true;
      recordTrade(s, p.debtor, p.creditor, i.giveResource, i.giveAmount, 'repayment');
    }
    if (p.debtor === PLAYER && isPlayerPromise(i)) {
      const triggered = p.triggered, progress = promiseProgress(s, p);
      if (!triggered && p.triggered) contact(s, p.creditor, `called-${p.id}`, `${kingdom(s, p.conditionHouseId).name} has attacked us. Your oath is now called. We expect your banners by turn ${p.deadline}.`, 100000);
      if (progress === 'released') { p.status = 'released'; appendConversation(s, p.creditor, 'council', 'Conditional oath expired without its trigger. No penalty or reward.', { kind: 'pledge' }); }
      else if (progress !== 'pending') finishPledge(s, p, progress === 'fulfilled');
    } else {
      let complete = p.delivered;
      if (i.type === 'POSITION') complete = forces.some(a => a.tile === i.targetId);
      if (i.type === 'DEFEND') {
        if (p.lastVerified !== s.turn) { p.held = forces.some(a => a.tile === i.targetId) && target?.owner === p.creditor ? Math.min(2, p.held + 1) : 0; p.lastVerified = s.turn; }
        complete = p.held >= 2;
      }
      if (i.type === 'WITHDRAW') complete = !forces.some(a => s.tiles[a.tile].owner === p.creditor);
      if (i.type === 'BUILD_DEFENSES') complete = target?.owner === p.debtor && target.building === 'fort';
      if (i.type === 'JOINT_WAR') complete = s.militaryEvents.some(e => e.turn >= p.created && (p.eventAfter === undefined || (e.id || 0) > p.eventAfter) && e.attacker === p.debtor && e.defender === i.targetId);
      if (!alive(s, p.debtor) || p.breached) finishPledge(s, p, false);
      else if (complete) finishPledge(s, p, true);
      else if (s.turn >= p.deadline) finishPledge(s, p, false);
    }
    if (p.status === 'pending' && p.debtor === PLAYER && p.deadline - s.turn <= 2 && p.triggered !== false) {
      contact(s, p.creditor, `reminder-${p.id}`, `${p.deadline - s.turn} turns remain on your word: ${LABELS[i.type]}. Our council is watching for action.`, 2);
      if (p.lastReminder !== s.turn) { appendConversation(s, p.creditor, 'council', `PROMISE DUE IN ${p.deadline - s.turn} TURNS`, { kind: 'pledge' }); p.lastReminder = s.turn; }
    }
  }
  s.pledges = s.pledges.filter((p, i, all) => p.status === 'pending' || i >= all.length - 60);
}
export function resolveRecurringTrade(s) {
  for (const t of s.treaties.filter(t => t.type === 'recurring' && t.expires > s.turn && t.lastPaid < s.turn)) {
    const [payer, receiver] = [kingdom(s, t.payer), kingdom(s, t.parties.find(id => id !== t.payer))], i = t.intent;
    if (!alive(s, payer.id) || !alive(s, receiver.id) || atWar(s, payer.id, receiver.id) || tradeBlocked(s, payer.id, receiver.id)) { t.expires = s.turn; continue; }
    const route=t.legacyRoute?{fee:0,status:'Legacy supply'}:tradeRoute(s,payer.id,receiver.id);
    const blocked=t.legacyRoute?null:contractCheck(s,payer.id,receiver.id,i,{existing:true,anchors:t.anchors||[]});
    if(i.tradeKind==='preferential')route.fee=0;
    t.routeStatus=blocked||route.status;
    if(blocked){t.lastPaid=s.turn;t.disrupted=(t.disrupted||0)+1;if(t.disrupted>=3)t.expires=s.turn;continue;}
    t.disrupted=0;
    const give = { [i.giveResource]: i.giveAmount }, receive = { [i.receiveResource]: i.receiveAmount };
    give.gold=(give.gold||0)+route.fee;receive.gold=(receive.gold||0)+route.fee;
    if (!canAfford(payer, give) || !canAfford(receiver, receive)) {
      const failed = !canAfford(payer, give) ? payer : receiver, harmed = failed === payer ? receiver : payer;
      changeRelation(s, harmed.id, failed.id, { trust: -8, grievance: 8 }, 'A recurring shipment failed; the agreement ended.');
      recordPoliticalMemory(s, harmed.id, failed.id, 'trade-interrupted', `${failed.name} could not supply the agreed shipment.`, 8);
      const ruler = payer.id === PLAYER ? receiver.id : receiver.id === PLAYER ? payer.id : null;
      if (ruler) contact(s, ruler, `trade-${t.id}`, 'The agreed shipment could not be made. Our trade agreement has ended; we must negotiate terms our treasuries can sustain.', 100000);
      t.expires = s.turn; continue;
    }
    // Both sides are checked before either is charged. No partial or repeated payment.
    pay(payer, give); pay(receiver, {[i.giveResource]:i.giveAmount}, 1); pay(receiver, receive); pay(payer, {[i.receiveResource]:i.receiveAmount}, 1); t.lastPaid = s.turn;
    recordTrade(s, payer.id, receiver.id, i.giveResource, i.giveAmount, 'recurring'); recordTrade(s, receiver.id, payer.id, i.receiveResource, i.receiveAmount, 'recurring');
    for (const [a, b] of [[payer, receiver], [receiver, payer]]) changeRelation(s, a.id, b.id, { opinion: relation(s, a.id, b.id).opinion < 45 ? 1 : 0, trust: relation(s, a.id, b.id).trust < 35 ? 1 : 0 }, 'A reciprocal trade shipment arrived.');
  }
}
function aiDiplomacy(s) {
  // No language-model calls occur during turn resolution.
  if (s.turn % 6 !== 0) return;
  for (const k of s.kingdoms.filter(k => k.id !== PLAYER && alive(s, k.id))) {
    const danger = s.kingdoms.find(o => o.id !== k.id && alive(s, o.id) && relation(s, k.id, o.id).fear > 35 && relation(s, k.id, o.id).trust < 10);
    if (danger) {
      const partner = s.kingdoms.find(o => ![PLAYER, k.id, danger.id].includes(o.id) && alive(s, o.id) && !atWar(s, k.id, o.id) && !treaty(s, k.id, o.id, 'alliance') && (atWar(s, o.id, danger.id) || relation(s, o.id, danger.id).trust < 0));
      if (partner) { addTreaty(s, k.id, partner.id, 'alliance', 10); log(s, `${k.name} and ${partner.name} form a defensive alliance against the threat of ${danger.name}.`, 'diplomacy'); }
    }
    if (relation(s, k.id, PLAYER).grievance >= 50 && k.resources.food >= 80) {
      const recipient = s.kingdoms.find(o => ![PLAYER, k.id].includes(o.id) && alive(s, o.id) && atWar(s, o.id, PLAYER) && !atWar(s, o.id, k.id) && !tradeBlocked(s, o.id, k.id));
      if (recipient) { pay(k, {food: 15}); pay(recipient, {food: 15}, 1); recordTrade(s, k.id, recipient.id, 'food', 15, 'war-aid'); recordPoliticalMemory(s, k.id, PLAYER, 'war-aid', `We supplied ${recipient.name} against Ashen in response to our grievances.`, 8); }
    }
    const other = s.kingdoms.find(o => o.id !== PLAYER && o.id !== k.id && alive(s, o.id) && !atWar(s, k.id, o.id) && !treaty(s, k.id, o.id, 'trade') && relation(s, k.id, o.id).opinion >= 0 && !tradeBlocked(s, k.id, o.id));
    if (other) { addTreaty(s, k.id, other.id, 'trade', 12); log(s, `${k.name} and ${other.name} sign a trade accord.`, 'diplomacy'); }
    if (k.honor < .4 && k.ambition > .8 && treaty(s, k.id, PLAYER, 'alliance')) {
      const ratio = armiesOf(s, k.id).reduce((n, a) => n + strength(a), 0) / Math.max(1, armiesOf(s, PLAYER).reduce((n, a) => n + strength(a), 0));
      if (ratio > 2.2 && relation(s, k.id, PLAYER).opinion < 25) {
        declareWar(s, k.id, PLAYER);
        s.pledges.filter(p => p.debtor === k.id && p.creditor === PLAYER && p.status === 'pending').forEach(p => { p.breached = true; });
      }
    }
  }
}
export function endTurn(s) {
  if (s.outcome) return s;
  s.treaties = s.treaties.filter(t => t.expires > s.turn);
  updatePoliticalState(s, { sendDispatches: false });
  recordPlayerPlans(s); aiDiplomacy(s); aiResourceTrade(s); strategyTurn(s); resolveEspionage(s); resolveMovement(s); finishPlans(s); resolveEconomy(s); resolveAmbassadors(s);
  finishStrategyRound(s);
  s.turn++; resolveRecurringTrade(s); verifyPledges(s); updatePoliticalState(s); scheduleTrade(s);
  s.diplomacy.messages = { turn: s.turn, regular: 0, hosts: {} }; s.diplomacy.processedTurn = s.turn;
  s.treaties = s.treaties.filter(t => t.expires > s.turn && t.parties.every(id => alive(s, id)));
  checkVictory(s); rebuildTerritory(s);
  return s;
}
export function retrieveMemories(s, rulerId, message) {
  const words = new Set(message.toLowerCase().split(/\W+/).filter(w => w.length > 3));
  return kingdom(s, rulerId).memories.map(m => ({ ...m, score: .5 * Math.exp(-(s.turn - m.turn) / 20) + .3 * m.importance / 10 + .2 * m.text.toLowerCase().split(/\W+/).filter(w => words.has(w)).length })).sort((a, b) => b.score - a.score).slice(0, 5).map(m => `Turn ${m.turn}: ${m.text}`);
}
export function makeContext(s, rulerId, message, { proposal = null, event = null } = {}) {
  const k = kingdom(s, rulerId), r = relation(s, rulerId, PLAYER), military = borderThreat(s, rulerId, PLAYER), economy = economicRelationship(s, rulerId, PLAYER);
  const rows = s.kingdoms.filter(h => alive(s, h.id));
  // The current game has a public board, but a rival treasury, orders and private
  // conversations are not public intelligence. Never ship the whole campaign.
  const houses = rows.map(h => ({ id: h.id, name: h.name, armyStrength: Math.round(armiesOf(s, h.id).reduce((n, a) => n + strength(a), 0) / 10) * 10, settlements: settlements(s, h.id).slice(0, 8).map(t => ({ id: t.id, name: String(t.name || '').slice(0, 45), capital: t.capital === h.id })), atWarWith: rows.filter(o => atWar(s, h.id, o.id)).map(o => o.id), allies: rows.filter(o => o.id !== h.id && treaty(s, h.id, o.id, 'alliance')).map(o => o.id) }));
  const self = { economicNeeds: economicNeeds(s,rulerId), constructionPlan:k.economicPlan||null, resources: { ...k.resources }, production: grossProduction(s, rulerId), shortages: RESOURCES.filter(resource => k.resources[resource] < 40), buildings: Object.values(s.tiles).filter(t => t.owner === rulerId && t.building).slice(0, 18).map(t => ({ id: t.id, type: t.building, walls: t.walls, market: t.market, envoyOffice: !!t.envoyOffice, chancery: !!t.chancery })), armies: armiesOf(s, rulerId).slice(0, 16).map(a => ({ id: a.id, tile: a.tile, strength: Math.round(strength(a)), order: a.order, target: a.target })), strategicGoal: k.goal, priorities: diplomaticPriorities(s, rulerId) };
  const relationship = Object.fromEntries(['opinion', 'trust', 'respect', 'fear', 'wariness', 'dependency', 'grievance', 'reliability', 'generosity', 'aggression'].map(key => [key, r[key] ?? 0]));
  const context = {
    turn: s.turn, rulerId, message: message.slice(0, 600),
    history: (s.conversations[rulerId] || []).slice(-12).map(m => ({ role: m.role, text: m.text.slice(0, 600) })),
    memories: retrieveMemories(s, rulerId, message).map(m => m.slice(0, 500)), summary: k.memorySummary.slice(0, 900),
    world: {
      knowledge: 'Public map; approximate foreign strength. Foreign treasuries, private chats and unobserved orders are unknown.',
      houses, self, relationship, economicRelationship: economy,
      politicalPosture: politicalAttitude(s,rulerId,PLAYER),
      disclosedPlans: visiblePlans(s,PLAYER,rulerId).slice(0,3).map(r=>({planId:r.planId,observedTurn:r.turn,...r.snapshot})),
      intelligenceIncidents: (s.intelligence?.incidents||[]).filter(i=>[i.owner,i.actor].includes(rulerId)&&[i.owner,i.actor].includes(PLAYER)).slice(-4),
      conversationInterpretation: k.conversationSummary ? `Unverified ruler interpretation: ${k.conversationSummary}` : '',
      knownPlayerConstruction: Object.values(s.tiles).filter(t => t.owner === PLAYER && (t.building || t.project)).slice(0, 16).map(t => ({ tile: t.id, building: t.building, construction: t.project?.type || null })),
      militaryRelationship: { ...military, nearby: military.nearby.slice(0, 12) },
      recentChanges: (r.history || []).slice(-5),
      playerReputation: { ...kingdom(s, PLAYER).reputation },
      openNegotiations: (s.diplomacy.offers?.[rulerId] || []).slice(0, 3).map(i => { const v = evaluateDeal(s, rulerId, i); return { proposal: v.intent, status: v.status, reason: v.reason, counter: v.counter || null }; }),
      wars: s.wars.slice(0, 15), treaties: s.treaties.filter(t => t.parties.includes(rulerId) && t.expires > s.turn).slice(-12).map(t => ({ type: t.type, parties: t.parties, expires: t.expires, targetId: t.targetId })),
      pledges: s.pledges.filter(p => [p.debtor, p.creditor].includes(rulerId)).slice(-8).map(p => ({ debtor: p.debtor, creditor: p.creditor, intent: p.intent, deadline: p.deadline, status: p.status, triggered: p.triggered, held: p.held })),
      recentMilitaryEvents: s.militaryEvents.filter(e => [e.attacker, e.defender].includes(rulerId) || e.attacker === PLAYER).slice(-6),
      otherHouseRelations: rows.filter(h => ![rulerId, PLAYER].includes(h.id)).map(h => ({ id: h.id, opinion: relation(s, rulerId, h.id).opinion, trust: relation(s, rulerId, h.id).trust, alliance: !!treaty(s, rulerId, h.id, 'alliance') })),
      ambassadorPresent: stationedAmbassador(s, PLAYER, rulerId),
      persuasion: { kind: message ? message.toLowerCase().includes('sorry') ? 'apology' : 'conversation' : 'event', repeated: Object.entries(r.speech || {}).filter(([, value]) => value.count >= 2).map(([key]) => key) }
    }
  };
  if (proposal) {
    const v = evaluateDeal(s, rulerId, proposal);
    context.world.negotiation = { proposal: v.intent, status: v.status, counter: v.counter || null, reason: v.reason, factors: v.factors || [] };
  }
  if (event) context.world.dispatch = String(event).slice(0, 500);
  // Fixed collections plus a final whole-payload bound prevent save growth from
  // ever expanding model prompts. Drop old context first, never the current terms.
  while (JSON.stringify(context.world).length > 13000 && context.world.recentMilitaryEvents.length) context.world.recentMilitaryEvents.shift();
  while (JSON.stringify(context.world).length > 13000 && context.world.pledges.length) context.world.pledges.shift();
  if (JSON.stringify(context.world).length > 13000) { context.world.self.buildings = []; context.world.recentChanges = []; context.world.economicRelationship.recent = []; }
  const bytes = () => new TextEncoder().encode(JSON.stringify(context)).length;
  while (bytes() > 22000 && context.history.length) context.history.shift();
  while (bytes() > 22000 && context.memories.length) context.memories.shift();
  if (bytes() > 22000) { context.world.recentMilitaryEvents = []; context.world.recentChanges = []; context.world.self.buildings = []; context.world.knownPlayerConstruction = []; }
  // An imported save may have arbitrarily verbose optional event metadata. Only
  // the compact canonical projections above are required for a useful council.
  if (bytes() > 22000) { context.world.pledges = context.world.pledges.map(p => ({ debtor: p.debtor, creditor: p.creditor, type: p.intent.type, status: p.status, deadline: p.deadline })); context.world.economicRelationship.recent = []; context.world.economicRelationship.recurring = []; }
  return context;
}
export function acceptRulerMemories(s, rulerId, response) {
  // These are interpretations of the conversation, explicitly not verified facts.
  // Board-derived memory always takes priority in the rolling summary.
  if (response.relationshipSummary) kingdom(s, rulerId).conversationSummary = response.relationshipSummary;
  for (const text of (response.memoryCandidates || []).slice(0, 2)) {
    remember(s, rulerId, `Council interpretation (unverified): ${text}`, 3);
    Object.assign(kingdom(s, rulerId).memories.at(-1), { subject: PLAYER, kind: 'interpretation', verified: false });
  }
}
export function scriptedReply(s, rulerId, message, options = {}) {
  const k = kingdom(s, rulerId), r = relation(s, rulerId, PLAYER), text = message.toLowerCase();
  const proposedPromise = detectPromise(s, rulerId, message);
  if (proposedPromise) return { reply: `Then let us be precise. ${describeIntent(proposedPromise)}. The deadline is turn ${s.turn + proposedPromise.duration}. Is that your word?`, tone: 'guarded', intents: [], promiseDetected: proposedPromise, speechAct: 'promise' };
  if (/\b(?:might|maybe|perhaps|could|would)\b/.test(text) && /help|promise|aid|join|send/.test(text)) return { reply: 'Are you offering a firm commitment, Regent? Name what you will do, where, and by which turn. I will not mistake a possibility for your oath.', tone: 'guarded', intents: [], speechAct: 'question' };
  if (options.proposal) {
    const verdict = evaluateDeal(s, rulerId, options.proposal), i = verdict.intent;
    const opening = k.greed > .8 ? 'Let us speak of terms that profit both courts.' : k.honor > .8 ? 'I weigh the obligations as carefully as the payment.' : 'There is a price to committing my House.';
    return { reply: `${opening} ${verdict.reason}`, tone: verdict.status === 'reject' ? 'cold' : 'guarded', intents: i ? [i] : [], speechAct: verdict.status === 'counter' ? 'counteroffer' : verdict.status, ...(verdict.counter ? { counterProposal: verdict.counter } : {}) };
  }
  const posture=politicalAttitude(s,rulerId,PLAYER);
  if (/\b(?:spy|spies|espionage|intelligence)\b/.test(text) && !/allian|peace|truce|trade|gift|aid|declar.*war|attack you/.test(text)) {
    const incident=(s.intelligence?.incidents||[]).filter(i=>[i.owner,i.actor].includes(rulerId)&&[i.owner,i.actor].includes(PLAYER)).at(-1);
    return {reply:incident?`Our courts remember the ${incident.action} incident from turn ${incident.turn}. Such actions affect confidence between our Houses.`:'Our secrets remain our own. We can discuss the concerns and obligations between our courts.',tone:'guarded',intents:[]};
  }
  if (/\b(?:plans?|schemes?|intentions?|priority|priorities)\b/.test(text) && !/allian|peace|truce|trade|gift|aid|declar.*war|attack you/.test(text)) {
    const known=visiblePlans(s,PLAYER,rulerId)[0];
    return {reply:known?`Your report from turn ${known.turn} concerns ${known.text} Intentions can change when circumstances do.`:`${posture.label}: ${diplomaticPriorities(s,rulerId)[0]} I will not disclose private military preparations.`,tone:'guarded',intents:[]};
  }
  const military = borderThreat(s, rulerId, PLAYER), economic = economicRelationship(s, rulerId, PLAYER);
  const third = HOUSES.find(h => ![PLAYER, rulerId].includes(h.id) && text.includes(h.id));
  if (third && /think|feel|why|abandon|enemy|tell|opinion/.test(text)) {
    const other = relation(s, rulerId, third.id);
    const stance = atWar(s, rulerId, third.id) ? 'Their banners threaten us; aid against them would have weight.' : treaty(s, rulerId, third.id, 'alliance') ? 'We have a sworn alliance. I will not discard it for a pleasant conversation.' : other.opinion < 0 ? 'We have cause to distrust them.' : 'There is room for cooperation when our interests align.';
    return { reply: `${third.name}? ${stance}${r.trust < 0 ? ' You have not earned the rest of my confidence.' : ` ${k.priorities?.[0] || diplomaticPriorities(s, rulerId)[0]}`}`, intents: [], tone: 'guarded' };
  }
  if (/your (?:army|armies|soldiers|troops|banners)/.test(text) && /border|near/.test(text)) {
    const forces = borderThreat(s, PLAYER, rulerId).nearby;
    const pledge = s.pledges.find(p => p.debtor === rulerId && p.creditor === PLAYER && p.status === 'pending');
    return { reply: forces.length ? `Our banners stand at ${forces.slice(0, 3).map(a => a.tile).join(', ')}. ${pledge ? 'Their orders serve the military obligation we agreed.' : atWar(s, rulerId, PLAYER) ? 'We are at war, Regent. Their purpose should be clear.' : 'My captains are protecting the interests of this House. We can discuss access or a withdrawal on exact terms.'}` : 'Our armies are not concentrated at your frontier. If you have a particular destination in mind, name it.', tone: 'guarded', intents: [] };
  }
  if (/need|want|concern|soldiers near|army near/.test(text)) return { reply: `${diplomaticPriorities(s, rulerId).join(' ')}${economic.majorPartner ? ' Your shipments matter to us; I would prefer to preserve them.' : ''}`, intents: [], tone: 'neutral' };
  let type = /allian|ally|friend/.test(text) ? 'ALLIANCE' : /peace|truce/.test(text) ? 'PEACE' : /trade|commerce/.test(text) ? 'TRADE' : /gift|aid|donat/.test(text) ? 'AID' : (/declar.*war|attack you/.test(text) && !/\b(?:not|won't|never)\b/.test(text)) ? 'WAR' : null;
  const amount = Number(text.match(/\b(\d{1,4})\s*(?:gold|coins)\b/)?.[1] || (type === 'AID' ? 25 : 0));
  const intent = type ? validateIntent({ type, giveAmount: amount, duration: 10 }) : null;
  const repeated = Object.entries(r.speech || {}).some(([kind, value]) => ['praise', 'reassurance', 'apology'].includes(kind) && value.count >= 3);
  const opening = military.score >= 20 ? `Your soldiers stand close to ${settlements(s, rulerId)[0]?.name || 'our frontier'}. Explain their purpose before you speak of friendship.` : r.trust < 0 ? 'We remember the word you failed to keep. What deed will follow this speech?' : repeated ? 'You have praised our honor often enough, Regent. I would prefer to hear concrete terms.' : k.honor > .8 ? 'Your deeds give weight to your word in this hall.' : k.greed > .8 ? 'Prosperity is a language we both understand.' : k.aggression > .7 ? 'Speak plainly. My captains are waiting.' : 'I am listening, Regent.';
  return { reply: `${opening} ${intent ? evaluateDeal(s, rulerId, intent).reason : economic.majorPartner ? 'Our people benefit from your trade. Tell me what you seek in return.' : diplomaticPriorities(s, rulerId)[0]}`, intents: intent ? [intent] : [], tone: military.score >= 20 || r.trust < 0 ? 'guarded' : r.opinion > 0 ? 'neutral' : 'cold' };
}
