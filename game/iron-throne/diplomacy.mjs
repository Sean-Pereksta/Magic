import { HOUSES, INTENT_TYPES, RESOURCES } from './data.mjs';
import { PLAYER, alive, armiesOf, atWar, buildCheck, canAfford, checkVictory, declareWar, distance, findPath, kingdom, log, makePeace, pair, pay, rebuildTerritory, relation, remember, resolveEconomy, resolveMovement, settlements, shiftRelation, sizeOf, strategyTurn, strength, treaty } from './core.mjs';

export const LABELS = { ALLIANCE: 'Alliance', PEACE: 'Peace treaty', TRADE: 'Trade agreement', EXCHANGE: 'Resource exchange', AID: 'Gift / military aid', JOINT_WAR: 'Joint war', DEFEND: 'Defend a settlement', POSITION: 'Position an army', WITHDRAW: 'Withdraw troops', BUILD_DEFENSES: 'Build a fort', TERRITORY: 'Request a province', TRIBUTE: 'Demand tribute', VASSALAGE: 'Request allegiance', PROMISE: 'Promise a later payment', WAR: 'Declare war', BETRAY: 'Break treaties and declare war' };
const VALUES = { food: 1, wood: 1.2, stone: 1.5, iron: 2, gold: 1.5 };
const TYPES = new Set(INTENT_TYPES);
const FIELDS = new Set(['type', 'targetId', 'giveResource', 'giveAmount', 'receiveResource', 'receiveAmount', 'duration']);
export function validateIntent(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !TYPES.has(value.type) || Object.keys(value).some(k => !FIELDS.has(k))) return null;
  const i = { type: value.type, duration: value.duration ?? 10, giveResource: value.giveResource ?? 'gold', giveAmount: value.giveAmount ?? 0, receiveResource: value.receiveResource ?? 'food', receiveAmount: value.receiveAmount ?? 0, targetId: value.targetId ?? '' };
  if (!Number.isInteger(i.duration) || i.duration < 2 || i.duration > 20 || !RESOURCES.includes(i.giveResource) || !RESOURCES.includes(i.receiveResource) || !Number.isInteger(i.giveAmount) || i.giveAmount < 0 || i.giveAmount > 1000 || !Number.isInteger(i.receiveAmount) || i.receiveAmount < 0 || i.receiveAmount > 1000 || typeof i.targetId !== 'string' || i.targetId.length > 60) return null;
  return i;
}
export function validateResponse(raw) {
  try {
    const out = typeof raw === 'string' ? JSON.parse(raw) : raw;
    if (!out || typeof out.reply !== 'string' || !out.reply.trim() || out.reply.length > 800 || !Array.isArray(out.intents) || out.intents.length > 3 || !['warm', 'neutral', 'cold', 'hostile'].includes(out.tone)) return null;
    const intents = out.intents.map(validateIntent);
    if (intents.some(i => !i)) return null;
    return { reply: out.reply, intents, tone: out.tone };
  } catch { return null; }
}
export function describeIntent(i) {
  return `${LABELS[i.type]}${i.targetId ? ` · ${i.targetId}` : ''}${i.giveAmount ? ` · you give ${i.giveAmount} ${i.giveResource}${i.type === 'PROMISE' ? ' later' : ' now'}` : ''}${i.receiveAmount ? ` · you receive ${i.receiveAmount} ${i.receiveResource}` : ''} · ${i.duration} turns`;
}
export function evaluateDeal(s, rulerId, raw) {
  const intent = validateIntent(raw), k = kingdom(s, rulerId), player = kingdom(s, PLAYER);
  const reject = reason => ({ status: 'reject', reason, intent });
  if (s.outcome) return reject('The campaign is over.');
  if (!intent || !k || rulerId === PLAYER || !alive(s, rulerId)) return reject('The proposed terms are invalid.');
  const i = intent, r = relation(s, rulerId, PLAYER), value = i.giveAmount * VALUES[i.giveResource] - i.receiveAmount * VALUES[i.receiveResource];
  if (i.type !== 'PROMISE' && !canAfford(player, { [i.giveResource]: i.giveAmount })) return reject('Your treasury cannot cover this offer.');
  if (!canAfford(k, { [i.receiveResource]: i.receiveAmount })) return reject('That house does not possess the requested resources.');
  if (!['EXCHANGE', 'TRIBUTE'].includes(i.type) && i.receiveAmount !== 0) return reject('Resources can only be requested through an exchange or tribute.');
  if (['WAR', 'BETRAY', 'WITHDRAW', 'TRIBUTE', 'PROMISE'].includes(i.type) && i.type !== 'PROMISE' && i.giveAmount !== 0) return reject('This action does not accept an upfront payment.');
  if (['ALLIANCE', 'TRADE', 'VASSALAGE', 'DEFEND', 'POSITION', 'BUILD_DEFENSES', 'TERRITORY', 'PROMISE'].includes(i.type) && atWar(s, PLAYER, rulerId)) return reject('Negotiate peace before making this agreement.');
  if (['WAR', 'BETRAY'].includes(i.type)) {
    if (atWar(s, PLAYER, rulerId)) return reject('You are already at war.');
    return { status: 'accept', reason: treaty(s, PLAYER, rulerId) ? 'This breaks your treaties and damages your reputation with every house.' : 'Hostilities open borders for invasion. Armies still need marching orders.', intent };
  }
  if (i.type === 'PEACE' && !atWar(s, PLAYER, rulerId)) return reject('You are already at peace.');
  const treatyType = { ALLIANCE: 'alliance', TRADE: 'trade', VASSALAGE: 'vassalage' }[i.type];
  if (treatyType && treaty(s, PLAYER, rulerId, treatyType)) return reject('This agreement is already active.');
  if (i.type === 'AID') {
    if (i.giveAmount < 10) return reject('A meaningful gift is at least 10 resources.');
    if (k.lastGiftTurn === s.turn) return reject('This ruler has already received a gift this turn.');
    return { status: 'accept', reason: 'A gift improves opinion and trust once per turn.', intent };
  }
  if (i.type === 'EXCHANGE' && (!i.giveAmount || !i.receiveAmount || i.giveResource === i.receiveResource)) return reject('Offer two different resources, each with a positive amount.');
  if (i.type === 'TRIBUTE' && !i.receiveAmount) return reject('Specify the tribute you demand.');
  const playerPower = armiesOf(s, PLAYER).reduce((n, a) => n + strength(a), 0), rulerPower = armiesOf(s, rulerId).reduce((n, a) => n + strength(a), 0);
  if (i.type === 'VASSALAGE' && (playerPower < rulerPower * 1.8 || settlements(s, PLAYER).length < 2)) return reject('Allegiance requires at least two settlements and overwhelming military strength.');
  if (i.type === 'ALLIANCE' && r.trust < 0) return reject('Rebuild trust before requesting an alliance.');
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
  if (i.type === 'PROMISE') {
    if (i.giveAmount < 10 || i.giveAmount > 250) return reject('Pledge between 10 and 250 resources.');
    if (s.pledges.some(p => p.debtor === PLAYER && p.creditor === rulerId && p.status === 'pending')) return reject('Keep your existing promise before making another.');
    return { status: 'accept', reason: `You must deliver ${i.giveAmount} ${i.giveResource} from the pledge ledger by turn ${s.turn + i.duration}. Trust changes only after delivery.`, intent };
  }
  let threshold = { ALLIANCE: 85, PEACE: 30, TRADE: 32, EXCHANGE: 0, JOINT_WAR: 105, DEFEND: 45, POSITION: 40, WITHDRAW: 0, BUILD_DEFENSES: 80, TRIBUTE: 30, VASSALAGE: 120, TERRITORY: 500 }[i.type] ?? 9999;
  let utility = value * (.7 + k.greed * .35) + r.opinion * .5 + r.trust * .7;
  if (i.type === 'ALLIANCE') utility += k.honor * 20 - k.paranoia * 12;
  if (i.type === 'PEACE') utility += (playerPower - rulerPower) * .7;
  if (i.type === 'JOINT_WAR') utility += k.aggression * 30;
  if (['TRIBUTE', 'VASSALAGE'].includes(i.type)) utility += (playerPower - rulerPower) * .7;
  if (i.type === 'EXCHANGE') {
    // Friendship never permits arbitrage: resource value must balance independently.
    utility = value; threshold = 0;
  }
  if (i.type === 'WITHDRAW' && !atWar(s, PLAYER, rulerId)) utility = 100;
  if (utility >= threshold) return { status: 'accept', reason: 'The council accepts these terms. Ratify to make them binding.', intent };
  if (['TRIBUTE', 'VASSALAGE', 'WITHDRAW'].includes(i.type)) return reject('Your leverage does not justify this demand.');
  const extra = Math.ceil((threshold - utility) / (VALUES[i.giveResource] * (i.type === 'EXCHANGE' ? 1 : .7 + k.greed * .35))) + 2;
  const counter = { ...i, giveAmount: i.giveAmount + extra };
  if (counter.giveAmount <= 1000) return { status: 'counter', reason: `The ruler requests ${counter.giveAmount} ${counter.giveResource} upfront for these terms.`, intent, counter };
  return reject('The terms are too costly for this house.');
}

function addTreaty(s, a, b, type, duration) {
  s.treaties.push({ id: `treaty-${s.nextId++}`, type, parties: [a, b], expires: s.turn + duration });
}
export function commitDeal(s, rulerId, raw) {
  const verdict = evaluateDeal(s, rulerId, raw);
  if (verdict.status !== 'accept') return { ok: false, error: verdict.reason };
  const i = verdict.intent, k = kingdom(s, rulerId), player = kingdom(s, PLAYER);
  if (i.type !== 'PROMISE') { pay(player, { [i.giveResource]: i.giveAmount }); pay(k, { [i.giveResource]: i.giveAmount }, 1); }
  if (i.receiveAmount) { pay(k, { [i.receiveResource]: i.receiveAmount }); pay(player, { [i.receiveResource]: i.receiveAmount }, 1); }
  if (['WAR', 'BETRAY'].includes(i.type)) declareWar(s, PLAYER, rulerId);
  if (i.type === 'PEACE') { makePeace(s, PLAYER, rulerId); addTreaty(s, PLAYER, rulerId, 'peace', i.duration); }
  const type = { ALLIANCE: 'alliance', TRADE: 'trade', VASSALAGE: 'vassalage' }[i.type];
  if (type) addTreaty(s, PLAYER, rulerId, type, i.duration);
  if (i.type === 'TERRITORY') { stateTransfer(s, i.targetId, PLAYER); }
  if (i.type === 'AID') { k.lastGiftTurn = s.turn; shiftRelation(s, rulerId, PLAYER, Math.min(15, Math.floor(i.giveAmount / 4)), Math.min(8, Math.floor(i.giveAmount / 10))); }
  if (['JOINT_WAR', 'DEFEND', 'POSITION', 'WITHDRAW', 'BUILD_DEFENSES', 'PROMISE'].includes(i.type)) {
    if (i.type === 'JOINT_WAR') { declareWar(s, PLAYER, i.targetId); declareWar(s, rulerId, i.targetId); }
    s.pledges.push({ id: `pledge-${s.nextId++}`, debtor: i.type === 'PROMISE' ? PLAYER : rulerId, creditor: i.type === 'PROMISE' ? rulerId : PLAYER, intent: i, deadline: s.turn + i.duration, status: 'pending', delivered: false, created: s.turn, held: 0, breached: false });
  }
  remember(s, rulerId, `${LABELS[i.type]} agreed with ${player.name}; ${i.giveAmount} ${i.giveResource}${i.type === 'PROMISE' ? ' promised' : ' paid'}.`, 7);
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
  finishPledge(s, p, true); return { ok: true };
}
function finishPledge(s, p, fulfilled) {
  p.status = fulfilled ? 'fulfilled' : 'broken';
  shiftRelation(s, p.creditor, p.debtor, fulfilled ? 8 : -18, fulfilled ? 12 : -25);
  if (!fulfilled) for (const k of s.kingdoms.filter(k => ![p.creditor, p.debtor].includes(k.id))) shiftRelation(s, k.id, p.debtor, -5, -7);
  remember(s, p.creditor, `${kingdom(s, p.debtor).name} ${p.status} a promise: ${LABELS[p.intent.type]}.`, 9);
  remember(s, p.debtor, `Our ${LABELS[p.intent.type]} pledge was ${p.status}.`, 8);
  log(s, `${kingdom(s, p.debtor).name}: ${LABELS[p.intent.type]} pledge ${p.status}.`, 'diplomacy');
}
export function verifyPledges(s) {
  for (const p of s.pledges.filter(p => p.status === 'pending')) {
    const i = p.intent, forces = armiesOf(s, p.debtor), target = s.tiles[i.targetId];
    let complete = p.delivered;
    if (i.type === 'POSITION') complete = forces.some(a => a.tile === i.targetId);
    if (i.type === 'DEFEND') {
      // Arrive and remain on station for two resolutions, not one passing visit.
      p.held = forces.some(a => a.tile === i.targetId) && target?.owner === p.creditor ? p.held + 1 : 0;
      complete = p.held >= 2;
    }
    if (i.type === 'WITHDRAW') complete = !forces.some(a => s.tiles[a.tile].owner === p.creditor);
    if (i.type === 'BUILD_DEFENSES') complete = target?.owner === p.debtor && target.building === 'fort';
    if (i.type === 'JOINT_WAR') complete = s.militaryEvents.some(e => e.turn >= p.created && e.attacker === p.debtor && e.defender === i.targetId) || !alive(s, i.targetId);
    if (!alive(s, p.debtor) || p.breached) finishPledge(s, p, false);
    else if (complete) finishPledge(s, p, true);
    else if (s.turn >= p.deadline) finishPledge(s, p, false);
  }
  s.pledges = s.pledges.filter((p, i, all) => p.status === 'pending' || i >= all.length - 60);
}
function aiDiplomacy(s) {
  // No language-model calls occur during turn resolution.
  if (s.turn % 6 !== 0) return;
  for (const k of s.kingdoms.filter(k => k.id !== PLAYER && alive(s, k.id))) {
    const other = s.kingdoms.find(o => o.id !== PLAYER && o.id !== k.id && alive(s, o.id) && !atWar(s, k.id, o.id) && !treaty(s, k.id, o.id, 'trade') && relation(s, k.id, o.id).opinion >= 0);
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
  aiDiplomacy(s); strategyTurn(s); resolveMovement(s); resolveEconomy(s);
  s.turn++; verifyPledges(s);
  s.treaties = s.treaties.filter(t => t.expires > s.turn && t.parties.every(id => alive(s, id)));
  checkVictory(s); rebuildTerritory(s);
  return s;
}
export function retrieveMemories(s, rulerId, message) {
  const words = new Set(message.toLowerCase().split(/\W+/).filter(w => w.length > 3));
  return kingdom(s, rulerId).memories.map(m => ({ ...m, score: .5 * Math.exp(-(s.turn - m.turn) / 20) + .3 * m.importance / 10 + .2 * m.text.toLowerCase().split(/\W+/).filter(w => words.has(w)).length })).sort((a, b) => b.score - a.score).slice(0, 5).map(m => `Turn ${m.turn}: ${m.text}`);
}
export function makeContext(s, rulerId, message) {
  const k = kingdom(s, rulerId);
  return {
    turn: s.turn, rulerId, message,
    history: (s.conversations[rulerId] || []).slice(-6).map(m => ({ role: m.role, text: m.text.slice(0, 800) })),
    memories: retrieveMemories(s, rulerId, message), summary: k.memorySummary,
    world: {
      houses: s.kingdoms.filter(k => alive(s, k.id)).map(k => ({ id: k.id, resources: k.resources, armyStrength: Math.round(armiesOf(s, k.id).reduce((n, a) => n + strength(a), 0)), settlements: settlements(s, k.id).map(t => ({ id: t.id, name: t.name })) })),
      relationship: relation(s, rulerId, PLAYER), wars: s.wars, treaties: s.treaties.filter(t => t.parties.includes(rulerId)), pledges: s.pledges.filter(p => p.status === 'pending' && [p.debtor, p.creditor].includes(rulerId))
    }
  };
}
export function scriptedReply(s, rulerId, message) {
  const k = kingdom(s, rulerId), text = message.toLowerCase();
  let type = /allian|ally|friend/.test(text) ? 'ALLIANCE' : /peace|truce/.test(text) ? 'PEACE' : /trade|commerce/.test(text) ? 'TRADE' : /gift|aid|donat/.test(text) ? 'AID' : /declar.*war|attack you/.test(text) ? 'WAR' : null;
  const amount = Number(text.match(/\b(\d{1,4})\s*(?:gold|coins)\b/)?.[1] || (type === 'AID' ? 25 : 0));
  const intent = type ? validateIntent({ type, giveAmount: amount, duration: 10 }) : null;
  const opening = k.honor > .8 ? 'Your word carries weight in this hall.' : k.greed > .8 ? 'Prosperity is a language we both understand.' : k.aggression > .7 ? 'Speak plainly. My captains are waiting.' : 'I am listening, Regent.';
  return { reply: intent ? `${opening} Let our councils examine the terms below.` : `${opening} ${k.motto} Use the treaty desk to propose exact terms, name a military destination, or make a promise.`, intents: intent ? [intent] : [], tone: relation(s, rulerId, PLAYER).opinion > 0 ? 'neutral' : 'cold' };
}
