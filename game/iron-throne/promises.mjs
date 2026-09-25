import { RESOURCES } from './data.mjs';
import { PLAYER, alive, armiesOf, atWar, distance, findPath, kingdom, passable, settlements, treaty } from './core.mjs';
import { borderThreat, capitalOf } from './living.mjs';

export const PLAYER_PROMISES = new Set(['PROMISE', 'GUARANTEE', 'PLEDGE_WAR', 'PLEDGE_ATTACK', 'PLEDGE_DEFEND', 'PLEDGE_WITHDRAW', 'PLEDGE_BUILD', 'PLEDGE_PEACE']);
export const isPlayerPromise = i => PLAYER_PROMISES.has(i?.type);
export function playerPromiseCheck(s, rulerId, i, actorHouseId = PLAYER) {
  if (!isPlayerPromise(i)) return 'Unknown promise.';
  if (s.pledges.filter(p => p.debtor === actorHouseId && p.creditor === rulerId && p.status === 'pending').length >= 3) return 'Resolve your existing promises to this House before making another.';
  if (s.pledges.some(p => p.debtor === actorHouseId && p.creditor === rulerId && p.status === 'pending' && p.intent.type === i.type && p.intent.targetId === i.targetId)) return 'This oath is already in your ledger.';
  if (i.receiveAmount) return 'A promise cannot withdraw resources from another treasury.';
  if (i.type !== 'PROMISE' && i.giveAmount) return 'A military promise has no upfront resource transfer.';
  if (i.conditionHouseId && (!['GUARANTEE', 'PLEDGE_WAR'].includes(i.type) || i.conditionHouseId !== i.targetId)) return 'Conditional oaths must name the House whose attack triggers your promise.';
  if (i.type === 'PROMISE') return i.giveAmount < 10 || i.giveAmount > 250 ? 'Pledge between 10 and 250 resources.' : null;
  if (['GUARANTEE', 'PLEDGE_WAR', 'PLEDGE_PEACE'].includes(i.type)) {
    if (!alive(s, i.targetId) || [actorHouseId, rulerId].includes(i.targetId) && i.type !== 'PLEDGE_PEACE') return 'Name a surviving third House.';
    if (i.targetId === actorHouseId) return 'Choose a foreign House.';
    if (i.type !== 'PLEDGE_PEACE' && treaty(s, actorHouseId, i.targetId)) return 'Existing treaties prevent this military oath.';
    if (i.type === 'PLEDGE_PEACE' && atWar(s, actorHouseId, i.targetId)) return 'Make peace before promising not to attack that House.';
    if (i.type === 'PLEDGE_WAR' && !i.conditionHouseId && atWar(s, actorHouseId, i.targetId)) return 'You have already declared war. Promise actual combat instead.';
    if ((i.type === 'GUARANTEE' || i.conditionHouseId) && atWar(s, rulerId, i.targetId)) return 'That war is already underway. Make an unconditional promise of assistance.';
  }
  if (i.type === 'PLEDGE_ATTACK') {
    const army = s.armies.find(a => a.id === i.targetId), tile = s.tiles[i.targetId];
    const owner = army?.owner || tile?.owner;
    if (!owner || [actorHouseId, rulerId].includes(owner) || treaty(s, actorHouseId, owner)) return 'Name an enemy army or enemy-held map location that treaties allow you to attack.';
    if (!armiesOf(s, actorHouseId).length) return 'You have no army to commit.';
  }
  if (i.type === 'PLEDGE_DEFEND') {
    const t = s.tiles[i.targetId];
    if (!t || t.owner !== rulerId || !['city', 'town', 'fort'].includes(t.building)) return 'Select a settlement or fort belonging to this ruler.';
    if (i.duration < 2) return 'Defense requires two turns on station.';
    if (!armiesOf(s, actorHouseId).some(a => a.tile === t.id || findPath(s, a.tile, t.id, actorHouseId).length)) return 'Obtain military access before pledging defense of this location.';
  }
  if (i.type === 'PLEDGE_BUILD') {
    const t = s.tiles[i.targetId];
    if (i.duration < 3 || !t || t.owner !== actorHouseId || !passable(t) || t.building || t.project) return 'Select empty owned land and allow at least three turns to build a fort.';
  }
  if (i.type === 'PLEDGE_WITHDRAW' && !borderThreat(s, rulerId, actorHouseId).nearby.length) return 'There are no armies of your House near this House to withdraw.';
  return null;
}
export function createPlayerPromise(s, rulerId, intent, actorHouseId = PLAYER) {
  const i = { ...intent };
  const p = { id: `pledge-${s.nextId++}`, debtor: actorHouseId, creditor: rulerId, intent: i, created: s.turn, deadline: s.turn + i.duration, status: 'pending', delivered: false, held: 0, breached: false, eventAfter: s.nextId - 1, conditionHouseId: i.conditionHouseId || (i.type === 'GUARANTEE' ? i.targetId : null), triggered: !(i.conditionHouseId || i.type === 'GUARANTEE'), activatedTurn: null };
  if (i.type === 'PLEDGE_WITHDRAW') p.trackedArmies = borderThreat(s, rulerId, actorHouseId).nearby.map(a => a.id);
  if (i.type === 'PLEDGE_ATTACK') { const a = s.armies.find(a => a.id === i.targetId); p.targetOwner = a?.owner || s.tiles[i.targetId]?.owner; }
  s.pledges.push(p);
  return p;
}
export function promiseProgress(s, p) {
  const i = p.intent, forces = armiesOf(s, p.debtor), target = s.tiles[i.targetId];
  const events = s.militaryEvents.filter(e => e.turn >= p.created && (p.eventAfter === undefined || (e.id || 0) > p.eventAfter) && e.attacker === p.debtor);
  if (p.breached || !alive(s, p.debtor)) return 'broken';
  if (p.conditionHouseId && !p.triggered) {
    const attack = s.diplomacy.warHistory.find(e => e.id > p.eventAfter && e.attacker === p.conditionHouseId && e.defender === p.creditor);
    if (attack) { p.triggered = true; p.activatedTurn = s.turn; }
    else return s.turn >= p.deadline ? 'released' : 'pending';
  }
  let complete = p.delivered;
  if (i.type === 'PLEDGE_WAR' || i.type === 'GUARANTEE') complete = atWar(s, p.debtor, i.targetId);
  if (i.type === 'PLEDGE_ATTACK') complete = events.some(e => e.defender === p.targetOwner && (e.tile === i.targetId || e.defenderArmyId === i.targetId));
  if (i.type === 'PLEDGE_WITHDRAW') {
    const owned = Object.values(s.tiles).filter(t => t.owner === p.creditor);
    const tracked = p.trackedArmies.map(id => forces.find(a => a.id === id));
    // Destruction/merging is not withdrawal; new stacks cannot replace a border threat.
    complete = tracked.every(a => a && owned.every(t => distance(t, s.tiles[a.tile]) > 3)) && !borderThreat(s, p.creditor, p.debtor).nearby.length;
  }
  if (i.type === 'PLEDGE_DEFEND') {
    if (p.lastVerified !== s.turn) { p.held = target?.owner === p.creditor && forces.some(a => a.tile === i.targetId) ? Math.min(2, p.held + 1) : 0; p.lastVerified = s.turn; }
    complete = p.held >= 2;
  }
  if (i.type === 'PLEDGE_BUILD') complete = target?.owner === p.debtor && target.building === 'fort';
  if (i.type === 'PLEDGE_PEACE') {
    if (s.diplomacy.warHistory.some(e => e.id > p.eventAfter && e.attacker === p.debtor && e.defender === i.targetId)) return 'broken';
    complete = s.turn >= p.deadline;
  }
  return complete ? 'fulfilled' : s.turn >= p.deadline ? 'broken' : 'pending';
}

// Scripted interpretation deliberately requires an explicit first-person commitment.
// Even a confidently parsed statement produces a proposal, never a ledger entry.
export function detectPromise(s, rulerId, message, actorHouseId = PLAYER) {
  const text = message.toLowerCase().replace(/[’]/g, "'");
  if (!/\b(?:i will|i'll|i promise|i pledge|i give my word|i won't|i will not)\b/.test(text) || /\b(?:might|maybe|perhaps|could|would|not sure|don't promise|do not promise)\b/.test(text)) return null;
  if (/\bi (?:will not|won't)\s+(?!attack\b)/.test(text)) return null;
  const named = s.kingdoms.filter(h => h.id !== actorHouseId && text.includes(h.id));
  const third = named.find(h => h.id !== rulerId);
  const duration = /next turn/.test(text) ? 1 : Number(text.match(/(?:in|within|give me)\s+(\d{1,2})\s+turn/)?.[1] || 5);
  const deadline = text.match(/(?:before|by)(?: the end of)? turn\s+(\d{1,6})/);
  const i = { type: '', targetId: '', giveAmount: 0, giveResource: 'gold', receiveAmount: 0, receiveResource: 'food', duration: deadline ? Number(deadline[1]) - s.turn : duration };
  const resource = text.match(/\b(\d{1,3})\s+(food|wood|stone|iron|gold)\b/);
  if (resource && /\b(?:send|deliver|give|supply|pay)\b/.test(text)) Object.assign(i, { type: 'PROMISE', giveAmount: Number(resource[1]), giveResource: resource[2] });
  else if (/\b(?:won't|will not|not to)\s+attack\b/.test(text) && named[0]) Object.assign(i, { type: 'PLEDGE_PEACE', targetId: named[0].id });
  else if (/\b(?:withdraw|move (?:my|our|the) (?:army|troops|soldiers) away)\b/.test(text)) i.type = 'PLEDGE_WITHDRAW';
  else if (/\b(?:defend|protect)\b/.test(text) && !third) {
    const target = settlements(s, rulerId).find(t => text.includes(t.name.toLowerCase())) || (/capital/.test(text) ? capitalOf(s, rulerId) : null);
    if (target) Object.assign(i, { type: 'PLEDGE_DEFEND', targetId: target.id });
  } else if (third && /\b(?:join|help|aid|fight|war|assist)\b/.test(text)) {
    Object.assign(i, { type: 'PLEDGE_WAR', targetId: third.id });
    if (/\bif\b/.test(text) && /\battacks?\b/.test(text)) i.conditionHouseId = third.id;
    else if (/\bif\b/.test(text)) return null; // Do not silently strip an unknown condition.
  } else if (/\b(?:build|construct)\b/.test(text) && /\b(?:fort|defenses|defences)\b/.test(text)) {
    const tile = text.match(/\b\d{1,2},\d{1,2}\b/)?.[0];
    if (tile) Object.assign(i, { type: 'PLEDGE_BUILD', targetId: tile });
  } else if (/\battack\b/.test(text)) {
    const target = s.armies.find(a => a.owner !== actorHouseId && text.includes(a.id))?.id || text.match(/\b\d{1,2},\d{1,2}\b/)?.[0];
    if (target) Object.assign(i, { type: 'PLEDGE_ATTACK', targetId: target });
  }
  return i.type && i.duration >= 1 && i.duration <= 20 && RESOURCES.includes(i.giveResource) ? i : null;
}
