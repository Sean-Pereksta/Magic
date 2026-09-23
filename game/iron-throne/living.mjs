// Political state belongs to the simulation. Model output is never an action.
import { BUILDINGS, HOUSES, INTENT_TYPES, RESOURCES } from './data.mjs';
import { PLAYER, alive, armiesOf, atWar, canAfford, declareWar, distance, kingdom, log, moveCost, neighbors, passable, pay, relation, remember, settlements, strength, treaty } from './core.mjs';

export const RELATION_DEFAULTS = { respect: 15, fear: 0, grievance: 0, dependency: 0, wariness: 0, reliability: 50, generosity: 0, aggression: 0 };
export const clamp = (n, low = 0, high = 100) => Math.max(low, Math.min(high, n));
export const capitalOf = (s, owner) => settlements(s, owner).find(t => t.capital === owner) || settlements(s, owner)[0];
const powerOf = (s, owner) => armiesOf(s, owner).reduce((n, a) => n + strength(a), 0);
const houseName = (s, id) => kingdom(s, id)?.name || id;

export function initializeLiving(s) {
  s.ambassadors ||= [];
  s.diplomacy ||= { messages: { turn: s.turn, regular: 0, hosts: {} }, tradeHistory: [], incidents: [], warHistory: [], offers: {}, processedTurn: 0 };
  for (const k of s.kingdoms) {
    k.reputation ||= { kept: 0, broken: 0, envoysKilled: 0 };
    k.priorities ||= []; k.conversationSummary ??= '';
    for (const r of Object.values(k.relations)) {
      for (const [key, value] of Object.entries(RELATION_DEFAULTS)) r[key] ??= value;
      r.history ||= []; r.speech ||= {}; r.wordGain ??= 0; r.unread ??= 0;
      r.contacts ||= {}; r.observations ||= {}; r.gifts ||= []; r.sharedEnemies ||= []; r.movements ||= {};
    }
  }
  return s;
}

export function appendConversation(s, rulerId, role, text, { unread = false, kind = '', proposal = null } = {}) {
  const history = s.conversations[rulerId] ||= [];
  const entry = { role, text: String(text).slice(0, 1600), turn: s.turn, kind };
  if (proposal) entry.proposal = proposal;
  history.push(entry);
  // Important actions live in the ledger and structured memory after chat rolls off.
  if (history.length > 50) history.splice(0, history.length - 50);
  if (unread) relation(s, rulerId, PLAYER).unread = Math.min(99, (relation(s, rulerId, PLAYER).unread || 0) + 1);
  return entry;
}
export function markRead(s, rulerId) { relation(s, rulerId, PLAYER).unread = 0; }
export function changeRelation(s, observer, subject, changes, reason) {
  const r = relation(s, observer, subject);
  if (!r) return;
  const applied = {};
  for (const [key, delta] of Object.entries(changes)) {
    if (!['opinion', 'trust', ...Object.keys(RELATION_DEFAULTS)].includes(key) || !Number.isFinite(delta)) continue;
    const previous = r[key] ?? RELATION_DEFAULTS[key] ?? 0;
    r[key] = clamp(previous + delta, ['opinion', 'trust'].includes(key) ? -100 : 0);
    if (r[key] !== previous) applied[key] = Math.round((r[key] - previous) * 10) / 10;
  }
  if (!Object.keys(applied).length) return;
  r.history ||= [];
  r.history.push({ turn: s.turn, changes: applied, reason: String(reason).slice(0, 220) });
  r.history = r.history.slice(-24);
  if (subject === PLAYER && observer !== PLAYER) {
    const significant = Object.entries(applied).filter(([, n]) => Math.abs(n) >= 4);
    if (significant.length) appendConversation(s, observer, 'council', `${significant.map(([key, n]) => `${key.toUpperCase()} ${n > 0 ? '+' : ''}${n}`).join(' · ')} — ${reason}`, { kind: 'relationship' });
  }
}
export function recordPoliticalMemory(s, observer, subject, kind, text, importance = 7) {
  remember(s, observer, text, importance);
  const memory = kingdom(s, observer).memories.at(-1);
  memory.subject = subject; memory.kind = kind; memory.verified = true;
}
export function contact(s, rulerId, key, text, cooldown = 4) {
  if (rulerId === PLAYER || !alive(s, rulerId)) return false;
  const r = relation(s, rulerId, PLAYER), last = r.contacts?.[key];
  if (last !== undefined && s.turn - last < cooldown) return false;
  r.contacts ||= {}; r.contacts[key] = s.turn;
  // Keep per-event contact IDs bounded, even in a very long campaign.
  r.contacts = Object.fromEntries(Object.entries(r.contacts).sort((a, b) => b[1] - a[1]).slice(0, 36));
  appendConversation(s, rulerId, 'ruler', text, { unread: true, kind: key });
  return true;
}

export function diplomaticCapacity(s, owner = PLAYER) {
  const towns = settlements(s, owner);
  return towns.some(t => t.chancery && t.envoyOffice) ? 5 : towns.some(t => t.envoyOffice) ? 4 : 3;
}
export function stationedAmbassador(s, owner, host) {
  const capital = capitalOf(s, host);
  return !!capital && s.ambassadors?.some(a => a.owner === owner && a.host === host && a.status === 'stationed' && a.tile === capital.id && !a.path.length);
}
export function messageAllowance(s, rulerId) {
  const recorded = s.diplomacy?.messages;
  const used = recorded?.turn === s.turn ? recorded : { regular: 0, hosts: {} };
  const hosted = stationedAmbassador(s, PLAYER, rulerId);
  const limit = hosted ? 10 : diplomaticCapacity(s);
  return { hosted, limit, used: hosted ? used.hosts[rulerId] || 0 : used.regular, remaining: Math.max(0, limit - (hosted ? used.hosts[rulerId] || 0 : used.regular)), regularRemaining: Math.max(0, diplomaticCapacity(s) - used.regular) };
}
export function consumeMessage(s, rulerId) {
  if (s.outcome || rulerId === PLAYER || !alive(s, rulerId)) return { ok: false, error: 'This council is unavailable.' };
  const allowance = messageAllowance(s, rulerId);
  if (!allowance.remaining) return { ok: false, error: allowance.hosted ? 'Your ambassador has used all 10 messages with this House. End the turn to continue.' : 'Your dispatches are used for this turn. End the turn or station an ambassador.' };
  if (s.diplomacy.messages.turn !== s.turn) s.diplomacy.messages = { turn: s.turn, regular: 0, hosts: {} };
  const used = s.diplomacy.messages;
  if (allowance.hosted) used.hosts[rulerId] = (used.hosts[rulerId] || 0) + 1;
  else used.regular++;
  return { ok: true };
}

export function speechKind(message) {
  const text = message.toLowerCase();
  // Negated threats and apologies are not threats.
  if (/\b(?:destroy you|burn your|crush you|pay or|kneel or)\b/.test(text)) return 'threat';
  if (/\b(?:fool|coward|idiot|worthless)\b/.test(text)) return 'insult';
  if (/\b(?:sorry|apologi[sz]e|forgive me)\b/.test(text)) return 'apology';
  if (/\b(?:no harm|trust me|mean you no|not your enemy)\b/.test(text)) return 'reassurance';
  if (/\b(?:amazing|respect|friend|honou?r|admire|great ruler|wonderful)\b/.test(text)) return 'praise';
  return 'negotiation';
}
export function applySpeech(s, rulerId, message) {
  const r = relation(s, rulerId, PLAYER), kind = speechKind(message);
  if (kind === 'negotiation') return;
  r.speech ||= {};
  const old = r.speech[kind] || { count: 0, turn: 0 };
  r.speech[kind] = { count: Math.min(99, old.count + 1), turn: s.turn };
  if (['praise', 'apology', 'reassurance'].includes(kind)) {
    const threatened = borderThreat(s, rulerId, PLAYER).score >= 20;
    const gain = threatened || r.trust < 0 || r.grievance > 25 ? 0 : Math.max(0, Math.min(2 - old.count, 4 - (r.wordGain || 0)));
    if (gain) { r.wordGain = (r.wordGain || 0) + gain; changeRelation(s, rulerId, PLAYER, { opinion: gain }, 'Courteous words; deeds are still expected.'); }
  } else {
    // Fear is leverage, never friendship. Words alone cannot create military fear.
    const fear = kind === 'threat' && powerOf(s, PLAYER) > powerOf(s, rulerId) ? 4 : 0;
    changeRelation(s, rulerId, PLAYER, { opinion: -3, trust: -2, grievance: 3, fear }, kind === 'threat' ? 'A threat made in council.' : 'An insult made in council.');
    recordPoliticalMemory(s, rulerId, PLAYER, kind, `Ashen ${kind === 'threat' ? 'threatened' : 'insulted'} our House in council: “${message.slice(0, 180)}”`, 8);
  }
}
export function applyGift(s, giver, receiver, resource, amount) {
  const r = relation(s, receiver, giver), k = kingdom(s, receiver);
  r.gifts ||= []; r.gifts = r.gifts.filter(g => s.turn - g.turn < 12);
  // Need is evaluated against stores before the transfer. Repeated gifts saturate.
  const need = k.resources[resource] - amount < 30;
  const factor = 1 / (1 + r.gifts.length * r.gifts.length);
  const opinion = Math.floor(Math.min(need ? 18 : 8, amount / 5) * factor);
  changeRelation(s, receiver, giver, { opinion, trust: need ? Math.floor(4 * factor) : 0, generosity: Math.floor(8 * factor) }, need ? `Relief supplied during a ${resource} shortage.` : 'Foreign aid received.');
  r.gifts.push({ turn: s.turn, resource, amount }); r.gifts = r.gifts.slice(-12);
  if (need) recordPoliticalMemory(s, receiver, giver, 'relief', `${houseName(s, giver)} supplied ${amount} ${resource} during our shortage.`, 9);
}

export function grossProduction(s, owner) {
  const output = Object.fromEntries(RESOURCES.map(r => [r, 0]));
  for (const t of Object.values(s.tiles)) if (t.owner === owner) {
    for (const [r, n] of Object.entries(BUILDINGS[t.building]?.yield || {})) output[r] += n + (t.building === 'farm' && t.resource === 'food' ? 4 : 0);
    if (['town', 'city'].includes(t.building)) { output.food += t.building === 'city' ? 14 : 8; output.gold += t.building === 'city' ? 8 : 5; }
    if (t.market) output.gold += 12;
    if (t.workshop) output.iron += 3;
  }
  return output;
}
export function recordTrade(s, from, to, resource, amount, kind = 'exchange') {
  if (!amount) return;
  const history = s.diplomacy.tradeHistory;
  history.push({ turn: s.turn, from, to, resource, amount, kind });
  s.diplomacy.tradeHistory = history.filter(t => s.turn - t.turn < 12).slice(-120);
}
export function economicRelationship(s, observer, subject) {
  const trades = (s.diplomacy?.tradeHistory || []).filter(t => s.turn - t.turn < 8 && [observer, subject].includes(t.from) && [observer, subject].includes(t.to));
  const imports = Object.fromEntries(RESOURCES.map(r => [r, trades.filter(t => t.to === observer && t.resource === r).reduce((n, t) => n + t.amount, 0)]));
  const exports = Object.fromEntries(RESOURCES.map(r => [r, trades.filter(t => t.from === observer && t.resource === r).reduce((n, t) => n + t.amount, 0)]));
  const production = grossProduction(s, observer);
  const dependency = Math.round(Math.max(0, ...RESOURCES.map(r => imports[r] / Math.max(1, production[r] * 8 + imports[r]) * 100)));
  return { imports, exports, dependency, majorPartner: dependency >= 20, recent: trades.slice(-8), recurring: s.treaties.filter(t => t.type === 'recurring' && t.expires > s.turn && t.parties.includes(observer) && t.parties.includes(subject)).map(t => ({ intent: t.intent, expires: t.expires })) };
}
export function tradeBlocked(s, a, b) {
  return s.treaties.some(t => t.type === 'embargo' && t.expires > s.turn && ((t.parties.includes(a) && t.targetId === b) || (t.parties.includes(b) && t.targetId === a)));
}

export function borderThreat(s, observer, subject) {
  const border = Object.values(s.tiles).filter(t => t.owner === observer);
  const capital = capitalOf(s, observer), forces = armiesOf(s, subject);
  const permitted = !!(treaty(s, observer, subject, 'alliance') || treaty(s, observer, subject, 'access') || treaty(s, observer, subject, 'vassalage'));
  const shared = s.kingdoms.filter(k => atWar(s, observer, k.id) && atWar(s, subject, k.id)).map(k => k.id);
  const previous = relation(s, observer, subject)?.observations || {};
  const nearby = forces.map(a => {
    const t = s.tiles[a.tile];
    const borderDistance = border.length ? Math.min(...border.map(b => distance(t, b))) : 100;
    const capitalDistance = capital ? distance(t, capital) : 100;
    return { id: a.id, tile: a.tile, strength: Math.round(strength(a)), borderDistance, capitalDistance, inside: t.owner === observer, movement: previous[a.id] === undefined ? 'unobserved' : borderDistance < previous[a.id] ? 'approaching' : borderDistance > previous[a.id] ? 'withdrawing' : (relation(s, observer, subject)?.movements?.[a.id]?.turn === s.turn ? relation(s, observer, subject).movements[a.id].direction : 'holding'), sharedDestination: !!a.target && shared.includes(s.tiles[a.target]?.owner) };
  }).filter(a => a.borderDistance <= 3);
  const localPower = Math.max(20, powerOf(s, observer));
  let score = nearby.reduce((n, a) => n + a.strength / localPower * (a.inside ? 28 : 20 / (1 + a.borderDistance)) * (a.capitalDistance <= 3 ? 1.4 : 1) * (permitted ? a.sharedDestination ? .2 : .45 : 1) * (a.movement === 'approaching' ? 1.2 : 1), 0);
  const expansion = s.militaryEvents.filter(e => e.attacker === subject && e.action === 'capture' && s.turn - e.turn <= 6 && capital && distance(s.tiles[e.tile], capital) <= 8).length;
  score += expansion * 8;
  const k = kingdom(s, observer);
  score *= (.7 + k.paranoia * .7) * (stationedAmbassador(s, subject, observer) ? .9 : 1);
  return { score: Math.round(clamp(score)), nearby, relativeStrength: Math.round(powerOf(s, subject) / Math.max(1, powerOf(s, observer)) * 10) / 10, permitted, sharedEnemies: shared, recentConquests: expansion };
}
export function diplomaticPriorities(s, owner) {
  const k = kingdom(s, owner), tasks = [];
  const production = grossProduction(s, owner);
  for (const resource of RESOURCES) if (k.resources[resource] < (resource === 'food' ? 40 : 20)) tasks.push(`Acquire ${resource}; stores are low (${k.resources[resource]}).`);
  const threat = s.kingdoms.filter(o => o.id !== owner && alive(s, o.id)).map(o => ({ id: o.id, r: relation(s, owner, o.id) })).sort((a, b) => b.r.wariness - a.r.wariness)[0];
  if (threat?.r.wariness >= 20) tasks.push(`Protect our frontier from ${houseName(s, threat.id)}.`);
  const enemies = s.kingdoms.filter(o => atWar(s, owner, o.id));
  if (enemies.length > 1) tasks.push('Avoid a war on several fronts; seek peace with one enemy.');
  if (enemies[0]) tasks.push(`Weaken ${enemies[0].name} and obtain military support.`);
  const loss = s.militaryEvents.filter(e => e.defender === owner && e.action === 'capture' && s.tiles[e.tile]?.owner !== owner).at(-1);
  if (loss) tasks.push(`Recover ${s.tiles[loss.tile].name || loss.tile}.`);
  if (!tasks.length) tasks.push(k.greed > .8 ? 'Secure profitable imports and preserve trade routes.' : k.ambition > .8 ? 'Gain territory and isolate vulnerable rivals.' : 'Preserve peace and dependable alliances.');
  if (production.iron < 8 && k.aggression > .5) tasks.push('Secure iron for a stronger army.');
  return tasks.slice(0, 4);
}
export function updatePoliticalState(s, { sendDispatches = true } = {}) {
  for (const observer of s.kingdoms) for (const subject of s.kingdoms) {
    if (observer.id === subject.id || !alive(s, observer.id) || !alive(s, subject.id)) continue;
    const r = relation(s, observer.id, subject.id), threat = borderThreat(s, observer.id, subject.id), economic = economicRelationship(s, observer.id, subject.id);
    const wasWary = r.wariness;
    const newShared = threat.sharedEnemies.filter(id => !(r.sharedEnemies || []).includes(id));
    if (newShared.length) changeRelation(s, observer.id, subject.id, { opinion: 2 }, 'A shared enemy creates a limited common interest.');
    r.sharedEnemies = threat.sharedEnemies;
    r.movements = Object.fromEntries(threat.nearby.filter(a => ['approaching', 'withdrawing'].includes(a.movement)).slice(0, 80).map(a => [a.id, { direction: a.movement, turn: s.turn }]));
    changeRelation(s, observer.id, subject.id, { wariness: threat.score - r.wariness, dependency: economic.dependency - r.dependency, fear: clamp((threat.relativeStrength - 1) * 25 + threat.score * .35) - r.fear }, threat.score > wasWary ? `Armies approached ${capitalOf(s, observer.id)?.name || 'our frontier'}.` : threat.score < wasWary ? 'Foreign forces withdrew from the frontier.' : 'Trade and military circumstances changed.');
    const fresh = subject.id === PLAYER && observer.id !== PLAYER;
    if (fresh && sendDispatches) {
      if (threat.score >= 20 && threat.score > wasWary + 8) contact(s, observer.id, 'border', `Your banners are close to ${capitalOf(s, observer.id)?.name}. Tell me their purpose, Regent. Friendship needs more than courteous words.`);
      else if (wasWary >= 20 && threat.score < wasWary - 12) contact(s, observer.id, 'withdrawal', 'Your army has withdrawn. I noticed. Perhaps your intentions deserve another hearing.');
      if (observer.resources.food < 35) contact(s, observer.id, 'shortage', 'Our grain stores are running low. A food shipment would carry more weight than another declaration of friendship.', 6);
      if (threat.sharedEnemies.length && r.trust >= 0) contact(s, observer.id, 'shared-enemy', `${houseName(s, threat.sharedEnemies[0])} threatens us both. Shall we agree on actual military aid?`, 8);
      if (r.trust > 40 && !atWar(s, observer.id, PLAYER) && !treaty(s, observer.id, PLAYER, 'alliance')) contact(s, observer.id, 'alliance', 'You have given us reason to rely on your word. Let us discuss an alliance and its obligations.', 10);
    }
    r.observations = Object.fromEntries(armiesOf(s, subject.id).slice(0, 80).map(a => {
      const border = Object.values(s.tiles).filter(t => t.owner === observer.id);
      return [a.id, border.length ? Math.min(...border.map(t => distance(t, s.tiles[a.tile]))) : 100];
    }));
  }
  for (const k of s.kingdoms) {
    k.priorities = diplomaticPriorities(s, k.id);
    if (k.id !== PLAYER) {
      const r = relation(s, k.id, PLAYER), economic = economicRelationship(s, k.id, PLAYER);
      k.memorySummary = `${k.name} views Ashen with ${r.trust < 0 ? 'distrust' : r.trust > 40 ? 'confidence' : 'caution'}. ${r.wariness >= 20 ? 'Ashen forces threaten our frontier.' : 'No immediate Ashen border concentration.'} ${economic.majorPartner ? 'Ashen is an important supplier.' : 'Trade dependence is limited.'} Ashen has kept ${kingdom(s, PLAYER).reputation.kept} oaths and broken ${kingdom(s, PLAYER).reputation.broken}. ${k.memories.filter(m => m.importance >= 8 && m.verified !== false).slice(-3).map(m => m.text).join(' ')}`.slice(0, 900);
    }
  }
}
export function relationDescriptions(s, rulerId) {
  const r = relation(s, rulerId, PLAYER), k = kingdom(s, rulerId), pending = s.pledges.find(p => p.debtor === PLAYER && p.creditor === rulerId && p.status === 'pending');
  return [ ['Trust', r.trust < 0 ? 'Broken confidence' : r.trust >= 45 ? 'Dependable' : 'Cautious'], ['Trade', r.dependency >= 20 ? 'Important supplier' : r.dependency > 0 ? 'Occasional partner' : 'Limited exchange'], ['Military', r.wariness >= 50 ? 'Alarmed by your forces' : r.wariness >= 20 ? 'Concerned about the frontier' : 'No immediate border concern'], ['Reputation', r.reliability < 40 ? 'Unreliable promises' : r.reliability > 65 ? 'Proven word' : 'Still being judged'], ['Current interest', k.priorities?.[0] || diplomaticPriorities(s, rulerId)[0]], ['Promise', pending ? `Awaiting your oath · turn ${pending.deadline}` : 'No outstanding oath'], ['Ambassador', stationedAmbassador(s, PLAYER, rulerId) ? 'Present at court · 10 messages per turn; border concerns are easier to clarify' : 'No resident envoy'], ...(stationedAmbassador(s, PLAYER, rulerId) ? [['Envoy report', RESOURCES.map(resource => `${resource}: ${k.resources[resource]}`).join(' · ')]] : []) ];
}

export function ambassadorCapacity(s, owner = PLAYER) { const capacity = diplomaticCapacity(s, owner); return capacity === 5 ? 3 : capacity === 4 ? 1 : 0; }
function civilianPath(s, startId, endId) {
  // Immunity grants passage, not teleportation: mountains and water still block travel.
  const queue = [startId], came = new Map([[startId, null]]);
  for (let n = 0; n < queue.length; n++) {
    const id = queue[n];
    if (id === endId) { const path = []; let step = id; while (step !== startId) { path.unshift(step); step = came.get(step); } return path; }
    for (const t of neighbors(s, s.tiles[id])) if (passable(t) && !came.has(t.id)) { came.set(t.id, id); queue.push(t.id); }
  }
  return [];
}
export function recruitAmbassador(s, owner = PLAYER) {
  const k = kingdom(s, owner), home = capitalOf(s, owner), cost = { gold: 35, wood: 15 };
  if (s.outcome || !home || !ambassadorCapacity(s, owner)) return { ok: false, error: 'Build an Envoy Office in a settlement first.' };
  if (s.ambassadors.filter(a => a.owner === owner && a.status !== 'dead').length >= ambassadorCapacity(s, owner)) return { ok: false, error: 'Your ambassador capacity is full. A Royal Chancery supports three.' };
  if (k.commands < 1 || !canAfford(k, cost)) return { ok: false, error: 'Recruitment needs one order, 35 gold and 15 wood.' };
  pay(k, cost); k.commands--;
  const a = { id: `envoy-${s.nextId++}`, owner, tile: home.id, host: null, target: null, path: [], status: 'idle', detainedBy: null };
  s.ambassadors.push(a); return { ok: true, ambassadorId: a.id };
}
export function assignAmbassador(s, owner, id, host = null) {
  const a = s.ambassadors.find(a => a.id === id && a.owner === owner), capital = capitalOf(s, host || owner);
  if (s.outcome || !a || ['dead', 'detained'].includes(a.status) || !capital || (host && (host === owner || !alive(s, host)))) return { ok: false, error: 'This ambassador cannot take that assignment.' };
  const path = civilianPath(s, a.tile, capital.id);
  if (a.tile !== capital.id && !path.length) return { ok: false, error: 'There is no passable route to that capital.' };
  a.path = path; a.target = capital.id; a.host = host; a.status = path.length ? (host ? 'travelling' : 'returning') : (host ? 'stationed' : 'idle');
  return { ok: true };
}
export function ambassadorIncident(s, actor, id, action, confirmed = false) {
  const a = s.ambassadors.find(a => a.id === id), actions = ['passage', 'turn-away', 'expel', 'detain', 'release', 'execute'];
  if (s.outcome || !a || a.owner === actor || a.status === 'dead' || !actions.includes(action)) return { ok: false, error: 'No foreign ambassador can be addressed here.' };
  const control = a.detainedBy ? a.detainedBy === actor : s.tiles[a.tile]?.owner === actor || armiesOf(s, actor).some(force => distance(s.tiles[force.tile], s.tiles[a.tile]) <= 1);
  if (!control) return { ok: false, error: 'Your officials cannot reach this ambassador.' };
  if (action === 'execute' && !confirmed) return { ok: false, error: 'Execution requires explicit confirmation. Every House will remember this.' };
  if (action === 'release') {
    if (a.detainedBy !== actor) return { ok: false, error: 'This House is not holding that envoy.' };
    a.detainedBy = null; a.status = 'idle'; assignAmbassador(s, a.owner, a.id, null);
  } else if (action === 'detain') {
    if (!atWar(s, actor, a.owner)) return { ok: false, error: 'Ambassadors may only be detained during war.' };
    a.status = 'detained'; a.detainedBy = actor; a.path = [];
    changeRelation(s, a.owner, actor, { opinion: -8, grievance: 8 }, 'Our ambassador was detained during war.');
  } else if (action === 'execute') {
    a.status = 'dead'; a.path = []; a.host = null; a.target = null; a.detainedBy = null;
    kingdom(s, actor).reputation.envoysKilled++;
    s.diplomacy.incidents.push({ turn: s.turn, kind: 'execution', actor, victim: a.owner, ambassadorId: a.id });
    s.diplomacy.incidents = s.diplomacy.incidents.slice(-40);
    for (const observer of s.kingdoms.filter(k => k.id !== actor)) {
      const victim = observer.id === a.owner;
      changeRelation(s, observer.id, actor, { opinion: victim ? -80 : -Math.round(12 + observer.honor * 20), trust: victim ? -90 : -Math.round(15 + observer.honor * 25), grievance: victim ? 75 : 20, aggression: 40, fear: 10 }, `${houseName(s, actor)} executed an ambassador under diplomatic immunity.`);
      recordPoliticalMemory(s, observer.id, actor, 'ambassador-execution', `${houseName(s, actor)} executed ${houseName(s, a.owner)}'s ambassador.`, 10);
      if (actor === PLAYER) contact(s, observer.id, `execution-${a.id}`, victim ? 'You killed the envoy who came under my protection. Our treaties are ashes. My banners will answer this crime.' : 'We have heard what happened to the ambassador. Do not expect our courts to forget it.', 100000);
    }
    s.treaties = s.treaties.filter(t => !(t.parties.includes(actor) && t.parties.includes(a.owner)));
    declareWar(s, a.owner, actor);
    s.pledges.filter(p => p.debtor === actor && p.creditor === a.owner && p.status === 'pending').forEach(p => { p.breached = true; });
    log(s, `${houseName(s, actor)} executed ${houseName(s, a.owner)}'s ambassador.`, 'diplomacy');
  } else if (['turn-away', 'expel'].includes(action)) {
    if (a.status === 'detained') return { ok: false, error: 'Release the detained envoy first.' };
    assignAmbassador(s, a.owner, a.id, null);
    changeRelation(s, a.owner, actor, { opinion: -5, wariness: 5 }, 'Our ambassador was turned away.');
  }
  return { ok: true };
}
export function resolveAmbassadors(s) {
  for (const a of s.ambassadors) {
    if (['dead', 'detained'].includes(a.status)) continue;
    if (!alive(s, a.owner)) { a.status = 'idle'; a.path = []; a.host = null; continue; }
    if (a.host && (!alive(s, a.host) || capitalOf(s, a.host)?.id !== a.target)) assignAmbassador(s, a.owner, a.id, null);
    let budget = diplomaticCapacity(s, a.owner) === 5 ? 12 : 9;
    while (a.path.length && budget > 0) {
      const next = s.tiles[a.path[0]], current = s.tiles[a.tile];
      if (!passable(next) || distance(current, next) !== 1) { a.path = []; a.status = 'idle'; a.host = null; break; }
      const cost = moveCost(current, next); if (cost > budget) break;
      a.tile = a.path.shift(); budget -= cost;
    }
    if (!a.path.length && ['travelling', 'returning'].includes(a.status)) {
      a.status = a.host && capitalOf(s, a.host)?.id === a.tile ? 'stationed' : 'idle';
      if (a.status === 'stationed' && a.owner === PLAYER) {
        appendConversation(s, a.host, 'council', 'AMBASSADOR ARRIVED — 10 messages per turn with this court.', { unread: true, kind: 'ambassador' });
        contact(s, a.host, 'ambassador-arrival', 'Your envoy has arrived safely. There is now a direct channel between our courts.', 2);
      }
    }
  }
  // Recruit at most one foreign envoy per resolution; total unit count stays bounded.
  if (s.turn % 3 === 0) {
    const k = s.kingdoms.slice(1).find(k => alive(s, k.id) && ambassadorCapacity(s, k.id) && !s.ambassadors.some(a => a.owner === k.id && a.status !== 'dead'));
    if (k && alive(s, PLAYER) && !atWar(s, k.id, PLAYER)) { const result = recruitAmbassador(s, k.id); if (result.ok) assignAmbassador(s, k.id, result.ambassadorId, PLAYER); }
  }
  s.ambassadors = s.ambassadors.filter(a => a.status !== 'dead' || s.diplomacy.incidents.slice(-12).some(e => e.ambassadorId === a.id));
}

export function validateLivingSave(s) {
  const fail = () => { throw new Error('Damaged living diplomacy data.'); };
  const number = (v, max = 100000) => Number.isFinite(v) && v >= 0 && v <= max;
  const int = (v, max = 100000) => Number.isInteger(v) && number(v, max);
  const house = id => HOUSES.some(h => h.id === id);
  const list = (value, max) => Array.isArray(value) && value.length <= max;
  const object = v => v && typeof v === 'object' && !Array.isArray(v);
  const text = (v, max) => typeof v === 'string' && v.length <= max;
  const d = s.diplomacy;
  if (!object(d) || !object(d.messages) || !int(d.messages.turn) || !int(d.messages.regular, 5) || !object(d.messages.hosts) || Object.keys(d.messages.hosts).some(id => !house(id) || !int(d.messages.hosts[id], 10)) || !list(d.tradeHistory, 120) || !list(d.incidents, 40) || !list(d.warHistory, 80) || !int(d.processedTurn)) fail();
  if (!object(d.offers) || Object.keys(d.offers).some(id => !house(id))) fail();
  for (const offers of Object.values(d.offers)) if (!list(offers, 4) || offers.some(i => !object(i) || !INTENT_TYPES.includes(i.type) || !RESOURCES.includes(i.giveResource) || !RESOURCES.includes(i.receiveResource) || !int(i.giveAmount, 1000) || !int(i.receiveAmount, 1000) || !int(i.duration, 20) || i.duration < 1 || !text(i.targetId, 60))) fail();
  if (s.presentation !== undefined && (!object(s.presentation) || typeof s.presentation.reducedEffects !== 'boolean')) fail();
  for (const t of d.tradeHistory) if (!object(t) || !int(t.turn) || !house(t.from) || !house(t.to) || !RESOURCES.includes(t.resource) || !int(t.amount, 1000) || !text(t.kind, 30)) fail();
  for (const e of d.warHistory) if (!object(e) || !int(e.id, 10000000) || !int(e.turn) || !house(e.attacker) || !house(e.defender)) fail();
  for (const e of d.incidents) if (!object(e) || !int(e.turn) || e.kind !== 'execution' || !house(e.actor) || !house(e.victim) || !text(e.ambassadorId, 60)) fail();
  for (const k of s.kingdoms) {
    if (!text(k.conversationSummary, 360) || !object(k.reputation) || !['kept', 'broken', 'envoysKilled'].every(key => int(k.reputation[key])) || !list(k.priorities, 4) || k.priorities.some(p => !text(p, 240))) fail();
    for (const r of Object.values(k.relations)) {
      if (!Object.keys(RELATION_DEFAULTS).every(key => number(r[key], 100)) || !list(r.history, 24) || !list(r.gifts, 12) || !int(r.wordGain, 4) || !int(r.unread, 99) || !object(r.speech) || !object(r.contacts) || !object(r.observations) || !list(r.sharedEnemies, 4) || r.sharedEnemies.some(id => !house(id)) || !object(r.movements)) fail();
      for (const h of r.history) if (!object(h) || !int(h.turn) || !text(h.reason, 220) || !object(h.changes) || Object.entries(h.changes).some(([key, value]) => !['opinion', 'trust', ...Object.keys(RELATION_DEFAULTS)].includes(key) || !Number.isFinite(value) || Math.abs(value) > 200)) fail();
      if (Object.entries(r.speech).some(([key, v]) => !['praise', 'apology', 'reassurance', 'insult', 'threat'].includes(key) || !object(v) || !int(v.count, 99) || !int(v.turn))) fail();
      if (Object.keys(r.contacts).length > 36 || Object.entries(r.contacts).some(([key, turn]) => !text(key, 100) || !int(turn))) fail();
      if (Object.keys(r.observations).length > 80 || Object.entries(r.observations).some(([key, n]) => !text(key, 60) || !number(n, 100))) fail();
      if (Object.keys(r.movements).length > 80 || Object.entries(r.movements).some(([id, m]) => !text(id, 60) || !object(m) || !['approaching', 'withdrawing'].includes(m.direction) || !int(m.turn))) fail();
      for (const gift of r.gifts) if (!object(gift) || !int(gift.turn) || !RESOURCES.includes(gift.resource) || !int(gift.amount, 1000)) fail();
    }
    for (const m of k.memories) if ((m.subject !== undefined && !house(m.subject)) || (m.kind !== undefined && !text(m.kind, 40)) || (m.verified !== undefined && typeof m.verified !== 'boolean')) fail();
  }
  if (!list(s.ambassadors, 40) || new Set(s.ambassadors.map(a => a.id)).size !== s.ambassadors.length) fail();
  for (const a of s.ambassadors) if (!object(a) || !text(a.id, 60) || !house(a.owner) || !s.tiles[a.tile] || (a.host !== null && !house(a.host)) || (a.target !== null && !s.tiles[a.target]) || !list(a.path, 1200) || a.path.some(id => !s.tiles[id]) || !['idle', 'travelling', 'returning', 'stationed', 'detained', 'dead'].includes(a.status) || (a.detainedBy !== null && !house(a.detainedBy)) || (a.status === 'detained' && !a.detainedBy)) fail();
  for (const t of Object.values(s.tiles)) for (const key of ['envoyOffice', 'chancery']) if (t[key] !== undefined && typeof t[key] !== 'boolean') fail();
  for (const p of s.pledges) {
    if (p.conditionHouseId !== undefined && p.conditionHouseId !== null && !house(p.conditionHouseId)) fail();
    if (p.trackedArmies !== undefined && (!list(p.trackedArmies, 500) || p.trackedArmies.some(id => !text(id, 60)))) fail();
    if (p.eventAfter !== undefined && !int(p.eventAfter, 10000000)) fail();
    if (p.lastVerified !== undefined && !int(p.lastVerified)) fail();
    if (p.triggered !== undefined && typeof p.triggered !== 'boolean') fail();
    if (p.activatedTurn !== undefined && p.activatedTurn !== null && !int(p.activatedTurn)) fail();
  }
  for (const t of s.treaties) {
    if (t.type === 'embargo' && (!house(t.targetId) || t.parties.includes(t.targetId))) fail();
    if (t.type === 'recurring') {
      const i = t.intent;
      if (!object(i) || !RESOURCES.includes(i.giveResource) || !RESOURCES.includes(i.receiveResource) || !int(i.giveAmount, 1000) || !int(i.receiveAmount, 1000) || !i.giveAmount || !i.receiveAmount || !t.parties.includes(t.payer) || !int(t.lastPaid)) fail();
    }
  }
  for (const [id, messages] of Object.entries(s.conversations)) {
    if (!house(id)) fail();
    for (const m of messages) if (m.kind !== undefined && !text(m.kind, 100)) fail();
  }
  if (JSON.stringify(d).length > 60000 || JSON.stringify(s.ambassadors).length > 80000) fail();
}
