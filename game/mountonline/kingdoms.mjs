import { FACTIONS, UNITS, armySize, armyPower, safeArmy, clamp, clone, hash } from "./data.mjs";
import { battleArmy } from "./world.mjs";

const IDS = () => FACTIONS.map((f) => f.id);
const keyFor = (a, b) => [a, b].sort().join(":");
const factionOf = (s) => s.currentFaction || s.faction;
const bounded = (array, value, limit) => { array.push(value); if (array.length > limit) array.splice(0, array.length - limit); };
const check = (ok, message) => { if (!ok) { const e = new Error(message); e.code = "INVALID_ACTION"; e.status = 400; throw e; } };
const number = (value, fallback = 0, low = 0, high = 1e9) => Number.isFinite(value) ? clamp(value, low, high) : fallback;
const durations = { pact: 600, trade: 600, passage: 360, defensiveAlliance: 720, militaryAlliance: 600, ceasefire: 180, peace: 900, protection: 600, tribute: 360 };
const treatyActions = new Set(Object.keys(durations));
const actions = new Set(["gift", "pact", "trade", "passage", "defensiveAlliance", "militaryAlliance", "aid", "war", "ceasefire", "peace", "prisoners", "tribute", "territory", "protection", "accept", "break"]);

export function ensureDiplomacy(p) {
  p.relations ??= {};
  for (const id of IDS()) p.relations[id] = number(p.relations[id], 0, -100, 100);
  const d = p.diplomacy ??= {};
  d.trust ??= {};
  for (const id of IDS()) d.trust[id] = number(d.trust[id], ["stone", "marsh"].includes(id) ? 8 : 20, -100, 100);
  d.treaties = Array.isArray(d.treaties) ? d.treaties.filter((t) => t && IDS().includes(t.faction) && treatyActions.has(t.type) && Number.isFinite(t.until)).slice(-120) : [];
  d.offers = Array.isArray(d.offers) ? d.offers.filter((o) => o && /^o-\d{1,9}$/.test(o.id) && IDS().includes(o.faction) && actions.has(o.action) && !["accept", "break", "war"].includes(o.action) && Number.isInteger(o.gold) && o.gold >= 0 && o.gold <= 100000 && Number.isFinite(o.expires) && Array.isArray(o.reasons)).slice(-24) : [];
  d.history = Array.isArray(d.history) ? d.history.filter((h) => h && typeof h.message === "string").slice(-40) : [];
  d.prisoners = Array.isArray(d.prisoners) ? d.prisoners.filter((n) => n && typeof n.id === "string" && IDS().includes(n.faction) && Number.isFinite(n.until)).slice(-30) : [];
  const wars = d.wars;
  d.wars = {};
  for (const id of IDS()) if (wars?.[id] && typeof wars[id] === "object") {
    const w = wars[id];
    d.wars[id] = { started: number(w.started), score: number(w.score, 0, -100, 100), exhaustion: number(w.exhaustion, 0, 0, 100), objective: String(w.objective || "Defend the realm").slice(0, 160), target: typeof w.target === "string" ? w.target.slice(0, 80) : null };
  }
  d.nextOffer = Math.floor(number(d.nextOffer, 1, 1, 999999999));
  return d;
}

export function createKingdomState(world, saved = {}) {
  if (!saved || typeof saved !== "object") saved = {};
  const state = {
    version: 1, clock: number(saved.clock, 0, 0, 12), step: Math.floor(number(saved.step)),
    factions: {}, relations: {}, wars: [], history: [], caravans: {},
  };
  for (const f of FACTIONS) {
    const s = saved.factions?.[f.id] || {};
    state.factions[f.id] = { id: f.id, wealth: number(s.wealth, 900, 0, 1e7), exhaustion: number(s.exhaustion, 0, 0, 100), strength: (world.npcs || []).filter((n) => n.faction === f.id && n.kind === "patrol").reduce((n, l) => n + armyPower(l.army), 0) + world.settlements.filter((s) => !s.owner && factionOf(s) === f.id).reduce((n, s) => n + s.garrison * 1.5, 0), tribute: number(s.tribute), lastOffer: number(s.lastOffer), grievances: number(s.grievances, 0, 0, 100) };
  }
  for (let i = 0; i < FACTIONS.length; i++) for (let j = i + 1; j < FACTIONS.length; j++) {
    const a = FACTIONS[i].id, b = FACTIONS[j].id, key = keyFor(a, b), s = saved.relations?.[key];
    const rivals = (a === "crown" && b === "ember") || (a === "ember" && b === "vale") || (a === "iron" && b === "marsh");
    state.relations[key] = { a, b, relation: number(s?.relation, rivals ? -42 : ((hash(i, j) % 35) - 8), -100, 100), trust: number(s?.trust, rivals ? 5 : 25, -100, 100), war: !!s?.war, treaties: Array.isArray(s?.treaties) ? s.treaties.filter((t) => treatyActions.has(t.type) && Number.isFinite(t.until)).slice(-6) : [] };
  }
  state.wars = Array.isArray(saved.wars) ? saved.wars.filter((w) => IDS().includes(w.a) && IDS().includes(w.b) && typeof w.id === "string").slice(-20).map((w) => ({ ...w, score: number(w.score, 0, -100, 100), started: number(w.started), exhaustion: number(w.exhaustion, 0, 0, 100) })) : [];
  state.history = Array.isArray(saved.history) ? saved.history.filter((h) => typeof h.message === "string").slice(-60) : [];
  for (const [id, count] of Object.entries(saved.caravans || {}).slice(0, 64)) state.caravans[id] = Math.floor(number(count));
  world.kingdoms = state;
  return state;
}

export function kingdomHostile(state, a, b) { return a !== b && !!state?.relations?.[keyFor(a, b)]?.war; }
export function playerTreaty(p, faction, type, time = 0) { return p.diplomacy?.treaties?.some((t) => t.faction === faction && t.type === type && t.until > time) || false; }
export function playerHostile(p, state, faction, time = 0) {
  if (p.diplomacy?.wars?.[faction]) return true;
  if (["peace", "ceasefire", "pact", "passage", "militaryAlliance", "defensiveAlliance", "protection", "tribute"].some((t) => playerTreaty(p, faction, t, time))) return false;
  return (p.relations?.[faction] || 0) < -25;
}

function history(world, time, message, p = null) {
  bounded(world.kingdoms.history, { time, message }, 60);
  if (p) bounded(ensureDiplomacy(p).history, { time, message }, 40);
}
function treaty(p, faction, type, time, terms = {}) {
  const d = ensureDiplomacy(p);
  d.treaties = d.treaties.filter((t) => !(t.faction === faction && t.type === type));
  bounded(d.treaties, { faction, type, started: time, until: time + durations[type], ...terms }, 120);
}
function offer(p, faction, action, gold, time, reasons, extra = {}) {
  const d = ensureDiplomacy(p);
  // IDs are persisted with the profile and never derived from untrusted input.
  let id;
  do { id = `o-${d.nextOffer}`; d.nextOffer = d.nextOffer >= 999999999 ? 1 : d.nextOffer + 1; } while (d.offers.some((o) => o.id === id));
  const o = { id, faction, action, gold, expires: time + 180, status: "pending", reasons: reasons.slice(0, 4), ...extra };
  bounded(d.offers, o, 24);
  return o;
}
function decision(p, status, action, faction, reasons, time, counteroffer = null) {
  const entry = { status, action, faction, reasons, time, ...(counteroffer ? { offerId: counteroffer.id } : {}) };
  p.diplomacy.lastDecision = entry;
  return `${FACTIONS.find((f) => f.id === faction)?.name || faction}: ${status}. ${reasons.join(" ")}${counteroffer ? ` Counteroffer: ${counteroffer.gold} gold (envoy ${counteroffer.id}).` : ""}`;
}

function negotiation(p, world, faction, action, gold, input, time) {
  const d = ensureDiplomacy(p), k = world.kingdoms.factions[faction], f = FACTIONS.find((f) => f.id === faction), relation = p.relations[faction], trust = d.trust[faction], war = d.wars[faction];
  const strength = armyPower(p.army) + world.settlements.filter((s) => s.owner === p.uid).reduce((n, s) => n + s.garrison * 2, 0);
  const advantage = clamp((strength - k.strength) / 30, -20, 25), exhaustion = (war?.exhaustion || 0) + k.exhaustion, score = war?.score || 0;
  let value = relation * 0.45 + trust * 0.5 + gold / 5 + advantage;
  const reasons = [];
  if (trust < 5) reasons.push("Past grievances make your promises difficult to trust.");
  else if (trust >= 30) reasons.push("Your record of honoring agreements supports this proposal.");
  if (gold) reasons.push(`Your offer of ${gold} gold provides a tangible benefit.`);
  if (advantage > 8) reasons.push("Your military strength makes cooperation attractive.");
  if (action === "peace" || action === "ceasefire" || action === "territory" || (action === "tribute" && war)) {
    value += exhaustion * 0.6 + score * 0.5 + 20;
    if (exhaustion > 20) reasons.push("War losses and exhaustion increase the desire for peace.");
    if (score > 10) reasons.push("Your battlefield and territorial gains strengthen your terms.");
  } else if (war) { value -= 80; reasons.push("An active war must be resolved before cooperation."); }
  if (action === "trade" && ["suncoast", "desert"].includes(faction)) { value += 20; reasons.push("Secure commerce serves this kingdom's economic interests."); }
  if (["defensiveAlliance", "militaryAlliance", "aid"].includes(action)) {
    value -= action === "militaryAlliance" ? 40 : 20;
    const mutual = Object.keys(d.wars).some((id) => kingdomHostile(world.kingdoms, faction, id));
    if (mutual) { value += 30; reasons.push("A shared enemy gives both sides a reason to cooperate."); }
    if (k.exhaustion > 40) { value -= 25; reasons.push("Their exhausted armies cannot sustain another campaign."); }
  }
  if (action === "protection") {
    value += advantage * 2 - 20;
    if (["marsh", "stone", "steppe"].includes(faction)) { value -= 25; reasons.push("Their leaders place a high value on independence."); }
  }
  if (action === "tribute" && input.demand) value += advantage + score * 0.5 - 40;
  if (action === "territory") value -= 40;
  if (["pact", "passage"].includes(action) && ["crown", "vale", "stone", "marsh"].includes(faction)) value += 10;
  if (["ember", "iron"].includes(faction) && ["peace", "pact"].includes(action)) value -= 12;
  if (!reasons.length) reasons.push(`${f?.leader || "Their council"} weighs security, wealth, and your diplomatic standing.`);
  return { value, reasons: reasons.slice(0, 4), threshold: action === "territory" ? 45 : 20 };
}

function enact(p, world, faction, action, gold, input, time) {
  const d = ensureDiplomacy(p), k = world.kingdoms.factions[faction];
  check(p.gold >= gold, `This agreement requires ${gold} gold.`);
  if (action === "aid") {
    check(IDS().includes(input.target) && input.target !== faction, "Choose a rival kingdom for the aid request.");
    check(d.wars[input.target], "You must be at war with the kingdom you ask them to fight.");
  }
  if (action === "territory") {
    const s = world.settlements.find((s) => s.id === input.settlement);
    check(s && !s.owner && factionOf(s) === faction, "Choose a settlement controlled by the negotiating kingdom.");
    check(d.wars[faction], "Territory can only be negotiated as part of a war settlement.");
    check((d.wars[faction].score || 0) >= 25, "Win battles or occupy territory before demanding a settlement.");
  }
  p.gold -= gold; k.wealth = Math.min(1e7, k.wealth + gold);
  if (treatyActions.has(action)) treaty(p, faction, action, time,
    ["tribute", "protection"].includes(action) ? { payment: action === "protection" || input.demand ? "kingdom" : "player", amount: action === "protection" ? 8 : 12, nextDue: time + 120 } : {});
  if (["peace", "ceasefire", "territory"].includes(action)) { delete d.wars[faction]; p.relations[faction] = Math.max(0, p.relations[faction]); }
  else p.relations[faction] = clamp(p.relations[faction] + 6, -100, 100);
  if (action === "tribute" && d.wars[faction]) { delete d.wars[faction]; treaty(p, faction, "peace", time); p.relations[faction] = Math.max(0, p.relations[faction]); }
  d.trust[faction] = clamp(d.trust[faction] + 6, -100, 100);
  if (action === "territory") {
    const s = world.settlements.find((s) => s.id === input.settlement);
    s.owner = p.uid; s.ownerName = p.username; s.ownerReputation = 0; s.loyalty = 45; s.garrison = Math.min(s.garrison, 12); s.garrisonArmy = {}; s.occupation = time;
    treaty(p, faction, "peace", time);
  }
  if (action === "tribute" && input.demand) { const payment = Math.min(100, Math.floor(k.wealth / 10)); k.wealth -= payment; p.gold += payment; }
  if (action === "prisoners") {
    const held = d.prisoners.filter((n) => n.faction === faction);
    for (const prisoner of held) { const n = world.npcs?.find((n) => n.id === prisoner.id); if (n) { n.capturedBy = null; n.capturedUntil = 0; n.respawn = time + 12; } }
    d.prisoners = d.prisoners.filter((n) => n.faction !== faction);
    const ransom = Math.min(k.wealth, held.length * 75); k.wealth -= ransom; p.gold += ransom;
  }
  if (action === "aid") declareKingdomWar(world, faction, input.target, time, "Aid an allied commander");
  if (action === "militaryAlliance" || action === "defensiveAlliance") {
    p.supplies = Math.min(200, p.supplies + 25);
    if (action === "militaryAlliance") for (const enemy of Object.keys(d.wars)) if (enemy !== faction) declareKingdomWar(world, faction, enemy, time, "Honor a military alliance");
  }
  history(world, time, `${p.username} and ${FACTIONS.find((f) => f.id === faction)?.name} agreed: ${action}.`, p);
}

export function diplomaticAction(p, world, input = {}, time = world.time || 0) {
  check(!p.activeBattle, "Finish the battle first.");
  check(IDS().includes(input.faction), "Unknown faction.");
  check(input.gold === undefined || (Number.isInteger(input.gold) && input.gold >= 0 && input.gold <= 100000), "Offer a whole amount between 0 and 100,000 gold.");
  const d = ensureDiplomacy(p), faction = input.faction, k = world.kingdoms.factions[faction], action = input.action || (input.peace ? "legacyPeace" : "gift");
  check(actions.has(action) || action === "legacyPeace", "Unknown diplomatic proposal.");
  check(input.target === undefined || (typeof input.target === "string" && IDS().includes(input.target)), "Unknown target kingdom.");
  if (action === "accept") {
    check(typeof input.offerId === "string" && /^o-\d{1,9}$/.test(input.offerId), "Choose a valid envoy offer.");
    const o = d.offers.find((o) => o.id === input.offerId && o.faction === faction);
    check(o && o.status === "pending" && o.expires > time, "This envoy offer has expired or has already been resolved.");
    enact(p, world, faction, o.action, o.gold, o, time);
    o.status = "accepted";
    return decision(p, "accepted", o.action, faction, o.reasons, time);
  }
  if (action === "gift" || action === "legacyPeace") {
    const gold = input.gold ?? (action === "legacyPeace" ? 120 : 35);
    check(gold > 0 && p.gold >= gold, `This diplomatic mission costs ${gold || 35} gold.`);
    p.gold -= gold; k.wealth = Math.min(1e7, k.wealth + gold);
    p.relations[faction] = clamp(p.relations[faction] + (action === "legacyPeace" ? 45 : Math.min(30, Math.round(gold / 3))), -100, 100);
    d.trust[faction] = clamp(d.trust[faction] + 4, -100, 100);
    if (action === "legacyPeace") { delete d.wars[faction]; treaty(p, faction, "peace", time); }
    const reasons = [action === "legacyPeace" ? "Compensation restores relations and protects your holdings under a lasting peace." : "Your gift improves relations and gives their council a reason to trust you."];
    history(world, time, `${p.username} sent ${gold} gold to ${faction}.`, p);
    return decision(p, "accepted", action, faction, reasons, time);
  }
  if (action === "war" || action === "break") {
    const active = d.treaties.filter((t) => t.faction === faction && t.until > time);
    if (action === "break") check(active.length, "There is no active agreement to break.");
    if (action === "war") check(!d.wars[faction], "You are already at war with this kingdom.");
    d.treaties = d.treaties.filter((t) => t.faction !== faction);
    if (active.length) {
      d.trust[faction] = clamp(d.trust[faction] - 40, -100, 100);
      for (const id of IDS()) if (id !== faction) { d.trust[id] = clamp(d.trust[id] - 8, -100, 100); p.relations[id] = clamp(p.relations[id] - 4, -100, 100); }
      p.renown = Math.max(0, p.renown - 10);
    }
    p.relations[faction] = Math.max(-100, p.relations[faction] - (action === "war" ? 50 : 20));
    if (action === "war") {
      const target = world.settlements.find((s) => !s.owner && factionOf(s) === faction && s.kind !== "village");
      d.wars[faction] = { started: time, score: 0, exhaustion: 0, objective: target ? `Capture ${target.name}` : "Force tribute", target: target?.id || null };
      for (const t of d.treaties) if (t.type === "militaryAlliance" && t.until > time && t.faction !== faction) declareKingdomWar(world, t.faction, faction, time, "Honor a military alliance");
    }
    const reasons = [active.length ? "Breaking your word damages trust across the continent." : "Your armies may now contest their territory and supply routes."];
    history(world, time, `${p.username} ${action === "war" ? "declared war on" : "broke treaties with"} ${faction}.`, p);
    return decision(p, "accepted", action, faction, reasons, time);
  }
  const gold = input.gold ?? 0;
  check(p.gold >= gold, "You do not have the offered gold.");
  if (action === "aid") check(IDS().includes(input.target) && input.target !== faction && d.wars[input.target], "Request aid against a kingdom you are fighting.");
  if (action === "territory") {
    const s = world.settlements.find((s) => s.id === input.settlement);
    check(s && !s.owner && factionOf(s) === faction, "Choose a settlement controlled by the negotiating kingdom.");
    check(d.wars[faction] && d.wars[faction].score >= 25, "Win battles or occupy territory before demanding a settlement.");
  }
  if (action === "prisoners") check(d.prisoners.some((n) => n.faction === faction), "You hold no prisoners from this kingdom.");
  const evaluation = negotiation(p, world, faction, action, gold, input, time);
  if (evaluation.value >= evaluation.threshold || action === "prisoners") {
    enact(p, world, faction, action, gold, input, time);
    return decision(p, "accepted", action, faction, evaluation.reasons, time);
  }
  if (evaluation.value >= evaluation.threshold - 65 && action !== "territory") {
    const counterGold = Math.min(100000, Math.max(gold + 20, Math.ceil((evaluation.threshold - evaluation.value) * 5 + gold)));
    const o = offer(p, faction, action, counterGold, time, evaluation.reasons, { ...(input.target ? { target: input.target } : {}), ...(input.demand ? { demand: true } : {}) });
    return decision(p, "countered", action, faction, evaluation.reasons, time, o);
  }
  return decision(p, "rejected", action, faction, evaluation.reasons, time);
}

function declareKingdomWar(world, a, b, time, reason = "Secure a border stronghold") {
  const state = world.kingdoms, r = state.relations[keyFor(a, b)];
  if (!r || r.war || state.wars.length >= 20) return;
  const protectedBy = r.treaties.some((t) => ["pact", "peace", "ceasefire"].includes(t.type) && t.until > time);
  if (protectedBy) return;
  r.war = true; r.relation = Math.min(-40, r.relation - 15); r.treaties = [];
  const target = world.settlements.find((s) => !s.owner && factionOf(s) === b && s.kind !== "village");
  bounded(state.wars, { id: `war-${a}-${b}-${state.step}`, a, b, started: time, score: 0, exhaustion: 0, objective: target ? `Capture ${target.name}` : reason, target: target?.id || null }, 20);
  history(world, time, `${a} declared war on ${b}: ${reason}.`);
  for (const ally of Object.values(state.relations)) {
    if (ally === r || ![ally.a, ally.b].includes(b) || !ally.treaties.some((t) => ["militaryAlliance", "defensiveAlliance"].includes(t.type) && t.until > time)) continue;
    const supporter = ally.a === b ? ally.b : ally.a;
    if (supporter !== a) declareKingdomWar(world, supporter, a, time, `Defend allied ${b}`);
  }
}

export function supportPlayerDefense(world, p, enemy, time) {
  for (const t of ensureDiplomacy(p).treaties)
    if (["militaryAlliance", "defensiveAlliance", "protection"].includes(t.type) && t.until > time && t.faction !== enemy)
      declareKingdomWar(world, t.faction, enemy, time, "Honor an obligation to defend an allied commander");
}

export function recordKingdomBattle(realm, battle) {
  const p = realm.players.get(battle.participants[0]);
  if (!p) return;
  const e = battle.encounter, n = e.kind === "npc" ? realm.npcs.find((n) => n.id === e.targetId) : null, s = ["siege", "raid"].includes(e.kind) ? realm.world.settlements.find((s) => s.id === e.targetId) : null, faction = n?.faction || s?.faction;
  if (!IDS().includes(faction)) return;
  const d = ensureDiplomacy(p), won = battle.winner === 0, losses = battle.units.filter((u) => u.team === 0 && !u.temporary && u.hp <= 0 && !u.routed).length;
  if (d.wars[faction]) { d.wars[faction].score = clamp(d.wars[faction].score + (won ? (e.kind === "raid" ? 8 : s ? 28 : e.variant === "rearguard" ? 4 : 12) : -12), -100, 100); d.wars[faction].exhaustion = clamp(d.wars[faction].exhaustion + 4 + losses * 0.3, 0, 100); }
  realm.kingdoms.factions[faction].exhaustion = clamp(realm.kingdoms.factions[faction].exhaustion + (won ? 8 : 2), 0, 100);
  const enemyDefeated = !battle.units.some((u) => u.team === 1 && u.hp > 0 && !u.routed);
  if (n?.lord && won && e.variant !== "rearguard" && enemyDefeated) {
    n.capturedBy = p.uid; n.capturedUntil = realm.time + 180; n.respawn = n.capturedUntil;
    bounded(d.prisoners, { id: n.id, name: n.name, faction, until: n.capturedUntil, ransom: 75 }, 30);
    realm.log(`${n.name} was captured. Negotiate ransom or a prisoner exchange.`, p.uid);
  }
  if (s && won && e.kind === "siege") { s.loyalty = 35; s.occupation = realm.time; s.supplies = 40; s.damage = Math.min(80, (s.damage || 0) + 25); }
}

export function tickKingdoms(realm, dt) {
  const state = realm.kingdoms, world = realm.world, time = realm.time;
  state.clock += dt;
  if (state.clock < 12) return;
  const elapsed = state.clock; state.clock = 0; state.step++;
  const activeWar = new Set(state.wars.flatMap((w) => [w.a, w.b]));
  for (const f of FACTIONS) {
    const k = state.factions[f.id], holdings = world.settlements.filter((s) => !s.owner && factionOf(s) === f.id), lords = realm.npcs.filter((n) => n.kind === "patrol" && n.faction === f.id);
    k.strength = lords.filter((n) => n.respawn <= time && !n.capturedBy).reduce((n, l) => n + armyPower(l.army), 0) + holdings.reduce((n, s) => n + s.garrison * 1.5, 0);
    const commerce = Object.values(state.relations).filter((r) => !r.war && [r.a, r.b].includes(f.id) && r.treaties.some((t) => t.type === "trade" && t.until > time)).length;
    k.wealth = clamp(k.wealth + holdings.reduce((n, s) => n + (s.kind === "village" ? 5 : 12) * (s.production ?? 1) * (1 - (s.damage || 0) / 100), 0) * (1 + commerce * 0.12) - lords.reduce((n, l) => n + armySize(l.army) / 20, 0), 0, 1e7);
    k.exhaustion = clamp(k.exhaustion + (activeWar.has(f.id) ? 0.15 : -1.5), 0, 100);
  }
  for (const s of world.settlements) {
    s.damage = Math.max(0, (s.damage || 0) - 0.7);
    s.production = Math.min(1, (s.production ?? 1) + 0.01);
    s.food = clamp((s.food ?? 50) + ((s.damage || 0) < 50 ? 2 : -1) - (s.battleId ? 4 : 0), 0, 100);
    s.recruitStock = Math.min(30, (s.recruitStock ?? 30) + ((s.damage || 0) < 65 ? 1 : 0));
    const owner = s.owner && realm.players.get(s.owner);
    const supplied = s.kind === "village" || world.settlements.some((v) => v !== s && !v.battleId && !v.underAttack && ["village", "town", "capital"].includes(v.kind) && Math.hypot(v.x - s.x, v.y - s.y) < 40 && (s.owner ? v.owner === s.owner || (owner && !v.owner && !playerHostile(owner, state, factionOf(v), time)) : !v.owner && factionOf(v) === factionOf(s)));
    s.supplyBlocked = !supplied || !!s.battleId || !!s.underAttack;
    s.supplies = clamp((s.supplies ?? 100) + (s.battleId || s.underAttack ? -4 : supplied ? 3 : -2) - (s.kind === "castle" ? s.garrison / 80 : 0), 0, 100);
    s.loyalty = clamp((s.loyalty ?? 100) + (s.owner && s.garrison < 8 ? -2 : 0.7), 0, 100);
    if (!s.owner && !s.battleId && s.supplies > 30 && s.kind !== "village") {
      const k = state.factions[factionOf(s)], max = (s.kind === "capital" ? 65 : 32) + (s.level || 1) * 8;
      if (k?.wealth > 15 && s.garrison < max) { s.garrison++; k.wealth -= 3; }
    }
    if (s.owner && !s.battleId && s.loyalty <= 5 && s.garrison < 5) {
      realm.log(`${s.name} rebelled after its garrison and local support collapsed.`, s.owner);
      s.owner = null; s.ownerName = null; s.faction = s.baseFaction || s.faction; s.currentFaction = s.faction; s.garrison = 12; s.garrisonArmy = {}; s.loyalty = 60;
    }
  }
  for (const n of realm.npcs) {
    if (n.kind === "merchant") {
      const count = Math.max(0, n.deliveryCount || 0), previous = state.caravans[n.id] || 0;
      if (count > previous && state.factions[n.faction]) {
        state.factions[n.faction].wealth = Math.min(1e7, state.factions[n.faction].wealth + (count - previous) * 30);
        for (const p of realm.players.values()) if (playerTreaty(p, n.faction, "trade", time)) p.gold = Math.min(1e9, p.gold + 5 * (count - previous));
      }
      state.caravans[n.id] = count;
    }
    if (n.battleId) continue;
    if (n.capturedBy) {
      if (n.capturedUntil > time) continue;
      const p = realm.players.get(n.capturedBy); if (p) ensureDiplomacy(p).prisoners = p.diplomacy.prisoners.filter((l) => l.id !== n.id);
      n.capturedBy = null; n.capturedUntil = 0; n.respawn = time + 12;
      history(world, time, `${n.name} returned from captivity and is rebuilding a company.`);
    }
    if (n.kind === "patrol") {
      const k = state.factions[n.faction], home = world.settlements.find((s) => s.id === n.home && !s.owner && factionOf(s) === n.faction) || world.settlements.find((s) => !s.owner && factionOf(s) === n.faction);
      if (!home || !k) continue;
      n.home = home.id;
      const maximum = n.lord ? 48 : 32, size = armySize(n.army);
      if (size < 16 && n.respawn <= time && Math.hypot(n.x - home.x, n.y - home.y) >= 6) {
        n.respawn = time + 90; n.x = home.x; n.y = home.y; n.route = []; n.routeIndex = 0; n.target = home.id;
        history(world, time, `${n.name} withdrew to ${home.name} to recover and recruit.`);
      }
      if (size < maximum && k.wealth >= 20 && (n.respawn > time || Math.hypot(n.x - home.x, n.y - home.y) < 6)) {
        const recruits = Math.min(4, maximum - size), reinforcementArmy = battleArmy(maximum, n.faction), roster = Object.keys(UNITS).filter((key) => reinforcementArmy[key] > 0), reinforcements = safeArmy();
        for (let i = 0; i < recruits; i++) reinforcements[roster[(state.step + i) % roster.length]]++;
        for (const key of Object.keys(UNITS)) n.army[key] = (n.army[key] || 0) + reinforcements[key];
        k.wealth -= recruits * 5;
      }
      if (n.respawn > time) continue;
      const war = state.wars.find((w) => w.a === n.faction || w.b === n.faction);
      const enemy = war && (war.a === n.faction ? war.b : war.a);
      const targets = war ? world.settlements.filter((s) => !s.owner && !s.battleId && factionOf(s) === enemy) : [];
      const target = targets.sort((a, b) => Math.hypot(n.x - a.x, n.y - a.y) - Math.hypot(n.x - b.x, n.y - b.y))[0];
      if (target) n.target = target.id;
      if (target && Math.hypot(n.x - target.x, n.y - target.y) < 3 && (n.raidAt || 0) <= time && armySize(n.army) >= 16) {
        n.raidAt = time + 36;
        const strength = armySize(n.army), damage = Math.max(4, Math.floor(strength / 4));
        target.damage = clamp((target.damage || 0) + 12, 0, 90); target.supplies = Math.max(0, target.supplies - 15);
        target.garrison = Math.max(0, target.garrison - damage);
        state.factions[enemy].exhaustion = Math.min(100, state.factions[enemy].exhaustion + 3);
        war.exhaustion = Math.min(100, war.exhaustion + 2);
        war.score = clamp(war.score + (war.a === n.faction ? 3 : -3), -100, 100);
        const remaining = battleArmy(Math.max(8, strength - 4), n.faction); n.army = remaining;
        if (!target.garrison && target.kind !== "village") {
          target.faction = n.faction; target.currentFaction = n.faction; target.loyalty = 35; target.occupation = time; target.garrison = 10; target.garrisonArmy = {};
          war.score = clamp(war.score + (war.a === n.faction ? 18 : -18), -100, 100);
          history(world, time, `${n.name} captured ${target.name} for ${n.faction}.`); realm.log(`${target.name} changed hands during the ${war.a}–${war.b} war.`);
        } else if (armySize(n.army) < 16 && target.garrison >= 16 && n.lord) {
          n.capturedBy = `kingdom:${enemy}`; n.capturedUntil = time + 120; n.respawn = n.capturedUntil; n.x = home.x; n.y = home.y; n.route = []; n.routeIndex = 0;
          history(world, time, `${n.name} was captured by ${enemy} after the failed attack on ${target.name}.`);
        }
      }
    }
    if (n.kind === "bandit" && n.respawn <= time && (n.raidAt || 0) <= time) {
      const victim = world.settlements.find((s) => s.kind === "village" && Math.hypot(s.x - n.x, s.y - n.y) < 2.5);
      const merchant = realm.npcs.find((m) => m.kind === "merchant" && m.respawn <= time && Math.hypot(m.x - n.x, m.y - n.y) < 1.4);
      if (victim) { victim.damage = clamp((victim.damage || 0) + 18, 0, 90); victim.recruitStock = Math.max(0, (victim.recruitStock ?? 20) - 5); n.raidAt = time + 60; }
      if (merchant) { merchant.respawn = time + 90; merchant.route = []; state.factions[merchant.faction].wealth = Math.max(0, state.factions[merchant.faction].wealth - 40); n.raidAt = time + 60; }
    }
  }
  for (const r of Object.values(state.relations)) r.treaties = r.treaties.filter((t) => t.until > time);
  // One diplomatic pair is considered per strategic turn, independent of players.
  const pairs = Object.values(state.relations), r = pairs[(state.step - 1) % pairs.length];
  if (r) {
    const a = state.factions[r.a], b = state.factions[r.b];
    if (r.war) {
      const war = state.wars.find((w) => keyFor(w.a, w.b) === keyFor(r.a, r.b));
      if (war && (war.exhaustion > 35 || Math.abs(war.score) > 55 || time - war.started > 900)) {
        r.war = false; r.relation = -10; r.treaties = [{ type: "peace", until: time + 360 }];
        state.wars = state.wars.filter((w) => w !== war); history(world, time, `${r.a} and ${r.b} negotiated peace after a costly war.`);
      }
    } else if (r.relation < -30 && Math.max(a.exhaustion, b.exhaustion) < 65 && !r.treaties.length) declareKingdomWar(world, r.a, r.b, time);
    else if (r.relation > 8 && !r.treaties.length) {
      const type = r.relation > 32 ? "defensiveAlliance" : "trade";
      r.treaties = [{ type, until: time + durations[type] }]; r.trust = Math.min(100, r.trust + 4); r.relation = Math.min(100, r.relation + 3);
      history(world, time, `${r.a} and ${r.b} signed a ${type} agreement.`);
    }
  }
  for (const p of realm.players.values()) {
    const d = ensureDiplomacy(p);
    for (const t of d.treaties) if (["militaryAlliance", "defensiveAlliance"].includes(t.type) && t.until > time)
      for (const war of state.wars.filter((w) => t.type === "militaryAlliance" ? [w.a, w.b].includes(t.faction) : w.b === t.faction)) {
        const enemy = war.a === t.faction ? war.b : war.a;
        if (!d.wars[enemy] && !["peace", "ceasefire", "pact"].some((type) => playerTreaty(p, enemy, type, time))) {
          d.wars[enemy] = { started: time, score: 0, exhaustion: 0, objective: `Support allied ${t.faction}`, target: war.target || null };
          p.relations[enemy] = Math.min(-30, p.relations[enemy]);
          bounded(d.history, { time, message: `Your ${t.type} obligation commits your company against ${enemy}.` }, 40);
        }
      }
    for (const t of d.treaties) if (["tribute", "protection"].includes(t.type) && t.until > time && (t.nextDue || 0) <= time) {
      const k = state.factions[t.faction], amount = Math.floor(number(t.amount, 10, 1, 1000));
      if (t.payment === "kingdom") { const paid = Math.min(k.wealth, amount); k.wealth -= paid; p.gold = Math.min(1e9, p.gold + paid); }
      else if (p.gold >= amount) { p.gold -= amount; k.wealth = Math.min(1e7, k.wealth + amount); }
      else { t.until = time; d.trust[t.faction] = Math.max(-100, d.trust[t.faction] - 12); bounded(d.history, { time, message: `Missed tribute to ${t.faction} ended the agreement.` }, 40); }
      t.nextDue = time + 120;
    }
    for (const t of d.treaties) if (t.until <= time && !t.expired) { t.expired = true; d.trust[t.faction] = Math.min(100, d.trust[t.faction] + 2); bounded(d.history, { time, message: `The ${t.type} agreement with ${t.faction} expired honorably.` }, 40); }
    d.treaties = d.treaties.filter((t) => t.until > time - 60);
    for (const o of d.offers) if (o.status === "pending" && o.expires <= time) o.status = "expired";
    for (const w of Object.values(d.wars)) w.exhaustion = Math.min(100, w.exhaustion + elapsed / 180);
    const f = FACTIONS[(state.step - 1) % FACTIONS.length], k = state.factions[f.id];
    if (time > 60 && time - k.lastOffer > 120 && !d.offers.some((o) => o.faction === f.id && o.status === "pending" && o.expires > time)) {
      const war = d.wars[f.id], ownWar = state.wars.find((w) => [w.a, w.b].includes(f.id));
      const action = war && (war.score > 15 || war.exhaustion + k.exhaustion > 30) ? "peace" : p.relations[f.id] >= 0 && !war ? (k.strength < armyPower(p.army) * 0.6 ? "protection" : ownWar && p.renown > 20 ? "militaryAlliance" : "trade") : !war && p.relations[f.id] < -20 && k.strength > armyPower(p.army) * 2 ? "tribute" : null;
      if (action && !playerTreaty(p, f.id, action, time)) {
        const reason = war ? "Losses and exhaustion make a negotiated settlement preferable." : action === "militaryAlliance" ? "Our kingdom is at war; a military alliance commits both sides to help one another against current enemies." : action === "tribute" ? "Pay compensation and recurring tribute, or face continued hostility at our borders." : "Our council sees a mutual benefit in secure commerce and cooperation.";
        const o = offer(p, f.id, action, action === "tribute" ? 35 : 0, time, [reason]); k.lastOffer = time; realm.log(`${f.name} sent an envoy ${action === "tribute" ? "demanding" : "offering"} ${action} (${o.id}).`, p.uid);
      }
    }
  }
}

export function kingdomView(realm, p) {
  const state = realm.kingdoms, d = ensureDiplomacy(p), time = realm.time;
  return {
    factions: FACTIONS.map((f) => ({ ...f, ...state.factions[f.id], settlements: realm.world.settlements.filter((s) => !s.owner && factionOf(s) === f.id).map((s) => s.id), wars: state.wars.filter((w) => w.a === f.id || w.b === f.id).map((w) => w.a === f.id ? w.b : w.a), allies: Object.values(state.relations).filter((r) => [r.a, r.b].includes(f.id) && r.treaties.some((t) => t.type.includes("Alliance") && t.until > time)).map((r) => r.a === f.id ? r.b : r.a), player: { relation: p.relations[f.id], trust: d.trust[f.id], atWar: !!d.wars[f.id], war: d.wars[f.id] || null, treaties: d.treaties.filter((t) => t.faction === f.id && t.until > time), offers: d.offers.filter((o) => o.faction === f.id && o.status === "pending" && o.expires > time) }, lords: realm.npcs.filter((n) => n.lord && n.faction === f.id).map((n) => ({ id: n.id, name: n.name, strength: armySize(n.army), capturedBy: n.capturedBy || null, recovering: n.respawn > time })) })),
    relations: clone(Object.values(state.relations)), wars: clone(state.wars), history: clone(state.history.slice(-12)),
    tradeRoutes: realm.npcs.filter((n) => n.kind === "merchant").map((n) => ({ id: n.id, faction: n.faction, from: n.lastStop || n.home, to: n.target, deliveries: n.deliveryCount || 0, disrupted: n.respawn > time })),
  };
}
