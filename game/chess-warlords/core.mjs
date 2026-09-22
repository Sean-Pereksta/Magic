// Pure helpers shared by the game and its regression tests. No Firebase or DOM work.
export const SYNC_PROTOCOL = 2;
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

export function stringDelta(before = '', after = '') {
  const patches = [];
  for (let i = 0; i < after.length;) {
    if (before[i] === after[i]) { i++; continue; }
    const start = i;
    while (i < after.length && before[i] !== after[i]) i++;
    patches.push({start, text: after.slice(start, i)});
  }
  return patches;
}

export function patchString(base, patches = []) {
  const chars = String(base || '').split('');
  for (const {start, text} of patches) {
    if (!Number.isInteger(start) || start < 0 || start + text.length > chars.length) throw new Error('Invalid land patch');
    for (let i = 0; i < text.length; i++) chars[start + i] = text[i];
  }
  return chars.join('');
}

// Every delta is relative to the durable checkpoint, never the previous packet.
// Firestore may coalesce intermediate snapshots; the latest packet is sufficient.
export function snapshotDelta(base, next, forceFull = false) {
  if (forceFull || !base || base.mapSeed !== next.mapSeed || base.controllerEpoch !== next.controllerEpoch) {
    return {...next, protocol: SYNC_PROTOCOL, fullSnapshot: true, baseVersion: next.version};
  }
  const out = {protocol: SYNC_PROTOCOL, fullSnapshot: false, baseVersion: base.version};
  for (const [k, value] of Object.entries(next)) {
    if (['pieces', 'factions', 'boardOwners', 'boardKinds', 'boardWalls', 'fullSnapshot'].includes(k)) continue;
    if (['version', 'controllerEpoch', 'mapSeed', 'updatedAt', 'updatedAtMs', 'updatedBy', 'reason', 'matchRecord'].includes(k) || !same(base[k], value)) out[k] = value;
  }
  const previous = new Map((base.pieces || []).map(p => [p.i, p]));
  const live = new Set((next.pieces || []).map(p => p.i));
  out.pieceDeltas = (next.pieces || []).filter(p => !same(previous.get(p.i), p));
  out.removedPieceIds = [...previous.keys()].filter(id => !live.has(id));
  out.factionDeltas = (next.factions || []).filter(f => !same((base.factions || []).find(b => b.idx === f.idx), f));
  for (const [field, patch] of [['boardOwners', 'ownerPatches'], ['boardKinds', 'kindPatches'], ['boardWalls', 'wallPatches']]) {
    const value = next[field] || '';
    if (value === base[field]) continue;
    const edits = stringDelta(base[field], value);
    if (!base[field] || base[field].length !== value.length || JSON.stringify(edits).length > value.length * .8) out[field] = value;
    else out[patch] = edits;
  }
  // A busy battle should roll the checkpoint instead of shipping a larger delta.
  if (JSON.stringify(out).length > JSON.stringify(next).length * .8) return snapshotDelta(null, next, true);
  return out;
}

export function restoreSnapshot(base, packet) {
  if (packet.fullSnapshot && Array.isArray(packet.pieces)) return {...packet};
  if (!base || packet.baseVersion !== base.version || packet.mapSeed !== base.mapSeed || packet.controllerEpoch !== base.controllerEpoch) return null;
  if (packet.version <= base.version) return null;
  const pieces = new Map((base.pieces || []).map(p => [p.i, p]));
  for (const id of packet.removedPieceIds || []) pieces.delete(id);
  for (const p of packet.pieceDeltas || []) pieces.set(p.i, p);
  const factions = new Map((base.factions || []).map(f => [f.idx, f]));
  for (const f of packet.factionDeltas || []) factions.set(f.idx, f);
  const merged = {...base, ...packet, pieces: [...pieces.values()], factions: [...factions.values()]};
  for (const [field, patch] of [['boardOwners', 'ownerPatches'], ['boardKinds', 'kindPatches'], ['boardWalls', 'wallPatches']]) {
    merged[field] = packet[field] ?? patchString(base[field], packet[patch]);
    delete merged[patch];
  }
  delete merged.pieceDeltas;
  delete merged.removedPieceIds;
  delete merged.factionDeltas;
  return merged;
}

export const RANK_TIERS = [
  {name:'Bronze', min:0, emoji:'🥉'}, {name:'Silver', min:200, emoji:'🥈'},
  {name:'Gold', min:300, emoji:'🥇'}, {name:'Diamond', min:400, emoji:'💎'},
  {name:'Masters', min:500, emoji:'⚔️'}, {name:'Grandmaster', min:650, emoji:'👑'}
];
export function rankProgress(value) {
  const elo = Math.max(0, Math.round(Number(value) || 0));
  const tierIndex = RANK_TIERS.findLastIndex(t => elo >= t.min);
  const tier = RANK_TIERS[tierIndex], next = RANK_TIERS[tierIndex + 1];
  if (!next) return {...tier, division:'', label:tier.name, progress:1, nextAt:null};
  const width = (next.min - tier.min) / 3;
  const part = Math.min(2, Math.floor((elo - tier.min) / width));
  const division = ['III', 'II', 'I'][part];
  return {...tier, division, label:`${tier.name} ${division}`, progress:(elo - tier.min - part * width) / width, nextAt:Math.ceil(tier.min + (part + 1) * width)};
}

// Pairwise expected results average across the field, so eight-player games do
// not inflate ELO sevenfold. A small, bounded king bonus rewards active survival.
export function placementRating(record, slot) {
  const participants = record?.participants || [];
  const player = participants.find(p => p.slot === slot);
  const placement = Number(record?.placements?.[slot]);
  if (!player || participants.length < 2 || !placement) return null;
  const opponents = participants.filter(p => p.slot !== slot);
  let actual = 0, expected = 0, higherBeaten = 0;
  for (const opponent of opponents) {
    const otherPlace = Number(record.placements[opponent.slot] || 1);
    const score = placement < otherPlace ? 1 : placement === otherPlace ? .5 : 0;
    actual += score;
    expected += 1 / (1 + Math.pow(10, ((Number(opponent.elo) || 100) - (Number(player.elo) || 100)) / 400));
    if (score === 1 && opponent.elo > player.elo) higherBeaten++;
  }
  const kings = Math.max(0, Number(record.kings?.[slot]) || 0);
  const delta = Math.round(48 * (actual - expected) / opponents.length + Math.min(3, kings));
  return {placement, field:participants.length, delta, kings, higherBeaten, territory:Math.max(0, Number(record.territory?.[slot]) || 0)};
}

export function seasonKey(time = Date.now()) {
  const d = new Date(time);
  return `${d.getUTCFullYear()}-Q${Math.floor(d.getUTCMonth() / 3) + 1}`;
}

export function applyWarResult(profile, result, faction, season = seasonKey()) {
  const oldElo = Math.max(0, Number(profile.elo) || 0);
  const elo = Math.max(0, oldElo + result.delta), win = result.placement === 1;
  const streak = win ? (profile.streak || 0) + 1 : 0;
  const factions = {...(profile.factions || {})};
  const f = factions[faction] || {games:0, wins:0};
  factions[faction] = {games:f.games + 1, wins:f.wins + (win ? 1 : 0)};
  const seasons = {...(profile.seasons || {})};
  const s = seasons[season] || {games:0, wins:0, points:0};
  seasons[season] = {games:s.games + 1, wins:s.wins + (win ? 1 : 0), points:s.points + Math.max(0, result.field - result.placement + 1)};
  // Bound profile size while keeping lifetime accomplishments.
  const recentSeasons = Object.fromEntries(Object.entries(seasons).sort(([a], [b]) => b.localeCompare(a)).slice(0, 8));
  return {...profile, elo, rating:elo, games:(profile.games || 0) + 1,
    wins:(profile.wins || 0) + (win ? 1 : 0), losses:(profile.losses || 0) + (win ? 0 : 1),
    topThree:(profile.topThree || 0) + (result.placement <= 3 ? 1 : 0),
    kingsCaptured:(profile.kingsCaptured || 0) + result.kings,
    factionsEliminated:(profile.factionsEliminated || 0) + result.kings,
    territoryCaptured:(profile.territoryCaptured || 0) + result.territory,
    highestElo:Math.max(elo, oldElo, profile.highestElo || 0), streak,
    bestStreak:Math.max(streak, profile.bestStreak || 0), factions, seasons:recentSeasons,
    favoriteFaction:Object.entries(factions).sort((a,b) => b[1].games - a[1].games)[0]?.[0] || faction,
    lastResult:{...result, before:oldElo, after:elo, delta:elo-oldElo, faction, season}};
}

export function pickHit(candidates) {
  return candidates.filter(c => c.hit).sort((a,b) =>
    (Number(b.exactTile) - Number(a.exactTile)) || a.boardDistance - b.boardDistance ||
    a.spriteDistance - b.spriteDistance || b.depth - a.depth || a.id - b.id)[0]?.id ?? null;
}

export function cosmeticBudget(level = 0) {
  return {ambient:level < 1, decoration:level < 2, effects:level < 3 ? 2 : 1,
    shadows:level < 4, terrain:level < 5};
}
