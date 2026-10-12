import { visionTiles } from './fog.mjs';
import { distance } from './world-hex.mjs';
import { BUILDINGS } from './data.mjs';

const SETTLEMENTS = new Set(['city', 'town']);
const DEFENSES = new Set(['fort', 'wall', 'watchtower']);
const SIGNIFICANT = new Set(['city', 'town', 'fort', 'wall', 'watchtower', 'market', 'tradeOutpost', 'harbor', 'envoyOffice', 'chancery', 'greatGranary', 'greatStable', 'royalArsenal', 'siegeFoundry']);
const level = (t, id) => t.levels?.[id] || (t.building === id ? 1 : 0);
const name = (s, id) => s.kingdoms.find(k => k.id === id)?.name || 'Unclaimed territory';
const label = t => t.name || `Hex ${t.id}`;
const treatyKey = t => `${t.type}:${[...t.parties].sort().join(':')}:${t.expires}`;

// The recorder runs after refreshKnowledge. Geography, ownership and structures
// come from dated observations. A remembered sighting from earlier in this same
// turn is not permission to inspect the current world or follow an enemy army.
function observations(s, owner) {
  const fog = s.fog?.houses?.[owner], seen = visionTiles(s, owner), tiles = {};
  for (const [id, x] of Object.entries(fog?.tiles || {})) {
    const t = x.tile, visible = seen.has(id);
    const project = visible ? s.tiles[id]?.project : null;
    if (!t.premiumRegion && !SIGNIFICANT.has(t.building) && ![...SIGNIFICANT].some(key => level(t, key)) && !SIGNIFICANT.has(project?.type)) continue;
    tiles[id] = {id, owner:t.owner || null, building:t.building || null, name:t.name || null, capital:t.capital || null,
      levels:Object.fromEntries([...SIGNIFICANT].filter(key => level(t, key)).map(key => [key, level(t, key)])),
      premiumRegion:t.premiumRegion || null, turn:x.turn, source:x.source,
      ...(visible ? {project:project && SIGNIFICANT.has(project.type) ? {type:project.type, level:project.level || 1} : null} : {})};
  }
  const armies = Object.fromEntries(Object.values(fog?.armies || {}).filter(a => a.owner !== owner && seen.has(a.tile) && a.turn === s.turn)
    .map(a => [a.id, {id:a.id, owner:a.owner, tile:a.tile, minimum:a.minimum, maximum:a.maximum, turn:a.turn}]));
  const ownTiles = Object.values(fog?.tiles || {}).filter(x => x.tile.owner === owner).map(x => s.tiles[x.tile.id]);
  const distances = new Map();
  const proximity = id => {
    if (!distances.has(id)) distances.set(id, s.tiles[id] && ownTiles.length ? Math.min(...ownTiles.map(t => distance(s.tiles[id], t))) : 99);
    return distances.get(id);
  };
  return {tiles, armies, proximity, treaties:(s.treaties || []).filter(t => t.expires > s.turn).map(t => ({type:t.type, parties:[...t.parties], expires:t.expires})), wars:[...(s.wars || [])]};
}

export function recordKingdomDevelopments(s, viewers = s.kingdoms.map(k => k.id)) {
  if (s.knowledgeView || s.projectionOnly || s.phase === 'founding') return;
  s.kingdomDevelopments ??= {version:1, houses:{}};
  for (const owner of viewers) {
    if (!s.kingdoms.some(k => k.id === owner)) continue;
    const next = observations(s, owner), record = s.kingdomDevelopments.houses[owner] ??= {baseline:null, events:[]}, previous = record.baseline;
    const added = [], add = (kind, key, title, body, options = {}) => {
      const id = `${s.turn}:${kind}:${key}`;
      if (record.events.some(e => e.id === id) || added.some(e => e.id === id)) return;
      const near = options.tile ? Math.max(0, 12 - next.proximity(options.tile)) : 0;
      added.push({id, turn:s.turn, kind, title, body, ...options, priority:(options.priority || 20) + near});
    };
    if (previous) {
      const regionReported = new Set();
      for (const [id, t] of Object.entries(next.tiles)) {
        const old = previous.tiles[id], changedObservation = !old || t.turn > old.turn || t.owner !== old.owner;
        const administrativeLoss = old?.owner === owner && t.owner !== owner && t.turn <= old.turn;
        const source = administrativeLoss ? 'Royal administration' : t.source === 'spy' ? 'Spy report' : t.source === 'ally' ? 'Allied observation' : 'Confirmed observation';
        const options = {tile:id, house:t.owner, observedTurn:administrativeLoss ? s.turn : t.turn, source};
        if (SETTLEMENTS.has(t.building) && t.owner && t.owner !== owner && !t.capital && (!old || !SETTLEMENTS.has(old.building)) && changedObservation) {
          add('settlement', id, old ? 'New frontier settlement observed' : 'Frontier settlement discovered', `${name(s, t.owner)} has a ${t.building} at ${label(t)}.`, {...options, priority:45});
        }
        if (t.premiumRegion && !regionReported.has(t.premiumRegion)) {
          const region = s.premiumRegions?.[t.premiumRegion], regionName = region?.name || 'Rich resource territory';
          if (!old?.premiumRegion) {
            add('region-discovery', t.premiumRegion, 'Valuable territory discovered', `${regionName} offers a promising development opportunity.`, {...options, priority:50});
            regionReported.add(t.premiumRegion);
          } else if (old.owner !== t.owner && changedObservation) {
            add('region-owner', t.premiumRegion, 'Rich territory ownership observed', `${regionName}: ${name(s, t.owner)} now controls the observed holding.`, {...options, priority:70});
            regionReported.add(t.premiumRegion);
          }
        }
        if (t.owner !== owner && t.project && DEFENSES.has(t.project.type) && (old?.project?.type !== t.project.type || old?.project?.level !== t.project.level)) {
          add('defense', `${id}:${t.project.type}`, 'Frontier defenses underway', `${name(s, t.owner)} is constructing ${BUILDINGS[t.project.type]?.name || t.project.type} at ${label(t)}.`, {...options, priority:55});
        }
        if (t.owner === owner && old) for (const [type, n] of Object.entries(t.levels)) if (n > (old.levels?.[type] || 0)) {
          add('complete', `${id}:${type}:${n}`, 'Strategic construction completed', `${BUILDINGS[type]?.name || type} is ready at ${label(t)}.`, {...options, priority:40});
        }
        // Keep a previously observed project while out of sight. Absence from a
        // remembered tile is not a report that construction has stopped.
        if (!Object.hasOwn(t, 'project') && old?.project) t.project = old.project;
      }
      for (const a of Object.values(next.armies)) {
        const old = previous.armies[a.id], near = next.proximity(a.tile), oldNear = old ? next.proximity(old.tile) : 99;
        if (a.maximum < 15 || near > 3 || old && oldNear <= 3 && oldNear - near < 2) continue;
        add('army', a.id, 'Army observed near your frontier', `${name(s, a.owner)}: approximately ${a.minimum}–${a.maximum} troops observed at ${a.tile}. Their orders are unknown.`, {tile:a.tile, house:a.owner, observedTurn:a.turn, source:'Confirmed observation', priority:80});
      }
      const oldTreaties = new Set(previous.treaties.map(treatyKey));
      for (const t of next.treaties) if (!oldTreaties.has(treatyKey(t)) && ['alliance','trade','recurring','non-aggression','access','vassalage','peace'].includes(t.type)) {
        const relevant = t.parties.includes(owner) || t.parties.some(p => Object.values(next.tiles).some(tile => tile.owner === p && next.proximity(tile.id) <= 8));
        if (relevant) add('treaty', treatyKey(t), t.type === 'trade' || t.type === 'recurring' ? 'Trade opportunity secured' : 'Regional agreement announced', `${t.parties.map(id => name(s, id)).join(' and ')}: ${t.type} agreement active through turn ${t.expires - 1}.`, {house:t.parties.find(p => p !== owner), source:'Public agreement', priority:t.parties.includes(owner) ? 65 : 45});
      }
      for (const war of next.wars) if (!previous.wars.includes(war)) {
        const parties = war.split(':'), relevant = parties.includes(owner) || parties.some(p => Object.values(next.tiles).some(t => t.owner === p && next.proximity(t.id) <= 10));
        if (relevant) add('war', war, 'War declared', `${parties.map(id => name(s, id)).join(' and ')} are at war.`, {house:parties.find(p => p !== owner), source:'Public declaration', priority:90});
      }
      for (const event of s.events || []) if (event.turn >= s.turn - 1 && event.audience?.includes(owner) && event.kind === 'economy' && event.message?.includes(' completed at ')) {
        const [buildingName, location] = event.message.split(' completed at '), type = [...SIGNIFICANT].find(id => BUILDINGS[id]?.name === buildingName || BUILDINGS[id]?.levels?.some(l => l.name === buildingName));
        const t = Object.values(next.tiles).find(tile => `${tile.name || tile.id}.` === location && tile.owner === owner);
        if (type && t && !added.some(e => e.kind === 'complete' && e.tile === t.id)) add('complete', `${t.id}:${type}:${level(t,type)}`, 'Strategic construction completed', `${buildingName} is ready at ${label(t)}.`, {tile:t.id, house:owner, observedTurn:event.turn, source:'Royal administration', priority:40});
      }
      // These action records are admitted by fog.mjs only at observation time.
      const observed = new Set(s.fog?.houses?.[owner]?.actions || []);
      for (const round of s.strategy?.history || []) if (round.turn >= s.turn - 1) for (const house of round.houses) for (const [i, action] of house.actions.entries()) {
        if (!observed.has(`${round.turn}:${house.owner}:${i}`) || !['build','complete'].includes(action.kind) || !DEFENSES.has(action.building) || house.owner === owner) continue;
        const t = next.tiles[action.tile];
        if (!t || added.some(e => e.kind === 'defense' && e.tile === action.tile)) continue;
        add('defense', `${action.tile}:${action.building}`, action.kind === 'build' ? 'Frontier defenses underway' : 'Frontier defenses completed', `${name(s, house.owner)} ${action.kind === 'build' ? 'started' : 'completed'} ${BUILDINGS[action.building]?.name || action.building} at ${label(t)}.`, {tile:action.tile, house:house.owner, observedTurn:round.turn, source:'Dated observation', priority:55});
      }
    }
    record.events = [...record.events, ...added].filter(e => e.turn >= s.turn - 8).sort((a,b) => b.turn - a.turn || b.priority - a.priority || a.id.localeCompare(b.id)).slice(0,24);
    record.baseline = {turn:s.turn, tiles:next.tiles, armies:next.armies, treaties:next.treaties, wars:next.wars};
  }
}

export function kingdomDevelopments(view, owner, limit = 6) {
  // This function accepts the UI knowledge projection; it never fills missing
  // event fields from a live tile, hidden army, or another House's baseline.
  return (view.kingdomDevelopments?.houses?.[owner]?.events || []).filter(e => e.turn >= view.turn - 3)
    .slice().sort((a,b) => b.turn - a.turn || b.priority - a.priority || a.id.localeCompare(b.id)).slice(0, limit);
}

export function validateKingdomDevelopments(s) {
  if (s.kingdomDevelopments === undefined) return;
  const fail = () => { throw new Error('Damaged Kingdom Developments data.'); };
  const object = x => x && typeof x === 'object' && !Array.isArray(x), string = (x,n) => typeof x === 'string' && x.length <= n;
  const integer = (x,max=s.turn) => Number.isInteger(x) && x >= 0 && x <= max;
  const owner = x => x === null || s.kingdoms.some(k => k.id === x), tile = x => typeof x === 'string' && !!s.tiles[x];
  const only = (x,keys) => Object.keys(x).every(k => keys.includes(k));
  const treaties = new Set(['alliance','peace','trade','vassalage','non-aggression','access','embargo','recurring']);
  const data = s.kingdomDevelopments;
  if (!object(data) || data.version !== 1 || !object(data.houses) || !only(data,['version','houses'])) fail();
  for (const [id, record] of Object.entries(data.houses)) {
    if (!s.kingdoms.some(k => k.id === id) || !object(record) || !only(record,['baseline','events']) || !Array.isArray(record.events) || record.events.length > 24) fail();
    const ids = new Set();
    for (const e of record.events) {
      if (!object(e) || !only(e,['id','turn','kind','title','body','priority','tile','house','observedTurn','source']) || !string(e.id,200) || ids.has(e.id) || !integer(e.turn) || !['settlement','region-discovery','region-owner','defense','complete','army','treaty','war'].includes(e.kind) || !string(e.title,120) || !string(e.body,600) || !integer(e.priority,150) || e.tile !== undefined && (!tile(e.tile) || !s.fog?.houses?.[id]?.tiles?.[e.tile]) || e.house !== undefined && !owner(e.house) || e.observedTurn !== undefined && !integer(e.observedTurn,e.turn) || !string(e.source,60)) fail();
      ids.add(e.id);
    }
    const b = record.baseline;
    // Projected multiplayer knowledge deliberately contains events only.
    if (b === undefined && (s.knowledgeView || s.projectionOnly) || b === null) continue;
    if (!object(b) || !only(b,['turn','tiles','armies','treaties','wars']) || !integer(b.turn) || !object(b.tiles) || Object.keys(b.tiles).length > Object.keys(s.tiles).length || !object(b.armies) || Object.keys(b.armies).length > 500 || !Array.isArray(b.treaties) || b.treaties.length > 5000 || !Array.isArray(b.wars) || b.wars.length > 100) fail();
    for (const [key,t] of Object.entries(b.tiles)) {
      if (!object(t) || !only(t,['id','owner','building','name','capital','levels','premiumRegion','turn','source','project']) || !tile(key) || t.id !== key || !s.fog?.houses?.[id]?.tiles?.[key] || !owner(t.owner) || t.building !== null && !BUILDINGS[t.building] || t.name !== null && !string(t.name,100) || !owner(t.capital) || t.premiumRegion !== null && !s.premiumRegions?.[t.premiumRegion] || !integer(t.turn,b.turn) || !['sight','spy','ally'].includes(t.source) || !object(t.levels) || Object.entries(t.levels).some(([type,n]) => !SIGNIFICANT.has(type) || !integer(n,BUILDINGS[type]?.maxLevel || 3) || n < 1)) fail();
      if (t.project !== undefined && t.project !== null && (!object(t.project) || !only(t.project,['type','level']) || !SIGNIFICANT.has(t.project.type) || !integer(t.project.level,BUILDINGS[t.project.type]?.maxLevel || 3) || t.project.level < 1)) fail();
    }
    for (const [key,a] of Object.entries(b.armies)) if (!object(a) || !only(a,['id','owner','tile','minimum','maximum','turn']) || key !== a.id || !string(a.id,80) || !owner(a.owner) || a.owner === null || !tile(a.tile) || !integer(a.minimum,2000000) || !integer(a.maximum,2000000) || a.maximum < a.minimum || !integer(a.turn,b.turn)) fail();
    for (const t of b.treaties) if (!object(t) || !only(t,['type','parties','expires']) || !treaties.has(t.type) || !Array.isArray(t.parties) || t.parties.length !== 2 || t.parties[0] === t.parties[1] || t.parties.some(p => !owner(p) || p === null) || !integer(t.expires,1000000)) fail();
    for (const war of b.wars) if (!string(war,100) || war.split(':').length !== 2 || war.split(':').some(p => !owner(p) || p === null) || war.split(':')[0] === war.split(':')[1]) fail();
  }
}
