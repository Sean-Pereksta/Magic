import { planningView } from './ai-knowledge.mjs';
import { economicNeeds } from './trade-economy.mjs';
import { armiesOf, atWar, distance, findPath, kingdom, neighbors, passable, settlements, strength } from './core.mjs';

// This assessment deliberately starts from the court's observations. Premium
// region registries describe the entire map and must never drive AI decisions.
export function premiumObjectives(world, owner, needs = null) {
  const s = planningView(world, owner), k = kingdom(s, owner), towns = settlements(s, owner);
  if (!k || !towns.length) return [];
  needs ??= economicNeeds(s, owner);
  const own = armiesOf(s, owner).filter(a => !a.scout), enemies = s.armies.filter(a => atWar(s, owner, a.owner));
  const objectives = Object.values(s.tiles).filter(t => t.fog !== 'unknown' && t.premiumRegion && t.premiumYield && passable(t)).map(tile => {
    const need = needs.find(n => n.resource === tile.premiumYield);
    const origin = [...towns].sort((a,b) => distance(a,tile)-distance(b,tile)||a.id.localeCompare(b.id))[0];
    const range = distance(origin, tile), resource = tile.premiumYield;
    const urgency = {Critical:40, Needed:29, Useful:19, Comfortable:10, Surplus:5, Excess:2}[need?.state] ?? 10;
    const shortage = Math.max(0, (resource === 'gold' ? 12 : 6) - (need?.production || 0));
    const rival = neighbors(s,tile).find(t => t.owner && t.owner !== owner)?.owner || (tile.owner !== owner ? tile.owner : null);
    const danger = enemies.filter(a => distance(s.tiles[a.tile],tile)<=3).reduce((n,a) => n+strength(a)*(a.confidence??1),0);
    const support = own.filter(a => distance(s.tiles[a.tile],tile)<=5).reduce((n,a) => n+strength(a),0);
    const personality = resource === 'gold' ? k.greed*12 : ['iron','horses'].includes(resource) ? k.aggression*10 : k.ambition*6;
    const score = urgency+shortage*1.5+((tile.premiumFactor||1.5)-1)*18+personality-range*2.1-
      Math.max(0,danger-support*.8)*.8-(tile.owner && tile.owner !== owner ? 22 : 0);
    return {tile,resource,origin,distance:range,rival,score};
  }).filter(o => o.distance <= 20).sort((a,b) => b.score-a.score||a.tile.id.localeCompare(b.tile.id));
  // Bound path searches even on large maps; geography and legal access decide
  // whether a rich site is a credible objective, never an arbitrary turn gate.
  return objectives.slice(0,12).filter(o => o.tile.owner===owner || o.origin.id===o.tile.id || findPath(s,o.origin.id,o.tile.id,owner).length);
}

export function premiumExpansionScore(s, owner, site, objectives) {
  let best = 0;
  for (const objective of objectives) {
    if (objective.tile.owner === owner || objective.tile.owner && !atWar(s,owner,objective.tile.owner)) continue;
    const remaining = distance(site,objective.tile), progress = objective.distance-remaining;
    if (progress<=0) continue;
    // A town should reach and preserve a productive deposit; settling directly
    // on timber or ore would replace the building that can exploit it.
    const reaches = remaining<=3 ? 16 : 0;
    const replaces = remaining===0 && objective.resource!=='gold' ? 24 : 0;
    best = Math.max(best,Math.max(0,objective.score)*Math.min(1,progress/4)*.6+reaches-replaces);
  }
  return best;
}
