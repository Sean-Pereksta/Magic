import { distance, neighbors, passable } from './world-hex.mjs';
import { settlements } from './core.mjs';
import { tileProduction } from './economy.mjs';

// Public geography only. This never inspects foreign plans or consumes RNG.
export function strategicLocation(s, tile, actor) {
  const around = neighbors(s, tile), open = around.filter(passable), reasons = [];
  let value = 0;
  if (passable(tile) && around.filter(t => t.terrain === 'mountain').length >= 2 && open.length <= 4) { value += 22; reasons.push('Mountain pass'); }
  if (tile.river && tile.road) { value += 18; reasons.push('River crossing'); }
  if (tile.road && open.filter(t => t.road).length >= 3) { value += 16; reasons.push('Road junction'); }
  if (tile.market || tile.tradeOutpost || tile.harbor) { value += 14; reasons.push('Trade route hub'); }
  const production = Object.values(tileProduction(tile, tile.owner || actor)).reduce((n, x) => n + x, 0);
  if (production >= 6 || around.filter(t => t.resource && ['rich', 'abundant'].includes(t.quality)).length >= 2) { value += Math.min(20, production + 8); reasons.push('Resource region'); }
  if (tile.building === 'fort' && around.some(t => t.owner !== tile.owner)) { value += 22; reasons.push('Frontier fort'); }
  const linked = settlements(s, tile.owner).filter(t => t.id !== tile.id && distance(t, tile) <= 7);
  if (['city', 'town'].includes(tile.building) && linked.length >= 2) { value += 18; reasons.push('Links settlements'); }
  if (tile.capital) { value += 18; reasons.push('Capital'); }
  return { value, reasons };
}

export function rankObjectives(s, actor, tiles, origin) {
  return tiles.map(tile => ({tile, ...strategicLocation(s, tile, actor)}))
    .map(x => ({...x, score: x.value - distance(origin, x.tile) * 2}))
    .sort((a, b) => b.score - a.score || a.tile.id.localeCompare(b.tile.id));
}
