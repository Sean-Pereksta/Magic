import { ROYAL_DECREES, openingActive, decreeRemaining } from './opening.mjs';
import { premiumRegion } from './premium-regions.mjs';
import { kingdomDevelopments } from './kingdom-developments.mjs';
import { BUILDINGS } from './data.mjs';
import { distance, neighbors, passable } from './world-hex.mjs';

const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const houseName = (s, id) => s.kingdoms.find(k => k.id === id)?.name || 'Unclaimed';
// UI helpers accept observed map data only. An unprojected tile has no fog
// status and is not authority to reveal its premium region or surroundings.
const known = t => t && ['visible','explored'].includes(t.fog);
const place = t => t.name || `Hex ${t.id}`;

export function royalOpeningPanel(s, owner) {
  const opening = s.kingdoms.find(k => k.id === owner)?.opening;
  if (s.ambitionVersion !== 1 || !opening || s.phase === 'founding') return '';
  const decree = ROYAL_DECREES[opening.decree], active = openingActive(s, owner), remaining = decreeRemaining(s, owner);
  const title = decree ? `${decree.name} · ${remaining ? `${remaining} turn${remaining === 1 ? '' : 's'} left` : 'concluded'}` : active ? 'Choose your Royal Decree' : 'Royal Decree window concluded';
  const commandBonus = s.turn <= 3 ? '<p class="fine">Opening orders: +2 build / recruit commands through turn 3. Infrastructure still adds commands, up to 8 total.</p>' : '';
  const charter = opening.charterUsed ? 'First Frontier Charter: used.' : 'First Frontier Charter: available. Your first additional town costs 25% fewer food, wood and stone (rounded up), with normal gold cost, and takes 2 turns. Placement rules apply.';
  const choices = !decree && active ? Object.values(ROYAL_DECREES).map(d => `<article class="decree-choice"><strong>${esc(d.name)}</strong><ul>${d.benefits.map(b => `<li>${esc(b)}</li>`).join('')}</ul><button type="button" data-decree="${d.id}">Issue ${esc(d.name)}</button></article>`).join('') : decree ? `<ul>${decree.benefits.map(b => `<li>${esc(b)}</li>`).join('')}</ul>` : '';
  return `<details class="royal-opening" ${!decree && active ? 'open' : ''}><summary>${esc(title)}</summary><div class="royal-opening-body">${active ? `<p class="fine">One decree per House. Benefits expire after turn 6${!decree ? ` · ${7 - s.turn} opening turns remain` : ''}. This choice uses no build / recruit order.</p>` : ''}${choices}${commandBonus}<p class="fine charter-status">${esc(charter)}</p></div></details>`;
}

function knownAccess(s, t) {
  const adjacent = neighbors(s, t), seen = adjacent.filter(known);
  if (seen.length < adjacent.length) return 'Approaches are partly unexplored; access has not been fully surveyed.';
  if (t.mountainPass) return 'A known mountain pass constrains the approach.';
  const blocked = seen.filter(n => !passable(n)).length;
  return blocked >= 2 ? 'Known mountains or shoreline limit the land approach.' : 'Known neighboring land offers an open approach. Normal borders and movement rules apply.';
}

export function premiumRegionPanel(s, t) {
  if (!known(t)) return '';
  const region = premiumRegion(s, t);
  if (!region) return '';
  const factor = t.premiumFactor || region.multiplier, building = BUILDINGS[region.building]?.name || region.building;
  const ownerLabel = t.fog === 'visible' ? 'Observed owner' : `Owner last observed T${t.observedTurn}`;
  return `<article class="premium-region"><span class="eyebrow">◆ RICH RESOURCE REGION</span><h3>${esc(region.name)}</h3><div class="stat-grid"><span>Natural potential</span><b>${esc(t.premiumYield || region.resource)}</b><span>Developed production</span><b>${esc(factor)}× normal base quality</b><span>${esc(ownerLabel)}</span><b>${esc(houseName(s, t.owner))}</b></div><p class="fine">Develop ${esc(building)}${region.resource === 'gold' && region.building !== 'harbor' ? ' and a local road' : ''} to use this potential. The region bonus replaces deposit quality; building tiers scale normally.</p><p class="fine">${esc(knownAccess(s, t))}</p>${t.fog !== 'visible' ? `<p class="fine">Dated observation from turn ${t.observedTurn}; ownership and development may have changed.</p>` : ''}</article>`;
}

export function premiumMapMarkers(s) {
  return Object.values(s.premiumRegions || {}).flatMap(region => {
    const t = s.tiles[region.center];
    return known(t) && t.premiumRegion === region.id ? [{tile:t.id, name:region.name, remembered:t.fog === 'explored'}] : [];
  });
}

const direction = (from, t) => {
  const dq = t.q - from.q, dr = t.r - from.r;
  if (Math.abs(dq) > Math.abs(dr) * 1.5) return dq > 0 ? 'east' : 'west';
  if (Math.abs(dr) > Math.abs(dq) * 1.5) return dr > 0 ? 'south' : 'north';
  return `${dr > 0 ? 'south' : 'north'}${dq > 0 ? 'east' : 'west'}`;
};

export function openingCouncilBriefing(s, owner) {
  if (s.phase === 'founding' || s.ambitionVersion !== 1 || s.turn > 6) return [];
  const own = Object.values(s.tiles).filter(t => known(t) && t.owner === owner), capital = own.find(t => t.capital === owner) || own.find(t => t.building === 'city');
  if (!capital) return [];
  const knownTiles = Object.values(s.tiles).filter(known), lines = [], byDistance = (a,b) => distance(capital,a) - distance(capital,b) || a.id.localeCompare(b.id);
  const regions = Object.values(s.premiumRegions || {}).map(r => ({region:r, tile:s.tiles[r.center]})).filter(x => known(x.tile) && distance(capital,x.tile) <= 12).sort((a,b) => byDistance(a.tile,b.tile));
  for (const {region, tile} of regions.slice(0,2)) lines.push({kind:'region', tile:tile.id, text:`${region.name} lies ${direction(capital,tile)} at ${tile.id}: ${region.resource} potential, ${tile.premiumFactor || region.multiplier}× base-quality production when developed.${tile.fog === 'explored' ? ` Last observed T${tile.observedTurn}.` : ''}`});
  const settlements = knownTiles.filter(t => ['city','town'].includes(t.building));
  const frontier = knownTiles.filter(t => !t.owner && BUILDINGS.town.terrain.includes(t.terrain) && distance(capital,t) >= 4 && distance(capital,t) <= 8 && settlements.every(x => distance(x,t) >= 4)).sort(byDistance)[0];
  if (frontier) lines.push({kind:'expansion', tile:frontier.id, text:`Known open land ${direction(capital,frontier)} near ${frontier.id} may support expansion. A town still requires materials, placement checks and a legal route.`});
  const approach = knownTiles.filter(t => distance(capital,t) <= 8 && distance(capital,t) >= 2 && passable(t) && (t.mountainPass || neighbors(s,t).filter(n => known(n) && !passable(n)).length >= 3)).sort(byDistance)[0];
  if (approach) lines.push({kind:'approach', tile:approach.id, text:`The surveyed approach at ${approach.id} is constrained by known terrain and may be useful to defend.`});
  const army = (s.armies || []).filter(a => a.owner !== owner && known(s.tiles[a.tile]) && s.tiles[a.tile].fog === 'visible' && Object.values(a.units || {}).reduce((n,v) => n + v, 0) >= 15 && own.some(t => distance(t,s.tiles[a.tile]) <= 3)).sort((a,b) => byDistance(s.tiles[a.tile],s.tiles[b.tile]))[0];
  if (army) lines.unshift({kind:'concern', tile:army.tile, house:army.owner, text:`${houseName(s,army.owner)} has an observed army near your frontier at ${army.tile}. Its intentions are unknown.`});
  const offer = (s.commerce?.offers || []).find(o => (o.to || 'ashen') === owner && o.status === 'pending' && o.expires >= s.turn);
  if (offer) lines.push({kind:'trade', house:offer.from, text:`${houseName(s,offer.from)} has sent a trade proposal. Review the exact terms with their ruler.`});
  return lines.slice(0,5);
}

export function openingBriefingPanel(s, owner) {
  if (s.phase === 'founding' || s.ambitionVersion !== 1 || s.turn > 6) return '';
  const briefing = openingCouncilBriefing(s, owner);
  return `<details class="opening-briefing" ${s.turn <= 3 ? 'open' : ''}><summary>Royal Council Briefing</summary><p class="fine">Based on surveyed land and received proposals. Unseen borders and forces remain unknown.</p>${briefing.length ? `<ul>${briefing.map(item => `<li><span>${esc(item.text)}</span>${item.tile ? `<button type="button" data-goto="${esc(item.tile)}">Inspect ${esc(item.tile)}</button>` : item.house ? `<button type="button" data-talk="${esc(item.house)}">Open diplomacy</button>` : ''}</li>`).join('')}</ul>` : '<p class="fine">No frontier opportunities have yet been confirmed. Your Royal Scouts reveal nearby land under the normal fog of war.</p>'}</details>`;
}

export function kingdomDevelopmentsPanel(s, owner, limit = 6) {
  const events = kingdomDevelopments(s, owner, limit);
  return `<section class="kingdom-developments" aria-label="Kingdom Developments"><div class="section-label">KINGDOM DEVELOPMENTS</div>${events.length ? events.map(e => `<article class="development-card"><div class="development-heading"><strong>${esc(e.title)}</strong><span>T${e.turn}</span></div><p>${esc(e.body)}</p><small>${esc(e.source || 'Known report')}${Number.isInteger(e.observedTurn) ? ` · observed T${e.observedTurn}` : ''}${e.tile ? ' · dated report; conditions may change' : ''}</small><div class="button-row">${e.tile ? `<button type="button" data-goto="${esc(e.tile)}">Inspect ${esc(e.tile)}</button>` : ''}${e.house && e.house !== owner ? `<button type="button" data-talk="${esc(e.house)}">Open diplomacy</button>` : ''}</div></article>`).join('') : '<p class="fine">Important observed frontier, military, construction and diplomatic changes will appear here after a turn resolves.</p>'}</section>`;
}
