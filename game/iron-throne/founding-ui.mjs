import { HOUSES, BUILDINGS } from './data.mjs';
import { MAP_PROFILES } from './map-profiles.mjs';
import { CAPITAL_NAMES, foundingCheck, foundingOutlook, startingFootprint } from './founding.mjs';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function foundingPanel(s,owner,selected) {
  const founded=s.founding.houses[owner].founded,outlook=foundingOutlook(s,selected);
  const error=foundingCheck(s,owner,selected,{checkRemaining:false}),footprint=!error&&startingFootprint(s,owner,s.tiles[selected]);
  return `<span class="eyebrow">${esc(MAP_PROFILES[s.mapProfile]?.name)} · SEED ${s.seed}</span><h2>Found your kingdom</h2>
    <p>${founded?'Your capital is established. Explore the world while the other rulers choose their homes.':'Select land on the map, inspect its surroundings, then found your permanent capital.'}</p>
    <p class="fine">Starting territory reaches up to 4 hexes across usable land. Capitals must be at least <b>8 hexes apart</b>. Shaded red tiles are too close to an established capital. AI Houses choose after the human rulers.</p>
    <div class="section-label">FOUNDING OUTLOOK · ${esc(outlook?.region)}</div>
    <div class="stat-grid">${Object.entries(outlook?.ratings||{}).map(([r,value])=>`<span>${esc(r)}</span><b>${value}</b>`).join('')}</div>
    <div class="section-label">SURROUNDING TERRAIN</div><div class="stat-grid">${Object.entries(outlook?.terrain||{}).filter(([t])=>t!=='water').map(([t,count])=>`<span>${t}</span><b>${count>=20?'High':count>=8?'Moderate':count?'Nearby':'None'}</b>`).join('')}</div>
    ${!founded?`<p role="status" class="${error?'negative':'fine'}">${esc(error||'Your full starting package fits naturally within 4 hexes.')}</p><button class="primary full" data-found-city="${esc(selected)}" data-site-valid="${!error}" ${error?'disabled':''}>FOUND CITY</button>`:`<p class="fine">${esc(CAPITAL_NAMES[owner])} · ${esc(s.founding.houses[owner].capital)}</p>`}
    ${footprint?`<details><summary>Starting structures & locations</summary>${footprint.structures.map(p=>`<p class="fine">${esc(BUILDINGS[p.type].name)} · ${p.tile}</p>`).join('')}<p class="fine">Connected roads, barracks, archery range, army and full starting resources included. Terrain and deposits stay as generated.</p></details>`:''}
    <div class="section-label">THE SIX HOUSES</div>${HOUSES.map(h=>`<p class="fine">${h.sigil} ${h.name} · ${s.founding.houses[h.id].founded?`${CAPITAL_NAMES[h.id]} founded`:'Choosing a home'}</p>`).join('')}`;
}
