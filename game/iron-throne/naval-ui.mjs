import { fleetArrivalTurns, arrivalText } from './map-orders.mjs';
import { ART } from './asset-manifest.mjs';
import { buildingLevel } from './economy.mjs';
import { SHIPS, cargoCount, fleetCapacity, fleetSpeed, troopCount } from './naval-state.mjs';
import { boardingArrival, shipBuildCheck, embarkCheck, boardingCount, reservedCargo, boardingFleet, boardingStack } from './naval.mjs';
import { adjacentShore } from './naval-graph.mjs';
const escape=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cost=spec=>`${Object.entries(spec.cost).map(([r,n])=>`${n} ${r}`).join(' · ')} · ${spec.crew} population`;
const counts=f=>Object.entries(SHIPS).map(([id,spec])=>`${spec.name}: ${f.ships.filter(v=>v.type===id).length}`).join(' · ');
const button=(label,action,fleet,extra='',reason='')=>`<button data-naval-action="${action}" data-fleet="${escape(fleet)}" ${extra} ${reason?'disabled':''} title="${escape(reason||label)}">${label}</button>`;
export function shipyardPanel(s,tile,owner) {
  const t=s.tiles[tile];if(t?.owner!==owner||!buildingLevel(t,'shipyard'))return '';
  let wait=0;
  return `<section class="naval-panel"><h3>Build ships</h3><p class="fine">One vessel at a time per Shipyard. Crew and resources are paid when queued. Launches into nearby water, joining a friendly fleet or using the nearest open connected position.</p><div class="naval-build-list">${Object.entries(SHIPS).map(([id,spec])=>{
    const reason=shipBuildCheck(s,owner,tile,id);
    return `<article class="naval-build"><img data-iron-art src="${ART.ships[id]}" alt=""><div><strong>${spec.name}</strong><p>${spec.turns} turn${spec.turns>1?'s':''}${spec.capacity?' · 25 troop capacity':''}</p><p class="fine">${cost(spec)}</p>${button('Build','build','',`data-ship="${id}" data-tile="${tile}"`,reason)}${reason?`<p class="fine negative">${escape(reason)}</p>`:''}</div></article>`;
  }).join('')}</div><h4>Construction queue</h4>${s.shipQueues.filter(q=>q.tile===tile&&q.owner===owner).map(q=>{wait+=q.remaining;return `<div class="naval-queue"><span><b>${SHIPS[q.type].name}</b> · ${q.remaining===0?'Awaiting clear launch water':`${wait} turn${wait===1?'':'s'} until launch`}<br><small>${cost(SHIPS[q.type])} · paid</small></span>${button('Cancel','cancel','',`data-id="${q.id}"`)}</div>`;}).join('')||'<p class="fine">No vessels under construction.</p>'}</section>`;
}
export function boardingSummary(s,a) {
  if(a.embarkOrder?.status==='paused')return `Boarding paused: ${a.embarkOrder.reason}`;
  const f=(s.fleets||[]).find(f=>f.id===a.embarkOrder?.fleet&&f.owner===a.owner);
  if(!f)return 'Boarding waiting: transport is no longer available.';
  if(boardingCount(s,a,f)<Math.min(a.embarkOrder.count,troopCount(a)))return 'Boarding waiting: transport no longer has enough capacity.';
  const route=boardingArrival(s,a,f);
  return `Boarding ${Math.min(a.embarkOrder.count,troopCount(a))} troops ${route?arrivalText(route.turns):'— route unavailable'} · ${Math.max(0,troopCount(a)-a.embarkOrder.count)} stay ashore`;
}
export function fleetPanel(s,tile,owner) {
  const local=s.fleets||[],fleets=local.filter(f=>f.tile===tile||f.owner===owner&&s.armies.some(a=>a.tile===tile&&a.owner===owner)&&adjacentShore(s,f,tile));
  const remembered=(s.lastSeenFleets||[]).filter(f=>f.tile===tile).map(f=>`<p class="fine">Fleet last seen T${f.turn}: ${escape(counts(f))}. Current position and cargo unknown.</p>`).join('');
  return remembered+fleets.filter(f=>f.owner!==owner||boardingStack(s,f)[0].id===f.id).map(f=>{
    const own=f.owner===owner,house=s.kingdoms.find(k=>k.id===f.owner),acted=f.resolvedTurn===s.turn?'This fleet has already acted this turn.':'';
    const stack=own?boardingFleet(s,f):f;
    const troops=own?`${cargoCount(stack)}/${fleetCapacity(stack)} troops aboard`:'Cargo unknown';
    let html=`<section class="naval-panel" style="--fleet-color:${house.color}"><h3>${escape(house.name)} fleet</h3><p>${escape(counts(stack))}</p><p><b>${troops}</b> · ${f.node.startsWith('river:')?'River':'Ocean'} ${escape(f.tile)}</p>`;
    if(!own)return html+'<p class="fine">Observed vessels. Troop manifests and orders are private.</p></section>';
    html+=`<p class="fine">${reservedCargo(s,f)?`${reservedCargo(s,f)} troop seats reserved for boarding. `:''}${f.order!=='hold'?`Order: ${escape(f.order)} · ${arrivalText(fleetArrivalTurns(f,s))}. `:''}Warship range: 2 hexes · other vessels: adjacent water. Sailing: 2.5× standard infantry.</p><p class="fine">Movement: ${Math.max(0,Math.floor((fleetSpeed(f)+(f.sailingCarry||0))*(f.resolvedTurn===s.turn?0:1)))} / ${fleetSpeed(f,f.node.startsWith('river:'))} · ${escape(f.order)}${f.attackTile?` firing at ${escape(f.attackTile)}`:f.landing?` to land ${escape(f.landing)}`:f.target?` to ${escape(f.target.split(':')[1])}`:''}</p><details><summary>Hull, crew and cargo</summary>${stack.ships.map(v=>`<p class="fine">${SHIPS[v.type].name}: ${v.hp}/${SHIPS[v.type].hull} hull · ${v.crew}/${SHIPS[v.type].crew} crew${v.type==='transport'?` · ${(v.cargo||[]).reduce((n,a)=>n+a.count,0)}/25 troops`:''}</p>`).join('')}${stack.cargo.map(a=>`<p class="fine">${troopCount(a)} troops · ${escape(a.id)}${a.commandId?' · General aboard':''}</p>`).join('')}</details><div class="button-row">${['move','attack','unload','intercept','blockade','hold'].map(action=>button({move:'Move',attack:'Attack / bombard',unload:'Unload',intercept:'Intercept',blockade:'Blockade',hold:'Hold'}[action],action,f.id,'',acted||(action==='unload'&&!cargoCount(f)?'No troops aboard.':action==='blockade'&&!f.ships.some(v=>v.type==='warship')?'A blockade requires a Warship.':''))).join('')}</div>`;
    const armies=s.armies.filter(a=>a.owner===owner&&adjacentShore(s,f,a.tile));
    if(armies.length)html+=`<h4>Board troops</h4>${armies.map(a=>{const reason=embarkCheck(s,owner,a.id,f.id),count=boardingCount(s,a,f),pending=a.embarkOrder?.fleet===f.id;return `<p>${troopCount(a)} troops ashore · ${count} can board · ${troopCount(a)-count} stay ashore</p>${button(pending?boardingSummary(s,a):`Board ${count}`,'embark',f.id,`data-army="${a.id}"`,pending?'Boarding is already queued. Use the army’s Cancel boarding button to cancel.':reason)}${reason?`<p class="fine negative">${escape(reason)}</p>`:''}`;}).join('')}`;
    const others=local.filter(x=>x.owner===owner&&x.id!==f.id&&x.order!=='escort');
    if(others.length)html+=`<label>Friendly fleet<select data-fleet-partner="${f.id}">${others.map(x=>`<option value="${x.id}">${escape(x.id)} · ${escape(x.tile)} · ${x.ships.length} vessels</option>`).join('')}</select></label><div class="button-row">${button('Escort','escort',f.id,'',acted)}${button('Combine here','merge',f.id,'',acted)}</div>`;
    return html+'<p class="fine">Boarding, sailing, attacks and unloading resolve at the end of your turn. All troops on a sunk transport are lost. Vessels remain on water.</p></section>';
  }).join('');
}
export function navalOverview(s,owner) {
  const fleets=(s.fleets||[]).filter(f=>f.owner===owner);
  return `<div class="section-label">YOUR FLEETS</div>${fleets.map(f=>`<button class="full" data-goto="${f.tile}">⚓ ${f.ships.length} vessels · ${cargoCount(f)}/${fleetCapacity(f)} troops · ${f.tile}</button>`).join('')||'<p class="fine">Build a standalone Shipyard on an empty owned land tile beside ocean or river water to construct your first fleet.</p>'}`;
}
