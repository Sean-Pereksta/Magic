// Persistent battlefield art. Rules and expiry live in the engine; this module only draws.
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const drawings={
  mycelium:'<path d="M5 58Q25 44 31 29M30 63Q34 39 53 10M9 25Q27 28 42 48M42 48l20 9M25 43 9 48M34 30l16 1" fill="none" stroke="#326c64" stroke-width="8"/><path d="M5 58Q25 44 31 29M30 63Q34 39 53 10M9 25Q27 28 42 48M42 48l20 9M25 43 9 48M34 30l16 1" fill="none" stroke="#97efd1" stroke-width="3"/><path d="M14 38v9m30-26v11M45 50v9" stroke="#e5edbd" stroke-width="4"/><path d="M6 37q7-16 16 0ZM35 21q8-17 18 0ZM37 50q8-13 17 0Z" fill="#8fe6c2" stroke="#326c64" stroke-width="2"/><g fill="#edf7a9"><circle cx="10" cy="33" r="2"/><circle cx="43" cy="16" r="2"/><circle cx="48" cy="46" r="2"/></g>',
  thornfield:'<path d="M3 55Q10 30 34 37t31-19M5 20Q27 24 32 52t29 9" fill="none" stroke="#294f51" stroke-width="9"/><path d="M3 55Q10 30 34 37t31-19M5 20Q27 24 32 52t29 9" fill="none" stroke="#8bb9a0" stroke-width="4"/><g fill="#f0a7c6" stroke="#4f4669" stroke-width="2"><path d="m14 42-3-14 12 8ZM28 37l8-16 3 17ZM46 35l8 13 4-20ZM24 40l-9 15 16-4ZM45 59l-2-13 13 12Z"/></g>',
  rift:'<ellipse cx="35" cy="40" rx="27" ry="19" fill="#58487d" stroke="#ffb1da" stroke-width="3"/><ellipse cx="35" cy="40" rx="18" ry="12" fill="#292b52" stroke="#a79bdf" stroke-width="2"/><path d="m39 9-9 13 12 7-17 10 17 9-10 15" fill="none" stroke="#f0adff" stroke-width="4"/><g fill="#fbe9b4"><path d="m10 14 2 5 6 1-6 3-2 5-2-6-5-2 5-1ZM60 45l2 4 5 2-5 2-2 5-2-5-4-2 4-2Z"/><circle cx="23" cy="48" r="2"/><circle cx="46" cy="32" r="2"/></g>',
  cinders:'<path d="M4 48q6-17 12-10-4-24 10-16 8-25 15-2 19-10 17 9 16 0 8 22-20 20-62-3Z" fill="#694169" stroke="#d8839f" stroke-width="2"/><path d="M18 49q-8-9 1-18 1 10 8 5-4-15 7-20-2 13 9 18 9-8 13-3-1 20-18 22Z" fill="#ed9c8e" stroke="#ad5377" stroke-width="2"/><path d="M28 47q-2-9 7-14 8 10 5 15" fill="#ffe4a0"/><g fill="#ffd2a4"><circle cx="14" cy="17" r="3"/><circle cx="52" cy="12" r="2"/><circle cx="58" cy="54" r="3"/></g>',
  sanctum:'<circle cx="35" cy="35" r="27" fill="#fff1b21f" stroke="#fff1bc" stroke-width="3"/><circle cx="35" cy="35" r="20" fill="none" stroke="#e3c46e" stroke-width="2" stroke-dasharray="3 4"/><path d="m35 14 17 10-2 19-15 14-15-14-2-19Z" fill="#f5df9a55" stroke="#fdf3ca" stroke-width="2"/><path d="M35 23v24M25 35h20" stroke="#fff7d9" stroke-width="4"/><path d="M15 31q-11-2-10-13l13 6m37 7q11-2 10-13l-13 6" fill="#e5efd0" stroke="#998c5c" stroke-width="2"/>',
  prismwall:'<path d="M3 49 13 21l9 17 13-29 14 27 8-16 10 29-31 13Z" fill="#95cbd5aa" stroke="#fff1b6" stroke-width="3"/><path d="m13 21 1 28 21-40 1 53 13-26 8 14M3 49l33 13 31-13" fill="none" stroke="#f6e5aa" stroke-width="2"/><path d="m20 14 4-6m25 6-3-7M9 58l-5 5m57-5 5 5" stroke="#fff8d9" stroke-width="2"/>',
};
export const GROUND_ART_KINDS=Object.freeze(Object.keys(drawings));
export function groundArt(kind){return Object.hasOwn(drawings,kind)?`<svg class="ground-art" viewBox="0 0 70 70" aria-hidden="true">${drawings[kind]}</svg>`:'';}
export function groundLayerHTML(effects,time,definitions,viewSide='player'){
  const active=effects.filter(e=>e.expiresAt>time&&definitions&&Object.hasOwn(definitions,e.kind));
  if(!active.length)return '';
  const visible=active.slice(-2),friendly=e=>e.side===(viewSide==='local'?'player':viewSide);
  return `<span class="ground-layers" aria-hidden="true">${visible.map(e=>`<span class="ground-paint ground-${esc(e.kind)} ground-${friendly(e)?'friendly':'hostile'}" data-ground-art="${esc(e.id)}">${groundArt(e.kind)}</span>`).join('')}</span><span class="ground-badges">${visible.map(e=>`<small class="ground-clock ground-${friendly(e)?'friendly':'hostile'}" data-ground-id="${esc(e.id)}" data-expires-at="${e.expiresAt}" title="${esc(definitions[e.kind].name)} · ${esc(definitions[e.kind].description)}" aria-label="${esc(definitions[e.kind].name)}, ${Math.ceil(e.expiresAt-time)} seconds remaining">${viewSide==='local'?(e.side==='player'?'P1':'P2'):e.side===viewSide?'◆':'◇'} <span>${Math.ceil(e.expiresAt-time)}s</span></small>`).join('')}${active.length>2?`<small class="ground-more">+${active.length-2}</small>`:''}</span>`;
}
export function groundDescription(effects,time,definitions,viewSide='player'){
  return effects.filter(e=>e.expiresAt>time&&definitions&&Object.hasOwn(definitions,e.kind)).map(e=>`${definitions[e.kind].name}, ${viewSide==='local'?(e.side==='player'?'Player 1':'Player 2'):e.side===viewSide?'allied':'enemy'} field, ${Math.ceil(e.expiresAt-time)} seconds: ${definitions[e.kind].description}`).join('. ');
}
export function formationOverlayHTML(state,unit,status){
  if(!unit||unit.hp<=0||!status?.active||!Array.isArray(status.unitIds)||status.unitIds.length<2)return '';
  const members=[...new Set(status.unitIds)].map(id=>state.units.find(u=>u.id===id&&u.hp>0&&u.side===unit.side&&u.faction===unit.faction)).filter(Boolean),paths=[];
  if(members.length<2||!members.some(member=>member.id===unit.id))return '';
  if(unit.faction==='chaos'){
    for(const id of new Set(status.targetIds||[])){const target=state.units.find(u=>u.id===id&&u.hp>0&&u.side!==unit.side);if(target)for(const m of members)if(Math.abs(m.x-target.x)+Math.abs(m.y-target.y)===1&&members.some(partner=>partner.id!==m.id&&partner.x===target.x*2-m.x&&partner.y===target.y*2-m.y))paths.push(`M${m.x+.5},${m.y+.5}L${target.x+.5},${target.y+.5}`);}
  }else for(let i=0;i<members.length;i++)for(let j=i+1;j<members.length;j++){const a=members[i],b=members[j];if(Math.abs(a.x-b.x)+Math.abs(a.y-b.y)===1)paths.push(`M${a.x+.5},${a.y+.5}L${b.x+.5},${b.y+.5}`);}
  if(!paths.length)return '';
  return `<svg class="formation-links formation-${esc(unit.faction)}" viewBox="0 0 ${state.width} ${state.height}" preserveAspectRatio="none" aria-hidden="true" data-formation-links="${esc(unit.id)}">${paths.map(d=>`<path d="${d}" vector-effect="non-scaling-stroke"/>`).join('')}${members.map(m=>`<circle cx="${m.x+.5}" cy="${m.y+.5}" r=".32" vector-effect="non-scaling-stroke"/>`).join('')}</svg>`;
}
