import { localHouseId } from './house-control.mjs';
import { armiesOf, distance, kingdom, sizeOf } from './core.mjs';
import { visiblePlans } from './espionage.mjs';

const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const supplies=value=>Object.entries(value||{}).map(([r,n])=>`${n} ${r}`).join(' · ')||'None';
const statusClass=status=>status==='fulfilled'||status==='Completed'?'fulfilled':status==='broken'||status==='Abandoned'?'broken':'';

function ownOperationCard(s,op,viewer){
  const plans=op.planIds.map(id=>s.intrigue.plans.find(p=>p.id===id)).filter(Boolean);
  const participants=op.participants.map(id=>{
    const p=plans.find(p=>p.actor===id),rally=s.tiles[op.rallyPoints[id]];
    const ready=armiesOf(s,id).filter(a=>rally&&distance(s.tiles[a.tile],rally)<=1).reduce((n,a)=>n+sizeOf(a),0);
    return `<div class="realm-card"><strong>${esc(kingdom(s,id).name)} · ${esc(op.roles[id])}</strong><p class="fine">${esc(p?.status||'Preparing')} · Rally: ${esc(rally?.name||op.rallyPoints[id])} · ${ready}/${op.requiredForces[id]} troops assembled</p><p class="fine">Supplies: ${p?.suppliesCommitted?'Allocated':'Not yet allocated'} · ${esc(supplies(op.supply[id]))}</p></div>`;
  }).join('');
  const commitments=op.commitments.map(id=>s.pledges.find(p=>p.id===id)).filter(Boolean);
  const pledgeRows=commitments.length?commitments.map(p=>`<p class="${statusClass(p.status)}"><strong>${esc(kingdom(s,p.debtor).name)}</strong> → ${esc(kingdom(s,p.creditor).name)} · ${esc(p.status==='fulfilled'?'Fulfilled':p.status==='broken'?'Broken':'Unfulfilled')}</p>`).join(''):'<p class="fine">No separate oath entries are attached to this operation.</p>';
  const preparation=plans.length?Math.round(plans.reduce((n,p)=>n+({Considering:10,Preparing:40,Committed:75,Executing:90,Completed:100,Abandoned:0}[p.status]??0),0)/plans.length):0;
  return `<article class="pledge-card"><span class="eyebrow">YOUR OPERATION</span><h3>${esc(op.name)}</h3><p><strong>Target:</strong> ${esc(kingdom(s,op.target).name)} · ${esc(s.tiles[op.targetTile]?.name||op.targetTile)}</p><p><strong>Objective:</strong> ${esc(op.objective)}</p><div class="stat-grid"><span>Preparation</span><b>${preparation}% · ${esc(op.status)}</b><span>Attack window</span><b>T${op.attackWindow[0]}–T${op.attackWindow[1]}</b><span>Siege requirement</span><b>${op.requiredSiege||'None'}</b><span>Exposure</span><b>${op.exposure}%</b></div><div class="section-label">PARTICIPANTS & ROLES</div>${participants}<div class="section-label">COMMITMENTS</div>${pledgeRows}${op.cancellationReason?`<p class="warning">${esc(op.cancellationReason)}</p>`:''}</article>`;
}
function discoveredOperationCard(s,r){
  const x=r.snapshot,o=x.operation;
  const target=x.target?kingdom(s,x.target)?.name:'Unknown target';
  if(!o)return `<article class="realm-card"><span class="eyebrow">PARTIAL INTELLIGENCE · T${r.turn}</span><h3>Suspected operation</h3><p>${esc(r.text)}</p><p class="fine">Details remain incomplete. This is a dated intelligence report, not live access to the enemy plan.</p></article>`;
  const participants=o.participants?.map(id=>kingdom(s,id)?.name||id).join(', ')||'Unknown';
  const full=r.detail>=3;
  return `<article class="realm-card"><span class="eyebrow">DISCOVERED OPERATION · T${r.turn}</span><h3>${esc(full?o.name:'Coordinated operation')}</h3><p><strong>Target:</strong> ${esc(target)}${full?` · ${esc(s.tiles[o.targetTile]?.name||o.targetTile)}`:''}</p><p><strong>Participants:</strong> ${esc(participants)}</p>${full?`<p><strong>Roles:</strong> ${esc(Object.entries(o.roles).map(([id,role])=>`${kingdom(s,id)?.name||id}: ${role}`).join(' · '))}</p><div class="stat-grid"><span>Observed status</span><b>${esc(o.status)}</b><span>Planned window</span><b>T${o.attackWindow[0]}–T${o.attackWindow[1]}</b><span>Siege requirement</span><b>${o.requiredSiege||'None'}</b><span>Exposure when observed</span><b>${o.exposure}%</b></div>`:`<p class="fine">${esc(r.text)} Exact objective, timing, roles and requirements remain unknown.</p>`}<p class="fine">Observed on turn ${r.turn}; later changes require fresh intelligence.</p></article>`;
}
export function warRoomPanel(s){
  const viewer=localHouseId(s),own=(s.intrigue?.operations||[]).filter(o=>o.participants.includes(viewer)).slice().reverse();
  const discovered=[],seen=new Set();
  for(const r of visiblePlans(s,viewer)){
    if(!(r.snapshot.operation||/military operation|coordinating against/i.test(r.text)))continue;
    const key=r.snapshot.operation?.id||r.planId;if(seen.has(key))continue;seen.add(key);discovered.push(r);
  }
  return `<span class="eyebrow">THE WAR ROOM</span><h2>Strategic operations</h2><p class="fine">Operations are authoritative simulation state: real objectives, commitments, preparation and consequences. Enemy plans appear only through dated intelligence reports.</p><div class="section-label">YOUR OPERATIONS</div>${own.length?own.map(op=>ownOperationCard(s,op,viewer)).join(''):'<p class="empty">No shared military operation has been ratified.</p>'}<div class="section-label">DISCOVERED ENEMY OPERATIONS</div>${discovered.length?discovered.map(r=>discoveredOperationCard(s,r)).join(''):'<p class="empty">No enemy operation has been discovered.</p>'}`;
}
