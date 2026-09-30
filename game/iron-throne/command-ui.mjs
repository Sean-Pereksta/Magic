import { generalPortrait, generalSummary } from './general-presentation.mjs';
import { armyName } from './army-organization.mjs';
import { allArmies } from './naval-state.mjs';
import { GENERAL_QUALITIES, COMMAND_KINDS, generalForArmy, manualOverride } from './command-state.mjs';
import { hireGeneral, assignGeneral, detachGeneral, approveGeneralOrder, describeGeneralOrder, validateGeneralOrder, localGeneralReply, recordGeneralConversation } from './generals.mjs';
import { issueVassalCommand, acceptVassalRequest, vassalBond } from './vassals.mjs';
import { kingdom, sizeOf, treaty } from './core.mjs';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const friendlyArmies=(s,owner)=>s.armies.filter(a=>a.owner===owner||['alliance','vassalage'].some(type=>treaty(s,owner,a.owner,type)));
const armyOptions=(s,owner,chosen)=>`<option value="">Fixed location</option>${friendlyArmies(s,owner).map(a=>`<option value="${a.id}" ${a.id===chosen?'selected':''}>${esc(kingdom(s,a.owner).name)} · ${a.id} · ${a.tile}</option>`).join('')}`;
const quality=g=>GENERAL_QUALITIES[g.quality];
const abilities=g=>`+${Math.round(quality(g).bonus*100)}% command effectiveness · ${g.specialty==='movement'?`+${g.quality>=3?2:1} movement`:g.specialty==='mustering'?`${Math.min(5,2+g.quality)} city / ${g.quality>=2?2:1} town levy mustering per round, paid from gold, supplies and population, using one build/recruit order`:'Battle specialist'}`;
export function generalArmyControls(s,a){
  const g=generalForArmy(s,a),available=(s.commanders?.roster||[]).filter(g=>g.owner===a.owner);
  const old=a.legacyOrder;
  return `${old?`<p class="fine">Previous simultaneous-round order: ${esc(old.order)} ${esc(old.target)}. Review before restoring.</p><button data-legacy-order="${a.id}">Review previous order</button>`:''}${g?`<div class="commander-card"><strong>${generalPortrait(g)} General ${esc(g.name)}</strong><p>${esc(abilities(g))}</p><p>${g.objective?`${esc(g.objective.status)} · ${esc(g.objective.reason)}`:'Awaiting an objective'}${manualOverride(s,a)?'<br><strong>Player override · protected this activation</strong>':''}</p><button data-general-open="${g.id}">Chat</button><button data-general-open="${g.id}" data-show-orders>Orders</button><button data-general-detach="${g.id}" data-army="${a.id}">Return detachment to manual control</button></div>`:available.length?`<label>General<select data-general-choice="${a.id}">${available.map(g=>`<option value="${g.id}">${esc(g.name)} · ${quality(g).name}</option>`).join('')}</select></label><button data-general-assign="${a.id}">Assign general</button>`:'<p class="fine">Rare general candidates appear in your cities.</p>'}`;
}
export function generalRoster(s,owner){
  const roster=(s.commanders?.roster||[]).filter(g=>g.owner===owner);
  return `<section class="command-roster" id="realm-generals"><h3>Generals</h3>${roster.length?roster.map(g=>{
    const armies=allArmies(s).filter(a=>a.commandId===g.commandId),a=armies[0];
    return `<article class="commander-card"><div class="commander-heading">${generalPortrait(g)}<strong>${esc(g.name)} · ${quality(g).name}</strong></div><p>${generalSummary(g)}</p><p>${a?`${armies.map(a=>esc(armyName(a))).join(', ')} · ${armies.reduce((n,a)=>n+sizeOf(a),0)} troops`:'Awaiting Command'}<br>${esc(g.objective?.status||'No approved objective')} · ${esc(g.objective?.reason||'Ready for your orders.')}<br>${quality(g).upkeep} gold per round, including while unassigned</p><button data-general-open="${g.id}">Chat</button>${a?`<button data-goto="${a.tile}" data-army="${a.id}">View Army</button>`:''}<button data-general-open="${g.id}" data-show-orders>Change Orders</button><button data-general-unassign="${g.id}">Unassign all forces</button><button data-general-dismiss="${g.id}">Dismiss…</button></article>`;
  }).join(''):'<p class="fine">Candidates appear in owned cities. Sixteen named commanders exist across the campaign.</p>'}</section>`;
}
export function vassalPanel(s,owner,only=null){
  const bonds=s.treaties.filter(t=>t.type==='vassalage'&&t.liege===owner&&t.expires>s.turn&&(!only||t.vassal===only));
  const incoming=(s.cooperation?.vassalOrders||[]).filter(o=>o.vassal===owner&&!o.accepted&&o.status!=='Completed');
  if(!bonds.length&&!incoming.length)return '';
  const sites=Object.values(s.tiles).filter(t=>t.fog!=='unknown'&&(t.building||t.owner===owner));
  return `<section class="vassal-group"><h3>♛ Your Vassals</h3>${bonds.map(t=>{const h=kingdom(s,t.vassal),f=s.fealty?.[h.id],o=(s.cooperation?.vassalOrders||[]).filter(o=>o.vassal===h.id).at(-1);return `<article class="vassal-card" style="--house:${h.color}"><strong>♛↔♛ ${esc(h.name)}</strong><p>Liege: ${esc(kingdom(s,owner).name)} · Fealty: ${esc(f?.status||'Loyal')}</p><p>${esc(f?.reason||'The sworn relationship is being honored.')}</p>${o?`<p><b>${esc(o.status)}</b> · ${esc(o.kind)} ${esc(o.target)}<br>${esc(o.reason)}</p>`:''}<form data-vassal-form="${h.id}"><label>Objective<select name="kind">${COMMAND_KINDS.map(k=>`<option>${k}</option>`).join('')}</select></label><label>Location<select name="target">${sites.map(t=>`<option value="${t.id}">${esc(t.name||t.building||'Frontier')} · ${t.id}</option>`).join('')}</select></label><label>Reinforce a moving army (optional)<select name="army">${armyOptions(s,owner)}</select></label><button>Issue persistent command</button></form></article>`;}).join('')}${incoming.map(o=>`<article class="vassal-card"><p>Request from ${esc(kingdom(s,o.liege).name)}: ${esc(o.kind)} ${esc(o.target)}. Your armies remain under your control.</p><button data-vassal-accept="${o.id}">Accept obligation</button></article>`).join('')}</section>`;
}
export function installCommandUI({getState,getView,getOwner,perform,send,toast,canAct=()=>true,onOpen=()=>{},onClose=()=>{}}){
  const bar=document.createElement('section');bar.className='general-notice';bar.setAttribute('aria-live','polite');document.getElementById('resources').before(bar);
  const dialog=document.createElement('dialog');dialog.className='general-dialog';dialog.id='general-orders';document.body.append(dialog);
  const verification=document.createElement('section');verification.className='general-verification';
  dialog.addEventListener('close',onClose);
  let current=null,draft=null,sending=false;
  function open(id,orders=false){dialog.classList.toggle('show-orders',orders);current=id;draft=null;draw();if(!dialog.open)dialog.showModal();onOpen(verification);}
  function draw(){
    const s=getState(),owner=getOwner(),g=s.commanders?.roster.find(g=>g.id===current&&g.owner===owner);if(!g){if(dialog.open)dialog.close();return;}
    const view=getView(),targets=Object.values(view.tiles).filter(t=>t.fog!=='unknown'&&(t.building||t.owner===owner)),q=quality(g);
    const scroll=dialog.querySelector('.general-history')?.scrollTop||0;
    dialog.innerHTML=`<header class="dialog-header"><div><div class="commander-heading">${generalPortrait(g)}<div><span class="commander-sigil" aria-label="Commander">⚔</span><h2>${esc(g.name)}</h2></div></div><p>${q.name} · ${generalSummary(g)}</p></div><button data-general-close aria-label="Close orders">×</button></header><div class="general-body"><div><p class="general-assignment">${allArmies(s).filter(a=>a.commandId===g.commandId).map(a=>`${esc(armyName(a))} · ${sizeOf(a)} troops · ${esc(a.tile)}`).join('<br>')||'Awaiting Command'}</p><p>${g.objective?esc(describeGeneralOrder(g.objective)):'No approved objective.'}</p><div class="general-history" role="log">${g.history.map(m=>`<p class="general-message ${m.role}"><small>Round ${m.turn} · ${m.role==='general'?esc(g.name):m.role==='player'?'You':'Order record'}</small><br>${esc(m.text)}</p>`).join('')}</div><button data-toggle-orders>Review / Change Orders</button><form id="general-chat-form"><label>Message your general<textarea name="message" maxlength="600" required placeholder="Why have you stopped advancing?"></textarea></label><button ${sending||!canAct()?'disabled':''}>${sending?'Awaiting reply…':'Send to general'}</button><p class="fine">Discuss freely. Proposed orders require your approval.</p></form></div><form id="general-order-form"><h3>Review campaign orders</h3><label>Objective<select name="kind">${COMMAND_KINDS.map(k=>`<option ${k===(draft?.kind||g.objective?.kind)?'selected':''}>${k}</option>`).join('')}</select></label><label>Locations (up to 3)<select name="targets" multiple size="6">${targets.map(t=>`<option value="${t.id}" ${(draft?.targets||g.objective?.targets||[]).includes(t.id)?'selected':''}>${esc(t.name||t.building||'Frontier')} · ${t.id}</option>`).join('')}</select></label><label>Reinforce a moving army (optional)<select name="army">${armyOptions(view,owner,draft?.army||g.objective?.army)}</select></label><label>Regroup after losses (%)<input name="lossLimit" type="number" min="15" max="65" value="${draft?.lossLimit||g.objective?.lossLimit||35}" required></label><label><input type="checkbox" name="allowSplit" ${(draft?.allowSplit??g.objective?.allowSplit)?'checked':''}> Permit viable detachments</label><p class="fine">Captured objectives are held. No new wars or discretionary spending. Manual overrides are protected through this activation. Select multiple locations with Ctrl / Command.</p><button>Review interpretation</button>${draft?`<p class="order-review">${esc(describeGeneralOrder(draft))}</p><button type="button" data-general-approve ${!canAct()?'disabled':''}>Approve these exact orders</button>`:''}</form></div>`;
    dialog.querySelector('.general-history').scrollTop=scroll;
    dialog.querySelector('#general-chat-form').append(verification);
  }
  function update(){
    const s=getState(),owner=getOwner(),list=(s.commanders?.candidates||[]).filter(g=>g.owner===owner&&g.expires>=s.turn);
    bar.hidden=!list.length;bar.innerHTML=list.map(g=>`<details><summary>⚔ General available in ${esc(s.tiles[g.city]?.name||g.city)} — ${quality(g).cost} gold · ${quality(g).upkeep} gold per round</summary><p>${generalPortrait(g)} ${esc(g.name)} · ${quality(g).name} · ${esc(g.personality)}<br>${esc(abilities(g))}<br>Available through round ${g.expires}</p><button data-general-hire="${g.id}" ${!canAct()||kingdom(s,owner).resources.gold<quality(g).cost?'disabled':''}>Recruit for ${quality(g).cost} gold</button></details>`).join('');
    if(dialog.open&&!dialog.contains(document.activeElement))draw();
  }
  document.addEventListener('click',e=>{
    const b=e.target.closest('button');if(!b||b.disabled)return;const d=b.dataset,s=getState(),owner=getOwner();
    if(d.generalOpen){open(d.generalOpen,'showOrders'in d);return;}
    if('toggleOrders'in d){dialog.classList.toggle('show-orders');return;}
    if('generalClose'in d){dialog.close();return;}
    if(d.generalHire)perform('generalHire',{id:d.generalHire},()=>hireGeneral(s,owner,d.generalHire));
    if(d.generalAssign){const id=document.querySelector(`[data-general-choice="${d.generalAssign}"]`)?.value,g=s.commanders?.roster.find(g=>g.id===id),assigned=g?allArmies(s).filter(a=>a.commandId===g.commandId&&a.id!==d.generalAssign):[];const transfer=assigned.length>0;if(transfer&&!confirm(`Transfer Command: ${g.name} currently commands ${assigned.map(armyName).join(', ')}. Transfer to this army?`))return;if(id)perform('generalAssign',{id,army:d.generalAssign,transfer},()=>assignGeneral(s,owner,id,d.generalAssign,transfer));}
    if(d.generalDetach||d.generalUnassign){const id=d.generalDetach||d.generalUnassign,army=d.army||null;perform('generalDetach',{id,army},()=>detachGeneral(s,owner,id,army));}
    if(d.generalDismiss&&confirm('Dismiss this general? All troops remain under manual control. Future upkeep stops.'))perform('generalDetach',{id:d.generalDismiss,dismiss:true,confirmed:true},()=>detachGeneral(s,owner,d.generalDismiss,null,true,true));
    if('generalApprove'in d&&draft){perform('generalOrder',{id:current,order:draft},()=>approveGeneralOrder(s,owner,current,draft));draft=null;draw();}
    if(d.vassalAccept)perform('vassalAccept',{id:d.vassalAccept},()=>acceptVassalRequest(s,owner,d.vassalAccept));
  });
  document.addEventListener('submit',async e=>{
    const f=e.target,s=getState(),owner=getOwner();
    if(f.dataset.vassalForm){e.preventDefault();const order={kind:f.elements.kind.value,target:f.elements.target.value,...(f.elements.army.value?{army:f.elements.army.value}:{})},vassal=f.dataset.vassalForm;perform('vassalCommand',{vassal,order},()=>issueVassalCommand(s,owner,vassal,order));return;}
    if(f.id==='general-order-form'){
      e.preventDefault();const raw={kind:f.elements.kind.value,targets:[...f.elements.targets.selectedOptions].map(o=>o.value),lossLimit:Number(f.elements.lossLimit.value),allowSplit:f.elements.allowSplit.checked};
      if(f.elements.army.value){raw.army=f.elements.army.value;const ally=friendlyArmies(getView(),owner).find(a=>a.id===raw.army);if(ally)raw.targets=[ally.tile];}
      const result=validateGeneralOrder(s,owner,current,raw);if(!result.ok){toast(result.error);return;}draft=raw;draw();return;
    }
    if(f.id!=='general-chat-form')return;e.preventDefault();if(sending||!canAct())return;
    const message=f.elements.message.value.trim(),id=current,turn=s.turn,activation=s.sequential?.id;if(!message)return;
    sending=true;
    try{
      const response=await send(id,message);
      if(getState().turn!==turn||getState().sequential?.id!==activation||!canAct()){toast('The activation ended. Review your message before sending again.');return;}
      const reply=response||localGeneralReply(s,owner,id,message);
      perform('generalChat',{id,message,response:reply},()=>recordGeneralConversation(s,owner,id,message,reply));
      if(id===current&&reply.order&&validateGeneralOrder(getState(),owner,id,reply.order).ok){draft=reply.order;dialog.classList.add('show-orders');}
    }catch(error){toast(error.message);}finally{sending=false;draw();}
  });
  return {update,open};
}
