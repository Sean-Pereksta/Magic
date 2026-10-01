import { OBJECTIVE_TYPES } from './strategic-locations.mjs';
import { diagnosticDetails } from './diagnostics.mjs';
import { ART } from './asset-manifest.mjs';
import { councilParticipants, councilActive, councilUnread, ownCouncil } from './council-state.mjs';
import { councilFacts } from './alliance-council.mjs';
import { followupCredit } from './proposal-followup.mjs';
import { diplomaticCapacity } from './living.mjs';
import { court } from './house-control.mjs';
const esc = v => String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const portrait = h => `<span class="alliance-portrait" style="--house:${h.color}" role="img" aria-label="${esc(h.name)}"><span>${esc(h.sigil)}</span><img src="${esc(ART.portraits[h.id])}" alt="" data-iron-art loading="lazy"></span>`;

export function allianceButtons(s, actor) {
  const records=(s.allianceCouncils||[]).filter(c=>c.participants.includes(actor)&&councilActive(s,c));
  const own=ownCouncil(s,actor), other=records.filter(c=>c.host!==actor);
  const button=(id,label,count)=>`<button class="dispatch-house alliance-dispatch" data-alliance="${esc(id)}"><span class="dispatch-sigil">⚜</span><span><b>${esc(label)}${count?` <em>${count}</em>`:''}</b><small>Shared ruler conversation</small></span></button>`;
  return (councilParticipants(s,actor).length>1?button(own?.id||'','Alliance Council',own?councilUnread(own,actor):0):'')+
    other.map(c=>button(c.id,`${s.kingdoms.find(k=>k.id===c.host).name.replace('House ','')} Council`,councilUnread(c,actor))).join('');
}

export function installAllianceCouncil(doc, options) {
  const dialog=doc.createElement('dialog');dialog.id='alliance-council';dialog.setAttribute('aria-labelledby','alliance-title');
  dialog.innerHTML=`<header class="dialog-header"><div><span class="eyebrow">THE ALLIED HOUSES</span><h2 id="alliance-title">Alliance Council</h2></div><button type="button" class="close" aria-label="Close Alliance Council">×</button></header>
    <div class="alliance-members" aria-label="Participating Houses"></div><p class="fine alliance-mood"></p>
    <button type="button" class="alliance-offer-request">Offer / Request</button><div class="alliance-formal-proposals"></div><div class="alliance-history" role="log" aria-live="polite" aria-relevant="additions" tabindex="0"></div>
    <button class="alliance-latest" hidden>Latest council messages ↓</button>
    <form class="alliance-compose"><label class="eyebrow" for="alliance-message">ADDRESS THE COUNCIL</label><textarea id="alliance-message" maxlength="600" rows="2" required placeholder="Name the goal and the role you ask each House to play…"></textarea>
    <div class="alliance-location"><label>Proposed action<select class="alliance-action">${Object.entries(OBJECTIVE_TYPES).map(([id,name])=>`<option value="${id}">${name}</option>`).join('')}</select></label><button type="button" class="alliance-map">Choose Location on Map</button><span class="alliance-location-label"></span><button type="button" class="alliance-location-clear" hidden>Remove location</button></div><div class="button-row"><label class="toggle"><input class="alliance-gemini" type="checkbox">Gemini conversation</label><small class="alliance-allowance"></small><button type="submit" class="primary">Send envoy →</button></div>
    <button type="button" class="alliance-diagnostics" hidden>Diagnostics</button><p class="fine alliance-ai-status" role="status"></p><div class="alliance-verification"></div><p class="fine alliance-notice" role="status"></p><p class="fine alliance-privacy" hidden>Gemini receives the council conversation and shared fictional game context. Keep personal information out of messages.</p></form>`;
  doc.body.append(dialog);
  const el=selector=>dialog.querySelector(selector), history=el('.alliance-history');
  let location=null;
  let currentId=null, signature='', pinned=true, busy=false, readSequence=-1,statusTimer=null;
  el('.alliance-offer-request').onclick=()=>options.offerRequest(currentId);
  el('.alliance-map').onclick=async()=>{const point=await options.pickLocation({initialTile:location?.targetTile,title:'Select Council location'});if(point){location={targetTile:point.tileId,objectiveType:el('.alliance-action').value};el('.alliance-location-label').textContent=point.displayName;el('.alliance-location-clear').hidden=false;}};
  el('.alliance-action').onchange=()=>{if(location)location.objectiveType=el('.alliance-action').value;};
  el('.alliance-location-clear').onclick=()=>{location=null;el('.alliance-location-label').textContent='';el('.alliance-location-clear').hidden=true;};
  el('.close').onclick=()=>dialog.close();
  dialog.addEventListener('close',()=>{clearInterval(statusTimer);statusTimer=null;options.onClose?.();});
  el('.alliance-diagnostics').onclick=()=>options.openDiagnostics?.(currentId);
  function updateStatus(){
    const diagnostic=options.diagnostic?.(currentId),seconds=options.cooldown?.()||0;
    el('.alliance-diagnostics').hidden=!diagnostic;
    el('.alliance-ai-status').textContent=diagnostic ? `${diagnosticDetails(diagnostic).reason} [${diagnostic.code}]${seconds?` Retry in ${seconds}s.`:' You can try another message.'}` : seconds ? `Gemini cooldown · retry in ${seconds}s.` : '';
  }
  history.addEventListener('scroll',()=>{pinned=history.scrollHeight-history.clientHeight-history.scrollTop<48;el('.alliance-latest').hidden=pinned;},{passive:true});
  el('.alliance-latest').onclick=()=>{pinned=true;history.scrollTop=history.scrollHeight;el('.alliance-latest').hidden=true;};
  el('.alliance-gemini').onchange=e=>options.setGemini(e.target.checked);
  dialog.addEventListener('click',e=>{
    const plan=e.target.closest('[data-council-location]');if(plan){const c=options.getState().allianceCouncils?.find(c=>c.id===currentId),m=c?.messages.find(m=>m.id===Number(plan.dataset.councilLocation));if(m?.location)options.planLocation(m.location);return;}
    const b=e.target.closest('[data-alliance-terms]');if(!b||b.disabled)return;
    const s=options.getState(),c=s.allianceCouncils?.find(c=>c.id===currentId),m=c?.messages.find(m=>m.id===Number(b.dataset.message));
    if(c&&councilActive(s,c))options.openTerms(b.dataset.allianceTerms,c.id,m?.requestedIntent||null);
  });
  el('form').onsubmit=async e=>{
    e.preventDefault();const message=el('textarea').value.trim();if(!message||busy||options.isBusy())return;
    busy=true;render();
    try {const result=await options.send(currentId,message,location);if(result?.ok===false)throw Error(result.error);el('textarea').value='';el('.alliance-location-clear').click();el('.alliance-notice').textContent=result?.notice||'The council has heard your envoy. Review terms before making commitments.';}
    catch(error){el('.alliance-notice').textContent=error.message;}
    finally{busy=false;render();options.changed();}
  };
  async function open(id='') {
    try {
      const previous=currentId;currentId=await options.open(id);if(previous!==currentId)el('.alliance-location-clear').click();if(!currentId)return;
      signature='';readSequence=-1;pinned=true;
      if(!dialog.open)dialog.showModal();clearInterval(statusTimer);statusTimer=setInterval(updateStatus,1000);render();options.onOpen?.(el('.alliance-verification'));el('textarea').focus();
    } catch(e){options.error(e.message);}
  }
  function render() {
    if(!dialog.open)return;
    updateStatus();
    const s=options.getState(),actor=options.getActor(),c=s.allianceCouncils?.find(c=>c.id===currentId&&c.participants.includes(actor));
    if(!c){dialog.close();return;}
    dialog.dataset.councilId=currentId;
    const active=councilActive(s,c),facts=councilFacts(s,c);
    el('.alliance-members').innerHTML=c.participants.map(id=>{const h=s.kingdoms.find(h=>h.id===id);return `<span title="${esc(h.ruler)}">${portrait(h)}<small>${esc(h.name.replace('House ',''))}</small>${id!==actor?`<button type="button" data-alliance-terms="${id}" ${!active?'disabled':''}>Terms</button>`:''}</span>`;}).join('');
    el('.alliance-mood').textContent=active?`Council mood: ${facts?.mood||'Cordial'}`:'This coalition has changed. Open the current council to continue.';
    const next=JSON.stringify([c.messages,s.proposalFollowups]);
    if(next!==signature){
      signature=next;const top=history.scrollTop;
      history.innerHTML=c.messages.map(m=>{
        const h=s.kingdoms.find(k=>k.id===m.speakerHouseId),credit=followupCredit(s,actor,h.id,c.id);
        return `${m.initiated?`<div class="dispatch-divider" role="separator"><b>NEW DISPATCH · TURN ${m.turn}</b><span>Alliance Council · ${esc(m.reason||"Council Concern")}</span></div>`:''}<article class="alliance-message ${h.id===actor?'from-player':''}" style="--speaker-color:${h.color}">${portrait(h)}<div><small>${esc(h.id===actor?'YOU':h.ruler)} · ${esc(h.name)} · Turn ${m.turn}${m.source==='gemini'?' · Gemini':m.source==='scripted'||m.initiated?' · Local dialogue':''}</small><p>${esc(m.message)}</p>${m.location?`<p>${esc(OBJECTIVE_TYPES[m.location.objectiveType])} · Hex ${esc(m.location.targetTile)} <button type="button" data-council-location="${m.id}">Plan operation here</button></p>`:''}${m.requestedIntent&&h.id!==actor&&active?`<button type="button" data-alliance-terms="${h.id}" data-message="${m.id}">${credit?'Requested terms · no extra envoy':'Review terms'}</button>`:''}</div></article>`;
      }).join('')||'<p class="fine alliance-empty">The allied rulers await your first proposal. Each House speaks for its own interests.</p>';
      history.scrollTop=pinned?history.scrollHeight:top;el('.alliance-latest').hidden=pinned;
    }
    const used=court(s,actor).messages,remaining=Math.max(0,diplomaticCapacity(s,actor)-(used.turn===s.turn?used.regular:0));
    el('.alliance-allowance').textContent=`${remaining} shared dispatches remaining`;
    el('[type="submit"]').disabled=busy||options.isBusy()||!active||!remaining||!!s.outcome;
    el('[type="submit"]').textContent=busy?'Envoy travelling…':'Send envoy →';
    const gemini=options.gemini();el('.alliance-gemini').checked=gemini.enabled;el('.alliance-gemini').disabled=!gemini.available;el('.alliance-privacy').hidden=!gemini.enabled;
    if(readSequence!==c.sequence){readSequence=c.sequence;options.read(c.id).catch(e=>options.error(e.message));}
  }
  return {dialog,open,render,setStatus:message=>{el('.alliance-notice').textContent=message;}};
}
