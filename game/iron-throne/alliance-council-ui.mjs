import { installChatComposer, installChatOptions } from './chat-workspace.mjs';
import { OBJECTIVE_TYPES } from './strategic-locations.mjs';
import { diagnosticDetails } from './diagnostics.mjs';
import { ART } from './asset-manifest.mjs';
import { councilParticipants, councilActive, councilUnread, ownCouncil } from './council-state.mjs';
import { councilFacts } from './alliance-council.mjs';
import { followupCredit } from './proposal-followup.mjs';
import { diplomaticCapacity } from './living.mjs';
import { court, isAiHouse } from './house-control.mjs';
import { isFormalCouncilBusy } from './formal-proposals.mjs';
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
    <div class="alliance-history" role="log" aria-live="polite" aria-relevant="additions text" tabindex="0"><div class="alliance-message-list"></div><div class="alliance-formal-proposals"></div><div class="alliance-pending-offers" aria-label="Unanswered offers"></div><div class="alliance-failed-replies" role="status"></div></div>
    <button class="alliance-latest" hidden>Latest council messages ↓</button>
    <form class="alliance-compose"><label class="eyebrow" for="alliance-message">ADDRESS THE COUNCIL</label><textarea id="alliance-message" maxlength="600" rows="1" required placeholder="Write to the council…"></textarea>
    <div class="chat-toolbar"><details class="chat-options alliance-chat-options"><summary class="alliance-options-toggle" role="button" aria-controls="alliance-options-panel">Offer / Request <span aria-hidden="true">⌃</span></summary><div class="chat-options-panel" id="alliance-options-panel"><strong>Diplomatic actions</strong><button type="button" class="alliance-offer-request">Make an offer / request</button><div class="alliance-member-actions" aria-label="Individual ruler terms"></div>
    <div class="alliance-location"><label>Attach a map action<select class="alliance-action">${Object.entries(OBJECTIVE_TYPES).map(([id,name])=>`<option value="${id}">${name}</option>`).join('')}</select></label><button type="button" class="alliance-map">Choose Location on Map</button><span class="alliance-location-label"></span><button type="button" class="alliance-location-clear" hidden>Remove location</button></div>
    <strong>Gemini connection</strong><label class="toggle"><input class="alliance-gemini" type="checkbox">Gemini conversation</label><button type="button" class="alliance-retry-gemini" hidden>Retry undelivered Gemini replies</button><button type="button" class="alliance-diagnostics" hidden>Diagnostics</button><p class="fine alliance-ai-status" role="status"></p><p class="fine alliance-notice" role="status"></p><p class="fine alliance-privacy" hidden>Gemini receives the council conversation and shared fictional game context. Keep personal information out of messages.</p></div></details><small class="alliance-allowance"></small><button type="submit" class="primary">Send envoy →</button></div>
    <div class="chat-connection-row"><span class="alliance-connection-status chat-connection-status" role="status"></span><span class="alliance-attachment-summary" hidden></span></div><div class="alliance-verification chat-verification"></div></form>`;
  doc.body.append(dialog);
  const el=selector=>dialog.querySelector(selector), history=el('.alliance-history'),messageList=el('.alliance-message-list');
  installChatOptions(el('.chat-options'));const resizeComposer=installChatComposer(el('textarea'));
  let location=null;
  function updateLocation(){const text=location?`${OBJECTIVE_TYPES[location.objectiveType]} · Hex ${location.targetTile}`:'';el('.alliance-location-label').textContent=text;el('.alliance-attachment-summary').textContent=text;el('.alliance-attachment-summary').hidden=!location;el('.alliance-location-clear').hidden=!location;}
  let currentId=null, signature='', pinned=true, busy=null, readSequence=-1,statusTimer=null;
  el('.alliance-offer-request').onclick=()=>options.offerRequest(currentId);
  el('.alliance-map').onclick=async()=>{const point=await options.pickLocation({initialTile:location?.targetTile,title:'Select Council location'});if(point){location={targetTile:point.tileId,objectiveType:el('.alliance-action').value};updateLocation();}};
  el('.alliance-action').onchange=()=>{if(location){location.objectiveType=el('.alliance-action').value;updateLocation();}};
  el('.alliance-location-clear').onclick=()=>{location=null;updateLocation();};
  el('.close').onclick=()=>dialog.close();
  dialog.addEventListener('close',()=>{clearInterval(statusTimer);statusTimer=null;options.onClose?.();});
  el('.alliance-diagnostics').onclick=()=>options.openDiagnostics?.(currentId);
  el('.alliance-retry-gemini').onclick=async()=>{
    if(busy===currentId||options.isBusy(currentId)||!options.retry)return;
    const sendingId=currentId;busy=sendingId;render();
    try{const result=await options.retry(sendingId);if(result?.ok===false&&currentId===sendingId)el('.alliance-notice').textContent=result.error||'Gemini could not deliver this reply.';}
    catch(error){if(currentId===sendingId)el('.alliance-notice').textContent=error.message;}
    finally{if(busy===sendingId)busy=null;render();options.changed();}
  };
  function updateStatus(){
    const diagnostic=options.diagnostic?.(currentId),seconds=options.cooldown?.()||0;
    el('.alliance-diagnostics').hidden=!diagnostic;
    const connection=options.connection?.();el('.alliance-connection-status').textContent=diagnostic?'Gemini · Reply unavailable':seconds?`Gemini · Retry in ${seconds}s`:connection?.text||'Gemini';el('.alliance-connection-status').title=diagnostic?diagnosticDetails(diagnostic).reason:connection?.detail||'';el('.chat-options').dataset.attention=String(!!diagnostic||!!connection?.attention);
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
    e.preventDefault();const message=el('textarea').value.trim();if(!message||busy===currentId||options.isBusy(currentId)||isFormalCouncilBusy(options.getState(),currentId))return;
    const sendingId=currentId;busy=sendingId;render();
    try {const result=await options.send(sendingId,message,location);if(result?.ok===false)throw Error(result.error);if(currentId!==sendingId)return;el('textarea').value='';resizeComposer();el('.alliance-location-clear').click();el('.alliance-notice').textContent=result?.notice||'The council has heard your envoy. Review terms before making commitments.';}
    catch(error){if(currentId===sendingId)el('.alliance-notice').textContent=error.message;}
    finally{if(busy===sendingId)busy=null;render();options.changed();}
  };
  async function open(id='') {
    try {
      const previous=currentId;currentId=await options.open(id);if(previous!==currentId)el('.alliance-location-clear').click();if(!currentId)return;
      signature='';readSequence=-1;pinned=true;
      if(!dialog.open)dialog.showModal();clearInterval(statusTimer);statusTimer=setInterval(updateStatus,1000);render();options.onOpen?.(el('.alliance-verification'));history.focus({preventScroll:true});resizeComposer();
    } catch(e){options.error(e.message);}
  }
  function render() {
    if(!dialog.open)return;
    updateStatus();
    const s=options.getState(),actor=options.getActor(),c=s.allianceCouncils?.find(c=>c.id===currentId&&c.participants.includes(actor));
    if(!c){dialog.close();return;}
    dialog.dataset.councilId=currentId;
    const active=councilActive(s,c),facts=councilFacts(s,c);
    el('.alliance-members').innerHTML=c.participants.map(id=>{const h=s.kingdoms.find(h=>h.id===id);return `<span title="${esc(h.ruler)}">${portrait(h)}<small>${esc(h.name.replace('House ',''))}</small></span>`;}).join('');
    el('.alliance-member-actions').innerHTML=c.participants.filter(id=>id!==actor).map(id=>`<button type="button" data-alliance-terms="${id}" ${!active?'disabled':''}>${esc(s.kingdoms.find(k=>k.id===id).name)} · Terms</button>`).join('');
    el('.alliance-mood').textContent=active?`Council mood: ${facts?.mood||'Cordial'}`:'This coalition has changed. Open the current council to continue.';
    const next=JSON.stringify([c.messages,s.proposalFollowups,c.messages.map(m=>!!options.queued?.(c.id,m.id))]);
    if(next!==signature){
      signature=next;const top=history.scrollTop;
      messageList.innerHTML=c.messages.map(m=>{
        const h=s.kingdoms.find(k=>k.id===m.speakerHouseId),credit=followupCredit(s,actor,h.id,c.id);
        if(isAiHouse(s,h.id)&&m.source!=='gemini')return options.queued?.(c.id,m.id)?`<p class="fine alliance-delivery-status" role="status">Waiting for ${esc(h.name)}’s Gemini reply…</p>`:'';
        return `${m.initiated?`<div class="dispatch-divider" role="separator"><b>NEW DISPATCH · TURN ${m.turn}</b><span>Alliance Council · ${esc(m.reason||"Council Concern")}</span></div>`:''}<article class="alliance-message ${h.id===actor?'from-player':''}" style="--speaker-color:${h.color}">${portrait(h)}<div><small>${esc(h.id===actor?'YOU':h.ruler)} · ${esc(h.name)} · Turn ${m.turn}${options.queued?.(c.id,m.id)?' · Queued for Gemini':m.source==='gemini'?' · Gemini':m.source==='scripted'||m.initiated?' · Local dialogue':''}</small><p>${esc(m.message)}</p>${m.location?`<p>${esc(OBJECTIVE_TYPES[m.location.objectiveType])} · Hex ${esc(m.location.targetTile)} <button type="button" data-council-location="${m.id}">Plan operation here</button></p>`:''}${m.requestedIntent&&h.id!==actor&&active?`<button type="button" data-alliance-terms="${h.id}" data-message="${m.id}">${credit?'Requested terms · no extra envoy':'Review terms'}</button>`:''}</div></article>`;
      }).join('')||'<p class="fine alliance-empty">The allied rulers await your first proposal. Each House speaks for its own interests.</p>';
      history.scrollTop=pinned?history.scrollHeight:top;el('.alliance-latest').hidden=pinned;
    }
    const used=court(s,actor).messages,remaining=Math.max(0,diplomaticCapacity(s,actor)-(used.turn===s.turn?used.regular:0));
    el('.alliance-allowance').textContent=`${remaining} dispatches left`;
    const processing=busy===currentId||options.isBusy(currentId)||isFormalCouncilBusy(s,currentId);
    const sequence=c.activeSequence,failed=Object.keys(sequence?.failed||{});
    el('.alliance-failed-replies').innerHTML=failed.map(h=>`<p class="fine">${esc(s.kingdoms.find(k=>k.id===h)?.name||h)} — Gemini reply unavailable.</p>`).join('');
    el('.alliance-retry-gemini').hidden=!failed.length||sequence?.status!=='complete'||sequence?.turn!==s.turn||sequence?.actor!==actor||!active;
    el('.alliance-retry-gemini').disabled=processing||!!s.outcome;
    el('[type="submit"]').disabled=processing||!active||!remaining||!!s.outcome;
    el('.alliance-offer-request').disabled=processing||!active||!!s.outcome;
    el('[type="submit"]').textContent=processing?'Conversation in progress…':'Send envoy →';
    const speaker=c.activeSequence?.status==='pending'&&c.activeSequence.turn===s.turn&&c.activeSequence.currentSpeaker;
    if(speaker)el('.alliance-notice').textContent=`${s.kingdoms.find(k=>k.id===speaker)?.name||speaker} is considering your message…`;
    const gemini=options.gemini();el('.alliance-gemini').checked=gemini.enabled;el('.alliance-gemini').disabled=!gemini.available;el('.alliance-privacy').hidden=!gemini.enabled;
    if(readSequence!==c.sequence){readSequence=c.sequence;options.read(c.id).catch(e=>options.error(e.message));}
  }
  return {dialog,open,render,setStatus:message=>{el('.alliance-notice').textContent=message;}};
}
