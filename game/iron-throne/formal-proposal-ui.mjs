import { LABELS, describeIntent } from './diplomacy.mjs';
import { RESOURCES } from './data.mjs';
import { isAiHouse } from './house-control.mjs';
import { formalDescription, formalConversationCurrent, formalReplyAvailable, runFormalResponseQueue } from './formal-proposals.mjs';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const tileTypes=new Set(['PLEDGE_ATTACK','DEFEND','PLEDGE_DEFEND','POSITION','BUILD_DEFENSES','PLEDGE_BUILD','TERRITORY']);
const houseTypes=new Set(['JOINT_WAR','PLEDGE_WAR','EMBARGO','GUARANTEE','PLEDGE_PEACE']);
const statuses={waiting:'Waiting',considering:'Considering…','awaiting-human':'Awaiting ruler',accepted:'Accepted · Active',declined:'Declined',counter:'Counteroffer',alternative:'Alternative Support',invalid:'Circumstances changed'};
const indicators={waiting:'○',considering:'…','awaiting-human':'○',accepted:'✓',declined:'✕',counter:'◐',alternative:'◐',invalid:'⚠'};
export function installFormalProposalUI(doc,options){
  const dialog=doc.createElement('dialog');dialog.id='formal-proposal-builder';dialog.setAttribute('aria-labelledby','formal-builder-title');doc.body.append(dialog);
  const privatePanel=doc.createElement('section');privatePanel.id='private-formal-proposals';doc.getElementById('messages').after(privatePanel);
  const button=doc.createElement('button');button.type='button';button.textContent='Offer / Request';button.id='private-offer-request';doc.getElementById('chat-form').before(button);button.onclick=()=>open({ruler:options.getRuler()});
  let scope=null,signature='',modifyId=null;
  const processors=new Map();
  const rows=()=>options.getState().cooperation?.formalProposals||[];
  const active=()=>options.getActor();
  function form(){return dialog.querySelector('form');}
  function open({councilId=null,ruler=null,proposal=null,intent=null,direction=null,replyTo=null}={}){
    const s=options.getState(),actor=active(),c=councilId&&s.allianceCouncils?.find(c=>c.id===councilId),members=replyTo?[replyTo.house]:c?c.participants.filter(h=>h!==actor):[ruler||options.getRuler()];
    if(councilId&&options.isCouncilBusy?.(councilId)){options.error('Wait for the current council conversation to finish.');return;}
    scope={councilId,members,replyTo};modifyId=proposal?.status==='draft'?proposal.id:null;
    const i=intent||proposal?.intent||{type:'JOINT_WAR',duration:10};
    dialog.innerHTML=`<header class="dialog-header"><h2 id="formal-builder-title">${replyTo?'Modify counteroffer':'Offer / Request'}</h2><button type="button" data-formal-close aria-label="Close proposal">×</button></header><form>${replyTo?'<p class="fine">The earlier counteroffer stays open until you send this revision. Sending closes those earlier terms and asks this House to consider your new offer.</p>':''}<p class="fine">Sending approves these exact terms. Each ruler decides independently. Accepted terms become active immediately.</p><label>Action<select name="type">${Object.entries(LABELS).filter(([id])=>!['MARRIAGE','INTELLIGENCE'].includes(id)&&(!c||!['WAR','BETRAY','VASSALAGE'].includes(id))).map(([id,label])=>`<option value="${id}" ${id===i.type?'selected':''}>${esc(id==='PLEDGE_ATTACK'?'Attack a specific location':id==='PLEDGE_DEFEND'?'Promise defense of a location':label)}</option>`).join('')}<option value="OPERATION" ${!intent&&proposal?.operationId?'selected':''}>Join a coordinated operation</option></select></label><label>Commitment<select name="direction"><option value="request">Request from selected ruler(s)</option><option value="offer">Offer my commitment / mutual agreement</option></select></label><label class="formal-target">Target<select name="target"></select><button type="button" data-select-map="Choose proposal location">Select on Map</button></label><label class="formal-operation" hidden>Operation<select name="operation">${(s.cooperation?.operations||[]).filter(o=>o.owner===actor&&o.status==='Preparing').map(o=>`<option value="${o.id}">${esc(o.name)} · Hex ${esc(o.targetTile)}</option>`).join('')}</select></label><label>Within turns<input name="duration" type="number" min="2" max="20" value="${i.duration||10}"></label><div class="formal-resources"><p class="fine">Resource amounts per participating House. Requests ask them to send; offers send from your treasury.</p>${Array.from({length:8},(_,n)=>n).map(n=>`<div class="form-row" data-resource-side="give" ${n>=Math.max(3,i.giveItems?.length||0)?'hidden':''}><label>Resource<select name="resource${n}">${RESOURCES.map(r=>`<option>${r}</option>`).join('')}</select></label><label>Amount<input name="amount${n}" type="number" min="0" max="1000" value="0"></label></div>`).join('')}<button type="button" data-add-formal-resource="give">Add resource</button></div><div class="formal-receive"><div class="form-row" data-resource-side="receive"><label><span data-formal-return-resource>Receive resource</span><select name="receiveResource">${RESOURCES.map(r=>`<option>${r}</option>`).join('')}</select></label><label><span data-formal-return-amount>Receive amount</span><input name="receiveAmount" type="number" min="0" max="1000" value="${i.receiveAmount||0}"></label></div>${Array.from({length:7},(_,n)=>n+1).map(n=>`<div class="form-row" data-resource-side="receive" ${n>=(i.receiveItems?.length||1)?'hidden':''}><label>Resource<select name="receiveResource${n}">${RESOURCES.map(r=>`<option>${r}</option>`).join('')}</select></label><label>Amount<input name="receiveAmount${n}" type="number" min="0" max="1000" value="0"></label></div>`).join('')}<button type="button" data-add-formal-resource="receive">Add resource</button></div><fieldset><legend>${replyTo?'Reply to':'Ask these rulers'}</legend>${members.map(h=>`<label><input type="checkbox" name="house" value="${h}" ${!proposal||proposal.requestedHouses.includes(h)?'checked':''}> ${esc(s.kingdoms.find(k=>k.id===h)?.name||h)}</label>`).join('')}</fieldset><p class="formal-error" role="alert"></p><div class="button-row"><button type="button" data-formal-close>Cancel</button><button type="submit" class="primary">${replyTo?'Send revised offer':c?'Send to Council':'Send Proposal'}</button></div></form>`;
    const f=form();f.elements.direction.value=direction||proposal?.direction||'request';f.elements.receiveResource.value=i.receiveResource||'food';
    (i.giveItems||[{resource:i.giveResource||'food',amount:i.giveAmount||0}]).slice(0,8).forEach((x,n)=>{f.elements[`resource${n}`].value=x.resource;f.elements[`amount${n}`].value=x.amount;});
    (i.receiveItems||[]).slice(0,8).forEach((x,n)=>{f.elements[`receiveResource${n||''}`].value=x.resource;f.elements[`receiveAmount${n||''}`].value=x.amount;});
    updateTargets(i.targetId||proposal?.targetTile);if(!dialog.open)dialog.showModal();f.elements.type.focus();
  }
  function updateTargets(chosen){
    const s=options.getState(),f=form(),type=f.elements.type.value,tile=tileTypes.has(type),house=houseTypes.has(type);
    dialog.querySelector('.formal-target').hidden=!tile&&!house;dialog.querySelector('[data-select-map]').hidden=!tile;dialog.querySelector('.formal-operation').hidden=type!=='OPERATION';
    dialog.querySelector('.formal-resources').hidden=type==='OPERATION'||['WAR','BETRAY','WITHDRAW','PLEDGE_WAR','PLEDGE_ATTACK','PLEDGE_DEFEND','PLEDGE_BUILD','PLEDGE_PEACE','GUARANTEE'].includes(type);
    const requested=f.elements.direction.value==='request'&&['EXCHANGE','RECURRING','LOAN'].includes(type);
    dialog.querySelector('[data-formal-return-resource]').textContent=requested?'Resource you return':'Resource you receive';
    dialog.querySelector('[data-formal-return-amount]').textContent=type==='LOAN'?(requested?'Amount you repay at deadline':'Repayment you receive at deadline'):(requested?'Amount you return':'Amount you receive');
    for(const button of dialog.querySelectorAll('[data-add-formal-resource]'))button.hidden=!['AID','EXCHANGE','RECURRING'].includes(type)||button.dataset.addFormalResource==='receive'&&!['EXCHANGE','RECURRING'].includes(type);
    for(const row of dialog.querySelectorAll('[data-resource-side=receive]'))if(!row.querySelector('[name=receiveResource]'))row.hidden=!['EXCHANGE','RECURRING'].includes(type)||!Number(row.querySelector('input').value);
    dialog.querySelector('.formal-receive').hidden=!['EXCHANGE','RECURRING','TRIBUTE','LOAN'].includes(type);
    const choices=tile?Object.values(s.tiles).filter(t=>t.fog!=='unknown'||t.knownCapital||t.id===chosen).map(t=>[t.id,t.fog==='unknown'?`Unexplored — Hex ${t.id}`:`${t.name||t.terrain} · Hex ${t.id}`]):s.kingdoms.filter(k=>k.id!==active()).map(k=>[k.id,k.name]);
    f.elements.target.innerHTML=choices.map(([id,label])=>`<option value="${id}">${esc(label)}</option>`).join('');if(chosen)f.elements.target.value=chosen;
  }
  dialog.addEventListener('change',e=>{if(['type','direction'].includes(e.target.name))updateTargets(form().elements.target.value);});
  dialog.addEventListener('click',e=>{if(e.target.closest('[data-formal-close]'))dialog.close();const add=e.target.closest('[data-add-formal-resource]');if(add){const row=dialog.querySelector(`[data-resource-side=${add.dataset.addFormalResource}][hidden]`);if(row){row.hidden=false;row.querySelector('select').focus();}if(!dialog.querySelector(`[data-resource-side=${add.dataset.addFormalResource}][hidden]`))add.hidden=true;}});
  async function submit(raw){const result=await options.action('formalSubmit',raw);if(!result?.ok){options.error(result?.error||'Proposal could not be submitted.');return result;}render();pump();return result;}
  dialog.addEventListener('submit',async e=>{
    e.preventDefault();const f=form(),type=f.elements.type.value,houses=[...f.querySelectorAll('[name=house]:checked')].map(x=>x.value),intent={type,duration:Number(f.elements.duration.value),targetId:tileTypes.has(type)||houseTypes.has(type)?f.elements.target.value:''};
    const items=Array.from({length:8},(_,n)=>n).map(n=>({resource:f.elements[`resource${n}`].value,amount:Number(f.elements[`amount${n}`].value)})).filter(x=>x.amount>0);
    if(!dialog.querySelector('.formal-resources').hidden){intent.giveResource=items[0]?.resource||'gold';intent.giveAmount=items[0]?.amount||0;if(type==='AID'&&items.length)intent.giveItems=items;if(['EXCHANGE','RECURRING'].includes(type)&&items.length)intent.giveItems=items;}
    if(!dialog.querySelector('.formal-receive').hidden){intent.receiveResource=f.elements.receiveResource.value;intent.receiveAmount=Number(f.elements.receiveAmount.value);if(['EXCHANGE','RECURRING'].includes(type)){intent.receiveItems=Array.from({length:8},(_,n)=>({resource:f.elements[`receiveResource${n||''}`].value,amount:Number(f.elements[`receiveAmount${n||''}`].value)})).filter(x=>x.amount>0);}}
    const raw={...(scope.replyTo?{replyTo:scope.replyTo}:{}),councilId:scope.councilId,requestedHouses:houses,direction:f.elements.direction.value,...(type==='OPERATION'?{operationId:f.elements.operation.value}:{intent})};
    const button=f.querySelector('[type=submit]');if(button.disabled)return;button.disabled=true;
    try{const result=await submit(raw);if(result?.ok){if(modifyId)await options.action('formalDismiss',{id:modifyId});dialog.close();}else dialog.querySelector('.formal-error').textContent=result?.error||'Review the terms.';}finally{button.disabled=false;}
  });
  function responseRow(p,h,s){
    const r=p.responses[h],state=r.status,own=p.proposer===active(),expired=s.turn>p.expires;
    const actionable=formalReplyAvailable(s,p,active(),h)||!expired&&r.status==='awaiting-human'&&h===active();
    const offered=r.counterIntent?formalDescription({...p,intent:r.counterIntent}):r.alternativeIntents?.map(i=>i.type==='AID'?(i.giveItems||[{resource:i.giveResource,amount:i.giveAmount}]).map(x=>`${x.amount} ${x.resource}`).join(' + '):describeIntent(i)).join(' · ');
    return `<section class="formal-response" data-formal-house="${h}" data-formal-status="${state}"><strong><span class="formal-indicator" aria-hidden="true">${indicators[state]}</span> ${esc(s.kingdoms.find(k=>k.id===h)?.name)} — <span class="formal-status ${state}">${esc(statuses[state])}</span></strong>${r.voiceComplete&&r.source==='failed'||r.source==='scripted'&&r.spoken?`<p class="formal-voice-unavailable">Gemini reply unavailable</p>${own&&r.source==='failed'&&formalConversationCurrent(s,p)?`<button type="button" data-formal-retry="${p.id}" data-house="${h}" ${processors.has(conversation(p))?'disabled':''}>Retry Gemini reply</button>`:''}`:''}${!p.councilId&&r.source==='gemini'&&r.voice?`<blockquote>${esc(r.voice)}</blockquote>`:''}${offered?`<div class="formal-counter-terms"><strong>${r.status==='alternative'?'Offered support':'Counteroffer terms'}</strong><p>${esc(offered)}</p></div>`:''}${r.offerAnswered?`<p class="formal-offer-result">${r.offerAnswered==='modified'?'Revised offer sent. The earlier terms are closed.':`Support / counteroffer ${esc(r.offerAnswered)}`}</p>`:actionable?`<div class="button-row formal-offer-actions"><button type="button" class="primary" data-formal-answer="${p.id}" data-house="${h}" data-decision="accept">Accept${r.status==='alternative'?' Support Offer':r.status==='counter'?' Counteroffer':''}</button><button type="button" data-formal-answer="${p.id}" data-house="${h}" data-decision="decline">Decline</button>${own?`<button type="button" data-formal-modify="${p.id}" data-house="${h}">Modify / New Offer</button>`:''}</div>`:expired&&['counter','alternative'].includes(state)?'<p class="fine">This offer has expired.</p>':''}</section>`;
  }
  function card(p,s,offersOnly=false){
    if(p.status==='dismissed')return '';
    const own=p.proposer===active(),expired=s.turn>p.expires,compact=!!p.councilId;
    if(p.status==='draft')return own?`<article class="formal-card" data-formal-card="${p.id}"><span class="eyebrow">INTERPRETED REQUEST · NOT SENT</span><h3>${esc(formalDescription(p))}</h3><p class="fine">Within ${p.intent?.duration||0} turns · ${p.requestedHouses.map(h=>esc(s.kingdoms.find(k=>k.id===h)?.name)).join(', ')}</p><div class="button-row"><button data-formal-ratify="${p.id}" ${expired?'disabled':''}>Ratify & Send</button><button data-formal-modify="${p.id}">Modify</button><button data-formal-dismiss="${p.id}">Dismiss</button></div></article>`:'';
    const houses=offersOnly?p.requestedHouses.filter(h=>formalReplyAvailable(s,p,active(),h)):p.requestedHouses;
    return `<article class="formal-card ${compact&&!offersOnly?'formal-tracker':''}" data-formal-card="${p.id}" aria-label="${offersOnly?'Unanswered offer':'Council Decision Tracker'}"><span class="eyebrow">${offersOnly?'UNANSWERED OFFER':compact?'COUNCIL DECISION TRACKER':'FORMAL PROPOSAL'} · TURN ${p.created}${p.intent?.duration?' · DEADLINE T'+((p.sentTurn||p.created)+p.intent.duration):''}</span><h3>${esc(formalDescription(p))}</h3>${compact?'':`<p class="fine">${esc(s.kingdoms.find(k=>k.id===p.proposer)?.name)} approved these terms. Each accepted commitment stands independently.</p>`}${houses.map(h=>responseRow(p,h,s)).join('')}</article>`;
  }
  function updateCards(holder,proposals,s,offersOnly=false){
    if(!holder)return;
    const keep=new Set();
    for(const p of proposals){
      const html=card(p,s,offersOnly);if(!html)continue;
      keep.add(p.id);const template=doc.createElement('template');template.innerHTML=html;
      const next=template.content.firstElementChild,existing=[...holder.children].find(e=>e.dataset.formalCard===p.id);
      if(existing){if(existing.innerHTML!==next.innerHTML)existing.innerHTML=next.innerHTML;existing.className=next.className;holder.append(existing);}else holder.append(next);
    }
    for(const node of [...holder.children])if(!keep.has(node.dataset.formalCard))node.remove();
  }
  function renderWithoutPump(){
    const s=options.getState(),all=rows(),id=doc.getElementById('alliance-council')?.dataset.councilId;
    const next=JSON.stringify([all,active(),options.getRuler(),id,s.turn]);
    if(signature===next)return;signature=next;
    updateCards(privatePanel,all.filter(p=>!p.councilId&&p.audience.includes(active())&&p.audience.includes(options.getRuler())),s);
    const councilRows=all.filter(p=>p.councilId===id),latest=councilRows.findLast(p=>p.approved&&p.status!=='dismissed');
    const history=doc.querySelector('.alliance-history'),pinned=history&&history.scrollHeight-history.clientHeight-history.scrollTop<48;
    updateCards(doc.querySelector('.alliance-formal-proposals'),councilRows.filter(p=>p===latest||p.status==='draft'),s);
    const outstanding=councilRows.filter(p=>p!==latest&&p.requestedHouses.some(h=>formalReplyAvailable(s,p,active(),h)));
    updateCards(doc.querySelector('.alliance-pending-offers'),outstanding,s,true);
    if(pinned)history.scrollTop=history.scrollHeight;
  }
  function render(){renderWithoutPump();queueMicrotask(pump);}
  const pendingHouse=p=>p.requestedHouses.find(h=>{const r=p.responses[h];return isAiHouse(options.getState(),h)&&r&&(['waiting','considering'].includes(r.status)||r.status!=='awaiting-human'&&!r.spoken&&!r.voiceComplete);});
  const conversation=p=>p.councilId||`private:${p.requestedHouses[0]}`;
  async function pump(){
    if(!options.canAct())return;
    const identity=options.getCampaignIdentity?.();
    const initial=rows().find(p=>(!processors.has(conversation(p))||processors.get(conversation(p)).identity!==identity)&&p.proposer===active()&&formalConversationCurrent(options.getState(),p)&&pendingHouse(p)&&(!p.councilId||!options.isCouncilBusy?.(p.councilId)));
    if(!initial)return;
    const key=conversation(initial),owner={identity};processors.set(key,owner);queueMicrotask(pump);
    const id=initial.id,actor=active(),turn=options.getState().turn,current=()=>rows().find(p=>p.id===id);
    const isCurrent=()=>options.canAct()&&active()===actor&&options.getState().turn===turn&&(!options.getCampaignIdentity||options.getCampaignIdentity()===identity)&&formalConversationCurrent(options.getState(),current());
    let result;
    try{
      result=await runFormalResponseQueue({
        isCurrent,gapMs:initial.councilId&&!options.getState().presentation?.reducedEffects?options.responseGapMs:0,
        next:()=>{if(!isCurrent())return null;const p=current(),house=pendingHouse(p);return house?{id,house,manualRetry:!!p.responses[house].retryRequested,resumeVoice:!['waiting','considering'].includes(p.responses[house].status)&&!p.responses[house].retryRequested}:null;},
        resolve:item=>{if(['waiting','considering'].includes(current().responses[item.house].status))return options.action('formalResolve',item);if(!item.manualRetry)item.resumeVoice=true;return {ok:true};},
        voice:item=>options.voice(current(),item.house),
        fallback:(item,error)=>({message:'',source:'failed',diagnostic:error?.diagnostic}),
        record:(item,reply)=>options.action('formalVoice',{...item,...(typeof reply==='string'?{message:'',source:'failed'}:{message:reply.message,source:reply.source,diagnostic:reply.diagnostic})}),
        onStatus:async(item,status)=>{
          let started;
          if(status==='considering'&&isCurrent()&&(['waiting','considering'].includes(current().responses[item.house].status)||current().responses[item.house].retryRequested))started=await options.action('formalConsider',item);
          renderWithoutPump();options.changed?.();return started;
        }
      });
    }catch(error){if(isCurrent())options.error(error.message||'The response could not be recorded.');}
    finally{if(processors.get(key)===owner)processors.delete(key);signature='';renderWithoutPump();if(result?.ok)queueMicrotask(pump);}
  }
  doc.addEventListener('click',async e=>{
    const b=e.target.closest('[data-formal-ratify],[data-formal-dismiss],[data-formal-answer],[data-formal-modify],[data-formal-retry]');if(!b||b.disabled)return;
    const id=b.dataset.formalRatify||b.dataset.formalDismiss||b.dataset.formalAnswer||b.dataset.formalModify||b.dataset.formalRetry,p=rows().find(p=>p.id===id);if(!p)return;
    if(b.dataset.formalModify){const house=b.dataset.house,r=p.responses[house];if(r&&!formalReplyAvailable(options.getState(),p,active(),house)){options.error('This offer has already been answered or expired.');return;}open({councilId:p.councilId,ruler:house||p.requestedHouses[0],proposal:r?{...p,requestedHouses:[house]}:p,intent:r?.counterIntent||r?.alternativeIntents?.[0],direction:r?.alternativeIntents?'request':p.direction,replyTo:r?{proposalId:p.id,house}:null});return;}
    const type=b.dataset.formalRetry?'formalRetryVoice':b.dataset.formalRatify?'formalRatify':b.dataset.formalDismiss?'formalDismiss':'formalAnswer',result=await options.action(type,{id,house:b.dataset.house,decision:b.dataset.decision});if(!result?.ok)options.error(result?.error||'Terms could not be applied.');render();
  });
  return {open,submit,render};
}
