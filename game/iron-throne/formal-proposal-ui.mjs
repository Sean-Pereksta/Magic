import { LABELS, describeIntent } from './diplomacy.mjs';
import { RESOURCES } from './data.mjs';
import { formalDescription, runFormalResponseQueue } from './formal-proposals.mjs';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const tileTypes=new Set(['PLEDGE_ATTACK','DEFEND','PLEDGE_DEFEND','POSITION','BUILD_DEFENSES','PLEDGE_BUILD','TERRITORY']);
const houseTypes=new Set(['JOINT_WAR','PLEDGE_WAR','EMBARGO','GUARANTEE','PLEDGE_PEACE']);
const statuses={waiting:'Waiting…',considering:'Considering…','awaiting-human':'Awaiting ruler',accepted:'Accepted · Active',declined:'Refused',counter:'Counteroffer',alternative:'Declined · Alternative support',invalid:'Unable / invalid'};
export function installFormalProposalUI(doc,options){
  const dialog=doc.createElement('dialog');dialog.id='formal-proposal-builder';dialog.setAttribute('aria-labelledby','formal-builder-title');doc.body.append(dialog);
  const privatePanel=doc.createElement('section');privatePanel.id='private-formal-proposals';doc.getElementById('messages').after(privatePanel);
  const button=doc.createElement('button');button.type='button';button.textContent='Offer / Request';button.id='private-offer-request';doc.getElementById('chat-form').before(button);button.onclick=()=>open({ruler:options.getRuler()});
  let scope=null,busy=false,considering=null,signature='',modifyId=null;
  const rows=()=>options.getState().cooperation?.formalProposals||[];
  const active=()=>options.getActor();
  function form(){return dialog.querySelector('form');}
  function open({councilId=null,ruler=null,proposal=null,intent=null,direction=null}={}){
    const s=options.getState(),actor=active(),c=councilId&&s.allianceCouncils?.find(c=>c.id===councilId),members=c?c.participants.filter(h=>h!==actor):[ruler||options.getRuler()];
    scope={councilId,members};modifyId=proposal?.status==='draft'?proposal.id:null;
    const i=intent||proposal?.intent||{type:'JOINT_WAR',duration:10};
    dialog.innerHTML=`<header class="dialog-header"><h2 id="formal-builder-title">Offer / Request</h2><button type="button" data-formal-close aria-label="Close proposal">×</button></header><form><p class="fine">Sending approves these exact terms. Each ruler decides independently. Accepted terms become active immediately.</p><label>Action<select name="type">${Object.entries(LABELS).filter(([id])=>!['MARRIAGE','INTELLIGENCE'].includes(id)&&(!c||!['WAR','BETRAY','VASSALAGE'].includes(id))).map(([id,label])=>`<option value="${id}" ${id===i.type?'selected':''}>${esc(id==='PLEDGE_ATTACK'?'Attack a specific location':id==='PLEDGE_DEFEND'?'Promise defense of a location':label)}</option>`).join('')}<option value="OPERATION" ${proposal?.operationId?'selected':''}>Join a coordinated operation</option></select></label><label>Commitment<select name="direction"><option value="request">Request from selected ruler(s)</option><option value="offer">Offer my commitment / mutual agreement</option></select></label><label class="formal-target">Target<select name="target"></select><button type="button" data-select-map="Choose proposal location">Select on Map</button></label><label class="formal-operation" hidden>Operation<select name="operation">${(s.cooperation?.operations||[]).filter(o=>o.owner===actor&&o.status==='Preparing').map(o=>`<option value="${o.id}">${esc(o.name)} · Hex ${esc(o.targetTile)}</option>`).join('')}</select></label><label>Within turns<input name="duration" type="number" min="2" max="20" value="${i.duration||10}"></label><div class="formal-resources"><p class="fine">Resource amounts per participating House. Requests ask them to send; offers send from your treasury.</p>${[0,1,2].map(n=>`<div class="form-row"><label>Resource<select name="resource${n}">${RESOURCES.map(r=>`<option>${r}</option>`).join('')}</select></label><label>Amount<input name="amount${n}" type="number" min="0" max="1000" value="0"></label></div>`).join('')}</div><div class="formal-receive"><label>Receive resource<select name="receiveResource">${RESOURCES.map(r=>`<option>${r}</option>`).join('')}</select></label><label>Receive amount<input name="receiveAmount" type="number" min="0" max="1000" value="${i.receiveAmount||0}"></label></div><fieldset><legend>Ask these rulers</legend>${members.map(h=>`<label><input type="checkbox" name="house" value="${h}" ${!proposal||proposal.requestedHouses.includes(h)?'checked':''}> ${esc(s.kingdoms.find(k=>k.id===h)?.name||h)}</label>`).join('')}</fieldset><p class="formal-error" role="alert"></p><div class="button-row"><button type="button" data-formal-close>Cancel</button><button type="submit" class="primary">${c?'Send to Council':'Send Proposal'}</button></div></form>`;
    const f=form();f.elements.direction.value=direction||proposal?.direction||'request';f.elements.receiveResource.value=i.receiveResource||'food';
    (i.giveItems||[{resource:i.giveResource||'food',amount:i.giveAmount||0}]).slice(0,3).forEach((x,n)=>{f.elements[`resource${n}`].value=x.resource;f.elements[`amount${n}`].value=x.amount;});
    updateTargets(i.targetId||proposal?.targetTile);if(!dialog.open)dialog.showModal();f.elements.type.focus();
  }
  function updateTargets(chosen){
    const s=options.getState(),f=form(),type=f.elements.type.value,tile=tileTypes.has(type),house=houseTypes.has(type);
    dialog.querySelector('.formal-target').hidden=!tile&&!house;dialog.querySelector('[data-select-map]').hidden=!tile;dialog.querySelector('.formal-operation').hidden=type!=='OPERATION';
    dialog.querySelector('.formal-resources').hidden=type==='OPERATION'||['WAR','BETRAY','WITHDRAW','PLEDGE_WAR','PLEDGE_ATTACK','PLEDGE_DEFEND','PLEDGE_BUILD','PLEDGE_PEACE','GUARANTEE'].includes(type);
    dialog.querySelector('.formal-receive').hidden=!['EXCHANGE','RECURRING','TRIBUTE','LOAN'].includes(type);
    const choices=tile?Object.values(s.tiles).filter(t=>t.fog!=='unknown'||t.knownCapital||t.id===chosen).map(t=>[t.id,t.fog==='unknown'?`Unexplored — Hex ${t.id}`:`${t.name||t.terrain} · Hex ${t.id}`]):s.kingdoms.filter(k=>k.id!==active()).map(k=>[k.id,k.name]);
    f.elements.target.innerHTML=choices.map(([id,label])=>`<option value="${id}">${esc(label)}</option>`).join('');if(chosen)f.elements.target.value=chosen;
  }
  dialog.addEventListener('change',e=>{if(e.target.name==='type')updateTargets();});
  dialog.addEventListener('click',e=>{if(e.target.closest('[data-formal-close]'))dialog.close();});
  async function submit(raw){const result=await options.action('formalSubmit',raw);if(!result?.ok){options.error(result?.error||'Proposal could not be submitted.');return result;}render();pump();return result;}
  dialog.addEventListener('submit',async e=>{
    e.preventDefault();const f=form(),type=f.elements.type.value,houses=[...f.querySelectorAll('[name=house]:checked')].map(x=>x.value),intent={type,duration:Number(f.elements.duration.value),targetId:tileTypes.has(type)||houseTypes.has(type)?f.elements.target.value:''};
    const items=[0,1,2].map(n=>({resource:f.elements[`resource${n}`].value,amount:Number(f.elements[`amount${n}`].value)})).filter(x=>x.amount>0);
    if(!dialog.querySelector('.formal-resources').hidden){intent.giveResource=items[0]?.resource||'gold';intent.giveAmount=items[0]?.amount||0;if(type==='AID'&&items.length)intent.giveItems=items;if(['EXCHANGE','RECURRING'].includes(type)&&items.length)intent.giveItems=items;}
    if(!dialog.querySelector('.formal-receive').hidden){intent.receiveResource=f.elements.receiveResource.value;intent.receiveAmount=Number(f.elements.receiveAmount.value);}
    const raw={councilId:scope.councilId,requestedHouses:houses,direction:f.elements.direction.value,...(type==='OPERATION'?{operationId:f.elements.operation.value}:{intent})};
    const result=await submit(raw);if(result?.ok){if(modifyId)await options.action('formalDismiss',{id:modifyId});dialog.close();}else dialog.querySelector('.formal-error').textContent=result?.error||'Review the terms.';
  });
  function card(p,s){
    if(p.status==='dismissed')return '';
    const own=p.proposer===active(),expired=s.turn>p.expires;
    if(p.status==='draft')return own?`<article class="formal-card" data-formal-card="${p.id}"><span class="eyebrow">INTERPRETED REQUEST · NOT SENT</span><h3>${esc(formalDescription(p))}</h3><p class="fine">Within ${p.intent?.duration||0} turns · ${p.requestedHouses.map(h=>esc(s.kingdoms.find(k=>k.id===h)?.name)).join(', ')}</p><div class="button-row"><button data-formal-ratify="${p.id}" ${expired?'disabled':''}>Ratify & Send</button><button data-formal-modify="${p.id}">Modify</button><button data-formal-dismiss="${p.id}">Dismiss</button></div></article>`:'';
    return `<article class="formal-card" data-formal-card="${p.id}"><span class="eyebrow">FORMAL PROPOSAL · TURN ${p.created}${p.intent?.duration?' · DEADLINE T'+((p.sentTurn||p.created)+p.intent.duration):''}</span><h3>${esc(formalDescription(p))}</h3><p class="fine">${esc(s.kingdoms.find(k=>k.id===p.proposer)?.name)} approved these terms. Each accepted commitment stands independently.</p>${p.requestedHouses.map(h=>{const r=p.responses[h],state=considering?.id===p.id&&considering.house===h?'considering':r.status;return `<section class="formal-response"><strong>${esc(s.kingdoms.find(k=>k.id===h)?.name)} — <span class="formal-status ${r.status}">${esc(statuses[state])}</span></strong><p>${esc(r.message)}</p>${r.voice&&r.voice!==r.message?`<blockquote>${esc(r.voice)}</blockquote>`:''}${r.counterIntent?`<p>${esc(describeIntent(r.counterIntent))}</p>`:''}${r.alternativeIntents?`<p>Offered support: ${r.alternativeIntents.map(i=>esc(i.type==='AID'?(i.giveItems||[{resource:i.giveResource,amount:i.giveAmount}]).map(x=>`${x.amount} ${x.resource}`).join(' + '):describeIntent(i))).join(' · ')}</p>`:''}${r.offerAnswered?`<p>Support / counteroffer ${esc(r.offerAnswered)}</p>`:!expired&&((r.status==='awaiting-human'&&h===active())||own&&['counter','alternative'].includes(r.status))?`<div class="button-row"><button data-formal-answer="${p.id}" data-house="${h}" data-decision="accept">Accept${r.status==='alternative'?' Support Offer':''}</button>${own?`<button data-formal-modify="${p.id}" data-house="${h}">Modify</button>`:''}<button data-formal-answer="${p.id}" data-house="${h}" data-decision="decline">Decline</button></div>`:''}</section>`;}).join('')}</article>`;
  }
  function render(){
    const s=options.getState(),all=rows(),council=doc.getElementById('alliance-council'),id=council?.dataset.councilId;
    const next=JSON.stringify([all,active(),options.getRuler(),id,considering,s.turn]);
    if(signature!==next){signature=next;privatePanel.innerHTML=all.filter(p=>!p.councilId&&p.audience.includes(active())&&p.audience.includes(options.getRuler())).map(p=>card(p,s)).join('');const holder=doc.querySelector('.alliance-formal-proposals');if(holder)holder.innerHTML=all.filter(p=>p.councilId===id).map(p=>card(p,s)).join('');}
    queueMicrotask(pump);
  }
  async function pump(){
    if(busy||!options.canAct())return;busy=true;
    try{await runFormalResponseQueue({next:()=>{if(!options.canAct())return null;for(const p of rows().filter(p=>p.proposer===active()&&p.approved&&p.status==='processing')){const house=p.requestedHouses.find(h=>p.responses[h].status==='waiting');if(house)return {id:p.id,house};}return null;},resolve:item=>options.action('formalResolve',item),voice:item=>{const p=rows().find(p=>p.id===item.id);return p?options.voice(p,item.house):null;},record:(item,message)=>options.action('formalVoice',{...item,message}),onStatus:(item,status)=>{considering=status==='considering'?item:null;render();}});}finally{busy=false;considering=null;renderWithoutPump();}
  }
  function renderWithoutPump(){signature='';const s=options.getState();privatePanel.innerHTML=rows().filter(p=>!p.councilId&&p.audience.includes(active())&&p.audience.includes(options.getRuler())).map(p=>card(p,s)).join('');const holder=doc.querySelector('.alliance-formal-proposals'),id=doc.getElementById('alliance-council')?.dataset.councilId;if(holder)holder.innerHTML=rows().filter(p=>p.councilId===id).map(p=>card(p,s)).join('');}
  doc.addEventListener('click',async e=>{
    const b=e.target.closest('[data-formal-ratify],[data-formal-dismiss],[data-formal-answer],[data-formal-modify]');if(!b||b.disabled)return;
    const id=b.dataset.formalRatify||b.dataset.formalDismiss||b.dataset.formalAnswer||b.dataset.formalModify,p=rows().find(p=>p.id===id);if(!p)return;
    if(b.dataset.formalModify){const r=p.responses[b.dataset.house];open({councilId:p.councilId,ruler:b.dataset.house||p.requestedHouses[0],proposal:r?{...p,requestedHouses:[b.dataset.house]}:p,intent:r?.counterIntent||r?.alternativeIntents?.[0],direction:r?.alternativeIntents?'request':p.direction});return;}
    const type=b.dataset.formalRatify?'formalRatify':b.dataset.formalDismiss?'formalDismiss':'formalAnswer',result=await options.action(type,{id,house:b.dataset.house,decision:b.dataset.decision});if(!result?.ok)options.error(result?.error||'Terms could not be applied.');render();
  });
  return {open,submit,render};
}
