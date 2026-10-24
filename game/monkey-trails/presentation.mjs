import {ELEMENTS,SPECIES,MOVES} from './data.mjs';

const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const finite=value=>Math.max(0,Number(value)||0);
export const NOTICE_PRIORITY={discovery:10,message:20,invitation:40,unlock:50,recruit:60,evolution:70,ultra:80,reward:90,conflict:100};
const STAT_LABELS={hp:'Maximum HP',atk:'Attack',def:'Defense',speed:'Speed',maxEnergy:'Maximum energy'};

// These records live inside the existing encrypted save. No locations or route
// history are added. Acknowledging a presentation never changes its game reward.
export function normalizePresentation(s){
  s.meta??={};
  const p=s.meta.presentation??={};
  p.activity=Array.isArray(p.activity)?p.activity.filter(row=>row&&typeof row.id==='string').slice(-160):[];
  p.reveals=Array.isArray(p.reveals)?p.reveals.filter(row=>row&&typeof row.id==='string').slice(-12):[];
  p.tutorial=Array.isArray(p.tutorial)?p.tutorial.filter(key=>typeof key==='string').slice(-24):[];
  return p;
}
export function recordActivity(s,event){
  const p=normalizePresentation(s),id=String(event.id||'').slice(0,120);
  if(!id)throw Error('An activity needs a stable event identity.');
  const existing=p.activity.find(row=>row.id===id);
  if(existing)return existing;
  const row={id,kind:String(event.kind||'message').slice(0,24),title:String(event.title||'Trail update').slice(0,120),detail:String(event.detail||'').slice(0,320),amount:finite(event.amount||1),important:Boolean(event.important),read:false,time:finite(event.time??Date.now())};
  p.activity.push(row);
  // Keep the latest information, preferring unread important notices. Rewards
  // remain in their original claim records even after an old notice is pruned.
  if(p.activity.length>160){const first=p.activity.findIndex(item=>item.read||!item.important);p.activity.splice(first<0?0:first,1);}
  return row;
}
export function activityRows(s){return [...normalizePresentation(s).activity].reverse();}
export function markActivityRead(s,id){const p=normalizePresentation(s);for(const row of p.activity)if(!id||row.id===id)row.read=true;}

// The view controller decides when a toast is visible. Important notices wait
// behind an open battle/reveal; only ordinary finds coalesce. This queue cannot
// create rewards, capture candidates, or combat state.
export class NotificationQueue{
  constructor(){this.pending=[];this.current=null;this.sequence=0;}
  push(event){
    const item={...event,priority:Number(event.priority??NOTICE_PRIORITY[event.kind]??NOTICE_PRIORITY.message),sequence:this.sequence++};
    if(item.id&&(this.current?.id===item.id||this.pending.some(row=>row.id===item.id)))return false;
    const merge=item.kind==='discovery'&&item.combineKey&&this.pending.find(row=>row.kind==='discovery'&&row.combineKey===item.combineKey);
    if(merge){merge.amount=finite(merge.amount||1)+finite(item.amount||1);merge.detail=item.detail||merge.detail;return true;}
    this.pending.push(item);this.pending.sort((a,b)=>b.priority-a.priority||a.sequence-b.sequence);return true;
  }
  next({blocked=false}={}){if(blocked||this.current)return null;this.current=this.pending.shift()||null;return this.current;}
  finish(){const item=this.current;this.current=null;return item;}
  entries(){return this.pending.map(row=>({...row}));}
  clear(){this.pending=[];this.current=null;}
}

export function renderActivity(s){
  const rows=activityRows(s);
  return `<section class="trail-activity"><div class="section-title"><h2>Trail activity</h2>${rows.some(row=>!row.read)?'<button data-action="activity-read" class="small">Mark as read</button>':''}</div>${rows.length?`<ol>${rows.map(row=>`<li class="${row.read?'is-read':'is-unread'}" data-activity-id="${esc(row.id)}"><span class="eyebrow">${esc(row.kind)}</span><strong>${esc(row.title)}${row.amount>1?` ×${Math.round(row.amount)}`:''}</strong>${row.detail?`<p>${esc(row.detail)}</p>`:''}${s.meta.presentation.reveals.some(reveal=>reveal.id===row.id)?`<button class="text-button" data-action="reveal-replay" data-id="${esc(row.id)}">Replay moment</button>`:''}</li>`).join('')}</ol>`:'<p class="muted">Discoveries, invitations and important rewards will be recorded here.</p>'}</section>`;
}
const defaultPortrait=p=>`<span class="reveal-placeholder" aria-hidden="true">${ELEMENTS[p.types?.[0]]?.icon||'✦'}</span>`;
const elementChips=p=>(p.types||SPECIES[p.species]?.types||[]).map(id=>ELEMENTS[id]?`<span class="type-chip" style="--element:${ELEMENTS[id].color}">${ELEMENTS[id].icon} ${esc(ELEMENTS[id].name)}</span>`:'').join('');
export function personality(p){
  const source=SPECIES[p.species]||{};
  return source.trait||({common:'Curious about every bend in the trail.',uncommon:'A bright companion with a talent all their own.',rare:'A remarkable friend with stories still to discover.',ultra:'An extraordinary presence, ready for a shared adventure.'}[source.rarity]||'A new friend for the road ahead.');
}
export function renderRecruitReveal(s,p,{portrait=defaultPortrait,duplicate=false,destination,collectionCount,momentId}={}){
  const sp=SPECIES[p.species]||{},owned=collectionCount??new Set(Object.values(s.collection||{}).map(row=>row.species)).size,inTeam=destination?destination==='team':s.run.party.includes(p.id),ultra=sp.rarity==='ultra';
  return `<section class="trail-reveal recruit-reveal ${ultra?'is-ultra':''}" data-reveal="recruit"${momentId?` data-reveal-id="${esc(momentId)}"`:''}><span class="eyebrow">${ultra?'AN EXTRAORDINARY FRIEND':'A NEW TRAIL COMPANION'}</span><div class="reveal-portrait">${portrait(p,{state:ultra?'arrival':'victory'})}</div><span class="reveal-rarity">${esc(sp.rarity||'companion')}</span><h1>${esc(p.nickname||sp.name||'New companion')}</h1><div class="type-chips">${elementChips(p)}</div><p class="reveal-personality">${esc(personality(p))}</p><p class="collection-progress">${owned} / ${Object.keys(SPECIES).length} species befriended</p><div class="notice">${inTeam?'Joined your active team. Your new friend is ready for the trail.':'Joined your Sanctuary. Bring this companion along at a camp or before the next expedition.'}</div>${duplicate?'<p class="fine">This is another individual of a known species. Keep your new friend, or review their exact bond-material conversion below.</p>':''}<button class="primary wide" data-action="${momentId?'reveal-done':'close-dialog'}"${momentId?` data-id="${esc(momentId)}"`:''}>Back to the trail</button></section>`;
}

// Capture both appearances and the actual stat result after committing an
// evolution. The replay is a saved receipt, never an evolve command.
export function evolutionMoment(before,after,{stats,formName,unlocked=()=>[]}={}){
  if(typeof stats!=='function')throw Error('Evolution presentation needs the actual combat stats.');
  const snap=p=>({id:p.id,species:p.species,nickname:p.nickname||'',types:[...(p.types||[])],stage:p.stage||0,level:p.level||1,...(p.mastery?{mastery:p.mastery}:{})});
  const oldStats=stats(before),newStats=stats(after),oldAbilities=new Set(unlocked(before));
  return {id:`evolution:${after.id}:${after.stage}:${[...(after.types||[])].sort().join('+')}:${after.mastery||''}`,kind:'evolution',before:snap(before),after:snap(after),name:formName?.(after)||SPECIES[after.species]?.name||'Evolved companion',stats:Object.fromEntries(Object.keys(STAT_LABELS).map(key=>[key,{before:finite(oldStats[key]),after:finite(newStats[key])}])),abilities:unlocked(after).filter(id=>!oldAbilities.has(id)),trait:after.mastery?`Mastery: ${ELEMENTS[after.mastery]?.name||after.mastery}`:''};
}
export function recruitMoment(s,p,{duplicate=false}={}){return {id:`recruit:${p.id}`,kind:'recruit',name:p.nickname||SPECIES[p.species]?.name||'New companion',monkey:{id:p.id,species:p.species,nickname:p.nickname||'',types:[...(p.types||[])],stage:p.stage||0,level:p.level||1},duplicate:Boolean(duplicate),destination:s.run.party.includes(p.id)?'team':'sanctuary',collectionCount:new Set(Object.values(s.collection||{}).map(row=>row.species)).size};}
export function renderSavedReveal(s,moment,options={}){return moment.kind==='evolution'?renderEvolutionReveal(moment,options):renderRecruitReveal(s,moment.monkey,{...options,duplicate:moment.duplicate,destination:moment.destination,collectionCount:moment.collectionCount,momentId:moment.id});}
export function rememberReveal(s,moment){
  const p=normalizePresentation(s);if(p.reveals.some(row=>row.id===moment.id))return false;
  p.reveals.push({...structuredClone(moment),seen:false});p.reveals=p.reveals.slice(-12);
  recordActivity(s,{id:moment.id,kind:moment.kind,title:moment.kind==='evolution'?`${moment.name} discovered`:`${moment.name} joined your adventure`,detail:'The result is saved. Replay it from Trail activity.',important:true});return true;
}
export function pendingReveal(s){return normalizePresentation(s).reveals.find(row=>!row.seen)||null;}
export function acknowledgeReveal(s,id){const row=normalizePresentation(s).reveals.find(row=>row.id===id);if(row)row.seen=true;markActivityRead(s,id);return row;}
export function renderEvolutionReveal(moment,{portrait=defaultPortrait}={}){
  return `<section class="trail-reveal evolution-reveal" data-reveal="evolution" data-reveal-id="${esc(moment.id)}"><span class="eyebrow">EVOLUTION DISCOVERED · SAVED</span><div class="evolution-reveal-pair"><div><small>Before</small>${portrait(moment.before,{state:'idle'})}</div><span aria-hidden="true">✦</span><div><small>New form</small>${portrait(moment.after,{state:'cast'})}</div></div><h1>${esc(moment.name)}</h1><div class="type-chips">${elementChips(moment.after)}</div><div class="reveal-stat-grid">${Object.entries(STAT_LABELS).map(([key,label])=>{const row=moment.stats[key],gain=row.after-row.before;return `<div><small>${label}</small><strong>${row.before} → ${row.after}</strong><b>${gain>=0?'+':''}${gain}</b></div>`;}).join('')}</div>${moment.abilities?.length?`<h3>New abilities</h3><p>${moment.abilities.map(id=>esc(MOVES[id]?.name||id)).join(' · ')}</p>`:''}${moment.trait?`<p>${esc(moment.trait)}</p>`:''}${moment.passive?`<h3>${esc(moment.passive.name)}</h3><p>${esc(moment.passive.desc)}</p>`:''}${moment.future?.length?`<details><summary>${moment.future.length} future techniques</summary>${moment.future.map(row=>`<p><strong>${esc(MOVES[row.id]?.name||row.id)}</strong> · ${esc(row.requirement)}${row.discovered?' · Permanently discovered':''}</p>`).join('')}</details>`:''}<p class="fine">Resources were spent when this form was saved. Closing or replaying this reveal never spends them again.</p><button class="primary wide" data-action="reveal-done" data-id="${esc(moment.id)}">Continue · skip animation</button></section>`;
}
export function renderUltraArrival(u,{portrait=defaultPortrait}={}){
  const p={species:u.species,types:SPECIES[u.species]?.types||[],stage:0};
  return `<div class="trail-reveal ultra-arrival is-ultra"><span class="eyebrow">AN ULTRA HAS ARRIVED</span><div class="reveal-portrait">${portrait(p,{state:'arrival'})}</div><h2>${esc(SPECIES[u.species]?.name||'Ultra companion')}</h2><div class="type-chips">${elementChips(p)}</div><p>A rare presence awaits. The sighting has no countdown. Choose your own safe route or use the safe-route challenge.</p></div>`;
}
export function playMomentSound(kind,prefs={},environment=globalThis){
  if(!prefs.sound)return false;
  const Context=environment.AudioContext||environment.webkitAudioContext;if(!Context)return false;
  try{const ctx=new Context(),gain=ctx.createGain();gain.connect(ctx.destination);const notes=kind==='ultra'?[392,523.25,659.25,783.99]:kind==='evolution'?[329.63,440,659.25]:kind==='recruit'?[523.25,659.25,783.99]:[660];
    notes.forEach((frequency,index)=>{const oscillator=ctx.createOscillator(),start=ctx.currentTime+index*.10;oscillator.type='sine';oscillator.frequency.value=frequency;oscillator.connect(gain);gain.gain.setValueAtTime(.025,start);gain.gain.exponentialRampToValueAtTime(.001,start+.18);oscillator.start(start);oscillator.stop(start+.20);if(index===notes.length-1)oscillator.onended=()=>ctx.close();});return true;
  }catch{return false;}
}
export function saveFeedback({text='',kind='local'},mode='gps'){
  if(kind==='error'||kind==='conflict'||/not saved locally|no device backup|device storage unavailable|device save failed|unable to encrypt/i.test(text))return text;
  if(mode==='practice')return 'Saved on this device · separate practice';
  if(kind==='saved')return 'Cloud synced';
  if(kind==='syncing')return 'Saved on this device · syncing';
  if(kind==='offline')return 'Saved on this device · waiting to sync';
  return 'Saved on this device · waiting to sync';
}
export function renderWelcomeChoices(){return `<div class="adventure-choices" role="group" aria-label="Choose your adventure"><button type="button" class="primary" data-action="welcome-new"><strong>New Adventure</strong><span>Choose a friend, name your save, then explore.</span></button><button type="button" data-action="welcome-continue"><strong>Continue</strong><span>Unlock your saved adventure with its name and password.</span></button></div>`;}
export function welcomeHelp(choice){return choice==='continue'?'Enter the same save name and password to return to your adventure and selected goal. Encrypted backups use the same credentials. There is no password reset.':'Choose a starter whose role you like, then protect your new save with a name and a unique password of at least 10 characters. Enable location when you are ready to walk.';}
export function tutorialHint(s){
  const seen=new Set(normalizePresentation(s).tutorial),r=s.run,party=(r.party||[]).map(id=>s.collection[id]).filter(Boolean),first=party[0];
  const candidates=[
    {id:'first-discovery',when:r.distance<76.2&&r.phase==='trail',title:'Your first trail discovery',detail:'Start walking on a safe route. Every 250 accepted feet brings fruit, essence, or useful supplies. You can pause and return later.'},
    {id:'battle-ready',when:Boolean(r.pendingEncounter),title:'Battle when you are ready',detail:'Your opponent waits while you keep walking. Stop somewhere safe, then choose Battle. Passing gives no battle rewards.'},
    {id:'battle',when:r.phase==='battle',title:'One action, one supply',detail:'Read the enemy intent, then choose an ability or switch. You may also use one supply in the same round. Element matchups change damage.'},
    {id:'recruit',when:r.phase==='befriend',title:'Invite a new friend',detail:'Use a Bond Charm to invite one defeated monkey. The opening encounters guarantee recruitment; later failures build protection. Extra friends rest in your Sanctuary.'},
    {id:'switch',when:party.length>1&&r.phase==='trail',title:'Build your crew',detail:'Bring up to three companions. Switch between conscious teammates in battle. Change team slots at a camp or before an expedition begins.'},
    {id:'training',when:first&&first.training>=22&&r.phase==='trail',title:'Help your companion grow',detail:'Open Team → Train to spend the exact fruit and XP shown. Training is an expedition upgrade; friendship and your collection remain after a restart.'},
    {id:'evolution',when:party.some(p=>p.level>=3&&p.stage===0)&&r.phase==='trail',title:'Discover an evolution',detail:'Open Team → Evolve to preview every stat increase and essence cost. Track the missing resources. Discovered forms remain in your collection; active forms reset with an expedition.'}
  ];return candidates.find(item=>item.when&&!seen.has(item.id))||null;
}
export function dismissTutorial(s,id){const p=normalizePresentation(s);if(!p.tutorial.includes(id))p.tutorial.push(String(id));}
export function renderTutorial(s){const h=tutorialHint(s);return h?`<aside class="context-tutorial" aria-label="Trail tip"><div><strong>${esc(h.title)}</strong><p>${esc(h.detail)}</p></div><button class="text-button" data-action="tutorial-dismiss" data-id="${h.id}" aria-label="Dismiss ${esc(h.title)} tip">Got it</button></aside>`:'';}
