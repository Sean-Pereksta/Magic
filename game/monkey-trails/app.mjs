import * as C from './core.mjs';
import {TrailGPS} from './gps.mjs';
import {Vault,importBackup} from './storage.mjs';
import {portrait} from './art.mjs';
const $=id=>document.getElementById(id),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let state=null,vault=null,selected='capuchin',tab='trail',busy=false,combatBusy=false,lastCheckpoint=0,toastTimer,scores=null;
let gpsInfo={reason:'Paused · tap Start walking when ready.',mph:null,accuracy:null};
const gps=new TrailGPS(fix=>{
  if(!state||state.run.phase!=='trail'||tab!=='trail')return pause();
  const oldCache=state.run.nextCache;C.advance(state,fix.meters,fix.east,fix.north);
  if(state.run.phase==='battle')pause('Rival found · tracking paused. Stop somewhere safe.');
  const milestone=state.run.nextCache!==oldCache||state.run.phase==='battle';
  if(milestone||Date.now()-lastCheckpoint>=5000)void checkpoint(milestone);
  render();
},info=>{gpsInfo=info;renderGPS();});
function toast(message){clearTimeout(toastTimer);$('toast').textContent=message;$('toast').hidden=false;toastTimer=setTimeout(()=>$('toast').hidden=true,4800);}
function saveStatus({text,kind}){$('save-status').textContent=text;$('save-dot').className=`save-dot ${kind}`;$('cloud-restore').hidden=kind!=='conflict';}
function starters(){
  $('starters').innerHTML=Object.entries(C.SPECIES).map(([id,p])=>`<button type="button" class="starter ${selected===id?'selected':''}" data-starter="${id}" aria-pressed="${selected===id}">${portrait({species:id,types:[],stage:0})}<span>${esc(p.name)}</span></button>`).join('');
  $('starter-trait').textContent=`${C.SPECIES[selected].title} · ${C.SPECIES[selected].trait}`;
}
starters();
$('starters').addEventListener('click',e=>{const b=e.target.closest('[data-starter]');if(b){selected=b.dataset.starter;starters();}});
function loginBusy(value){busy=value;document.querySelectorAll('#save-form button,#practice-button,#import-button').forEach(b=>b.disabled=value);}
async function enter(name,password,options){
  loginBusy(true);$('login-message').textContent='Opening your encrypted trail…';
  const nextVault=new Vault(saveStatus);
  try{
    const next=await nextVault.open(name,password,options);vault=nextVault;state=next;tab='trail';
    $('creature-password').value='';$('welcome').hidden=true;$('game').hidden=false;$('login-message').textContent='';
    $('mode-label').textContent=state.mode==='practice'?'PRACTICE · UNRANKED':'GPS TRAIL · FOREGROUND ONLY';
    $('creature-label').textContent=`${state.name} · Run ${state.meta.runs}`;
    if(state.run.phase==='battle')state.run.battle.ready=false;
    render();
  }catch(error){$('login-message').textContent=error.message;}
  finally{loginBusy(false);}
}
$('save-form').addEventListener('submit',e=>{e.preventDefault();if(busy)return;void enter($('creature-name').value,$('creature-password').value,{create:e.submitter?.value!=='open',species:selected,mode:'gps'});});
$('practice-button').addEventListener('click',async()=>{
  if(busy)return;loginBusy(true);$('login-message').textContent='Opening the separate practice save…';
  const v=new Vault(saveStatus),name='Trail Practice',password='practice-only-not-ranked';
  try{
    let s;try{s=await v.open(name,password,{mode:'practice'});}catch(e){if(!e.message.startsWith('No save found'))throw e;s=await v.open(name,password,{create:true,species:selected,mode:'practice'});}
    vault=v;state=s;tab='trail';$('welcome').hidden=true;$('game').hidden=false;$('login-message').textContent='';$('mode-label').textContent='PRACTICE · UNRANKED';
    if(state.run.phase==='battle')state.run.battle.ready=false;render();
  }catch(e){$('login-message').textContent=e.message;}finally{loginBusy(false);}
});
$('import-button').addEventListener('click',()=>{if(!busy)$('backup-file').click();});
$('backup-file').addEventListener('change',async e=>{
  const file=e.target.files[0];if(!file||busy)return;
  const name=$('creature-name').value,password=$('creature-password').value;
  if(!name.trim()||!password){$('login-message').textContent='Enter the backup creature name and password first.';e.target.value='';return;}
  if(file.size>200000){$('login-message').textContent='Backup file is too large.';e.target.value='';return;}
  if(!confirm('Restore this backup on this device? Existing device progress will be kept as a recovery copy. A newer cloud save will not be overwritten.')){e.target.value='';return;}
  loginBusy(true);
  try{const mode=await importBackup(name,password,await file.text());await enter(name,password,{mode});}
  catch(error){$('login-message').textContent=error.message;}finally{e.target.value='';loginBusy(false);}
});
function pause(message='Paused · tap Start walking to resume.'){
  gps.stop();gpsInfo={reason:message,mph:null,accuracy:null};renderGPS();if(state&&vault)void checkpoint(true);
}
async function checkpoint(urgent=false){if(!state||!vault)return;lastCheckpoint=Date.now();try{await vault.checkpoint(state,urgent);}catch(e){saveStatus({text:e.message,kind:'error'});}}
const feet=n=>Math.floor(n/C.FEET+1e-6).toLocaleString();
function chip(type){return `<span class="type-chip" style="color:${C.ELEMENTS[type].color}">${C.ELEMENTS[type].icon} ${C.ELEMENTS[type].name}</span>`;}
function chips(p){return p.types.length?p.types.map(chip).join(''):'<span class="type-chip" style="color:#edc77d">◇ Unevolved</span>';}
function bar(value,max,label,kind=''){return `<div class="meter-label"><span>${esc(label)}</span><span>${Math.round(value)} / ${Math.round(max)}</span></div><div class="meter ${kind}" role="progressbar" aria-label="${esc(label)}" aria-valuenow="${Math.round(value)}" aria-valuemin="0" aria-valuemax="${Math.round(max)}"><i style="width:${C.clamp(value/max*100,0,100)}%"></i></div>`;}
function resources(){const r=state.run;return `<div class="fruit-row"><span>● ${r.fruit} fruit</span><span>◇ ${state.meta.legacy} legacy</span></div><div class="resources">${Object.entries(C.ELEMENTS).map(([id,e])=>`<div class="resource" style="color:${e.color}"><span>${e.icon} ${e.direction} · ${e.name}</span><b>${r.essence[id]}</b><span>${e.resource}</span></div>`).join('')}</div>`;}
function journal(){return `<section class="panel"><h2>Trail journal</h2><ol class="log">${[...state.log].reverse().map(t=>`<li>${esc(t)}</li>`).join('')}</ol></section>`;}
function practiceControls(){return `<div class="notice practice"><strong>Practice only — simulated movement.</strong> This separate save never posts scores. Each button adds 250 feet in that direction.</div><div class="button-row">${Object.entries(C.DIRECTIONS).map(([d,t])=>`<button data-action="practice-step" data-id="${d}">${d} ${C.ELEMENTS[t].icon}</button>`).join('')}</div>`;}
function trail(){
  const r=state.run,p=r.monkey,st=C.playerStats(state),bearing={N:0,E:90,S:180,W:270}[r.direction];
  if(r.phase==='dead')return death();if(r.phase==='battle')return battle();
  const cacheLeft=r.nextCache-r.distance,encounterLeft=r.nextEncounter-r.distance;
  return `<div class="two-col"><div class="stack"><section class="panel trail-hero"><div class="hero-copy"><span class="eyebrow">RUN ${state.meta.runs} · ${r.wins} VICTORIES</span><div class="big-distance">${feet(r.distance)}<small>ft</small></div><div class="distance-sub">${(r.distance/1609.344).toFixed(2)} miles on this run</div><h2 style="margin:25px 0 4px">${esc(C.formName(p))}</h2><span class="muted">Level ${p.level}</span><div class="type-chips">${chips(p)}</div></div>${portrait(p)}</section><section class="panel">${bar(p.hp,st.hp,'Companion health')}${bar(C.RESOURCE_STEP-cacheLeft,C.RESOURCE_STEP,`${Math.ceil(cacheLeft/C.FEET)} ft to the next cache`,'cache')}${bar(C.ENCOUNTER_STEP-encounterLeft,C.ENCOUNTER_STEP,`${Math.ceil(encounterLeft/C.FEET)} ft to rival ${r.encounters+1}`)}<div style="margin-top:20px">${resources()}</div><p class="fine">Every fifth rival is a guardian. Stop to train, equip, and heal before the next encounter.</p>${state.mode==='practice'?practiceControls():''}</section></div>
    <div class="stack"><section class="panel tracking-panel"><h2>Follow your own path</h2><div class="compass" aria-label="Current resource direction ${r.direction}">${[['N','n'],['S','s'],['E','e'],['W','w']].map(([d,pos])=>`<span class="bearing ${pos}" style="color:${C.ELEMENTS[C.DIRECTIONS[d]].color}"><b>${d}</b>${C.ELEMENTS[C.DIRECTIONS[d]].name}</span>`).join('')}<div class="needle" style="transform:rotate(${bearing}deg)"></div></div><div id="gps-status" class="gps-status"></div><div id="gps-meta" class="gps-meta"></div>${state.mode==='gps'?'<button data-action="tracking" id="tracking-button" class="primary wide">Start walking</button>':'<div class="tag">SIMULATED LOCATION</div>'}<p class="fine">Compass origin: the previous fight. A new location fix is required after pauses. No maps or routes are uploaded.</p></section><section class="panel"><h2>Rival element forecast</h2>${Object.entries(C.odds(state)).map(([type,n])=>`<div class="meter-label"><span style="color:${C.ELEMENTS[type].color}">${C.ELEMENTS[type].icon} ${C.ELEMENTS[type].name}</span><strong>${Math.round(n*100)}%</strong></div><div class="meter"><i style="width:${n*100}%;background:${C.ELEMENTS[type].color}"></i></div>`).join('')}<p class="fine" style="margin-top:12px">Direction biases encounters, but never guarantees them. Later rivals may have two elements.</p></section>${journal()}</div></div>`;
}
function fighter(p,st,isEnemy){return `<div class="fighter">${portrait(p,{enemy:isEnemy})}<h3>${esc(C.formName(p))}</h3><small>Level ${p.level}${p.elite?' · GUARDIAN':''}</small><div class="type-chips">${chips(p)}</div>${bar(p.hp,st.hp,isEnemy?'Rival health':'Your health',isEnemy?'enemy':'')}${!isEnemy?bar(p.energy,st.maxEnergy,'Energy','energy'):''}${Object.keys(p.status).length?`<small>${Object.entries(p.status).map(([x,n])=>`${esc(x)} ${n}t`).join(' · ')}</small>`:''}</div>`;}
function battle(){
  const r=state.run,b=r.battle,p=r.monkey,e=b.enemy,m=C.MOVES[b.intent];
  return `<div class="two-col"><section class="panel"><div class="battle-title"><h2>${b.boss?'Guardian encounter':'A rival on the trail'}</h2><span class="tag">TURN ${b.turn}</span></div><div class="battlefield">${fighter(p,C.playerStats(state),false)}<div class="versus">VS</div>${fighter(e,C.stats(e),true)}</div>
    ${!b.ready?'<div class="notice">Tracking has stopped. Look up, find a safe place, and stand still before playing.</div><button class="primary wide" data-action="ready">I have stopped safely · begin battle</button>':`<div class="intent">Rival intends: <strong>${esc(m.name)}</strong> · ${m.cost} energy</div><div class="moves">${p.loadout.map(id=>{const move=C.MOVES[id],effect=move.power?` · ×${C.moveEffect(move,e.types)}`:'';return `<button class="move" data-action="move" data-id="${id}" ${p.energy<move.cost||combatBusy?'disabled':''}><span class="move-meta">${move.cost} EN${effect}</span><strong style="color:${C.ELEMENTS[move.type]?.color||'#e2e8cf'}">${esc(move.name)}</strong><small>${esc(move.desc)}</small></button>`;}).join('')}</div><div class="battle-items">${Object.entries(C.ITEMS).map(([id,item])=>`<button data-action="item" data-id="${id}" ${r.items[id]<1||combatBusy?'disabled':''}>${item.name} ×${r.items[id]}</button>`).join('')}</div><p class="fine">An item uses your turn. Speed and move priority decide attack order. You recover 1 energy after each full turn.</p>`}
  </section><div class="stack">${journal()}<section class="panel"><h2>Read the matchup</h2><p class="muted">Both defensive elements apply. Weaknesses multiply, resistances combine, and a weakness can cancel a resistance. Hybrid attacks choose their better elemental matchup.</p><div class="matchups">${Object.keys(C.ELEMENTS).map(t=>`<span style="color:${C.ELEMENTS[t].color}">${C.ELEMENTS[t].name} → rival ×${C.effectiveness(t,e.types)}</span>`).join('')}</div><p class="fine" style="margin-top:12px">Burn and poison wear down health. Slow reduces initiative. Brace acts first; damage still matters. Fire resists burn and Leaf resists poison.</p></section></div></div>`;
}
function monkeyView(){
  const r=state.run,p=r.monkey,st=C.playerStats(state),cost=8+p.level*4,price=cost-Math.min(cost-2,Math.floor(p.training/4));
  return `<div class="two-col"><div class="stack"><section class="panel"><div class="profile-art">${portrait(p)}<div><span class="eyebrow">YOUR COMPANION</span><h2>${esc(C.formName(p))}</h2><p class="muted">Level ${p.level} · Evolution ${p.stage}/3</p><div class="type-chips">${chips(p)}</div></div></div><p class="muted">${C.SPECIES[p.species].trait}</p>${bar(p.hp,st.hp,'Health')}${bar(p.energy,st.maxEnergy,'Energy','energy')}<div class="stat-grid">${[['atk','Attack'],['def','Defense'],['speed','Speed'],['hp','Max HP']].map(([k,l])=>`<span><b>${st[k]}</b>${l}</span>`).join('')}</div><button class="primary wide" data-action="train" ${r.phase!=='trail'||p.level>=60||r.fruit<price?'disabled':''}>Train to level ${p.level+1} · ${price} fruit</button><p class="fine">Battle training discounts fruit costs. Leveling restores the extra maximum health plus 8 HP. Available training: ${p.training}.</p>${resources()}</section><section class="panel"><h2>Your defensive matchups</h2><div class="matchups">${Object.keys(C.ELEMENTS).map(t=>`<span style="color:${C.ELEMENTS[t].color}">${C.ELEMENTS[t].name} ×${C.effectiveness(t,p.types)}</span>`).join('')}</div><p class="fine" style="margin-top:12px">×2 and ×4 are weaknesses. ×0.5 and ×0.25 are resistances. ×1 is neutral.</p></section></div>
    <div class="stack"><section class="panel"><h2>${p.stage===3?'Ascendant evolution complete':'Choose an evolution path'}</h2><p class="muted">Level 3: choose an element. Level 6: deepen it or mix a second element. Level 10: ascend your chosen path. Choices last for this run.</p><div class="evolution-grid">${Object.keys(C.ELEMENTS).filter(t=>p.stage<3&&(p.stage!==2||t===p.types[0])).map(t=>{const offer=C.evolutionCost(state,t);if(!offer)return '';const type=offer.types.length===2?C.hybrid(offer.types).name:C.ELEMENTS[t].name,afford=p.level>=offer.level&&Object.entries(offer.cost).every(([x,n])=>r.essence[x]>=n);return `<div class="evolution"><h3 style="color:${C.ELEMENTS[t].color}">${p.stage===2?'Ascendant ':''}${type}</h3><p>Level ${offer.level} · ${Object.entries(offer.cost).map(([x,n])=>`${n} ${C.ELEMENTS[x].name}`).join(' + ')} essence</p><button data-action="evolve" data-id="${t}" ${r.phase!=='trail'||!afford?'disabled':''}>${offer.types.length===2?'Mix elements':'Evolve'}</button></div>`;}).join('')}</div></section><section class="panel"><h2>Four equipped abilities</h2><div class="loadout">${Array.from({length:4},(_,i)=>`<label>Slot ${i+1}<select data-slot="${i}" ${r.phase!=='trail'||i===0?'disabled':''}>${!p.loadout[i]?'<option value="">Choose an ability</option>':''}${C.unlocked(p).map(id=>`<option value="${id}" ${p.loadout[i]===id?'selected':''}>${C.MOVES[id].name} (${C.MOVES[id].cost} EN)</option>`).join('')}</select></label>`).join('')}</div><p class="fine" style="margin-top:12px">Equip between encounters. Palm strike stays in slot 1 so you can always attack without energy. Evolutions suggest a new loadout.</p><div>${C.unlocked(p).map(id=>`<p class="muted"><strong>${C.MOVES[id].name}:</strong> ${C.MOVES[id].desc}</p>`).join('')}</div></section></div></div>`;
}
function pack(){
  const r=state.run;return `<div class="two-col"><section class="panel"><h2>Trail supplies</h2>${Object.entries(C.ITEMS).map(([id,item])=>`<div class="item-card"><div><h3>${item.name} ×${r.items[id]}</h3><p>${item.desc}${r.phase==='battle'?' Uses your battle turn.':''}</p></div><button data-action="item" data-id="${id}" ${!r.items[id]||r.phase==='dead'||r.phase==='battle'&&!r.battle.ready?'disabled':''}>Use</button></div>`).join('')}<div style="margin-top:22px">${resources()}</div><p class="fine">Fruit, essence, level, and unused consumables reset after defeat. Legacy grants extra fruit and berries to the next run.</p></section><section class="panel"><h2>Permanent equipment</h2><p class="muted">All found equipment survives defeat. Equip one weapon, one armor, and one charm. New copies of higher tier replace the weaker version; duplicates become fruit.</p>${Object.keys(C.GEAR).map(id=>{const g=C.GEAR[id],tier=state.meta.gear[id]||0,on=state.meta.equipped[g.slot]===id;return `<div class="gear-card"><span class="gear-icon">${g.icon}</span><div><h3>${g.name}${tier?` +${tier}`:''}</h3><p>${tier?`+${g.amount*tier} ${g.stat.toUpperCase()} · ${g.slot}`:'Not discovered · win encounters to find it'}</p></div><button data-action="equip" data-id="${id}" ${!tier||on||r.phase!=='trail'?'disabled':''}>${on?'Equipped':'Equip'}</button></div>`;}).join('')}</section></div>`;
}
function recordRows(records){return records.length?records.map((h,i)=>`<div class="record"><span class="tag">${i+1}</span><div><strong>${esc(h.name)}</strong><small>${esc(h.form)} · ${Number(h.feet).toLocaleString()} ft · ${Number(h.wins)} wins${h.mode==='practice'?' · PRACTICE':''}</small></div><b>${Number(h.score).toLocaleString()}</b></div>`).join(''):'<p class="muted">Finish a run to record its story here.</p>';}
function records(){return `<div class="two-col"><section class="panel"><h2>Your trail stories</h2><div class="stat-grid"><span><b>${state.meta.best.toLocaleString()}</b>Best score</span><span><b>${state.meta.runs}</b>Runs begun</span><span><b>${state.meta.legacy}</b>Legacy</span><span><b>${Object.keys(state.meta.gear).length}</b>Relics</span></div>${recordRows(state.meta.history)}<p class="fine" style="margin-top:15px">Score = feet walked + 500 per victory + 1,000 per evolution stage + 100 per level above 1.</p></section><section class="panel"><h2>Compass Wilds leaderboard</h2><p class="muted">Top 25 finished GPS runs. These are client-reported, casual scores, not verified fitness records. No location coordinates are published.</p><button class="wide" data-action="scores" ${state.mode==='practice'?'disabled':''}>Refresh Firestore leaderboard</button>${state.mode==='practice'?'<p class="fine">Practice runs never enter the real leaderboard.</p>':''}<div style="margin-top:12px">${scores?recordRows(scores):'<p class="fine">Refresh to read the current scores.</p>'}</div></section></div>`;}
function death(){const h=state.meta.history[0];return `<div class="two-col"><section class="panel run-end"><span class="eyebrow">THE TRAIL CONTINUES</span>${portrait(state.run.monkey)}<h1>A brave journey.</h1><p>${feet(state.run.distance)} feet · ${state.run.wins} victories · ${h?.score.toLocaleString()||0} points</p><div class="notice">Equipment kept. ${state.meta.legacy} legacy now grants ${Math.min(20,state.meta.legacy)*2} extra fruit and ${Math.floor(Math.min(20,state.meta.legacy)/5)} extra berries at the start.</div><label for="restart-species">Choose your next companion</label><select id="restart-species">${Object.entries(C.SPECIES).map(([id,p])=>`<option value="${id}" ${id===state.run.monkey.species?'selected':''}>${p.name} · ${p.title}</option>`).join('')}</select><button class="primary wide" style="margin-top:12px" data-action="restart">Begin the next run</button><p class="fine" style="margin-top:12px">The completed run stays in your history. Cloud scores retry automatically if you are offline.</p></section>${journal()}</div>`;}
function renderGPS(){
  if($('gps-status'))$('gps-status').textContent=state?.mode==='practice'?'Practice mode · use direction buttons':gpsInfo.reason;
  if($('gps-meta'))$('gps-meta').textContent=[gpsInfo.mph===null?'Speed unavailable':`${gpsInfo.mph.toFixed(1)} mph`,gpsInfo.accuracy===null?'':`GPS ±${Math.round(gpsInfo.accuracy)} m`].filter(Boolean).join(' · ');
  if($('tracking-button'))$('tracking-button').textContent=gps.active?'Pause walking':'Start walking';
}
function render(){
  if(!state)return;$('creature-label').textContent=`${state.name} · Run ${state.meta.runs}`;
  document.querySelectorAll('[data-action="tab"]').forEach(b=>b.setAttribute('aria-current',b.dataset.id===tab?'page':'false'));
  $('view').innerHTML=tab==='trail'?trail():tab==='monkey'?monkeyView():tab==='pack'?pack():records();renderGPS();
}
document.addEventListener('change',e=>{
  if(e.target.matches('[data-slot]')&&state)try{C.setMove(state,Number(e.target.dataset.slot),e.target.value);void checkpoint(true);render();}catch(error){toast(error.message);render();}
});
document.addEventListener('click',async e=>{
  const b=e.target.closest('[data-action]');if(!b||b.disabled||!state)return;
  const a=b.dataset.action,id=b.dataset.id;
  try{
    if(a==='tab'){if(id!=='trail')pause('Paused while using menus. Return to Trail to resume.');tab=id;render();return;}
    if(a==='tracking'){if(gps.active){pause();await checkpoint(true);}else if(state.mode==='gps'&&state.run.phase==='trail'&&tab==='trail')await gps.start();renderGPS();return;}
    if(a==='sync'){await checkpoint(true);void vault.sync();toast('Checkpoint queued. See the save status below.');return;}
    if(a==='export'){
      await checkpoint(false);const data=await vault.export(),url=URL.createObjectURL(new Blob([data],{type:'application/json'})),link=document.createElement('a');
      link.href=url;link.download=`monkey-trails-${state.name.replace(/[^A-Za-z0-9_-]/g,'-')}.json`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);return;
    }
    if(a==='cloud'){
      pause();if(!confirm('Load the cloud checkpoint? A separate encrypted recovery copy of this device’s progress will be kept. Export a backup first to keep a portable copy.'))return;
      state=await vault.loadCloud();if(state.run.phase==='battle')state.run.battle.ready=false;render();return;
    }
    if(a==='scores'){b.disabled=true;b.textContent='Reading scores…';try{scores=await vault.leaderboard();render();}catch(error){render();throw error;}return;}
    if(a==='practice-step'){
      if(state.mode!=='practice'||state.run.phase!=='trail')return;
      const vectors={N:[0,1],S:[0,-1],E:[1,0],W:[-1,0]},v=vectors[id];if(!v)return;
      C.advance(state,C.RESOURCE_STEP,v[0]*C.RESOURCE_STEP,v[1]*C.RESOURCE_STEP);
    }else if(a==='ready'){C.readyBattle(state);}
    else if(a==='move'||a==='item'){
      if(combatBusy)return;
      if(a==='move')C.turn(state,id);else C.useItem(state,id);
      combatBusy=true;setTimeout(()=>{combatBusy=false;render();},300);
      if(state.run.phase==='trail')pause('Battle complete · Start walking to set the new compass origin.');
    }else if(a==='train')C.train(state);
    else if(a==='evolve'){C.evolve(state,id);toast(`Evolved into ${C.formName(state.run.monkey)}!`);}
    else if(a==='equip')C.equip(state,id);
    else if(a==='restart'){C.restart(state,$('restart-species').value);tab='trail';pause();}
    else return;
    render();await checkpoint(true);
  }catch(error){toast(error.message);}
});
document.addEventListener('visibilitychange',()=>{
  if(document.hidden){pause('Tracking paused while hidden. Resume manually; the gap is not counted.');void checkpoint(true);}
  else renderGPS();
});
window.addEventListener('pagehide',()=>{gps.stop();void checkpoint(true);});
window.addEventListener('online',()=>{if(vault)void vault.sync();});
window.addEventListener('offline',()=>{if(state)saveStatus({text:'Offline · encrypted device saves continue; Firestore will retry.',kind:'offline'});});
setInterval(()=>{if(vault&&state?.mode==='gps'&&vault.dirty&&!document.hidden)void vault.sync();},20000);
