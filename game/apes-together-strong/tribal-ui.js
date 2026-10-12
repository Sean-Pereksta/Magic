/* The existing advanced army panel owns these controls on desktop and mobile. */
(() => {
'use strict';
function mount({getGame,army,renderer,onBuildChange=()=>{}}){
 const style=document.createElement('style');style.textContent=`
 #menu{align-items:flex-start}.menu-content{margin-block:auto}#menu .game-title{font-size:clamp(48px,5.2vw,76px)}
 .tribe-choices{border:0;padding:0;margin:12px 0;display:grid;grid-template-columns:repeat(3,1fr);gap:7px}.tribe-choices legend{font:11px system-ui;letter-spacing:.1em;margin-bottom:8px;color:#dec48a}.tribe-choice{position:relative;padding:8px 4px;border:1px solid #526057;border-radius:8px;cursor:pointer;text-align:center;background:#132624}.tribe-choice:has(input:checked){border-color:var(--tribe);background:#24372e;box-shadow:inset 0 0 0 1px var(--tribe)}.tribe-choice input{position:absolute;left:6px;top:6px;width:auto}.tribe-choice canvas{width:70px;height:65px;display:block;margin:auto}.tribe-choice strong{font:600 11px system-ui;color:var(--tribe)}.tribe-choice small{font:10px/1.4 system-ui;display:block;color:#c1c8b9;margin-top:4px}.tribe-details{font:11px/1.5 system-ui;color:#c4cbbf;margin-bottom:10px;max-width:540px}.tribe-details b{color:#e8d6a4}.tribal-orders{display:flex;gap:4px;flex-wrap:wrap;align-items:center;max-width:620px;padding:5px;background:#102c27;border:1px solid #557260;border-radius:6px}.tribal-orders[hidden]{display:none}#armyDock .tribal-orders button{width:auto;min-width:44px;height:32px;padding:4px 8px;font:11px system-ui}#armyDock .tribal-orders select{width:auto;min-height:32px;padding:4px;font:11px system-ui}.tribal-status{flex-basis:100%;font:11px system-ui;color:#d9cf9b;text-align:center}.tribal-build{border-top:1px solid #51664b;margin-top:12px;padding-top:12px}.tribal-build button{margin:4px;padding:8px;font:12px system-ui}.tribal-build p{font:12px/1.5 system-ui;color:#c9d0bf}@media(pointer:coarse){#armyDock .tribal-orders button{min-height:44px}.tribal-orders{max-width:340px;max-height:170px;overflow:auto}}`;
 document.head.append(style);
 const field=document.createElement('fieldset');field.className='tribe-choices';field.id='tribeChoices';const legend=document.createElement('legend');legend.textContent='CHOOSE YOUR TRIBE';field.append(legend);
 for(const [id,t]of Object.entries(ATSTribes)){
  const label=document.createElement('label');label.className='tribe-choice';label.style.setProperty('--tribe',t.color);
  const input=document.createElement('input');input.type='radio';input.name='startingTribe';input.value=id;input.checked=id==='ironback';input.setAttribute('aria-label',t.name);
  const canvas=document.createElement('canvas');canvas.width=140;canvas.height=130;canvas.setAttribute('aria-hidden','true');
  const name=document.createElement('strong');name.textContent=t.name;const subtitle=document.createElement('small');subtitle.textContent=t.style;label.append(input,canvas,name,subtitle);field.append(label);
  const paint=()=>{const c=canvas.getContext('2d');c.clearRect(0,0,140,130);c.save();c.translate(70,119);c.scale(1.3,1.3);renderer.drawApe(c,{id:'king',tribeId:id,species:'gorilla',hp:160,maxHp:160,dir:.65,phase:0,crownTier:1,equipment:{armorLevel:1}},true);c.restore()};
  paint();window.ATSVisualAssets?.onProgress(a=>{if(a.status!=='loading')paint()});
 }
 const menu=document.getElementById('newRun').parentElement;menu.before(field);
 const details=document.createElement('div');details.className='tribe-details';field.after(details);
 function selected(){return field.querySelector('input:checked')?.value||'ironback'}
 function describe(){const t=ATSTribes[selected()];details.replaceChildren();const b=document.createElement('b');b.textContent=t.bonuses;details.append(b,document.createElement('br'),t.drawback,document.createElement('br'),'Opening rescues: '+t.opening.join(', ')+'.',document.createElement('br'),'Exclusive: '+({ironback:'Shieldwall · Battering Assault · Gatehouse',shadowhand:'Ghost Entry · Master Distraction · Silent Relay Cut',emberwood:'Holdfast Defense · Masterwork Towers · Engineer Reserve'})[selected()]);}
 field.addEventListener('change',describe);describe();
 const row=document.createElement('div');row.className='tribal-orders';row.hidden=true;army.root.append(row);const buttons={};
 const button=(id,label,fn)=>{const b=document.createElement('button');b.type='button';b.textContent=label;b.dataset.tribal=id;b.onclick=()=>{if(army.getScreen()!=='play')return;fn();army.update(true);refresh()};row.append(b);buttons[id]=b;return b};
 const response=document.createElement('select');response.setAttribute('aria-label','When infiltrators are discovered');for(const [value,text]of [['continue','Discovered: continue'],['fight','Discovered: fight'],['retreat','Discovered: retreat']]){const o=document.createElement('option');o.value=value;o.textContent=text;response.append(o)}
 button('infiltrate','Infiltrate',()=>{const g=getGame(),on=!g.siege.selectedMembers().some(a=>a.infiltration);g.setInfiltration(on,response.value)});
 button('distract','Distract',()=>{army.mode='distract';getGame().siege.status='Tap reachable ground for noise · 14s cooldown'});
 button('ghost','Ghost Entry',()=>getGame().ghostEntry(response.value));
 button('relay','Silent Relay Cut',()=>{if(getGame().silentRelay())army.mode='sabotage'});
 button('shieldwall','Shieldwall',()=>{getGame().ironbackShieldwall();army.mode='shield'});
 button('battering','Battering Assault',()=>getGame().batteringAssault());
 row.append(response);response.onchange=()=>{const g=getGame();g.setInfiltration(true,response.value,g.siege.selectedMembers().some(a=>a.infiltration?.ghost))};
 const status=document.createElement('div');status.className='tribal-status';status.setAttribute('role','status');row.append(status);
 function refresh(){const g=getGame();if(!g)return;row.hidden=!g.siege.selected.length;const id=g.tribe.id,units=g.siege.selectedMembers(),active=units.some(a=>a.infiltration);
  buttons.infiltrate.setAttribute('aria-pressed',String(active));buttons.ghost.hidden=buttons.relay.hidden=id!=='shadowhand';buttons.shieldwall.hidden=buttons.battering.hidden=id!=='ironback';buttons.battering.disabled=g.time<(g.tribe.batteringReady||0);response.hidden=!active;
  const states=[...new Set(units.map(a=>a.routeStatus).filter(Boolean))];status.textContent=g.siege.status||states.slice(0,3).join(' · ');
 }
 const update=army.update.bind(army);army.update=function(...args){const r=update(...args);refresh();return r};
 const build=document.createElement('div');build.id='tribalBuild';build.className='tribal-build';document.getElementById('buildTraining').after(build);
 function updateBuild(s){const g=getGame();build.replaceChildren();const title=document.createElement('strong');title.textContent=ATSTribes[g.tribe.id]?.name||'Balanced kingdom';build.append(title);
  const p=document.createElement('p');p.textContent=s.garrison?`${s.garrison.phase} · ${s.garrison.tower} tower crew · ${s.garrison.gate} gate guards · ${s.garrison.wall} wall guards · ${s.garrison.reserve} reserves · ${s.garrison.emergency} lodge defenders`:'Defenders will organize as residents take their posts.';build.append(p);
  const available=(label,fn,disabled=false)=>{const b=document.createElement('button');b.className='button';b.textContent=label;b.disabled=disabled;b.onclick=()=>{const ok=fn();if(!ok)p.textContent='Visit the lodge with enough timber and food, outside an attack.';else{onBuildChange();updateBuild(s)}};build.append(b)};
  if(g.tribe.id==='emberwood')available(s.holdfast?'Holdfast Defense: on':'Holdfast Defense: off',()=>{s.holdfast=!s.holdfast;s._garrisonAt=0;return true});
  for(const [id,spec]of Object.entries(ATSTribalUpgrades)){if(spec.tribe!==g.tribe.id)continue;
   const targets=id==='gatehouse'?(s.barriers||[]).filter(o=>o.gate&&o.hp>0&&!o.ironbackGatehouse):id==='masterwork'?(s.facilities||[]).filter(o=>['spearTower','spearBattery','spearBallista'].includes(o.kind)&&o.hp>0&&o.stage===4&&!o.masterwork):[null];
   if(id==='engineers'&&s.engineerReserve||id==='decoys'&&g.tribe.unlocks.includes(id))continue;
   const target=targets[0],label=spec.label+' · '+spec.wood+' wood / '+spec.food+' food';let selector;
   if(target){selector=document.createElement('select');selector.setAttribute('aria-label','Choose '+spec.label+' target');targets.forEach((o,i)=>{const option=document.createElement('option');option.value=o.id;option.textContent=(o.kind||'Gate')+' '+(i+1)+' · '+Math.round(o.hp)+' / '+Math.round(o.maxHp)+' durability';selector.append(option)});build.append(selector)}
   available(label,()=>g.tribalUpgrade(id,s.id,selector?.value||target?.id),!targets.length||s.attack||s.wood<spec.wood||s.food<spec.food);
  }
 }
 return{selected,updateBuild};
}
window.ATSTribalUI={mount};
})();
