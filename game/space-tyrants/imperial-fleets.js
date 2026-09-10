/* Fleet designs extend named fleet records. Crossing encounters retain transit
   ships and resume their original missions from the physical engagement point. */
const STX_IFD_HULLS={escorts:{power:.7,speed:1.35,hull:'corvette'},line:{power:1.2,speed:1,hull:'cruiser'},heavy:{power:1.8,speed:.72,hull:'battleship'},carriers:{power:1.35,speed:.85,hull:'carrier'},transports:{power:.4,speed:.9,hull:'frigate'},support:{power:.55,speed:1,hull:'destroyer'}};
const STX_IFD_TEMPLATES={Patrol:{escorts:6},'Fast Response':{escorts:10,line:4},Battle:{heavy:5,line:12,escorts:14},Invasion:{line:12,escorts:8,transports:6,support:4},Siege:{heavy:10,line:6,support:4},'Carrier Group':{carriers:3,line:6,escorts:12,support:3},Escort:{escorts:12,line:2,support:2},Raiding:{escorts:8,line:4}};
const STX_IFD_ROLES=['Patrol','Intercept','Defend','Assault','Raid','Escort','Reserve'];
const STX_IFD_DOCTRINES=['Aggressive','Balanced','Cautious','Evasive','Hold Position'];
function stxIFDComposition(input){const out={};for(const k of Object.keys(STX_IFD_HULLS)){const n=Number(input?.[k]||0);if(!Number.isSafeInteger(n)||n<0||n>500)return null;out[k]=n}const count=Object.values(out).reduce((a,b)=>a+b,0);return count>0&&count<=500?out:null}
function stxIFDStats(comp){let count=0,power=0,speed=0;for(const [k,n] of Object.entries(comp||{})){const s=STX_IFD_HULLS[k];if(s){count+=n;power+=n*s.power;speed+=n*s.speed}}return {count,power,speed:count?speed/count:1,detection:36+(comp?.escorts||0)*1.5+(comp?.carriers||0)*3,siege:1+(comp?.heavy||0)/Math.max(1,count)*.35,logistics:count+(comp?.heavy||0)*.5+(comp?.carriers||0)*.5}}
function stxIFDEnsure(f){
  if(!f||f.destroyed)return f;
  if(!f.imperialComposition){const count=Math.max(1,Math.round(f.vesselCount||f.strength/1.2));f.imperialComposition={line:count};f.imperialTarget={line:count};f.imperialHullPower=f.strength/count}
  f.imperialDoctrine=f.imperialDoctrine||'Balanced';f.imperialRole=f.imperialRole||'Reserve';f.imperialHistory=f.imperialHistory||[];return f;
}
function stxIFDDesign(p,comp,name,role='Reserve',doctrine='Balanced'){
  comp=stxIFDComposition(comp);if(!comp||p?.owner!==0||!STX_IFD_ROLES.includes(role)||!STX_IFD_DOCTRINES.includes(doctrine))return false;
  const stats=stxIFDStats(comp),f=stxLPMobilize(p,stats.count,'fleet',name);if(!f)return false;
  f.imperialComposition={...comp};f.imperialTarget={...comp};f.imperialDoctrine=doctrine;f.imperialRole=role;f.strength=stats.power*(1+empire(0).tech.weapons*.15);f.maxServiceStrength=f.strength;f.imperialHullPower=f.strength/stats.count;stxIDHistory(f,`Commissioned at ${p.name}: ${stats.count} vessels`);return f;
}
function stxIFDSaveTemplate(name,comp){comp=stxIFDComposition(comp);name=String(name).trim().slice(0,48);if(!comp||!name||['__proto__','constructor','prototype'].includes(name))return false;const e=empire(0);e.imperialTemplates=e.imperialTemplates||{};e.imperialTemplates[name]=comp;return true}
function stxIFDReconcile(f){
  stxIFDEnsure(f);if(!f?.imperialComposition)return;
  const current=stxIFDStats(f.imperialComposition).count,remaining=f.destroyed?0:Math.min(current,Math.max(f.strength>0?1:0,Math.ceil(f.strength/Math.max(.01,f.imperialHullPower||1.2))));
  let lost=current-remaining;if(lost<=0)return;
  for(const k of Object.keys(STX_IFD_HULLS)){const n=Math.min(lost,f.imperialComposition[k]||0);f.imperialComposition[k]=(f.imperialComposition[k]||0)-n;lost-=n}
  f.vesselCount=remaining;f.personnel=Math.min(f.personnel||0,remaining*STX_LP_CREW);f.imperialShipsLost=(f.imperialShipsLost||0)+current-remaining;
}
function stxIFDReinforce(p,f){
  if(!p||p.owner!==0||!f||f.owner!==0||f.destroyed||f.imperialReinforcement||p.underAttack||p.infra.shipyard<1)return false;
  stxIFDReconcile(f);const missing={};for(const [k,n] of Object.entries(f.imperialTarget))missing[k]=Math.max(0,n-(f.imperialComposition[k]||0));const count=stxIFDStats(missing).count;
  const need=stxECRScaleCost(STX_ECR_SHIP_COST,count);if(!count||p.stock.trained<count*STX_LP_CREW||Object.entries(need).some(([r,n])=>p.stock[r]<n))return false;
  for(const [r,n] of Object.entries(need))p.stock[r]-=n;p.stock.trained-=count*STX_LP_CREW;
  f.imperialReinforcement={source:p.id,composition:missing,crew:count*STX_LP_CREW,progress:0};stxIDHistory(f,`${count} replacements ordered from ${p.name}`);return true;
}
function stxIFDTickReinforcements(dt){for(const f of state.fleets){const q=f.imperialReinforcement;if(!q)continue;const p=state.planets.find(p=>p.id===q.source);if(!p||p.owner!==f.owner){delete f.imperialReinforcement;continue}if(f.destroyed){p.reserveVessels+=stxIFDStats(q.composition).count;p.stock.trained+=q.crew;delete f.imperialReinforcement;continue}if(p.underAttack||f.imperialEncounter)continue;
  q.progress=Math.min(1,q.progress+dt*.015*(1+p.infra.shipyard*.2)*stxLPWorkforce(p));if(q.progress<1)continue;
  const pos=stxFleetPosition(f);if(!pos)continue;q.x=q.x??p.x;q.y=q.y??p.y;const d=Math.hypot(pos.x-q.x,pos.y-q.y),step=100*dt;if(d>step){q.x+=(pos.x-q.x)*step/d;q.y+=(pos.y-q.y)*step/d;continue}
  for(const [k,n] of Object.entries(q.composition))f.imperialComposition[k]=(f.imperialComposition[k]||0)+n;const n=stxIFDStats(q.composition).count;f.strength+=n*(f.imperialHullPower||1.2);f.vesselCount=stxIFDStats(f.imperialComposition).count;f.personnel=(f.personnel||0)+q.crew;const ship=state.ships.find(s=>s.fleetId===f.id);if(ship)ship.strength=f.strength;stxIDHistory(f,`${n} replacement vessels arrived`);delete f.imperialReinforcement;
}}
function stxIFDEncounters(){const e=empire(0);if(!e)return [];e.imperialEncounters=e.imperialEncounters||[];return e.imperialEncounters}
function stxIFDEngages(a,b){const doctrine=a.imperialDoctrine||'Balanced';return doctrine==='Aggressive'||doctrine==='Balanced'&&a.strength>=b.strength*.7||doctrine==='Cautious'&&a.strength>=b.strength*1.2}
// Relative-motion quadratic: both fleets must be close at the SAME instant.
function stxIFDContact(a,b,va,vb,dt,radius){const x=a.x-b.x,y=a.y-b.y,vx=va.x-vb.x,vy=va.y-vb.y,A=vx*vx+vy*vy,B=2*(x*vx+y*vy),C=x*x+y*y-radius*radius;if(C<=0)return 0;if(A<1e-12)return null;const disc=B*B-4*A*C;if(disc<0)return null;const t=(-B-Math.sqrt(disc))/(2*A);return t>=0&&t<=dt?t:null}
function stxIFDTarget(s){return Number.isFinite(s.targetX)?{x:s.targetX,y:s.targetY}:state.planets.find(p=>p.id===s.to)}
function stxIFDVelocity(s){if(s.stxStationed)return {x:0,y:0,eta:Infinity};const t=stxIFDTarget(s);if(!t)return {x:0,y:0,eta:0};const d=Math.hypot(t.x-s.x,t.y-s.y),f=fleetRecord(s.fleetId),speed=s.speed*(f?.imperialComposition?stxIFDStats(f.imperialComposition).speed:1);return {x:d?(t.x-s.x)/d*speed:0,y:d?(t.y-s.y)/d*speed:0,eta:d/Math.max(.01,speed)}}
function stxIFDStart(a,b,x,y){
  const fa=fleetRecord(a.fleetId),fb=fleetRecord(b.fleetId);if(!fa||!fb||fa.imperialEncounter||fb.imperialEncounter)return null;
  const encounter={id:stxDSId('crossing'),x,y,attacker:fa.owner,defender:fb.owner,attackerFleetIds:[fa.id],defenderFleetIds:[fb.id],elapsed:0,maxDuration:24,attackerInitial:fa.strength,defenderInitial:fb.strength};
  for(const [f,s] of [[fa,a],[fb,b]]){f.imperialEncounter=encounter.id;f.location=null;f.status='Deep-space engagement';f.battles=(f.battles||0)+1;s.x=x;s.y=y;stxIDHistory(f,'Entered deep-space engagement')}
  stxIFDEncounters().push(encounter);return encounter;
}
function stxIFDResolve(b){
  const all=state.fleets.filter(f=>f.imperialEncounter===b.id),strength=owner=>all.filter(f=>f.owner===owner&&!f.destroyed).reduce((n,f)=>n+f.strength,0),winner=strength(b.attacker)>=strength(b.defender)?b.attacker:b.defender;
  for(const f of all){delete f.imperialEncounter;f.imperialEncounterCooldown=state.simTime+12;stxIFDReconcile(f);if(f.strength<=.1){f.destroyed=true;state.ships=state.ships.filter(s=>s.fleetId!==f.id);stxIDHistory(f,'Destroyed in deep space');continue}
    if(f.owner===winner){f.victories=(f.victories||0)+1;f.veterans=clamp((f.veterans||0)+.04,0,1);stxIDHistory(f,'Won deep-space engagement');f.status='Resuming mission'}
    else{stxIDHistory(f,'Retreated from deep-space engagement');state.ships=state.ships.filter(s=>s.fleetId!==f.id);stxDSReturnFleet(f,b,f.owner)}
    const s=state.ships.find(s=>s.fleetId===f.id);if(s){s.strength=f.strength;s.startX=s.x=b.x;s.startY=s.y=b.y;s.progress=0;const t=stxIFDTarget(s);s.distance=t?Math.hypot(t.x-b.x,t.y-b.y):1}
  }
  const list=stxIFDEncounters(),i=list.indexOf(b);if(i>=0)list.splice(i,1);
}
function stxIFDBattles(elapsed){for(const b of [...stxIFDEncounters()]){const dt=Math.max(0,elapsed-(b.firstTickDelay||0));delete b.firstTickDelay;
  const a=b.attackerFleetIds.map(fleetRecord).filter(f=>f&&!f.destroyed),d=b.defenderFleetIds.map(fleetRecord).filter(f=>f&&!f.destroyed),ap=a.reduce((n,f)=>n+f.strength,0),dp=d.reduce((n,f)=>n+f.strength,0);
  if(!empiresAtWar(b.attacker,b.defender)){stxIFDResolve(b);continue}
  const damage=(fleets,power,total)=>{for(const f of fleets){f.strength=Math.max(0,f.strength-(.18+power*.008)*dt*f.strength/Math.max(.01,total)/(1+(f.imperialComposition?.support||0)/Math.max(1,stxIFDStats(f.imperialComposition).count)*.6));stxIFDReconcile(f);const s=state.ships.find(s=>s.fleetId===f.id);if(s)s.strength=f.strength}};
  damage(a,dp,ap);damage(d,ap,dp);b.elapsed+=dt;b.attackerStrength=a.reduce((n,f)=>n+f.strength,0);b.defenderStrength=d.reduce((n,f)=>n+f.strength,0);
  if(b.attackerStrength<.2||b.defenderStrength<.2||b.elapsed>=b.maxDuration)stxIFDResolve(b);
}}
function stxIFDIntercept(f,target){
  if(!f||f.owner!==0||f.destroyed||f.imperialEncounter||!target||target.destroyed||!empiresAtWar(f.owner,target.owner)||!stxFleetVisible(target))return null;
  const a=stxFleetPosition(f),b=stxFleetPosition(target),ship=state.ships.find(s=>s.fleetId===target.id);if(!a||!b||!ship)return null;
  stxIFDEnsure(f);const velocity=stxIFDVelocity(ship),speed=86*(1+empire(f.owner).tech.propulsion*.08)*stxIFDStats(f.imperialComposition).speed;
  const x=b.x-a.x,y=b.y-a.y,A=velocity.x**2+velocity.y**2-speed**2,B=2*(x*velocity.x+y*velocity.y),C=x*x+y*y,disc=B*B-4*A*C;
  let t;if(Math.abs(A)<1e-8)t=B<0?-C/B:Infinity;else if(disc>=0)t=[(-B-Math.sqrt(disc))/(2*A),(-B+Math.sqrt(disc))/(2*A)].filter(t=>t>=0).sort((a,b)=>a-b)[0];
  if(!Number.isFinite(t)||t>velocity.eta)return null;return {x:b.x+velocity.x*t,y:b.y+velocity.y*t,time:t};
}
function stxIFDOrderIntercept(f,target){const point=stxIFDIntercept(f,target);if(!point)return false;const ok=stxDSDispatchFleet(f,{...point,name:'Projected interception'},'concentrate',{stxDeepMission:'intercept',imperialIntercept:target.id});if(ok){f.imperialRole='Intercept';f.status=`Intercepting ${target.name}`}return ok}
function stxIFDJoin(f,b){if(!f||f.owner!==0||f.destroyed||f.imperialEncounter||![b.attacker,b.defender].includes(f.owner))return false;return stxDSDispatchFleet(f,{x:b.x,y:b.y,name:'Deep-space engagement'},'concentrate',{stxDeepMission:'join',imperialJoin:b.id})}
let stxIFDHeldMilitary=0;
const STX_IFD_tickShips=tickShips;
tickShips=function(dt){
  const military=state.ships.filter(s=>s.fleetId&&!s.stxCancelled&&!s.stxIntercepted&&['fleet','patrol'].includes(s.type));
  const militarySet=new Set(military);state.ships=state.ships.filter(s=>!militarySet.has(s));
  stxIFDHeldMilitary=military.length;try{STX_IFD_tickShips(dt)}finally{state.ships.push(...military);stxIFDHeldMilitary=0}
  for(const s of military){stxIFDEnsure(fleetRecord(s.fleetId));if(s.progress>=1&&!fleetRecord(s.fleetId)?.imperialEncounter){const t=stxIFDTarget(s);if(t){s.x=t.x;s.y=t.y}}}
  const movable=military.filter(s=>{const f=fleetRecord(s.fleetId);return f&&!f.destroyed&&!f.imperialEncounter&&(!s.stxStationed||s.imperialIntercept)});
  const velocities=new Map(movable.map(s=>[s,stxIFDVelocity(s)])),events=[];
  const swept=movable.map(s=>{const v=velocities.get(s),time=Math.min(dt,v.eta),x=s.x+v.x*time,y=s.y+v.y*time;return {s,left:Math.min(s.x,x)-70,right:Math.max(s.x,x)+70,top:Math.min(s.y,y)-70,bottom:Math.max(s.y,y)+70}}).sort((a,b)=>a.left-b.left);
  for(let i=0;i<swept.length;i++)for(let j=i+1;j<swept.length;j++){
    if(swept[j].left>swept[i].right)break;if(swept[j].top>swept[i].bottom||swept[j].bottom<swept[i].top)continue;
    const a=swept[i].s,b=swept[j].s,fa=fleetRecord(a.fleetId),fb=fleetRecord(b.fleetId);if(!empiresAtWar(a.owner,b.owner)||(fa.imperialEncounterCooldown||0)>state.simTime||(fb.imperialEncounterCooldown||0)>state.simTime||(!stxIFDEngages(fa,fb)&&!stxIFDEngages(fb,fa)&&a.imperialIntercept!==fb.id&&b.imperialIntercept!==fa.id))continue;
    const va=velocities.get(a),vb=velocities.get(b),radius=Math.min(70,Math.max(stxIFDStats(fa.imperialComposition).detection,stxIFDStats(fb.imperialComposition).detection));
    const time=stxIFDContact(a,b,va,vb,Math.min(dt,va.eta,vb.eta),radius);if(time!=null)events.push({a,b,time});
  }
  for(const {a,b,time} of events.sort((a,b)=>a.time-b.time)){const fa=fleetRecord(a.fleetId),fb=fleetRecord(b.fleetId);if(fa.imperialEncounter||fb.imperialEncounter)continue;const va=velocities.get(a),vb=velocities.get(b);const battle=stxIFDStart(a,b,(a.x+va.x*time+b.x+vb.x*time)/2,(a.y+va.y*time+b.y+vb.y*time)/2);if(battle)battle.firstTickDelay=time}
  for(const s of movable){const f=fleetRecord(s.fleetId);if(f.imperialEncounter)continue;const v=velocities.get(s),step=Math.min(dt,v.eta);s.x+=v.x*step;s.y+=v.y*step;const t=stxIFDTarget(s);if(!t)continue;s.progress=clamp(1-Math.hypot(t.x-s.x,t.y-s.y)/Math.max(1,s.distance),0,1);
    if(v.eta>dt&&s.progress<1)continue;
    if(s.imperialJoin){const b=stxIFDEncounters().find(b=>b.id===s.imperialJoin);if(b&&[b.attacker,b.defender].includes(f.owner)){const ids=f.owner===b.attacker?b.attackerFleetIds:b.defenderFleetIds;if(!ids.includes(f.id))ids.push(f.id);f.imperialEncounter=b.id;f.battles++;f.status='Reinforcing deep-space engagement';delete s.imperialJoin;continue}}
    if(s.imperialIntercept||s.stxDeepMission==='join'){s.stxStationed=true;f.status='Holding projected contact position';continue}
    const done=stxActionArrive(s,()=>s.stxDeepTransit?stxDSHandleTransitArrival(s):arriveShip(s,t));if(done)state.ships=state.ships.filter(x=>x!==s);
  }
  stxIFDBattles(dt);stxIFDTickReinforcements(dt);for(const f of state.fleets)stxIFDReconcile(f);stxIFDRoleTick();
};
// Encounter participants stay unavailable to background fleet allocation.
const STX_IFD_dispatch=stxFCADispatchFleet;
stxFCADispatchFleet=function(f,...args){if(f?.imperialEncounter)return false;return STX_IFD_dispatch(f,...args)};
const STX_IFD_dsDispatch=stxDSDispatchFleet;
stxDSDispatchFleet=function(f,...args){if(f?.imperialEncounter)return false;return STX_IFD_dsDispatch(f,...args)};
if(typeof stxGVIndex==='function'){
const STX_IFD_gvIndex=stxGVIndex;
stxGVIndex=function(){const index=STX_IFD_gvIndex();for(const b of empire(0)?.imperialEncounters||[]){for(const id of [...b.attackerFleetIds,...b.defenderFleetIds])index.busy.add(id);index.battles.push({b,location:{x:b.x,y:b.y,name:'Deep-space engagement'},invasion:false})}return index};
}
const STX_IFD_generate=generateGalaxy;
generateGalaxy=function(){STX_IFD_generate();state.fleets.forEach(stxIFDEnsure)};
const STX_IFD_load=loadGame;
loadGame=function(){const ok=STX_IFD_load();if(ok)state.fleets.forEach(stxIFDEnsure);return ok};
function stxIFDDraft(p){const d=stxLPDraft(p);if(!d.composition)d.composition={...STX_IFD_TEMPLATES.Battle};d.doctrine=d.doctrine||'Balanced';d.role=d.role||'Reserve';return d}
function stxIFDFleetHTML(f,p){stxIFDEnsure(f);const n=stxIFDStats(f.imperialComposition).count,target=stxIFDStats(f.imperialTarget).count,missing=Math.max(0,target-n),esc=stxRTEscape;
  return `<details><summary>${esc(f.name)} · ${n} / ${target} vessels</summary><small>Admiral ${esc(f.admiral?.name||'Unassigned')} · ${Math.round((f.veterans||0)*100)}% veterans · ${f.victories||0}/${f.battles||0} victories · ${f.imperialShipsLost||0} vessels lost</small><p>${esc(f.status)}</p><label>Role<select data-ifd-role="${f.id}">${STX_IFD_ROLES.map(k=>`<option ${k===f.imperialRole?'selected':''}>${k}</option>`).join('')}</select></label><label>Doctrine<select data-ifd-doctrine="${f.id}">${STX_IFD_DOCTRINES.map(k=>`<option ${k===f.imperialDoctrine?'selected':''}>${k}</option>`).join('')}</select></label>${missing&&!f.imperialReinforcement?`<button class="choice-btn" data-ifd-reinforce="${f.id}">Build ${missing} replacements at ${esc(p.name)} · ${esc(stxECRCostText(stxECRScaleCost(STX_ECR_SHIP_COST,missing)))} · ${missing*200} personnel</button>`:''}${f.imperialReinforcement?`<p>Replacements: ${Math.floor(f.imperialReinforcement.progress*100)}% built${f.imperialReinforcement.x!=null?' · in transit':''}</p>`:''}${state.fleets.filter(t=>t.owner!==0&&!t.destroyed&&empiresAtWar(0,t.owner)&&stxFleetVisible(t)).map(t=>{const point=stxIFDIntercept(f,t);return point?`<button class="choice-btn" data-ifd-intercept="${f.id}" data-target="${t.id}">Intercept ${esc(t.name)} · projected contact ${(point.time/6).toFixed(1)} cycles · FREE</button>`:''}).join('')}${stxIFDEncounters().filter(b=>[b.attacker,b.defender].includes(f.owner)&&!f.imperialEncounter).map(b=>{const pos=stxFleetPosition(f),eta=pos?Math.hypot(pos.x-b.x,pos.y-b.y)/(86*stxIFDStats(f.imperialComposition).speed):Infinity;return eta<b.maxDuration-b.elapsed?`<button class="choice-btn" data-ifd-join="${f.id}" data-battle="${b.id}">Join engagement · ${(eta/6).toFixed(1)} cycles · FREE</button>`:''}).join('')}<small>${f.imperialHistory.slice(0,10).map(h=>`Cycle ${Math.floor(h.time/6)+1}: ${esc(h.text)}`).join('<br>')}</small></details>`;
}
const STX_IFD_panel=stxLPPanel;
stxLPPanel=function(p){let html=STX_IFD_panel(p);if(p.owner!==0)return html;const d=stxIFDDraft(p),stats=stxIFDStats(d.composition),esc=stxRTEscape,templates={...STX_IFD_TEMPLATES,...empire(0).imperialTemplates};
  html+=`<section class="stx-lp-panel"><details ${d.designerOpen?'open':''} id="stxIFDDesigner"><summary>Fleet designer · ${esc(p.name)}</summary><label>Template<select id="stxIFDTemplate"><option value="">Custom</option>${Object.keys(templates).map(k=>`<option value="${esc(k)}">${esc(k)}</option>`).join('')}</select></label>${Object.keys(STX_IFD_HULLS).map(k=>`<label>${k}<input data-ifd-hull="${k}" type="number" min="0" max="500" value="${d.composition[k]||0}"></label>`).join('')}<label>Name<input id="stxIFDName" maxlength="48" value="${esc(d.fleetName||'')}"></label><label>Role<select id="stxIFDRole">${STX_IFD_ROLES.map(k=>`<option ${d.role===k?'selected':''}>${k}</option>`).join('')}</select></label><label>Doctrine<select id="stxIFDDoctrine">${STX_IFD_DOCTRINES.map(k=>`<option ${d.doctrine===k?'selected':''}>${k}</option>`).join('')}</select></label><p id="stxIFDQuote">${stats.count} vessels · ${stats.count*200} personnel · ${stats.count*2} credits</p><small>Escorts: faster detection and travel. Heavy warships: stronger, slower, improved siege. Transports improve planetary assault. Support vessels reduce deep-space losses. Carriers extend detection. Commissioning uses staged hulls and personnel. No material fee.</small><button class="choice-btn" id="stxIFDCommission">Review commission</button><button class="choice-btn" id="stxIFDSave">Save named template</button></details><div class="section-label">FLEET COMMAND & HISTORY</div>${state.fleets.filter(f=>f.owner===0&&!f.destroyed).map(f=>stxIFDFleetHTML(f,p)).join('')}</section>`;return html;
};
const STX_IFD_render=renderPlanet;
renderPlanet=function(){STX_IFD_render();const p=state.selected,body=$('planetBody');if(!p||p.owner!==0||!body)return;const d=stxIFDDraft(p),finish=()=>{saveGame(false);renderPlanet();updateHud(true)};
  const quote=()=>{const comp=stxIFDComposition(d.composition),s=stxIFDStats(comp);if($('stxIFDQuote'))$('stxIFDQuote').textContent=comp?`${s.count} vessels · ${s.count*200} personnel · ${s.count*2} credits · power ${s.power.toFixed(1)} before technology · speed ×${s.speed.toFixed(2)} · logistics ${s.logistics.toFixed(1)}`:'Choose 1–500 whole vessels';if($('stxIFDCommission'))$('stxIFDCommission').disabled=!comp||!stxLPFleetPlan(p,s.count).ok};
  if($('stxIFDDesigner'))$('stxIFDDesigner').ontoggle=e=>d.designerOpen=e.target.open;
  if($('stxIFDTemplate'))$('stxIFDTemplate').onchange=e=>{const t={...STX_IFD_TEMPLATES,...empire(0).imperialTemplates}[e.target.value];if(t){d.composition={...t};body.querySelectorAll('[data-ifd-hull]').forEach(b=>b.value=t[b.dataset.ifdHull]||0);quote()}};
  body.querySelectorAll('[data-ifd-hull]').forEach(b=>b.oninput=()=>{d.composition[b.dataset.ifdHull]=Number(b.value);quote()});
  if($('stxIFDName'))$('stxIFDName').oninput=e=>d.fleetName=e.target.value;
  for(const [id,key] of [['stxIFDRole','role'],['stxIFDDoctrine','doctrine']])if($(id))$(id).onchange=e=>d[key]=e.target.value;
  if($('stxIFDCommission'))$('stxIFDCommission').onclick=()=>{const s=stxIFDStats(d.composition);if(window.confirm(`Commission ${d.fleetName||'fleet'} at ${p.name}? ${s.count} reserve vessels, ${s.count*200} personnel, ${s.count*2} credits. Role: ${d.role}. Doctrine: ${d.doctrine}. Completion: immediate.`)){showToast(stxIFDDesign(p,d.composition,d.fleetName,d.role,d.doctrine)?'Fleet commissioned':'Commission unavailable');finish()}};
  if($('stxIFDSave'))$('stxIFDSave').onclick=()=>{showToast(stxIFDSaveTemplate(d.fleetName,d.composition)?'Template saved':'Enter a name and valid composition');finish()};
  for(const kind of ['role','doctrine'])body.querySelectorAll(`[data-ifd-${kind}]`).forEach(b=>b.onchange=()=>{const f=fleetRecord(b.dataset[kind==='role'?'ifdRole':'ifdDoctrine']);if(f)f[kind==='role'?'imperialRole':'imperialDoctrine']=b.value;finish()});
  body.querySelectorAll('[data-ifd-reinforce]').forEach(b=>b.onclick=()=>{showToast(stxIFDReinforce(p,fleetRecord(b.dataset.ifdReinforce))?'Replacement construction started':'Reinforcement unavailable');finish()});
  body.querySelectorAll('[data-ifd-intercept]').forEach(b=>b.onclick=()=>{showToast(stxIFDOrderIntercept(fleetRecord(b.dataset.ifdIntercept),fleetRecord(b.dataset.target))?'Interception ordered':'Contact no longer reachable');finish()});
  body.querySelectorAll('[data-ifd-join]').forEach(b=>b.onclick=()=>{const battle=stxIFDEncounters().find(x=>x.id===b.dataset.battle);if(battle)stxIFDJoin(fleetRecord(b.dataset.ifdJoin),battle);finish()});quote();
};

const STX_IFD_register=registerFleet;
registerFleet=function(...args){const f=STX_IFD_register(...args);stxIFDEnsure(f);return f};

const STX_IFD_create=createShip;
createShip=function(...args){if(state.ships.length+stxIFDHeldMilitary>=280)return null;return STX_IFD_create(...args)};
const STX_IFD_transit=stxDSCreateTransit;
stxDSCreateTransit=function(...args){if(state.ships.length+stxIFDHeldMilitary>=280)return null;return STX_IFD_transit(...args)};
function stxIFDRoleTick(){
  for(const f of state.fleets){
    if(f.owner!==0||f.destroyed||f.imperialEncounter||f.imperialRole==='Reserve'||f.imperialRole==='Assault'||f.imperialDoctrine==='Hold Position'||f.imperialDoctrine==='Evasive'||state.ships.some(s=>s.fleetId===f.id)||state.simTime<(f.imperialNextRole||0))continue;
    f.imperialNextRole=state.simTime+12;
    if([...state.battles,...(state.deepSpaceBattles||[])].some(b=>[...(b.attackerFleetIds||[]),...(b.defenderFleetIds||[])].includes(f.id)))continue;
    const pos=stxFleetPosition(f);if(!pos)continue;
    if(f.imperialRole==='Intercept'){
      const enemy=state.fleets.filter(t=>t.owner!==0&&!t.destroyed&&empiresAtWar(0,t.owner)&&stxFleetVisible(t)&&stxIFDEngages(f,t)).map(t=>({t,point:stxIFDIntercept(f,t)})).filter(x=>x.point).sort((a,b)=>a.point.time-b.point.time)[0];if(enemy)stxIFDOrderIntercept(f,enemy.t);
    }else if(f.imperialRole==='Raid'){
      const target=state.planets.filter(p=>p.owner!=null&&empiresAtWar(0,p.owner)&&dist(pos,p)<900).sort((a,b)=>(b.tradeVolume||0)-(a.tradeVolume||0))[0];if(target)stxDSDispatchFleet(f,target,'raid');
    }else{
      let target;if(f.imperialRole==='Defend')target=state.planets.find(p=>p.id===f.homePort&&p.owner===0);
      else if(f.imperialRole==='Escort'){const convoy=state.ships.find(s=>s.owner===0&&s.commercial&&dist(pos,s)<600);target=convoy&&state.planets.find(p=>p.id===convoy.to&&p.owner===0)}
      else if(f.imperialRole==='Patrol')target=playerWorlds().filter(p=>p.id!==f.location&&!p.underAttack).sort((a,b)=>dist(pos,a)-dist(pos,b))[0];
      if(target&&dist(pos,target)>40)stxDSDispatchFleet(f,target,'concentrate');
    }
  }
}
// Existing planetary battles retain their resolver; composition adjusts the
// initial assault effectiveness while geology strengthens actual defenses.
const STX_IFD_startBattle=startBattle;
startBattle=function(s,p){const before=new Set(state.battles.map(b=>b.id)),result=STX_IFD_startBattle(s,p),b=state.battles.find(b=>b.planetId===p.id&&!before.has(b.id));if(b){const f=fleetRecord(s.fleetId),comp=f?.imperialComposition,stats=stxIFDStats(comp),assault=stats.siege+(comp?.transports||0)/Math.max(1,stats.count)*.5,defense=stxIDTrait(p,'defense');b.attackerStrength*=assault;b.attackerInitial*=assault;b.defenderStrength*=defense;b.defenderInitial*=defense;stxIDHistory(p,'Battle began above this world');if(f)stxIDHistory(f,`Assault on ${p.name}`)}return result};
// Support vessels improve endurance in a crossing engagement.
const STX_IFD_draw=draw;
draw=function(){STX_IFD_draw();if(typeof stxGVShip!=='function')return;let budget=20;for(const f of state.fleets){const q=f.imperialReinforcement;if(!q||q.x==null||f.owner!==0||!visible(q.x,q.y,80)||budget--<=0)continue;const s=worldToScreen(q.x,q.y),target=stxFleetPosition(f);if(target)stxGVShip(s.x,s.y,Math.atan2(target.y-q.y,target.x-q.x),2,empire(f.owner).color,'frigate',stxGV.reduced?0:1)}};
const STX_IFD_mobilize=stxLPMobilize;
stxLPMobilize=function(...args){const f=STX_IFD_mobilize(...args);if(f){f.imperialComposition={line:f.vesselCount};f.imperialTarget={line:f.vesselCount};f.imperialHullPower=f.strength/Math.max(1,f.vesselCount)}return f};
