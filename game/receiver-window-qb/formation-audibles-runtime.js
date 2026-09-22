/* Browser integration for full-team audibles. Load before game.js. */
(function(root){
  'use strict';
  const C=root.QBFormationAudibles,doc=root.document,THREE=root.THREE;
  if(!C||!doc||!THREE||root.__qbFormationAudiblesInstalled)return;
  const {clamp,lerp,smooth,normalize,GROUP_ORDER,buildCatalog,recommendedPlays,hashString,defenseResponsePlan,moveTowards}=C;
  function installBrowser(){
    const doc=root.document,THREE=root.THREE;
    if(!doc||!THREE||root.__qbFormationAudiblesInstalled)return;
    root.__qbFormationAudiblesInstalled=true;

    const scenes=new Set();
    const originalAdd=THREE.Scene?.prototype?.add;
    if(typeof originalAdd==='function'){
      THREE.Scene.prototype.add=function(...objects){scenes.add(this);return originalAdd.apply(this,objects)};
    }

    let catalog=[],currentIndex=0,activeGroup='Recommended',transition=null,lastFrame=0,lastPhase='',audibleCount=0,sequence=0,statusHideAt=0;
    const recentTargets=[];

    const isPlayerRoot=obj=>!!(obj&&obj.parent&&obj.userData&&obj.userData.body&&Array.isArray(obj.userData.arms));
    const scenePlayers=scene=>scene?.children?.filter(isPlayerRoot)||[];
    function activeScene(){
      let best=null,bestCount=-1;
      for(const scene of scenes){const count=scenePlayers(scene).length;if(count>bestCount){best=scene;bestCount=count}}
      return best;
    }
    function activeUnits(){
      const scene=activeScene(),players=scenePlayers(scene);
      return {scene,offense:players.filter(p=>p.userData.trackRing).slice(-4),defense:players.filter(p=>!p.userData.trackRing).slice(-6)};
    }
    const clonePosition=obj=>({x:obj.position.x,y:obj.position.y,z:obj.position.z,rotationY:obj.rotation?.y||0});
    const setPosition=(obj,p)=>{obj.position.set(p.x,p.y||0,p.z)};
    const phase=()=>doc.body?.dataset?.phase||'';

    function currentPlayIndex(){
      const text=doc.getElementById('playName')?.textContent||'';
      const found=catalog.find(play=>text.startsWith(play.name)||normalize(text).startsWith(normalize(play.name)));
      return found?found.index:currentIndex;
    }

    function injectStyles(){
      if(doc.getElementById('formationAudibleStyles'))return;
      const style=doc.createElement('style');style.id='formationAudibleStyles';style.textContent=`
#teamAudibleBtn{background:linear-gradient(135deg,#ffb321,#ef4d2f);color:#111;font-weight:950;letter-spacing:.045em;border:0;box-shadow:0 8px 22px rgba(239,77,47,.26)}
#teamAudiblePanel{display:none;margin-top:10px;border-top:1px solid rgba(255,255,255,.16);padding-top:10px;max-height:min(53vh,520px);overflow:auto;overscroll-behavior:contain}
#audiblePanel.teamAudibleMode #teamAudiblePanel{display:block}
#audiblePanel.teamAudibleMode #audibleReceivers,#audiblePanel.teamAudibleMode #audibleRouteGrid,#audiblePanel.teamAudibleMode .formationActions{display:none!important}
.teamAudibleIntro{font-size:12px;line-height:1.4;color:#dbe9f7;margin-bottom:9px}.teamAudibleIntro b{color:#fff}
.teamAudibleTabs{display:flex;gap:6px;overflow-x:auto;padding:2px 0 8px;scrollbar-width:thin}.teamAudibleTabs button{white-space:nowrap;min-height:34px;padding:7px 10px;border-radius:10px;font-size:11px;font-weight:850}.teamAudibleTabs button.active{background:#f3f7fb;color:#111;border-color:#fff}
.teamAudibleGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}.teamAudibleCall{text-align:left;min-height:62px;padding:9px 10px;border-radius:11px;display:flex;flex-direction:column;align-items:flex-start;justify-content:center;gap:4px}.teamAudibleCall strong{font-size:12px;color:#fff}.teamAudibleCall small{font-size:10px;line-height:1.25;color:#bcd0e2}.teamAudibleCall .screenPill{font-size:9px;font-weight:950;color:#191100;background:#ffc44f;border-radius:999px;padding:2px 6px;margin-left:5px}
#audibleShiftStatus{display:none;margin-top:7px;padding:7px 9px;border-radius:9px;background:rgba(8,18,28,.78);border:1px solid rgba(111,201,255,.42);font-size:10px;line-height:1.32;letter-spacing:.025em;color:#dff3ff}#audibleShiftStatus.active{display:block}#audibleShiftStatus.quick{border-color:rgba(255,190,67,.78);box-shadow:0 0 18px rgba(255,167,38,.12)}#audibleShiftStatus b{color:#fff}
@media(max-width:700px){#teamAudibleBtn{width:100%;min-height:43px}.teamAudibleGrid{grid-template-columns:1fr 1fr}.teamAudibleCall{min-height:58px;padding:8px}.teamAudibleTabs{padding-bottom:6px}#teamAudiblePanel{max-height:48vh}}
@media(max-width:420px){.teamAudibleGrid{grid-template-columns:1fr}.teamAudibleCall{min-height:52px}}
`;
      doc.head.appendChild(style);
    }

    function ensureUI(){
      if(doc.getElementById('teamAudibleBtn'))return true;
      const snap=doc.getElementById('snapBtn'),actions=snap?.parentElement,audiblePanel=doc.getElementById('audiblePanel');
      if(!snap||!actions||!audiblePanel)return false;
      const trigger=doc.createElement('button');trigger.id='teamAudibleBtn';trigger.type='button';trigger.textContent='TEAM AUDIBLE · SHIFT+A';
      actions.insertBefore(trigger,actions.querySelector('.hint')||null);
      const section=doc.createElement('section');section.id='teamAudiblePanel';section.setAttribute('aria-label','Full-team audible play calls');
      section.innerHTML=`<div class="teamAudibleIntro"><b>Change the entire formation and concept.</b> Your receivers must move and become set. The defense will recognize the check, communicate, and physically adjust—but a quick snap can catch late or crossed assignments.</div><div class="teamAudibleTabs" role="tablist"></div><div class="teamAudibleGrid"></div>`;
      audiblePanel.appendChild(section);
      const defensePanel=doc.getElementById('defensePanel');
      if(defensePanel){const status=doc.createElement('div');status.id='audibleShiftStatus';status.setAttribute('aria-live','polite');defensePanel.appendChild(status)}
      trigger.addEventListener('click',openTeamAudibles);
      section.addEventListener('click',event=>{
        const tab=event.target.closest('[data-audible-group]');if(tab){activeGroup=tab.dataset.audibleGroup;renderTeamAudibles();return}
        const call=event.target.closest('[data-team-play-index]');if(call)callTeamAudible(Number(call.dataset.teamPlayIndex));
      });
      const close=doc.getElementById('audibleClose');
      close?.addEventListener('click',()=>exitTeamMode());
      doc.addEventListener('pointerdown',event=>{if(event.target.closest('[data-audible-receiver],[data-audible-slot]'))exitTeamMode()},true);
      return true;
    }

    function exitTeamMode(){
      const panel=doc.getElementById('audiblePanel');panel?.classList.remove('teamAudibleMode');
    }

    function openTeamAudibles(){
      if(phase()!=='call')return;
      currentIndex=currentPlayIndex();
      const panel=doc.getElementById('audiblePanel');if(!panel)return;
      panel.style.display='block';panel.classList.add('teamAudibleMode');
      const title=doc.getElementById('audibleTitle');if(title)title.textContent='FULL-TEAM AUDIBLE';
      activeGroup='Recommended';renderTeamAudibles();
    }

    function filteredCatalog(){
      const current=catalog[currentIndex];
      if(activeGroup==='Recommended')return recommendedPlays(catalog,current);
      return catalog.filter(play=>play.group===activeGroup&&play.index!==currentIndex).slice(0,18);
    }

    function renderTeamAudibles(){
      const tabs=doc.querySelector('#teamAudiblePanel .teamAudibleTabs'),grid=doc.querySelector('#teamAudiblePanel .teamAudibleGrid');if(!tabs||!grid)return;
      tabs.innerHTML=GROUP_ORDER.map(group=>`<button type="button" role="tab" aria-selected="${group===activeGroup}" class="${group===activeGroup?'active':''}" data-audible-group="${group}">${group}</button>`).join('');
      const plays=filteredCatalog();
      grid.innerHTML=plays.map(play=>`<button type="button" class="teamAudibleCall" data-team-play-index="${play.index}"><strong>${play.name}${play.screen?'<span class="screenPill">SCREEN</span>':''}</strong><small>${play.setLabel} · ${play.category||play.group}</small></button>`).join('')||'<div class="teamAudibleIntro">No calls in this set.</div>';
    }

    function repeatCount(name){return recentTargets.filter(target=>target===name).length}
    function rememberTarget(name){recentTargets.push(name);if(recentTargets.length>8)recentTargets.shift()}

    function status(message,{quick=false,hold=0}={}){
      const node=doc.getElementById('audibleShiftStatus');if(!node)return;
      node.innerHTML=message;node.classList.add('active');node.classList.toggle('quick',quick);statusHideAt=hold?performance.now()+hold:0;
    }
    function hideStatus(){const node=doc.getElementById('audibleShiftStatus');node?.classList.remove('active','quick');statusHideAt=0}

    function findPlayButton(index){
      const buttons=[...doc.querySelectorAll('#playGrid button')];
      if(buttons[index])return buttons[index];
      const name=normalize(catalog[index]?.name);
      return buttons.find(button=>normalize(button.textContent).includes(name));
    }

    function callTeamAudible(targetIndex){
      if(phase()!=='call'||!catalog[targetIndex])return;
      const fromIndex=currentPlayIndex();if(targetIndex===fromIndex){exitTeamMode();doc.getElementById('audiblePanel').style.display='none';return}
      const before=activeUnits();if(before.offense.length<4||before.defense.length<4)return;
      const oldOffense=before.offense.map(clonePosition),oldDefense=before.defense.map(clonePosition);
      const from=catalog[fromIndex]||catalog[0],to=catalog[targetIndex];
      audibleCount++;sequence++;
      const coverage=doc.getElementById('defenseName')?.textContent||'';
      const seed=hashString(`${from.name}|${to.name}|${coverage}|${sequence}|${audibleCount}`);
      const plan=defenseResponsePlan({fromPlay:from,toPlay:to,coverageName:coverage,repeatCount:repeatCount(to.name),audibleCount,seed,defenderCount:oldDefense.length});
      rememberTarget(to.name);
      const button=findPlayButton(targetIndex);if(!button)return;
      button.click();
      currentIndex=targetIndex;exitTeamMode();
      queueMicrotask(()=>beginPhysicalShift({oldOffense,oldDefense,from,to,plan,targetIndex}));
    }

    function beginPhysicalShift(context,retry=0){
      const units=activeUnits();
      if((units.offense.length<4||units.defense.length<4)&&retry<4){requestAnimationFrame(()=>beginPhysicalShift(context,retry+1));return}
      const now=performance.now(),offenseTargets=units.offense.map(clonePosition),defenseTargets=units.defense.map(clonePosition);
      const offense=context.oldOffense.map((start,i)=>{
        const mesh=units.offense[i],target=offenseTargets[i]||start;if(mesh)setPosition(mesh,start);
        return {mesh,start,target};
      }).filter(move=>move.mesh);
      const maxOffenseTravel=offense.reduce((max,move)=>Math.max(max,Math.hypot(move.target.x-move.start.x,move.target.z-move.start.z)),0);
      const offenseDuration=clamp(context.plan.offenseBaseSeconds+maxOffenseTravel/36,.52,1.08);
      const defense=context.oldDefense.map((start,i)=>{
        const mesh=units.defense[i],rule=context.plan.defenders[i]||context.plan.defenders.at(-1),ownTarget=defenseTargets[i]||start;
        const assignmentTarget=defenseTargets[rule.assignmentIndex]||ownTarget;
        const target={x:assignmentTarget.x+rule.wrongX,y:assignmentTarget.y,z:assignmentTarget.z+rule.wrongZ,rotationY:ownTarget.rotationY};
        if(mesh)setPosition(mesh,start);
        return {mesh,start,target,correctTarget:ownTarget,rule,lastControlled:{...start},released:false,corrected:!rule.busted,caughtAtSnap:false};
      }).filter(move=>move.mesh);
      const snap=doc.getElementById('snapBtn'),wasDisabled=!!snap?.disabled;if(snap)snap.disabled=true;
      transition={...context,startedAt:now,lastAt:now,offenseDuration,offense,defense,offenseReady:false,snapWasDisabled:wasDisabled,snapAt:0,liveAt:0,phase:'call'};
      const late=context.plan.defenders.filter(d=>d.delay>.65||d.busted).length;
      status(`<b>${context.from.setLabel.toUpperCase()} → ${context.to.setLabel.toUpperCase()}</b><br>Offense changing sets · defense communicating (${late} late reads)`,{quick:false});
    }

    function faceMovement(mesh,from,to,amount=1){
      if(!mesh||!mesh.rotation)return;const dx=to.x-from.x,dz=to.z-from.z;if(Math.hypot(dx,dz)<.04)return;
      const target=Math.atan2(dx,dz),current=mesh.rotation.y||0;let delta=((target-current+Math.PI*3)%(Math.PI*2))-Math.PI;mesh.rotation.y=current+delta*clamp(amount,0,1);
    }

    function updateCallShift(now,dt,phaseName){
      if(!transition)return;
      if(['live','thrown','run','tackle'].includes(phaseName)){
        if(!transition.liveAt){
          transition.liveAt=now;
          for(const move of transition.defense){
            const distance=Math.hypot(move.mesh.position.x-move.correctTarget.x,move.mesh.position.z-move.correctTarget.z);
            move.caughtAtSnap=!move.corrected||distance>1.25;
          }
        }
        updateLiveShift(now,dt);return;
      }
      if(!['call','countdown'].includes(phaseName)){cancelTransition(false);return}
      const elapsed=(now-transition.startedAt)/1000;
      if(phaseName==='call'){
        let offenseReady=true;
        for(const move of transition.offense){
          const t=clamp(elapsed/transition.offenseDuration,0,1),eased=smooth(t);
          const next={x:lerp(move.start.x,move.target.x,eased),y:lerp(move.start.y,move.target.y,eased),z:lerp(move.start.z,move.target.z,eased)};
          setPosition(move.mesh,next);faceMovement(move.mesh,move.start,move.target,.18);
          if(t<1)offenseReady=false;else if(move.mesh.rotation)move.mesh.rotation.y=move.target.rotationY;
        }
        if(offenseReady&&!transition.offenseReady){
          transition.offenseReady=true;const snap=doc.getElementById('snapBtn');if(snap)snap.disabled=transition.snapWasDisabled;
        }
      }
      let moving=0,busted=0;
      for(const move of transition.defense){
        const rule=move.rule;
        if(elapsed<rule.delay){
          const falseStep=Math.sin(clamp(elapsed/rule.delay,0,1)*Math.PI)*Math.min(.32,rule.delay*.24);
          const side=rule.index%2?1:-1;const hold={x:move.start.x+side*falseStep,y:move.start.y,z:move.start.z};setPosition(move.mesh,hold);move.lastControlled=hold;moving++;if(rule.busted)busted++;continue;
        }
        const correcting=!rule.busted||elapsed>=rule.correctionAt,target=correcting?move.correctTarget:move.target;
        const current={x:move.mesh.position.x,y:move.mesh.position.y,z:move.mesh.position.z};
        const next=moveTowards(current,target,rule.speed*dt);setPosition(move.mesh,next);faceMovement(move.mesh,current,next,.24);move.lastControlled={x:next.x,y:next.y,z:next.z};
        if(rule.busted&&correcting&&next.reached)move.corrected=true;
        if(rule.busted&&!move.corrected)busted++;
        if(!next.reached)moving++;
      }
      if(transition.offenseReady){
        const quick=moving>0||busted>0;
        status(quick?`<b>QUICK-SNAP WINDOW</b><br>${moving} defenders still moving · ${busted} assignment ${busted===1?'check':'checks'}`:`<b>DEFENSE SET</b><br>The audible was recognized; the surprise window has closed.`,{quick});
      }
    }

    function updateLiveShift(now,dt){
      if(!transition)return;
      const liveElapsed=(now-transition.liveAt)/1000;let controlled=0;
      for(const move of transition.defense){
        if(move.released)continue;
        const gamePosition={x:move.mesh.position.x,y:move.mesh.position.y,z:move.mesh.position.z};
        const hold=move.caughtAtSnap?move.rule.postSnapHold:Math.min(.08,move.rule.postSnapHold);
        if(liveElapsed<hold){
          const reaction=move.rule.busted?.65:1.35;
          const next=moveTowards(move.lastControlled,gamePosition,move.rule.speed*reaction*dt);
          setPosition(move.mesh,next);move.lastControlled={x:next.x,y:next.y,z:next.z};controlled++;continue;
        }
        const blend=clamp((liveElapsed-hold)/.24,0,1),next={x:lerp(move.lastControlled.x,gamePosition.x,smooth(blend)),y:lerp(move.lastControlled.y,gamePosition.y,smooth(blend)),z:lerp(move.lastControlled.z,gamePosition.z,smooth(blend))};
        setPosition(move.mesh,next);move.lastControlled=next;
        if(blend>=1)move.released=true;else controlled++;
      }
      if(controlled===0){
        const caught=transition.defense.filter(move=>move.caughtAtSnap).length;
        status(caught?`<b>AUDIBLE CREATED LEVERAGE</b><br>${caught} defenders were still recovering at the snap.`:`<b>DEFENSE RECOVERED</b><br>The check was communicated before it created a major bust.`,{quick:caught>0,hold:1700});
        cancelTransition(true);
      }
    }

    function cancelTransition(keepStatus){
      if(!transition)return;
      const snap=doc.getElementById('snapBtn');if(snap&&phase()==='call')snap.disabled=transition.snapWasDisabled;
      transition=null;if(!keepStatus)hideStatus();
    }

    function tick(now){
      const phaseName=phase(),dt=clamp((now-(lastFrame||now))/1000,0,.05);lastFrame=now;
      if(lastPhase&&lastPhase!=='call'&&phaseName==='call'){audibleCount=0;cancelTransition(false)}
      lastPhase=phaseName;
      if(transition)updateCallShift(now,dt,phaseName);
      if(statusHideAt&&now>=statusHideAt&&!transition)hideStatus();
      requestAnimationFrame(tick);
    }

    function init(){
      injectStyles();catalog=buildCatalog(root.QBVariety?.concepts||[]);ensureUI();
      const playName=doc.getElementById('playName');
      if(playName)new MutationObserver(()=>{currentIndex=currentPlayIndex()}).observe(playName,{childList:true,subtree:true,characterData:true});
      doc.addEventListener('keydown',event=>{
        if(event.shiftKey&&event.code==='KeyA'&&phase()==='call'&&!event.repeat){event.preventDefault();event.stopImmediatePropagation();openTeamAudibles();return}
        if(transition&&!transition.offenseReady&&phase()==='call'&&event.code==='Space'){
          event.preventDefault();event.stopImmediatePropagation();status('<b>RECEIVERS MOVING</b><br>The offense must become set before the snap.',{quick:false});return;
        }
        if(['KeyX','KeyH','KeyY','KeyZ'].includes(event.code)&&!event.shiftKey)exitTeamMode();
      },true);
      requestAnimationFrame(tick);
    }

    if(doc.readyState==='loading')doc.addEventListener('DOMContentLoaded',init,{once:true});else queueMicrotask(init);
  }

  installBrowser();
})(typeof globalThis!=='undefined'?globalThis:this);
