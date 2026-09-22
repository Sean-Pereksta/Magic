/*
 * Full-team formation audibles for Receiver Window QB.
 *
 * The game rebuilds its actors whenever a play is selected. This module keeps the
 * old pre-snap locations when a play is changed through the audible menu, then
 * makes both units physically shift into the new set. The offense must become set
 * before the snap button unlocks. Defensive recognition is delayed, imperfect,
 * coverage-aware, and improves when the same audible is repeated.
 */
(function(root,factory){
  'use strict';
  const api=factory(root||{});
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  if(root)root.QBFormationAudibles=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(root){
  'use strict';

  const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number(n)||0));
  const lerp=(a,b,t)=>a+(b-a)*t;
  const smooth=t=>{t=clamp(t,0,1);return t*t*(3-2*t)};
  const normalize=value=>String(value||'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();

  const BASE_PLAYS=[
    {name:'Four Verticals',routes:['Go','Go','Go','Go'],category:'Deep Shots',xs:[-18,-6,6,18]},
    {name:'Mesh',routes:['Drag','Post','Corner','Drag'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Levels',routes:['Dig','Drag','Dig','Go'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Smash',routes:['Curl','Corner','Corner','Curl'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Dagger',routes:['Double Move','Dig','Post','Go'],category:'Deep Shots',xs:[-18,-6,6,18]},
    {name:'Drive',routes:['Dig','Drag','Post','Out'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Crossfire',routes:['Post','Slant','Slant','Corner'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Sideline',routes:['Fade','Wheel','Wheel','Fade'],category:'Deep Shots',xs:[-18,-6,6,18]}
  ];

  const GROUP_ORDER=['Recommended','Screens','Spread','Trips','Bunch / Stack','Motion','Shot Plays'];

  function hashString(value){
    let h=2166136261;
    for(const c of String(value)){h^=c.charCodeAt(0);h=Math.imul(h,16777619)}
    return h>>>0;
  }
  function seededRandom(seed){
    let a=(Number(seed)>>>0)||0x9e3779b9;
    return function(){
      a|=0;a=a+0x6D2B79F5|0;
      let t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;
      return ((t^t>>>14)>>>0)/4294967296;
    };
  }

  function isScreenPlay(play){
    const routes=Array.isArray(play?.routes)?play.routes:[];
    return Number.isInteger(play?.screen)||play?.family==='Screens'||play?.category==='Screens'||
      /screen|bubble|tunnel|slip/i.test(play?.name||'')||routes.some(route=>/bubble|tunnel|slip|screen choice/i.test(route));
  }

  function formationShape(play){
    const family=String(play?.family||'');
    if(family==='Bunch')return 'Bunch';
    if(family==='Stack')return 'Stack';
    if(family==='Trips')return 'Trips';
    const xs=(Array.isArray(play?.xs)&&play.xs.length===4?play.xs:[-18,-6,6,18]).map(Number);
    const sorted=[...xs].sort((a,b)=>a-b);
    const duplicate=sorted.some((x,i)=>i&&Math.abs(x-sorted[i-1])<=1.1);
    if(duplicate)return 'Stack';
    const left=xs.filter(x=>x<0).length,right=xs.filter(x=>x>0).length;
    const sameSide=left>=3||right>=3;
    const tightestThree=Math.min(sorted[2]-sorted[0],sorted[3]-sorted[1]);
    if(tightestThree<=7.5)return 'Bunch';
    if(sameSide)return 'Trips';
    return 'Spread';
  }

  function playMeta(play,index=0){
    const shape=formationShape(play),screen=isScreenPlay(play),motion=!!play?.motion||play?.family==='Motion'||play?.category==='Motion';
    const category=String(play?.category||play?.family||'Intermediate');
    const shot=!screen&&(/deep|shot/i.test(category)||Array.isArray(play?.routes)&&play.routes.filter(r=>/go|fade|post|corner|wheel|double move|tunnel go/i.test(r)).length>=3);
    const group=screen?'Screens':motion?'Motion':shape==='Trips'?'Trips':(['Bunch','Stack'].includes(shape)?'Bunch / Stack':shot?'Shot Plays':'Spread');
    return {...play,index,shape,screen,motion,shot,group,setLabel:screen?(shape==='Spread'?'Screen':`${shape} Screen`):(motion?`${shape} Motion`:shape)};
  }

  function buildCatalog(concepts=[]){
    return [...BASE_PLAYS,...(Array.isArray(concepts)?concepts:[])].map((play,index)=>playMeta(play,index));
  }

  function setMismatch(fromPlay,toPlay){
    const from=fromPlay?.shape?fromPlay:playMeta(fromPlay||{}),to=toPlay?.shape?toPlay:playMeta(toPlay||{});
    let value=0;
    if(from.shape!==to.shape)value+=.42;
    if(from.screen!==to.screen)value+=.45;
    if(from.motion!==to.motion)value+=.15;
    if(from.group!==to.group)value+=.12;
    const fromXs=Array.isArray(from.xs)?from.xs:[-18,-6,6,18],toXs=Array.isArray(to.xs)?to.xs:[-18,-6,6,18];
    const travel=fromXs.reduce((sum,x,i)=>sum+Math.abs(Number(x)-Number(toXs[i]??x)),0)/48;
    return clamp(value+travel*.22,0,1);
  }

  function coverageTraits(name){
    const text=String(name||'').toUpperCase();
    return {
      man:/MAN|PRESS|BRACKET/.test(text),
      zone:/ZONE|TWO HIGH|ROBBER|MATCH/.test(text),
      pressure:/PRESSURE|BLITZ|ALL OUT/.test(text),
      disguise:/DISGUISE|MATCH/.test(text),
      deep:/DEEP|TWO HIGH/.test(text)
    };
  }

  function defenseResponsePlan({fromPlay,toPlay,coverageName='',repeatCount=0,audibleCount=1,seed=1,defenderCount=6}={}){
    const from=fromPlay?.shape?fromPlay:playMeta(fromPlay||{}),to=toPlay?.shape?toPlay:playMeta(toPlay||{});
    const traits=coverageTraits(coverageName),mismatch=setMismatch(from,to),passToScreen=!from.screen&&to.screen,screenToShot=from.screen&&!to.screen;
    const learned=clamp(repeatCount*.115+Math.max(0,audibleCount-1)*.045,0,.36);
    let surprise=.18+mismatch*.48+(passToScreen?.18:0)+(screenToShot?.08:0)+(traits.disguise?.05:0)+(traits.pressure&&passToScreen?.10:0)-learned;
    if(traits.zone&&passToScreen)surprise-=.045;
    surprise=clamp(surprise,.08,.82);
    const random=seededRandom(seed);
    const defenders=[];
    let busts=0;
    for(let i=0;i<defenderCount;i++){
      const corner=i<Math.min(4,defenderCount),safety=!corner;
      let delay=.12+surprise*(corner?.72:.93)+random()*.32;
      if(passToScreen&&traits.man&&corner)delay+=.14;
      if(passToScreen&&safety)delay+=.12;
      if(screenToShot&&traits.deep&&safety)delay-=.10;
      delay=clamp(delay,.10,1.45);
      let bustChance=.025+surprise*.19+(passToScreen&&traits.man&&corner?.075:0)+(traits.pressure?.035:0)-learned*.18;
      if(safety)bustChance*=.72;
      bustChance=clamp(bustChance,.015,.31);
      const busted=random()<bustChance;
      if(busted)busts++;
      let assignmentIndex=i;
      if(busted&&corner){
        const direction=random()<.5?-1:1;
        assignmentIndex=(i+direction+4)%4;
      }
      const side=(i%2===0?-1:1)*(random()<.5?-1:1);
      const wrongX=busted?side*lerp(1.25,3.8,random()):0;
      const wrongZ=busted?(passToScreen?-lerp(1.2,3.3,random()):screenToShot?lerp(1.0,2.8,random()):lerp(-1.8,1.8,random())):0;
      const speed=lerp(5.15,7.35,random())+(traits.zone?.15:0);
      const postSnapHold=clamp((busted?.28:.035)+surprise*(busted?.55:.17)+random()*(busted?.22:.10),.03,.88);
      defenders.push({index:i,role:corner?'corner':'safety',delay,speed,bustChance,busted,assignmentIndex,wrongX,wrongZ,postSnapHold});
    }
    const averageDelay=defenders.reduce((sum,d)=>sum+d.delay,0)/Math.max(1,defenders.length);
    const label=busts>=2?'LATE CHECK':busts===1?'ASSIGNMENT CHANGE':averageDelay>.68?'DEFENSE SHIFTING':'DEFENSE COMMUNICATING';
    return {from,to,mismatch,surprise,learned,passToScreen,screenToShot,busts,averageDelay,label,defenders,
      offenseBaseSeconds:clamp(.48+mismatch*.20,.48,.72)};
  }

  function moveTowards(position,target,maxDistance){
    const dx=target.x-position.x,dy=(target.y||0)-(position.y||0),dz=target.z-position.z;
    const distance=Math.hypot(dx,dy,dz);
    if(distance<=maxDistance||distance<1e-6)return {x:target.x,y:target.y||0,z:target.z,reached:true,distance};
    const scale=maxDistance/distance;
    return {x:position.x+dx*scale,y:(position.y||0)+dy*scale,z:position.z+dz*scale,reached:false,distance};
  }

  function recommendedPlays(catalog,current){
    const otherScreen=!current?.screen;
    const score=play=>{
      let value=0;
      if(otherScreen&&play.screen)value+=10;
      if(!otherScreen&&!play.screen)value+=7;
      if(play.shape!==current?.shape)value+=3;
      if(play.motion)value+=1.4;
      if(play.name===current?.name)value-=100;
      if(['WR Bubble','Tunnel Screen','Slip Screen','Bunch Screen','Jet Bubble','Slant / Flat','Mesh','Bunch Flood','Stack Switch','Dagger','Four Verticals'].includes(play.name))value+=2;
      return value;
    };
    return [...catalog].sort((a,b)=>score(b)-score(a)||a.index-b.index).slice(0,12);
  }

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
        return {mesh,start,target,correctTarget:ownTarget,rule,lastControlled:{...start},released:false};
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
        transition.liveAt=transition.liveAt||now;updateLiveShift(now,dt);return;
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
        const rule=move.rule;if(rule.busted)busted++;
        if(elapsed<rule.delay){
          const falseStep=Math.sin(clamp(elapsed/rule.delay,0,1)*Math.PI)*Math.min(.32,rule.delay*.24);
          const side=rule.index%2?1:-1;const hold={x:move.start.x+side*falseStep,y:move.start.y,z:move.start.z};setPosition(move.mesh,hold);move.lastControlled=hold;moving++;continue;
        }
        const current={x:move.mesh.position.x,y:move.mesh.position.y,z:move.mesh.position.z};
        const next=moveTowards(current,move.target,rule.speed*dt);setPosition(move.mesh,next);faceMovement(move.mesh,current,next,.24);move.lastControlled={x:next.x,y:next.y,z:next.z};
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
        const hold=move.rule.postSnapHold;
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
        const caught=transition.defense.filter(move=>move.rule.busted||move.rule.delay>.72).length;
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

  if(root.document&&root.THREE)installBrowser();

  return {BASE_PLAYS,GROUP_ORDER,hashString,seededRandom,isScreenPlay,formationShape,playMeta,buildCatalog,setMismatch,coverageTraits,defenseResponsePlan,moveTowards,recommendedPlays};
});
