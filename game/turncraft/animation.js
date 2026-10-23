/* TurnCraft cosmetic animation. Grid positions, timers and snapshots remain authoritative. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.TurncraftAnimation=api})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  const DIRECTIONS=['N','NE','E','SE','S','SW','W','NW'];
  const QUALITY={low:{fps:12,particles:48,ghosts:24,textureBytes:48*1024*1024,textureScale:.5,lighting:false,shadows:false},medium:{fps:24,particles:120,ghosts:48,textureBytes:96*1024*1024,textureScale:.75,lighting:false,shadows:true},high:{fps:40,particles:180,ghosts:72,textureBytes:192*1024*1024,textureScale:1,lighting:true,shadows:true}};
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  const finite=(v,fallback=0)=>Number.isFinite(Number(v))?Number(v):fallback;
  const wrap=a=>((a+Math.PI)%(Math.PI*2)+Math.PI*2)%(Math.PI*2)-Math.PI;
  const HEAL_PHASES={prepare:140,activate:160,complete:220};
  function healingPresentation(entity,index,time,isVisible=()=>true){
    if(entity?.type!=='medic'||entity.alive===false||entity.garrisonedIn||entity.healTargetId==null||!(entity.healUntil>time))return null;
    const target=index.get(entity.healTargetId),start=finite(entity.healStartedAt,time);
    if(!target||target.alive===false||target.garrisonedIn||target.faction!==entity.faction||!isVisible(entity)||!isVisible(target)||Math.hypot(target.x-entity.x,target.y-entity.y)>finite(entity.healRange,3)+.05)return null;
    if(entity.attackAnim?.endAt>time||entity.projectileAnim?.endAt>time||entity.attackAnim?.startAt>=start||entity.projectileAnim?.startAt>=start||entity.healOrder!==undefined&&(entity.order??'')!==entity.healOrder||entity.healAbilityReadyAt!==undefined&&finite(entity.abilityReadyAt)!==entity.healAbilityReadyAt)return null;
    const age=Math.max(0,time-start),phase=age<HEAL_PHASES.prepare?'healPrepare':age<HEAL_PHASES.prepare+HEAL_PHASES.activate?'healActivate':'healLoop';
    return{target,start,end:entity.healUntil,age,phase,beam:phase==='healLoop'};
  }
  const assetTypeFor=entity=>entity?.kind==='cocoon'?'cocoon':entity?.kind==='larva'?'larva':entity?.type;
  function selectionRenderContext(entities,selectedIds,selectedId,index){
    const lookup=index||new Map(entities.filter(e=>e.alive!==false).map(e=>[e.id,e])),selected=new Set(selectedIds||[]),targets=new Set();if(selectedId!=null)selected.add(selectedId);
    const orderIds=new Set(selectedIds?.length?selectedIds:selectedId!=null?[selectedId]:[]);
    for(const id of orderIds){const unit=lookup.get(id);if(unit?.alive!==false&&unit?.targetId!=null)targets.add(unit.targetId)}
    return{selected,targets};
  }
  function directionForVector(dx,dy,previous='S',hysteresis=.12){
    if(Math.hypot(dx,dy)<.0001)return previous;
    const angle=Math.atan2(dx,-dy),index=DIRECTIONS.indexOf(previous),center=(index<0?4:index)*Math.PI/4;
    if(Math.abs(wrap(angle-center))<=Math.PI/8+hysteresis)return previous;
    return DIRECTIONS[(Math.round(angle/(Math.PI/4))+8)%8];
  }
  function sampleSegment(segment,time){
    if(!segment)return null;const progress=clamp((time-segment.start)/Math.max(1,segment.end-segment.start),0,1);
    // Constant speed avoids a stop/start ease at every grid boundary.
    if(segment.points?.length>1){const total=segment.length||1,distance=total*progress;let walked=0;for(let i=1;i<segment.points.length;i++){const from=segment.points[i-1],to=segment.points[i],length=Math.hypot(to.x-from.x,to.y-from.y);if(distance<=walked+length||i===segment.points.length-1){const q=clamp((distance-walked)/Math.max(.0001,length),0,1);return{x:from.x+(to.x-from.x)*q,y:from.y+(to.y-from.y)*q,progress,moving:progress<1&&total>.001,dx:to.x-from.x,dy:to.y-from.y}}walked+=length}}
    return {x:segment.fromX+(segment.toX-segment.fromX)*progress,y:segment.fromY+(segment.toY-segment.fromY)*progress,progress,moving:progress<1&&Math.hypot(segment.toX-segment.fromX,segment.toY-segment.fromY)>.001};
  }
  function frameForState(state,direction,elapsedMs,options={}){
    if(!state)return null;
    const directions=state.directions||['S'],dir=directions.includes(direction)?direction:directions.includes('S')?'S':directions[0];
    const coordinates=state.coordinates?.[dir],count=Math.max(1,coordinates?.length||finite(state.frames,1));
    const fps=Math.max(.1,finite(state.fps,6)),cycle=count*1000/fps;
    let phase=options.progress==null?Math.max(0,elapsedMs)*finite(options.rate,1)/cycle:clamp(options.progress,0,1);
    if(options.progress==null&&state.loop!==false)phase%=1;else phase=clamp(phase,0,1);
    const index=Math.min(count-1,Math.floor(phase*count));
    const explicit=coordinates?.[index];
    return {x:explicit?.x??((finite(state.firstColumn)+index)*finite(state.frameWidth,128)),y:explicit?.y??(finite(state.rows?.[dir],Math.max(0,directions.indexOf(dir)))*finite(state.frameHeight,128)),w:explicit?.w??finite(state.frameWidth,128),h:explicit?.h??finite(state.frameHeight,128),index,direction:dir,cycle};
  }
  function eventProgress(definition,event,time){
    if(!event)return undefined;const duration=Math.max(1,event.end-event.start);
    const hasFrame=definition?.contactFrame!=null&&Number.isFinite(Number(definition.contactFrame)),hasMs=definition?.contactMs!=null&&Number.isFinite(Number(definition.contactMs));
    if(event.kind!=='attack'||(!hasFrame&&!hasMs)||!Number.isFinite(event.contactAt))return clamp((time-event.start)/duration,0,1);
    const count=Math.max(1,finite(definition.frames,6)),cycle=Math.max(1,count*1000/Math.max(1,finite(definition.fps,10))),contact=clamp(hasFrame?Number(definition.contactFrame)/count:Number(definition.contactMs)/cycle,0,.95),at=clamp(event.contactAt,event.start,event.end);
    // Skip anticipation when the logical shot has already fired, and align melee contact
    // to its existing 270 ms impact; this function never reschedules game events.
    return time<at?contact*clamp((time-event.start)/Math.max(1,at-event.start),0,1):contact+(1-contact)*clamp((time-at)/Math.max(1,event.end-at),0,1);
  }
  const COMBAT_QUALITY={low:{events:32,particles:36,detail:2,fields:12,atlas:8},medium:{events:96,particles:96,detail:4,fields:32,atlas:32},high:{events:160,particles:156,detail:7,fields:56,atlas:64}};
  const ATTACK_FAMILIES={rifleman:'ballistic',reaper:'ballistic',medic:'ballistic',marauder:'explosive',hellion:'flame',siegeTank:'artillery',goliath:'missile',missileTurret:'missile',wraith:'plasma',battlecruiser:'plasma',dragoon:'psionic',photonCannon:'psionic',zealot:'blade',sentinel:'electrical',sentry:'electrical',templar:'electrical',immortal:'lance',disruptor:'nova',phoenix:'lance',carrier:'interceptors',hydralisk:'spine',roach:'acid',ravager:'acid',lurker:'subterranean',sunkenColony:'subterranean',mutalisk:'glaive',defiler:'spores',ultralisk:'heavy',zergling:'claw'};
  const COMBAT_COLORS={ballistic:'#ffe3a0',explosive:'#ffb667',flame:'#ff9453',artillery:'#ffd3a0',missile:'#ffc985',plasma:'#a6edff',psionic:'#b8b1ff',blade:'#b2f4ff',electrical:'#b1e5ff',lance:'#f9e8ba',nova:'#e3bdff',interceptors:'#b8f4ff',spine:'#d9edb0',acid:'#a8f16d',subterranean:'#cfbe8a',glaive:'#b9e78f',spores:'#b6eb7a',heavy:'#dbc7a8',claw:'#dbb4bc'};
  function combatFamily(entity){return ATTACK_FAMILIES[entity?.type]||'ballistic'}
  function abilityVisual(id){if(id==='phaseStep'||id==='warpConduit'||id==='tunnelTravel')return'teleport';if(/Repair|Surge|Mend|Stabilization/.test(id))return'heal';if(/shield|Shield|Barrier|guardianLink/.test(id))return'shield';if(/siegeMode|burrow|Muster|Breach/.test(id))return'deploy';if(/fungal|acid|Bile|creep/i.test(id))return'organic';if(/Storm|Lance|Nova|graviton/i.test(id))return'psionic';if(/Charge|Rush|afterburner|Boost|Dive|Leap/.test(id))return'charge';return'buff'}
  function abilityWasCast(entity,previous,time){return entity.lastAbilityCastAt!=null?entity.lastAbilityCastAt!==previous.cast:finite(previous.ability)<=time&&finite(entity.abilityReadyAt)>time&&finite(entity.abilityReadyAt)>finite(previous.ability)}
  const noise=(seed,index)=>{const n=Math.sin(seed*12.9898+index*78.233)*43758.5453;return n-Math.floor(n)};
  function floatingLabels(items,limit=64){const grouped=new Map(),labels=[],rows=new Map();for(const item of items){const numeric=/^[-+]\d+$/.test(item.text),key=numeric?`${Math.floor(item.x)}:${Math.floor(item.y+.5)}:${item.color}:${item.text[0]}`:null;if(key&&grouped.has(key)){const label=grouped.get(key);label.total+=Number(item.text);label.text=(label.total>0?'+':'')+label.total;label.life=Math.max(label.life,item.life);continue}if(labels.length>=limit)continue;const cell=`${Math.floor(item.x)}:${Math.floor(item.y+.5)}`,row=rows.get(cell)||0;if(row>=3)continue;rows.set(cell,row+1);const label={...item,y:item.y-.24-row*.22,total:numeric?Number(item.text):0};labels.push(label);if(key)grouped.set(key,label)}return labels}
  // Cosmetic event and particle pools are bounded independently from simulation.
  // Impacts are emitted only from observed HP/shield losses, never projectile expiry.
  class CombatEffects{
    constructor(quality='medium'){this.events=[];this.freeEvents=[];this.particles=[];this.freeParticles=[];this.seen=new Map();this.observed=new Map();this.serial=0;this.setQuality(quality)}
    setQuality(value){this.quality=Object.hasOwn(COMBAT_QUALITY,value)?value:'medium';this.budget=COMBAT_QUALITY[this.quality];while(this.events.length>this.budget.events)this.freeEvents.push(this.events.shift());while(this.particles.length>this.budget.particles)this.freeParticles.push(this.particles.pop());this.freeEvents.length=Math.min(this.freeEvents.length,this.budget.events);this.freeParticles.length=Math.min(this.freeParticles.length,this.budget.particles)}
    reset(){this.freeEvents.push(...this.events);this.freeParticles.push(...this.particles);this.events.length=0;this.particles.length=0;this.seen.clear();this.observed.clear();this.setQuality(this.quality)}
    emit(key,kind,x,y,time,{duration=520,family='psionic',color,scale=1,defense,entity,from,visible=true}={}){
      if(!visible||![x,y,time].every(Number.isFinite)||this.seen.has(key))return false;
      this.seen.set(key,time+duration+1000);while(this.seen.size>this.budget.events*6)this.seen.delete(this.seen.keys().next().value);
      const event=this.events.length>=this.budget.events?this.events.shift():this.freeEvents.pop()||{};Object.assign(event,{key,kind,x,y,start:time,end:time+duration,family,color:color||COMBAT_COLORS[family]||'#bddcff',scale,defense,entity:entity?{type:assetTypeFor(entity),kind:entity.kind,faction:entity.faction,direction:entity.direction||'S'}:null,from,seed:++this.serial});this.events.push(event);
      if(['impact','death','teleportOut','teleportIn','heal','cast'].includes(kind))this.burst(event,time);
      return true;
    }
    burst(event,time){const base=event.kind==='death'?10:event.kind==='impact'?5:4,count=Math.min(this.budget.particles-this.particles.length,Math.ceil(base*this.budget.detail/4*event.scale));for(let i=0;i<count;i++){const p=this.freeParticles.pop()||{},angle=noise(event.seed,i)*Math.PI*2,speed=(.25+noise(event.seed,i+21)*.8)*event.scale;Object.assign(p,{x:event.x+.5,y:event.y+.5,vx:Math.cos(angle)*speed,vy:Math.sin(angle)*speed-(event.kind==='heal'?.6:0),start:time,end:time+260+noise(event.seed,i+44)*360,color:event.color,seed:event.seed,size:.022+noise(event.seed,i+55)*.035,organic:['acid','spores','claw'].includes(event.family),gravity:event.family==='artillery'||event.defense==='armor'});this.particles.push(p)}}
    attack(entity,time,{visible=true,origin}={}){const animation=entity.projectileAnim||entity.attackAnim;if(!animation||animation.startAt>time||time-animation.startAt>220)return false;return this.emit(`a:${entity.id}:${animation.startAt}:${animation.endAt}:${animation.seed??''}`,'release',origin?.x??entity.px??entity.x,origin?.y??entity.py??entity.y,animation.startAt,{duration:180,family:combatFamily(entity),visible,from:{x:animation.tx??animation.targetX+.5,y:animation.ty??animation.targetY+.5}})}
    impact(entity,time,{loss=0,shieldLoss=0,attacker,armored=false,visible=true}={}){if(loss+shieldLoss<=0)return false;const family=combatFamily(attacker),defense=shieldLoss>0?'shield':armored?'armor':'flesh',scale=loss+shieldLoss>=20||loss+shieldLoss>finite(entity.maxHp,100)*.22?1.5:1;return this.emit(`h:${entity.id}:${entity.lastDamagedAt}:${entity.hp}:${entity.shield}`,'impact',entity.px??entity.x,entity.py??entity.y,time,{duration:family==='flame'?640:480,family,defense,scale,color:defense==='shield'?'#b1d5ff':defense==='armor'?'#ffe1aa':['acid','spores'].includes(family)?COMBAT_COLORS[family]:'#dbabb3',visible})}
    ability(entity,id,time,{origin,target,visible=true}={}){const kind=abilityVisual(id),key=`b:${entity.id}:${entity.lastAbilityCastAt??entity.abilityReadyAt}:${id}`,family=kind==='organic'?'spores':kind==='charge'?'flame':kind==='heal'?'psionic':combatFamily(entity),color={teleport:'#ad9cff',heal:'#9bffce',shield:'#aadfff',deploy:'#f6cf9f',organic:'#b3ec78',psionic:'#c0b7ff',charge:'#ffe0a7',buff:COMBAT_COLORS[family]}[kind];if(kind==='teleport'&&origin&&(origin.x!==entity.x||origin.y!==entity.y)){this.emit(key+':out','teleportOut',origin.x,origin.y,time,{duration:420,color,family,entity,visible:origin.visible??visible});return this.emit(key,'teleportIn',entity.x,entity.y,time,{duration:420,color,family,visible})}const radius={yamatoBlast:.85,missileBarrage:1.55,purificationNova:2.05}[id];if(radius&&target)this.emit(key+':ground','detonation',target.x,target.y,time,{duration:520,color,family,scale:radius/.7,visible});return this.emit(key,kind==='heal'?'heal':kind==='deploy'?'deploy':'cast',entity.px??entity.x,entity.py??entity.y,time,{duration:620,color,family,defense:kind,from:target,visible})}
    prepare(entity,time,{visible=true}={}){const remaining=finite(entity.attackReadyAt)-time;if(remaining<=0||remaining>180)return false;return this.emit(`p:${entity.id}:${entity.attackReadyAt}`,'prepare',entity.px??entity.x,entity.py??entity.y,time,{duration:remaining,family:combatFamily(entity),visible})}
    death(entity,time,visible=true){return this.emit(`d:${entity.id}:${entity.lastDamagedAt}`,'death',entity.px??entity.x,entity.py??entity.y,time,{duration:680,family:combatFamily(entity),color:COMBAT_COLORS[combatFamily(entity)],scale:entity.kind==='structure'?1.8:1,visible})}
    link(source,target,time,shield=false,visible=true){return this.emit(`l:${source.id}:${source.abilityReadyAt}:${target.id}`,'link',source.px??source.x,source.py??source.y,time,{duration:480,color:shield?'#b7dcff':'#a9ffd3',from:{x:(target.px??target.x)+.5,y:(target.py??target.y)+.5},visible})}
    observe(entity,time,{visible=true,armored=false}={}){let previous=this.observed.get(entity.id);if(previous){if(entity.lastDamagedAt!==previous.hit&&time-finite(entity.lastDamagedAt)<400)this.impact(entity,entity.lastDamagedAt,{loss:Math.max(0,previous.hp-entity.hp),shieldLoss:Math.max(0,previous.shield-finite(entity.shield)),armored,visible});if(abilityWasCast(entity,previous,time)&&entity.lastAbilityId)this.ability(entity,entity.lastAbilityId,time,{origin:{x:previous.x,y:previous.y,visible:previous.visible},target:entity.projectileAnim?{x:entity.projectileAnim.tx-.5,y:entity.projectileAnim.ty-.5}:null,visible});const gain=entity.hp-previous.hp;if(gain>0&&gain<=finite(entity.maxHp,100))this.emit(`r:${entity.id}:${time}:${entity.hp}`,'heal',entity.x,entity.y,time,{duration:480,color:'#a8ffd0',scale:.7,visible});}else{previous={};this.observed.set(entity.id,previous)}this.attack(entity,time,{visible});Object.assign(previous,{x:entity.x,y:entity.y,hp:entity.hp,shield:finite(entity.shield),hit:entity.lastDamagedAt,ability:finite(entity.abilityReadyAt),cast:entity.lastAbilityCastAt,visible})}
    update(time,alive){for(let i=this.events.length-1;i>=0;i--)if(this.events[i].end<=time)this.freeEvents.push(...this.events.splice(i,1));for(let i=this.particles.length-1;i>=0;i--)if(this.particles[i].end<=time)this.freeParticles.push(...this.particles.splice(i,1));for(const [key,end]of this.seen)if(end<=time)this.seen.delete(key);if(alive)for(const id of this.observed.keys())if(!alive.has(id))this.observed.delete(id)}
    draw(ctx,time,{size=48,worldToScreen=(x,y)=>({x,y}),visible=()=>true,inViewport=()=>true,atlas=()=>false,silhouette=()=>false}={}){
      this.update(time);let count=0,atlasDraws=0;const drawAtlas=(...args)=>{if(atlasDraws>=this.budget.atlas)return false;const result=atlas(...args);if(result!==false)atlasDraws++;return result};
      for(const event of this.events){if(time<event.start||!visible(event.x,event.y)||!inViewport(event.x,event.y))continue;const p=clamp((time-event.start)/(event.end-event.start),0,1),s=worldToScreen(event.x+.5,event.y+.5),r=size*event.scale,color=event.color;ctx.save();ctx.globalAlpha=1-p;ctx.strokeStyle=color;ctx.fillStyle=color;ctx.lineWidth=Math.max(1,size*.035);ctx.lineCap='round';if(this.quality==='high'){ctx.shadowColor=color;ctx.shadowBlur=size*.13}
        const ring=(radius,alpha=1)=>{ctx.globalAlpha=(1-p)*alpha;ctx.beginPath();ctx.arc(s.x,s.y,r*radius,0,Math.PI*2);ctx.stroke()};
        if(event.kind==='impact'){
          const row=event.defense==='shield'?'shield':event.defense==='flesh'?'organic':['artillery','explosive','missile'].includes(event.family)?'explosion':['psionic','nova','plasma','electrical','lance'].includes(event.family)?'energy':'ballistic';if(time-event.start<260)drawAtlas(row,s.x,s.y,r*.88,time-event.start,{quality:this.quality,alpha:1-p});
          if(event.defense==='shield'){ring(.18+p*.44);ring(.12+p*.3,.5);ctx.globalAlpha=(1-p)*.18;ctx.beginPath();ctx.arc(s.x,s.y,r*.45,0,Math.PI*2);ctx.fill()}
          else if(['artillery','explosive','missile','heavy','subterranean','nova'].includes(event.family)){ring(.12+p*.7,.7);if(this.quality!=='low'&&time-event.start<260)drawAtlas('dust',s.x,s.y+r*.15,r*(.65+p),time-event.start,{quality:this.quality,alpha:(1-p)*.4});}
          else if(event.family==='blade'||event.family==='claw'){ctx.beginPath();ctx.arc(s.x,s.y,r*(.23+p*.2),-.9+p,.8+p);ctx.stroke()}
          if(event.defense==='armor'){for(let i=0;i<this.budget.detail;i++){const a=noise(event.seed,i)*Math.PI*2,d=r*(.08+p*.4);ctx.beginPath();ctx.moveTo(s.x+Math.cos(a)*d,s.y+Math.sin(a)*d);ctx.lineTo(s.x+Math.cos(a)*(d+r*.1),s.y+Math.sin(a)*(d+r*.1));ctx.stroke()}}
        }else if(event.kind==='detonation'){ring(.1+p*.6,.8);drawAtlas(['acid','spores'].includes(event.family)?'organic':'explosion',s.x,s.y,r*.9,time-event.start,{quality:this.quality,alpha:(1-p)*.8});}
        else if(event.kind==='prepare'){ctx.globalAlpha=.2+p*.6;ctx.beginPath();ctx.arc(s.x,s.y,r*(.36-p*.12),p*2,p*2+Math.PI*1.5);ctx.stroke();}
        else if(event.kind==='release'){
          const angle=Math.atan2((event.from?.y??event.y+.5)-event.y-.5,(event.from?.x??event.x+.5)-event.x-.5),sx=s.x+Math.cos(angle)*r*.25,sy=s.y+Math.sin(angle)*r*.25;ctx.translate(sx,sy);ctx.rotate(angle);ctx.beginPath();ctx.moveTo(r*.2,0);ctx.lineTo(-r*.05,-r*.11);ctx.lineTo(0,0);ctx.lineTo(-r*.05,r*.11);ctx.closePath();ctx.fill();
        }else if(event.kind==='link'){if(event.from&&visible(Math.floor(event.from.x),Math.floor(event.from.y))){const end=worldToScreen(event.from.x,event.from.y);ctx.globalAlpha=(1-p)*.6;ctx.lineWidth=Math.max(1,size*.025);ctx.beginPath();ctx.moveTo(s.x,s.y);ctx.lineTo(end.x,end.y);ctx.stroke();for(let i=0;i<this.budget.detail;i++){const q=(p+i/this.budget.detail)%1;ctx.beginPath();ctx.arc(s.x+(end.x-s.x)*q,s.y+(end.y-s.y)*q,size*.025,0,Math.PI*2);ctx.fill()}}}
        else if(event.kind==='teleportOut'){ctx.globalAlpha=(1-p)*.45;silhouette(event,s,r,p);ring(.3+p*.2,.7);}
        else if(event.kind==='teleportIn'){ring(.12+p*.56);ring(.08+p*.36,.6);}
        else if(event.kind==='death'){ring(.15+p*.55,.65);drawAtlas(['acid','spores','claw','heavy'].includes(event.family)?'organic':'explosion',s.x,s.y,r*(.8+p*.3),time-event.start,{quality:this.quality,alpha:(1-p)*.6});}
        else if(event.kind==='deploy'){for(let i=0;i<4;i++){const a=i*Math.PI/2+p*.45,x=s.x+Math.cos(a)*r*.4,y=s.y+Math.sin(a)*r*.4;ctx.save();ctx.translate(x,y);ctx.rotate(a);ctx.strokeRect(-r*.1,-r*.06,r*.2,r*.12);ctx.restore()}ring(.24+p*.12,.45)}
        else if(event.kind==='heal'){ring(.18+p*.3,.6);ctx.beginPath();ctx.moveTo(s.x-r*.1,s.y-r*p*.3);ctx.lineTo(s.x+r*.1,s.y-r*p*.3);ctx.moveTo(s.x,s.y-r*(p*.3+.1));ctx.lineTo(s.x,s.y-r*(p*.3-.1));ctx.stroke()}
        else{ring(.18+p*.38,.8);if(event.defense==='shield')ring(.26+p*.42,.45);else if(event.defense==='charge'){for(let i=0;i<3;i++){ctx.beginPath();ctx.moveTo(s.x-r*.35+i*r*.28,s.y+r*.18);ctx.lineTo(s.x-r*.18+i*r*.28,s.y);ctx.lineTo(s.x-r*.35+i*r*.28,s.y-r*.18);ctx.stroke()}}else if(event.defense==='psionic'){for(let i=0;i<3;i++){const a=i*Math.PI*2/3+p;ctx.beginPath();ctx.moveTo(s.x,s.y);ctx.lineTo(s.x+Math.cos(a)*r*.18,s.y+Math.sin(a+.2)*r*.2);ctx.lineTo(s.x+Math.cos(a)*r*.42,s.y+Math.sin(a)*r*.42);ctx.stroke()}}}
        ctx.restore();count++;
      }
      // One compatible fill per color keeps the small pooled aftermath cheap.
      ctx.save();ctx.shadowBlur=0;for(const particle of this.particles){const age=(time-particle.start)/1000,p=(time-particle.start)/(particle.end-particle.start),x=particle.x+particle.vx*age,y=particle.y+particle.vy*age+(particle.gravity?age*age*.8:0);if(p<0||!visible(Math.floor(x),Math.floor(y))||!inViewport(x,y))continue;const s=worldToScreen(x,y);ctx.globalAlpha=(1-p)*.8;ctx.fillStyle=particle.color;ctx.beginPath();ctx.arc(s.x,s.y,Math.max(.8,size*particle.size*(1-p*.6)),0,Math.PI*2);ctx.fill()}ctx.restore();return count;
    }
    drawTrails(ctx,projectile,time,{size=48,worldToScreen=(x,y)=>({x,y}),visible=()=>true}={}){
      if(!projectile||time<projectile.startAt||time>=projectile.endAt)return false;const p=clamp((time-projectile.startAt)/Math.max(1,projectile.endAt-projectile.startAt),0,1),{x,y,tx,ty}=projectile,dx=tx-x,dy=ty-y,length=Math.hypot(dx,dy);if(!length||!visible(Math.floor(x),Math.floor(y))||!visible(Math.floor(tx),Math.floor(ty)))return false;const style=projectile.style,start=worldToScreen(x,y),end=worldToScreen(tx,ty),angle=Math.atan2(dy,dx),detail=this.budget.detail;ctx.save();ctx.strokeStyle=projectile.color;ctx.fillStyle=projectile.color;ctx.lineCap='round';ctx.shadowBlur=0;
      if(style==='flame'){for(let i=0;i<detail*2;i++){const q=p*(i+1)/(detail*2),s=worldToScreen(x+dx*q,y+dy*q),w=Math.sin(q*21+time/40)*size*.025;ctx.globalAlpha=(1-q/(p+.001))*.5;ctx.fillStyle=i%2?'#fff1b1':'#ff8a42';ctx.beginPath();ctx.arc(s.x-Math.sin(angle)*w,s.y+Math.cos(angle)*w,size*(.055+q*.075),0,Math.PI*2);ctx.fill()}if(this.quality!=='low'){ctx.globalAlpha=.2;ctx.strokeStyle='#ffc888';ctx.lineWidth=size*.025;ctx.beginPath();ctx.moveTo(start.x,start.y-size*.08);ctx.quadraticCurveTo((start.x+end.x)/2,start.y-size*.17,end.x,end.y-size*.1);ctx.stroke()}}
      else if(style==='groundSpine'){for(let i=1;i<=detail;i++){const q=p-i*.07;if(q<0)continue;const s=worldToScreen(x+dx*q,y+dy*q);ctx.globalAlpha=(1-i/(detail+1))*.7;ctx.beginPath();ctx.moveTo(s.x-size*.07,s.y+size*.08);ctx.lineTo(s.x,s.y-size*(.12+.06*Math.sin(time/70+i)));ctx.lineTo(s.x+size*.07,s.y+size*.08);ctx.fill()}}
      else{const supported=['energyOrb','plasma','lance','lightning','interceptors','glaive','acid','corrosive','spine','missile','artillery','nova'];if(!supported.includes(style)){ctx.restore();return false}if(style==='plasma'){const head=worldToScreen(x+dx*p,y+dy*p);ctx.globalAlpha=.2;ctx.lineWidth=size*.08;ctx.beginPath();ctx.moveTo(start.x,start.y);ctx.lineTo(head.x,head.y);ctx.stroke()}const strands=style==='interceptors'?3:1;for(let strand=0;strand<strands;strand++){ctx.globalAlpha=.3;ctx.lineWidth=Math.max(1,size*(style==='plasma'?.085:.035));ctx.beginPath();for(let i=0;i<=detail;i++){const q=clamp(p-i*.035-strand*.045,0,1),w=style==='interceptors'?Math.sin(q*28+strand)*size*.1:Math.sin(q*25+time/110)*size*.018,arc=['artillery','corrosive'].includes(style)?Math.sin(Math.PI*q)*size*(style==='artillery'?.5:.25):0,s=worldToScreen(x+dx*q,y+dy*q);if(i===0)ctx.moveTo(s.x-Math.sin(angle)*w,s.y+Math.cos(angle)*w-arc);else ctx.lineTo(s.x-Math.sin(angle)*w,s.y+Math.cos(angle)*w-arc)}ctx.stroke()}}
      ctx.restore();return true;
    }
    drawFields(ctx,areas,obstacles,time,{size=48,worldToScreen=(x,y)=>({x,y}),visible=()=>true,inViewport=()=>true}={}){
      let count=0;for(const effect of areas){if(count>=this.budget.fields)break;if(effect.endAt<=time||effect.startAt>time||!['psionicStorm','fungalBulb','acidPool','bileWarning'].includes(effect.kind))continue;
        if(effect.kind==='bileWarning'){if(effect.resolved){if(visible(effect.x,effect.y))this.emit('bile:'+effect.id,'detonation',effect.x,effect.y,effect.impactAt,{duration:520,family:'acid',color:'#c6ff85',scale:1.35/.7});continue}let any=false;ctx.save();ctx.beginPath();for(let y=-2;y<=2;y++)for(let x=-2;x<=2;x++)if(visible(effect.x+x,effect.y+y)&&inViewport(effect.x+x,effect.y+y)){const cell=worldToScreen(effect.x+x,effect.y+y);ctx.rect(cell.x,cell.y,size+.5,size+.5);any=true}ctx.clip();if(any&&time<effect.impactAt){const p=clamp((time-effect.startAt)/Math.max(1,effect.impactAt-effect.startAt),0,1),s=worldToScreen(effect.x+.5,effect.y+.5);ctx.strokeStyle='#c6ff8b';ctx.lineWidth=Math.max(1,size*.03);ctx.globalAlpha=.9;ctx.beginPath();ctx.arc(s.x,s.y,size*1.35,-Math.PI/2,-Math.PI/2+Math.PI*2*p);ctx.stroke();ctx.beginPath();ctx.moveTo(s.x,s.y-size*(1.2-p));ctx.lineTo(s.x,s.y-size*.14);ctx.stroke();ctx.fillStyle='#e2ffbd';ctx.beginPath();ctx.arc(s.x,s.y-size*(1.2-p),size*.08,0,Math.PI*2);ctx.fill();count++}ctx.restore();continue}
        const cells=effect.size||1;for(let dy=0;dy<cells&&count<this.budget.fields;dy++)for(let dx=0;dx<cells&&count<this.budget.fields;dx++){const x=effect.x+dx,y=effect.y+dy;if(!visible(x,y)||!inViewport(x,y))continue;const s=worldToScreen(x,y),age=time-effect.startAt,seed=x*73+y*137;ctx.save();ctx.beginPath();ctx.rect(s.x,s.y,size,size);ctx.clip();ctx.strokeStyle=effect.kind==='psionicStorm'?'#d3edff':'#bbff8d';ctx.lineWidth=Math.max(1,size*.025);if(effect.kind==='psionicStorm'){for(let i=0;i<this.budget.detail;i++){const phase=Math.floor(age/95),bx=s.x+noise(seed+phase,i)*size,by=s.y+noise(seed+phase,i+4)*size;ctx.globalAlpha=.45+.35*noise(seed+phase,i+9);ctx.beginPath();ctx.moveTo(bx,by-size*.3);ctx.lineTo(bx-size*.09,by);ctx.lineTo(bx+size*.06,by+size*.14);ctx.lineTo(bx-size*.1,by+size*.4);ctx.moveTo(bx-size*.09,by);ctx.lineTo(bx+size*.18,by-size*.08);ctx.lineTo(bx+size*.23,by+size*.06);ctx.stroke()}}
          else if(effect.kind==='bileWarning'&&!effect.resolved&&time<effect.impactAt){const p=clamp(age/Math.max(1,effect.impactAt-effect.startAt),0,1),center=worldToScreen(effect.x+.5,effect.y+.5);ctx.globalAlpha=.8;ctx.beginPath();ctx.arc(center.x,center.y,size*.58,-Math.PI/2,-Math.PI/2+Math.PI*2*p);ctx.stroke();ctx.setLineDash([3,4]);ctx.beginPath();ctx.moveTo(center.x,center.y-size*(1.2-p));ctx.lineTo(center.x,center.y-size*.14);ctx.stroke();ctx.setLineDash([]);ctx.fillStyle='#cdff9e';ctx.beginPath();ctx.arc(center.x,center.y-size*(1.2-p),size*.07,0,Math.PI*2);ctx.fill()}
          else{const growth=clamp(age/240,0,1);for(let i=0;i<this.budget.detail;i++){const a=i*2.399+age/1400,r=(.12+noise(seed,i)*.35)*growth,sx=s.x+size*(.5+Math.cos(a)*r),sy=s.y+size*(.5+Math.sin(a)*r);ctx.globalAlpha=.2+.2*Math.sin(age/150+i);ctx.fillStyle=effect.kind==='fungalBulb'?'#b2ff8b':'#d0ff92';ctx.beginPath();ctx.arc(sx,sy,size*(.025+noise(seed,i+5)*.05),0,Math.PI*2);ctx.fill()}}
          ctx.restore();count++;}}
      for(const obstacle of obstacles){if(count>=this.budget.fields)break;if(obstacle.endAt<=time||obstacle.hp<=0||!visible(obstacle.x,obstacle.y)||!inViewport(obstacle.x,obstacle.y))continue;const s=worldToScreen(obstacle.x,obstacle.y),age=time-obstacle.startAt,growth=clamp(age/380,0,1);ctx.save();ctx.beginPath();ctx.rect(s.x,s.y,size,size);ctx.clip();ctx.strokeStyle=obstacle.kind==='forcefield'?'#e2c7ff':'#bcecff';ctx.lineWidth=Math.max(1,size*.022);ctx.globalAlpha=(1-growth)*.8+.2;const lines=this.quality==='low'?2:4;for(let i=0;i<lines;i++){const yy=s.y+size*((time/1700+i/lines)%1);ctx.beginPath();ctx.moveTo(s.x+size*.12,yy);ctx.lineTo(s.x+size*.88,yy);ctx.stroke()}if(growth<1){ctx.globalAlpha=1-growth;ctx.strokeRect(s.x+size*(.5-growth*.4),s.y+size*(.5-growth*.4),size*growth*.8,size*growth*.8)}ctx.restore();count++}return count;
    }
    stats(){return{events:this.events.length,eventLimit:this.budget.events,particles:this.particles.length,particleLimit:this.budget.particles,pooledEvents:this.freeEvents.length,pooledParticles:this.freeParticles.length,observed:this.observed.size,remembered:this.seen.size,quality:this.quality}}
  }
  class SpriteTintCache{
    constructor({maxBytes=12*1024*1024,createCanvas}={}){this.maxBytes=Math.max(4,maxBytes);this.createCanvas=createCanvas||(()=>typeof document!=='undefined'?document.createElement('canvas'):typeof OffscreenCanvas!=='undefined'?new OffscreenCanvas(1,1):null);this.sources=new WeakMap();this.entries=new Set();this.bytes=0}
    frame(image,{sx=0,sy=0,sw=image?.naturalWidth||image?.width,sh=image?.naturalHeight||image?.height,tint}={}){
      if(!image||!tint||![sx,sy,sw,sh].every(Number.isFinite)||sw<=0||sh<=0)return null;
      let frames=this.sources.get(image);if(!frames){frames=new Map();this.sources.set(image,frames)}
      const key=`${sx}:${sy}:${sw}:${sh}:${tint}`,cached=frames.get(key);if(cached){this.entries.delete(cached);this.entries.add(cached);return cached.image}
      const canvas=this.createCanvas();if(!canvas)return null;const padding=sx||sy||sw<(image.naturalWidth||image.width)||sh<(image.naturalHeight||image.height)?1:0,width=sw+padding*2,height=sh+padding*2,scale=Math.min(1,256/Math.max(width,height),Math.sqrt(this.maxBytes/(4*width*height)));canvas.width=Math.max(1,Math.floor(width*scale));canvas.height=Math.max(1,Math.floor(height*scale));const context=canvas.getContext('2d');if(!context)return null;
      // Keep one sample outside a frame for the same edge interpolation as the
      // original atlas. The extra sample is excluded from its painted bounds.
      context.drawImage(image,sx-padding,sy-padding,width,height,0,0,canvas.width,canvas.height);canvas.spriteTintCrop=[padding*canvas.width/width,padding*canvas.height/height,sw*canvas.width/width,sh*canvas.height/height];
      // Source-atop preserves every transparent edge, even on Cloudflare images
      // without pixel-read access. A light overlay keeps faction detail visible.
      context.globalCompositeOperation='source-atop';context.globalAlpha=.3;context.fillStyle=tint;context.fillRect(0,0,canvas.width,canvas.height);context.globalCompositeOperation='source-over';context.globalAlpha=1;
      const entry={image:canvas,frames,key,bytes:canvas.width*canvas.height*4};frames.set(key,entry);this.entries.add(entry);this.bytes+=entry.bytes;this.evict();return canvas;
    }
    evict(){for(const entry of this.entries){if(this.bytes<=this.maxBytes)break;this.entries.delete(entry);entry.frames.delete(entry.key);this.bytes-=entry.bytes;entry.image.width=0;entry.image.height=0}}
    setBudget(bytes){this.maxBytes=Math.max(4,bytes);this.evict()}
    clear(){for(const entry of this.entries){entry.image.width=0;entry.image.height=0}this.sources=new WeakMap();this.entries.clear();this.bytes=0}
    stats(){return{bytes:this.bytes,limit:this.maxBytes,frames:this.entries.size}}
  }
  class TextureCache{
    constructor({baseUrl='',maxBytes=QUALITY.medium.textureBytes,concurrency=2,createImage,createCanvas}={}){this.baseUrl=baseUrl;this.maxBytes=maxBytes;this.concurrency=concurrency;this.createImage=createImage||(()=>new Image());this.createCanvas=createCanvas||(()=>typeof document!=='undefined'?document.createElement('canvas'):typeof OffscreenCanvas!=='undefined'?new OffscreenCanvas(1,1):null);this.entries=new Map();this.queue=[];this.active=0;this.bytes=0;this.counter=0;this.pinned=new Set();this.resolution=1;this.generation=0}
    resolve(file){try{return new URL(file,this.baseUrl||(typeof location!=='undefined'?location.href:'http://localhost/')).href}catch{return file}}
    beginFrame(){this.pinned.clear()}
    setCropRepairs(repairs){if(this.cropRepairs===repairs)return;this.cropRepairs=repairs;this.generation++;for(const entry of this.entries.values())this.dispose(entry.image);this.entries.clear();this.queue.length=0;this.pinned.clear();this.bytes=0}
    setLocalFallbacks(fallbacks){this.localFallbacks=fallbacks||{}}
    repairAtlas(image,repair){
      // Generated poses do not always respect their nominal grid. Repack the
      // measured crops once, with empty gutters, before any quality downsample.
      // Drawing is allowed for cross-origin images; no canvas pixel reads occur.
      const canvas=this.createCanvas();if(!canvas)throw new Error('Atlas repair needs a canvas');
      canvas.width=repair.width;canvas.height=repair.height;const context=canvas.getContext('2d');if(!context)throw new Error('Atlas repair needs a 2D context');
      const width=image.naturalWidth||image.width,height=image.naturalHeight||image.height;
      for(let row=0;row<repair.crops.length;row++)for(let col=0;col<repair.crops[row].length;col++){
        const [x,y,w,h]=repair.crops[row][col],offset=repair.destinationOffsets?.[row]?.[col]||[(repair.frameWidth-w)/2,y-row*repair.frameHeight],dx=offset[0],dy=offset[1];
        if(![x,y,w,h,dx,dy].every(Number.isFinite)||x<0||y<0||w<=0||h<=0||x+w>width||y+h>height||dx<0||dy<0||dx+w>repair.frameWidth||dy+h>repair.frameHeight)throw new Error('Invalid measured sprite crop');
        const left=col*repair.frameWidth+dx,top=row*repair.frameHeight+dy;
        context.drawImage(image,x,y,w,h,left,top,w,h);
        // A few disconnected neighbor scraps lie inside a body's bounding box.
        for(const [ex,ey,ew,eh]of repair.erasures?.[`${row},${col}`]||[])context.clearRect(left+ex,top+ey,ew,eh);
      }
      return canvas;
    }
    frameExpansion(file){return this.entries.get(file)?.frameExpansion||1}
    frameExpansionY(file){return this.entries.get(file)?.frameExpansionY||1}
    releaseOriginal(image){image.onload=null;image.onerror=null;if(typeof HTMLImageElement!=='undefined'&&image instanceof HTMLImageElement)image.src=''}
    dispose(image){if(!image)return;if(typeof image.close==='function')image.close();else if(typeof image.getContext==='function'){image.width=0;image.height=0}else this.releaseOriginal(image)}
    get(file,{priority=false}={}){if(!file)return null;let entry=this.entries.get(file);if(!entry||entry.status==='evicted'&&Date.now()>=entry.retryAt){entry={status:'queued',last:++this.counter,bytes:0};this.entries.set(file,entry);if(priority)this.queue.unshift(file);else this.queue.push(file);this.pump()}else if(priority&&entry.status==='queued'){const index=this.queue.indexOf(file);if(index>=0){this.queue.splice(index,1);this.queue.unshift(file)}}entry.last=++this.counter;this.pinned.add(file);return entry.status==='ready'?entry.image:null}
    pump(){while(this.active<this.concurrency&&this.queue.length){const file=this.queue.shift(),entry=this.entries.get(file);if(!entry||entry.status!=='queued')continue;const generation=this.generation,resolution=this.resolution;entry.status='loading';this.active++;let image;try{image=this.createImage()}catch{entry.status='failed';this.active--;continue}image.decoding='async';let done=false;const finish=ok=>{if(done)return;done=true;clearTimeout(timer);image.onload=null;image.onerror=null;this.active--;if(generation!==this.generation||this.entries.get(file)!==entry){this.releaseOriginal(image);this.pump();return}if(ok){let output=image,sourceScale=1;const repair=this.cropRepairs?.[file];if(repair){try{output=this.repairAtlas(image,repair);entry.frameExpansion=repair.frameWidth/repair.sourceFrameWidth;entry.frameExpansionY=repair.frameHeight/(repair.sourceFrameHeight||repair.frameHeight)}catch{entry.status='failed';this.releaseOriginal(image);this.pump();return}}const width=finite(output.naturalWidth||output.width),height=finite(output.naturalHeight||output.height);if(resolution<1){try{const canvas=this.createCanvas();if(canvas){canvas.width=Math.max(1,Math.round(width*resolution));canvas.height=Math.max(1,Math.round(height*resolution));const context=canvas.getContext('2d');if(context){context.imageSmoothingEnabled=true;context.imageSmoothingQuality='high';context.drawImage(output,0,0,canvas.width,canvas.height);if(output!==image)this.dispose(output);output=canvas;sourceScale=resolution}}}catch{/* Original artwork remains a safe fallback when a canvas is unavailable. */}}entry.image=output;entry.sourceScale=sourceScale;entry.status='ready';entry.bytes=finite(output.naturalWidth||output.width)*finite(output.naturalHeight||output.height)*4;this.bytes+=entry.bytes;if(output!==image)this.releaseOriginal(image)}else{entry.status='failed';this.releaseOriginal(image)}this.evict();this.pump()};const timer=setTimeout(()=>finish(false),12000);image.onload=()=>finish(true);const local=this.localFallbacks?.[file];let usingLocal=!!local;image.onerror=()=>{if(usingLocal){usingLocal=false;image.src=this.resolve(file)}else finish(false)};image.src=this.resolve(local||file)}}
    evict(){if(this.bytes<=this.maxBytes)return;const candidates=[...this.entries].filter(([,e])=>e.status==='ready').sort((a,b)=>Number(this.pinned.has(a[0]))-Number(this.pinned.has(b[0]))||a[1].last-b[1].last);for(const [file,entry]of candidates){if(this.bytes<=this.maxBytes)break;this.bytes-=entry.bytes;this.dispose(entry.image);this.entries.set(file,{status:'evicted',last:entry.last,bytes:0,retryAt:Date.now()+3000})}}
    setBudget(bytes){this.maxBytes=bytes;this.evict()}
    setResolution(scale){const next=clamp(finite(scale,1),.25,1);if(next===this.resolution)return;this.resolution=next;this.generation++;for(const entry of this.entries.values())this.dispose(entry.image);this.entries.clear();this.queue.length=0;this.pinned.clear();this.bytes=0}
    sourceScale(file){return this.entries.get(file)?.sourceScale||1}
    stats(){return{bytes:this.bytes,limit:this.maxBytes,resolution:this.resolution,ready:[...this.entries.values()].filter(e=>e.status==='ready').length,pending:this.queue.length+this.active}}
  }
  class SpriteRuntime{
    constructor({manifest=null,baseUrl='',quality='medium',cache,tintCache}={}){this.manifest=manifest;this.cache=cache||new TextureCache({baseUrl});this.tintCache=tintCache||new SpriteTintCache();this.cache.setCropRepairs?.(manifest?.cropRepairs);this.cache.setLocalFallbacks?.(manifest?.localFallbacks);this.states=new Map();this.ghosts=[];this.ghostIds=new Set();this.time=0;this.entityIndex=new Map();this.combat=new CombatEffects(quality);this.setQuality(quality)}
    setQuality(value){this.quality=Object.hasOwn(QUALITY,value)?value:'medium';this.budget=QUALITY[this.quality];this.combat?.setQuality(this.quality);this.cache.setBudget(this.budget.textureBytes);this.cache.setResolution(this.budget.textureScale);this.tintCache.setBudget(this.budget.textureBytes/8);while(this.ghosts.length>this.budget.ghosts){this.ghostIds.delete(this.ghosts.shift().id)}}
    async loadManifest(url){const controller=typeof AbortController!=='undefined'?new AbortController():null,timer=controller?setTimeout(()=>controller.abort(),6000):null;try{const response=await fetch(url,controller?{signal:controller.signal}:undefined);if(!response.ok)return false;const data=await response.json();if(!data||!data.assets)return false;this.manifest=data;this.cache.setCropRepairs?.(data.cropRepairs);this.cache.setLocalFallbacks?.(data.localFallbacks);return true}catch{return false}finally{if(timer)clearTimeout(timer)}}
    reset(){this.states.clear();this.ghosts.length=0;this.ghostIds.clear();this.entityIndex.clear();this.tintCache.clear();this.combat.reset()}
    ensure(entity,time){let state=this.states.get(entity.id);if(!state||state.type!==entity.type||state.kind!==entity.kind){state={id:entity.id,type:entity.type,assetType:assetTypeFor(entity),kind:entity.kind,faction:entity.faction,x:finite(entity.x),y:finite(entity.y),direction:'S',state:'idle',stateAt:time,phaseAt:time,event:null,segment:null,logicalX:entity.x,logicalY:entity.y,visible:false,lastMoveReady:finite(entity.moveReadyAt),lastAttackReady:finite(entity.attackReadyAt),lastHit:finite(entity.lastDamagedAt),lastAbility:finite(entity.abilityReadyAt),lastCast:entity.lastAbilityCastAt,lastDeploy:false};this.states.set(entity.id,state)}return state}
    turn(state,dx,dy,time){const next=directionForVector(dx,dy,state.direction);if(next!==state.direction){state.previousDirection=state.direction;state.turnAt=time;state.direction=next}}
    route(entity,path){const state=this.ensure(entity,this.time);state.pendingPath=path?.slice(0,16).map(p=>({x:p.x,y:p.y}))}
    move(entity,from,time,duration,{teleport=false,remote=false,maxDistance=8,path}={}){
      const state=this.ensure(entity,time),current=sampleSegment(state.segment,time)||{x:state.x,y:state.y},distance=Math.hypot(entity.x-current.x,entity.y-current.y);
      if(teleport||distance>maxDistance||entity.kind!=='unit'){state.segment=null;state.pendingPath=null;state.moving=false;state.x=entity.x;state.y=entity.y;state.arrivedAt=time;state.appearAt=teleport?time:0;if(teleport){entity.px=entity.x;entity.py=entity.y}}
      else if(distance>.001){const remaining=finite(entity.moveReadyAt)-time;const interval=remote?(remaining>0?Math.min(duration,remaining):Math.min(300,duration)):duration;state.segment={fromX:from?.x??current.x,fromY:from?.y??current.y,toX:entity.x,toY:entity.y,start:time,end:time+Math.max(90,interval)};const route=path||state.pendingPath;state.pendingPath=null;if(route?.length){const end=route.findIndex(p=>p.x===entity.x&&p.y===entity.y);if(end>=0){state.segment.points=[{x:state.segment.fromX,y:state.segment.fromY},...route.slice(0,end+1)];state.segment.length=state.segment.points.reduce((sum,p,i,points)=>i?sum+Math.hypot(p.x-points[i-1].x,p.y-points[i-1].y):sum,0)}}state.moveDistance=state.segment.length||distance;state.moveDuration=interval;if(state.state!=='move')state.phaseAt=time}
      if(state.segment){this.turn(state,state.segment.points?.[1]?state.segment.points[1].x-state.segment.fromX:entity.x-state.segment.fromX,state.segment.points?.[1]?state.segment.points[1].y-state.segment.fromY:entity.y-state.segment.fromY,time)}state.logicalX=entity.x;state.logicalY=entity.y;state.lastMoveReady=finite(entity.moveReadyAt);return state;
    }
    signal(entity,kind,time,{target,origin,duration,contactAt,visible=true}={}){
      if(!entity||!visible)return;
      const state=this.ensure(entity,time),definition=this.manifest?.assets?.[state.assetType]?.states?.[kind]||this.manifest?.assets?.[state.assetType]?.states?.[kind==='ability'?'special':kind];
      if(definition?.sheet)this.cache.get(definition.sheet,{priority:true});
      const natural=Math.max(100,(finite(definition?.frames,6)*1000/Math.max(1,finite(definition?.fps,10))));
      // Impact/reaction is short; attacks retain the already authoritative projectile/contact clock.
      const length=duration??(kind==='hit'?180:kind==='attack'?520:natural);
      if(kind==='hit'&&state.event?.kind==='attack'&&state.event.end>time){state.hitAt=time;return}
      state.event={kind,start:time,end:time+length,contactAt};if(target)this.turn(state,target.x-(origin?.x??entity.x),target.y-(origin?.y??entity.y),time);
      state.stateAt=time;
    }
    kill(entity,time,visible){
      if(!entity||!visible||entity.garrisonedIn||this.ghostIds.has(entity.id))return false;
      const state=this.ensure(entity,time),position=sampleSegment(state.segment,time)||state,death=this.manifest?.assets?.[state.assetType]?.states?.death,duration=clamp(finite(death?.frames,8)*1000/Math.max(1,finite(death?.fps,10)),350,1800);
      this.combat.death(entity,time,visible);
      if(death?.sheet)this.cache.get(death.sheet,{priority:true});
      const ghost={id:entity.id,type:state.assetType,kind:entity.kind,faction:entity.faction,x:position.x,y:position.y,logicalX:entity.x,logicalY:entity.y,direction:state.direction,start:time,end:time+duration};
      this.ghosts.push(ghost);this.ghostIds.add(entity.id);while(this.ghosts.length>this.budget.ghosts)this.ghostIds.delete(this.ghosts.shift().id);this.states.delete(entity.id);return true;
    }
    beginFrame(entities,time,{isVisible=()=>true,moveDuration=()=>600,maxDistance=()=>8,workTarget=()=>null,armored=()=>false,remote=false}={}){
      this.time=time;this.cache.beginFrame();this.entityIndex=new Map(entities.filter(e=>e.alive!==false).map(e=>[e.id,e]));
      const alive=new Set();
      for(const entity of entities){if(entity.alive===false)continue;alive.add(entity.id);const state=this.ensure(entity,time),visible=isVisible(entity);state.visible=visible;
        if(entity.garrisonedIn){state.segment=null;state.x=entity.x;state.y=entity.y;state.logicalX=entity.x;state.logicalY=entity.y;continue}
        if(state.logicalX!==entity.x||state.logicalY!==entity.y){const reset=entity.px===entity.x&&entity.py===entity.y,blink=entity.lastAbilityId==='phaseStep'&&abilityWasCast(entity,{ability:state.lastAbility,cast:state.lastCast},time);this.move(entity,null,time,Math.max(90,moveDuration(entity)),{teleport:reset||blink,remote,maxDistance:maxDistance(entity)})}
        const sample=sampleSegment(state.segment,time);if(sample){state.x=sample.x;state.y=sample.y;state.moving=sample.moving;if(!sample.moving)state.segment=null}else state.moving=false;
        // Reuse existing cosmetic coordinates; serializer already explicitly removes them.
        entity.px=state.x;entity.py=state.y;
        this.combat.observe(entity,time,{visible,armored:armored(entity)});
        if(visible){
          const shot=entity.projectileAnim,melee=entity.attackAnim,event=shot?.endAt>time?shot:melee?.endAt>time?melee:null;
          if(finite(entity.attackReadyAt)>state.lastAttackReady&&event){this.signal(entity,'attack',time,{target:{x:event.targetX??event.tx-.5,y:event.targetY??event.ty-.5},duration:Math.max(90,event.endAt-time),contactAt:shot?.startAt??event.startAt+270});state.event.start=event.startAt;state.event.end=event.endAt}
          if(finite(entity.lastDamagedAt)>state.lastHit&&time-finite(entity.lastDamagedAt)<350)this.signal(entity,'hit',finite(entity.lastDamagedAt),{duration:180});
          if(abilityWasCast(entity,{ability:state.lastAbility,cast:state.lastCast},time))this.signal(entity,'ability',time,{duration:700});
        }
        state.lastAttackReady=finite(entity.attackReadyAt);state.lastHit=finite(entity.lastDamagedAt);state.lastAbility=finite(entity.abilityReadyAt);state.lastCast=entity.lastAbilityCastAt;
        const deploying=!!(entity.fabricatorDeployAt||entity.tunnelDigging||entity.siegeMode||entity.burrowedUntil>time);
        if(visible&&deploying&&!state.lastDeploy)this.signal(entity,'deploy',time,{duration:600});state.lastDeploy=deploying;
        const target=visible?workTarget(entity):null;state.workTarget=target;
        let next=entity.complete===false?'construction':state.event?.end>time?state.event.kind:entity.kind==='cocoon'?'morph':deploying&&(entity.fabricatorDeployAt||entity.tunnelDigging)?'deploy':state.moving?'move':target?'work':entity.queue?.length?'production':'idle';
        if(entity.channelEffectId||entity.warpChannel)next='ability';
        if(target&&!state.moving&&next==='work')this.turn(state,target.x-entity.x,target.y-entity.y,time);
        if(state.moving&&next==='move'&&state.segment)this.turn(state,sample?.dx??state.segment.toX-state.x,sample?.dy??state.segment.toY-state.y,time);
        if(next!==state.state){state.state=next;state.stateAt=time;if(next==='move')state.phaseAt=time}
      }
      this.updateHealing(entities,time,isVisible);
      for(const [id,state]of this.states)if(!alive.has(id))this.states.delete(id);
      this.ghosts=this.ghosts.filter(g=>{if(g.end>time&&!alive.has(g.id))return true;this.ghostIds.delete(g.id);return false});this.combat.update(time,alive);this.cache.evict();
    }

    updateHealing(entities,time,isVisible){
      for(const entity of entities){if(entity.type!=='medic'||entity.alive===false)continue;const state=this.states.get(entity.id);if(!state)continue;
        const healing=healingPresentation(entity,this.entityIndex,time,isVisible);
        if(healing){
          state.healing=healing;state.healCompletion=null;state.state=healing.phase;
          state.stateAt=healing.start+(healing.phase==='healActivate'?HEAL_PHASES.prepare:healing.phase==='healLoop'?HEAL_PHASES.prepare+HEAL_PHASES.activate:0);
          const target=healing.target;this.turn(state,(target.px??target.x)-(entity.px??entity.x),(target.py??target.y)-(entity.py??entity.y),time);
          const sheet=this.manifest?.assets?.medic?.states?.[healing.phase]?.sheet;if(sheet)this.cache.get(sheet,{priority:true});
        }else{
          if(state.healing&&state.healing.end<=time&&!entity.garrisonedIn&&!state.moving&&!(state.event?.end>time))state.healCompletion={start:entity.healUntil,end:entity.healUntil+HEAL_PHASES.complete};
          state.healing=null;
          if(state.healCompletion?.end>time&&!state.moving&&!entity.garrisonedIn&&!(state.event?.end>time)){state.state='healComplete';state.stateAt=state.healCompletion.start}else state.healCompletion=null;
        }
      }
    }
    healingEndpoints(entity,time,{size=48,zoom=1,worldToScreen=(x,y)=>({x,y})}={}){
      const state=this.states.get(entity.id),healing=state?.healing;if(!healing?.beam||!healingPresentation(entity,this.entityIndex,time,e=>this.states.get(e.id)?.visible!==false))return null;
      const frequency=zoom<.55?8:this.budget.fps,elapsed=Math.floor(Math.max(0,time-state.stateAt)/(1000/frequency))*(1000/frequency);
      const definition=this.metadata('medic',state.state),frame=frameForState(definition?.state,state.direction,elapsed),asset=definition?.asset;
      const device=asset?.healing?.deviceAnchors?.[state.direction]?.[(definition?.state?.firstColumn||0)+(frame?.index||0)]||[64,86],scale=finite(definition?.state?.scale,1),anchor=definition?.state?.anchor||asset?.anchor||[.5,.5];
      const body=worldToScreen((entity.px??entity.x)+.5,(entity.py??entity.y)+.5),target=healing.target,impact=worldToScreen((target.px??target.x)+.5,(target.py??target.y)+.5);
      return{origin:{x:body.x+(device[0]/128-anchor[0])*size*scale,y:body.y+size*((asset?.anchor?.[1]||.5)-.5)+(device[1]/160-anchor[1])*size*scale*160/128},target:{x:impact.x,y:impact.y-size*.22},healing};
    }
    drawHealingEffects(ctx,time,{size=48,zoom=1,worldToScreen,visible=()=>true,inViewport=()=>true}={}){
      const effect=this.manifest?.effects?.medicHealing;if(!effect)return 0;
      let count=0,limit=this.quality==='low'?12:this.quality==='high'?64:32;
      for(const entity of this.entityIndex.values()){
        if(count>=limit)break;if(entity.type!=='medic'||!visible(entity))continue;
        const endpoints=this.healingEndpoints(entity,time,{size,zoom,worldToScreen});if(!endpoints||!visible(endpoints.healing.target)||!inViewport(entity)&&!inViewport(endpoints.healing.target))continue;
        const {origin,target,healing}=endpoints,dx=target.x-origin.x,dy=target.y-origin.y,length=Math.hypot(dx,dy);if(length<2)continue;
        const image=this.cache.get(effect.sheet,{priority:true}),sourceScale=this.cache.sourceScale(effect.sheet),frame=Math.floor((time-healing.start)/70)%8,thickness=Math.max(7,size*.24);
        ctx.save();ctx.lineCap='round';
        if(image){
          ctx.translate(origin.x,origin.y);ctx.rotate(Math.atan2(dy,dx));
          ctx.drawImage(image,(frame*128+4)*sourceScale,40*sourceScale,120*sourceScale,48*sourceScale,0,-thickness/2,length,thickness);
        }else{
          ctx.strokeStyle='rgba(123,246,173,.35)';ctx.lineWidth=Math.max(3,size*.08);ctx.beginPath();ctx.moveTo(origin.x,origin.y);ctx.lineTo(target.x,target.y);ctx.stroke();ctx.strokeStyle='#edfff3';ctx.lineWidth=Math.max(1,size*.025);ctx.stroke();
        }
        ctx.restore();
        if(image){ctx.save();ctx.globalAlpha*=.65;const impactSize=size*(this.quality==='low'?.4:.58);ctx.drawImage(image,frame*128*sourceScale,128*sourceScale,128*sourceScale,128*sourceScale,target.x-impactSize/2,target.y-impactSize*.62,impactSize,impactSize);ctx.restore()}
        const density=this.quality==='low'?1:this.quality==='high'?5:3;
        ctx.save();ctx.fillStyle='#ddffeb';
        for(let i=0;i<density;i++){const pulse=((time-healing.start)/650+i/density)%1,x=origin.x+dx*pulse,y=origin.y+dy*pulse;ctx.globalAlpha=.55+.4*Math.sin(pulse*Math.PI);ctx.beginPath();ctx.arc(x,y,Math.max(1,size*.025)*(1-pulse*.3),0,Math.PI*2);ctx.fill()}
        ctx.restore();count++;
      }
      return count;
    }
    metadata(type,state){const asset=this.manifest?.assets?.[type];if(!asset)return null;const aliases={ability:'special',deploy:'ability',production:'work',construction:'deploy',morph:'construction'};return{asset,state:asset.states?.[state]||asset.states?.[aliases[state]]||asset.states?.idle}}
    drawState(ctx,type,state,{elapsedMs=0,progress,direction='S',size=48,x=0,y=0,rate=1,alpha=1,tint=null}={}){
      const meta=this.metadata(type,state);if(!meta?.state?.sheet)return false;const image=this.cache.get(meta.state.sheet);if(!image)return false;
      const frame=frameForState(meta.state,direction,elapsedMs,{progress,rate}),anchor=meta.state.anchor||meta.asset.anchor||[.5,.5],scale=finite(meta.state.scale,finite(meta.asset.scale,1)),width=size*scale,height=width*frame.h/frame.w;
      const sourceScale=this.cache.sourceScale(meta.state.sheet),expansion=this.cache.frameExpansion?.(meta.state.sheet)||1,expansionY=this.cache.frameExpansionY?.(meta.state.sheet)||1,sx=frame.x*expansion*sourceScale,sy=frame.y*expansionY*sourceScale,sw=frame.w*expansion*sourceScale,sh=frame.h*expansionY*sourceScale,drawWidth=width*expansion,drawHeight=height*expansionY;
      if(sx+sw>(image.naturalWidth||image.width)+1||sy+sh>(image.naturalHeight||image.height)+1)return false;
      // Extra transparent padding surrounds the original cell. Keep its pivot,
      // native pixel scale and logical hit bounds while isolating drifted rows.
      const top=y-height*anchor[1]-(drawHeight-height)/2;
      const colored=this.tintCache.frame(image,{sx,sy,sw,sh,tint}),source=colored||image,crop=colored?colored.spriteTintCrop:[sx,sy,sw,sh];
      ctx.save();ctx.globalAlpha*=alpha;if(meta.state.directionFlipX?.[frame.direction]===true){ctx.translate(x,y);ctx.scale(-1,1);ctx.drawImage(source,...crop,-drawWidth*(1-anchor[0]),top-y,drawWidth,drawHeight)}else ctx.drawImage(source,...crop,x-drawWidth*anchor[0],top,drawWidth,drawHeight);ctx.restore();return true;
    }
    bounds(entity,{size=48}={}){const state=this.ensure(entity,this.time),meta=this.metadata(state.assetType,state.state);if(!meta?.state)return null;const frame=frameForState(meta.state,state.direction,0),anchor=meta.state.anchor||meta.asset.anchor||[.5,.5],assetAnchor=meta.asset.anchor||[.5,.5],scale=finite(meta.state.scale,finite(meta.asset.scale,1)),width=size*scale,height=width*frame.h/frame.w;return{x:-width*anchor[0],y:size*(assetAnchor[1]-.5)-height*anchor[1],width,height}}
    draw(ctx,entity,time,{size=48,zoom=1,constructionProgress=1,morphProgress=0,tint=null}={}){
      const state=this.ensure(entity,time);let elapsed=time-(state.state==='healComplete'?state.healCompletion?.start??state.stateAt:state.state==='move'?state.phaseAt:state.event?.kind===state.state?state.event.start:state.stateAt),rate=1;
      if(state.state==='move'){const meta=this.metadata(state.assetType,'move');const cycle=Math.max(100,finite(meta?.state?.frames,8)*1000/Math.max(1,finite(meta?.state?.fps,10)));rate=clamp(cycle/Math.max(100,state.moveDuration/Math.max(1,state.moveDistance)),.55,2.8)}
      if(state.state==='idle')elapsed+=finite(entity.id)*137;
      const frequency=zoom<.55?8:this.budget.fps;elapsed=Math.floor(Math.max(0,elapsed)/(1000/frequency))*(1000/frequency);
      const progress=state.state==='construction'?constructionProgress:state.state==='morph'?morphProgress:state.event?.kind===state.state&&state.event.end>time?eventProgress(this.metadata(state.assetType,state.state)?.state,state.event,time):undefined;
      const anchor=this.manifest?.assets?.[state.assetType]?.anchor||[.5,.5];
      // Discrete facing poses represent one body. Blending two directions can
      // expose two heads/sets of legs while a unit turns, even with isolated crops.
      const alpha=state.appearAt?clamp((time-state.appearAt)/180,.25,1):1,options={elapsedMs:elapsed,progress,size,rate,tint,y:size*(anchor[1]-.5)};
      return this.drawState(ctx,state.assetType,state.state,{...options,direction:state.direction,alpha});
    }
    drawGhosts(ctx,time,{visible=()=>true,worldToScreen,size=48,fallback=()=>false,airOffset=0,tintFor=()=>null}={}){
      for(const ghost of this.ghosts){if(!visible(ghost))continue;const screen=worldToScreen(ghost.x+.5,ghost.y+.5),alpha=clamp((ghost.end-time)/220,0,1);const anchor=this.manifest?.assets?.[ghost.type]?.anchor||[.5,.5];ctx.save();ctx.globalAlpha=alpha;const y=screen.y+size*(anchor[1]-.5)+(this.manifest?.assets?.[ghost.type]?.flying?airOffset:0);if(!this.drawState(ctx,ghost.type,'death',{elapsedMs:time-ghost.start,progress:(time-ghost.start)/(ghost.end-ghost.start),direction:ghost.direction,x:screen.x,y,size,tint:tintFor(ghost)}))fallback(ghost,screen,alpha);ctx.restore()}
    }
    stats(){return{quality:this.quality,entities:this.states.size,ghosts:this.ghosts.length,textures:this.cache.stats(),tints:this.tintCache.stats(),combat:this.combat.stats()}}
  }
  return {DIRECTIONS,QUALITY,COMBAT_QUALITY,ATTACK_FAMILIES,combatFamily,abilityVisual,CombatEffects,floatingLabels,HEAL_PHASES,healingPresentation,assetTypeFor,selectionRenderContext,directionForVector,sampleSegment,frameForState,eventProgress,TextureCache,SpriteTintCache,SpriteRuntime};
});
