import { createMotionTrack, retargetMotion, sampleMotion, sampleProjectile } from './motion.mjs';

export const EFFECT_LIMITS = Object.freeze({ active:128, projectiles:64, particles:48, trails:12, large:4, pool:96 });
export function createQualityGovernor() {
  let quality=2, slow=0, fast=0, average=16.7;
  return {
    sample(ms) {
      if (ms<=0 || ms>200) return quality; // Ignore suspension and idle gaps.
      average=average*0.94+ms*0.06;
      if (average>25) {slow++;fast=0;} else if (average<19) {fast++;slow=0;} else {slow=0;fast=0;}
      if(slow>=45 && quality>0){quality--;slow=0;fast=0;}
      if(fast>=240 && quality<2){quality++;fast=0;slow=0;}
      return quality;
    },
    get level(){return quality;}, get label(){return ['low','medium','high'][quality];}
  };
}

export function createEffectStore({ now = Date.now, limits = EFFECT_LIMITS, release = () => {} } = {}) {
  const effects=new Map();let serial=0;
  const remove = id => {const effect=effects.get(id);if(!effect)return;effects.delete(id);release(effect);};
  const prune = () => {const time=now();for(const [id,e] of effects)if(e.expiresAt<=time)remove(id);};
  return {
    add(effect) {
      prune();
      const category=effect.category || 'cue';
      const cap=category==='particle' ? limits.particles : category==='trail' ? limits.trails : category==='large' ? limits.large : category==='projectile' ? limits.projectiles : limits.active;
      let count=0;for(const e of effects.values())if(e.category===category)count++;
      if(count>=cap) return null;
      if(effects.size>=limits.active) {
        // Essential hits/projectiles can displace decoration. Telegraphs live
        // on their unit or keyed marker, independent of the particle budget.
        const priority=effect.priority ?? (category==='projectile' ? 3 : effect.critical ? 1 : 0);
        const replaceable=[...effects].find(([,e])=>(e.priority ?? (e.category==='projectile' ? 3 : e.critical ? 1 : 0))<priority);
        if(!replaceable) return null;
        remove(replaceable[0]);
      }
      const id=++serial, time=now();
      const value={...effect,id,category,startedAt:time+(effect.delay || 0),expiresAt:time+(effect.delay || 0)+(effect.duration || 400)};
      effects.set(id,value);return value;
    }, remove, prune, clear(){for(const id of effects.keys())remove(id);},
    values:()=>effects.values(), get size(){return effects.size;}
  };
}

const point = p => ({x:p.x,y:p.y});
const colors = {hit:'#ffe6a0',rocket:'#ffac59',tesla:'#b7f3ff',laser:'#ffc3e9',web:'#eef8ff',acorn:'#cdb483',rabbit:'#f3e1ff',slime:'#80dd78',gust:'#d5f4f6',shield:'#b4edff',spawn:'#e6dbc1',death:'#bca78c',claw:'#fff1d9',rally:'#fde59b'};

// The only active animation loop. syncEntities does DOM reconciliation; frame
// only samples existing tracks/effects and changes transforms. No world scans.
export function createCombatPresentation({ document, project, depth, createUnit, now = Date.now,
  requestFrame = requestAnimationFrame, cancelFrame = cancelAnimationFrame,
  hidden = () => document.hidden, inView = () => true, onFrame = () => {}, onObservedHit = () => {},
  reducedMotion = () => false, maxProjectiles = 64 }) {
  const units=new Map(), dying=new Map(), markers=new Map(), pool=[], pulses=new Map();
  const governor=createQualityGovernor();let unitLayer=null,effectLayer=null,frameId=null,lastFrame=0,primed=false;
  let shakeUntil=0,shakePower=0;
  const store=createEffectStore({now,limits:{...EFFECT_LIMITS,projectiles:maxProjectiles},release:e=>{if(!e.node)return;e.node.remove();if(pool.length<EFFECT_LIMITS.pool)pool.push(e.node);}});
  function wake() {if(frameId===null && !hidden()){lastFrame=0;frameId=requestFrame(frame);}}
  function nodeFor(e) {
    if(e.node || !effectLayer)return e.node;
    const node=pool.pop() || document.createElement('div');
    node.className=`cm-fx cm-${e.kind} cm-${e.style || 'hit'}`;
    node.textContent=e.text || '';
    node.style.cssText='';node.style.setProperty('--cm-color',colors[e.style] || colors.hit);
    effectLayer.appendChild(node);e.node=node;return node;
  }
  function add(effect) {const e=store.add(effect);if(e)wake();return e;}
  function burst(position,style='hit',count=6,major=false,delay=0) {
    if(!inView(position))return;
    const quality=governor.level;
    add({kind:'ring',style,...point(position),duration:major?480:240,critical:true,delay});
    if(major && quality)add({kind:'smoke',style,...point(position),duration:600,category:'large',delay});
    const amount=reducedMotion()?Math.min(2,count):Math.min(count,[1,3,7][quality]);
    for(let i=0;i<amount;i++) {
      const angle=(i/Math.max(1,amount))*Math.PI*2;
      add({kind:'particle',style,...point(position),to:{x:position.x+Math.cos(angle)*0.5,y:position.y+Math.sin(angle)*0.5},duration:260+i*22,category:'particle',delay});
    }
  }
  function pulseNode(key,node,className='cm-hit',duration=220) {
    if(!node)return;
    const previous=pulses.get(key);if(previous && (previous.node!==node || previous.className!==className))previous.node.classList.remove(previous.className);
    node.classList.add(className);pulses.set(key,{node,className,until:now()+duration});wake();
  }
  function shake(power=1.6) {if(reducedMotion())return;shakePower=Math.min(2.2,Math.max(shakePower,power));shakeUntil=now()+160;wake();}
  function projectile(from,to,style='rabbit',{arc,duration,targetKey}={}) {
    if(!inView(from) && !inView(to))return null;
    const travel=duration ?? Math.min(420,100+(Math.abs(to.x-from.x)+Math.abs(to.y-from.y))*30);
    const shot=add({kind:'projectile',style,category:'projectile',from:point(from),to:point(to),duration:travel,arc:arc ?? (style==='acorn'?16:style==='web'?6:2),critical:true,targetKey});
    if(shot){
      burst(from,style,2);
      if(governor.level && !reducedMotion())add({kind:'trail',style,from:point(from),to:point(to),duration:travel,category:'trail'});
    }
    return shot;
  }
  function beam(from,to,style='tesla',duration=190) {
    if(!inView(from) && !inView(to))return;
    add({kind:'beam',style,category:'projectile',from:point(from),to:point(to),duration,critical:true});
    burst(to,style,3,false,style==='laser'?65:0);
    if(style==='laser')burst(from,style,3);
  }
  function attack(key,to,style='claw',heavy=false) {
    const unit=units.get(key);
    if(unit)pulseNode(`attack:${key}`,unit.node,'cm-attacking',heavy?330:180);
    add({kind:'slash',style,...point(to),duration:240,critical:true});
    burst(to,style,heavy?7:3,heavy);
    if(heavy)shake();
  }
  function statusMarker(key,position,style,until,text='') {
    if(until<=now()) {const old=markers.get(key);old?.node?.remove();markers.delete(key);return;}
    let marker=markers.get(key);
    if(!marker){marker={};markers.set(key,marker);}
    Object.assign(marker,{...point(position),style,until,text});wake();
  }
  function getPosition(key,fallback) {const unit=units.get(key);return unit?point(unit.track.rendered):fallback?point(fallback):null;}
  function syncEntities(entries,{focusKey=null}={}) {
    const time=now(),seen=new Set();
    for(const entry of entries) {
      if(!Number.isFinite(entry.x) || !Number.isFinite(entry.y))continue;
      seen.add(entry.key);let unit=units.get(entry.key);
      if(!unit){
        const node=createUnit(entry);if(!node)continue;
        node.classList.add('cm-unit');node.dataset.unitKey=entry.key;node.dataset.unitType=entry.type;
        unit={node,track:createMotionTrack(entry,time,entry.type),signature:entry.artSignature,health:entry.health,status:''};
        units.set(entry.key,unit);unitLayer?.appendChild(node);
        if(primed && entry.spawnAt && time-entry.spawnAt<4500) {
          if(Number.isFinite(entry.spawnX) && Number.isFinite(entry.spawnY)){
            // Emerge from the producer-facing edge of the open spawn tile.
            // The actual spawn position already passed simulation collision.
            unit.track.from={x:entry.x+(entry.spawnX-entry.x)*0.35,y:entry.y+(entry.spawnY-entry.y)*0.35};
            unit.track.rendered={...unit.track.from};unit.track.duration=300;unit.track.moving=true;
          }
          pulseNode(`spawn:${entry.key}`,node,'cm-spawning',320);
          burst(entry,'spawn',entry.type==='ratking'?7:3,entry.type==='ratking');
        }
      } else if(unit.signature!==entry.artSignature) {
        // Only the sprite changes on facing/asset changes; the motion wrapper,
        // nameplate, health bar and existing animation timeline stay alive.
        const fresh=createUnit(entry),body=fresh?.querySelector('.cm-unit-body');
        if(body){unit.node.querySelector('.cm-unit-body')?.remove();unit.node.prepend(body);}
        unit.signature=entry.artSignature;
      }
      if(Number.isFinite(unit.health) && Number.isFinite(entry.health) && entry.health<unit.health) {
        pulseNode(`hit:${entry.key}`,unit.node);
        burst(unit.track.rendered,'hit',3);
        onObservedHit(entry,unit.health-entry.health);
      }
      unit.health=entry.health;
      unit.trappedUntil=entry.trappedUntil || 0;unit.slowedUntil=entry.slowedUntil || 0;
      retargetMotion(unit.track,entry,time,{local:entry.local,teleport:entry.teleport,duration:entry.duration,speed:entry.speed});
      unit.node.classList.toggle('local-mouse-obscured',!!entry.obscured);
      unit.node.classList.toggle('cm-trapped',entry.trappedUntil>time);
      unit.node.classList.toggle('cm-slowed',entry.slowedUntil>time);
      unit.node.classList.toggle('cm-rally',!!entry.rally);
      if(entry.rally && !unit.rally)burst(entry,'rally',2);unit.rally=entry.rally;
      unit.node.classList.toggle('cm-winding',!!entry.attackIntent);
      if(entry.attackIntent)statusMarker(`windup:${entry.key}`,entry.attackIntent,'warning',entry.attackIntent.executeAt+200,'!');
      else {markers.get(`windup:${entry.key}`)?.node?.remove();markers.delete(`windup:${entry.key}`);}
      let label=unit.node.querySelector('.cm-unit-label');
      if(entry.label){if(!label){label=document.createElement('div');label.className='name cm-unit-label';unit.node.appendChild(label);}label.textContent=entry.label;label.style.color=entry.labelColor || '';}
      else label?.remove();
      let bar=unit.node.querySelector('.cm-unit-health');
      if(entry.type==='cat' && entry.label){if(!bar){bar=document.createElement('div');bar.className='health-bar cm-unit-health';unit.node.appendChild(bar);}bar.style.width=`${Math.max(10,Math.min(100,entry.health/Math.max(1,entry.maxHealth)*100))}%`;}
      else bar?.remove();
      placeUnit(unit,time);
    }
    for(const [key,unit] of units)if(!seen.has(key)) {
      units.delete(key);markers.get(`windup:${key}`)?.node?.remove();markers.delete(`windup:${key}`);
      if(primed && inView(unit.track.rendered) && dying.size<12){
        unit.node.classList.add('cm-dying');dying.set(key,{node:unit.node,until:time+250});burst(unit.track.rendered,'death',4);
      }else unit.node.remove();
    }
    primed=true;
    presentation.focusKey=focusKey;wake();
  }
  function placeUnit(unit,time) {
    const sample=sampleMotion(unit.track,time,reducedMotion()),p=project(sample.x,sample.y);
    const node=unit.node;
    node.style.left='0px';node.style.top='0px';
    const transform=`translate3d(${p.x.toFixed(2)}px,${p.y.toFixed(2)}px,0) translate(-50%,-50%)`;
    if(unit.transform!==transform){node.style.transform=transform;unit.transform=transform;}
    node.style.zIndex=String(depth(sample.x,sample.y,node.classList.contains('local-mouse-obscured')?17:6));
    node.style.setProperty('--cm-hop',`${sample.lift.toFixed(2)}px`);
    node.classList.toggle('cm-moving',sample.moving);node.classList.toggle('cm-idle',!sample.moving);
    node.classList.toggle('cm-trapped',unit.trappedUntil>time);node.classList.toggle('cm-slowed',unit.slowedUntil>time);
    return sample.moving || unit.trappedUntil>time || unit.slowedUntil>time;
  }
  function placeEffect(e,time) {
    if(time<e.startedAt)return;
    const node=nodeFor(e);if(!node)return;
    const t=Math.max(0,Math.min(1,(time-e.startedAt)/e.duration));
    let x=e.x,y=e.y,lift=0;
    if(e.kind==='projectile' || e.kind==='trail'){
      const sample=sampleProjectile(e,time,reducedMotion());x=sample.x;y=sample.y;lift=sample.lift;
      if(sample.finished && e.kind==='projectile' && !e.impacted){
        e.impacted=true;burst(e.to,e.style,e.style==='rocket'?7:3,e.style==='rocket');
        const target=units.get(e.targetKey);if(target)pulseNode(`hit:${e.targetKey}`,target.node);
        if(e.style==='rocket')shake(2);
      }
    } else if(e.kind==='beam') {
      const a=project(e.from.x,e.from.y),b=project(e.to.x,e.to.y);
      node.style.left=`${a.x}px`;node.style.top=`${a.y-12}px`;
      node.style.width=`${Math.hypot(b.x-a.x,b.y-a.y)}px`;
      node.style.transform=`rotate(${Math.atan2(b.y-a.y,b.x-a.x)}rad)`;
      node.style.opacity=String(1-t);node.style.zIndex=String(depth(e.to.x,e.to.y,14));return;
    } else if(e.kind==='particle') {x += (e.to.x-e.x)*t;y += (e.to.y-e.y)*t;lift=reducedMotion()?0:Math.sin(t*Math.PI)*10;}
    const p=project(x,y);
    node.style.left=`${p.x}px`;node.style.top=`${p.y-lift-10}px`;node.style.zIndex=String(depth(x,y,14));
    node.style.opacity=String(e.kind==='projectile'?1:1-t);
    const scale=reducedMotion()?1:e.kind==='ring'?0.5+t*1.4:e.kind==='smoke'?0.7+t*0.6:1;
    node.style.transform=`translate(-50%,-50%) scale(${scale})`;
    if(e.kind==='projectile') {const a=project(e.from.x,e.from.y),b=project(e.to.x,e.to.y);node.style.transform+=` rotate(${Math.atan2(b.y-a.y,b.x-a.x)}rad)`;}
  }
  function frame() {
    frameId=null;if(hidden()) {lastFrame=0;return;}
    const time=now(),dt=lastFrame?time-lastFrame:16.7;lastFrame=time;governor.sample(dt);
    let active=false;
    for(const unit of units.values())active=placeUnit(unit,time)||active;
    for(const e of [...store.values()])placeEffect(e,time);
    store.prune();
    for(const [key,item] of dying)if(item.until<=time){item.node.remove();dying.delete(key);}
    for(const [key,pulse] of pulses)if(pulse.until<=time){pulse.node.classList.remove(pulse.className);pulses.delete(key);}
    for(const [key,marker] of markers) {
      if(marker.until<=time){marker.node?.remove();markers.delete(key);continue;}
      if(!marker.node && effectLayer){marker.node=document.createElement('div');marker.node.className=`cm-marker cm-${marker.style}`;marker.node.textContent=marker.text;effectLayer.appendChild(marker.node);}
      if(marker.node){const p=project(marker.x,marker.y);marker.node.style.left=`${p.x}px`;marker.node.style.top=`${p.y}px`;marker.node.style.zIndex=String(depth(marker.x,marker.y,15));}
    }
    const shakeRemaining=Math.max(0,(shakeUntil-time)/160);
    const shakeOffset=reducedMotion()?{x:0,y:0}:{x:Math.sin(time*0.15)*shakePower*shakeRemaining,y:Math.cos(time*0.12)*shakePower*shakeRemaining*0.6};
    const cameraActive=onFrame(getPosition(presentation.focusKey),dt,shakeOffset)===true;
    active=active || store.size>0 || dying.size>0 || pulses.size>0 || markers.size>0 || shakeRemaining>0 || cameraActive;
    if(active)frameId=requestFrame(frame);else lastFrame=0;
  }
  const presentation={syncEntities,projectile,beam,burst,attack,statusMarker,pulseNode,shake,getPosition,focusKey:null,
    moveEntity(key,position,options={}){const unit=units.get(key);if(!unit)return false;
      const changed=retargetMotion(unit.track,position,now(),options);if(changed){placeUnit(unit,now());wake();}return changed;},
    attach(nextUnits,nextEffects){unitLayer=nextUnits;effectLayer=nextEffects;for(const u of units.values())unitLayer.appendChild(u.node);wake();},
    suspend(){if(frameId!==null)cancelFrame(frameId);frameId=null;lastFrame=0;},
    resume(){for(const unit of units.values())retargetMotion(unit.track,unit.track.grid,now(),{teleport:true});wake();},
    clear(){this.suspend();store.clear();for(const u of units.values())u.node.remove();for(const u of dying.values())u.node.remove();for(const m of markers.values())m.node?.remove();for(const p of pulses.values())p.node.classList.remove(p.className);units.clear();dying.clear();markers.clear();pulses.clear();primed=false;},
    get stats(){return {units:units.size,effects:store.size,dying:dying.size,markers:markers.size,pooled:pool.length,quality:governor.label,animating:frameId!==null,reducedMotion:reducedMotion()};}
  };
  return presentation;
}
