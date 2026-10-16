import {animationDefinition,frameRectangle,drawSpriteFrame} from "./assets.js";
import {createBattleEffects,UNIT_EFFECTS} from "./effects.js";
import {evolutionFor} from "./evolutions.js";

/** Battle evolution changes artwork, independently of permanent Forge statistics. */
export function unitArtTier(unit){return Math.max(1,Math.min(3,Math.max(Number(unit.forgeTier)||1,Number(unit.battleRank||unit.lvl)||1)));}

/** Visual interpolation never changes logical coordinates or creates network writes. */
export class MotionTrack {
  constructor(x,y,time=0){this.x=x;this.y=y;this.fromX=x;this.fromY=y;this.targetX=x;this.targetY=y;this.startedAt=time;this.duration=320;this.facing=1;}
  sample(time){
    const t=Math.max(0,Math.min(1,(time-this.startedAt)/Math.max(1,this.duration)));
    const ease=t*t*(3-2*t);
    this.x=this.fromX+(this.targetX-this.fromX)*ease;this.y=this.fromY+(this.targetY-this.fromY)*ease;
    return {x:this.x,y:this.y,moving:t<1&&(this.fromX!==this.targetX||this.fromY!==this.targetY),facing:this.facing};
  }
  move(x,y,time,duration=320){
    if(x===this.targetX && y===this.targetY)return;
    this.sample(time);this.fromX=this.x;this.fromY=this.y;this.targetX=x;this.targetY=y;this.startedAt=time;this.duration=Math.min(500,Math.max(100,duration));
    if(Math.abs(x-this.x)>.001)this.facing=x>this.x?1:-1;
  }
}

export function animationFrame(spec,state,elapsed){
  const definition=animationDefinition(spec,state),frames=definition.frames;
  const progress=Math.max(0,elapsed)/Math.max(1,definition.duration);
  const index=definition.loop ? Math.floor(progress*frames.length)%frames.length : Math.min(frames.length-1,Math.floor(progress*frames.length));
  return frames[index];
}

export function resolveAnimation(unit,time,moving=false){
  if(unit.removing || unit.hp<=0)return "death";
  if(unit.actionClass==="casting" || unit.animType==="cast" && unit.animUntil>time)return "cast";
  if(unit.actionClass==="attacking" || unit.animType==="attack" && unit.animUntil>time)return "attack";
  if(unit.hitUntil>time)return "hit";
  return moving ? "move" : "idle";
}

export function unitEffectLabels(unit,time=Date.now()){
  const labels=[];
  if((unit.comboShield||0)>0&&(unit.comboShieldUntil||0)>time||(unit.startShield||0)>0&&(unit.startShieldUntil||0)>time)labels.push('Shielded');
  if((unit.statusPoisonUntil||0)>time)labels.push('Poisoned');
  if((unit.statusAcidUntil||0)>time)labels.push('Corroded');
  if((unit.bindUntil||0)>time)labels.push('Bound');
  if(Math.max(unit.statusShockUntil||0,unit.shockUntil||0)>time)labels.push('Shocked');
  if((unit.woundUntil||0)>time)labels.push('Wounded');
  if(Math.max(unit.suppressedUntil||0,unit.suppressUntil||0,unit.slowUntil||0)>time)labels.push('Slowed');
  return labels;
}

export function createBattleRenderer({layer,assets,cell=64,gap=4,now=()=>performance.now(),troops={},fallbackPath=()=>"",reducedMotion=globalThis.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches || false,...effectOptions}={}){
  const records=new Map(),reactions=new Map();let running=0;
  const clock=now;
  const effects=createBattleEffects({layer,cell,gap,now,reducedMotion,...effectOptions,sizeFor:id=>records.get(id)?.unit.size||troops[records.get(id)?.unit.type]?.size||1,positionFor:id=>{const entry=records.get(id);return entry?.node?entry.motion.sample(clock()):entry?.unit;},artFor:id=>{const entry=records.get(id);return entry?.sheet?{...entry.sheet,frame:animationFrame(entry.sheet.spec,entry.state,clock()-entry.stateAt),flip:entry.motion.facing<0}:null;}});
  function record(unit,time){
    let entry=records.get(unit.id);
    if(!entry){entry={id:unit.id,type:unit.type,tier:unitArtTier(unit),unit:{...unit},motion:new MotionTrack(unit.x,unit.y,time),state:"idle",stateAt:time,frame:null,pending:false,loadAttempted:false,loadVersion:0,sheet:null,canvas:null,host:null,lastHp:unit.targetHp??unit.hp,lastActionUntil:unit.animUntil||0};entry.motion.facing=unit.p===1?-1:1;records.set(unit.id,entry);}
    return entry;
  }
  async function load(entry){
    if(entry.pending || entry.loadAttempted || !assets?.enabled())return;
    entry.pending=true;entry.loadAttempted=true;const key=entry.type,tier=entry.tier,version=entry.loadVersion;
    let sheet=null;
    for(let candidate=tier;candidate>=1&&!sheet;candidate--){try{sheet=await assets.unit(key,candidate);}catch{}}
    if(entry.loadVersion!==version)return;
    entry.pending=false;
    if(entry.type===key && entry.tier===tier){entry.sheet=sheet||entry.sheet;entry.frame=null;}
  }
  function renderUnit(host,unit,time=clock(),options={}){
    if(!host)return false;
    const entry=record(unit,time);entry.unit={...unit};entry.host=host;
    const wallTime=Date.now(),labels=unitEffectLabels(unit,wallTime),evolution=unit.evolution&&evolutionFor(unit.type,unit.evolution);
    const effectStamp=JSON.stringify([unit.type,unit.battleRank||unit.lvl,evolution?.id,labels]);
    if(host._ffEffectStamp!==effectStamp&&host.ownerDocument){
      host._ffEffectStamp=effectStamp;
      let badge=host.querySelector('.ff-unit-upgrade');
      if(evolution&&(unit.battleRank||unit.lvl||1)>=2){
        if(!badge){badge=host.ownerDocument.createElement('span');badge.className='ff-unit-upgrade';host.appendChild(badge);Object.assign(badge.style,{position:'absolute',right:'-5px',top:'-7px',zIndex:'8',padding:'2px 4px',border:'1px solid #dfc270',borderRadius:'3px',background:'#111b27ec',color:'#ffe8a7',font:'700 8px system-ui',lineHeight:'11px',letterSpacing:'.03em',maxWidth:'80px',pointerEvents:'none'});}
        const short=evolution.signature==='burst'?(unit.type==='rifleman'?'BURST':unit.type==='roach'?'ACID ×2':'FOLLOW-UP'):evolution.signature.replaceAll('-',' ').toUpperCase().slice(0,18);
        badge.textContent=`${(unit.battleRank||unit.lvl)>=3?'III':'II'} · ${short}`;
        badge.title=`${evolution.name}: ${evolution.description}`;
      }else badge?.remove();
      host.setAttribute('aria-label',[troops[unit.type]?.name||unit.type,evolution?.name,...labels].filter(Boolean).join(' · '));
    }
    host.classList.toggle("ff-illusion",!!unit.illusion);host.classList.toggle("ff-marked",(unit.markedUntil||0)>Date.now());
    host.classList.toggle("ff-bound",(unit.bindUntil||0)>Date.now());host.classList.toggle("ff-shocked",(unit.statusShockUntil||unit.shockUntil||0)>Date.now());
    host.classList.toggle("ff-suppressed",(unit.suppressedUntil||unit.suppressUntil||0)>Date.now());
    if(!assets?.enabled())return false;
    if(entry.type!==unit.type || entry.tier!==unitArtTier(unit)){const previousSheet=entry.type===unit.type?entry.sheet:null;entry.type=unit.type;entry.tier=unitArtTier(unit);entry.sheet=previousSheet;entry.frame=null;entry.pending=false;entry.loadAttempted=false;entry.loadVersion++;}
    const waitingForImpact=time<effects.impactDeadline(unit.id),currentHp=unit.targetHp??unit.hp;
    if(!waitingForImpact){if(currentHp<entry.lastHp)entry.hitUntil=Date.now()+200;entry.lastHp=currentHp;}
    if(!entry.loadAttempted)load(entry);
    if(!entry.sheet)return false;
    let art=host.querySelector(".troop-art");
    if(!art){art=host.ownerDocument.createElement("div");art.className="troop-art";host.appendChild(art);}
    if(!entry.canvas || !art.contains(entry.canvas)){
      art.replaceChildren();entry.canvas=host.ownerDocument.createElement("canvas");entry.canvas.className="ff-unit-sprite";entry.canvas.setAttribute("aria-hidden","true");art.appendChild(entry.canvas);entry.frame=null;
    }
    host.classList.add("ff-sprite");
    const moving=options.moving ?? (Math.hypot((unit.targetX??unit.x)-unit.x,(unit.targetY??unit.y)-unit.y)>.03);
    const reaction=reactions.get(unit.id),alivePresentation=waitingForImpact?{...unit,hp:Math.max(1,unit.hp),removing:false}:unit;
    let state=resolveAnimation({...alivePresentation,hitUntil:waitingForImpact?0:entry.hitUntil},Date.now(),moving);
    if(reaction && state!=="death"){
      if(reaction.castUntil>time)state="cast";else if(reaction.attackUntil>time)state="attack";
      else if(reaction.hitUntil>time&&!waitingForImpact)state="hit";
    }
    if(state!==entry.state || ["cast","attack"].includes(state) && (unit.animUntil||0)>entry.lastActionUntil){entry.state=state;entry.stateAt=time;entry.frame=null;}
    entry.lastActionUntil=unit.animUntil||0;
    const frame=animationFrame(entry.sheet.spec,state,effects.settings.reducedMotion && ["idle","move"].includes(state)?0:time-entry.stateAt);
    const facing=options.facing || entry.motion.facing || (unit.p===1?-1:1), stamp=JSON.stringify(frame)+"/"+facing;
    const pulse=reaction?.swellingUntil>time&&!effects.settings.reducedMotion?1+Math.sin((time-(reaction.swellingUntil-180))/180*Math.PI)*.12:1;
    const burrow=reaction?.burrowUntil>time&&!effects.settings.reducedMotion?Math.abs((time-(reaction.burrowUntil-400))/200-1):1;
    const hit=!waitingForImpact&&entry.hitUntil>Date.now(),flash=effects.settings.screenFlash&&hit;
    entry.canvas.style.filter=flash?"brightness(1.22)":(unit.statusShockUntil||unit.shockUntil||0)>Date.now()?"drop-shadow(0 0 4px #8cdcff)":(unit.bindUntil||0)>Date.now()?"drop-shadow(0 0 3px #c6a1ff)":"";
    entry.canvas.style.opacity=burrow<1?String(.22+burrow*.78):"";
    entry.canvas.style.transform=`translate(${-100*(entry.sheet.spec.pivot?.[0]??.5)}%, ${-100*(entry.sheet.spec.pivot?.[1]??.9375)}%) scale(${pulse},${pulse*(.3+burrow*.7)})`;
    if(stamp===entry.frame)return true;
    const rect=frameRectangle(entry.sheet.spec,frame,entry.sheet.image),canvas=entry.canvas;
    canvas.width=rect.width;canvas.height=rect.height;
    const context=canvas.getContext("2d",{alpha:true});if(!context)return false;
    context.clearRect(0,0,rect.width,rect.height);context.imageSmoothingEnabled=false;
    drawSpriteFrame(context,entry.sheet.image,rect,{flip:facing<0});
    const pivot=entry.sheet.spec.pivot || [.5,.9375];
    canvas.style.transform=`translate(${-100*(pivot[0]??.5)}%, ${-100*(pivot[1]??.9375)}%) scale(${pulse},${pulse*(.3+burrow*.7)})`;
    canvas.style.width=`${(entry.sheet.spec.displayScale || 1.35)*100}%`;canvas.style.height="auto";
    entry.frame=stamp;return true;
  }
  function sync(battle,time=clock()){
    const live=new Set();
    for(const unit of battle?.units || []){
      live.add(unit.id);const entry=record(unit,time);
      entry.motion.move(unit.x,unit.y,time,battle.tickMs || 340);entry.unit={...unit};entry.removingAt=null;
      if(!entry.host && layer){
        const node=layer.ownerDocument.createElement("div");node.className="ff-visual-unit";node.dataset.unitId=String(unit.id);node.innerHTML='<div class="unit"><div class="troop-art"></div></div><div class="hp"><i></i></div>';layer.appendChild(node);entry.node=node;entry.host=node.querySelector(".unit");
        const art=entry.host.querySelector(".troop-art"),path=fallbackPath(unit.type);if(path){const img=layer.ownerDocument.createElement("img");img.className="troop-image";img.src=path;img.alt=troops[unit.type]?.name||unit.type;art.appendChild(img);}else art.textContent=troops[unit.type]?.emoji||"◆";
      }
    }
    for(const entry of records.values())if(!live.has(entry.id) && !entry.removingAt){entry.removingAt=time;entry.unit.removing=true;}
  }
  function step(time){
    effects.step(time);
    for(const [id,reaction] of reactions)if(time>Math.max(reaction.attackUntil||0,reaction.castUntil||0,reaction.hitUntil||0,reaction.swellingUntil||0,reaction.burrowUntil||0)+1000)reactions.delete(id);
    for(const [id,entry] of records){
      // Existing game nodes already receive renderUnit from their visual loop.
      // Only nodes owned by this renderer are advanced a second time here.
      if(!entry.node)continue;
      const visual=entry.motion.sample(time),unit=entry.unit,size=unit.size===2||troops[unit.type]?.size===2?2:1;
      if(entry.node){entry.node.style.transform=`translate(${visual.x*(cell+gap)}px,${visual.y*(cell+gap)}px)`;entry.node.style.width=entry.node.style.height=`${size*cell+(size-1)*gap}px`;const hp=entry.node.querySelector(".hp i");hp.style.width=`${Math.max(0,100*(unit.hp||0)/Math.max(1,unit.maxhp||unit.hp||1))}%`;}
      renderUnit(entry.host,unit,time,{moving:visual.moving,facing:visual.facing});
      if(entry.removingAt && time>effects.impactDeadline(id)&&time-entry.removingAt>deathDuration(unit.type,unit.forgeTier)){entry.node?.remove();records.delete(id);}
    }
  }
  function start(){if(running)return;const loop=t=>{step(t);running=requestAnimationFrame(loop);};running=requestAnimationFrame(loop);}
  function event(ev){
    const handled=effects.event(ev);if(!handled||!ev)return handled;
    const time=clock();
    if(ev.sourceId!=null){
      const reaction=reactions.get(ev.sourceId)||{};
      if(['attack','rocket','rail-shot','rail-overload','evolution-echo','precision-hit'].includes(ev.kind)){reaction.attackUntil=time+480;reaction.castUntil=0;if(ev.unitType==='blood')reaction.swellingUntil=time+180;}
      else if(!['death','hit','rail-impact','ward-break','rocket-explosion','shockwave','shield-hit'].includes(ev.kind)){reaction.castUntil=time+500;}
      if(ev.kind==='burrow'||ev.kind==='ambush')reaction.burrowUntil=time+400;
      reactions.set(ev.sourceId,reaction);
    }
    if(ev.kind==='hit'&&ev.targetId!=null){const reaction=reactions.get(ev.targetId)||{};reaction.hitUntil=Math.max(time,effects.impactDeadline(ev.targetId))+220;reactions.set(ev.targetId,reaction);}
    return handled;
  }
  function deathDuration(key,tier=1){const entries=[...records.values()],entry=entries.find(e=>e.type===key&&e.tier===tier)||entries.find(e=>e.type===key);return entry?.sheet?animationDefinition(entry.sheet.spec,"death").duration:700;}
  async function paintTerrain(cells,viewport,theme="contested-forge"){
    if(!assets?.enabled())return false;
    const [terrain,background]=await Promise.all([assets.asset("terrain",theme),assets.asset("backgrounds",theme)]);
    if(background && viewport){viewport.style.backgroundImage=`linear-gradient(#070c1590,#070c15a0),url("${background.url}")`;viewport.style.backgroundSize="cover";viewport.style.backgroundPosition="center";}
    if(!terrain || !cells)return !!background;
    const spec=terrain.spec,width=spec.frameWidth||128,height=spec.frameHeight||128,columns=spec.columns||Math.floor(terrain.image.width/width),rows=spec.rows||Math.floor(terrain.image.height/height),total=Math.max(1,columns*rows);
    for(let i=0;i<cells.children.length;i++){const node=cells.children[i],frame=(i*7+Math.floor(i/8)*3)%total,col=frame%columns,row=Math.floor(frame/columns);node.classList.add("ff-ground-cell");node.style.backgroundImage=`url("${terrain.url}")`;node.style.backgroundSize=`${columns*100}% ${rows*100}%`;node.style.backgroundPosition=`${columns>1?col/(columns-1)*100:0}% ${rows>1?row/(rows-1)*100:0}%`;}
    return true;
  }
  function paintFields(battle,time=Date.now()){
    effects.syncFields(battle,time,clock());
  }
  function clear(){for(const entry of records.values())entry.node?.remove();records.clear();reactions.clear();effects.clear();}
  function destroy(){if(running)cancelAnimationFrame(running);running=0;clear();effects.destroy();}
  return {renderUnit,sync,step,start,event,clear,destroy,deathDuration,paintTerrain,paintFields,configure:effects.configure,unlockAudio:effects.unlockAudio,impactDeadline:effects.impactDeadline,createMotionTrack:(x,y,time=clock())=>new MotionTrack(x,y,time),forget:id=>{records.delete(id);reactions.delete(id);},get projectiles(){return effects.projectiles;},get attachments(){return effects.attachments;},get quality(){return effects.quality;},get settings(){return effects.settings;},get stats(){return effects.stats;},get size(){return records.size;}};
}
