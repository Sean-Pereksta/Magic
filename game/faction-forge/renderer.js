import {animationDefinition,frameRectangle,drawSpriteFrame} from "./assets.js";

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

export function createBattleRenderer({layer,assets,cell=64,gap=4,now=()=>performance.now(),troops={},fallbackPath=()=>"",reducedMotion=globalThis.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches || false}={}){
  const records=new Map(), effects=new Map(), fields=new Map();let running=0,lastFrame=0,lowQuality=false,slowFrames=0;
  const clock=now;
  function record(unit,time){
    let entry=records.get(unit.id);
    if(!entry){entry={id:unit.id,type:unit.type,tier:unit.forgeTier||1,unit:{...unit},motion:new MotionTrack(unit.x,unit.y,time),state:"idle",stateAt:time,frame:null,pending:false,loadAttempted:false,sheet:null,canvas:null,host:null,lastHp:unit.targetHp??unit.hp,lastActionUntil:unit.animUntil||0};entry.motion.facing=unit.p===1?-1:1;records.set(unit.id,entry);}
    return entry;
  }
  async function load(entry){
    if(entry.pending || entry.loadAttempted || !assets?.enabled())return;
    entry.pending=true;entry.loadAttempted=true;const key=entry.type,tier=entry.tier;
    const sheet=await assets.unit(key,tier);entry.pending=false;
    if(entry.type===key && entry.tier===tier)entry.sheet=sheet;
  }
  function renderUnit(host,unit,time=clock(),options={}){
    if(!host)return false;
    host.classList.toggle("ff-illusion",!!unit.illusion);host.classList.toggle("ff-marked",(unit.markedUntil||0)>Date.now());
    if(!assets?.enabled())return false;
    const entry=record(unit,time);entry.unit={...unit};entry.host=host;
    if(entry.type!==unit.type || entry.tier!==(unit.forgeTier||1)){entry.type=unit.type;entry.tier=unit.forgeTier||1;entry.sheet=null;entry.pending=false;entry.loadAttempted=false;}
    const currentHp=unit.targetHp??unit.hp;if(currentHp<entry.lastHp)entry.hitUntil=Date.now()+200;entry.lastHp=currentHp;
    if(!entry.sheet){load(entry);return false;}
    let art=host.querySelector(".troop-art");
    if(!art){art=host.ownerDocument.createElement("div");art.className="troop-art";host.appendChild(art);}
    if(!entry.canvas || !art.contains(entry.canvas)){
      art.replaceChildren();entry.canvas=host.ownerDocument.createElement("canvas");entry.canvas.className="ff-unit-sprite";entry.canvas.setAttribute("aria-hidden","true");art.appendChild(entry.canvas);entry.frame=null;
    }
    host.classList.add("ff-sprite");
    const moving=options.moving ?? (Math.hypot((unit.targetX??unit.x)-unit.x,(unit.targetY??unit.y)-unit.y)>.03);
    const state=resolveAnimation({...unit,hitUntil:entry.hitUntil},Date.now(),moving);
    if(state!==entry.state || ["cast","attack"].includes(state) && (unit.animUntil||0)>entry.lastActionUntil){entry.state=state;entry.stateAt=time;entry.frame=null;}
    entry.lastActionUntil=unit.animUntil||0;
    const frame=animationFrame(entry.sheet.spec,state,reducedMotion && ["idle","move"].includes(state)?0:time-entry.stateAt);
    const facing=options.facing || entry.motion.facing || (unit.p===1?-1:1), stamp=JSON.stringify(frame)+"/"+facing;
    if(stamp===entry.frame)return true;
    const rect=frameRectangle(entry.sheet.spec,frame,entry.sheet.image),canvas=entry.canvas;
    canvas.width=rect.width;canvas.height=rect.height;
    const context=canvas.getContext("2d",{alpha:true});if(!context)return false;
    context.clearRect(0,0,rect.width,rect.height);context.imageSmoothingEnabled=false;
    drawSpriteFrame(context,entry.sheet.image,rect,{flip:facing<0});
    const pivot=entry.sheet.spec.pivot || [.5,.9375];
    canvas.style.transform=`translate(${-100*(pivot[0]??.5)}%, ${-100*(pivot[1]??.9375)}%)`;
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
    const frameGap=time-lastFrame;if(lastFrame && frameGap>38)slowFrames++;else slowFrames=Math.max(0,slowFrames-1);lowQuality=slowFrames>15;
    if(lowQuality && frameGap<32)return;
    lastFrame=time;
    for(const [id,entry] of records){
      // Existing game nodes already receive renderUnit from their visual loop.
      // Only nodes owned by this renderer are advanced a second time here.
      if(!entry.node)continue;
      const visual=entry.motion.sample(time),unit=entry.unit,size=unit.size===2||troops[unit.type]?.size===2?2:1;
      if(entry.node){entry.node.style.transform=`translate(${visual.x*(cell+gap)}px,${visual.y*(cell+gap)}px)`;entry.node.style.width=entry.node.style.height=`${size*cell+(size-1)*gap}px`;const hp=entry.node.querySelector(".hp i");hp.style.width=`${Math.max(0,100*(unit.hp||0)/Math.max(1,unit.maxhp||unit.hp||1))}%`;}
      renderUnit(entry.host,unit,time,{moving:visual.moving,facing:visual.facing});
      if(entry.removingAt && time-entry.removingAt>deathDuration(unit.type,unit.forgeTier)){entry.node?.remove();records.delete(id);}
    }
    for(const [id,effect] of effects){
      const p=(time-effect.at)/effect.duration;if(p>=1){effect.node.remove();effects.delete(id);continue;}
      const x=effect.a.x+(effect.b.x-effect.a.x)*p,y=effect.a.y+(effect.b.y-effect.a.y)*p,size=effect.size||0;
      effect.node.style.transform=`translate(${x-size/2}px,${y-size/2}px) rotate(${effect.angle}rad)`;
      if(effect.sheet){
        const {image,spec}=effect.sheet,columns=spec.columns || Math.floor(image.width/(spec.frameWidth||128)),rows=spec.rows || 1;
        const effective={...spec,frameWidth:spec.frameWidth||128,frameHeight:spec.frameHeight||128,columns,animations:spec.animations||{idle:{frames:Array.from({length:columns*rows},(_,i)=>i),duration:effect.duration,loop:false}}};
        const frame=animationFrame(effective,"idle",time-effect.at);
        if(frame!==effect.lastFrame){const rect=frameRectangle(effective,frame,image),context=effect.canvas.getContext("2d");if(context){if(effect.canvas.width!==rect.width || effect.canvas.height!==rect.height){effect.canvas.width=rect.width;effect.canvas.height=rect.height;}context.clearRect(0,0,rect.width,rect.height);drawSpriteFrame(context,image,rect);effect.lastFrame=frame;}}
      }
    }
  }
  function start(){if(running)return;const loop=t=>{step(t);running=requestAnimationFrame(loop);};running=requestAnimationFrame(loop);}
  function event(ev){
    if(!layer || !ev || lowQuality)return false;
    const traveling=["beam","hbeam"].includes(ev.type) || ev.type==="transport" && ev.kind==="rail-shot",kind=ev.kind || ev.type;
    if(!traveling && !ev.kind && !["impact","burst","storm"].includes(ev.type))return false;
    const point=(x,y)=>({x:x*(cell+gap)+cell/2,y:y*(cell+gap)+cell/2});
    const a=traveling?point(ev.x1,ev.y1):point(ev.targetX??ev.x??ev.cx,ev.targetY??ev.y??ev.cy),b=traveling?point(ev.x2,ev.y2):a;
    if(!Number.isFinite(a.x+a.y+b.x+b.y))return false;
    const node=layer.ownerDocument.createElement("div");node.className=traveling?"ff-projectile"+(ev.type==="hbeam"?" healing":""):"ff-effect-node";layer.appendChild(node);
    const id=ev.id||String(clock()),effect={node,a,b,at:clock(),duration:traveling?Math.min(350,Math.max(130,Math.hypot(b.x-a.x,b.y-a.y)/1.3)):(ev.ttl||650),angle:traveling?Math.atan2(b.y-a.y,b.x-a.x):0,size:0};effects.set(id,effect);
    if(assets?.enabled())assets.asset("effects",kind).then(sheet=>{
      if(!sheet || !effects.has(id))return;
      effect.sheet=sheet;effect.size=traveling?Math.round(cell*.65):Math.round(cell*(["temporal-field","hive-frenzy","mirage"].includes(kind)?2:1.35));
      node.className="ff-effect-node";node.style.width=node.style.height=`${effect.size}px`;node.replaceChildren();effect.canvas=layer.ownerDocument.createElement("canvas");effect.canvas.className="ff-effect-canvas";node.appendChild(effect.canvas);
    }).catch(()=>{});
    return true;
  }
  function deathDuration(key,tier=1){const entry=[...records.values()].find(e=>e.type===key&&e.tier===tier);return entry?.sheet?animationDefinition(entry.sheet.spec,"death").duration:700;}
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
    if(!layer)return;
    const live=new Set();for(const field of (battle?.temporalFields || []).slice(-18)){
      if(field.until<=time)continue;const key=[field.x,field.y,field.p,field.until].join(":");live.add(key);let node=fields.get(key);
      if(!node){node=layer.ownerDocument.createElement("div");node.className="ff-temporal-field"+(field.fracture?" ff-fracture-field":"");node.setAttribute("aria-hidden","true");layer.appendChild(node);fields.set(key,node);}
      const diameter=(field.radius*2+1)*(cell+gap);node.style.width=node.style.height=`${diameter}px`;node.style.left=`${field.x*(cell+gap)+cell/2-diameter/2}px`;node.style.top=`${field.y*(cell+gap)+cell/2-diameter/2}px`;
    }
    for(const [key,node] of fields)if(!live.has(key)){node.remove();fields.delete(key);}
  }
  function clear(){for(const entry of records.values())entry.node?.remove();for(const e of effects.values())e.node.remove();for(const node of fields.values())node.remove();records.clear();effects.clear();fields.clear();}
  function destroy(){if(running)cancelAnimationFrame(running);running=0;clear();}
  return {renderUnit,sync,step,start,event,clear,destroy,deathDuration,paintTerrain,paintFields,createMotionTrack:(x,y,time=clock())=>new MotionTrack(x,y,time),forget:id=>records.delete(id),get quality(){return lowQuality?"low":"high";},get size(){return records.size;}};
}
