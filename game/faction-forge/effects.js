/** Presentation only: this module consumes immutable combat events. It never
 * selects damage targets, advances combat RNG or writes to a battle snapshot. */
import {createBattleAudio} from './audio.js';
import {frameRectangle,drawSpriteFrame} from './assets.js';

export const EFFECT_LIMITS=Object.freeze({objects:480,high:420,low:130,seenEvents:1024,impactDelayMs:220,labels:5});
const profile=(attack,ability,death,color)=>Object.freeze({attack,ability,death,color});
export const UNIT_EFFECTS=Object.freeze({
  rifleman:profile('bullet','bullet','mechanical','#ffd58c'),
  vrocket:profile('missile','missile','mechanical','#ffb35a'),
  vguard:profile('shield-slam','shield','mechanical','#a8dfff'),
  miner:profile('slash','ambush','mechanical','#e8be72'),
  medic:profile('bullet','healing','mechanical','#8df6b6'),
  suppressor:profile('foam','suppression','mechanical','#b3e5e8'),
  shieldcap:profile('shield-slam','shield','mechanical','#a6dbff'),
  marksman:profile('precision','mark','mechanical','#ffe8ae'),
  emp:profile('emp','emp','mechanical','#80dfff'),
  railbreaker:profile('rail','charge','mechanical','#b9f1ff'),
  signalhawk:profile('mark-shot','mark','mechanical','#ffd880'),
  phaser:profile('orb','teleport','arcane','#c0b4ff'),
  blade:profile('slash','slash','arcane','#e7d4ff'),
  storm:profile('storm','storm','arcane','#9fe1ff'),
  guardian:profile('slam','slam','arcane','#b7b5ff'),
  wardmaker:profile('orb','ward','arcane','#95e5ff'),
  architect:profile('orb','ward','arcane','#8fcaff'),
  golem:profile('slam','shield','arcane','#bbdbff'),
  runebinder:profile('binding-orb','binding','arcane','#d1a6ff'),
  chronoweaver:profile('orb','temporal','arcane','#c7b2ff'),
  miragewarden:profile('orb','mirage','arcane','#b2e9ff'),
  hellspawn:profile('bite','swarm','organic','#e1b46c'),
  roach:profile('acid','toxic','organic','#b5e268'),
  defiler:profile('sting','toxic','organic','#a9f466'),
  blood:profile('blood-explosion','blood-explosion','organic','#ff655c'),
  spitter:profile('acid','toxic','organic','#b4f46d'),
  matron:profile('swarm-orb','swarm','organic','#e9c177'),
  viper:profile('venom','venom','organic','#b9ed87'),
  carrion:profile('blood-orb','wound','organic','#ef8e9e'),
  burrower:profile('bite','ambush','organic','#c7ab7b'),
  siphonleech:profile('drain','healing','organic','#f195ac'),
  hiveherald:profile('swarm-orb','swarm','organic','#e9be75')
});
const aliases=Object.freeze({'rocket':'missile','bloodburst-explosion':'blood-explosion','lightning-chain':'lightning','heal':'healing','siphon':'healing','toxic-cloud':'toxic','temporal-field':'temporal','time-fracture':'temporal','hive-frenzy':'swarm','rail-shot':'rail','rail-overload':'rail','rail-charge':'charge','burrow':'ambush','spawn':'swarm','guardian-shockwave':'slam','arcane-binding':'binding','healing-pulse':'healing'});
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
const finite=(v,fallback=0)=>Number.isFinite(Number(v))?Number(v):fallback;
const TAU=Math.PI*2;
function hash(value){let seed=2166136261;for(const c of String(value)){seed=Math.imul(seed^c.charCodeAt(0),16777619);}return seed>>>0;}
function random(seed){seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;}

/** A quadratic flight path gives real projectiles stable locations at any fps. */
export function projectilePosition(effect,time,reducedMotion=false){
  const p=clamp((time-effect.start)/effect.duration,0,1),curve=reducedMotion?0:(effect.curve||0);
  return {x:effect.x+(effect.x2-effect.x)*p,y:effect.y+(effect.y2-effect.y)*p-Math.sin(p*Math.PI)*curve,p};
}

/** Fixed object slots are recycled, including particles and delayed impacts. */
export class EffectPool {
  constructor(capacity=EFFECT_LIMITS.objects){this.capacity=capacity;this.active=[];this.free=Array.from({length:capacity},()=>({}));this.dropped=0;}
  acquire(values,limit=this.capacity){
    if(this.active.length>=limit||!this.free.length){
      if((values.priority||0)<2){this.dropped++;return null;}
      const index=this.active.findIndex(item=>item.priority<2);
      if(index<0){this.dropped++;return null;}
      this.free.push(this.active[index]);this.active.splice(index,1);
    }
    const item=this.free.pop();
    // Clear optional fields so a recycled missile cannot retain its old target.
    for(const key of Object.keys(item))delete item[key];
    Object.assign(item,values);this.active.push(item);return item;
  }
  prune(time){for(let i=this.active.length-1;i>=0;i--)if(time>=this.active[i].start+this.active[i].duration){this.free.push(this.active[i]);this.active.splice(i,1);}}
  clear(){this.free.push(...this.active);this.active.length=0;}
}

export function createBattleEffects({layer,cell=64,gap=4,columns=8,rows=16,now=()=>globalThis.performance?.now?.()??Date.now(),audio=createBattleAudio({now}),reducedMotion=globalThis.matchMedia?.('(prefers-reduced-motion: reduce)')?.matches||false,intensity='auto',screenFlash=true,sizeFor=()=>1,positionFor=()=>null,artFor=()=>null,deviceMemory=globalThis.navigator?.deviceMemory,cores=globalThis.navigator?.hardwareConcurrency}={}){
  const pool=new EffectPool(),seen=new Set(),deadlines=new Map(),persistent=new Map(),deferred=new Map(),surfaces=[];
  let destroyed=false,lowQuality=(deviceMemory||8)<=2||(cores||8)<=2,slowFrames=0,lastFrame=0,lastMeasured=0,serial=0,currentSeed=1;
  let settings={intensity:['auto','high','low','off'].includes(intensity)?intensity:'auto',reducedMotion:!!reducedMotion,screenFlash:!!screenFlash,...audio.settings};
  const stride=cell+gap,width=columns*stride-gap,height=rows*stride-gap;
  const quality=()=>settings.intensity==='low'||settings.intensity==='auto'&&lowQuality?'low':'high';
  const limit=()=>quality()==='low'?EFFECT_LIMITS.low:EFFECT_LIMITS.high;
  const point=(x,y,size=1)=>({x:finite(x)*stride+cell/2+(size-1)*stride/2,y:finite(y)*stride+cell/2+(size-1)*stride/2});
  function surface(depth){
    if(!layer?.ownerDocument)return null;
    const canvas=layer.ownerDocument.createElement('canvas'),parent=layer.parentElement||layer;
    canvas.className=`ff-battle-effects ff-battle-effects-${depth}`;canvas.setAttribute('aria-hidden','true');
    // The game board has ten pixels of tile padding; all effects use tile centers.
    const padding=parent.querySelector?.('.cells')?10:0;
    Object.assign(canvas.style,{position:'absolute',left:`${padding}px`,top:`${padding}px`,width:`${width}px`,height:`${height}px`,pointerEvents:'none',zIndex:depth==='ground'?'10':'16'});
    parent.appendChild(canvas);let context=null;try{context=canvas.getContext('2d',{alpha:true});}catch{}
    const item={canvas,context,depth,dpr:0};surfaces.push(item);return item;
  }
  surface('ground');surface('air');
  function resize(){
    const dpr=quality()==='low'?1:Math.min(1.5,globalThis.devicePixelRatio||1);
    for(const s of surfaces)if(s.dpr!==dpr){s.dpr=dpr;s.canvas.width=Math.ceil(width*dpr);s.canvas.height=Math.ceil(height*dpr);s.context?.setTransform(dpr,0,0,dpr,0,0);}
  }
  function spawn(family,a,b=a,options={}){
    if(destroyed||settings.intensity==='off')return null;
    const start=options.start??now();
    return pool.acquire({family,x:a.x,y:a.y,x2:b.x,y2:b.y,start,duration:options.duration||450,radius:options.radius??cell*.42,color:options.color||'#b9deff',depth:options.depth||'air',seed:(currentSeed+(++serial)*97)>>>0,rank:options.rank||1,priority:options.priority??1,...options},limit());
  }
  function particles(a,color,count=10,{at=now(),radius=cell*.7,kind='spark',...options}={}){
    if(settings.reducedMotion)return;
    count=Math.floor(count*(quality()==='low' ? .3 : 1));
    for(let i=0;i<count;i++){
      const seed=(currentSeed+i*739+serial*13)>>>0,angle=random(seed)*TAU,distance=radius*(.35+random(seed+117)*.65);
      spawn(kind,a,{x:a.x+Math.cos(angle)*distance,y:a.y+Math.sin(angle)*distance},{start:at,duration:320+random(seed+53)*430,radius:1.5+random(seed+71)*3,color,priority:0,...options});
    }
  }
  function ring(a,color,radius,at=now(),duration=450,options={}){return spawn('ring',a,a,{start:at,color,radius,duration,depth:'ground',priority:2,...options});}
  function explosion(a,{radius=1,color='#ffae50',blood=false,at=now(),rank=1,electric=false}={}){
    // Radius is supplied by actual splash damage; do not invent a larger damage ring.
    const pixels=Math.max(cell*.3,radius*stride+cell*.45);
    spawn('explosion',a,a,{start:at,duration:520,radius:pixels,color,blood,rank,priority:3});
    ring(a,blood?'#ff7375':'#ffd299',pixels,at,580);
    spawn('smoke',a,{x:a.x,y:a.y-cell*.4},{start:at+90,duration:850,radius:pixels*.68,color:blood?'#562d39':'#424955',priority:0});
    particles(a,blood?'#ff6b58':'#ffd179',rank===3?20:14,{at,radius:pixels*1.2});
    particles(a,'#413e3b',8,{at,radius:pixels,kind:'debris'});
    if(electric){ring(a,'#8deaff',pixels,at+50,420);particles(a,'#b5f4ff',8,{at:at+30,radius:pixels});}
    audio.play('explosion',{delay:Math.max(0,at-now())/1000,power:rank===3?1.15:1});
  }
  function lightning(a,b,{at=now(),color='#a1e7ff',rank=1}={}){
    spawn('lightning',a,b,{start:at,duration:260,color,rank,priority:3});
    spawn('hit',b,b,{start:at,duration:250,color,radius:cell*.45,priority:2});
    particles(b,color,7,{at,radius:cell*.6});audio.play('lightning',{delay:Math.max(0,at-now())/1000});
  }
  function storm(a,targets,{radius=1,at=now(),rank=1}={}){
    const cloud={x:a.x,y:Math.max(cell*.25,a.y-cell*1.25)},pixels=Math.max(cell*.7,radius*stride);
    spawn('cloud',cloud,cloud,{start:at,duration:1100,radius:pixels,rank,priority:3});
    lightning(cloud,a,{at:at+65,rank});
    // Arcs connect only simulation-provided damaged targets.
    let previous=a;for(const target of targets.slice(0,6)){lightning(previous,target,{at:at+85,rank});previous=target;}
  }
  function holdImpact(ev,at){
    const ids=[ev.targetId,...(ev.targets||[]).map(t=>t.id)].filter(id=>id!=null);
    for(const id of ids)deadlines.set(id,Math.max(deadlines.get(id)||0,at));
  }
  function projectile(family,a,b,ev,{color='#d4b7ff',radius=0,rank=1}={}){
    // A bonus shot must leave the weapon separately from the primary round.
    // Its bounded delay is presentation only; damage/event order stays intact.
    const delay=ev.followUp?(settings.reducedMotion?25:75):0;
    const at=now()+delay,distance=Math.hypot(b.x-a.x,b.y-a.y),duration=settings.reducedMotion?60:clamp(distance/(family==='bullet'?5:2.5),family==='bullet'?45:95,EFFECT_LIMITS.impactDelayMs-delay);
    const impactAt=at+duration;
    holdImpact(ev,impactAt);
    spawn(family,a,b,{start:at,duration,curve:['missile','emp','acid'].includes(family)?Math.min(cell*.9,distance*.12):0,color,rank,radius:cell*.1,priority:3,sourceId:ev.sourceId,targetId:ev.targetId,followUp:!!ev.followUp});
    spawn('muzzle',a,a,{start:at,duration:ev.followUp?125:90,color,radius:cell*(ev.followUp?.17:.14),angle:Math.atan2(b.y-a.y,b.x-a.x),priority:2,sourceId:ev.sourceId,followUp:!!ev.followUp});
    if(ev.followUp)spawn('shot-cue',a,a,{start:at,duration:340,color,radius:cell*.2,priority:2,sourceId:ev.sourceId});
    if(family==='missile'){
      explosion(b,{radius:Math.max(.25,radius),at:impactAt,rank});audio.play('missile');
    }else if(family==='emp'){
      ring(b,'#88eaff',Math.max(cell*.65,radius*stride+cell*.45),impactAt,600);particles(b,'#a4edff',15,{at:impactAt,radius:cell*.9});audio.play('lightning',{delay:duration/1000});
    }else if(family==='acid'){
      spawn('splash',b,b,{start:impactAt,duration:550,color:'#b2ef6a',radius:cell*.43,depth:'ground',priority:2});particles(b,color,8,{at:impactAt,radius:cell*.45});audio.play('toxic');
    }else{
      spawn('hit',b,b,{start:impactAt,duration:180,color,radius:cell*.24,priority:2});particles(b,color,5,{at:impactAt,radius:cell*.4});audio.play(family==='bullet'?'bullet':'magic');
    }
  }
  function melee(a,b,ev,{color,rank=1,family='slash'}={}){
    spawn('slash',a,b,{duration:260,color,radius:cell*(rank===3?.8:.55),mode:family,rank,priority:2});
    spawn('hit',b,b,{duration:170,color,radius:cell*.27,priority:2});particles(b,color,5);audio.play('melee');
  }
  function ability(kind,a,b,ev,style){
    const color=style.color,rank=clamp(finite(ev.rank??ev.battleRank,1),1,3),radius=Math.max(0,finite(ev.radius,kind==='storm'?1:0));
    const targets=(ev.targets||[]).filter(t=>t.id!==ev.targetId&&Number.isFinite(t.x)&&Number.isFinite(t.y)).map(t=>point(t.x,t.y,t.size||sizeFor(t.id)));
    if(kind==='evolution-echo'){
      const echo=ev.unitType==='phaser'?'lightning':style.attack;
      return ability(echo,a,b,ev,style);
    }
    if(kind==='precision-hit'){
      if(['viper','siphonleech'].includes(ev.unitType))melee(a,b,ev,{color,rank,family:'venom'});
      else projectile('bullet',a,b,ev,{color:'#ffe0a6',rank});return;
    }
    if(kind==='shockwave'){
      ring(b,color,Math.max(cell*.5,radius*stride+cell*.4),now(),650);particles(b,'#bca88b',12,{radius:radius*stride,kind:'debris'});
      for(const t of targets)spawn('hit',t,t,{duration:220,color,priority:2});audio.play('melee');return;
    }
    if(kind==='rocket-explosion'){explosion(b,{radius:Math.max(.25,radius),rank});return;}
    if(kind==='rail-impact'){spawn('hit',b,b,{duration:240,color:'#d6f8ff',radius:cell*.35,priority:2});particles(b,'#b9edff',10,{radius:cell*.55});return;}
    if(kind==='shield-hit'){spawn('unit-shield',b,b,{duration:260,color:'#dcf3ff',radius:cell*.37,priority:2});particles(b,'#a6deff',3,{radius:cell*.22});audio.play('shield');return;}
    if(['lightning-field','blood-field','rupture'].includes(kind)){
      const tint=kind==='lightning-field'?'#80dfff':kind==='blood-field'?'#b94d65':'#ac906b';
      spawn(kind,b,b,{duration:ev.duration||1800,color:tint,radius:Math.max(cell*.5,radius*stride+cell*.4),depth:'ground',rank,priority:2});
      if(kind==='lightning-field')for(const target of targets)lightning(b,target,{color:'#a8eaff',rank});
      else particles(b,tint,7,{radius:cell*.65,kind:kind==='rupture'?'debris':'bubble'});
      audio.play(kind==='lightning-field'?'lightning':kind==='rupture'?'melee':'toxic');return;
    }
    if(kind==='ward-break'){
      const wards=(ev.targets||[]).filter(t=>Number.isFinite(t.x)&&Number.isFinite(t.y));
      for(const ward of wards.slice(0,6)){const at=point(ward.x,ward.y);particles(at,'#abe7ff',10,{kind:'debris',radius:cell*.65});ring(at,'#a0deff',cell*.45,now(),360);}
      audio.play('shield');return;
    }
    if(['missile','emp','acid','orb','bullet'].includes(kind)){projectile(kind,a,b,ev,{color,radius,rank});return;}
    if(['precision','foam','mark-shot','binding-orb','swarm-orb','blood-orb'].includes(kind)){
      projectile(kind==='precision'?'bullet':'orb',a,b,ev,{color,radius,rank});
      if(kind==='foam')ring(b,'#afd8dd',cell*.38,now()+100,450);
      if(kind==='binding-orb')spawn('binding',b,b,{start:now()+110,duration:600,color,priority:2});
      return;
    }
    if(['slash','bite','venom','sting','shield-slam'].includes(kind)){
      melee(a,b,ev,{color,rank,family:kind});if(kind==='shield-slam')ring(a,color,cell*.6);if(kind==='venom'||kind==='sting')particles(b,'#a9ea6c',6);return;
    }
    if(kind==='storm'){storm(b,targets,{radius,rank});return;}
    if(kind==='lightning'){lightning(a,b,{rank});let prev=b;for(const t of targets.slice(0,6)){lightning(prev,t,{rank});prev=t;}return;}
    if(kind==='blood-explosion'){
      // Visual windup shares a bounded delay with the displayed health/death.
      const impactAt=now()+(settings.reducedMotion?35:110);
      holdImpact(ev,impactAt);if(ev.sourceId!=null)deadlines.set(ev.sourceId,Math.max(deadlines.get(ev.sourceId)||0,impactAt));
      spawn('charge',a,a,{duration:110,color:'#ff6c68',radius:cell*.52,priority:2});explosion(b,{radius:Math.max(.25,radius),blood:true,color:'#ff6754',rank,at:impactAt});return;
    }
    if(kind==='slam'){melee(a,b,ev,{color,rank});ring(b,color,Math.max(cell*.55,radius*stride+cell*.4));particles(b,'#b3a995',10,{radius:cell});return;}
    if(kind==='rail'){
      spawn('rail',a,b,{duration:280,color:'#b9f0ff',rank,priority:3});spawn('muzzle',a,a,{duration:170,color:'#e0faff',radius:cell*.35,priority:2});particles(b,'#c6f4ff',12,{radius:cell*.6});audio.play('lightning');return;
    }
    if(kind==='healing'||kind==='drain'){
      const drain=kind==='drain'||ev.kind==='siphon',origin=drain?b:a,destination=drain?a:b,healColor=drain?'#f296b2':'#8ff4b3';
      const recipients=!drain&&ev.targets?.length?ev.targets.filter(t=>Number.isFinite(t.x)&&Number.isFinite(t.y)&&t.amount>0).slice(0,6).map(t=>point(t.x,t.y,t.size||sizeFor(t.id))):[destination];
      for(const recipient of recipients){spawn('drain',origin,recipient,{duration:280,color:healColor,radius:2,priority:2});spawn('healing-unit',recipient,recipient,{start:now()+170,duration:450,color:healColor,radius:cell*.18,priority:2});particles(recipient,healColor,3,{at:now()+170,radius:cell*.23});}audio.play('heal');return;
    }
    if(kind==='toxic'||kind==='wound'){
      if(kind==='wound'){spawn('status-wound',b,b,{duration:Math.min(1800,ev.duration||900),color:'#ef7392',radius:cell*.18,priority:2});audio.play('toxic');return;}
      spawn('toxic',b,b,{duration:Math.min(2400,ev.duration||ev.ttl||1300),color:kind==='wound'?'#a74262':'#8fce5e',radius:Math.max(cell*.5,radius*stride+cell*.4),depth:'ground',priority:2});particles(b,kind==='wound'?'#d45773':'#b4ef6a',9,{kind:'bubble',radius:cell*.6});audio.play('toxic');return;
    }
    if(kind==='ambush'){
      ring(a,'#9d825e',cell*.45);spawn('burrow',a,a,{duration:300,color:'#6f5844',radius:cell*.47,depth:'ground',priority:2});
      spawn('burrow',b,b,{duration:470,color:'#8b7156',radius:cell*.63,depth:'ground',priority:2});particles(b,'#a88b61',16,{kind:'debris',radius:cell*.8});audio.play('swarm');return;
    }
    if(kind==='teleport'||kind==='mirage'){
      const art=kind==='teleport'?artFor(ev.sourceId):null;
      if(art&&!settings.reducedMotion)spawn('afterimage',a,a,{duration:520,color,art,cell,priority:2});
      spawn('teleport',a,a,{duration:420,color,radius:cell*.5,priority:2});spawn('teleport',b,b,{start:now()+70,duration:520,color,radius:cell*.6,priority:2});particles(b,color,10,{radius:cell*.65});audio.play('magic');return;
    }
    if(kind==='swarm'){
      ring(a,'#dab773',Math.max(cell*.65,radius*stride+cell*.3),now(),650);particles(a,'#d4b46e',12,{kind:'swarm',radius:cell*1.3});audio.play('swarm');return;
    }
    if(kind==='shield'){
      const recipients=ev.targets?.length?ev.targets.filter(t=>Number.isFinite(t.x)&&Number.isFinite(t.y)).slice(0,6).map(t=>({id:t.id,...point(t.x,t.y,t.size||sizeFor(t.id))})):[{id:ev.targetId??ev.sourceId,...b}];
      for(const recipient of recipients){spawn('unit-shield',recipient,recipient,{duration:700,color:'#a6deff',radius:cell*.37,priority:2,sourceId:recipient.id});if(recipient.id!==ev.sourceId)spawn('energy-link',a,recipient,{duration:350,color:'#a6deff',priority:1});}audio.play('shield');return;
    }
    if(['binding','ward','temporal','mark','suppression','charge'].includes(kind)){
      const center=['charge','ward','shield','temporal'].includes(kind)?a:b;
      spawn(kind,center,center,{duration:Math.min(3000,ev.duration||ev.ttl||700),color,radius:kind==='binding'||kind==='suppression'?cell*.32:Math.max(cell*.42,radius*stride+cell*.35),depth:['binding','ward','temporal','suppression'].includes(kind)?'ground':'air',rank,priority:2});audio.play('magic');return;
    }
    return false;
  }
  function combo(ev,a,b,style){
    const id=ev.comboId||'',rank=clamp(ev.rank||ev.battleRank||1,1,3),radius=Math.max(.25,finite(ev.radius,1)),at=now();
    const targets=(ev.targets||[]).filter(t=>Number.isFinite(t.x)&&Number.isFinite(t.y)).map(t=>point(t.x,t.y,sizeFor(t.id)));
    if(pool.active.filter(e=>e.family==='label').length<EFFECT_LIMITS.labels)spawn('label',b,b,{duration:1050,text:String(ev.name||id.replace(/-/g,' ')).toUpperCase().slice(0,36),color:style.color,priority:3});
    if(['static-tempest','thunderclap'].includes(id)){if(id==='thunderclap')ring(b,'#c7b5ff',radius*stride,at,500);storm(b,targets,{radius,rank,at});}
    else if(id==='lightning-echo'){lightning(a,b,{rank});for(const t of targets.slice(0,4))lightning(b,t,{rank});}
    else if(id==='arcane-fortress'){
      ring(a,'#a7daff',radius*stride,at,650);spawn('shield',a,a,{duration:1000,radius:cell*.8,color:'#9bdaff',depth:'ground',priority:2});
      const links=(ev.links||[]).filter(t=>t.id!==ev.sourceId&&Number.isFinite(t.x)&&Number.isFinite(t.y));
      for(const link of links.slice(0,6))spawn('energy-link',a,point(link.x,link.y,link.size||sizeFor(link.id)),{duration:650,color:'#a7dfff',priority:2});
      for(const target of targets.slice(0,3))lightning(a,target,{rank});
    }
    else if(['blood-chain','underground-detonation'].includes(id)){
      if(id==='underground-detonation')ability('ambush',a,b,ev,style);
      for(let i=0;i<Math.min(targets.length,5);i++)spawn('charge',targets[i],targets[i],{duration:450,color:'#fa6c6c',radius:cell*.5,priority:2});
      ring(b,'#ff7073',radius*stride,at,600);particles(b,'#ff8b68',12,{radius:radius*stride});
    }
    else if(id==='plague-explosion'){
      explosion(b,{radius,blood:true,color:'#ff6856',rank});ability('toxic',a,b,{...ev,duration:1500},style);
    }
    else if(id==='brood-detonation'){ring(b,'#ff7b74',radius*stride,at,500);ability('swarm',b,b,ev,style);}
    else if(id==='predators-feast'){for(const t of targets.slice(0,6))spawn('drain',b,t,{duration:470,color:'#f291ae',priority:2});particles(b,'#cf5775',6);audio.play('heal');}
    else if(['guided-barrage','suppression-bombardment','fortified-artillery'].includes(id)){
      if(id==='guided-barrage')spawn('mark',b,b,{duration:650,color:'#ffe099',priority:2});
      if(id==='suppression-bombardment')spawn('suppression',b,b,{duration:650,color:'#b5e2e8',depth:'ground',priority:2});
      if(id==='fortified-artillery')spawn('shield',a,a,{duration:650,color:'#a7deff',depth:'ground',priority:2});
      // These are damage-time accents. Actual missiles come from real rocket attack events.
      ring(b,'#ffd49a',radius*stride,at,450);particles(b,'#ffd486',8,{radius:cell*.7});
    }
    else if(id==='electromagnetic-barrage'){ring(b,'#88eaff',radius*stride,at,550);for(const t of targets.slice(0,5))lightning(b,t,{rank});audio.play('lightning');}
    else if(id==='precision-execution'){
      const source=Number.isFinite(ev.followX)&&Number.isFinite(ev.followY)?point(ev.followX,ev.followY,sizeFor(ev.followSourceId)):a;
      spawn('mark',b,b,{duration:650,color:'#ffe09d',priority:2});spawn('rail',source,b,{duration:230,color:'#bcf1ff',priority:3});particles(b,'#ceefff',9);audio.play('bullet');
    }
    else return false;
    return true;
  }
  function event(ev){
    if(destroyed||!ev)return false;
    const style=UNIT_EFFECTS[ev.unitType]||profile('orb','orb','arcane','#bcdfff');
    const raw=ev.kind||ev.type,kind=aliases[raw]||raw;
    const recognized=ev.type==='combat'||['beam','hbeam','burst','impact','storm','transport','status'].includes(ev.type)||Object.hasOwn(aliases,raw);
    if(!recognized)return false;
    if(ev.id&&seen.has(ev.id))return true;
    if(ev.id){seen.add(ev.id);if(seen.size>EFFECT_LIMITS.seenEvents)seen.delete(seen.values().next().value);}
    if(settings.intensity==='off')return true;
    const sourceX=ev.x1??ev.x??ev.cx??ev.targetX??ev.x2,sourceY=ev.y1??ev.y??ev.cy??ev.targetY??ev.y2;
    const targetX=ev.x2??ev.targetX??ev.x??ev.cx??sourceX,targetY=ev.y2??ev.targetY??ev.y??ev.cy??sourceY;
    if(![sourceX,sourceY,targetX,targetY].every(v=>Number.isFinite(Number(v))))return false;
    const a=point(sourceX,sourceY,ev.sourceSize||sizeFor(ev.sourceId)),b=point(targetX,targetY,ev.targetSize||1);
    currentSeed=hash(ev.id||`${raw}:${ev.sourceId}:${ev.targetId}:${ev.at}`);
    if(deferred.size>=192)deferred.delete(deferred.keys().next().value);
    if(kind==='combo'){
      deferred.set(ev.id||`combo:${++serial}`,{ev,a,b,style,seed:currentSeed,at:now(),kind});return true;
    }
    if(kind==='death'){
      deferred.set(ev.id||`death:${ev.sourceId??++serial}`,{ev,a,b,style,seed:currentSeed,at:now(),kind});return true;
    }
    if(kind==='hit'){deferred.set(ev.id||`hit:${ev.targetId??++serial}`,{ev,a,b,style,seed:currentSeed,at:now(),kind});return true;}
    if(['rocket-explosion','shockwave','ward-break','rail-impact','shield-hit'].includes(kind)){deferred.set(ev.id||`impact:${++serial}`,{ev,a,b,style,seed:currentSeed,at:now(),kind,ability:true});return true;}
    if(kind==='impact'){spawn('hit',b,b,{duration:180,color:style.color,priority:2});return true;}
    if(kind==='attack')return ability(style.attack,a,b,ev,style)!==false;
    if(kind==='ability')return ability(style.ability,a,b,ev,style)!==false;
    if(ev.type==='beam')return ability(ev.unitType?style.attack:'orb',a,b,ev,style)!==false;
    if(ev.type==='hbeam')return ability('healing',a,b,ev,style)!==false;
    if(ev.type==='burst'){particles(a,style.color,10);ring(a,style.color,cell*.45);return true;}
    if(ev.type==='transport'&&!ev.kind)return ability('teleport',a,b,ev,style)!==false;
    if(ev.type==='status'&&!ev.kind){spawn('hit',b,b,{duration:260,color:style.color,priority:2});return true;}
    return ability(kind,a,b,ev,style)!==false;
  }
  function configure(next={}){
    if(['auto','high','low','off'].includes(next.intensity))settings.intensity=next.intensity;
    for(const key of ['reducedMotion','screenFlash'])if(key in next)settings[key]=!!next[key];
    settings={...settings,...audio.configure(next)};
    if(settings.intensity==='off'){pool.clear();deadlines.clear();deferred.clear();}
    while(pool.active.length>limit()){const index=pool.active.findIndex(e=>e.priority<2);if(index<0)break;pool.free.push(pool.active[index]);pool.active.splice(index,1);}
    resize();return {...settings};
  }
  /** Persistent fields are reconciled by real simulation expiry and source life. */
  function syncFields(battle,wallTime=Date.now(),time=now()){
    const live=new Set(),units=battle?.units||[],aliveSources=new Set(units.filter(u=>u.hp>0).map(u=>u.id));
    const fieldStyle=kind=>kind==='lightning-field'?{family:kind,color:'#80dfff'}:kind==='blood-field'?{family:kind,color:'#b94d65'}:kind==='rupture'?{family:kind,color:'#ac906b'}:{family:'toxic',color:'#91ca64'};
    const entries=[...(battle?.temporalFields||[]).map(f=>({...f,family:'temporal',color:'#b6a3f1'})),...(battle?.toxic||[]).map(f=>({...f,family:'toxic',color:'#91ca64'})),...(battle?.acid||[]).map(f=>({...f,family:'toxic',color:'#b2d874'})),...(battle?.wards||[]).map(f=>({...f,family:'ward',color:'#8cceff'})),...(battle?.comboFields||[]).map(f=>({...f,...fieldStyle(f.kind),requiresSource:false}))];
    for(const field of entries.slice(-32)){
      if(field.until&&field.until<=wallTime||field.hp!=null&&field.hp<=0||field.requiresSource!==false&&field.sourceId&&!aliveSources.has(field.sourceId))continue;
      const key=field.id||[field.family,field.x,field.y,field.p,field.sourceId].join(':');live.add(key);
      const position=point(field.x,field.y),radius=Math.max(.35,finite(field.radius,.5))*stride;
      let item=persistent.get(key);
      if(!item){item={family:field.family,...position,x2:position.x,y2:position.y,start:time,duration:100000,radius,color:field.color,depth:'ground',seed:hash(key),priority:2,rank:field.fracture?3:1,field:true};persistent.set(key,item);}
      item.radius=radius;
    }
    // Buffs and debuffs belong to their recipients, never an aura-sized disc on
    // the caster. They follow the unit's presentation position while it moves.
    for(const unit of units.filter(u=>u.hp>0).slice(0,96)){
      const states=[];
      if((unit.comboShield||0)>0&&(unit.comboShieldUntil||0)>wallTime||(unit.startShield||0)>0&&(unit.startShieldUntil||0)>wallTime)states.push(['unit-shield','#a6deff',.37]);
      if((unit.statusPoisonUntil||0)>wallTime||(unit.statusAcidUntil||0)>wallTime)states.push(['status-poison','#b7ec78',.18]);
      if((unit.bindUntil||0)>wallTime)states.push(['status-bound','#d1afff',.31]);
      if(Math.max(unit.statusShockUntil||0,unit.shockUntil||0)>wallTime)states.push(['status-shock','#98e6ff',.18]);
      if((unit.woundUntil||0)>wallTime)states.push(['status-wound','#ee7b99',.18]);
      if(Math.max(unit.suppressedUntil||0,unit.suppressUntil||0,unit.slowUntil||0)>wallTime)states.push(['status-slow','#b5e2e8',.25]);
      for(const [family,color,scale] of states.slice(0,3)){
        const key=`unit:${unit.id}:${family}`,position=point(unit.x,unit.y,unit.size||sizeFor(unit.id));live.add(key);
        let item=persistent.get(key);
        if(!item){item={family,...position,x2:position.x,y2:position.y,start:Math.max(time,deadlines.get(unit.id)||0),duration:100000,radius:cell*scale,color,depth:'air',seed:hash(key),priority:2,attached:true,sourceId:unit.id,size:unit.size||1};persistent.set(key,item);}
        Object.assign(item,position,{x2:position.x,y2:position.y});
      }
    }
    for(const key of persistent.keys())if(!live.has(key))persistent.delete(key);
  }
  function step(time=now()){
    if(destroyed)return;
    // A death may precede its attack event in the same authoritative batch.
    // Resolve these cues after the complete batch establishes flight deadlines.
    for(const [key,item] of deferred){
      const {ev,a,b,style,kind}=item,id=kind==='death'?ev.sourceId:ev.targetId;
      if(time<Math.max(item.at,deadlines.get(id)||0))continue;
      currentSeed=item.seed;
      if(kind==='combo')combo(ev,a,b,style);
      else if(item.ability)ability(kind,a,b,ev,style);
      else if(kind==='hit')spawn('hit',b,b,{start:time,duration:180,color:style.color,priority:2});
      else{
        if(style.death==='mechanical'){particles(a,'#ffd49a',10,{at:time,kind:'debris',radius:cell*.6});spawn('smoke',a,{x:a.x,y:a.y-cell*.4},{start:time,duration:700,color:'#464d58',radius:cell*.35,priority:0});}
        else particles(a,style.color,14,{at:time,kind:style.death==='arcane'?'spark':'debris',radius:cell*.7});
        ring(a,style.color,cell*.42,time,420);
      }
      deferred.delete(key);
    }
    if(lastMeasured){const delta=time-lastMeasured;if(delta>39&&delta<200)slowFrames++;else slowFrames=Math.max(0,slowFrames-1);if(slowFrames>=18)lowQuality=true;if(slowFrames===0&&(deviceMemory||8)>2&&(cores||8)>2)lowQuality=false;}
    lastMeasured=time;pool.prune(time);
    for(const item of persistent.values())if(item.attached){const location=positionFor(item.sourceId);if(location&&Number.isFinite(location.x)&&Number.isFinite(location.y)){const position=point(location.x,location.y,item.size);Object.assign(item,position,{x2:position.x,y2:position.y});}}
    for(const [id,deadline] of deadlines)if(time>deadline+1200)deadlines.delete(id);
    if(quality()==='low'&&lastFrame&&time-lastFrame<30)return;
    lastFrame=time;resize();
    for(const surface of surfaces){
      const ctx=surface.context;if(!ctx)continue;ctx.clearRect(0,0,width,height);
      if(settings.intensity==='off')continue;
      for(const item of persistent.values())if(item.depth===surface.depth&&time>=item.start)drawEffect(ctx,item,time,settings);
      for(const item of pool.active)if(item.depth===surface.depth&&time>=item.start)drawEffect(ctx,item,time,settings);
    }
  }
  function clear(){pool.clear();seen.clear();deadlines.clear();persistent.clear();deferred.clear();audio.clear();for(const s of surfaces)s.context?.clearRect(0,0,width,height);}
  function destroy(){clear();destroyed=true;audio.destroy();for(const s of surfaces)s.canvas.remove();surfaces.length=0;}
  resize();
  return {event,step,configure,syncFields,clear,destroy,unlockAudio:()=>audio.unlock(),impactDeadline:id=>deadlines.get(id)||0,get projectiles(){return pool.active.filter(e=>['bullet','missile','emp','acid','orb'].includes(e.family)).map(e=>({...e}));},get attachments(){return [...persistent.values()].filter(e=>e.attached).map(e=>({...e}));},get quality(){return quality();},get settings(){return {...settings};},get stats(){return {active:pool.active.length,free:pool.free.length,dropped:pool.dropped,persistent:persistent.size,seen:seen.size,voices:audio.activeVoices,quality:quality()};},pool};
}

function circle(ctx,x,y,r,fill,stroke){ctx.beginPath();ctx.arc(x,y,Math.max(.1,r),0,TAU);if(fill){ctx.fillStyle=fill;ctx.fill();}if(stroke){ctx.strokeStyle=stroke;ctx.stroke();}}
function line(ctx,x1,y1,x2,y2,color,width=2){ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);ctx.lineWidth=width;ctx.strokeStyle=color;ctx.stroke();}
function glow(ctx,x,y,r,color,alpha=.7){ctx.save();ctx.globalAlpha*=alpha;circle(ctx,x,y,r,color);ctx.globalAlpha*=.6;circle(ctx,x,y,r*.55,'#e7f6ff');ctx.restore();}

/** Canvas drawing does no DOM allocation and never reads the simulation. */
export function drawEffect(ctx,e,time,settings={}){
  const p=clamp((time-e.start)/e.duration,0,1),q=1-p,r=e.radius||24,color=e.color||'#c0dfff',motion=!settings.reducedMotion,phase=motion?(time-e.start)/1000:0;
  ctx.save();ctx.globalAlpha=clamp(q*1.5,0,1);ctx.lineCap='round';ctx.lineJoin='round';
  if(['missile','bullet','emp','orb','acid','drain'].includes(e.family)){
    const v=projectilePosition(e,time,!motion),dx=e.x2-e.x,dy=e.y2-e.y,angle=Math.atan2(dy-Math.cos(p*Math.PI)*(e.curve||0)*Math.PI,dx);
    if(e.family==='missile'){
      // Trail samples follow the same curved path as the physical rocket body.
      for(let i=1;i<=(motion?7:2);i++){const t=Math.max(e.start,time-i*e.duration*.045),trail=projectilePosition(e,t,!motion);ctx.globalAlpha=.12*(1-i/9);circle(ctx,trail.x,trail.y,3+i*1.1,'#7b7c80');}
      ctx.globalAlpha=.95;ctx.translate(v.x,v.y);ctx.rotate(angle);
      ctx.fillStyle='#f8bb54';ctx.beginPath();ctx.moveTo(-11,-3);ctx.lineTo(-23-(motion?Math.sin(phase*60)*4:0),0);ctx.lineTo(-11,3);ctx.fill();
      ctx.fillStyle='#e5edf3';ctx.beginPath();ctx.moveTo(13,0);ctx.lineTo(4,-4);ctx.lineTo(-10,-4);ctx.lineTo(-12,0);ctx.lineTo(-10,4);ctx.lineTo(4,4);ctx.closePath();ctx.fill();
      ctx.fillStyle=e.rank===3?'#d6a862':'#7498ae';ctx.fillRect(-7,-4,8,8);ctx.fillStyle='#d8e1e8';ctx.beginPath();ctx.moveTo(-8,-3);ctx.lineTo(-15,-8);ctx.lineTo(-13,0);ctx.lineTo(-15,8);ctx.lineTo(-8,3);ctx.fill();
    }else if(e.family==='bullet'){
      line(ctx,v.x-Math.cos(angle)*12,v.y-Math.sin(angle)*12,v.x,v.y,color,2.5);circle(ctx,v.x,v.y,1.8,'#fff0c4');
    }else if(e.family==='emp'){
      ctx.translate(v.x,v.y);ctx.rotate(phase*9);ctx.fillStyle='#45677c';ctx.fillRect(-5,-6,10,12);ctx.strokeStyle='#acf1ff';ctx.lineWidth=2;ctx.strokeRect(-5,-6,10,12);circle(ctx,0,0,2,'#b7f8ff');
    }else{
      const count=e.family==='drain'?5:3;
      for(let i=count;i>0;i--){const t=Math.max(e.start,time-i*12),trail=projectilePosition(e,t,!motion);ctx.globalAlpha=.16;circle(ctx,trail.x,trail.y,Math.max(2,6-i),color);}
      ctx.globalAlpha=.9;glow(ctx,v.x,v.y,e.family==='acid'?6:5,color,.85);
    }
  }else if(e.family==='explosion'){
    // Smoke stays translucent; the bright core is restricted to a small area.
    const expand=motion?Math.sin(Math.min(1,p*1.5)*Math.PI*.5):.75,outer=r*(.15+expand*.85);
    ctx.globalAlpha=q*.45;circle(ctx,e.x,e.y,outer,e.blood?'#9a3744':'#a95524');
    for(let i=0;i<7;i++){const angle=i*TAU/7,rr=outer*.4;ctx.globalAlpha=q*.48;circle(ctx,e.x+Math.cos(angle)*rr,e.y+Math.sin(angle)*rr*.7,outer*.44,e.blood?'#ee594b':'#eb8c36');}
    ctx.globalAlpha=q*.8;circle(ctx,e.x,e.y,outer*.36,color);
    if(settings.screenFlash!==false&&p<.35){ctx.globalAlpha=(.35-p)*1.3;circle(ctx,e.x,e.y,outer*.22,'#fff2c7');}
  }else if(e.family==='cloud'){
    // Volumetric silhouettes drift, pulse and fade instead of a static symbol.
    ctx.globalAlpha=Math.min(1,p*7,q*3)*.78;
    const drift=motion?Math.sin(phase*1.5)*r*.06:0;
    for(let i=0;i<7;i++){const offset=(i-3)*r*.24,wave=Math.sin(i*2+phase*1.8)*r*.05;circle(ctx,e.x+offset+drift,e.y+wave,r*(.25+(i%3)*.04),i%2?'#354157':'#242d43');}
    ctx.globalAlpha=Math.min(1,q*4)*.28;circle(ctx,e.x-r*.1+drift,e.y+r*.04,r*.28,'#64758c');
  }else if(e.family==='lightning'){
    const dx=e.x2-e.x,dy=e.y2-e.y,len=Math.hypot(dx,dy)||1,nx=-dy/len,ny=dx/len;
    // Branch shape stays stable for two frames, avoiding rapid full-screen flicker.
    const frame=!motion||settings.screenFlash===false?0:Math.floor(phase*14),seed=e.seed+frame*149;
    for(let pass=0;pass<2;pass++){
      ctx.beginPath();ctx.moveTo(e.x,e.y);for(let i=1;i<=9;i++){const t=i/9,jitter=i===9?0:(random(seed+i*97)-.5)*15;ctx.lineTo(e.x+dx*t+nx*jitter,e.y+dy*t+ny*jitter);}
      ctx.globalAlpha=q*(pass ? .9 : .25);ctx.strokeStyle=pass?'#effaff':color;ctx.lineWidth=pass?1.7:6;ctx.stroke();
    }
    if(motion)for(let i=1;i<4;i++){const t=i/4;line(ctx,e.x+dx*t,e.y+dy*t,e.x+dx*t+nx*14,e.y+dy*t+ny*14,color,1);}
  }else if(e.family==='rail'||e.family==='energy-link'){
    ctx.globalAlpha=q*(e.family==='energy-link' ? .32 : .65);line(ctx,e.x,e.y,e.x2,e.y2,color,e.family==='rail'?7:2);ctx.globalAlpha=q*.9;line(ctx,e.x,e.y,e.x2,e.y2,'#dff9ff',e.family==='rail'?2:1);
  }else if(e.family==='ring'){
    const scale=motion ? .18+p*.82 : .72;ctx.globalAlpha=q*.55;ctx.lineWidth=1+q*2;ctx.beginPath();ctx.ellipse(e.x,e.y,r*scale,r*scale*.68,0,0,TAU);ctx.strokeStyle=color;ctx.stroke();
  }else if(['spark','debris','bubble','swarm'].includes(e.family)){
    const ease=1-Math.pow(q,2),x=e.x+(e.x2-e.x)*(motion?ease:.3),y=e.y+(e.y2-e.y)*(motion?ease:.3)+(e.family==='debris'?p*p*19:-Math.sin(p*Math.PI)*10);
    ctx.globalAlpha=q*.85;
    if(e.family==='debris'){ctx.translate(x,y);ctx.rotate(p*7);ctx.fillStyle=color;ctx.fillRect(-r/2,-r/2,r,r*.7);}
    else if(e.family==='bubble'){ctx.lineWidth=1;circle(ctx,x,y,r*(.4+p),null,color);}
    else if(e.family==='swarm'){ctx.fillStyle=color;ctx.beginPath();ctx.ellipse(x,y,r*1.1,r*.5,phase*9,0,TAU);ctx.fill();}
    else circle(ctx,x,y,r*q,color);
  }else if(e.family==='smoke'){
    ctx.globalAlpha=q*.2;for(let i=0;i<5;i++)circle(ctx,e.x+(i-2)*r*.25,e.y-(motion?p*r*.8:0)+(i%2)*r*.15,r*(.35+p*.65),color);
  }else if(e.family==='slash'){
    const angle=Math.atan2(e.y2-e.y,e.x2-e.x),sweep=motion?p*.5:0;ctx.translate(e.x+(e.x2-e.x)*.5,e.y+(e.y2-e.y)*.5);ctx.rotate(angle);ctx.globalAlpha=q*.75;ctx.lineWidth=e.rank===3?5:3;ctx.strokeStyle=color;ctx.beginPath();ctx.arc(0,0,r,-1.05+sweep,1.05+sweep);ctx.stroke();
    if(e.mode==='bite'||e.mode==='venom'){line(ctx,0,-8,14,3,color,2);line(ctx,0,8,14,-3,color,2);}
  }else if(e.family==='muzzle'){
    ctx.globalAlpha=q*(settings.screenFlash===false?.4:.85);ctx.translate(e.x,e.y);ctx.rotate(e.angle||0);
    line(ctx,3,0,r+4,0,color,2);line(ctx,5,-3,r+1,0,color,1.5);line(ctx,5,3,r+1,0,color,1.5);
  }else if(e.family==='hit'){
    ctx.globalAlpha=q*(settings.screenFlash===false ? .35 : .8);for(let i=0;i<4;i++){const angle=i*TAU/4;line(ctx,e.x+Math.cos(angle)*r*.22,e.y+Math.sin(angle)*r*.22,e.x+Math.cos(angle)*r*q,e.y+Math.sin(angle)*r*q,color,1.5);}
  }else if(e.family==='shot-cue'){
    ctx.globalAlpha=q*.9;line(ctx,e.x-r*.7,e.y-r*1.4,e.x-r*.1,e.y-r*1.4,color,2);line(ctx,e.x+r*.2,e.y-r*1.4,e.x+r*.8,e.y-r*1.4,color,2);
  }else if(e.family==='healing-unit'){
    ctx.globalAlpha=q*.85;const y=e.y-8-(motion?p*8:0);line(ctx,e.x-r*.45,y,e.x+r*.45,y,color,2.5);line(ctx,e.x,y-r*.45,e.x,y+r*.45,color,2.5);
  }else if(e.family==='unit-shield'){
    ctx.globalAlpha=e.attached?.7:q*.8;ctx.strokeStyle=color;ctx.lineWidth=1.6;
    ctx.beginPath();ctx.arc(e.x,e.y,r,-.85,.85);ctx.stroke();ctx.beginPath();ctx.arc(e.x,e.y,r,Math.PI-.85,Math.PI+.85);ctx.stroke();
    const x=e.x+r*.75,y=e.y-r*.65;ctx.beginPath();ctx.moveTo(x-4,y-4);ctx.lineTo(x+4,y-4);ctx.lineTo(x+3,y+1);ctx.lineTo(x,y+4);ctx.lineTo(x-3,y+1);ctx.closePath();ctx.stroke();
  }else if(e.family.startsWith('status-')){
    ctx.globalAlpha=e.attached?.85:q*.8;const x=e.x+r*.95,y=e.y+r*.6;
    if(e.family==='status-poison'){circle(ctx,x,y,3,null,color);circle(ctx,x+5,y-7,2,null,color);line(ctx,x-3,y+5,x+4,y+5,color,1.3);}
    else if(e.family==='status-wound'){line(ctx,x-3,y-7,x+1,y,color,2);line(ctx,x+2,y-4,x+5,y+3,color,1.8);}
    else if(e.family==='status-shock'){line(ctx,e.x-r,e.y-r*.6,e.x-r-4,e.y,color,1.8);line(ctx,e.x-r-4,e.y,e.x-r+2,e.y-1,color,1.8);line(ctx,e.x-r+2,e.y-1,e.x-r-2,e.y+6,color,1.8);}
    else if(e.family==='status-bound'){for(const side of [-1,1]){line(ctx,e.x+side*r,e.y+6,e.x+side*r,e.y+12,color,1.6);line(ctx,e.x+side*r,e.y+12,e.x+side*(r-5),e.y+12,color,1.6);}line(ctx,e.x-7,e.y+10,e.x+7,e.y+10,color,1.2);}
    else if(e.family==='status-slow'){line(ctx,e.x-5,e.y+r,e.x+5,e.y+r,color,1.5);line(ctx,e.x-3,e.y+r+4,e.x+3,e.y+r+4,color,1.5);}
  }else if(e.family==='lightning-field'){
    ctx.globalAlpha=.17*Math.min(1,q*4);ctx.lineWidth=1;ctx.beginPath();ctx.ellipse(e.x,e.y,r,r*.62,0,0,TAU);ctx.strokeStyle=color;ctx.stroke();
    for(let i=0;i<5;i++){const angle=i*TAU/5+(motion?phase*.15:0),x=e.x+Math.cos(angle)*r*.65,y=e.y+Math.sin(angle)*r*.4;line(ctx,x-6,y+3,x,y-4,color,1.4);line(ctx,x,y-4,x+6,y+2,color,1.4);}
  }else if(e.family==='rupture'){
    ctx.globalAlpha=.55*Math.min(1,q*4);ctx.lineWidth=2;for(let i=0;i<7;i++){const angle=i*TAU/7,len=r*(.6+random(e.seed+i*31)*.4);ctx.beginPath();ctx.moveTo(e.x,e.y);ctx.lineTo(e.x+Math.cos(angle+.12)*len*.45,e.y+Math.sin(angle+.12)*len*.3);ctx.lineTo(e.x+Math.cos(angle)*len,e.y+Math.sin(angle)*len*.65);ctx.strokeStyle=i%2?'#382e29':color;ctx.stroke();}
  }else if(e.family==='toxic'||e.family==='splash'||e.family==='blood-field'){
    ctx.globalAlpha=(e.family==='splash'?.16:.04)*Math.min(1,q*4);ctx.beginPath();ctx.ellipse(e.x,e.y,r,r*.6,0,0,TAU);ctx.fillStyle=color;ctx.fill();
    ctx.globalAlpha=.25*Math.min(1,q*4);ctx.lineWidth=1;ctx.strokeStyle=color;ctx.stroke();
    for(let i=0;i<4;i++){const angle=i*1.9+e.seed%5,bob=motion?Math.sin(phase*2+i)*3:0;ctx.globalAlpha=q*.45;ctx.lineWidth=1;circle(ctx,e.x+Math.cos(angle)*r*.5,e.y+Math.sin(angle)*r*.25-bob,2+(motion?Math.sin(phase*3+i):0),null,color);}
  }else if(e.family==='binding'){
    ctx.globalAlpha=.65*Math.min(1,q*4);ctx.strokeStyle=color;ctx.lineWidth=2;for(let i=0;i<3;i++){ctx.beginPath();ctx.ellipse(e.x,e.y-9+i*9,r*.65,r*.2,Math.sin(phase*2+i)*.3,0,TAU);ctx.stroke();}line(ctx,e.x-r*.5,e.y-r*.45,e.x+r*.4,e.y+r*.45,color,1);
  }else if(e.family==='temporal'||e.family==='ward'||e.family==='shield'||e.family==='suppression'){
    ctx.globalAlpha=e.duration>5000?.42:Math.min(.5,q);ctx.lineWidth=e.rank===3?2.5:1.5;
    ctx.translate(e.x,e.y);if(motion)ctx.rotate(phase*(e.family==='temporal' ? .2 : -.12));ctx.strokeStyle=color;
    ctx.beginPath();ctx.ellipse(0,0,r,r*.65,0,0,TAU);ctx.stroke();
    if(e.family==='temporal'){for(let i=0;i<12;i++){const a=i*TAU/12;line(ctx,Math.cos(a)*r*.84,Math.sin(a)*r*.55,Math.cos(a)*r*.97,Math.sin(a)*r*.63,color,1.4);}line(ctx,0,0,r*.5*Math.cos(phase),r*.32*Math.sin(phase),color,2);}
    else if(e.family==='shield'){ctx.beginPath();ctx.moveTo(-r*.4,-r*.6);ctx.lineTo(r*.4,-r*.6);ctx.lineTo(r*.35,0);ctx.lineTo(0,r*.35);ctx.lineTo(-r*.35,0);ctx.closePath();ctx.stroke();}
    else if(e.family==='ward'){for(let i=0;i<4;i++){const a=i*Math.PI*.5;line(ctx,Math.cos(a)*r*.45,Math.sin(a)*r*.3,Math.cos(a+Math.PI*.5)*r*.45,Math.sin(a+Math.PI*.5)*r*.3,color,1.3);}}
  }else if(e.family==='mark'){
    ctx.globalAlpha=q*.8;ctx.strokeStyle=color;ctx.lineWidth=1.5;circle(ctx,e.x,e.y-28,9,null,color);for(let i=0;i<4;i++){const angle=i*Math.PI*.5;line(ctx,e.x+Math.cos(angle)*7,e.y-28+Math.sin(angle)*7,e.x+Math.cos(angle)*13,e.y-28+Math.sin(angle)*13,color,1.5);}
  }else if(e.family==='afterimage'){
    const art=e.art;if(art?.image&&art.spec){
      try{const rect=frameRectangle(art.spec,art.frame,art.image),width=(e.cell||64)*(art.spec.displayScale||1.35),height=width*rect.height/rect.width,pivot=art.spec.pivot||[.5,.9375];
        ctx.globalAlpha=q*.42;drawSpriteFrame(ctx,art.image,rect,{x:e.x-width*pivot[0],y:e.y+(e.cell||64)/2-height*pivot[1],width,height,flip:art.flip});
      }catch{}
    }
  }else if(e.family==='teleport'||e.family==='charge'){
    ctx.globalAlpha=q*.5;ctx.lineWidth=2;circle(ctx,e.x,e.y,r*(e.family==='charge'?.7+p*.4:q*.8+.2),null,color);ctx.globalAlpha=q*.15;circle(ctx,e.x,e.y,r*.6,color);
  }else if(e.family==='burrow'){
    ctx.globalAlpha=q*.6;ctx.beginPath();ctx.ellipse(e.x,e.y,r*(.5+p*.5),r*.25,0,0,TAU);ctx.fillStyle=color;ctx.fill();ctx.strokeStyle='#c1a780';ctx.lineWidth=2;ctx.stroke();
  }else if(e.family==='label'){
    ctx.globalAlpha=Math.min(1,p*8,q*4);ctx.font='600 11px system-ui, sans-serif';ctx.textAlign='center';ctx.textBaseline='middle';const y=e.y-38-(motion?p*12:0);ctx.lineWidth=3;ctx.strokeStyle='#101722';ctx.strokeText(e.text,e.x,y);ctx.fillStyle=color;ctx.fillText(e.text,e.x,y);
  }
  ctx.restore();
}
