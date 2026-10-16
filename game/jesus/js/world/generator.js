import { seededRandom } from "../common.js";
import { VIEW, PHYSICS } from "../config.js";
import { REGIONS, normalizeBiome, BIOME_ORDER } from "./regions.js";
import { roomsFor, roomForId, SAFE_FALLBACK_ROOM, CHUNK_WIDTH } from "./rooms.js";
import { validateRoom, validateLevel } from "./room-validator.js";
import { surfaceY } from "../physics.js";

const clone=value=>JSON.parse(JSON.stringify(value));
const validationCache=new Map();
function numericSeed(value) {
  if(typeof value==="number")return (value>>>0)||1;
  let n=2166136261;for(const char of String(value)){n^=char.charCodeAt(0);n=Math.imul(n,16777619);}return n>>>0;
}
function validationFor(template,abilities) {
  const key=template.id+":"+Boolean(abilities.wings)+":"+Boolean(abilities.sandals);
  if(!validationCache.has(key))validationCache.set(key,validateRoom(template,PHYSICS,{...abilities,dash:false,wallJump:false}));
  return validationCache.get(key);
}
const stateFields=["platforms","movers","hazards","enemies","pickups","props","scenery","switches"];
export function isSecretPickup(pickup) { return Boolean(pickup.secret||["scroll","relic","chest","secret"].includes(pickup.type)); }
const GROUNDED_PICKUPS=new Set(["bread","fish","chest"]);
function supportingSurface(platforms,x,minimumY=-Infinity) {
  return platforms.filter(p=>p.kind!=="wall"&&p.kind!=="gate"&&x>=p.x&&x<=p.x+p.w&&surfaceY(p,x)>=minimumY)
    .sort((a,b)=>surfaceY(a,x)-surfaceY(b,x))[0]||null;
}
function groundPickup(pickup,platforms) {
  if(!GROUNDED_PICKUPS.has(pickup.type))return;
  const center=pickup.x+pickup.w/2;
  const support=supportingSurface(platforms,center,pickup.y+pickup.h-4)||supportingSurface(platforms,center);
  if(!support)return;
  pickup.grounded=true;pickup.groundId=support.id;pickup.groundY=surfaceY(support,center);pickup.y=pickup.groundY-pickup.h;
}
function dressPlatform(platform,index,descriptor,biome,tags) {
  if(!platform.oneWay)return;
  if(platform.kind==="collapsing"){platform.visualKind="ledge";return;}
  if(platform.kind==="moving"){
    platform.visualKind="ledge";platform.supportTop=platform.baseY-(platform.axis==="y"?platform.range:0)-160;return;
  }
  const rhythm=(index+descriptor.index)%3;
  platform.visualKind=tags.has("river")&&rhythm===1?"bridge":
    platform.branch?"ledge":tags.has("vertical")||tags.has("wind")?rhythm===0?"supported":"ledge":rhythm===0?"ledge":"supported";
  if(platform.arena)platform.visualKind="supported";
  if(platform.visualKind!=="ledge"){
    platform.supportBottom=Math.max(platform.y+150,VIEW.floorY+260);
    platform.supportStyle=["city","ruins"].includes(biome)?"arch":["mountains","storm"].includes(biome)?"rock":"posts";
  }
}
function finalTemplate(template) {
  const result=clone(template);result.id+="-arena";
  result.platforms=result.platforms.filter(p=>!p.branch&&p.x<1520).map(p=>({...p,w:Math.min(p.w,1660-p.x)}));
  const lastRoute=result.platforms.filter(p=>p.route||p.entry).sort((a,b)=>a.x-b.x).at(-1);
  const stairY=lastRoute.y+Math.max(-75,Math.min(75,VIEW.floorY-lastRoute.y));
  result.platforms.push({x:1660,y:VIEW.floorY,w:1540,h:500,kind:"ground",oneWay:false,exit:true});
  result.platforms.push({x:1520,y:stairY,w:160,h:24,kind:"platform",oneWay:true});
  result.movers=[];result.switches=[];result.enemySpawns=[];
  result.props=result.props.filter(p=>p.switchLocal==null&&p.x<1660);
  result.pickupSpawns=result.pickupSpawns.filter(p=>!isSecretPickup(p)&&p.x<1660);
  return result;
}

export function createLevel({biome,region="countryside",seed=1,difficulty=1,mode="journey",abilities={},chunkCount=null}={}) {
  biome=normalizeBiome(biome||region);const definition=REGIONS[biome],runSeed=numericSeed(seed);
  const count=Math.max(3,Math.min(112,Math.floor(chunkCount||definition.chunks)));
  const width=count*CHUNK_WIDTH;
  const world={
    id:`${biome}-${runSeed}`,name:definition.name,regionName:definition.name,biome,region:biome,seed:runSeed,
    tier:Math.max(1,Math.min(6,difficulty)),mode,width,height:1700,chunkWidth:CHUNK_WIDTH,chunkCount:count,
    bounds:{left:0,right:width,top:-700,bottom:1000},deathY:1000,
    spawn:{x:80,y:VIEW.floorY-PHYSICS.playerHeight},exit:{x:width-170,y:VIEW.floorY-92,w:110,h:92},
    enemyPool:[...definition.enemyPool],bossId:definition.bossId,hazardTypes:[...definition.hazardTypes],weather:{...definition.weather},
    platforms:[],movers:[],hazards:[],enemies:[],pickups:[],props:[],scenery:[],switches:[],bossHazards:[],effects:[],enemyProjectiles:[],
    chunks:[],chunkPlan:[],checkpoints:[],residentLimit:4,furthestX:0,time:0,totalSecrets:0,
    abilities:{dash:true,wallJump:true,wings:false,sandals:false,...abilities},
    _resident:new Map(),_state:{collected:new Set(),dead:new Set(),activated:new Set()},_streamStart:-1,
  };
  const random=seededRandom(runSeed),used=new Map();let previous="";
  for(let index=0;index<count;index++) {
    const progress=index/(count-1);
    const effectiveTier=Math.max(2,Math.min(6,world.tier+Math.floor(progress*2)));
    let candidates=roomsFor(biome,effectiveTier).filter(r=>r.id!==previous&&validationFor(r,world.abilities).valid);
    if(index===0)candidates=[SAFE_FALLBACK_ROOM];
    if(!candidates.length)candidates=[SAFE_FALLBACK_ROOM];
    // Select among least-used eligible modules, then shuffle ties deterministically.
    const least=Math.min(...candidates.map(r=>used.get(r.id)||0));
    const pool=candidates.filter(r=>(used.get(r.id)||0)<=least);
    const chosen=pool[Math.floor(random()*pool.length)];used.set(chosen.id,(used.get(chosen.id)||0)+1);previous=chosen.id;
    const rest=index%8===0;
    const actualTemplate=index===count-1?finalTemplate(chosen):chosen;
    const secretCount=actualTemplate.pickupSpawns.filter(isSecretPickup).length;
    world.totalSecrets+=secretCount;
    world.chunkPlan.push({index,x:index*CHUNK_WIDTH,width:CHUNK_WIDTH,moduleId:chosen.id,name:chosen.name,tags:[...actualTemplate.tags],rest,final:index===count-1,
      tier:effectiveTier,seed:numericSeed(runSeed+Math.imul(index+1,2654435761)),entryY:VIEW.floorY,exitY:VIEW.floorY,
      secretCount,validation:validationFor(actualTemplate,world.abilities)});
    if(rest)world.checkpoints.push({id:`${world.id}:checkpoint:${index}`,x:index*CHUNK_WIDTH+100,y:VIEW.floorY-PHYSICS.playerHeight,
      w:48,h:68,floorY:VIEW.floorY,chunkIndex:index,rest:true,activated:false});
  }
  const arenaLeft=width-1540;
  world.arena={left:arenaLeft,right:width,top:-220,bottom:1000,floorY:VIEW.floorY,entranceX:arenaLeft+70};
  world.bossTriggerX=arenaLeft+120;
  world.checkpoints.push({id:`${world.id}:checkpoint:arena`,x:arenaLeft+74,y:VIEW.floorY-PHYSICS.playerHeight,w:48,h:68,floorY:VIEW.floorY,chunkIndex:count-1,rest:true,activated:false,arena:true});
  world.validation=validateLevel(world);world.routeValid=world.validation.valid;
  updateWorldStreaming(world,world.spawn.x,0);
  return world;
}

function stampChunk(world,descriptor) {
  const source=roomForId(descriptor.moduleId)||SAFE_FALLBACK_ROOM;
  const template=clone(descriptor.final?finalTemplate(source):source),random=seededRandom(descriptor.seed);
  const offset=descriptor.x,prefix=`${world.id}:chunk:${descriptor.index}`;
  const chunk={...descriptor,platforms:[],movers:[],hazards:[],enemies:[],pickups:[],props:[],scenery:[],switches:[],tags:[...template.tags]};
  const assign=(items,type)=>items.map((item,index)=>({...item,id:`${prefix}:${type}:${index}`,x:item.x+offset,chunkIndex:descriptor.index}));
  chunk.platforms=assign(template.platforms,"platform");chunk.movers=assign(template.movers,"mover");
  for(const p of chunk.movers){p.baseX+=offset;p.dx=0;p.dy=0;}
  const styleTags=new Set(template.tags);
  chunk.platforms.forEach((p,index)=>dressPlatform(p,index,descriptor,world.biome,styleTags));
  chunk.movers.forEach((p,index)=>dressPlatform(p,index,descriptor,world.biome,styleTags));
  chunk.switches=assign(template.switches||[],"switch");
  for(const p of chunk.platforms)if(p.switchLocal!=null)p.switchId=chunk.switches[p.switchLocal]?.id;
  chunk.props=assign(template.props||[],"prop");
  for(const p of chunk.props)if(p.switchLocal!=null)p.switchId=chunk.switches[p.switchLocal]?.id;
  chunk.enemies=descriptor.rest||descriptor.final?[]:template.enemySpawns.map((spawn,index)=>({
    ...spawn,id:`${prefix}:enemy:${index}`,chunkIndex:descriptor.index,type:world.enemyPool[(index+descriptor.index)%world.enemyPool.length],
    x:spawn.x+offset,y:spawn.y,w:40,h:52,vx:0,vy:0,patrol:index%2?-1:1,facing:index%2?-1:1,hp:spawn.elite?3:2,
    elite:Boolean(spawn.elite),dead:false,homeX:spawn.x+offset,homeY:spawn.y,patrolLeft:spawn.x+offset-45,patrolRight:spawn.x+offset+90,
  }));
  chunk.pickups=template.pickupSpawns.map((spawn,index)=>({...spawn,secret:isSecretPickup(spawn),id:`${prefix}:pickup:${index}`,chunkIndex:descriptor.index,x:spawn.x+offset,w:28,h:28,collected:false}));
  if(descriptor.rest)chunk.pickups.push({id:`${prefix}:pickup:rest`,chunkIndex:descriptor.index,x:offset+165,y:VIEW.floorY-52,w:28,h:28,type:"bread",collected:false});
  // Append new IDs so existing checkpoints keep the same collected-item IDs.
  // Supplies sit on the ordinary route, before jumps and combat, in every biome.
  const routes=chunk.platforms.filter(p=>p.route&&!p.temporary);
  const movementSurface=descriptor.index===0?chunk.platforms[0]:routes[0];
  const supply=(type,p,x)=>{if(p)chunk.pickups.push({id:`${prefix}:supply:${type}`,chunkIndex:descriptor.index,x,y:surfaceY(p,x+14)-48,w:28,h:28,type,collected:false});};
  supply(descriptor.index%2===0?'water':'wind',movementSurface,descriptor.index===0?offset+245:(movementSurface?.x||offset)+20);
  const fireSurface=routes[Math.min(3,routes.length-1)];
  supply('fireRain',fireSurface,(fireSurface?.x||offset)+25);
  for(const pickup of chunk.pickups)groundPickup(pickup,chunk.platforms);
  for(let i=0;i<10;i++) {
    const p=chunk.platforms[Math.min(chunk.platforms.length-1,1+i)];if(!p)continue;
    const artFrame=REGIONS[world.biome].props[(i+Math.floor(random()*5))%REGIONS[world.biome].props.length];
    chunk.scenery.push({id:`${prefix}:scenery:${i}`,chunkIndex:descriptor.index,x:p.x+p.w*0.55,y:p.y,w:90,h:110,artFrame,kind:artFrame,
      scale:0.7+random()*0.5,depth:i%3===0?"far":"near",flip:random()<0.5});
  }
  if(!descriptor.rest&&!descriptor.final){
    const tags=new Set(template.tags);
    if(tags.has("river"))chunk.hazards.push({id:`${prefix}:hazard:water`,type:"water",x:offset+305,y:790,w:2550,h:110,active:true});
    if(tags.has("wind"))chunk.hazards.push({id:`${prefix}:hazard:wind`,type:"wind",x:offset+550,y:-170,w:1850,h:760,force:100+world.tier*10,phase:random()*6});
    const type=tags.has("lightning")?"lightning":tags.has("rockfall")?"fallingRock":world.hazardTypes[(descriptor.index+1)%world.hazardTypes.length];
    if(type!=="water"&&type!=="wind") {
      for(const index of [4,8]) {
        const p=chunk.platforms[index];if(!p)continue;
        const timed=["lightning","fallingRock","fireJet"].includes(type);
        chunk.hazards.push({id:`${prefix}:hazard:${index}`,type,x:p.x+p.w*0.55,y:p.y-(timed?78:19),w:timed?38:35,h:timed?78:19,
          period:4.2,warningDuration:0.9,activeDuration:0.65,phase:random()*4.2,timed,warning:false,active:!timed});
      }
    }
    if(tags.has("chase"))chunk.hazards.push({id:`${prefix}:hazard:chase`,type:"chase",x:offset-180,y:-300,w:100,h:1050,
      triggerX:offset+500,endX:offset+2760,speed:315+world.tier*4,active:false,triggered:false});
  }
  for(const field of stateFields)for(const item of chunk[field]){
    if(world._state.collected.has(item.id))item.collected=true;
    if(world._state.dead.has(item.id))item.dead=true;
    if(world._state.activated.has(item.id))item.activated=true;
  }
  for(const checkpoint of world.checkpoints.filter(c=>c.chunkIndex===descriptor.index)){
    checkpoint.activated=world._state.activated.has(checkpoint.id)||checkpoint.activated;
    const support=supportingSurface(chunk.platforms,checkpoint.x+PHYSICS.playerWidth/2,checkpoint.y+PHYSICS.playerHeight-4);
    if(support){checkpoint.floorY=surfaceY(support,checkpoint.x+PHYSICS.playerWidth/2);checkpoint.groundId=support.id;checkpoint.y=checkpoint.floorY-PHYSICS.playerHeight;}
    chunk.props.push({id:checkpoint.id+":art",x:checkpoint.x,y:checkpoint.floorY,w:52,h:92,kind:"checkpoint",checkpoint,activated:checkpoint.activated});
  }
  return chunk;
}

function captureChunk(world,chunk) {
  for(const field of stateFields)for(const item of chunk[field]||[]){
    if(item.collected)world._state.collected.add(item.id);
    if(item.dead&&field!=="platforms")world._state.dead.add(item.id);
    if(item.activated)world._state.activated.add(item.id);
  }
  for(const checkpoint of world.checkpoints)if(checkpoint.activated)world._state.activated.add(checkpoint.id);
}

export function updateWorldStreaming(world,playerX=0,cameraX=playerX) {
  if(!world._resident)return world;
  const center=Math.max(0,Math.min(world.chunkCount-1,Math.floor(playerX/CHUNK_WIDTH)));
  const start=Math.max(0,Math.min(world.chunkCount-world.residentLimit,center-1));
  if(start===world._streamStart){world.streaming.playerChunk=center;return world;}
  const end=Math.min(world.chunkCount-1,start+world.residentLimit-1);
  for(const [index,chunk]of world._resident)if(index<start||index>end){captureChunk(world,chunk);world._resident.delete(index);}
  for(let index=start;index<=end;index++)if(!world._resident.has(index))world._resident.set(index,stampChunk(world,world.chunkPlan[index]));
  world.chunks=[...world._resident.values()].sort((a,b)=>a.index-b.index);
  for(const field of stateFields)world[field]=world.chunks.flatMap(chunk=>chunk[field]);
  world._streamStart=start;world.streaming={start,end,resident:world.chunks.length,total:world.chunkCount,playerChunk:center};
  return world;
}

export function persistWorldState(world) {
  if(!world._state)return null;
  for(const chunk of world._resident.values())captureChunk(world,chunk);
  return{version:1,seed:world.seed,biome:world.biome,collected:[...world._state.collected].sort(),dead:[...world._state.dead].sort(),activated:[...world._state.activated].sort(),furthestX:world.furthestX||0};
}
export function restoreWorldState(world,snapshot,playerX=world.spawn.x) {
  if(!snapshot||!world._resident)return world;
  if(snapshot.seed!=null&&snapshot.seed!==world.seed)return world;
  world._state={collected:new Set(snapshot.collected||[]),dead:new Set(snapshot.dead||[]),activated:new Set(snapshot.activated||[])};
  world.furthestX=snapshot.furthestX||0;
  for(const c of world.checkpoints)c.activated=world._state.activated.has(c.id);
  world._resident.clear();world._streamStart=-1;updateWorldStreaming(world,playerX,playerX);return world;
}

// Previous callers receive the same long streamed world through this facade.
export function generateRoom({region="countryside",biome,depth=0,seed=1,journeyDifficulty=null,...rest}={}) {
  return createLevel({region,biome,seed,difficulty:journeyDifficulty||Math.min(6,1+Math.floor(depth/2)),...rest});
}
export { validateLevel, BIOME_ORDER, CHUNK_WIDTH };
