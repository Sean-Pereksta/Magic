// Persistent, DOM-free landscape progress. All distances and positions use meters.
// Encounter direction is intentionally separate: battles reset that short-term
// vector, while a land keeps its location across encounters and expeditions.
export const LANDS=Object.freeze([
  {id:'emerald-jungle',name:'Emerald Jungle',unlock:0,types:['leaf'],label:'Nature · Normal',description:'Follow winding rivers through a lush canopy of giant leaves and sunlit clearings.',landmark:'Hidden Canopy Temple',color:'#57d48a'},
  {id:'sunstone-savannah',name:'Sunstone Savannah',unlock:2000,types:['earth','fire'],label:'Earth · Fire',description:'Golden grass, warm stone arches, and shady acacias stretch toward the horizon.',landmark:'Sunstone Arch',color:'#edbe65'},
  {id:'crystal-caverns',name:'Crystal Caverns',unlock:6000,types:['earth','storm'],label:'Rock · Electric',description:'Glowing crystals light a mysterious underground river and glittering passages.',landmark:'Prismatic Grotto',color:'#a494f8'},
  {id:'whispering-marsh',name:'Whispering Marsh',unlock:12000,types:['water','shadow'],label:'Water · Poison',description:'Lilies, crooked roots, and firefly lights fill a dreamy emerald wetland.',landmark:'Firefly Lagoon',color:'#6fc4c0'},
  {id:'frostpeak-mountains',name:'Frostpeak Mountains',unlock:20000,types:['ice','storm'],label:'Ice · Wind',description:'Snowy pines and icy bridges lead beneath bright, towering mountain peaks.',landmark:'Aurora Summit',color:'#9bd9ef'},
  {id:'thunder-highlands',name:'Thunder Highlands',unlock:32000,types:['storm'],label:'Electric · Air',description:'Clouds roll between grassy sky cliffs and ancient stones charged with lightning.',landmark:'Stormwatch Plateau',color:'#b8b3fa'},
  {id:'ember-volcano',name:'Ember Volcano',unlock:48000,types:['fire','earth'],label:'Fire · Earth',description:'Lava ribbons weave through charcoal cliffs beneath a glowing volcanic crown.',landmark:'Ember Crater',color:'#fa9368'},
  {id:'moonlit-forest',name:'Moonlit Forest',unlock:72000,types:['shadow','light'],label:'Shadow · Psychic',description:'Moonbeams, blue mushrooms, and silver leaves illuminate an enchanted woodland.',landmark:'Moonwell Clearing',color:'#b4a3e8'},
  {id:'celestial-ruins',name:'Celestial Ruins',unlock:100000,types:['light'],label:'Light · Mystic',description:'Floating columns and starlit pools surround a forgotten sanctuary in the sky.',landmark:'Starlight Sanctuary',color:'#e9d7a0'},
  {id:'ancient-monkey-kingdom',name:'Ancient Monkey Kingdom',unlock:140000,types:['light','earth'],label:'Dragon · Ancient · Legendary',description:'Vine-covered golden towers reveal the grand home of the first monkey explorers.',landmark:'The Golden Crown',color:'#e9ba68'}
].map(land=>Object.freeze({...land,types:Object.freeze(land.types)})));

const byId=new Map(LANDS.map(land=>[land.id,land]));
const MAX_DISTANCE=1e9,MAX_POSITION=1e7,TRAIL_LIMIT=128;
const LEGACY_UNLOCKS=[0,500,1500,3000,5000,8000,12000,18000,25000,35000];
const isUnlocked=(land,world)=>world.unlocked.includes(land.id)||land.unlock<=world.totalDistance+1e-6;
const earned=(total,legacy=false)=>LANDS.filter((land,index)=>(legacy?LEGACY_UNLOCKS[index]:land.unlock)<=total+1e-6).map(land=>land.id);
const nonnegative=n=>Number.isFinite(n)?Math.max(0,Math.min(MAX_DISTANCE,n)):0;
const coordinate=n=>Number.isFinite(n)?Math.max(-MAX_POSITION,Math.min(MAX_POSITION,n)):0;
const heading=n=>Number.isFinite(n)?((n%360)+360)%360:0;
const point=p=>({east:coordinate(p?.east),north:coordinate(p?.north)});
const region=()=>({distance:0,heading:0,position:{east:0,north:0},trail:[{east:0,north:0}]});

// Older checkpoints never recorded cumulative walking. Retain the furthest
// verifiable expedition once; score is not distance, and histories can overlap.
function legacyDistance(s){
  let distance=nonnegative(s.run?.distance);
  for(const entry of s.meta?.history||[])if(!entry.mode||entry.mode===s.mode)distance=Math.max(distance,nonnegative(entry.feet)*.3048);
  return nonnegative(distance);
}

export function normalizeLands(s){
  if(!s||typeof s!=='object')throw Error('A trail save is required.');
  const old=s.world;
  if(!old||typeof old!=='object'||![1,2].includes(old.version)){
    const first=region();first.distance=legacyDistance(s);first.position=point(s.run?.vector);
    const length=Math.hypot(first.position.east,first.position.north),limit=nonnegative(s.run?.distance);
    if(length>limit&&length>0){first.position.east*=limit/length;first.position.north*=limit/length;}
    if(first.position.east||first.position.north)first.trail.push({...first.position});
    s.world={version:2,totalDistance:first.distance,unlocked:earned(first.distance,true),activeLand:LANDS[0].id,regions:{[LANDS[0].id]:first}};
  }else{
    // Restrict both imported data and the stored trail size before drawing it.
    const totalDistance=nonnegative(old.totalDistance),regions={},unlocked=[...new Set([...(old.version===1?earned(totalDistance,true):Array.isArray(old.unlocked)?old.unlocked.filter(id=>byId.has(id)):[]),...earned(totalDistance)])];
    for(const land of LANDS){
      const saved=old.regions?.[land.id];if(!saved||typeof saved!=='object')continue;
      const position=point(saved.position),trail=Array.isArray(saved.trail)?saved.trail.slice(-TRAIL_LIMIT).filter(p=>p&&Number.isFinite(p.east)&&Number.isFinite(p.north)).map(point):[];
      regions[land.id]={distance:Math.min(totalDistance,nonnegative(saved.distance)),heading:heading(saved.heading),position,trail:trail.length?trail:[{...position}]};
    }
    const requested=byId.get(old.activeLand),world={version:2,totalDistance,unlocked,regions},activeLand=requested&&isUnlocked(requested,world)?requested.id:LANDS[0].id;
    s.world={...world,activeLand};
  }
  s.world.regions[s.world.activeLand]??=region();return s.world;
}

export function landProgress(s){
  const world=normalizeLands(s),active=byId.get(world.activeLand),next=LANDS.find(land=>!isUnlocked(land,world))||null,current=world.regions[active.id];
  return{active,activeId:active.id,totalDistance:world.totalDistance,next,remaining:next?Math.max(0,next.unlock-world.totalDistance):0,
    unlocked:LANDS.filter(land=>isUnlocked(land,world)),position:current.position,heading:current.heading,region:current,
    lands:LANDS.map(land=>({...land,unlocked:isUnlocked(land,world),remaining:isUnlocked(land,world)?0:land.unlock-world.totalDistance}))};
}

export function selectLand(s,id){
  const land=byId.get(id);if(!land)throw Error('Choose a land from the world map.');
  const world=normalizeLands(s);if(!isUnlocked(land,world))throw Error('Keep walking to unlock this land.');
  if(!['trail','camp'].includes(s.run?.phase))throw Error('Finish this encounter before traveling to another land.');
  world.regions[id]??=region();world.activeLand=id;return land;
}

// Called by core.advance with only its accepted segment and proportional vector.
export function advanceLand(s,meters,east=0,north=0){
  if(s.run?.phase!=='trail'||![meters,east,north].every(Number.isFinite)||meters<=0)return 0;
  const world=normalizeLands(s),current=world.regions[world.activeLand],used=Math.min(meters,MAX_DISTANCE-world.totalDistance),length=Math.hypot(east,north),scale=length>used?used/length:1;
  if(used<=0)return 0;
  world.totalDistance+=used;world.unlocked=[...new Set([...world.unlocked,...earned(world.totalDistance)])];current.distance=Math.min(MAX_DISTANCE,current.distance+used);
  const position={east:coordinate(current.position.east+east*scale),north:coordinate(current.position.north+north*scale)},movedEast=position.east-current.position.east,movedNorth=position.north-current.position.north;
  if(movedEast||movedNorth)current.heading=heading(Math.atan2(movedEast,movedNorth)*180/Math.PI);
  current.position=position;
  const last=current.trail.at(-1);if(!last||Math.hypot(current.position.east-last.east,current.position.north-last.north)>=8){current.trail.push({...current.position});current.trail=current.trail.slice(-TRAIL_LIMIT);}
  return used;
}
