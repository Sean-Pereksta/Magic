// Region ecology, saved research, and claim-once adventures. No GPS coordinates
// are stored here: accepted distance and the existing fantasy map are enough.
import {SPECIES,ELEMENTS,DIRECTIONS,ITEMS} from './data.mjs';
import {LANDS,landProgress,normalizeLands} from './lands.mjs';
import {terrainAt} from './terrain.mjs';
import {recordTribeProgress} from './tribe-adventures.mjs';
import {awardGearCopy,normalizeEquipmentInventory} from './equipment-inventory.mjs';
import {normalizeLivingWorld,advanceLivingWorld,recordLivingWorldEvent} from './living-world.mjs';

const landById=new Map(LANDS.map(l=>[l.id,l]));
const regionId=(index)=>LANDS[index].id;
const rareHomes={
 wildvine:[0,1],tempestvine:[0,5],rootstone:[0,9],bramblebat:[0,7],
 kilnfist:[1,6],steampelt:[3,6],plasmafist:[5,6],magmajaw:[6,9],cinderfrost:[4,6],sunflare:[1,8],ashshade:[6,7],
 cascadeHowler:[3],thunderfang:[2,5],iciclefang:[4],dawncrest:[7,8],frostfern:[4],gloamorchid:[3,7],typhoontail:[3,5],
 glacierflow:[4],moonripple:[3,7],fulgurite:[2,5],auroraswing:[4,8],eclipsemask:[7,9],obsidianback:[6,9],
 thermosage:[3,6],glassmanta:[2,4],magnetuft:[2,5],lumenmoth:[7,8],brineveil:[3,7],haildrummer:[4,5],pyreclasp:[6,7]
};
const ultraHomes={solkar:[1,8],verdara:[0,9],tiderex:[3],voltaro:[5,8],cryomane:[4],obsidrax:[2,6],noctyra:[5,7],oroluma:[0,3],chronogibbon:[4,5],basaltoracle:[2,9],mirecrown:[3,7],phoenixmacaque:[1,6]};
const habitatNames={trail:'Trail',riverbank:'Riverbank',grove:'Grove',cave:'Cave formations',ruins:'Ancient ruins',landmark:'Landmark clearing'};
const habitatTypes={trail:[],riverbank:['water','ice'],grove:['leaf','shadow'],cave:['earth','ice'],ruins:['light','shadow'],landmark:['fire','storm','light']};
const researchDistance={common:1200,uncommon:1800,rare:3200,ultra:6000};
const isUltra=id=>SPECIES[id]?.rarity==='ultra'||SPECIES[id]?.ultra;
const finite=n=>Number.isFinite(n)&&n>=0?Math.min(n,1e9):0;
const unique=a=>[...new Set(a)];
const record=x=>x&&typeof x==='object'&&!Array.isArray(x);
const FEET=.3048;

export function speciesHomes(id){
 const p=SPECIES[id];if(!p)return [];
 if(p.region&&landById.has(p.region))return[p.region];
 const explicit=ultraHomes[id]||rareHomes[id];
 if(explicit)return explicit.map(regionId);
 if(!p.types.length)return LANDS.map(l=>l.id);
 const homes=LANDS.filter(l=>p.types.some(type=>l.types.includes(type))).map(l=>l.id);
 return homes.length?homes:[regionId(0)];
}
export function speciesMinimumFeet(id){
 const p=SPECIES[id];if(!p)return Infinity;
 if(isUltra(id))return Math.max(5000,p.minFeet||0);
 return Math.max(p.expansion?(p.minFeet||0):0,p.rarity==='rare'||p.types.length>1?10000:0);
}
export function regionSpeciesPool(s,candidates=Object.keys(SPECIES),{ultra=false}={}){
 const {activeId}=landProgress(s);
 return candidates.filter(id=>SPECIES[id]&&!!isUltra(id)===ultra&&speciesHomes(id).includes(activeId));
}
// The marker grid is the same one used by terrainMap. Water/path queries also
// come from the map renderer. Habitat never gates physical walking.
function currentHabitat(s){
 const {activeId,active,position:p}=landProgress(s),surface=terrainAt(activeId,p.east,p.north);
 const marker={east:Math.round((p.east-62)/540)*540+62,north:Math.round((p.north-36)/540)*540+36};
 const hash=((Math.floor(p.east/95)*73)^(Math.floor(p.north/95)*131))>>>0;
 const id=Math.hypot(p.east-marker.east,p.north-marker.north)<40?'landmark':surface.riverDistance<28?'riverbank':surface.kind==='trail'?'trail':['grove','cave','ruins'][hash%3];
 return {id,activeId,active};
}
export function habitatView(s){
 const {id,activeId,active}=currentHabitat(s);
 const likely=regionSpeciesPool(s,Object.keys(SPECIES),{ultra:false}).filter(id=>speciesMinimumFeet(id)<=s.run.distance/FEET).sort((a,b)=>habitatAffinity(b,id)-habitatAffinity(a,id)||SPECIES[a].number-SPECIES[b].number).slice(0,4);
 return{id,name:id==='landmark'?active.landmark:habitatNames[id],landId:activeId,types:habitatTypes[id],likely,
  description:`${habitatTypes[id].length?habitatTypes[id].map(t=>ELEMENTS[t].name).join(' and ')+' companions are more likely here.':'The open trail has no additional habitat preference.'} Region and expedition depth determine eligibility; the compass adjusts odds within that pool. The two introductory encounters use fixed companions. Walk any safe, accessible route.`};
}
const habitatAffinity=(species,habitat)=>SPECIES[species]?.habitats?.includes(habitat)?3:SPECIES[species]?.types.some(t=>habitatTypes[habitat]?.includes(t))?2.5:1;
export function regionalWeight(s,id){
 const h=currentHabitat(s),p=SPECIES[id],weights=s.run.weights||{},total=Object.values(weights).reduce((n,v)=>n+finite(v),0);
 const compass=total?Object.entries(DIRECTIONS).reduce((n,[direction,type])=>n+(p?.types.includes(type)?finite(weights[direction])/total:0),0):0;
 return habitatAffinity(id,h.id)*(1+compass*.8);
}
export function regionSnapshot(s){const h=habitatView(s);return{landId:h.landId,habitat:h.id};}

const THEMES=[
 ['The Hidden Canopy Trail','A leaf-marked path bends toward the hidden temple.','Canopy rubbing','Sunfruit plant','rootstone','Canopy keepsake','Canopy Guardian'],
 ['The Disturbed Supply Trail','Scattered straps lead from an abandoned supply trail to the arch.','Broken supply seal','Abandoned pack','kilnfist','Sunstone crest','Sunstone Sentinel'],
 ['The Crystal Echo','A distant chime answers the footsteps in the cavern.','Echo crystal','Prismatic cluster','magnetuft','Prismatic charm','Grotto Guardian'],
 ['The Firefly Nest','Fireflies gather around a nest disturbed by a restless lagoon defender.','Reed clue','Moonreed fruit','moonripple','Firefly lantern','Lagoon Protector'],
 ['The Frozen Markers','Frosted trail markers point toward a shelter below the aurora.','Frozen marker','Frost crystal','iciclefang','Aurora ribbon','Summit Guardian'],
 ['Stormwatch Restoration','Silent storm markers need their missing carved stones returned.','Storm inscription','Carved storm stone','thunderfang','Stormwatch pennant','Stormwatch Warden'],
 ['The Obsidian Passage','Fresh cracks in the obsidian reveal a passage to the crater.','Obsidian shard','Crater shrine piece','magmajaw','Obsidian seal','Crater Guardian'],
 ['Signs of the Moonwell','Silver woodland signs lead toward a quiet, changing moonwell.','Silverleaf sign','Moonwell bloom','eclipsemask','Moonwell pendant','Moonwell Sentinel'],
 ['The Sanctuary Beacons','A ruined sanctuary waits for its scattered beacon pieces.','Beacon inscription','Ancient beacon piece','dawncrest','Starlight emblem','Sanctuary Guardian'],
 ['The Golden Crown','Ancient tablets reveal the first explorers and their final guardian.','Royal tablet rubbing','Golden shrine piece','obsidianback','Golden Crown stamp','Crown Guardian']
];
export const REGION_ADVENTURES=Object.freeze(LANDS.map((land,index)=>{
 const [title,story,clue,object,guardian,keepsake,guardianName]=THEMES[index];
 const order=index%3===1?[0,2,1,3,4,5,6]:index%3===2?[0,1,3,2,4,5,6]:[0,1,2,3,4,5,6];
 const actions=[
  {kind:'distance',title:`Discover ${land.landmark}`,goal:300+index*40,unit:'m',text:story,reward:{fruit:1,items:{},essence:{}}},
  {kind:'discoveries',title:`Investigate the ${clue.toLowerCase()}`,goal:3+Math.floor(index/3),unit:'discoveries',text:`Look through accepted trail discoveries for the ${clue.toLowerCase()}.`,reward:{fruit:1,items:{berry:1},essence:{}}},
  {kind:'objects',title:`Find the ${object.toLowerCase()}`,goal:420+index*45,unit:'m',text:`Explore any safe route to uncover the ${object.toLowerCase()}. Claim it once the trail bar fills.`,reward:{fruit:2,items:{},essence:{[land.types[0]]:1}}},
  {kind:'seen',title:'Meet the local companions',goal:index<3?1:2,unit:'species',text:`Win encounters with different ${land.types.map(t=>ELEMENTS[t].name).join(' or ')} companions from this region. Recruitment remains your choice.`,reward:{fruit:1,items:{bondCharm:1},essence:{}}},
  {kind:'victories',title:`Secure the ${index%2?'supply trail':'landmark approach'}`,goal:2,unit:'wins',text:'Prepare elemental counters and win two wild encounters from this region.',reward:{fruit:2,items:{tonic:1},essence:{}}},
  {kind:'guardian',title:`Challenge the ${guardianName}`,goal:1,unit:'guardian',text:`A ${SPECIES[guardian].name} guards the final passage. Its intent reveals when to defend, break a shield, or switch. Requires ${speciesMinimumFeet(guardian).toLocaleString()} accepted expedition feet.`,reward:{fruit:2,items:{salve:1},essence:{}}},
  {kind:'distance',title:`Return with the ${keepsake}`,goal:350+index*35,unit:'m',text:`Carry the story home along any safe route and earn your ${keepsake}.`,reward:{fruit:2,items:{},essence:{[land.types[0]]:2},gear:[['trailfinderCompass','charcoalHeart','staticRelayCharm','nightfangTooth','quakehideBelt','trailfinderCompass','resourceLens','dawnRelayBell','dewdropChalice','briarloopBand'][index]],cosmetic:`${land.id}:keepsake`}}
 ];
 return Object.freeze({id:land.id,landId:land.id,title,story,guardian,guardianName,keepsake,steps:Object.freeze(order.map((i,n)=>Object.freeze({...actions[i],id:`adventure:${land.id}:${n}`,landId:land.id,index:n})))});
}));
const adventureById=new Map(REGION_ADVENTURES.map(a=>[a.id,a]));
const stepById=new Map(REGION_ADVENTURES.flatMap(a=>a.steps.map(step=>[step.id,step])));

export function normalizeAdventures(s){
 normalizeLands(s);const old=record(s.meta.adventures)?s.meta.adventures:{},regions={};
 for(const a of REGION_ADVENTURES){
  const saved=record(old.regions?.[a.id])?old.regions[a.id]:{};
  const claimed=unique((Array.isArray(saved.claimed)?saved.claimed:[]).filter(id=>a.steps.some(step=>step.id===id)));
  // A valid sequence is a prefix; preserve every claim while exposing only the
  // earliest missing step. A damaged import cannot manufacture future progress.
  const next=a.steps.find(step=>!claimed.includes(step.id));
  regions[a.id]={claimed,current:finite(saved.current),seen:unique((Array.isArray(saved.seen)?saved.seen:[]).filter(id=>SPECIES[id])).slice(0,Object.keys(SPECIES).length),stepId:next?.id||null};
  if(saved.stepId!==regions[a.id].stepId){regions[a.id].current=0;regions[a.id].seen=[];}
 }
 const hunts={};for(const [id,h]of Object.entries(record(old.hunts)?old.hunts:{}))if(SPECIES[id])hunts[id]={meters:finite(h?.meters),attempts:Math.floor(finite(h?.attempts))};
 const cosmetics=unique((Array.isArray(old.cosmetics)?old.cosmetics:[]).filter(id=>typeof id==='string'&&id.endsWith(':keepsake')&&landById.has(id.slice(0,-9))));
 s.meta.adventures={version:1,regions,hunts,cosmetics};normalizeLivingWorld(s);return s.meta.adventures;
}
function unlocked(s,landId){return s.world.unlocked.includes(landId);}
function currentStep(s,landId){const saved=normalizeAdventures(s),a=adventureById.get(landId);return a?.steps.find(step=>step.id===saved.regions[landId].stepId)||null;}
export function adventureSteps(s,landId=landProgress(s).activeId){
 const saved=normalizeAdventures(s),a=adventureById.get(landId);if(!a)throw Error('Choose a region.');const r=saved.regions[landId];
 return a.steps.map(step=>{const claimed=r.claimed.includes(step.id),active=step.id===r.stepId,current=claimed?step.goal:active?Math.min(step.goal,r.current):0;return{...step,current,complete:current+1e-6>=step.goal,claimed,active,unlocked:unlocked(s,landId),percent:Math.min(100,current/step.goal*100),tracked:s.meta.pinnedGoal?.kind==='adventure'&&s.meta.pinnedGoal.id===step.id};});
}
export function adventureView(s,landId=landProgress(s).activeId){const a=adventureById.get(landId);if(!a)throw Error('Choose a region.');return{...a,steps:adventureSteps(s,landId)};}
export function trackAdventure(s,id){
 const step=stepById.get(id);if(!step)throw Error('Choose an adventure step.');const row=adventureSteps(s,step.landId).find(x=>x.id===id);
 if(row.claimed)throw Error('This adventure reward is already claimed.');
 s.meta.pinnedGoal={kind:'adventure',id};if(s.meta.regionChallenges)s.meta.regionChallenges.tracked=null;return row;
}
export function trackHunt(s,id){if(!SPECIES[id])throw Error('Choose a monkey from the collection.');normalizeAdventures(s);s.meta.adventures.hunts[id]??={meters:0,attempts:0};s.meta.pinnedGoal={kind:'hunt',id};if(s.meta.regionChallenges)s.meta.regionChallenges.tracked=null;return huntView(s);}
export function speciesLocation(s,id){
 const p=SPECIES[id];if(!p)throw Error('Unknown species.');const homes=speciesHomes(id),progress=landProgress(s),available=homes.filter(id=>unlocked(s,id)),requiredFeet=speciesMinimumFeet(id),depthReady=s.run.distance/FEET+1e-6>=requiredFeet,winReady=!isUltra(id)||s.run.ordinaryWins>=2;
 const preferred=p.habitats?.length?p.habitats.filter(id=>habitatNames[id]):Object.entries(habitatTypes).filter(([,types])=>p.types.some(t=>types.includes(t))).map(([id])=>id);
 const activeHere=homes.includes(progress.activeId),landId=activeHere?progress.activeId:available[0]||homes[0],land=landById.get(landId);
 const nextStep=!available.length?`Walk ${Math.ceil(Math.max(0,land.unlock-progress.totalDistance))} more accepted meters to unlock ${land.name}.`:!activeHere?`Visit ${land.name}.`:!depthReady?`Reach ${Math.ceil(requiredFeet).toLocaleString()} accepted expedition feet (${Math.ceil(requiredFeet-s.run.distance/FEET).toLocaleString()} remaining).`:!winReady?'Complete the two introductory wild victories.':`Explore ${preferred.length?preferred.map(id=>habitatNames[id].toLowerCase()).join(' or '):'the trails'}; accepted research can reveal a targeted encounter.`;
 return{species:id,name:p.name,rarity:p.rarity,homes:homes.map(id=>({...landById.get(id),unlocked:unlocked(s,id)})),landId,habitats:preferred.length?preferred:['trail'],unlocked:available.length>0,depthReady,requiredFeet,available:available.length>0&&activeHere&&depthReady&&winReady,nextStep,
  availability:isUltra(id)?'Particularly rare Ultra sighting':p.rarity==='rare'?'Rare regional encounter':p.rarity==='uncommon'?'Uncommon regional encounter':'Common regional encounter'};
}
export function huntView(s,id=s.meta.pinnedGoal?.kind==='hunt'?s.meta.pinnedGoal.id:null){
 if(!id||!SPECIES[id])return null;const saved=normalizeAdventures(s),location=speciesLocation(s,id),h=saved.hunts[id]||{meters:0,attempts:0},goal=researchDistance[SPECIES[id].rarity]||1800;
 return{...location,id,current:Math.min(goal,h.meters),goal,percent:Math.min(100,h.meters/goal*100),ready:h.meters+1e-6>=goal&&location.available,attempts:h.attempts};
}
export function advanceAdventures(s,meters){
 if(s.run?.phase!=='trail'||!Number.isFinite(meters)||meters<=0)return;
 advanceLivingWorld(s,meters);
 const saved=normalizeAdventures(s),landId=s.world.activeLand,r=saved.regions[landId],step=stepById.get(r.stepId);
 if(step&&['distance','objects'].includes(step.kind))r.current=Math.min(step.goal,r.current+meters);
 const hunt=huntView(s);if(hunt?.available){const h=s.meta.adventures.hunts[hunt.id]??={meters:0,attempts:0};h.meters=Math.min(hunt.goal,h.meters+meters);}
}
// Called once from the corresponding completed core event. Social battle copies
// never call this path. The origin land comes from the frozen encounter record.
export function recordAdventureEvent(s,kind,amount=1,landId=landProgress(s).activeId,details={}){
 if(!['discoveries','victories','seen','guardian'].includes(kind)||!Number.isSafeInteger(amount)||amount<=0||amount>1000||!landById.has(landId))return;
 recordLivingWorldEvent(s,kind,amount,landId,details);
 const saved=normalizeAdventures(s);if(!unlocked(s,landId))return;const r=saved.regions[landId],step=stepById.get(r.stepId);if(!step||step.kind!==kind)return;
 if(kind==='seen'){
  const ids=Array.isArray(details.species)?details.species:[details.species];
  for(const id of ids)if(SPECIES[id]&&speciesHomes(id).includes(landId)&&SPECIES[id].types.some(t=>landById.get(landId).types.includes(t))&&!r.seen.includes(id))r.seen.push(id);
  r.current=Math.min(step.goal,r.seen.length);
 }else r.current=Math.min(step.goal,r.current+amount);
}
function canClaim(s){return ['trail','camp'].includes(s.run?.phase)&&s.run.party.some(id=>s.collection[id]?.hp>0);}
export function claimAdventure(s,id){
 const step=stepById.get(id);if(!step)throw Error('Choose an adventure step.');const row=adventureSteps(s,step.landId).find(x=>x.id===id);
 if(row.claimed)throw Error('This adventure reward is already claimed.');if(!row.active||!row.complete||!row.unlocked)throw Error('Complete this adventure step first.');if(!canClaim(s))throw Error('Return to a living trail or camp to claim this reward.');
 const reward=step.reward,r=s.run;
 if(!Number.isSafeInteger(r.fruit)||r.fruit<0||!record(r.items)||!record(r.essence)||Object.entries(reward.items).some(([id,n])=>!ITEMS[id]||!Number.isSafeInteger(r.items[id])||r.items[id]<0||r.items[id]+n>Number.MAX_SAFE_INTEGER)||r.fruit+reward.fruit>Number.MAX_SAFE_INTEGER||Object.entries(reward.essence).some(([id,n])=>!ELEMENTS[id]||!Number.isSafeInteger(r.essence[id])||r.essence[id]<0||r.essence[id]+n>Number.MAX_SAFE_INTEGER))throw Error('Your expedition supplies are unavailable.');
 if(reward.gear?.length)normalizeEquipmentInventory(s);
 for(const itemId of reward.gear||[]){const awarded=awardGearCopy(s,itemId,{source:`milestone:${id}`});if(awarded.kind==='salvaged'){s.meta.fragments??={};s.meta.fragments[itemId]=(s.meta.fragments[itemId]||0)+5;}}
 r.fruit+=reward.fruit;for(const [id,n]of Object.entries(reward.items))r.items[id]+=n;for(const [id,n]of Object.entries(reward.essence))r.essence[id]+=n;
 const saved=s.meta.adventures,local=saved.regions[step.landId];local.claimed.push(id);local.current=0;local.seen=[];local.stepId=adventureById.get(step.landId).steps.find(x=>!local.claimed.includes(x.id))?.id||null;
 if(reward.cosmetic&&!saved.cosmetics.includes(reward.cosmetic))saved.cosmetics.push(reward.cosmetic);
 if(!local.stepId)recordTribeProgress(s,{challenges:1},`regional-adventure:${step.landId}`);
 if(s.meta.pinnedGoal?.kind==='adventure'&&s.meta.pinnedGoal.id===id)s.meta.pinnedGoal=local.stepId?{kind:'adventure',id:local.stepId}:null;
 return JSON.parse(JSON.stringify(reward));
}
export function guardianChallenge(s,landId=landProgress(s).activeId){
 const row=currentStep(s,landId),a=adventureById.get(landId);if(!a||row?.kind!=='guardian'||!canClaim(s)||s.world.activeLand!==landId)throw Error('Reach this region’s guardian adventure step before challenging it.');
 if(!unlocked(s,landId))throw Error('Unlock this region first.');
 if(s.meta.adventures.regions[landId].current>=row.goal)throw Error('The guardian is defeated. Claim the adventure reward.');
 if(s.run.distance/FEET+1e-6<speciesMinimumFeet(a.guardian))throw Error(`Reach ${speciesMinimumFeet(a.guardian).toLocaleString()} accepted expedition feet before challenging this guardian.`);
 return{species:a.guardian,landId,habitat:'landmark',guardian:true,id:row.id,name:a.guardianName};
}
export function consumeReadyHunt(s){
 const h=huntView(s);if(!h?.ready||!['trail','camp'].includes(s.run?.phase))return null;
 const saved=s.meta.adventures.hunts[h.id];saved.meters=0;saved.attempts++;
 return{species:h.id,landId:s.world.activeLand,habitat:habitatView(s).id,ultra:!!isUltra(h.id),hunt:true,id:`hunt:${h.id}:${saved.attempts}`};
}
export function trackedRegionalGoal(s){
 const pin=s.meta.pinnedGoal;if(pin?.kind==='hunt'){const h=huntView(s);return h?{...h,kind:'hunt',title:`Find ${h.name}`,unit:'m',description:h.ready?'Research complete · stop safely to begin.':h.nextStep}:null;}
 if(pin?.kind==='adventure'){const step=stepById.get(pin.id);return step?{...adventureSteps(s,step.landId).find(x=>x.id===pin.id),kind:'adventure',description:step.text}:null;}
 return null;
}
