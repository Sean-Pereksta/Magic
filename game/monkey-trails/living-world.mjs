// Persistent regional opportunities. Only accepted core walking and completed
// battles advance these goals. All map points are relative fantasy coordinates.
import {LANDS,normalizeLands,landProgress} from './lands.mjs';
import {SPECIES,ELEMENTS,GEAR,ITEMS} from './data.mjs';
import {awardGearCopy,normalizeEquipmentInventory,assertGearAwardCapacity} from './equipment-inventory.mjs';
import {equipmentSpecialDiscovery,equipmentLocationBonusChance} from './equipment-trail.mjs';
import {recordTribeProgress} from './tribe-adventures.mjs';
import {recordLegacyLocation,recordLegacyMilestone} from './legacy.mjs';

export const WORLD_LIMITS=Object.freeze({minimumSpawnFeet:1500,maximumSpawnFeet:3500,activePerRegion:3,arrivalMeters:35,tickets:10000});
const FEET=.3048,landById=new Map(LANDS.map(l=>[l.id,l])),record=v=>v&&typeof v==='object'&&!Array.isArray(v);
const safe=n=>Number.isSafeInteger(n)&&n>=0,finite=n=>Number.isFinite(n)&&n>=0?Math.min(1e9,n):0,copy=v=>JSON.parse(JSON.stringify(v));
const hash=value=>{let h=2166136261;for(const c of String(value)){h^=c.charCodeAt(0);h=Math.imul(h,16777619);}return h>>>0;};
const rng=s=>{let n=s.rng|0;n^=n<<13;n^=n>>>17;n^=n<<5;s.rng=(n>>>0)||1234567;return s.rng/4294967296;};
const pick=(s,rows)=>rows[Math.floor(rng(s)*rows.length)];
const alive=s=>['trail','camp'].includes(s.run?.phase)&&s.run.party?.some(id=>s.collection?.[id]?.hp>0);
const requireAlive=s=>{if(!alive(s))throw Error('Return to a living trail or camp before visiting or collecting rewards.');};
const metaRarity=id=>GEAR[id]?.rarity||({trailfinderCompass:'rare',resourceLens:'rare',briarloopBand:'rare',quakehideBelt:'rare',charcoalHeart:'rare',staticRelayCharm:'rare',nightfangTooth:'rare',dawnRelayBell:'rare',dewdropChalice:'rare',frostguardBrooch:'rare'})[id]||'common';
const questKinds=['distance','discoveries','types','essence','tactics','rescue','landmarks'];
const questNames={distance:'Explore the hidden paths',discoveries:'Recover scattered supplies',types:'Study the regional companions',essence:'Gather elemental echoes',tactics:'Practice a clever counter',rescue:'Guide a lost companion home',landmarks:'Discover a changing landmark'};
function questDefinition(land,slot,generation){
 const index=LANDS.indexOf(land),kind=questKinds[(slot*2+generation)%questKinds.length],element=land.types[generation%land.types.length];
 const goal=({distance:350+index*45,discoveries:4+Math.floor(index/3),types:2,essence:4+Math.floor(index/3),tactics:1,rescue:500+index*40,landmarks:1})[kind];
 const description=({distance:'Explore any safe route in this region.',discoveries:'Make accepted trail discoveries in this region.',types:`Win wild encounters against ${land.types.map(t=>ELEMENTS[t].name).join(' or ')} companions.`,essence:`Find ${ELEMENTS[element].name} essence through trail discoveries.`,tactics:'Win a wild battle after blocking, interrupting, or making a defensive counter.',rescue:'Walk any safe route to escort a lost companion to shelter.',landmarks:'Visit one special destination in this region.'})[kind];
 return{id:`sidequest:${land.id}:${slot}:${generation}`,landId:land.id,slot,generation,kind,element,title:`${questNames[kind]} · ${generation+1}`,goal,unit:['distance','rescue'].includes(kind)?'m':kind==='types'?'wins':kind==='essence'?'essence':'steps',description,
  reward:{fruit:4+index+Math.floor(generation/3),items:{[kind==='rescue'?'bondCharm':kind==='tactics'?'salve':'berry']:1},essence:{[element]:2},...(generation%3===2?{materials:3+Math.floor(index/2)}:{})}};
}
const mainThemes=[
 ['canopy-temple','Canopy Offering Temple','temple','offering',{fruit:10},'Place a fruit offering beneath the living canopy.'],
 ['mirage-bazaar','Mirage Bazaar','merchant','bargain',{essence:{earth:4},items:{berry:2}},'Follow the merchant’s clues through three thoughtful bargains.'],
 ['resonance-chamber','Resonance Chamber','crystal','pattern',{essence:{earth:3}},'Repeat the displayed elemental pattern to awaken the crystal.'],
 ['lantern-ferry','Lantern Ferry','ferry','rescue',{items:{tonic:1}},'Escort stranded companions along any safe route.'],
 ['frozen-monastery','Frozen Monastery','monastery','defense',{},'Win three guardian trials, using a block or defensive counter in each.'],
 ['lightning-spire','Lightning Spire','spire','counter',{},'Win two trials with a successful interrupt or defensive counter. Timing is optional.'],
 ['ember-forge','Ancient Ember Forge','forge','forge',{essence:{fire:5},items:{charmMaterial:4}},'Choose a resource sacrifice to improve this special craft’s quality.'],
 ['wishing-moonwell','Wishing Moonwell','well','offering',{essence:{shadow:4,light:4}},'Offer paired moonlight and shadow for a mysterious reward.'],
 ['astral-observatory','Astral Observatory','observatory','constellation',{essence:{light:4}},'Connect the displayed constellation to reveal resources or a rare encounter.'],
 ['crown-vault','Crown Vault','vault','guardian',{},'Defeat a three-stage guardian crew to open the royal treasure vault.']
];
const smallerKinds=[['cache','Hidden Cache','cache','explore'],['shrine','Abandoned Shrine','shrine','offering'],['merchant','Wandering Merchant','merchant','bargain'],['ruins','Guarded Ruins','ruins','guardian'],['pool','Elemental Pool','pool','pattern'],['den','Rare-Monkey Den','den','explore'],['arena','Temporary Challenge Arena','arena','counter']];
export const TEMPLE_DIRECTIONS=Object.freeze([
 {id:'east',name:'Dawnstride Temple',label:'East',axis:'east',sign:1,element:'light',item:'bondCharm'},
 {id:'west',name:'Duskpath Temple',label:'West',axis:'east',sign:-1,element:'shadow',item:'treasureScent'},
 {id:'south',name:'Sunfire Temple',label:'South',axis:'north',sign:-1,element:'fire',item:'tonic'},
 {id:'north',name:'Northwind Temple',label:'North',axis:'north',sign:1,element:'ice',item:'revive'}
].map(Object.freeze));
export const SPECIAL_LOCATIONS=Object.freeze(LANDS.flatMap((land,index)=>{
 const [key,name,icon,interaction,cost,description]=mainThemes[index];
 const main={id:`${land.id}:${key}`,landId:land.id,key,name,icon,interaction,cost,description,main:true,element:land.types[0],stages:['defense','guardian'].includes(interaction)?3:interaction==='counter'?2:interaction==='bargain'?3:['pattern','constellation'].includes(interaction)?4:1};
 const small=smallerKinds.map(([key,name,icon,interaction])=>({id:`${land.id}:${key}`,landId:land.id,key,name:`${land.name.split(' ')[0]} ${name}`,icon,interaction,cost:key==='merchant'?{fruit:6}:key==='shrine'?{essence:{[land.types[0]]:2}}:{},description:({cache:'Explore a short safe route to uncover forgotten supplies.',shrine:'Leave an elemental offering for the region’s companions.',merchant:'Read a wandering merchant’s clue and choose a fair bargain.',ruins:'Defeat the watchful guardian to recover the lost cache.',pool:'Repeat the elemental ripples to gather concentrated essence.',den:'Explore carefully to reveal a recruitable rare companion.',arena:'Win with an interrupt or defensive counter to earn the arena cache.'})[key],main:false,element:land.types[0],stages:interaction==='pattern'?3:1}));
 const temples=TEMPLE_DIRECTIONS.map((d,i)=>({id:`${land.id}:temple-${d.id}`,landId:land.id,key:`temple-${d.id}`,name:d.name,icon:'temple',interaction:'directional',direction:d.id,directionLabel:d.label,goalFeet:500+index*100+i*250,cost:{},description:`After visiting, walk ${500+index*100+i*250} feet ${d.label.toLowerCase()} to awaken this temple. Earn a guaranteed supply and elemental essence, plus a mystery reward.`,main:false,element:d.element,stages:1}));
 return[Object.freeze(main),...small.map(Object.freeze),...temples.map(Object.freeze)];
}));
const siteById=new Map(SPECIAL_LOCATIONS.map(l=>[l.id,l]));
const trialSpecies=['rootstone','kilnfist','magnetuft','moonripple','iciclefang','thunderfang','magmajaw','eclipsemask','dawncrest','obsidianback'];
function spawnGap(landId,serial){const i=LANDS.findIndex(l=>l.id===landId);return(1500+(hash(`${landId}:${serial}:gap`)%1601)+i*44)*FEET;}
function nextQuest(slot){return{generation:safe(slot?.generation)?Math.min(1000000,slot.generation):0,current:finite(slot?.current)};}
export function normalizeLivingWorld(s){
 normalizeLands(s);s.meta??={};const old=record(s.meta.livingWorld)?s.meta.livingWorld:{},regions={};
 for(const land of LANDS){const r=record(old.regions?.[land.id])?old.regions[land.id]:{};regions[land.id]={slots:Array.from({length:3},(_,i)=>nextQuest(r.slots?.[i])),completed:safe(r.completed)?Math.min(1000000,r.completed):0,chain:safe(r.chain)?Math.min(3,r.chain):0,visits:safe(r.visits)?Math.min(1000000,r.visits):0,spawnSerial:safe(r.spawnSerial)?Math.min(1000000,r.spawnSerial):0,spawnMeters:finite(r.spawnMeters)};}
 const locations={};
 for(const[id,l]of Object.entries(record(old.locations)?old.locations:{})){
  const def=siteById.get(l?.definitionId);if(!def||l.id!==id||!Number.isSafeInteger(l.serial)||l.serial<1||l.serial>regions[def.landId].spawnSerial||id!==`location:${def.landId}:${l.serial}`||!['discovered','visited','claimed','dismissed'].includes(l.status))continue;
  locations[id]={id,definitionId:def.id,serial:l.serial,landId:def.landId,status:l.status,position:{east:Number.isFinite(l.position?.east)?Math.max(-1e7,Math.min(1e7,l.position.east)):0,north:Number.isFinite(l.position?.north)?Math.max(-1e7,Math.min(1e7,l.position.north)):0},safeRoute:!!l.safeRoute,approachMeters:finite(l.approachMeters),challengeMeters:finite(l.challengeMeters),templeAnyRoute:!!l.templeAnyRoute,stage:Math.min(def.stages,Math.floor(finite(l.stage))),patternIndex:Math.min(def.stages,Math.floor(finite(l.patternIndex))),boosted:!!l.boosted,battleSerial:Math.floor(finite(l.battleSerial)),receipt:record(l.receipt)?copy(l.receipt):null,encounterTaken:!!l.encounterTaken};
 }
 // Retain active opportunities and the latest receipt; a region's monotonic
 // serial makes discarded historic IDs permanently ineligible for a new claim.
 const pendingEncounter=l=>l.status==='claimed'&&l.receipt?.reward?.encounter&&!l.encounterTaken;
 const recent=Object.values(locations).filter(l=>['claimed','dismissed'].includes(l.status)&&!pendingEncounter(l)).sort((a,b)=>b.serial-a.serial).slice(0,12);
 for(const l of Object.values(locations))if(['claimed','dismissed'].includes(l.status)&&!pendingEncounter(l)&&!recent.includes(l))delete locations[l.id];
 const receipts=record(old.receipts)?old.receipts:{};
 s.meta.livingWorld={version:1,regions,locations,pinned:typeof old.pinned==='string'?old.pinned:null,lastReceipt:record(old.lastReceipt)?copy(old.lastReceipt):null,receipts};
 const w=s.meta.livingWorld;if(w.pinned&&!locations[w.pinned]&&!Object.values(regions).some((r)=>r.slots.some((slot,i)=>questDefinition(landById.get(Object.keys(regions).find(k=>regions[k]===r)),i,slot.generation).id===w.pinned)))w.pinned=null;
 return w;
}
export function rotatingSideQuests(s,landId=landProgress(s).activeId){
 const w=normalizeLivingWorld(s),land=landById.get(landId);if(!land)throw Error('Choose a region.');
 return w.regions[landId].slots.map((slot,i)=>{const def=questDefinition(land,i,slot.generation),current=Math.min(def.goal,slot.current);return{...def,current,percent:Math.min(100,current/def.goal*100),complete:current+1e-6>=def.goal,unlocked:s.world.unlocked.includes(landId),pinned:w.pinned===def.id};});
}
export function regionalAchievement(s,landId=landProgress(s).activeId){const w=normalizeLivingWorld(s),r=w.regions[landId];if(!r)throw Error('Choose a region.');const goal=[3,9,18][r.chain]||18;return{landId,stage:r.chain,current:r.completed,goal,percent:Math.min(100,r.completed/goal*100),claimed:r.chain===3,complete:r.chain<3&&r.completed>=goal,title:['Regional helper','Regional pathfinder','Regional champion'][r.chain]||'Regional champion',reward:{fruit:10+10*r.chain,essence:{[landById.get(landId).types[0]]:5+5*r.chain},gearRarity:['rare','epic','legendary'][r.chain]||'legendary'}};}
function advanceQuest(w,landId,kind,amount,details={}){const land=landById.get(landId);if(!land)return;for(const[slot,r]of w.regions[landId].slots.entries()){const q=questDefinition(land,slot,r.generation);if(q.kind===kind&&(kind!=='essence'||q.element===details.element))r.current=Math.min(q.goal,r.current+amount);}}
export function recordLivingWorldEvent(s,kind,amount=1,landId=landProgress(s).activeId,details={}){
 if(!Number.isFinite(amount)||amount<=0||amount>1000||!landById.has(landId))return;
 const w=normalizeLivingWorld(s);if(!s.world.unlocked.includes(landId))return;
 if(kind==='discoveries'){advanceQuest(w,landId,'discoveries',amount);const d=s.run.lastDiscovery;if(d?.category==='essence'||d?.kind==='essence'||d?.type==='essence'){const element=d.element||d.id;if(ELEMENTS[element])advanceQuest(w,landId,'essence',d.amount||1,{element});}}
 if(kind==='seen'){const ids=Array.isArray(details.species)?details.species:[details.species];if(ids.some(id=>SPECIES[id]?.types.some(t=>landById.get(landId).types.includes(t))))advanceQuest(w,landId,'types',1);}
}
export function advanceLivingWorld(s,meters,east=0,north=0){
 if(s.run?.phase!=='trail'||!Number.isFinite(meters)||meters<=0)return;
 const w=normalizeLivingWorld(s),landId=s.world.activeLand,r=w.regions[landId];advanceQuest(w,landId,'distance',meters);advanceQuest(w,landId,'rescue',meters);
 for(const l of Object.values(w.locations))if(l.landId===landId){if(l.status==='discovered'&&l.safeRoute)l.approachMeters+=meters;if(l.status==='visited'){const def=siteById.get(l.definitionId);let progress=meters;if(def.interaction==='directional'&&!l.templeAnyRoute){const d=TEMPLE_DIRECTIONS.find(d=>d.id===def.direction),length=Math.hypot(east,north);const scale=length>meters?meters/length:1;progress=Number.isFinite(east)&&Number.isFinite(north)?Math.max(0,(d.axis==='east'?east:north)*d.sign*scale):0;}l.challengeMeters=Math.min(1e9,l.challengeMeters+progress);}}
 const gap=Math.max(WORLD_LIMITS.minimumSpawnFeet*FEET,spawnGap(landId,r.spawnSerial)*(1-equipmentSpecialDiscovery(s))),active=Object.values(w.locations).filter(l=>l.landId===landId&&(['discovered','visited'].includes(l.status)||l.status==='claimed'&&l.receipt?.reward?.encounter&&!l.encounterTaken));r.spawnMeters=Math.min(gap,r.spawnMeters+meters);
 if(r.spawnMeters+1e-6<gap||active.length>=WORLD_LIMITS.activePerRegion||r.spawnSerial>=1000000)return;
 r.spawnMeters=0;const serial=++r.spawnSerial,defs=SPECIAL_LOCATIONS.filter(l=>l.landId===landId),main=serial===1||hash(`${landId}:${serial}:rarity`)%100<35,def=serial>=2&&serial<=5?defs.find(d=>d.direction===TEMPLE_DIRECTIONS[serial-2].id):main?defs[0]:defs[1+hash(`${landId}:${serial}:kind`)%(defs.length-1)],position=s.world.regions[landId].position,angle=hash(`${landId}:${serial}`)%360*Math.PI/180,radius=100+hash(`${landId}:${serial}:radius`)%91,id=`location:${landId}:${serial}`;
 w.locations[id]={id,definitionId:def.id,landId,serial,status:'discovered',position:{east:position.east+Math.sin(angle)*radius,north:position.north+Math.cos(angle)*radius},safeRoute:false,approachMeters:0,challengeMeters:0,stage:0,patternIndex:0,boosted:false,battleSerial:0,receipt:null,encounterTaken:false};
}
const patternFor=(l,def)=>Array.from({length:def.stages},(_,i)=>(def.interaction==='constellation'?['crown','moon','star','sun']:['leaf','fire','water','storm'])[(hash(`${l.id}:pattern`)+i*3+i*i)%4]);
function rewardTable(l,def,{boosted}={}){
 const table=(rows)=>rows.map(([kind,tickets,rarity])=>({kind,tickets,rarity:rarity||null,chance:tickets/WORLD_LIMITS.tickets,label:kind==='essence'?`${ELEMENTS[def.element].name} essence bundle`:kind==='materials'?'Rare crafting materials':kind==='encounter'?'Rare regional encounter':`${rarity[0].toUpperCase()+rarity.slice(1)} equipment`}));
 if(def.interaction==='directional')return table([['essence',4500],['materials',3500],['gear',1500,'rare'],['gear',400,'epic'],['gear',100,'legendary']]);
 if(def.key==='canopy-temple')return table([['essence',4500],['materials',3000],['gear',1500,'rare'],['gear',800,'epic'],['gear',180,'legendary'],['gear',20,'mythic']]);
 if(def.key==='ember-forge')return table((boosted??l.boosted)?[['gear',2500,'rare'],['gear',4500,'epic'],['gear',2700,'legendary'],['gear',300,'mythic']]:[['gear',5500,'rare'],['gear',3500,'epic'],['gear',900,'legendary'],['gear',100,'mythic']]);
 if(def.key==='crown-vault')return table([['gear',4500,'epic'],['gear',5000,'legendary'],['gear',500,'mythic']]);
 if(['astral-observatory','wishing-moonwell'].includes(def.key))return table([['essence',3000],['materials',2500],['encounter',3000],['gear',1400,'epic'],['gear',100,'legendary']]);
 if(def.main)return table([['essence',3500],['materials',3000],['gear',2200,'rare'],['gear',1100,'epic'],['gear',190,'legendary'],['gear',10,'mythic']]);
 if(def.key==='den')return table([['encounter',10000]]);
 return table([['essence',5000],['materials',3500],['gear',1300,'rare'],['gear',200,'epic']]);
}
export function locationRewardTable(s,id,options={}){const w=normalizeLivingWorld(s),l=w.locations[id],def=siteById.get(l?.definitionId);if(!def)throw Error('This destination is no longer available.');return rewardTable(l,def,options);}
function locationCost(def,l){const c=copy(def.cost);if(def.interaction==='forge'&&l.boosted){c.fruit=(c.fruit||0)+8;c.essence??={};c.essence.fire=(c.essence.fire||0)+5;}return c;}
function affordable(s,c){return(!c.fruit||safe(s.run.fruit)&&s.run.fruit>=c.fruit)&&Object.entries(c.items||{}).every(([k,n])=>safe(s.run.items[k])&&s.run.items[k]>=n)&&Object.entries(c.essence||{}).every(([k,n])=>safe(s.run.essence[k])&&s.run.essence[k]>=n);}
function challengeView(l,def){
 let goal=def.stages,current=l.stage,unit='stages';
 if(['offering','forge'].includes(def.interaction)){goal=1;current=l.status==='visited'?1:0;unit='offering';}
 if(['pattern','constellation'].includes(def.interaction)){current=l.patternIndex;unit='symbols';}
 if(def.interaction==='directional'){goal=def.goalFeet*(l.templeAnyRoute?2:1);current=Math.min(goal,l.challengeMeters/FEET);unit='ft';}
 if(['rescue','explore'].includes(def.interaction)){goal=def.interaction==='rescue'?450:def.key==='den'?250:120;current=Math.min(goal,l.challengeMeters);unit='m';}
 return{current,goal,unit,complete:current+1e-6>=goal,percent:Math.min(100,current/goal*100)};
}
export function specialLocations(s,landId=landProgress(s).activeId,{includeFinished=false}={}){
 const w=normalizeLivingWorld(s),p=s.world.regions[landId]?.position||{east:0,north:0};return Object.values(w.locations).filter(l=>l.landId===landId&&(includeFinished||['discovered','visited'].includes(l.status)||l.status==='claimed'&&l.receipt?.reward?.encounter&&!l.encounterTaken)).map(l=>{
  const def=siteById.get(l.definitionId),distance=Math.hypot(p.east-l.position.east,p.north-l.position.north),approachGoal=(500+LANDS.findIndex(x=>x.id===landId)*40)*FEET,cost=locationCost(def,l),challenge=challengeView(l,def),ready=l.status==='visited'&&challenge.complete;
  return{...copy(l),...def,id:l.id,definitionId:def.id,distance,approachGoal,approachRemaining:Math.max(0,approachGoal-l.approachMeters),arrived:l.safeRoute?l.approachMeters+1e-6>=approachGoal:distance<=WORLD_LIMITS.arrivalMeters,pinned:w.pinned===l.id,challenge,cost,canAfford:affordable(s,cost),ready,pattern:patternFor(l,def),bonusChance:equipmentLocationBonusChance(s,{offering:['offering','forge'].includes(def.interaction)}),odds:rewardTable(l,def)};
 });
}
export function specialLocation(s,id){const w=normalizeLivingWorld(s),l=w.locations[id];if(!l)throw Error('This destination is no longer available.');return specialLocations(s,l.landId,{includeFinished:true}).find(x=>x.id===id);}
export function discoverableLocations(landId){return SPECIAL_LOCATIONS.filter(l=>l.landId===landId);}
export function pinLivingWorldGoal(s,id){const w=normalizeLivingWorld(s),l=w.locations[id];if(id===null){w.pinned=null;return null;}if(l&&['discovered','visited'].includes(l.status)){w.pinned=id;return specialLocation(s,id);}for(const land of LANDS){const q=rotatingSideQuests(s,land.id).find(q=>q.id===id);if(q){if(!q.unlocked)throw Error('Unlock this region before pinning a side quest.');s.meta.livingWorld.pinned=id;return q;}}throw Error('Choose an active quest or destination.');}
export function pinnedLivingWorldGoal(s){const w=normalizeLivingWorld(s),id=w.pinned;if(!id)return null;if(w.locations[id])return{kind:'location',...specialLocation(s,id)};for(const land of LANDS){const q=rotatingSideQuests(s,land.id).find(q=>q.id===id);if(q)return{kind:'sidequest',...q};}return null;}
export function useLocationSafeRoute(s,id){requireAlive(s);normalizeLivingWorld(s);const l=s.meta.livingWorld.locations[id];if(!l||l.status!=='discovered')throw Error('Choose an unvisited destination.');l.safeRoute=true;return specialLocation(s,id);}
export function useTempleFlexibleRoute(s,id){requireAlive(s);const row=specialLocation(s,id);if(row.interaction!=='directional'||row.status!=='visited'||s.world.activeLand!==row.landId)throw Error('Visit this directional temple first.');s.meta.livingWorld.locations[id].templeAnyRoute=true;return specialLocation(s,id);}
export function dismissLocation(s,id){requireAlive(s);const w=normalizeLivingWorld(s),l=w.locations[id];if(!l||!['discovered','visited'].includes(l.status))throw Error('This destination is already resolved.');l.status='dismissed';if(w.pinned===id)w.pinned=null;return true;}
export function visitLocation(s,id){requireAlive(s);const view=specialLocation(s,id);if(s.world.activeLand!==view.landId)throw Error('Return to this region before visiting.');if(view.status!=='discovered')throw Error('This destination has already been visited.');if(!view.arrived)throw Error('Approach on the fantasy map or complete the safe-route alternative first.');const w=s.meta.livingWorld,l=w.locations[id];l.status='visited';w.regions[l.landId].visits++;advanceQuest(w,l.landId,'landmarks',1);recordLegacyLocation(s,view.definitionId,view.name,{major:view.main});return specialLocation(s,id);}
export function locationPuzzleInput(s,id,symbol){requireAlive(s);const row=specialLocation(s,id);if(row.status!=='visited'||!['pattern','constellation','bargain'].includes(row.interaction)||s.world.activeLand!==row.landId)throw Error('Visit this region’s puzzle first.');const l=s.meta.livingWorld.locations[id];if(row.challenge.complete)return{complete:true,correct:true};const expected=row.interaction==='bargain'?bargainClue(row).answer:row.pattern[l.patternIndex],correct=symbol===expected;if(row.interaction==='bargain'){if(correct)l.stage++;else l.stage=0;}else if(correct)l.patternIndex++;else l.patternIndex=0;return{correct,complete:specialLocation(s,id).challenge.complete};}
export function bargainClue(row){const clues=[{text:'“No haste: offer after the desert settles.”',answer:'patient'},{text:'“Meet me halfway, with a fair and steady trade.”',answer:'fair'},{text:'“A humble opening leaves room for agreement.”',answer:'humble'}];return clues[(hash(row.id)+row.stage)%clues.length];}
export function setLocationForgeBoost(s,id,value){requireAlive(s);const row=specialLocation(s,id);if(row.status!=='visited'||row.interaction!=='forge'||typeof value!=='boolean')throw Error('Visit the Ember Forge first.');s.meta.livingWorld.locations[id].boosted=value;return specialLocation(s,id);}
export function locationTrial(s,id){
 requireAlive(s);const row=specialLocation(s,id);if(s.run.phase!=='trail'||row.status!=='visited'||s.world.activeLand!==row.landId||!['guardian','defense','counter'].includes(row.interaction)||row.challenge.complete)throw Error('Visit an unfinished trial on the trail first.');
 const l=s.meta.livingWorld.locations[id];l.battleSerial++;
 return{species:trialSpecies[LANDS.findIndex(x=>x.id===row.landId)],landId:row.landId,habitat:'landmark',guardian:true,worldLocation:true,worldLocationId:row.id,worldLocationStage:l.stage,id:`${row.id}:trial:${l.stage}:${l.battleSerial}`,name:`${row.name} · Trial ${l.stage+1}`};
}
export function recordLivingWorldBattle(s,b){
 if(!b||!b.id||!b.rewarded||b.social)return;const w=normalizeLivingWorld(s),landId=b.originLandId||b.landId||s.world.activeLand;if(!landById.has(landId))return;
 const key=`battle:${b.id}`;if(w.receipts[key])return;w.receipts[key]=true;
 const m=b.worldMetrics||b.metrics||{},counter=(m.counters||m.counter||0)+(m.interrupts||m.interrupted||0),blocked=(m.blocks||m.block||0)+(m.braces||0),tactical=counter+blocked>0;
 if(tactical)advanceQuest(w,landId,'tactics',1);
 const l=w.locations[b.worldLocationId],def=siteById.get(l?.definitionId);
 if(l&&l.status==='visited'&&l.landId===landId&&l.stage===(b.worldLocationStage??l.stage)&&((def.interaction==='counter'&&counter>0)||(def.interaction==='defense'&&tactical)||def.interaction==='guardian'))l.stage=Math.min(def.stages,l.stage+1);
}
function validatePayout(s,reward,cost={}){
 if(!affordable(s,cost))throw Error('Gather the displayed offering resources first.');
 for(const[k,n]of Object.entries(reward.essence||{}))if(!safe(s.run.essence?.[k])||s.run.essence[k]+n>Number.MAX_SAFE_INTEGER)throw Error('Your essence inventory is unavailable.');
 if(!safe(s.run.fruit)||s.run.fruit+(reward.fruit||0)>Number.MAX_SAFE_INTEGER)throw Error('Your fruit inventory is unavailable.');
 for(const[k,n]of Object.entries(reward.items||{}))if(!ITEMS[k]||!safe(s.run.items?.[k])||s.run.items[k]+n>Number.MAX_SAFE_INTEGER)throw Error('Your item inventory is unavailable.');
 if(reward.gear){normalizeEquipmentInventory(s);assertGearAwardCapacity(s,reward.gear);}
 if(reward.materials){const old=s.run.items?.charmMaterial||0;if(!safe(old)||old+reward.materials>Number.MAX_SAFE_INTEGER)throw Error('Your material inventory is unavailable.');}
 if(reward.bondSpecies){s.meta.bondMaterials??={};const old=s.meta.bondMaterials[reward.bondSpecies]||0;if(!safe(old)||old+(reward.bondCount||0)>Number.MAX_SAFE_INTEGER)throw Error('Your species materials are unavailable.');}
}
function payout(s,reward,cost={}){
 validatePayout(s,reward,cost);s.run.fruit-=cost.fruit||0;for(const[k,n]of Object.entries(cost.items||{}))s.run.items[k]-=n;for(const[k,n]of Object.entries(cost.essence||{}))s.run.essence[k]-=n;
 s.run.fruit+=reward.fruit||0;for(const[k,n]of Object.entries(reward.essence||{}))s.run.essence[k]+=n;for(const[k,n]of Object.entries(reward.items||{}))s.run.items[k]+=n;
 if(reward.materials)s.run.items.charmMaterial=(s.run.items.charmMaterial||0)+reward.materials;if(reward.bondSpecies)s.meta.bondMaterials[reward.bondSpecies]=(s.meta.bondMaterials[reward.bondSpecies]||0)+reward.bondCount;
 if(reward.gear){const award=awardGearCopy(s,reward.gear,{source:'living-world'});if(award.kind==='salvaged'){s.meta.fragments??={};s.meta.fragments[reward.gear]=(s.meta.fragments[reward.gear]||0)+5;}reward.equipmentAward=award;}
}
function atomic(s,fn){const working=copy(s),result=fn(working);if(working.run?.activeId&&working.collection?.[working.run.activeId])working.run.monkey=working.collection[working.run.activeId];for(const key of Object.keys(s))delete s[key];Object.assign(s,working);return result;}
function regionalSpecies(landId,rarity){const guardian=trialSpecies[LANDS.findIndex(l=>l.id===landId)];return Object.keys(SPECIES).filter(id=>{const p=SPECIES[id],home=p.region||p.landId;return(!rarity||p.rarity===rarity)&&(home===landId||!home&&id===guardian);});}
function gearPool(landId,rarity){const land=landById.get(landId),ids=Object.keys(GEAR).filter(id=>metaRarity(id)===rarity),regional=ids.filter(id=>land.types.includes(GEAR[id].element)||GEAR[id].element==='neutral');return regional.length?regional:ids;}
export function claimSideQuest(s,id){requireAlive(s);return atomic(s,d=>{
  const w=normalizeLivingWorld(d);for(const land of LANDS){const q=rotatingSideQuests(d,land.id).find(q=>q.id===id);if(!q)continue;if(!q.unlocked||!q.complete)throw Error('Complete this side quest before collecting its reward.');const reward=copy(q.reward);payout(d,reward);const current=d.meta.livingWorld,r=current.regions[land.id],slot=r.slots[q.slot];if(slot.generation!==q.generation)throw Error('This side quest was already collected.');slot.generation++;slot.current=0;r.completed++;current.lastReceipt={operationId:id,kind:'sidequest',reward};if(current.pinned===id)current.pinned=questDefinition(land,q.slot,slot.generation).id;recordTribeProgress(d,{challenges:1},id);return current.lastReceipt;}
  throw Error('This side quest has already rotated.');
 });}
export function claimRegionalAchievement(s,landId){requireAlive(s);return atomic(s,d=>{const a=regionalAchievement(d,landId);if(!a.complete||a.claimed||!d.world.unlocked.includes(landId))throw Error('Complete more regional side quests first.');const pool=gearPool(landId,a.reward.gearRarity);if(!pool.length)throw Error('This regional equipment reward is unavailable.');const reward={fruit:a.reward.fruit,essence:a.reward.essence,gear:pick(d,pool)};payout(d,reward);d.meta.livingWorld.regions[landId].chain++;recordLegacyMilestone(d,`regional-chain:${landId}:${a.stage}`,a.title);return d.meta.livingWorld.lastReceipt={operationId:`regional-chain:${landId}:${a.stage}`,kind:'achievement',reward};});}
export function claimLocation(s,id){requireAlive(s);return atomic(s,d=>{
 const row=specialLocation(d,id);if(row.status!=='visited'||!row.ready||d.world.activeLand!==row.landId)throw Error('Complete this destination’s challenge before collecting.');if(!row.canAfford)throw Error('Gather the displayed offering resources first.');
 const roll=Math.floor(rng(d)*WORLD_LIMITS.tickets);let cursor=0,outcome=row.odds.at(-1);for(const o of row.odds){cursor+=o.tickets;if(roll<cursor){outcome=o;break;}}
 const reward={},regional=regionalSpecies(row.landId,'rare'),locals=regionalSpecies(row.landId).filter(id=>SPECIES[id].rarity!=='ultra');
 if(outcome.kind==='essence')reward.essence={[row.element]:row.main?8:4};
 if(outcome.kind==='materials'){reward.materials=row.main?6:3;const id=pick(d,regional.length?regional:locals);if(id){reward.bondSpecies=id;reward.bondCount=row.main?5:2;}}
 if(outcome.kind==='gear'){const pool=gearPool(row.landId,outcome.rarity);if(!pool.length)throw Error('This equipment reward pool is unavailable.');reward.gear=pick(d,pool);}
 if(outcome.kind==='encounter'){const pool=regional.length?regional:locals;if(!pool.length)throw Error('This region’s encounter pool is unavailable.');reward.encounter={species:pick(d,pool),landId:row.landId,habitat:'landmark',id:`${id}:encounter`,hunt:true};}
 if(row.interaction==='directional'){const d=TEMPLE_DIRECTIONS.find(d=>d.id===row.direction);reward.essence??={};reward.essence[d.element]=(reward.essence[d.element]||0)+2;reward.items={[d.item]:1};}
 const bonusRoll=row.bonusChance?rng(d):null;if(bonusRoll!==null&&bonusRoll<row.bonusChance){reward.essence??={};reward.essence[row.element]=(reward.essence[row.element]||0)+1;reward.equipmentBonus={element:row.element,count:1,chance:row.bonusChance};}
 payout(d,reward,row.cost);const l=d.meta.livingWorld.locations[id],receipt={operationId:id,kind:'location',definitionId:row.definitionId,outcome:copy(outcome),roll,cost:row.cost,reward};l.status='claimed';l.receipt=copy(receipt);d.meta.livingWorld.lastReceipt=copy(receipt);if(d.meta.livingWorld.pinned===id)d.meta.livingWorld.pinned=null;recordTribeProgress(d,{challenges:1},id);return receipt;
 });}
export function consumeLocationEncounter(s,id){requireAlive(s);const w=normalizeLivingWorld(s),l=w.locations[id];if(!l||l.status!=='claimed'||l.encounterTaken||!l.receipt?.reward?.encounter)throw Error('There is no unclaimed encounter at this destination.');if(s.run.phase!=='trail'||s.world.activeLand!==l.landId)throw Error('Return to this region’s trail to begin.');l.encounterTaken=true;return copy(l.receipt.reward.encounter);}
