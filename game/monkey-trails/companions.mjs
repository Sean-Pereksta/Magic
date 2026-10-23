// Permanent friendship and compact memories. Only accepted core travel calls the
// travel hook; no coordinates or real-world route records are stored here.
import {ELEMENTS,DIRECTIONS,GEAR,SPECIES} from './data.mjs';
import {LANDS} from './lands.mjs';

export const FRIENDSHIP_TIERS=Object.freeze([
  Object.freeze({id:'new',name:'New Friend',bond:0,reward:'A place in your Sanctuary',pose:'idle',title:'New Friend',accent:'none'}),
  Object.freeze({id:'partner',name:'Trail Partner',bond:12,reward:'Happy portrait pose and Trail Partner title',pose:'victory',title:'Trail Partner',accent:'leaf-pin'}),
  Object.freeze({id:'trusted',name:'Trusted Companion',bond:40,reward:'Trusted Companion title and silver friendship accent',pose:'arrival',title:'Trusted Companion',accent:'silver-vine'}),
  Object.freeze({id:'legend',name:'Trail Legend',bond:100,reward:'Trail Legend title and golden friendship accent',pose:'victory',title:'Trail Legend',accent:'golden-crown'})
]);
export const FRIENDSHIP_METERS=1000,BOND_PER_MATERIAL=4,MAX_LINEUPS=4,MAX_FAVORITES=3;
const safe=n=>Number.isFinite(n)?Math.max(0,Math.min(1e9,n)):0;
const whole=n=>Math.floor(safe(n));
const validRegion=id=>LANDS.some(l=>l.id===id);
const label=value=>String(value||'').trim().slice(0,28);
const party=s=>(s.run?.party||[]).map(id=>s.collection?.[id]).filter(Boolean);
const prepare=s=>['trail','camp'].includes(s.run?.phase);

export function normalizeCompanions(s){
  s.meta??={};s.meta.bondMaterials??={};
  const old=s.meta.companions&&typeof s.meta.companions==='object'?s.meta.companions:{};
  const ids=new Set(Object.keys(s.collection||{})),lineups=[];
  for(const row of Array.isArray(old.lineups)?old.lineups:[]){
    if(lineups.length>=MAX_LINEUPS||!row||typeof row.id!=='string'||!label(row.name)||lineups.some(x=>x.id===row.id))continue;
    const members=[...new Set(Array.isArray(row.members)?row.members:[])].filter(id=>ids.has(id)).slice(0,3);
    if(members.length)lineups.push({id:row.id.slice(0,60),name:label(row.name),members});
  }
  s.meta.companions={version:1,serial:whole(old.serial),favorites:[...new Set(Array.isArray(old.favorites)?old.favorites:[])].filter(id=>ids.has(id)).slice(0,MAX_FAVORITES),lineups,
    evolutionGoal:ids.has(old.evolutionGoal?.monkeyId)&&ELEMENTS[old.evolutionGoal?.type]?{monkeyId:old.evolutionGoal.monkeyId,type:old.evolutionGoal.type,stage:Math.max(1,Math.min(3,whole(old.evolutionGoal.stage)||1))}:null};
  for(const p of Object.values(s.collection||{})){
    const m=p.memories&&typeof p.memories==='object'?p.memories:{};
    p.memories={distance:safe(m.distance),travelRemainder:safe(m.travelRemainder)%FRIENDSHIP_METERS,firstRegion:validRegion(m.firstRegion)?m.firstRegion:null,
      firstGuardian:validRegion(m.firstGuardian)?m.firstGuardian:null,victories:whole(m.victories),guardianVictories:whole(m.guardianVictories),expeditions:whole(m.expeditions),
      activeRun:typeof m.activeRun==='string'?m.activeRun.slice(0,80):null,lastCompletedRun:typeof m.lastCompletedRun==='string'?m.lastCompletedRun.slice(0,80):null};
  }
  return s.meta.companions;
}

export function friendshipInfo(p){
  const bond=safe(p?.bond),index=FRIENDSHIP_TIERS.findLastIndex(t=>bond>=t.bond),tier=FRIENDSHIP_TIERS[Math.max(0,index)],next=FRIENDSHIP_TIERS[index+1]||null;
  return{bond,tier,next,remaining:next?Math.max(0,next.bond-bond):0,current:next?bond-tier.bond:bond,goal:next?next.bond-tier.bond:Math.max(1,bond),percent:next?Math.min(100,(bond-tier.bond)/(next.bond-tier.bond)*100):100,
    unlocked:FRIENDSHIP_TIERS.filter(t=>bond>=t.bond).map(t=>t.id)};
}
export function recordCompanionTravel(s,meters,regionId=s.world?.activeLand){
  if(!Number.isFinite(meters)||meters<=0||s.run?.phase!=='trail')return 0;
  normalizeCompanions(s);let earned=0;
  for(const p of party(s)){
    if(p.hp<=0)continue;const m=p.memories;m.activeRun=s.run.id;if(!m.firstRegion&&validRegion(regionId))m.firstRegion=regionId;
    const amount=Math.min(meters,1e9-m.distance);m.distance+=amount;m.travelRemainder+=amount;const points=Math.floor((m.travelRemainder+1e-7)/FRIENDSHIP_METERS);m.travelRemainder=Math.max(0,m.travelRemainder-points*FRIENDSHIP_METERS);
    p.bond=Math.min(1e9,safe(p.bond)+points);earned+=points;
  }
  return earned;
}
export function recordCompanionVictory(s,{guardian=false,regionId=s.world?.activeLand}={}){
  normalizeCompanions(s);
  for(const p of party(s)){
    const m=p.memories;m.activeRun=s.run.id;m.victories=Math.min(1e9,m.victories+1);
    if(guardian){m.guardianVictories=Math.min(1e9,m.guardianVictories+1);if(!m.firstGuardian&&validRegion(regionId))m.firstGuardian=regionId;}
  }
}
export function recordCompanionRecruit(s,id){normalizeCompanions(s);const p=s.collection?.[id];if(p)p.memories.activeRun=s.run?.id||null;return p;}
export function recordCompanionExpedition(s){
  normalizeCompanions(s);if(!s.run?.id)return;
  for(const p of Object.values(s.collection||{})){const m=p.memories;if(m.activeRun===s.run.id&&m.lastCompletedRun!==s.run.id){m.expeditions=Math.min(1e9,m.expeditions+1);m.lastCompletedRun=s.run.id;}}
}
export function bondMaterialPreview(s,id,amount=1){
  const p=s.collection?.[id];if(!p)throw Error('Choose a companion.');
  const count=Number.isSafeInteger(amount)&&amount>0?amount:1,balance=whole(s.meta?.bondMaterials?.[p.species]),info=friendshipInfo(p),gain=count*BOND_PER_MATERIAL,after=friendshipInfo({...p,bond:info.bond+gain});
  return{species:p.species,speciesName:SPECIES[p.species].name,balance,cost:count,gain,before:info.bond,after:after.bond,nextReward:info.next?.reward||'All friendship cosmetics unlocked',unlocks:after.unlocked.filter(t=>!info.unlocked.includes(t)),affordable:balance>=count&&info.bond<FRIENDSHIP_TIERS.at(-1).bond,
    explanation:`Spend ${count} ${SPECIES[p.species].name} bond material${count===1?'':'s'} for +${gain} permanent friendship. No companion is removed.`};
}
export function spendBondMaterial(s,id,amount=1){
  if(!prepare(s))throw Error('Develop friendship between encounters.');
  if(!Number.isSafeInteger(amount)||amount<1||amount>25)throw Error('Choose one to 25 materials.');
  normalizeCompanions(s);const offer=bondMaterialPreview(s,id,amount),p=s.collection[id];
  if(p.hp<=0)throw Error('Revive this companion before spending friendship materials.');
  if(!offer.affordable)throw Error(offer.balance<offer.cost?'Gather more materials for this species.':'All friendship cosmetics are already unlocked.');
  s.meta.bondMaterials[p.species]-=amount;p.bond=offer.after;return offer;
}
export function assertDuplicateConvertible(s,id){
  const data=normalizeCompanions(s),p=s.collection?.[id];if(!p)throw Error('Choose the newly recruited duplicate.');
  if(!prepare(s)&&!(s.run?.phase==='befriend'&&s.run.capture?.claimed&&s.run.lastCapture?.id===id))throw Error('Convert a newly recruited duplicate between encounters.');
  if(data.favorites.includes(id))throw Error('Remove this companion from Sanctuary favorites before converting it.');
  if(data.lineups.some(row=>row.members.includes(id)))throw Error('Remove this companion from saved lineups before converting it.');
  if(Object.values(p.equipment||{}).some(Boolean))throw Error('Remove this companion’s equipment before converting it.');
  return p;
}
export function duplicateConversionPreview(s,id){
  const p=s.collection?.[id];if(!p)throw Error('Choose a newly recruited duplicate.');
  let reason='';try{assertDuplicateConvertible(s,id);}catch(error){reason=error.message;}
  const last=s.run?.lastCapture,allowed=last?.id===id&&last.duplicate&&!last.converted&&p.hp>0&&Object.values(s.collection).some(other=>other.id!==id&&other.species===p.species);
  if(!allowed&&!reason)reason='Only the newly recruited conscious duplicate may be converted.';
  return{allowed:allowed&&!reason,reason,species:p.species,materials:3,friendship:3*BOND_PER_MATERIAL,explanation:`Remove this newly recruited duplicate for 3 ${SPECIES[p.species].name} bond materials. Each material grants +4 friendship to another ${SPECIES[p.species].name}; 3 materials grant +12. Equipment, favorites and saved lineups are protected.`};
}
export function saveLineup(s,name,members=s.run?.party){
  const data=normalizeCompanions(s),title=label(name);
  if(!title)throw Error('Give this lineup a name.');
  if(!Array.isArray(members)||members.length<1||members.length>3||new Set(members).size!==members.length||members.some(id=>!s.collection?.[id]))throw Error('Save one to three different collected companions.');
  let row=data.lineups.find(x=>x.name.toLowerCase()===title.toLowerCase());
  if(!row){if(data.lineups.length>=MAX_LINEUPS)throw Error('Keep up to four saved lineups; remove one to add another.');data.serial++;row={id:`lineup-${data.serial}`,name:title,members:[]};data.lineups.push(row);}
  row.name=title;row.members=[...members];return row;
}
export function removeLineup(s,id){const data=normalizeCompanions(s),before=data.lineups.length;data.lineups=data.lineups.filter(row=>row.id!==id);return before!==data.lineups.length;}
export function lineupAvailability(s,id){
  const data=normalizeCompanions(s),row=data.lineups.find(r=>r.id===id),r=s.run;
  if(!row)return{allowed:false,reason:'This lineup is unavailable.'};
  if(!['trail','camp'].includes(r?.phase))return{allowed:false,reason:'Apply this lineup on the trail or at camp, between encounters.'};
  if(row.members.some(id=>!s.collection[id]||!r.party.includes(id)&&s.collection[id].hp<=0)||!row.members.some(id=>s.collection[id]?.hp>0))return{allowed:false,reason:'Revive incoming companions and keep at least one conscious teammate.'};
  const equipment=new Set();
  for(const id of row.members)for(const[slot,gear]of Object.entries(s.collection[id].equipment||{}))if(gear){if(!GEAR[gear]||GEAR[gear].slot!==slot||!s.meta.gear?.[gear]||equipment.has(gear))return{allowed:false,reason:'Resolve the selected companions’ equipment before applying this lineup.'};equipment.add(gear);}
  return{allowed:true,reason:'Applies these individuals with their current injuries and equipment.',lineup:row};
}
export function applyLineup(s,id,setParty){
  const offer=lineupAvailability(s,id);if(!offer.allowed)throw Error(offer.reason);
  if(typeof setParty!=='function')throw Error('The expedition team action is unavailable.');
  // The existing core setter is the sole owner of party, active ID and injuries.
  return setParty(s,[...offer.lineup.members]);
}
export function setFavorite(s,id,value){
  const data=normalizeCompanions(s);if(!s.collection?.[id])throw Error('Choose a collected companion.');const exists=data.favorites.includes(id),enable=value===undefined?!exists:Boolean(value);
  if(enable&&!exists){if(data.favorites.length>=MAX_FAVORITES)throw Error('Choose up to three Sanctuary favorites.');data.favorites.push(id);}else if(!enable)data.favorites=data.favorites.filter(x=>x!==id);
  return enable;
}
export function favoriteCompanions(s){const data=normalizeCompanions(s);return(data.favorites.length?data.favorites:s.run?.party||Object.keys(s.collection||{})).slice(0,MAX_FAVORITES).map(id=>s.collection[id]).filter(Boolean);}

export function readiness(s,id,{trainingCost,evolutionCost,unlocked}={}){
  const p=s.collection?.[id];if(!p)return null;
  if(p.hp<=0)return{section:'train',label:'Needs revival before training or evolving'};
  const inTeam=s.run?.party?.includes(id),available=prepare(s)&&inTeam;
  const blockedLabel=action=>!prepare(s)?`Finish the encounter to ${action}`:`Add to your lineup to ${action}`;
  if(typeof evolutionCost==='function')for(const type of Object.keys(ELEMENTS)){const cost=evolutionCost(s,type,id);if(cost&&p.level>=cost.level&&Object.entries(cost.cost).every(([t,n])=>(s.run.essence[t]||0)>=n))return{section:'evolve',label:available?'Evolution available':blockedLabel('evolve'),type};}
  if(p.level<60&&typeof trainingCost==='function'){
    const cost=trainingCost(p),fruit=Math.max(0,cost.fruit-(s.run?.fruit||0)),xp=Math.max(0,cost.xp-(p.training||0));
    if(!fruit&&!xp)return{section:'train',label:available?'Ready to level up':blockedLabel('level up')};
    if(!xp&&fruit)return{section:'train',label:`Needs ${fruit} more fruit`};
  }
  if(typeof unlocked==='function'){const fresh=unlocked(p).filter(move=>!p.loadout.includes(move));if(fresh.length)return{section:'abilities',label:'Unlocked abilities available'};}
  if(typeof evolutionCost==='function'){
    const offers=Object.keys(ELEMENTS).map(type=>({type,cost:evolutionCost(s,type,id)})).filter(row=>row.cost&&p.level>=row.cost.level);
    const options=offers.map(row=>({...row,missing:Object.entries(row.cost.cost).map(([type,n])=>({type,count:Math.max(0,n-(s.run.essence[type]||0))})).filter(x=>x.count>0)})).sort((a,b)=>a.missing.reduce((n,x)=>n+x.count,0)-b.missing.reduce((n,x)=>n+x.count,0));
    if(options[0]?.missing.length){const short=options[0].missing[0];return{section:'evolve',type:options[0].type,label:`Needs ${short.count} ${ELEMENTS[short.type].name} essence`};}
  }
  return{section:'train',label:'Keep exploring to train your companion'};
}
export function trackEvolution(s,id,type,evolutionCost){
  normalizeCompanions(s);const p=s.collection?.[id],cost=typeof evolutionCost==='function'?evolutionCost(s,type,id):null;
  if(!p||!cost)throw Error('Choose an available evolution path.');
  const goal={monkeyId:id,type,stage:p.stage+1};s.meta.companions.evolutionGoal=goal;s.meta.pinnedGoal={kind:'evolution',id,monkeyId:id,type};if(s.meta.regionChallenges)s.meta.regionChallenges.tracked=null;return evolutionGoal(s,evolutionCost);
}
export function evolutionGoal(s,evolutionCost){
  const data=normalizeCompanions(s),goal=data.evolutionGoal,p=s.collection?.[goal?.monkeyId];if(!p)return null;
  if(p.stage>=goal.stage&&p.types.includes(goal.type)&&(goal.stage!==3||p.mastery===goal.type))return{...goal,complete:true,label:`${p.nickname||SPECIES[p.species].name} evolved`,remaining:[]};
  const cost=typeof evolutionCost==='function'?evolutionCost(s,goal.type,p.id):null;if(!cost)return null;
  const habitats={leaf:'Groves',water:'Riverbanks',fire:'Landmark clearings',storm:'Landmark clearings',earth:'Cave formations',ice:'Riverbanks and cave formations',light:'Ancient ruins and landmark clearings',shadow:'Groves and ancient ruins'};
  const remaining=Object.entries(cost.cost).map(([type,total])=>({type,total,current:s.run.essence[type]||0,needed:Math.max(0,total-(s.run.essence[type]||0)),habitats:habitats[type],compass:Object.entries(DIRECTIONS).find(([,element])=>element===type)?.[0]||null,regions:LANDS.filter(l=>l.types.includes(type)).map(l=>({id:l.id,name:l.name,unlocked:(s.world?.unlocked||[]).includes(l.id),remaining:Math.max(0,l.unlock-(s.world?.totalDistance||0)),questEssence:l.types[0]===type}))}));
  return{...goal,complete:false,label:`Evolve ${p.nickname||SPECIES[p.species].name}`,requiredLevel:cost.level,currentLevel:p.level,ready:p.level>=cost.level&&remaining.every(row=>!row.needed),remaining,
    hint:'Essence comes from accepted trail discoveries, regional rewards and victories. Compass direction influences ordinary essence finds; four spare essence can be exchanged for one needed essence.'};
}
