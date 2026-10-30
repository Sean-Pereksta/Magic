// Permanent progression uses accepted expedition walking, never virtual depth.
// Receipts live in the encrypted save alongside the rewards they protect.
import {ELEMENTS,SPECIES} from './data.mjs';

const FEET=.3048,MAX_COUNTER=1e9;
const record=v=>v&&typeof v==='object'&&!Array.isArray(v);
const finite=v=>Number.isFinite(v)&&v>=0?Math.min(MAX_COUNTER,v):0;
const count=v=>Math.floor(finite(v));
const unique=v=>[...new Set(Array.isArray(v)?v.filter(x=>typeof x==='string'&&x.length<=180):[])];
const clone=v=>JSON.parse(JSON.stringify(v));
export const LEGACY_LIMITS=Object.freeze({walkingFeet:2000,startingFeet:50000,recordFeet:2000,recordWisdom:2,history:25,victories:3,captures:2,crafted:3});
// Keep the original four prices. Later ranks rise gradually into a long-term goal.
const extendedCosts=(first,max)=>Object.freeze([...first,...Array.from({length:max-first.length},(_,i)=>first.at(-1)+Math.ceil((i+1)*.5+(i+1)**2*.002))]);
export const LEGACY_UPGRADES=Object.freeze([
 {id:'trailblazer',name:'Trailblazer',icon:'🥾',description:'An earned head start on the virtual trail.',costs:extendedCosts([3,6,10,15],200)},
 {id:'prepared',name:'Prepared Expedition',icon:'🎒',description:'A few extra supplies for your next adventure.',costs:extendedCosts([3,5,8,12],100)},
 {id:'protection',name:"Scout’s Protection",icon:'🛡️',description:'Gentle protection in the opening wild battles.',costs:extendedCosts([4,6,9,13],100)},
 {id:'instinct',name:'Trail Instinct',icon:'🍃',description:'A modest improvement to useful trail finds.',costs:extendedCosts([4,7,10,14],100)},
 {id:'crew',name:'Experienced Crew',icon:'🐒',description:'Small health and energy improvements for your crew.',costs:extendedCosts([4,6,9,13],100)}
].map(row=>Object.freeze({...row,costs:Object.freeze(row.costs)})));
const upgradeById=new Map(LEGACY_UPGRADES.map(row=>[row.id,row]));
const ranksFor=raw=>Object.fromEntries(LEGACY_UPGRADES.map(row=>[row.id,Math.min(row.costs.length,count(raw?.[row.id]))]));
const spentFor=ranks=>LEGACY_UPGRADES.reduce((sum,row)=>sum+row.costs.slice(0,ranks[row.id]).reduce((a,b)=>a+b,0),0);

function previousClaims(s){
 const claims={};
 for(const id of s.meta.regionChallenges?.claimed||[])claims[`challenge:${id}`]=true;
 for(const [landId,region] of Object.entries(s.meta.adventures?.regions||{})){
  for(const id of region.claimed||[])claims[`milestone:${id}`]=true;
  if(region.claimed?.includes(`adventure:${landId}:0`))claims[`location:landmark:${landId}`]=true;
  // Guardian is the penultimate step in every permanent regional adventure.
  if(region.claimed?.includes(`adventure:${landId}:5`))claims[`guardian:${landId}`]=true;
 }
 for(const l of Object.values(s.meta.livingWorld?.locations||{}))if(['visited','claimed'].includes(l.status))claims[`location:${l.definitionId}`]=true;
 for(const [landId,region] of Object.entries(s.meta.livingWorld?.regions||{}))for(let stage=0;stage<Math.min(3,count(region.chain));stage++)claims[`milestone:regional-chain:${landId}:${stage}`]=true;
 for(const p of Object.values(s.collection||{}))if(SPECIES[p.species]?.rarity==='ultra'||SPECIES[p.species]?.ultra)claims[`ultra:${p.species}`]=true;
 return claims;
}

function newRunLedger(s,ranks=ranksFor({}),adopt=false){
 const r=s.run,old=s.meta.history?.find(entry=>entry.id===r.id);
 return{version:1,ranks:{...ranks},earned:0,walkingWisdom:adopt?Math.floor(finite(r.distance)/FEET/LEGACY_LIMITS.walkingFeet):0,expandedWisdom:true,victories:adopt?count(r.wins):0,
  captured:0,ultras:0,equipmentCrafted:0,equipmentDiscovered:0,locations:[],achievements:[],ordinaryBattles:adopt?count(r.ordinaryWins):0,
  completed:!!old,bestBeforeFeet:s.meta.trailWisdom.personalBestFeet,bestBeforeDepthFeet:s.meta.trailWisdom.personalBestDepthFeet,
  startingWisdom:s.meta.trailWisdom.earned-s.meta.trailWisdom.spent};
}

export function normalizeLegacy(s){
 if(!record(s?.meta))throw Error('Open a saved sanctuary first.');
 const old=record(s.meta.trailWisdom)?s.meta.trailWisdom:null,ranks=ranksFor(old?.ranks),spent=Math.max(spentFor(ranks),count(old?.spent));
 const history=Array.isArray(s.meta.history)?s.meta.history:[],historicalFeet=Math.max(0,...history.filter(h=>!h.mode||h.mode===s.mode).map(h=>finite(h.feet))),historicalDepth=Math.max(historicalFeet,...history.map(h=>finite(h.depthFeet)));
 const claims=old&&record(old.claims)?Object.fromEntries(Object.entries(old.claims).filter(([key,value])=>key.length<=180&&value===true).slice(0,2000)):previousClaims(s);
 s.meta.trailWisdom={version:1,earned:Math.max(spent,count(old?.earned)),spent,ranks,claims,
  personalBestFeet:Math.max(finite(old?.personalBestFeet),historicalFeet),personalBestDepthFeet:Math.max(finite(old?.personalBestDepthFeet),historicalDepth)};
 if(s.run){
  const r=s.run;if(!record(r.legacy)||r.legacy.version!==1)r.legacy=newRunLedger(s,ranksFor({}),true);
  const l=r.legacy;l.ranks=ranksFor(l.ranks);for(const key of ['earned','walkingWisdom','captured','ultras','equipmentCrafted','equipmentDiscovered','ordinaryBattles','startingWisdom'])l[key]=count(l[key]);
  if(!l.expandedWisdom){if(l.walkingWisdom>=12)l.walkingWisdom=Math.max(l.walkingWisdom,Math.floor(finite(r.distance)/FEET/LEGACY_LIMITS.walkingFeet));l.victories=count(r.wins);l.expandedWisdom=true;}
  l.victories=count(l.victories);l.locations=unique(l.locations).slice(0,100);l.achievements=unique(l.achievements).slice(0,120);l.completed=!!l.completed;
  l.bestBeforeFeet=finite(l.bestBeforeFeet);l.bestBeforeDepthFeet=finite(l.bestBeforeDepthFeet);
  r.startingDepth=Math.min(LEGACY_LIMITS.startingFeet*FEET,finite(r.startingDepth));
 }
 return s.meta.trailWisdom;
}

export function legacyBonuses(ranks={}){
 const r=ranksFor(ranks);
 return{startingFeet:r.trailblazer*250,fruit:r.prepared*2,essence:r.prepared>=2?r.prepared-1:0,utilityItem:r.prepared>=4,
  protectionPercent:r.protection<=4?[0,5,8,8,10][r.protection]:10+(r.protection-4)*.2,protectionBattles:r.protection>4?2+r.protection-4:r.protection>=3?2:r.protection?1:0,
  discoveryBonus:r.instinct<=4?r.instinct*.0075:.03+(r.instinct-4)*.00125,crewHp:r.crew*2,crewEnergy:Math.floor(r.crew/2)};
}
export function expeditionDepth(s){return finite(s.run?.distance)+finite(s.run?.startingDepth);}
export function startLegacyExpedition(s){
 const permanent=normalizeLegacy(s),r=s.run;r.legacy=newRunLedger(s,permanent.ranks);const bonuses=legacyBonuses(r.legacy.ranks);r.startingDepth=bonuses.startingFeet*FEET;
 r.fruit+=bonuses.fruit;
 if(bonuses.essence){const types=Object.keys(ELEMENTS);for(let i=0;i<bonuses.essence;i++)r.essence[types[(s.meta.runs+i)%types.length]]++;}
 if(bonuses.utilityItem){const items=['trailRibbon','treasureScent','scoutFeather'];r.items[items[(s.rng>>>0)%items.length]]++;}
 for(const p of Object.values(s.collection||{})){delete p.trailCrew;if(bonuses.crewHp||bonuses.crewEnergy)p.trailCrew={hp:bonuses.crewHp,maxEnergy:bonuses.crewEnergy};}
 return bonuses;
}
function award(s,key,amount,label){
 const permanent=normalizeLegacy(s),l=s.run?.legacy;if(!l||l.completed||s.run.phase==='dead'||permanent.claims[key])return 0;
 permanent.claims[key]=true;permanent.earned=Math.min(MAX_COUNTER,permanent.earned+amount);l.earned+=amount;
 if(label&&!l.achievements.includes(label))l.achievements.push(label);return amount;
}
export function recordLegacyWalking(s,meters){
 if(s.run?.phase!=='trail'||!Number.isFinite(meters)||meters<=0)return 0;
 normalizeLegacy(s);const l=s.run.legacy,feet=finite(s.run.distance)/FEET,target=Math.floor((feet+1e-6)/LEGACY_LIMITS.walkingFeet),amount=Math.max(0,target-l.walkingWisdom);
 if(amount&&!l.completed){s.meta.trailWisdom.earned+=amount;l.earned+=amount;l.walkingWisdom=target;}return amount;
}
export function recordLegacyLocation(s,id,name,{major=true}={}){
 if(!major||typeof id!=='string')return 0;const earned=award(s,`location:${id}`,2,`Discovered ${String(name||'a major location').slice(0,100)}`);
 if(earned)s.run.legacy.locations.push(id);return earned;
}
export function recordLegacyMilestone(s,id,title){return award(s,`milestone:${id}`,2,String(title||'Regional milestone complete').slice(0,120));}
export function recordLegacyChallenge(s,id,title){return award(s,`challenge:${id}`,2,String(title||'Regional challenge complete').slice(0,120));}
function repeatableAward(s,amount){const l=s.run?.legacy;if(!l||l.completed||s.run.phase==='dead')return 0;const actual=Math.min(amount,MAX_COUNTER-s.meta.trailWisdom.earned);s.meta.trailWisdom.earned+=actual;l.earned+=actual;return actual;}
export function recordLegacyBattle(s,b){
 if(!b?.id||!b.rewarded||b.online||b.social||b.legacyRecorded)return 0;
 normalizeLegacy(s);const l=s.run.legacy;if(l.completed)return 0;b.legacyRecorded=true;l.victories++;let earned=l.victories%LEGACY_LIMITS.victories===0?repeatableAward(s,1):0;
 if(b.guardian&&!b.worldGuardian)earned+=award(s,`guardian:${b.originLandId||b.landId||s.world?.activeLand}`,3,'First regional guardian defeated');
 if(b.ultra){l.ultras++;const species=b.enemies?.find(p=>SPECIES[p.species]?.rarity==='ultra'||SPECIES[p.species]?.ultra)?.species||b.enemies?.[0]?.species;if(species)earned+=award(s,`ultra:${species}`,5,`First Ultra victory: ${SPECIES[species]?.name||species}`);}
 return earned;
}
export function recordLegacyCapture(s){normalizeLegacy(s);const l=s.run.legacy;if(!l.completed&&s.run.phase!=='dead'){l.captured++;if(l.captured%LEGACY_LIMITS.captures===0)repeatableAward(s,1);}}
export function recordLegacyEquipment(s,{crafted=false}={}){normalizeLegacy(s);const l=s.run.legacy;if(!l.completed&&s.run.phase!=='dead'){l[crafted?'equipmentCrafted':'equipmentDiscovered']++;if(crafted&&l.equipmentCrafted%LEGACY_LIMITS.crafted===0)repeatableAward(s,1);}}
export function beginLegacyBattle(s,b){
 normalizeLegacy(s);const l=s.run.legacy;if(b.online||b.social||b.ultra||b.guardian||b.hunt)return;
 if(b.legacyProtectionPercent!==undefined)return;
 const bonuses=legacyBonuses(l.ranks);l.ordinaryBattles++;b.legacyProtectionPercent=l.ordinaryBattles<=bonuses.protectionBattles?bonuses.protectionPercent:0;
}
export function legacyDamage(s,damage){const b=s.run?.battle;return b&&!b.online&&!b.social&&b.legacyProtectionPercent?Math.max(damage>0?1:0,Math.round(damage*(1-Math.min(30,finite(b.legacyProtectionPercent))/100))):damage;}
export function legacyDiscoveryBonus(s){return legacyBonuses(s.run?.legacy?.ranks).discoveryBonus;}

export function completeLegacyExpedition(s,{score=0,level=1,form='',endedAt=Date.now()}={}){
 const permanent=normalizeLegacy(s),r=s.run,l=r.legacy,old=s.meta.history?.find(entry=>entry.id===r.id);if(old){l.completed=true;return old;}
 if(l.completed)return l.recap||null;
 const feet=Math.floor(r.distance/FEET+1e-6),depthFeet=Math.floor(expeditionDepth(s)/FEET+1e-6),newRecord=feet>l.bestBeforeFeet;
 if(newRecord&&feet-l.bestBeforeFeet>=LEGACY_LIMITS.recordFeet){permanent.earned+=LEGACY_LIMITS.recordWisdom;l.earned+=LEGACY_LIMITS.recordWisdom;l.achievements.push('New personal distance record');}
 permanent.personalBestFeet=Math.max(permanent.personalBestFeet,feet);permanent.personalBestDepthFeet=Math.max(permanent.personalBestDepthFeet,depthFeet);
 const recap={id:r.id,name:s.name,score,feet,depthFeet,wins:count(r.wins),level,form,mode:s.mode,startedAt:r.startedAt||endedAt,endedAt,
  captured:l.captured,ultras:l.ultras,equipmentCrafted:l.equipmentCrafted,equipmentDiscovered:l.equipmentDiscovered,locations:l.locations.length,newLocations:[...l.locations],
  captures:l.captured,equipment:l.equipmentCrafted+l.equipmentDiscovered,records:newRecord?['New personal distance record']:[],upgrades:{...l.ranks},
  wisdom:l.earned,achievements:[...l.achievements],newRecord,previousBestFeet:l.bestBeforeFeet,improvementFeet:Math.max(0,feet-l.bestBeforeFeet),bonuses:legacyBonuses(l.ranks)};
 s.meta.history=[recap,...(s.meta.history||[]).filter(entry=>entry.id!==r.id)].slice(0,LEGACY_LIMITS.history);l.completed=true;l.recap=clone(recap);return recap;
}
export function expeditionRecap(s){
 normalizeLegacy(s);const row=s.meta.history?.find(entry=>entry.id===s.run?.id)||s.run?.legacy?.recap||s.meta.history?.[0];
 return row?{captured:0,captures:row.captured||0,equipment:(row.equipmentCrafted||0)+(row.equipmentDiscovered||0),ultras:0,equipmentCrafted:0,equipmentDiscovered:0,locations:0,wisdom:0,records:[],upgrades:{},achievements:[],newLocations:[],newRecord:false,depthFeet:row.feet||0,...clone(row)}:null;
}
function benefit(id,rank){
 const b=legacyBonuses({[id]:rank});return({trailblazer:`Start ${b.startingFeet.toLocaleString()} virtual feet along the trail`,prepared:`+${b.fruit} fruit${b.essence?`, +${b.essence} starter essence`:''}${b.utilityItem?', one utility item':''}`,
 protection:`${Number(b.protectionPercent.toFixed(1))}% less damage in the first ${b.protectionBattles} ordinary ${b.protectionBattles===1?'battle':'battles'}`,instinct:`+${(b.discoveryBonus*100).toFixed(2)} percentage points toward useful finds`,crew:`+${b.crewHp} starting health, +${b.crewEnergy} maximum energy`})[id];
}
export function legacyView(s){
 const saved=normalizeLegacy(s),wisdom=saved.earned-saved.spent,upgrades=LEGACY_UPGRADES.map(row=>{const rank=saved.ranks[row.id],cost=row.costs[rank]??null;return{...row,rank,maxRank:row.costs.length,cost,affordable:cost!==null&&wisdom>=cost,benefit:benefit(row.id,rank),nextBenefit:cost===null?'Fully upgraded':benefit(row.id,rank+1)};});
 const next=upgrades.filter(row=>row.cost!==null).sort((a,b)=>a.cost-b.cost)[0]||null,feet=finite(s.run?.distance)/FEET,nextWalking=(Math.floor((feet+1e-6)/LEGACY_LIMITS.walkingFeet)+1)*LEGACY_LIMITS.walkingFeet,l=s.run?.legacy;
 return{wisdom,earned:saved.earned,spent:saved.spent,upgrades,bonuses:legacyBonuses(saved.ranks),nextUpgrade:next?{id:next.id,name:next.name,rank:next.rank+1,cost:next.cost,remaining:Math.max(0,next.cost-wisdom),percent:Math.min(100,wisdom/next.cost*100)}:null,
  personalBestFeet:saved.personalBestFeet,milestone:{label:'Next walking Wisdom',current:feet%LEGACY_LIMITS.walkingFeet,goal:LEGACY_LIMITS.walkingFeet,remainingFeet:Math.max(0,nextWalking-feet),percent:feet%LEGACY_LIMITS.walkingFeet/LEGACY_LIMITS.walkingFeet*100,capped:false},repeatable:{victories:count(l?.victories)%LEGACY_LIMITS.victories,captures:count(l?.captured)%LEGACY_LIMITS.captures,crafted:count(l?.equipmentCrafted)%LEGACY_LIMITS.crafted}};
}
export function purchaseLegacyUpgrade(s,id){
 const row=upgradeById.get(id);if(!row)throw Error('Choose a permanent upgrade.');const saved=normalizeLegacy(s),rank=saved.ranks[id],cost=row.costs[rank];
 if(cost===undefined)throw Error('This upgrade is already at its maximum rank.');if(saved.earned-saved.spent<cost)throw Error('Earn more Trail Wisdom for this rank.');
 saved.spent+=cost;saved.ranks[id]++;return legacyView(s);
}
