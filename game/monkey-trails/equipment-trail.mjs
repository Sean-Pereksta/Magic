// Trail passives use accepted discovery/camp events, never their bonus rewards.
import {ELEMENTS,DIRECTIONS,GEAR,SPECIES} from './data.mjs';
import {LANDS} from './lands.mjs';
import {equipmentLoadout} from './equipment-inventory.mjs';
import {LEGACY_GEAR_AFFINITIES} from './equipment-data.mjs';

const members=s=>(s.run?.party||[]).map(id=>s.collection?.[id]).filter(Boolean);
const scale=tier=>1+.08*(Math.max(1,Math.min(12,tier))-1);
const effect=(s,p,key)=>Math.max(0,...Object.entries(equipmentLoadout(s,p)).map(([id,tier])=>Number(GEAR[id].effects?.[key]||0)*scale(tier)));
const teamEffect=(s,key)=>Math.max(0,...members(s).map(p=>effect(s,p,key)));
const plain=o=>o&&typeof o==='object'&&!Array.isArray(o);
const whole=n=>Number.isSafeInteger(n)&&n>=0&&n<=1e9;
const random=s=>{let x=s.rng|0;x^=x<<13;x^=x>>>17;x^=x<<5;s.rng=(x>>>0)||1234567;return s.rng/4294967296;};
const choice=(s,rows)=>rows[Math.floor(random(s)*rows.length)];
const commonFragment=id=>id.startsWith('fragment:')&&(GEAR[id.slice(9)]?.rarity||LEGACY_GEAR_AFFINITIES[id.slice(9)]?.rarity)==='common';
const validQualityId=id=>id==='fruit'||/^essence:(leaf|earth|fire|storm|water|shadow|ice|light)$/.test(id)||/^item:(berry|tonic|salve)$/.test(id)||commonFragment(id);
export const isCommonCraftIngredient=id=>id==='fruit'||/^item:(berry|tonic|salve)$/.test(id)||commonFragment(id);

export function normalizeEquipmentTrail(s){
  const r=s.run;if(!r)return s;
  r.discoveries??=0;if(!whole(r.discoveries))throw Error('Invalid equipment discovery checkpoint.');
  if(r.forgeMaterials!==undefined&&(!plain(r.forgeMaterials)||Object.entries(r.forgeMaterials).some(([id,n])=>!validQualityId(id)||!whole(n))))throw Error('Invalid upgraded trail materials.');
  const saved=r.equipmentTrail;
  if(saved!==undefined&&(!plain(saved)||saved.version!==1||!whole(saved.lastDiscovery)||saved.lastDiscovery>(r.discoveries||0)||!whole(saved.foragerProgress)||saved.foragerProgress>=12||!Array.isArray(saved.camps)||saved.camps.length>10000||saved.camps.some(id=>typeof id!=='string'||id.length>100)||new Set(saved.camps).size!==saved.camps.length))throw Error('Invalid equipment trail checkpoint.');
  // Normalization never awards historical discoveries or previously visited camps.
  r.equipmentTrail??={version:1,lastDiscovery:r.discoveries||0,foragerProgress:0,camps:r.camp?.id?[r.camp.id]:[]};
  r.forgeMaterials??={};return s;
}

export function eligibleCompassElements(s,itemId='prismCompass'){
  if(itemId==='trailfinderCompass')return Object.keys(ELEMENTS);
  // This is exactly the ordinary essence pool used by the directional trail roll.
  return [...new Set([DIRECTIONS[s.run?.direction]||'leaf','earth','ice','light','shadow'])];
}
export const equipmentDiscoveryReduction=s=>Math.max(teamEffect(s,'discoveryReduction'),members(s).some(p=>Object.keys(equipmentLoadout(s,p)).some(id=>GEAR[id].discoveryReduction))?.05:0);
export function equipmentEncounterWeight(s,speciesId){
  const selected=s.meta?.equipmentChoices?.trailfinderCompass;
  return ELEMENTS[selected]&&SPECIES[speciesId]?.types.includes(selected)?1+teamEffect(s,'encounterWeight'):1;
}
export function equipmentCaptureChance(s,chance,cap=.99){
  if(!Number.isFinite(chance)||chance<=0||chance>=1)return chance;
  const active=s.collection?.[s.run?.activeId]||s.run?.monkey;
  return Math.min(cap,chance*(1+(active?effect(s,active,'captureBonus'):0)));
}
export const equipmentWalkingRecovery=(s,p,fraction)=>fraction*(1+(s.run?.party?.includes(p?.id)?effect(s,p,'distanceHealing'):0));
export const equipmentTrainingExperience=(s,p,experience)=>experience*(1+(s.run?.party?.includes(p?.id)?effect(s,p,'trainingExperience'):0));
export const equipmentCraftQuality=s=>Math.min(.06,teamEffect(s,'craftQuality'));
export const equipmentSpecialDiscovery=s=>Math.min(.12,teamEffect(s,'specialDiscovery'));
export const equipmentLocationBonusChance=(s,{offering=false}={})=>Math.min(.30,teamEffect(s,offering?'offeringBonus':'specialResource'));

function baseResource(discovery){
  if(discovery.category==='fruit')return'fruit';
  if(discovery.category==='essence'&&ELEMENTS[discovery.id])return`essence:${discovery.id}`;
  if(['item','fragment'].includes(discovery.category))return`${discovery.category}:${discovery.id}`;
  return null;
}
function amountOf(s,id){if(id==='fruit')return s.run.fruit;const[k,...rest]=id.split(':'),key=rest.join(':');return(k==='essence'?s.run.essence:k==='item'?s.run.items:s.meta.fragments)?.[key]||0;}
function addResource(s,id,n){if(id==='fruit'){s.run.fruit+=n;return;}const[k,...rest]=id.split(':'),key=rest.join(':'),inventory=k==='essence'?s.run.essence:k==='item'?s.run.items:(s.meta.fragments??={});inventory[key]=(inventory[key]||0)+n;}

export function recordEquipmentDiscovery(s,discovery=s.run?.lastDiscovery){
  const r=s.run;if(!r||r.phase!=='trail'||!discovery||!whole(r.discoveries)||r.discoveries<1)return{bonuses:[]};
  // A core call supplies one newly persisted discovery serial. Replayed events
  // and callback/UI redraws cannot create more RNG draws, progress, or resources.
  if(r.equipmentTrail===undefined)r.equipmentTrail={version:1,lastDiscovery:r.discoveries-1,foragerProgress:0,camps:[]};
  normalizeEquipmentTrail(s);const saved=r.equipmentTrail;
  if(r.discoveries<=saved.lastDiscovery)return{bonuses:[]};
  if(r.discoveries!==saved.lastDiscovery+1)throw Error('Equipment rewards require the next accepted discovery.');
  const bonuses=[],base=baseResource(discovery);saved.lastDiscovery=r.discoveries;
  if(!base||!(discovery.amount>0))return{bonuses};
  const eligible=eligibleCompassElements(s),prism=s.meta?.equipmentChoices?.prismCompass;
  let awardedBase=base;
  if(discovery.category==='essence'&&ELEMENTS[prism]&&eligible.includes(prism)&&prism!==discovery.id&&teamEffect(s,'prismConvert')>0&&random(s)<teamEffect(s,'prismConvert')&&amountOf(s,base)>=1){
    addResource(s,base,-1);awardedBase=`essence:${prism}`;addResource(s,awardedBase,1);bonuses.push({kind:'converted',from:base,id:awardedBase,count:1,quality:'common'});
  }
  if(validQualityId(awardedBase)&&teamEffect(s,'resourceUpgrade')>0&&random(s)<teamEffect(s,'resourceUpgrade')&&amountOf(s,awardedBase)>=1){
    addResource(s,awardedBase,-1);r.forgeMaterials[awardedBase]=(r.forgeMaterials[awardedBase]||0)+1;bonuses.push({kind:'upgraded',id:`quality:${awardedBase}`,count:1,quality:'uncommon'});
  }
  if(teamEffect(s,'foragerPouch')>0){
    saved.foragerProgress++;
    if(saved.foragerProgress>=12){
      saved.foragerProgress=0;let extra=awardedBase;
      if(discovery.category==='item')extra=`item:${choice(s,['berry','tonic','salve'])}`;
      if(discovery.category==='fragment')extra=commonFragment(base)?base:`fragment:${choice(s,Object.keys(LEGACY_GEAR_AFFINITIES).filter(id=>commonFragment(`fragment:${id}`)))}`;
      addResource(s,extra,1);bonuses.push({kind:'foraged',id:extra,count:1,quality:'common'});
    }
  }
  // Preserve the original event; generated resources never recursively discover.
  discovery.equipmentBonuses=bonuses;return{bonuses};
}

export function awardEquipmentCamp(s,campId=s.run?.camp?.id){
  const r=s.run;if(!r||r.phase!=='camp'||!campId||typeof campId!=='string'||campId!==r.camp?.id)return null;
  if(r.equipmentTrail===undefined)r.equipmentTrail={version:1,lastDiscovery:r.discoveries||0,foragerProgress:0,camps:[]};
  normalizeEquipmentTrail(s);if(r.equipmentTrail.camps.includes(campId))return null;
  r.equipmentTrail.camps.push(campId);if(!teamEffect(s,'campEssence'))return null;
  const eligible=LANDS.find(land=>land.id===s.world?.activeLand)?.types||['leaf'],element=eligible.length===1?eligible[0]:choice(s,eligible),id=`essence:${element}`;
  r.forgeMaterials[id]=(r.forgeMaterials[id]||0)+1;const reward={id:`quality:${id}`,element,count:1,quality:'uncommon'};
  r.camp.equipmentReward=reward;return reward;
}

// Called after a camp discount is calculated. Only spent common ingredients
// qualify; elemental essence and higher-quality materials never qualify.
export function equipmentCraftRefund(s,costInputs={}){
  if(!teamEffect(s,'craftRefund'))return{};
  const eligible=Object.entries(costInputs).filter(([id,n])=>Number.isSafeInteger(n)&&n>0&&isCommonCraftIngredient(id));
  let due=Math.floor(eligible.reduce((n,[,count])=>n+count,0)/5);const refunds={};
  for(const[id,count]of eligible.sort(([a],[b])=>a.localeCompare(b))){const amount=Math.min(count,due);if(amount)refunds[id]=amount;due-=amount;if(!due)break;}
  return refunds;
}
export function applyEquipmentCraftRefund(s,refunds={}){for(const[id,n]of Object.entries(refunds))if(Number.isSafeInteger(n)&&n>0)addResource(s,id,n);return refunds;}
