// DOM-free expedition forge. A preview never writes inventory, receipts, or RNG.
import {ELEMENTS,GEAR,ITEMS,SPECIES} from './data.mjs';
import {GEAR_RARITIES,LEGACY_GEAR_AFFINITIES} from './equipment-data.mjs';
import {previewGearAward,awardGearCopy,normalizeEquipmentInventory,assertGearAwardCapacity,canChangeEquipment} from './equipment-inventory.mjs';
import {equipmentCraftRefund,applyEquipmentCraftRefund,isCommonCraftIngredient} from './equipment-trail.mjs';

export {GEAR_RARITIES};
export const FORGE_LIMITS={minimumValue:8,maximumUnits:60,maximumTier:12,tickets:1000000};
const terms=s=>{const campBonus=s.run?.phase==='camp';return{campBonus,discount:campBonus?.2:0,qualityBoost:campBonus?.10:0};};
const chargedCount=(count,campBonus)=>campBonus?Math.max(1,Math.ceil(count*4/5)):count;
const ELEMENT_IDS=Object.keys(ELEMENTS),AFFINITY_IDS=[...ELEMENT_IDS,'neutral'],RARITY_IDS=Object.keys(GEAR_RARITIES);
const has=(object,id)=>Object.prototype.hasOwnProperty.call(object,id);
const copy=value=>JSON.parse(JSON.stringify(value));
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
const validCount=n=>Number.isSafeInteger(n)&&n>=0;
const owned=n=>Number.isFinite(n)&&n>=0?Math.floor(n):0;
const metadata=id=>({...LEGACY_GEAR_AFFINITIES[id],...GEAR[id]});
const supplyValues={berry:[3,.10],tonic:[4,.20],salve:[4,.20],bondCharm:[5,.40],revive:[10,.55],trailRibbon:[6,.40],treasureScent:[6,.45],scoutFeather:[8,.65],campfireSeed:[6,.45]};
const rarityForQuality=q=>q>=.9?'legendary':q>=.7?'rare':q>=.3?'uncommon':'common';
const tierOf=(s,id)=>s.meta?.gear?.[id]||0;

function resource(s,id){
  let row;
  if(id.startsWith('quality:')){
    const baseId=id.slice(8),base=baseId.startsWith('quality:')?null:resource(s,baseId);
    if(!base||base.rarity!=='common'||!['fruit','essence','item','fragment'].includes(base.kind))return null;
    row={...base,name:`Uncommon ${base.name.toLowerCase()}`,kind:'quality',value:Math.max(base.value+1,Math.ceil(base.value*1.5)),quality:Math.max(.45,base.quality+.20),rarity:'uncommon',raw:s.run?.forgeMaterials?.[baseId]};
  }
  else if(id==='fruit')row={name:'Trail fruit',kind:'fruit',icon:'●',value:1,quality:0,raw:s.run?.fruit};
  else if(id.startsWith('essence:')){
    const element=id.slice(8),e=has(ELEMENTS,element)&&ELEMENTS[element];if(!e)return null;
    row={name:`${e.name} essence`,kind:'essence',icon:e.icon,value:2,quality:.25,element,elementWeight:3,raw:s.run?.essence?.[element]};
  }else if(id==='item:charmMaterial')row={name:'Charm material',kind:'item',icon:'◇',value:3,quality:.35,raw:s.run?.items?.charmMaterial};
  else if(id.startsWith('item:')){
    const item=id.slice(5),v=has(supplyValues,item)&&supplyValues[item];if(!has(ITEMS,item)||!v)return null;
    row={name:ITEMS[item].name,kind:'item',icon:'▰',value:v[0],quality:v[1],raw:s.run?.items?.[item]};
  }else if(id.startsWith('fragment:')){
    const gearId=id.slice(9),g=has(GEAR,gearId)&&metadata(gearId);if(!g)return null;
    row={name:`${g.name} fragment`,kind:'fragment',icon:g.icon,value:4,quality:({common:.45,uncommon:.60,rare:.75,epic:.85,legendary:.95,mythic:1})[g.rarity],rarity:g.rarity,element:g.element,elementWeight:2,raw:s.meta?.fragments?.[gearId]};
  }else if(id.startsWith('bond:')){
    const species=id.slice(5),p=has(SPECIES,species)&&SPECIES[species];if(!p)return null;
    const value=({common:4,uncommon:6,rare:9,ultra:12})[p.rarity],quality=({common:.35,uncommon:.55,rare:.80,ultra:1})[p.rarity];
    row={name:`${p.name} bond material`,kind:'bond',icon:'♡',value,quality,rarity:p.rarity==='ultra'?'legendary':p.rarity,elements:p.types,elementWeight:3,raw:s.meta?.bondMaterials?.[species]};
  }
  if(!row)return null;
  const rarity=row.rarity||rarityForQuality(row.quality);
  return{...row,id,owned:owned(row.raw),rarity,qualityLabel:GEAR_RARITIES[rarity].name,description:`${GEAR_RARITIES[rarity].name} material · ${row.value} forge value each.`};
}

export function forgeInventory(s){
  const ids=['fruit',...ELEMENT_IDS.map(t=>`essence:${t}`),'item:charmMaterial',...Object.keys(ITEMS).map(id=>`item:${id}`),...Object.keys(GEAR).map(id=>`fragment:${id}`),...Object.keys(SPECIES).map(id=>`bond:${id}`),...Object.keys(s.run?.forgeMaterials||{}).map(id=>`quality:${id}`)];
  return [...new Set(ids)].map(id=>resource(s,id)).filter(row=>row&&(row.owned>0||row.kind==='fruit'||row.kind==='essence'||row.id==='item:charmMaterial')).map(({raw,...row})=>row);
}

function mix(s,inputs){
  if(!inputs||typeof inputs!=='object'||Array.isArray(inputs)||![Object.prototype,null].includes(Object.getPrototypeOf(inputs)))throw Error('Choose resource amounts for your mix.');
  const entries=Object.entries(inputs);if(entries.length>180)throw Error('This forge mix contains too many resource types.');
  const normalized={},costInputs={},costSummary=[],weights=Object.fromEntries(AFFINITY_IDS.map(id=>[id,2])),benefits=terms(s);let units=0,value=0,qualityValue=0,chargedUnits=0,chargedValue=0;
  for(const[id,count]of entries.sort(([a],[b])=>a.localeCompare(b))){
    const row=resource(s,id);if(!row)throw Error('This mix contains an unknown resource.');
    if(!validCount(count))throw Error('Use whole, non-negative resource amounts.');
    if(!count)continue;
    if(!Number.isFinite(row.raw)||row.raw<0||count>row.raw)throw Error(`Gather more ${row.name.toLowerCase()} for this mix.`);
    units+=count;if(units>FORGE_LIMITS.maximumUnits)throw Error(`Use at most ${FORGE_LIMITS.maximumUnits} resource units per craft.`);
    normalized[id]=count;value+=count*row.value;qualityValue+=count*row.value*row.quality;
    const elements=row.elements?.length?row.elements:row.element?[row.element]:[];
    for(const element of elements)if(element in weights)weights[element]+=count*(row.elementWeight||0)/elements.length;
    const cost=chargedCount(count,benefits.campBonus);costInputs[id]=cost;chargedUnits+=cost;chargedValue+=cost*row.value;
    costSummary.push({id,name:row.name,count:cost,offeredCount:count,value:row.value*cost,offeredValue:row.value*count});
  }
  // Resource rarity is the main influence; a larger offering adds a small,
  // capped benefit. All rarity tiers always retain a visible possibility.
  const resourceQuality=value?qualityValue/value:0;
  const baseQuality=clamp(.85*resourceQuality+.15*clamp((value-8)/72,0,1),0,1),quality=clamp(baseQuality+benefits.qualityBoost,0,1);
  return{inputs:normalized,costInputs,costSummary,units,offeredUnits:units,chargedUnits,value,chargedValue,resourceQuality,baseQuality,quality,...benefits,weights,fingerprint:JSON.stringify(normalized)};
}

// Largest remainders make the actual lottery and every displayed percentage
// agree exactly: one ticket is 0.0001%. No hidden rerolls or separate odds.
function apportion(rows){
  const sum=rows.reduce((n,row)=>n+row.weight,0);if(!(sum>0)||!Number.isFinite(sum))return[];
  const scaled=rows.map((row,index)=>{const exact=row.weight/sum*FORGE_LIMITS.tickets,tickets=Math.floor(exact);return{...row,index,tickets,remainder:exact-tickets};});
  let remaining=FORGE_LIMITS.tickets-scaled.reduce((n,row)=>n+row.tickets,0);
  for(const row of [...scaled].sort((a,b)=>b.remainder-a.remainder||a.index-b.index)){if(remaining--<=0)break;row.tickets++;}
  return scaled.map(({weight,index,remainder,...row})=>({...row,chance:row.tickets/FORGE_LIMITS.tickets}));
}

function odds(s,m){
  const q=m.quality,rarityWeights={common:.70-.64*q,uncommon:.23-.10*q,rare:.058+.23*q,epic:.010+.22*q,legendary:.0018+.20*q,mythic:.0002+.09*q};
  const weightTotal=Object.values(m.weights).reduce((a,b)=>a+b,0),buckets={};
  for(const id of Object.keys(GEAR)){const g=metadata(id),key=`${g.rarity}:${g.element}`;(buckets[key]??=[]).push(id);}
  const raw=[];
  for(const rarity of RARITY_IDS)for(const element of AFFINITY_IDS){
    const ids=buckets[`${rarity}:${element}`]||[];
    for(const id of ids){const g=metadata(id),currentTier=tierOf(s,id),award=previewGearAward(s,id);raw.push({id,name:g.name,icon:g.icon,rarity,element,slot:g.slot,desc:g.desc,currentTier,nextTier:award.tier,kind:award.kind,copyId:award.copyId,weight:rarityWeights[rarity]*m.weights[element]/weightTotal/ids.length});}
  }
  const outcomes=apportion(raw);
  const rarityOdds=RARITY_IDS.map(id=>{const tickets=outcomes.filter(row=>row.rarity===id).reduce((n,row)=>n+row.tickets,0);return{id,...GEAR_RARITIES[id],tickets,chance:tickets/FORGE_LIMITS.tickets};});
  const elementOdds=AFFINITY_IDS.map(id=>{const tickets=outcomes.filter(row=>row.element===id).reduce((n,row)=>n+row.tickets,0);return{id,...(id==='neutral'?{name:'Neutral',icon:'◇',color:'#c9c0ac'}:{name:ELEMENTS[id].name,icon:ELEMENTS[id].icon,color:ELEMENTS[id].color}),tickets,chance:tickets/FORGE_LIMITS.tickets};});
  return{outcomes,rarityOdds,elementOdds};
}

function ledger(s){
  const f=s.meta?.forge;
  if(f===undefined)return{serial:0,last:null};
  if(!f||typeof f!=='object'||Array.isArray(f)||!validCount(f.serial)||f.serial>999999999999||f.serial>0&&!f.last||f.serial===0&&f.last)throw Error('This forge checkpoint is invalid.');
  if(f.last){
    const r=f.last,g=has(GEAR,r.itemId)&&metadata(r.itemId),bounded=(n,min,max)=>Number.isFinite(n)&&n>=min&&n<=max;
    let normalized;try{normalized=mixForReplay(r.inputs);}catch{throw Error('This forge checkpoint has an invalid receipt.');}
    if(!g||r.operationId!==`forge-${f.serial}`||r.serial!==f.serial||r.fingerprint!==JSON.stringify(normalized)||!['discovered','upgraded','salvaged'].includes(r.kind)||!Number.isInteger(r.tier)||r.tier<1||r.tier>12||r.kind==='discovered'&&r.tier!==1||r.kind==='upgraded'&&r.tier<2||r.kind==='salvaged'&&r.tier!==12||r.rarity!==g.rarity||r.element!==g.element||r.slot!==g.slot||!bounded(r.quality,0,1)||!bounded(r.chance,Number.MIN_VALUE,1)||!validCount(r.value)||r.value<8||r.value>720||Object.values(normalized).reduce((a,b)=>a+b,0)>60||!Object.keys(normalized).length)throw Error('This forge checkpoint has an invalid receipt.');
    for(const key of ['name','desc','icon'])if(typeof r[key]!=='string'||r[key].length>1200)throw Error('This forge checkpoint has an invalid receipt.');
    const hasCharges=['costInputs','chargedUnits','chargedValue','campBonus','discount','qualityBoost','baseQuality','offeredUnits'].some(key=>has(r,key));
    let costs=normalized;
    if(hasCharges){
      try{costs=mixForReplay(r.costInputs);}catch{throw Error('This forge checkpoint has invalid charged inputs.');}
      if(typeof r.campBonus!=='boolean'||r.discount!==(r.campBonus?.2:0)||r.qualityBoost!==(r.campBonus?.1:0)||!bounded(r.baseQuality,0,1)||Math.abs(r.quality-clamp(r.baseQuality+r.qualityBoost,0,1))>1e-12||r.offeredUnits!==Object.values(normalized).reduce((a,b)=>a+b,0)||r.chargedUnits!==Object.values(costs).reduce((a,b)=>a+b,0)||!validCount(r.chargedValue)||r.chargedValue<1||r.chargedValue>r.value||Object.keys(costs).length!==Object.keys(normalized).length||Object.entries(normalized).some(([id,count])=>costs[id]!==chargedCount(count,r.campBonus)))throw Error('This forge checkpoint has invalid camp benefits.');
    }
    if(!Array.isArray(r.costSummary)||r.costSummary.length!==Object.keys(normalized).length||new Set(r.costSummary.map(row=>row?.id)).size!==r.costSummary.length||r.costSummary.some(row=>!row||costs[row.id]!==row.count||!validCount(row.value)||row.value<row.count||typeof row.name!=='string'||row.name.length>200||hasCharges&&(normalized[row.id]!==row.offeredCount||!validCount(row.offeredValue)||row.offeredValue<row.value||row.value*row.offeredCount!==row.offeredValue*row.count))||r.costSummary.reduce((n,row)=>n+row.value,0)!==(hasCharges?r.chargedValue:r.value)||hasCharges&&r.costSummary.reduce((n,row)=>n+row.offeredValue,0)!==r.value)throw Error('This forge checkpoint has an invalid cost receipt.');
    if(r.kind==='salvaged'){
      const expected=Math.max(1,Math.floor(r.value*.4/4)),a=r.salvage;
      if(!a||a.id!==`fragment:${r.itemId}`||a.itemId!==r.itemId||a.count!==expected||a.value!==expected*4||typeof a.name!=='string'||a.name.length>200)throw Error('This forge checkpoint has invalid salvage.');
    }else if(r.salvage!==null)throw Error('This forge checkpoint has invalid salvage.');
    if(r.copyId!==undefined&&r.copyId!==null&&(typeof r.copyId!=='string'||r.copyId.length>100))throw Error('This forge checkpoint has invalid equipment ownership.');
    if(r.refunds!==undefined){
      const refunds=r.refunds,limit=Math.floor(Object.entries(costs).reduce((n,[id,count])=>n+(isCommonCraftIngredient(id)?count:0),0)/5);
      if(!refunds||typeof refunds!=='object'||Array.isArray(refunds)||Object.entries(refunds).some(([id,count])=>!isCommonCraftIngredient(id)||!validCount(count)||count<=0||count>(costs[id]||0))||Object.values(refunds).reduce((n,count)=>n+count,0)>limit)throw Error('This forge checkpoint has invalid ingredient refunds.');
    }
  }
  return f;
}

export function validateForgeCheckpoint(s){ledger(s);return s;}
export function nextForgeOperationId(s){const f=ledger(s);if(f.serial===999999999999)throw Error('This forge has reached its operation limit.');return`forge-${f.serial+1}`;}

export function previewForge(s,inputs={}){
  let m,reason='';
  try{m=mix(s,inputs);}catch(error){const benefits=terms(s);reason=error.message;m={inputs:{},costInputs:{},costSummary:[],units:0,offeredUnits:0,chargedUnits:0,value:0,chargedValue:0,resourceQuality:0,baseQuality:0,quality:benefits.qualityBoost,...benefits,weights:Object.fromEntries(AFFINITY_IDS.map(id=>[id,2])),fingerprint:'{}'};}
  if(!reason&&m.value<FORGE_LIMITS.minimumValue)reason=`Add resources worth at least ${FORGE_LIMITS.minimumValue} forge value.`;
  if(!reason&&!canChangeEquipment(s))reason='Finish the active battle before crafting equipment.';
  const probabilities=odds(s,m);if(!reason&&!probabilities.outcomes.length)reason='No eligible equipment is available. Choose another resource mix.';
  return{canCraft:!reason,reason,...m,...probabilities,refunds:equipmentCraftRefund(s,m.costInputs),limits:{...FORGE_LIMITS}};
}

function assertEquipmentInventory(s){
  if(!s.meta?.gear||typeof s.meta.gear!=='object'||Array.isArray(s.meta.gear))throw Error('Equipment inventory is unavailable.');
  for(const key of ['fragments','bondMaterials'])if(s.meta[key]!==undefined&&(!s.meta[key]||typeof s.meta[key]!=='object'||Array.isArray(s.meta[key])))throw Error('This forge resource inventory is invalid.');
  for(const[id,tier]of Object.entries(s.meta.gear))if(!has(GEAR,id)||!Number.isSafeInteger(tier)||tier<1||tier>FORGE_LIMITS.maximumTier)throw Error('Equipment inventory contains an invalid tier.');
  if(!Number.isSafeInteger(s.rng)||s.rng<0||s.rng>4294967295)throw Error('This forge checkpoint has invalid randomness.');
  for(const id of Object.keys(GEAR)){const amount=s.meta.fragments?.[id];if(amount!==undefined&&(!validCount(amount)||amount>Number.MAX_SAFE_INTEGER-1000))throw Error('Equipment fragment inventory is invalid.');}
  if(s.run?.forgeMaterials!==undefined&&(!s.run.forgeMaterials||typeof s.run.forgeMaterials!=='object'||Array.isArray(s.run.forgeMaterials)||Object.entries(s.run.forgeMaterials).some(([id,count])=>!resource(s,`quality:${id}`)||!validCount(count)||count>1e9)))throw Error('Upgraded trail materials are invalid.');
  // Validate individual ownership without migrating or consuming the live save.
  const normalized=copy(s);normalizeEquipmentInventory(normalized);return normalized;
}

function spend(s,id,count){
  if(id==='fruit'){s.run.fruit-=count;return;}
  if(id.startsWith('quality:')){s.run.forgeMaterials[id.slice(8)]-=count;return;}
  const split=id.indexOf(':'),kind=id.slice(0,split),key=id.slice(split+1);
  const inventory=kind==='essence'?s.run.essence:kind==='item'?s.run.items:kind==='fragment'?s.meta.fragments:s.meta.bondMaterials;
  inventory[key]-=count;
}

export function craftForge(s,inputs,operationId){
  // The next sequence ID is chosen before a roll. Replaying its receipt after a
  // reload returns the same reward, even if the current camp bonus has changed.
  const f=ledger(s),normalized=mixForReplay(inputs),fingerprint=JSON.stringify(normalized);
  if(typeof operationId!=='string'||!/^forge-[1-9]\d{0,11}$/.test(operationId))throw Error('Choose a valid forge operation before crafting.');
  if(f.last?.operationId===operationId){if(f.last.fingerprint!==fingerprint)throw Error('That forge operation already used a different mix.');return copy(f.last);}
  if(operationId!==`forge-${f.serial+1}`)throw Error('That forge operation is already resolved or out of sequence.');
  const preview=previewForge(s,inputs);if(!preview.canCraft)throw Error(preview.reason);
  const verified=assertEquipmentInventory(s);
  // All checks precede the only RNG transition and every inventory mutation.
  let x=s.rng|0;x^=x<<13;x^=x>>>17;x^=x<<5;const nextRng=(x>>>0)||1234567;
  const roll=Math.floor(nextRng/4294967296*FORGE_LIMITS.tickets);let remaining=roll,result=preview.outcomes.at(-1);
  for(const row of preview.outcomes){remaining-=row.tickets;if(remaining<0){result=row;break;}}
  const award=previewGearAward(verified,result.id),salvageCount=award.kind==='salvaged'?Math.max(1,Math.floor(preview.value*.4/4)):0;
  assertGearAwardCapacity(verified,result.id);
  const salvage=salvageCount?{id:`fragment:${result.id}`,itemId:result.id,name:`${result.name} fragments`,count:salvageCount,value:salvageCount*4}:null;
  const receipt={operationId,fingerprint,itemId:result.id,name:result.name,rarity:result.rarity,element:result.element,slot:result.slot,icon:result.icon,desc:result.desc,kind:award.kind,tier:award.tier,copyId:award.copyId,salvage,refunds:preview.refunds,inputs:preview.inputs,costInputs:preview.costInputs,costSummary:preview.costSummary,value:preview.value,chargedValue:preview.chargedValue,offeredUnits:preview.offeredUnits,chargedUnits:preview.chargedUnits,campBonus:preview.campBonus,discount:preview.discount,qualityBoost:preview.qualityBoost,baseQuality:preview.baseQuality,quality:preview.quality,chance:result.chance,serial:f.serial+1};
  const checkpoint={...verified,meta:{...verified.meta,forge:{serial:receipt.serial,last:receipt}}};
  if(JSON.stringify(checkpoint).length+256>650000)throw Error('Equipment storage is full. Upgrade available duplicate copies first.');
  for(const[id,count]of Object.entries(preview.costInputs))spend(s,id,count);
  if(salvage){s.meta.fragments??={};s.meta.fragments[result.id]=(s.meta.fragments[result.id]||0)+salvageCount;}
  else receipt.copyId=awardGearCopy(s,result.id,{source:'forge',value:preview.value}).copyId;
  applyEquipmentCraftRefund(s,receipt.refunds);
  s.rng=nextRng;s.meta.forge={serial:receipt.serial,last:copy(receipt)};
  return copy(receipt);
}

function mixForReplay(inputs){
  if(!inputs||typeof inputs!=='object'||Array.isArray(inputs)||![Object.prototype,null].includes(Object.getPrototypeOf(inputs)))throw Error('Choose resource amounts for your mix.');
  const normalized={};
  for(const[id,count]of Object.entries(inputs).sort(([a],[b])=>a.localeCompare(b))){if(!resource({},id))throw Error('This mix contains an unknown resource.');if(!validCount(count))throw Error('Use whole, non-negative resource amounts.');if(count)normalized[id]=count;}
  return normalized;
}
