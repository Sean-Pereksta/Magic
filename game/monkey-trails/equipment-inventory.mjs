// Individual ownership layered over the original, save-stable catalogue ranks.
import {GEAR,SPECIES,ELEMENTS} from './data.mjs';
import {recordLegacyEquipment} from './legacy.mjs';

export const equipmentSlots=p=>['weapon','armor','charm',...(SPECIES[p?.species]?.rarity==='ultra'||SPECIES[p?.species]?.ultra||p?.classification==='ultra'?['ultra']:[])];
const members=s=>Object.values(s.collection||{});
const validTier=t=>Number.isInteger(t)&&t>=1&&t<=12;
export const canChangeEquipment=s=>Boolean(s?.run)&&!['battle','replacement'].includes(s.run.phase);
const unlocked=s=>{if(!canChangeEquipment(s))throw Error('Change equipment between encounters.');};
const copyOwner=(s,id)=>{for(const p of members(s))for(const[slot,copyId]of Object.entries(p.equipmentCopies||{}))if(copyId===id)return{ownerId:p.id,slot};return{ownerId:null,slot:null};};
function createCopy(s,itemId,tier=1,source='discovery'){
  if(s.meta.equipmentSerial>=999999999999||Object.keys(s.meta.equipmentCopies).length>=4000)throw Error('Equipment storage is full. Upgrade available duplicate copies first.');
  const id=`gear-copy-${++s.meta.equipmentSerial}`;
  const row={id,itemId,tier,favorite:false,protected:false,isNew:true,acquiredAt:s.meta.equipmentSerial,source};
  s.meta.equipmentCopies[id]=row;return row;
}
function mirror(s,itemId){const tiers=Object.values(s.meta.equipmentCopies).filter(c=>c.itemId===itemId).map(c=>c.tier);if(tiers.length)s.meta.gear[itemId]=Math.max(...tiers);else delete s.meta.gear[itemId];}

export function normalizeEquipmentInventory(s){
  if(!s?.meta)return s;
  s.meta.gear??={};s.meta.equipmentSerial??=0;
  if(!Number.isSafeInteger(s.meta.equipmentSerial)||s.meta.equipmentSerial<0)throw Error('Invalid equipment serial.');
  if(s.meta.equipmentCopies===undefined){
    s.meta.equipmentCopies={};
    for(const[itemId,tier]of Object.entries(s.meta.gear)){
      if(!Object.hasOwn(GEAR,itemId)||!validTier(tier))throw Error('Invalid equipment rank.');
      const row=createCopy(s,itemId,tier,'restored');row.isNew=false;
    }
    for(const p of members(s)){
      p.equipmentCopies={};
      for(const[slot,itemId]of Object.entries(p.equipment||{}))if(itemId){const row=Object.values(s.meta.equipmentCopies).find(c=>c.itemId===itemId&&!copyOwner(s,c.id).ownerId);if(row)p.equipmentCopies[slot]=row.id;}
    }
  }
  const copies=s.meta.equipmentCopies;
  if(!copies||Array.isArray(copies)||typeof copies!=='object'||Object.keys(copies).length>4000)throw Error('Invalid equipment collection.');
  for(const[id,c]of Object.entries(copies))if(!c||c.id!==id||!Object.hasOwn(GEAR,c.itemId)||!validTier(c.tier)||!Number.isSafeInteger(c.acquiredAt)||c.acquiredAt<0||['favorite','protected','isNew'].some(k=>typeof c[k]!=='boolean'))throw Error('Invalid equipment copy.');
  s.meta.equipmentSerial=Math.max(s.meta.equipmentSerial,...Object.values(copies).map(c=>Math.max(c.acquiredAt,Number(/^gear-copy-(\d+)$/.exec(c.id)?.[1]||0))));
  if(s.meta.equipmentSerial>=999999999999)throw Error('Invalid equipment serial.');
  // Older clients and original fragment recipes may add a new definition.
  for(const[itemId,tier]of Object.entries(s.meta.gear))if(!Object.values(copies).some(c=>c.itemId===itemId)){
    if(!Object.hasOwn(GEAR,itemId)||!validTier(tier))throw Error('Invalid equipment rank.');createCopy(s,itemId,tier,'restored');
  }
  const assigned=new Set();
  for(const p of members(s)){
    p.equipment??={};p.equipmentCopies??={};const named=new Set(),slots=equipmentSlots(p);
    for(const slot of slots){
      p.equipment[slot]??=null;
      if(!p.equipmentCopies[slot]&&p.equipment[slot]){const row=Object.values(copies).find(c=>c.itemId===p.equipment[slot]&&!assigned.has(c.id)&&!copyOwner(s,c.id).ownerId);if(row)p.equipmentCopies[slot]=row.id;}
      const id=p.equipmentCopies[slot];if(!id){if(p.equipment[slot])throw Error('Equipment needs an individual available copy.');continue;}const row=copies[id];
      if(!row||assigned.has(id)||named.has(GEAR[row.itemId].name)||p.equipment[slot]!==row.itemId)throw Error('Invalid equipment assignment.');
      assigned.add(id);named.add(GEAR[row.itemId].name);
    }
    if(Object.entries(p.equipmentCopies).some(([slot,id])=>id&&!slots.includes(slot))||Object.entries(p.equipment).some(([slot,id])=>id&&!slots.includes(slot)))throw Error('This monkey cannot use that equipment slot.');
  }
  for(const itemId of new Set(Object.values(copies).map(c=>c.itemId)))mirror(s,itemId);
  return s;
}

export function ownedEquipmentCopies(s){normalizeEquipmentInventory(s);return Object.values(s.meta.equipmentCopies).map(c=>({...c,...copyOwner(s,c.id)}));}
export function equipmentCopy(s,id){normalizeEquipmentInventory(s);const row=s.meta.equipmentCopies[id];return row?{...row,...copyOwner(s,id)}:null;}
export function equipmentLoadout(s,p){
  const out={};for(const[slot,itemId]of Object.entries(p?.equipment||{}))if(itemId){const row=s.meta?.equipmentCopies?.[p.equipmentCopies?.[slot]];const tier=row?.itemId===itemId?row.tier:s.meta?.gear?.[itemId];if(validTier(tier)&&GEAR[itemId])out[itemId]=tier;}
  return out;
}
export function inspectGearCopies(s,itemId){return ownedEquipmentCopies(s).filter(c=>c.itemId===itemId);}
export function previewGearAward(s,itemId,{allowSalvage=true}={}){
  if(!Object.hasOwn(GEAR,itemId))throw Error('Unknown equipment.');
  let copies=s.meta.equipmentCopies?Object.values(s.meta.equipmentCopies).filter(c=>c.itemId===itemId).map(c=>({...c,...copyOwner(s,c.id)})):[];
  if(!copies.length&&s.meta.gear[itemId])copies=[{id:null,itemId,tier:s.meta.gear[itemId],acquiredAt:0,ownerId:members(s).find(p=>Object.values(p.equipment||{}).includes(itemId))?.id}];
  const free=copies.filter(c=>!c.ownerId&&!c.favorite&&!c.protected).sort((a,b)=>b.tier-a.tier||a.acquiredAt-b.acquiredAt),row=free.find(c=>c.tier<12)||free[0];
  return row&&(allowSalvage||row.tier<12)?{kind:row.tier===12?'salvaged':'upgraded',tier:Math.min(12,row.tier+1),copyId:row.id}:{kind:'discovered',tier:1,copyId:null};
}
export function awardGearCopy(s,itemId,{source='discovery',allowSalvage=true}={}){
  assertGearAwardCapacity(s,itemId,{allowSalvage});
  normalizeEquipmentInventory(s);
  const result=previewGearAward(s,itemId,{allowSalvage});
  if(result.kind==='discovered'){const row=createCopy(s,itemId,1,source);result.copyId=row.id;}
  else if(result.kind==='upgraded'){const row=s.meta.equipmentCopies[result.copyId];row.tier=result.tier;row.isNew=true;}
  mirror(s,itemId);if(result.kind!=='salvaged')recordLegacyEquipment(s,{crafted:source==='forge'||source==='fragments'});return result;
}
export function assertGearAwardCapacity(s,itemId,options={}){
  const award=previewGearAward(s,itemId,options);
  if(award.kind==='discovered'&&(Object.keys(s.meta.equipmentCopies||s.meta.gear||{}).length>=4000||JSON.stringify(s).length+256>650000))throw Error('Equipment storage is full. Upgrade available duplicate copies first.');
  return award;
}
export function previewEquipmentLoadout(s,copyId,monkeyId,slot){
  normalizeEquipmentInventory(s);const p=s.collection?.[monkeyId],copy=equipmentCopy(s,copyId);if(!p||!copy)throw Error('Choose owned equipment and a monkey.');
  const slots=equipmentSlots(p);slot??=slots.find(k=>!p.equipment[k])||GEAR[copy.itemId].slot||slots[0];
  if(!slots.includes(slot))throw Error('Choose a usable equipment slot.');
  if(Object.entries(p.equipment).some(([k,id])=>k!==slot&&id&&GEAR[id].name===GEAR[copy.itemId].name&&p.equipmentCopies[k]!==copyId))throw Error('One copy of a named item per monkey.');
  const before=equipmentLoadout(s,p),after={...before};delete after[p.equipment[slot]];after[copy.itemId]=copy.tier;
  return{before,after,copy,ownerId:copy.ownerId,slot};
}
export function assignEquipment(s,copyId,monkeyId,slot){
  unlocked(s);const view=previewEquipmentLoadout(s,copyId,monkeyId,slot),p=s.collection[monkeyId];
  for(const other of members(s))for(const[k,id]of Object.entries(other.equipmentCopies))if(id===copyId){other.equipment[k]=null;delete other.equipmentCopies[k];}
  p.equipment[view.slot]=view.copy.itemId;p.equipmentCopies[view.slot]=copyId;s.meta.equipmentCopies[copyId].isNew=false;return p;
}
export function removeEquipment(s,slot,monkeyId){unlocked(s);normalizeEquipmentInventory(s);const p=s.collection?.[monkeyId];if(!p||!equipmentSlots(p).includes(slot))throw Error('Choose a usable equipment slot.');p.equipment[slot]=null;delete p.equipmentCopies[slot];return p;}
export function setEquipmentCopyFlag(s,copyId,flag,value){normalizeEquipmentInventory(s);if(!['favorite','protected','isNew'].includes(flag)||typeof value!=='boolean'||!s.meta.equipmentCopies[copyId])throw Error('Choose an owned equipment copy.');s.meta.equipmentCopies[copyId][flag]=value;return equipmentCopy(s,copyId);}
export function upgradeEquipmentCopy(s,copyId){
  unlocked(s);const target=equipmentCopy(s,copyId);if(!target||target.tier>=12)throw Error('Choose equipment below maximum rank.');
  const duplicate=ownedEquipmentCopies(s).find(c=>c.id!==copyId&&c.itemId===target.itemId&&c.tier===target.tier&&!c.ownerId&&!c.protected&&!c.favorite);
  if(!duplicate)throw Error('Keep an available, unprotected duplicate at the same rank to upgrade.');
  delete s.meta.equipmentCopies[duplicate.id];s.meta.equipmentCopies[copyId].tier++;mirror(s,target.itemId);return equipmentCopy(s,copyId);
}
export function setEquipmentChoice(s,itemId,element){unlocked(s);if(!['trailfinderCompass','prismCompass'].includes(itemId)||!ELEMENTS[element])throw Error('Choose an available element.');s.meta.equipmentChoices??={};s.meta.equipmentChoices[itemId]=element;return element;}
