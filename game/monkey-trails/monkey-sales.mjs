// Permanent, per-individual recycling receipts. No sale payout enters the
// expedition until explicitly withdrawn from the persistent Forge Reserve.
import {normalizeExplorer} from './exploration.mjs';
import {SPECIES,GEAR,ELEMENTS,DIRECTIONS} from './data.mjs';
import {normalizeEquipmentInventory} from './equipment-inventory.mjs';
import {normalizeCompanions} from './companions.mjs';

export const SALE_PAYOUTS=Object.freeze({
  common:{fruit:12,essence:5,materials:3,fragments:[['common',1]]},
  uncommon:{fruit:20,essence:9,materials:6,fragments:[['uncommon',1]]},
  rare:{fruit:35,essence:15,materials:11,fragments:[['rare',2]]},
  ultra:{fruit:60,essence:24,materials:18,fragments:[['epic',1],['legendary',1]]}
});
const count=n=>Number.isSafeInteger(n)&&n>=0&&n<=1e9;
const object=o=>o&&typeof o==='object'&&!Array.isArray(o);
const validResource=id=>id==='fruit'||id.startsWith('essence:')&&!!ELEMENTS[id.slice(8)]||id.startsWith('bond:')&&!!SPECIES[id.slice(5)]||id.startsWith('fragment:')&&!!GEAR[id.slice(9)];
const copy=o=>JSON.parse(JSON.stringify(o));
export function normalizeSales(s){
  s.meta??={};s.meta.monkeySales??={version:1,reserve:{},receipts:{},transferSerial:0,lastTransfer:null};
  const data=s.meta.monkeySales;
  if(!object(data)||data.version!==1||!object(data.reserve)||!object(data.receipts)||!count(data.transferSerial)||Object.entries(data.reserve).some(([id,n])=>!validResource(id)||!count(n)))throw Error('Invalid Forge Reserve checkpoint.');
  for(const[id,row]of Object.entries(data.receipts)){
    if(!object(row)||row.monkeyId!==id||row.operationId!==`sale:${id}`||!SPECIES[row.species]||!object(row.payout)||Object.entries(row.payout).some(([key,n])=>!validResource(key)||!count(n))||s.collection?.[id])throw Error('Invalid monkey sale receipt.');
  }
  if(data.lastTransfer!==null&&(!object(data.lastTransfer)||data.lastTransfer.operationId!==`reserve:${data.transferSerial}`||!object(data.lastTransfer.resources)||Object.entries(data.lastTransfer.resources).some(([id,n])=>!validResource(id)||!count(n))))throw Error('Invalid Forge Reserve withdrawal.');
  return data;
}
const canPrepare=s=>['trail','camp'].includes(s.run?.phase)&&!s.run?.pendingEncounter&&!s.run?.battle&&!s.run?.capture;
function saleBlock(s,p){
  if(!p)return 'Choose a collected monkey.';
  if(!canPrepare(s))return 'Finish combat and unresolved encounters before selling.';
  if(Object.keys(s.collection).length<3)return 'Keep at least two companions. Selling requires three owned monkeys before the sale.';
  if(p.favorite||p.protected||normalizeCompanions(s).favorites.includes(p.id))return 'Remove this monkey’s favorite or protection first.';
  const others=Object.values(s.collection).filter(q=>q.id!==p.id);
  if(!others.some(q=>q.hp>0))return 'Revive another companion before selling your last conscious monkey.';
  return '';
}
export function salePreview(s,id){
  const data=normalizeSales(s),p=s.collection?.[id];
  if(!p){if(data.receipts[id])return{allowed:false,reason:'This monkey has already been sold.',receipt:copy(data.receipts[id])};return{allowed:false,reason:'Choose a collected monkey.',payout:{},equipment:[],lineups:[]};}
  normalizeEquipmentInventory(s);const species=SPECIES[p.species],base=SALE_PAYOUTS[species.rarity],bonus=Math.min(.20,Math.floor((p.level-1)/10)*.025+(p.stage||0)*.025),payout={fruit:Math.floor(base.fruit*(1+bonus)),[`bond:${p.species}`]:Math.floor(base.materials*(1+bonus))};
  // Dual types share the displayed essence total, never duplicate it.
  const types=[...new Set(species.types.length?species.types:p.types?.length?p.types:[DIRECTIONS[s.run.direction]||'leaf'])],total=Math.floor(base.essence*(1+bonus));types.forEach((t,i)=>{payout[`essence:${t}`]=Math.floor(total/types.length)+(i<total%types.length?1:0);});
  for(const[rarity,amount]of base.fragments){const pool=Object.entries(GEAR).filter(([,g])=>g.rarity===rarity),matching=pool.filter(([,g])=>types.includes(g.element)),[itemId]=(matching.length?matching:pool).sort(([a],[b])=>a.localeCompare(b))[0];payout[`fragment:${itemId}`]=(payout[`fragment:${itemId}`]||0)+amount;}
  const equipment=Object.entries(p.equipmentCopies||{}).filter(([,copyId])=>copyId).map(([slot,copyId])=>({slot,...s.meta.equipmentCopies[copyId]})),lineups=normalizeCompanions(s).lineups.filter(row=>row.members.includes(id)).map(row=>row.name),reason=saleBlock(s,p);
  return{allowed:!reason,reason,monkeyId:id,species:p.species,rarity:species.rarity,bonus,payout,equipment,lineups,inTeam:s.run.party.includes(id),remaining:Object.keys(s.collection).length-1};
}
export function sellMonkey(s,id,{confirmUltra=false}={}){
  const data=normalizeSales(s);if(data.receipts[id])return copy(data.receipts[id]);
  const preview=salePreview(s,id);if(!preview.allowed)throw Error(preview.reason);
  if(preview.rarity==='ultra'&&!confirmUltra)throw Error('Confirm the additional Ultra sale warning.');
  const p=s.collection[id],friends=normalizeCompanions(s),receipt={operationId:`sale:${id}`,monkeyId:id,species:p.species,payout:copy(preview.payout),equipment:preview.equipment.map(row=>row.id)};
  for(const[key,n]of Object.entries(preview.payout))if((data.reserve[key]||0)+n>1e9)throw Error('Transfer some Forge Reserve resources before selling again.');
  if(JSON.stringify(s).length+JSON.stringify(receipt).length>650000)throw Error('This save has reached its storage limit.');
  // Every check precedes removal and payment; ownership references are repaired
  // together. Equipment copies stay in inventory and become unassigned.
  for(const slot of Object.keys(p.equipment||{}))p.equipment[slot]=null;p.equipmentCopies={};
  delete s.collection[id];delete s.run.injuries?.[id];
  s.run.party=s.run.party.filter(member=>member!==id);
  if(!s.run.party.some(member=>s.collection[member]?.hp>0)){const next=Object.values(s.collection).find(q=>q.hp>0);s.run.party=[next.id,...s.run.party.filter(member=>member!==next.id)].slice(0,3);}
  if(!s.run.party.includes(s.run.activeId)||s.collection[s.run.activeId]?.hp<=0)s.run.activeId=s.run.party.find(member=>s.collection[member]?.hp>0)||s.run.party[0];
  s.run.monkey=s.collection[s.run.activeId];normalizeExplorer(s);
  friends.lineups=friends.lineups.map(row=>({...row,members:row.members.filter(member=>member!==id)})).filter(row=>row.members.length);
  friends.favorites=friends.favorites.filter(member=>member!==id);if(friends.evolutionGoal?.monkeyId===id)friends.evolutionGoal=null;
  if(s.meta.pinnedGoal?.monkeyId===id)s.meta.pinnedGoal=null;
  if(s.run.lastCapture?.id===id)s.run.lastCapture=null;
  for(const[key,n]of Object.entries(preview.payout))data.reserve[key]=(data.reserve[key]||0)+n;
  data.receipts[id]=receipt;return copy(receipt);
}
export function nextReserveOperation(s){return `reserve:${normalizeSales(s).transferSerial+1}`;}
export function transferReserve(s,resources,operationId){
  const data=normalizeSales(s),normalized=Object.fromEntries(Object.entries(resources||{}).filter(([,n])=>n!==0).sort(([a],[b])=>a.localeCompare(b))),fingerprint=JSON.stringify(normalized);
  if(data.lastTransfer?.operationId===operationId){if(JSON.stringify(data.lastTransfer.resources)!==fingerprint)throw Error('That withdrawal used different resources.');return copy(data.lastTransfer);}
  if(!canPrepare(s))throw Error('Transfer reserved resources between encounters.');
  if(operationId!==nextReserveOperation(s)||!Object.keys(normalized).length)throw Error('Choose the next reserve withdrawal and some resources.');
  const targets=[];
  for(const[id,n]of Object.entries(normalized)){
    if(!validResource(id)||!count(n)||n<1||(data.reserve[id]||0)<n)throw Error('The Forge Reserve does not contain that amount.');
    const bag=id==='fruit'?s.run:id.startsWith('essence:')?s.run.essence:id.startsWith('bond:')?(s.meta.bondMaterials??={}):s.meta.fragments,key=id==='fruit'?'fruit':id.slice(id.indexOf(':')+1);
    if((bag[key]||0)+n>1e9)throw Error('This expedition cannot carry that many resources.');targets.push({bag,key,id,n});
  }
  for(const{bag,key,id,n}of targets){data.reserve[id]-=n;bag[key]=(bag[key]||0)+n;}
  data.transferSerial++;return copy(data.lastTransfer={operationId,resources:normalized});
}
export function setMonkeyProtection(s,id,protectedValue){const p=s.collection?.[id];if(!p||typeof protectedValue!=='boolean')throw Error('Choose a collected monkey.');p.protected=protectedValue;return p;}
export function reserveResourceName(id){if(id==='fruit')return 'Fruit';const [kind,key]=id.split(':');return kind==='essence'?`${ELEMENTS[key]?.name} essence`:kind==='bond'?`${SPECIES[key]?.name} species materials`:`${GEAR[key]?.name} fragments`;}

