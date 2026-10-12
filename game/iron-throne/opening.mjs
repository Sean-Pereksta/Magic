import { BUILDINGS, UNITS } from './data.mjs';

export const OPENING_RESOURCES = Object.freeze({food:220,wood:170,stone:140,iron:75,gold:300,horses:16,tools:0,arms:0});
export const ROYAL_DECREES = Object.freeze({
  prosperity:{id:'prosperity',name:'Prosperity Decree',duration:6,benefits:['+10% food, wood, stone, iron and horse production from developed land.','First-level farms, lumber camps, mines, quarries, roads, barracks and ranges take one fewer turn through turn 6.']},
  mobilization:{id:'mobilization',name:'Mobilization Decree',duration:6,benefits:['Levies, spearmen, archers and scouts cost 10% fewer resources (rounded up).','Land armies receive +1 movement per turn.']},
  influence:{id:'influence',name:'Diplomatic Influence Decree',duration:6,benefits:['+1 regular diplomatic dispatch per turn.','Ambassadors receive +1 movement per turn.']}
});
const house=(s,owner)=>s?.kingdoms?.find(k=>k.id===owner);
export const openingEnabled=(s,owner)=>s?.ambitionVersion===1&&!!house(s,owner)?.opening;
export const openingActive=(s,owner,turn=s?.turn)=>openingEnabled(s,owner)&&turn>=1&&turn<=6;
export const openingCommandBonus=(s,owner,turn=s?.turn)=>openingEnabled(s,owner)&&turn>=0&&turn<=3?2:0;
export const decreeRemaining=(s,owner)=>openingActive(s,owner)&&house(s,owner).opening.decree?7-s.turn:0;
export const hasRoyalDecree=(s,owner,id)=>openingActive(s,owner)&&house(s,owner).opening.decree===id;
export const openingProductionFactor=(s,owner)=>hasRoyalDecree(s,owner,'prosperity')?1.1:1;

export function initializeOpeningHouse(s,owner,capital) {
  if(!openingEnabled(s,owner))return;
  const k=house(s,owner),tiles=Object.values(s.tiles).filter(t=>t.owner===owner);
  const infrastructure=tiles.reduce((n,t)=>n+(t.workshop?(t.levels?.workshop||1):0)+(t.building==='city'?(BUILDINGS.city.levels[(t.levels?.city||1)-1].orders||0):0),0);
  k.commands=Math.min(8,5+infrastructure);
  if(s.armies.some(a=>a.owner===owner&&a.scout))return;
  s.armies.push({id:`army-${s.nextId++}`,name:'Royal Scouts',owner,tile:capital.id,scout:true,units:{...Object.fromEntries(Object.keys(UNITS).map(id=>[id,0])),scout:2},formation:'balanced',retreats:0,morale:1,order:'hold',path:[],target:null});
}
export function chooseRoyalDecree(s,owner,id) {
  const k=house(s,owner);
  if(s.phase==='founding'||s.outcome||!openingActive(s,owner))return {ok:false,error:'Royal Decrees are available after founding through turn 6 in new campaigns.'};
  if(!Object.hasOwn(ROYAL_DECREES,id))return {ok:false,error:'Choose a valid Royal Decree.'};
  if(k.opening.decree)return {ok:false,error:'Your House has already selected its Royal Decree.'};
  k.opening.decree=id;k.opening.decreeTurn=s.turn;
  return {ok:true};
}
export function applyOpeningConstruction(s,owner,t,type,spec) {
  if(!spec||!openingEnabled(s,owner)||spec.repair)return spec;
  const first=spec.level===1||spec.level===undefined&&!t.levels?.[type]&&t.building!==type&&!t[type];
  const charter=type==='town'&&first&&!house(s,owner).opening.charterUsed;
  const basic=first&&['farm','lumber','mine','quarry','road','barracks','range'].includes(type)&&(openingCommandBonus(s,owner)>0||hasRoyalDecree(s,owner,'prosperity'));
  if(!charter&&!basic)return spec;
  return {...spec,turns:Math.max(1,spec.turns-1),cost:charter?Object.fromEntries(Object.entries(spec.cost).map(([r,n])=>[r,r==='gold'?n:Math.ceil(n*.75)])):{...spec.cost},openingBonus:charter?'First Frontier Charter':'Opening construction',charter};
}
export function applyOpeningRecruitment(s,owner,type,cost) {
  return cost&&hasRoyalDecree(s,owner,'mobilization')&&['levy','spearman','archer','scout'].includes(type)?Object.fromEntries(Object.entries(cost).map(([r,n])=>[r,Math.ceil(n*.9)])):cost;
}
export function validateOpening(s) {
  const fail=()=>{throw new Error('Damaged opening campaign data.');};
  if(s.ambitionVersion!==undefined&&s.ambitionVersion!==1)fail();
  for(const k of s.kingdoms){
    if(s.ambitionVersion===undefined){if(k.opening!==undefined)fail();continue;}
    const o=k.opening;
    if(!o||typeof o!=='object'||Array.isArray(o)||typeof o.charterUsed!=='boolean'||o.decree!==null&&!Object.hasOwn(ROYAL_DECREES,o.decree))fail();
    if(o.decree===null?o.decreeTurn!==null:!Number.isInteger(o.decreeTurn)||o.decreeTurn<1||o.decreeTurn>Math.min(6,s.turn))fail();
  }
  for(const a of s.armies)if(a.scout!==undefined&&typeof a.scout!=='boolean')fail();
}
