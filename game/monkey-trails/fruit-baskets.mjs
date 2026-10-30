// Fruit baskets use only accepted core discoveries and subsequent trail walking.
// Positions are relative fantasy-map coordinates, never personal GPS fixes.
import {LANDS} from './lands.mjs';
export const FRUIT_BASKET_FEET=500;
const FOOT=.3048,GOAL=FRUIT_BASKET_FEET*FOOT,MAX=1e9;
const ids=new Set(LANDS.map(row=>row.id)),forest=new Set(['emerald-jungle','moonlit-forest']);
const copy=value=>JSON.parse(JSON.stringify(value)),record=value=>value&&typeof value==='object'&&!Array.isArray(value),count=value=>Number.isSafeInteger(value)&&value>=0&&value<=MAX;
const hash=value=>{let h=2166136261;for(const c of String(value)){h^=c.charCodeAt(0);h=Math.imul(h,16777619);}return h>>>0;};
const point=p=>({east:Number.isFinite(p?.east)?Math.max(-1e7,Math.min(1e7,p.east)):0,north:Number.isFinite(p?.north)?Math.max(-1e7,Math.min(1e7,p.north)):0});
function state(s){
 const old=s.meta?.fruitBaskets;if(old===undefined)return{version:1,regions:Object.fromEntries(LANDS.map(row=>[row.id,{serial:0,discoveries:0,active:null,last:null}]))};
 if(!record(old)||old.version!==1||!record(old.regions))throw Error('This fruit basket checkpoint is invalid.');
 const regions={};
 for(const land of LANDS){const row=old.regions[land.id]??{serial:0,discoveries:0,active:null,last:null};
  if(!record(row)||!count(row.serial)||!count(row.discoveries)||row.discoveries>12)throw Error('This fruit basket discovery checkpoint is invalid.');
  const active=row.active;
  if(active&&(!record(active)||active.id!==`fruit-basket:${land.id}:${row.serial}`||!row.serial||!Number.isInteger(active.fruit)||active.fruit<3||active.fruit>5||!Number.isFinite(active.meters)||active.meters<0||active.meters>GOAL+1e-7||!record(active.position)||!Number.isFinite(active.position.east)||!Number.isFinite(active.position.north)||Math.abs(active.position.east)>1e7||Math.abs(active.position.north)>1e7))throw Error('This active fruit basket is invalid.');
  const last=row.last;
  if(last&&(!record(last)||!new RegExp(`^fruit-basket:${land.id}:[1-9]\\d*$`).test(last.id)||!Number.isInteger(last.fruit)||last.fruit<3||last.fruit>5))throw Error('This fruit basket receipt is invalid.');
  regions[land.id]={serial:row.serial,discoveries:row.discoveries,active:active?copy(active):null,last:last?copy(last):null};
 }
 return{version:1,regions};
}
export function normalizeFruitBaskets(s){s.meta??={};return s.meta.fruitBaskets=state(s);}
function eligible(landId,habitat){const kind=habitat?.kind||habitat?.id;return kind==='grove'||forest.has(landId)&&kind==='trail';}
export function recordFruitBasketDiscovery(s,habitat={}){
 const landId=s.world?.activeLand;if(s.run?.phase!=='trail'||!ids.has(landId)||!eligible(landId,habitat))return null;
 const ledger=state(s),region=ledger.regions[landId];if(region.active||region.serial>=MAX)return null;
 const target=6+hash(`${landId}:${region.serial+1}:basket-gap`)%7;
 region.discoveries++;let basket=null;
 if(region.discoveries>=target){region.discoveries=0;region.serial++;basket={id:`fruit-basket:${landId}:${region.serial}`,fruit:3+hash(`${landId}:${region.serial}:basket-fruit`)%3,meters:0,position:point(habitat.position||s.world.regions?.[landId]?.position)};region.active=basket;}
 s.meta??={};s.meta.fruitBaskets=ledger;return basket?{...copy(basket),landId}:null;
}
export function advanceFruitBaskets(s,meters){
 const landId=s.world?.activeLand;if(s.run?.phase!=='trail'||!ids.has(landId)||!Number.isFinite(meters)||meters<=0)return 0;
 const ledger=state(s),basket=ledger.regions[landId].active;if(!basket)return 0;
 const previous=basket.meters;basket.meters=Math.min(GOAL,previous+meters);s.meta.fruitBaskets=ledger;return basket.meters-previous;
}
export function fruitBasketView(s,landId=s.world?.activeLand){
 if(!ids.has(landId))return null;const active=state(s).regions[landId].active;if(!active)return null;
 const remainingMeters=Math.max(0,GOAL-active.meters),remainingFeet=Math.max(0,Math.ceil(remainingMeters/FOOT-1e-6)),ready=remainingMeters<=1e-7;
 return{...copy(active),landId,goalFeet:FRUIT_BASKET_FEET,progressFeet:Math.min(FRUIT_BASKET_FEET,active.meters/FOOT),remainingFeet,ready,canClaim:ready&&s.world?.activeLand===landId&&['trail','camp'].includes(s.run?.phase)&&s.run.party?.some(id=>s.collection?.[id]?.hp>0)};
}
export function claimFruitBasket(s,id){
 if(typeof id!=='string')throw Error('Choose an active fruit basket.');const ledger=state(s),entry=Object.entries(ledger.regions).find(([,row])=>row.active?.id===id||row.last?.id===id);if(!entry)throw Error('This fruit basket is no longer available.');const[landId,region]=entry;
 if(region.last?.id===id)return copy(region.last);
 const view=fruitBasketView(s,landId);if(!view?.canClaim)throw Error('Walk 500 accepted feet after finding this basket, then collect it in its region between encounters.');
 if(!count(s.run.fruit)||s.run.fruit+view.fruit>MAX)throw Error('Your fruit inventory cannot hold this basket.');
 const next=copy(s),receipt={id,fruit:view.fruit,kind:'fruit-basket',landId};next.run.fruit+=view.fruit;region.active=null;region.discoveries=0;region.last=receipt;next.meta.fruitBaskets=ledger;
 if(next.run.activeId&&next.collection[next.run.activeId])next.run.monkey=next.collection[next.run.activeId];if(JSON.stringify(next).length>650000)throw Error('Your save is too large to collect this basket.');
 for(const key of Object.keys(s))delete s[key];Object.assign(s,next);return copy(receipt);
}
