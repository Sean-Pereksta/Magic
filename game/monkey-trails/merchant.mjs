// Pack barter uses the forge's material catalogue; shops never invent item values.
import {forgeInventory} from './forge.mjs';
import {specialLocation} from './living-world.mjs';
import {ITEMS,ELEMENTS} from './data.mjs';

const MAX=1e9,copy=v=>JSON.parse(JSON.stringify(v)),record=v=>v&&typeof v==='object'&&!Array.isArray(v)&&[Object.prototype,null].includes(Object.getPrototypeOf(v)),whole=n=>Number.isSafeInteger(n)&&n>=0&&n<=MAX;
const paymentKinds=new Set(['fruit','essence','item','fragment','bond','quality']);
const stockDefinitions=[
 {id:'berries',name:'Trail Berry bundle',resource:'item:berry',quantity:2,limit:3},
 {id:'revival',name:'Revival Seed',resource:'item:revive',quantity:1,limit:1},
 {id:'bonds',name:'Bond Charm pair',resource:'item:bondCharm',quantity:2,limit:2},
 {id:'essence',name:'Regional essence bundle',resource:'regional',quantity:4,limit:3},
 {id:'ingots',name:'Ingot pair',resource:'item:ingot',quantity:2,limit:2},
 {id:'star-ore',name:'Star Ore',resource:'item:starOre',quantity:1,limit:1}
];
function state(s){
 const old=s.meta?.merchants;if(old===undefined)return{version:1,credit:0,serial:0,shops:{},last:null};
 if(!record(old)||old.version!==1||!whole(old.credit)||!whole(old.serial)||!record(old.shops)||Object.keys(old.shops).length>1000||old.serial>0&&!record(old.last)||!old.serial&&old.last)throw Error('This merchant checkpoint is invalid.');
 const next=copy(old);
 for(const[id,shop]of Object.entries(next.shops))if(!/^location:[a-z0-9-]+:[1-9]\d*$/.test(id)||!record(shop)||!record(shop.purchased)||Object.entries(shop.purchased).some(([id,n])=>!stockDefinitions.some(row=>row.id===id&&whole(n)&&n<=row.limit)))throw Error('This merchant stock checkpoint is invalid.');
 if(next.last&&(next.last.operationId!==`merchant-${next.serial}`||typeof next.last.fingerprint!=='string'||!record(next.last.reward)||!whole(next.last.price)||!whole(next.last.paymentValue)||!whole(next.last.creditAfter)))throw Error('This merchant receipt is invalid.');
 return next;
}
export function normalizeMerchants(s){const value=state(s);s.meta??={};s.meta.merchants=value;return value;}
export function nextMerchantOperationId(s){const value=state(s);if(value.serial>=MAX)throw Error('This merchant ledger is full.');return`merchant-${value.serial+1}`;}
function location(s,id){
 if(!['trail','camp'].includes(s.run?.phase)||!s.run.party?.some(id=>s.collection?.[id]?.hp>0))throw Error('Visit merchants between encounters with a conscious teammate.');
 const row=specialLocation(copy(s),id);
 if(row.icon!=='merchant'||row.interaction!=='bargain'||!['visited','claimed'].includes(row.status)||s.world?.activeLand!==row.landId)throw Error('Visit this merchant in its region before trading.');
 return row;
}
export function merchantPaymentInventory(s){return forgeInventory(s).filter(row=>paymentKinds.has(row.kind)&&!row.equipmentId&&row.owned>0);}
function inventoryFor(s,row){
 if(row.id==='fruit')return[s.run,'fruit'];
 if(row.kind==='quality')return[s.run.forgeMaterials,row.id.slice(8)];
 const split=row.id.indexOf(':'),key=row.id.slice(split+1);
 return[row.kind==='essence'?s.run.essence:row.kind==='item'?s.run.items:row.kind==='fragment'?s.meta.fragments:s.meta.bondMaterials,key];
}
function normalizePayment(s,payment){
 if(!record(payment)||Object.keys(payment).length>180)throw Error('Choose whole quantities from your pack.');
 const inventory=new Map(merchantPaymentInventory(s).map(row=>[row.id,row])),normalized={},summary=[];let value=0;
 for(const[id,quantity]of Object.entries(payment).sort(([a],[b])=>a.localeCompare(b))){
  if(!whole(quantity))throw Error('Use whole, non-negative payment quantities.');if(!quantity)continue;
  const row=inventory.get(id);if(!row)throw Error('That payment material is unavailable. Equipment cannot be bartered here.');
  const[bag,key]=inventoryFor(s,row),balance=bag?.[key];
  if(!whole(balance)||balance<quantity)throw Error(`Not enough ${row.name.toLowerCase()} in your pack.`);
  const amount=quantity*row.value;if(!whole(amount)||value+amount>MAX)throw Error('This payment is too large.');
  value+=amount;normalized[id]=quantity;summary.push({id,name:row.name,quantity,unitValue:row.value,value:amount});
 }
 return{payment:normalized,paymentValue:value,paymentSummary:summary};
}
function stock(s,row,ledger){
 // Probe the catalogue with the offered items stocked; this never changes a save.
 const probe=copy(s);probe.run.items??={};probe.run.essence??={};for(const def of stockDefinitions)if(def.resource.startsWith('item:'))probe.run.items[def.resource.slice(5)]=1;probe.run.essence[row.element]=1;
 const catalogue=new Map(forgeInventory(probe).map(row=>[row.id,row]));
 return stockDefinitions.map(def=>{const resource=def.resource==='regional'?`essence:${row.element}`:def.resource,material=catalogue.get(resource);if(!material)throw Error('This merchant offer is unavailable.');const baseValue=def.quantity*material.value,price=Math.ceil(baseValue*1.25),purchased=ledger.shops[row.id]?.purchased[def.id]||0;return{...def,resource,name:def.resource==='regional'?`${ELEMENTS[row.element].name} essence bundle`:def.name,unitValue:material.value,craftValue:baseValue,price,remaining:def.limit-purchased};});
}
export function merchantView(s,id,payment={}){
 const row=location(s,id),ledger=state(s);let selected={payment:{},paymentValue:0,paymentSummary:[]},reason='';try{selected=normalizePayment(s,payment);}catch(error){reason=error.message;}
 return{locationId:id,name:row.name,credit:ledger.credit,...selected,reason,offers:stock(s,row,ledger).map(offer=>({...offer,canBuy:!reason&&offer.remaining>0&&ledger.credit+selected.paymentValue>=offer.price,creditAfter:ledger.credit+selected.paymentValue-offer.price})),inventory:merchantPaymentInventory(s)};
}
function rewardBag(s,resource){const split=resource.indexOf(':'),kind=resource.slice(0,split),id=resource.slice(split+1);if(kind==='item'&&ITEMS[id])return[s.run.items,id];if(kind==='essence'&&ELEMENTS[id])return[s.run.essence,id];throw Error('This merchant item is unavailable.');}
export function purchaseMerchant(s,id,offerId,payment,operationId){
 const ledger=state(s),fingerprint=JSON.stringify({locationId:id,offerId,payment:record(payment)?Object.fromEntries(Object.entries(payment).filter(([,n])=>n!==0).sort(([a],[b])=>a.localeCompare(b))):payment});
 if(typeof operationId!=='string'||!/^merchant-[1-9]\d{0,9}$/.test(operationId))throw Error('Choose a valid merchant operation.');
 if(ledger.last?.operationId===operationId){if(ledger.last.fingerprint!==fingerprint)throw Error('This purchase already used a different payment or item.');return copy(ledger.last);}
 if(operationId!==nextMerchantOperationId(s))throw Error('This merchant purchase is already resolved or out of sequence.');
 const view=merchantView(s,id,payment),offer=view.offers.find(row=>row.id===offerId);if(view.reason)throw Error(view.reason);if(!offer)throw Error('Choose an item from this merchant.');if(offer.remaining<=0)throw Error('This merchant has sold out of that item.');if(!offer.canBuy)throw Error('Offer more pack value or use your saved merchant credit.');
 const next=copy(s),selected=normalizePayment(next,payment),newLedger=state(next);let[bag,key]=rewardBag(next,offer.resource);if(!whole(bag?.[key]??0))throw Error('Your reward inventory is invalid.');
 for(const part of selected.paymentSummary){const row=merchantPaymentInventory(next).find(row=>row.id===part.id),[inventory,key]=inventoryFor(next,row);inventory[key]-=part.quantity;}
 [bag,key]=rewardBag(next,offer.resource);if((bag[key]??0)+offer.quantity>MAX)throw Error('There is no room for that purchase.');bag[key]=(bag[key]??0)+offer.quantity;
 const creditAfter=newLedger.credit+selected.paymentValue-offer.price;if(!whole(creditAfter))throw Error('This purchase would exceed the merchant credit limit.');
 newLedger.credit=creditAfter;
 // Resolved locations disappear from the world catalogue; their stock can never
 // be reopened, so old shop entries need not grow forever.
 if(!newLedger.shops[id])for(const oldId of Object.keys(newLedger.shops))if(!next.meta.livingWorld?.locations?.[oldId])delete newLedger.shops[oldId];
 if(!newLedger.shops[id]&&Object.keys(newLedger.shops).length>=1000)throw Error('Your merchant ledger is full. Resolve older destinations before opening another shop.');
 const shop=newLedger.shops[id]??={purchased:{}};shop.purchased[offer.id]=(shop.purchased[offer.id]||0)+1;newLedger.serial++;
 const receipt={operationId,fingerprint,locationId:id,offerId:offer.id,name:offer.name,price:offer.price,payment:selected.payment,paymentValue:selected.paymentValue,creditBefore:ledger.credit,creditAfter,reward:{resource:offer.resource,quantity:offer.quantity}};newLedger.last=copy(receipt);next.meta.merchants=newLedger;
 if(next.run.activeId&&next.collection[next.run.activeId])next.run.monkey=next.collection[next.run.activeId];
 if(JSON.stringify(next).length>650000)throw Error('Your save is too large for this purchase. Free storage before trading again.');
 for(const key of Object.keys(s))delete s[key];Object.assign(s,next);return copy(receipt);
}
