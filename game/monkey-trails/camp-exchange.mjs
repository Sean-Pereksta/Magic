// Fixed, one-way resource exchange. No new currency and no RNG are involved.
import {ELEMENTS,ITEMS} from './data.mjs';

const BATCH_LIMIT=25;
const recipes=[
  {id:'fruit',name:'Trail fruit',amount:1,fruit:10,campFruit:8,items:{}},
  {id:'berry',name:'Healing Berries',amount:1,fruit:0,items:{berry:3}},
  {id:'tonic',name:'Spark Tonics',amount:1,fruit:0,items:{tonic:2}},
  {id:'salve',name:'Cleanse Balms',amount:1,fruit:0,items:{salve:2}},
  {id:'charmMaterial',name:'Charm materials',amount:1,fruit:0,items:{charmMaterial:3}},
  {id:'revive',name:'Revival Seed',amount:2,fruit:0,items:{revive:1}}
];
const count=n=>Number.isSafeInteger(n)&&n>=0;
const availableBalance=n=>Number.isFinite(n)&&n>=0?Math.floor(n):0;
const phaseAllowed=s=>['trail','camp'].includes(s.run?.phase);

export function exchangeOffers(s){
  return recipes.map(recipe=>{
    const discounted=s.run?.phase==='camp'&&recipe.campFruit!==undefined;
    const cost={fruit:discounted?recipe.campFruit:recipe.fruit,items:{...recipe.items}};
    const costSummary=[...(cost.fruit?[{id:'fruit',name:'Trail fruit',count:cost.fruit,owned:availableBalance(s.run?.fruit)}]:[]),
      ...Object.entries(cost.items).map(([id,n])=>({id,name:id==='charmMaterial'?'Charm materials':ITEMS[id].name,count:n,owned:availableBalance(s.run?.items?.[id])}))];
    const available=Math.min(BATCH_LIMIT,...costSummary.map(row=>Math.floor(row.owned/row.count)));
    const reason=!phaseAllowed(s)?'Trade on the trail or at camp, between encounters.':available<1?'Gather the resources shown for this exchange.':'';
    return{id:recipe.id,name:recipe.name,amount:recipe.amount,cost,costSummary,available,canBuy:!reason,reason,discounted,maximumQuantity:BATCH_LIMIT};
  });
}

export function buyEssence(s,{offerId,element,quantity=1}={}){
  if(!phaseAllowed(s))throw Error('Trade on the trail or at camp, between encounters.');
  if(!Object.hasOwn(ELEMENTS,element))throw Error('Choose an available essence element.');
  if(!Number.isSafeInteger(quantity)||quantity<1||quantity>BATCH_LIMIT)throw Error(`Choose one to ${BATCH_LIMIT} exchange batches.`);
  const offer=exchangeOffers(s).find(row=>row.id===offerId);
  if(!offer)throw Error('Choose an available resource exchange.');
  const current=s.run.essence?.[element];
  if(!count(current)||current>Number.MAX_SAFE_INTEGER-offer.amount*quantity)throw Error('This essence balance cannot receive that exchange.');
  const costSummary=offer.costSummary.map(row=>({...row,count:row.count*quantity}));
  // Validate all balances and requirements before the first debit.
  for(const row of costSummary){
    const balance=row.id==='fruit'?s.run.fruit:s.run.items?.[row.id];
    if(!Number.isFinite(balance)||balance<row.count)throw Error(`Gather ${row.count} ${row.name.toLowerCase()} for this exchange.`);
  }
  for(const row of costSummary){if(row.id==='fruit')s.run.fruit-=row.count;else s.run.items[row.id]-=row.count;}
  const amount=offer.amount*quantity;s.run.essence[element]+=amount;
  return{offerId,element,quantity,amount,costSummary,discounted:offer.discounted};
}
