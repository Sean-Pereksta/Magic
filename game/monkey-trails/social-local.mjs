import {team,xpMultiplier,ITEMS} from './core.mjs';
const integer=n=>Number.isSafeInteger(n)&&n>=0;
export function localSocial(state){
  state.social??={};state.social.applied??={};state.social.deposits??={};return state.social;
}
function inventory(state,asset){
  if(asset==='fruit')return [state.run,'fruit'];
  if(!ITEMS[asset])throw Error('Choose fruit or a trail supply.');return[state.run.items,asset];
}
export function availableInPack(state,asset){const [bag,key]=inventory(state,asset);return bag[key]||0;}
export async function depositFromPack({state,store,uid,saveId,asset,amount,persist,id=()=>crypto.randomUUID()}){
  if(!Number.isSafeInteger(amount)||amount<1)throw Error('Enter a whole amount of at least one.');
  if(!['trail','camp'].includes(state.run.phase))throw Error('Finish the expedition encounter before moving supplies.');
  const [bag,key]=inventory(state,asset);if((bag[key]||0)<amount)throw Error('There are not enough supplies in your expedition pack.');
  const local=localSocial(state),op=id();
  bag[key]-=amount;local.deposits[op]={asset,amount,uid,saveId,runId:state.run.id};
  try{await persist();}catch(error){bag[key]+=amount;delete local.deposits[op];throw error;}
  // Retain the saved debit until an idempotent remote deposit succeeds. A timeout
  // may have committed remotely; refunding blindly here would duplicate supplies.
  await store.deposit(asset,amount,op);delete local.deposits[op];await persist();return op;
}
export async function retryPackDeposits({state,store,uid,saveId,persist}){
  for(const [op,pending]of Object.entries(localSocial(state).deposits)){
    if(pending.uid!==uid||pending.saveId!==saveId)continue;
    await store.deposit(pending.asset,pending.amount,op);delete state.social.deposits[op];await persist();
  }
}
export function applySocialReceipt(state,receipt){
  if(!receipt||typeof receipt.id!=='string'||!receipt.id||receipt.id.length>160||typeof receipt.uid!=='string'||!receipt.uid||receipt.uid.length>160)throw Error('Invalid social receipt identity.');
  const p=receipt.payload||{};
  if(typeof p!=='object'||Array.isArray(p)||!integer(p.fruit??0)||!integer(p.xp??0)||p.items!==undefined&&(!p.items||typeof p.items!=='object'||Array.isArray(p.items))||Object.entries(p.items||{}).some(([id,n])=>!ITEMS[id]||!integer(n))||p.teamIds!==undefined&&(!Array.isArray(p.teamIds)||p.teamIds.length>3||p.teamIds.some(id=>typeof id!=='string'||!id||id.length>160)))throw Error('Invalid social reward.');
  const local=localSocial(state),key=`${receipt.uid}:${receipt.id}`;
  if(local.applied[key])return false;
  if(!receipt.walletCredited){
    state.run.fruit+=p.fruit||0;
    for(const [id,n]of Object.entries(p.items||{}))state.run.items[id]=(state.run.items[id]||0)+n;
  }
  const ids=[...new Set(p.teamIds||team(state).map(m=>m.id))],members=ids.map(id=>state.collection[id]).filter(Boolean);
  let remaining=p.xp||0;
  members.forEach((m,i)=>{const base=i===members.length-1?remaining:Math.floor((p.xp||0)/members.length);remaining-=base;const total=(m.xpRemainder||0)+base*xpMultiplier(m),earned=Math.floor(total+1e-9);m.training+=earned;m.xpRemainder=Math.max(0,total-earned);});
  local.applied[key]=true;return true;
}
