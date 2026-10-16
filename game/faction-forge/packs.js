/** Fixed previews and request-seeded random rewards share the same purchase receipts. */
const pack=(id,name,faction,dust,keys)=>Object.freeze({id,name,faction,dust,cards:Object.freeze(keys.map(troopKey=>Object.freeze({troopKey,quantity:5}))),totalCards:keys.length*5});
export const FORGE_PACKS=Object.freeze([
  pack('vanir','Vanir Reinforcements','Vanir',50,['rifleman','vguard','medic']),
  pack('aesir','Aesir Reinforcements','Aesir',50,['phaser','blade','guardian']),
  pack('helions','Helion Reinforcements','Helions',50,['hellspawn','roach','defiler']),
  pack('war-chest','War Chest',null,90,['vrocket','marksman','storm','wardmaker','spitter','viper']),
  Object.freeze({id:'random-reinforcements',name:'Random Reinforcements',faction:null,dust:50,cards:Object.freeze([]),totalCards:15,random:true,draws:3,cardsPerDraw:5})
]);
export const ROUND_WIN_CARDS=3;
export const ROUND_WIN_DAILY_CAP=70;
export const WIN_REWARDS=Object.freeze({campaign:Object.freeze({cards:6,dust:20,cap:30}),boss:Object.freeze({cards:12,dust:40,cap:30}),replay:Object.freeze({cards:2,dust:5,cap:5}),bot:Object.freeze({cards:4,dust:10,cap:10}),endless:Object.freeze({cards:4,dust:10,cap:10}),pvp:Object.freeze({cards:8,dust:20,cap:10})});
export const getForgePack=id=>FORGE_PACKS.find(item=>item.id===id)||null;
export const randomPackPool=profile=>Object.keys(profile?.troops||{}).filter(key=>profile.troops[key]?.unlocked).sort();
export function forgePackCards(profile,packId,requestId){
  const item=getForgePack(packId);if(!item)return [];
  if(!item.random)return item.cards.map(card=>({...card}));
  const pool=randomPackPool(profile);
  if(!pool.length||typeof requestId!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(requestId))return [];
  let seed=2166136261;
  for(const char of `${profile.username}|${item.id}|${requestId}`)seed=Math.imul(seed^char.charCodeAt(0),16777619)>>>0;
  // Shuffle a stable pool; Auth UID changes and transaction retries do not reroll.
  const next=()=>{seed=(seed+0x6D2B79F5)>>>0;let t=seed;t=Math.imul(t^(t>>>15),t|1);t^=t+Math.imul(t^(t>>>7),t|61);return ((t^(t>>>14))>>>0)/4294967296;};
  for(let i=pool.length-1;i>0;i--){const j=Math.floor(next()*(i+1));[pool[i],pool[j]]=[pool[j],pool[i]];}
  return Array.from({length:item.draws},(_,index)=>({troopKey:pool[index%pool.length],quantity:item.cardsPerDraw}));
}
export function packEligibility(profile,packId,{active=false}={}){
  const item=getForgePack(packId);
  if(!item)return {available:false,reason:'Choose a card pack.'};
  if(active||profile?.activeSessionId)return {available:false,reason:'Finish your current match before buying packs.',pack:item};
  if(!profile?.uid)return {available:false,reason:'Sign in to buy card packs.',pack:item};
  if(item.random?!randomPackPool(profile).length:item.cards.some(card=>!profile.troops?.[card.troopKey]?.unlocked))return {available:false,reason:item.random?'Unlock a troop before buying this pack.':'Unlock the troops in this pack first.',pack:item};
  if(!Number.isSafeInteger(profile.forgeDust)||profile.forgeDust<item.dust)return {available:false,reason:`Requires ${item.dust} Forge Dust.`,pack:item};
  return {available:true,reason:'Ready to buy',pack:item};
}
