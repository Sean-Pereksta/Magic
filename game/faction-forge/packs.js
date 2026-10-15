/** Fixed card sets: the Forge shows every card before any Dust is spent. */
const pack=(id,name,faction,dust,keys)=>Object.freeze({id,name,faction,dust,cards:Object.freeze(keys.map(troopKey=>Object.freeze({troopKey,quantity:5}))),totalCards:keys.length*5});
export const FORGE_PACKS=Object.freeze([
  pack('vanir','Vanir Reinforcements','Vanir',50,['rifleman','vguard','medic']),
  pack('aesir','Aesir Reinforcements','Aesir',50,['phaser','blade','guardian']),
  pack('helions','Helion Reinforcements','Helions',50,['hellspawn','roach','defiler']),
  pack('war-chest','War Chest',null,90,['vrocket','marksman','storm','wardmaker','spitter','viper'])
]);
export const ROUND_WIN_CARDS=3;
export const ROUND_WIN_DAILY_CAP=70;
export const WIN_REWARDS=Object.freeze({campaign:Object.freeze({cards:6,dust:20,cap:30}),boss:Object.freeze({cards:12,dust:40,cap:30}),replay:Object.freeze({cards:2,dust:5,cap:5}),bot:Object.freeze({cards:4,dust:10,cap:10}),endless:Object.freeze({cards:4,dust:10,cap:10}),pvp:Object.freeze({cards:8,dust:20,cap:10})});
export const getForgePack=id=>FORGE_PACKS.find(item=>item.id===id)||null;
export function packEligibility(profile,packId,{active=false}={}){
  const item=getForgePack(packId);
  if(!item)return {available:false,reason:'Choose a card pack.'};
  if(active||profile?.activeSessionId)return {available:false,reason:'Finish your current match before buying packs.',pack:item};
  if(!profile?.uid)return {available:false,reason:'Sign in to buy card packs.',pack:item};
  if(item.cards.some(card=>!profile.troops?.[card.troopKey]?.unlocked))return {available:false,reason:'Unlock the troops in this pack first.',pack:item};
  if(!Number.isSafeInteger(profile.forgeDust)||profile.forgeDust<item.dust)return {available:false,reason:`Requires ${item.dust} Forge Dust.`,pack:item};
  return {available:true,reason:'Ready to buy',pack:item};
}
