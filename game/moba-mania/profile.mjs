import { HEROES } from './data.mjs';
import { ITEMS, ITEM_BY_ID, purchaseState, itemBonus, killRewardBonus, ITEM_BONUS_CAPS } from './items.mjs';

export const PROFILE_KEY = 'moba-mania.profile.v1';
export const SAVE_KEY = 'moba-mania.match.v1';
export const COSMETICS = Object.freeze([
  {id:'classic',name:'Classic',xp:0,armorTint:null,trail:null,portrait:'classic',victory:'classic'},
  {id:'verdant',name:'Verdant',xp:120,armorTint:'#86c99c',trail:'#9be8b7',portrait:'verdant',victory:'verdant'},
  {id:'astral',name:'Astral',xp:300,armorTint:'#b49ce8',trail:'#b9d8ff',portrait:'astral',victory:'astral'},
  {id:'sovereign',name:'Sovereign',xp:700,armorTint:'#ead28f',trail:'#ffe2a1',portrait:'sovereign',victory:'sovereign'},
]);
const finite = (value, fallback=0, max=1e9) => Number.isFinite(value) ? Math.max(0,Math.min(max,value)) : fallback;
export function normalizeProfile(value={}) {
  const profile={schema:1,heroes:{},completed:[]};
  for(const id of Object.keys(HEROES)) {
    const old=value?.heroes?.[id]||{},xp=Math.floor(finite(old.xp));
    const equipped=COSMETICS.find(c=>c.id===old.equipped&&c.xp<=xp)?.id||'classic';
    profile.heroes[id]={xp,matches:Math.floor(finite(old.matches)),wins:Math.floor(finite(old.wins)),equipped};
  }
  profile.completed=Array.isArray(value?.completed)?[...new Set(value.completed.filter(id=>typeof id==='string'&&id.length<=160))].slice(-100):[];
  return profile;
}
export function readProfile(storage) {
  try{return normalizeProfile(JSON.parse(storage.getItem(PROFILE_KEY)||'{}'));}catch{return normalizeProfile();}
}
export function writeProfile(storage,profile) {
  try{storage.setItem(PROFILE_KEY,JSON.stringify(normalizeProfile(profile)));return true;}catch{return false;}
}
export function heroCosmetics(profile,heroId) {
  const entry=profile?.heroes?.[heroId],variant=COSMETICS.find(c=>c.id===entry?.equipped&&c.xp<=finite(entry?.xp))||COSMETICS[0];
  return {armorTint:variant.armorTint,trail:variant.trail,portrait:variant.portrait,victory:variant.victory};
}
export function equipCosmetic(profile,heroId,id) {
  const entry=profile?.heroes?.[heroId],variant=COSMETICS.find(c=>c.id===id);
  if(!entry||!variant||entry.xp<variant.xp)return false;
  entry.equipped=id;return true;
}
export function recordMatch(profile,game) {
  const hero=game?.player,entry=profile?.heroes?.[hero?.heroId],id=game?.matchId;
  if(!entry||typeof id!=='string'||!id||game.winner==null||profile.completed.includes(id))return {earned:0,unlocks:[]};
  const won=game.winner===hero.team;
  const earned=40+(won?20:0)+Math.min(40,Math.floor(finite(game.time)/60)*2);
  const before=entry.xp;entry.xp+=earned;entry.matches++;if(won)entry.wins++;
  profile.completed.push(id);profile.completed=profile.completed.slice(-100);
  return {earned,unlocks:COSMETICS.filter(c=>c.xp>before&&c.xp<=entry.xp).map(c=>c.name)};
}

/** One local save slot. Invalid data remains recoverable until the player replaces it. */
export function readSavedMatch(storage,restore) {
  if(!storage)return {status:'unavailable'};
  try {
    const raw=storage.getItem(SAVE_KEY);if(!raw)return {status:'empty'};
    if(raw.length>4_000_000)throw new Error('The saved match is too large.');
    const record=JSON.parse(raw);
    if(record?.schema!==1||!HEROES[record.heroId]||!record.snapshot||typeof record.matchId!=='string')throw new Error('This save uses an unsupported format.');
    const game=restore(record.snapshot);
    if(game.player.heroId!==record.heroId||game.matchId!==record.matchId||game.winner!=null)throw new Error('This saved match cannot be resumed.');
    game.paused=true;
    return {status:'ready',record,game};
  } catch(error) {return {status:'invalid',message:error?.message||'The saved match could not be read.'};}
}
export function saveMatch(storage,game,ui={},replace=false) {
  if(!game?.matchId||game.winner!=null)return {ok:false,reason:'There is no unfinished match to save.'};
  try {
    const previous=storage.getItem(SAVE_KEY);
    if(previous&&!replace) {
      let old;try{old=JSON.parse(previous);}catch{return {ok:false,reason:'A different saved match needs your confirmation before replacement.'};}
      if(old.matchId!==game.matchId)return {ok:false,reason:'Another match was saved in this browser. Resume it or confirm starting a new match.'};
    }
    const record={schema:1,savedAt:Date.now(),matchId:game.matchId,heroId:game.player.heroId,snapshot:game.serialize(),ui:{pinnedGoal:ITEM_BY_ID[ui.pinnedGoal]?.tier===3?ui.pinnedGoal:null}};
    const encoded=JSON.stringify(record);if(encoded.length>4_000_000)throw new Error('Save is too large');
    storage.setItem(SAVE_KEY,encoded);return {ok:true,record};
  } catch{return {ok:false,reason:'This browser could not save the match. Keep it open to continue.'};}
}
export function clearSavedMatch(storage,matchId=null) {
  try {
    if(matchId){const old=JSON.parse(storage.getItem(SAVE_KEY)||'null');if(old?.matchId!==matchId)return false;}
    storage.removeItem(SAVE_KEY);return true;
  }catch{return false;}
}

/** Dependency-first plan, with shared prerequisites counted once. */
export function itemPlan(hero,goalId) {
  const goal=ITEM_BY_ID[goalId];if(!goal)return {goal:null,remaining:[],cost:0,next:null,affordable:false};
  const owned=new Set(hero?.items||[]),visited=new Set(),remaining=[];
  function visit(id){if(visited.has(id)||owned.has(id))return;visited.add(id);const item=ITEM_BY_ID[id];if(!item)return;item.requires.forEach(visit);remaining.push(item);}
  visit(goalId);
  const next=remaining.find(item=>item.requires.every(id=>owned.has(id)))||null;
  return {goal,remaining,cost:remaining.reduce((sum,item)=>sum+item.cost,0),next,affordable:!!next&&purchaseState(hero,next.id).ok};
}
export function recommendedItems(hero) {
  const weights={bulwark:{hp:8,shield:7,healing:2},dragon:{hp:5,damage:6,lifesteal:8,burnPower:10},ember:{damage:9,speed:6,lifesteal:7,killGold:3},ranger:{attackSpeed:9,damage:7,speed:3,poisonPower:9,killXp:3},druid:{summonPower:10,summonHaste:8,healing:5},angel:{healing:10,shield:8,cooldown:7},seraph:{damage:8,attackSpeed:7,cooldown:4,shield:3,speed:4,slowPower:5},arc:{spellDamage:10,cooldown:8,mana:2,slowPower:4},rivet:{summonPower:12,summonHaste:9,cooldown:7,shield:3,hp:2},brinehook:{hp:8,shield:9,cooldown:6},vela:{attackSpeed:10,damage:7,lifesteal:10,speed:5,killGold:10},solstice:{shield:10,healing:8,cooldown:10,mana:2,slowPower:4},ferrite:{hp:9,shield:8,cooldown:6,spellDamage:3,slowPower:4},cadence:{spellDamage:8,cooldown:8,shield:9,slowPower:3,killXp:6}}[hero?.heroId]||{};
  return ITEMS.filter(item=>item.tier===3).sort((a,b)=>{
    const score=item=>Object.entries(weights).reduce((n,[key,weight])=>n+(item.bonuses[key]||0)*weight,0);
    return score(b)-score(a)||a.cost-b.cost;
  }).slice(0,3);
}
/** Uses the same stat and cooldown functions as combat without mutating the hero. */
export function previewItem(game,hero,itemId) {
  if(!ITEM_BY_ID[itemId])return null;
  const after={...hero,items:[...new Set([...(hero.items||[]),itemId])],talents:{...hero.talents}};
  game.refreshStats(after);
  const snapshot=h=>({hp:h.maxHp,mana:h.maxMana,damage:h.damage,speed:h.speed,attackInterval:h.attackRate,
    spellMultiplier:(1+game.bonus(h,'damage'))*(1+game.bonus(h,'spellDamage')),
    cooldownCut:Math.min(.5,game.bonus(h,'cooldown')),rawCooldown:game.bonus(h,'cooldown'),
    killXp:killRewardBonus(h,'xp'),killGold:killRewardBonus(h,'gold'),
    burnPower:Math.min(ITEM_BONUS_CAPS.burnPower,itemBonus(h,'burnPower')),poisonPower:Math.min(ITEM_BONUS_CAPS.poisonPower,itemBonus(h,'poisonPower')),
    slowPower:Math.min(ITEM_BONUS_CAPS.slowPower,itemBonus(h,'slowPower')),
    burnDurationPct:Math.min(ITEM_BONUS_CAPS.burnDurationPct,itemBonus(h,'burnDurationPct')),poisonDurationPct:Math.min(ITEM_BONUS_CAPS.poisonDurationPct,itemBonus(h,'poisonDurationPct')),slowDurationPct:Math.min(ITEM_BONUS_CAPS.slowDurationPct,itemBonus(h,'slowDurationPct')),
    abilities:HEROES[h.heroId].abilities.map((a,index)=>({name:a.name,cooldown:game.getAbility(h,index).cooldown}))});
  return {before:snapshot(hero),after:snapshot(after),capped:game.bonus(after,'cooldown')>.5};
}
export function normalizeControls(value={}) {
  const clamp=(n,low,high,fallback)=>Number.isFinite(Number(n))?Math.max(low,Math.min(high,Number(n))):fallback;
  return {scale:clamp(value.scale,.8,1.25,1),height:clamp(value.height,0,120,0),inset:clamp(value.inset,0,48,0),handed:value.handed==='left'?'left':'right'};
}
export function matchBadges(hero) {
  const m=hero.metrics||{},badges=[];
  if((m.beastsCaptured||0)+(m.towersDestroyed||0)>0)badges.push('Objective keeper');
  if((m.healingDone||0)+(m.shieldingDone||0)>=500)badges.push('Team guardian');
  if((m.bestStreak||0)>=3)badges.push('On a streak');
  if((m.lastHits||0)>=25)badges.push('Precise finisher');
  if((hero.deaths||0)===0)badges.push('Unbroken');
  if((m.shutdowns||0)>0)badges.push('Giant slayer');
  return badges.length?badges:['Rift explorer'];
}
