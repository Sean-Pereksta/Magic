// Saved event-driven equipment state shared by expedition, duel and raid combat.
// Hooks mutate only HP/shields and this JSON state; triggered effects never re-enter
// a damage/healing hook, so reflection, healing and revival cannot form chains.
import {GEAR} from './data.mjs';
import {equipmentEffects,equipmentTier,equipmentTierScale,equipmentPercentBonuses,applyEquipmentRecovery} from './equipment-effects.mjs';

const alive=p=>!!p&&p.hp>0;
const positive=n=>Number.isFinite(n)?Math.max(0,n):0;
const types=move=>move.types?.length?move.types:[move.type||'neutral'];
const hasType=(move,type)=>types(move).includes(type);
const blank=()=>({gear:{},maximum:0,turns:0,used:{},buffs:{},equipmentShield:0,furnaceStreak:0,dealtThisTurn:false});
const safeGear=gear=>Object.fromEntries(Object.entries(gear||{}).filter(([id,tier])=>GEAR[id]&&equipmentTier(tier)).map(([id,tier])=>[id,equipmentTier(tier)]));
const state=b=>b?.equipmentBattle;
function unit(b,p){const data=state(b);if(!data||!p)return null;return data.units[p.equipmentBattleKey]||Object.values(data.units).find(u=>u.id===p.id)||null;}
function effect(b,p,key){let sum=0;for(const[id,tier]of Object.entries(unit(b,p)?.gear||{})){const value=GEAR[id].effects?.[key];if(typeof value==='number')sum+=value*equipmentTierScale(tier);else if(value===true)sum=1;}return sum;}
const used=(b,p,key)=>!!unit(b,p)?.used[key];
function claim(b,p,key){const u=unit(b,p);if(!u||u.used[key])return false;u.used[key]=true;return true;}
function maximum(b,p){return unit(b,p)?.maximum||positive(p?.maxHp||p?.combatStats?.hp);}
function heal(b,p,amount){if(!alive(p))return 0;const n=Math.min(Math.max(0,maximum(b,p)-p.hp),Math.max(0,Math.round(amount)));p.hp+=n;return n;}
function buff(b,p,key,stats,duration){const u=unit(b,p);if(u)u.buffs[key]={...stats,remaining:duration};}
function addShield(b,p,fraction){
 if(!alive(p))return 0;const u=unit(b,p);if(!u)return 0;
 u.equipmentShield=Math.min(positive(u.equipmentShield),positive(p.shield));
 const amount=Math.max(0,Math.min(Math.round(maximum(b,p)*fraction),Math.floor(maximum(b,p)*.30)-u.equipmentShield));
 p.shield=positive(p.shield)+amount;u.equipmentShield+=amount;return amount;
}
function group(b,p){const data=state(b),u=unit(b,p);return data?.groups[u?.group];}
function teamState(b,p){const data=state(b),u=unit(b,p);return data?.teamUsed[u?.group];}

export function lockEquipmentBattle(b,teams,gearFor=p=>p.gear||{},statsFor=p=>p.combatStats||{hp:p.maxHp}){
 if(b.equipmentBattle)return b.equipmentBattle;
 const data=b.equipmentBattle={version:1,units:{},groups:[],teamUsed:[],knockoutSeen:{}};
 for(const[m,members]of teams.entries()){
  data.groups[m]=[];data.teamUsed[m]={};
  for(const p of members){const key=`${m}:${p.id}`;p.equipmentBattleKey=key;const gear=safeGear(gearFor(p));data.units[key]={...blank(),id:p.id,group:m,gear,maximum:positive(statsFor(p,gear)?.hp||p.maxHp)};data.groups[m].push(key);}
 }
 return data;
}
export function equipmentBattleGear(b,p){return unit(b,p)?.gear||p?.gear||{};}
export function equipmentApplyRecovery(b,p,stats,gear=equipmentBattleGear(b,p)){
 const u=unit(b,p),before=positive(p?.shield),result=applyEquipmentRecovery(p,stats,gear);if(!u)return result;
 u.equipmentShield=Math.min(positive(u.equipmentShield),before);
 const added=Math.max(0,positive(p.shield)-before),accepted=Math.min(added,Math.max(0,Math.floor(maximum(b,p)*.30)-u.equipmentShield));
 p.shield-=added-accepted;u.equipmentShield+=accepted;result.shield=accepted;return result;
}
export function equipmentBattleStats(b,p,baseStats,team=[]){
 const u=unit(b,p);if(!u)return baseStats;
 const opts={p,team,hpMaximum:maximum(b,p)},staticPct=equipmentPercentBonuses(u.gear),allPct=equipmentPercentBonuses(u.gear,{...opts,includeConditional:true}),out={...baseStats};
 for(const entry of Object.values(u.buffs))for(const key of ['atk','def','speed'])if(entry.remaining>0)allPct[key]+=entry[key]||0;
 for(const key of ['atk','def','speed'])out[key]=Math.max(1,Math.round(baseStats[key]*(1+allPct[key])/(1+staticPct[key])));
 return out;
}
export function equipmentEnter(b,p,team=[],{reason='initial'}={}){
 const u=unit(b,p),result={healing:0,shield:0};if(!u||!alive(p))return result;
 if(effect(b,p,'entrySpeed')&&claim(b,p,'entrySpeed'))buff(b,p,'entrySpeed',{speed:effect(b,p,'entrySpeed')},3);
 if(effect(b,p,'entryShield')&&claim(b,p,'entryShield'))result.shield+=addShield(b,p,(p.types||[]).includes('ice')?effect(b,p,'matchingEntryShield'):effect(b,p,'entryShield'));
 if(reason==='voluntary'){
  if(effect(b,p,'switchInHeal')&&claim(b,p,'switchInHeal'))result.healing+=heal(b,p,maximum(b,p)*effect(b,p,'switchInHeal'));
  if(effect(b,p,'switchInAttack')&&claim(b,p,'switchInAttack'))u.eclipseReady=true;
  if(effect(b,p,'switchInGuard')&&claim(b,p,'switchInGuard'))u.nextReduction=Math.min(.30,(u.nextReduction||0)+effect(b,p,'switchInGuard'));
 }
 if(reason==='knockout'&&effect(b,p,'replacementSpeed')&&claim(b,p,'replacementSpeed')){
  buff(b,p,'replacementSpeed',{speed:effect(b,p,'replacementSpeed')},2);u.nextDamage=(u.nextDamage||0)+effect(b,p,'replacementDamage');
 }
 const pending=teamState(b,p)?.replacement;
 if(reason==='knockout'&&pending){result.shield+=addShield(b,p,pending.shield);buff(b,p,'trailwardDamage',{damage:pending.damage},2);delete teamState(b,p).replacement;}
 return result;
}
export function equipmentSwitch(b,outgoing,incoming,team=[],{voluntary=true}={}){
 const u=unit(b,outgoing),result={healing:0,shield:0};if(u){u.furnaceStreak=0;u.dealtThisTurn=false;u.furnaceReady=false;}
 if(voluntary&&alive(outgoing)){
  const value=key=>effect(b,outgoing,key),once=key=>value(key)&&claim(b,outgoing,key);
  if(once('switchOutSpeed'))buff(b,incoming,'relaySpeed',{speed:value('switchOutSpeed')},2);
  if(once('switchOutHeal'))result.healing+=heal(b,incoming,maximum(b,incoming)*value('switchOutHeal'));
  if(once('switchOutShield'))result.shield+=addShield(b,incoming,value('switchOutShield'));
  if(once('switchOutDamage')){const next=unit(b,incoming);if(next)next.nextDamage=(next.nextDamage||0)+value('switchOutDamage');}
  if(value('switchOutTeamHeal')&&!teamState(b,outgoing).worldroot){teamState(b,outgoing).worldroot=true;for(const p of team)result.healing+=heal(b,p,maximum(b,p)*value('switchOutTeamHeal'));}
 }
 const entry=equipmentEnter(b,incoming,team,{reason:voluntary?'voluntary':'knockout'});result.healing+=entry.healing;result.shield+=entry.shield;return result;
}

// Attack bonuses include released elemental/physical/energy bonuses, so callers
// replace their old multiplier instead of multiplying by both implementations.
export function equipmentAttackDetails(b,actor,target,move,{consume=false}={}){
 if(!move.power)return{bonus:0,rawBonus:0,multiplier:1,capped:false};
 const u=unit(b,actor),gear=equipmentBattleGear(b,actor),effects=equipmentEffects(gear),attackTypes=types(move),targetRatio=target.hp/maximum(b,target),actorRatio=actor.hp/maximum(b,actor);
 let bonus=((move.cost||0)>0?effects.energyDamage:0)+(attackTypes.every(t=>t==='neutral')?effects.physicalDamage:0)+attackTypes.reduce((n,t)=>n+(effects.elementDamage[t]||0),0)/attackTypes.length;
 const once=(key,condition=true)=>{if(condition&&effect(b,actor,key)&&!used(b,actor,key)){bonus+=effect(b,actor,key);if(consume)claim(b,actor,key);}};
 once('firstAttackDamage');
 if(move.interrupted||move.interrupt&&(target.charging||target.charge||b.enemyAction?.charging||b.enemyAction?.charged))bonus+=effect(b,actor,'interruptDamage');
 if(move.perfectTiming&&hasType(move,'storm'))bonus+=effect(b,actor,'perfectStormDamage');
 if(move.comboActive)bonus+=effect(b,actor,'comboDamage');
 if(targetRatio>.75)bonus+=effect(b,actor,'healthyTargetDamage');
 if(targetRatio<.35)bonus+=effect(b,actor,'criticalTargetDamage');
 once('firstHigherFireDamage',hasType(move,'fire')&&targetRatio>actorRatio);
 if(u?.furnaceReady){bonus+=effect(b,actor,'consecutiveDamage');if(consume){u.furnaceReady=false;u.furnaceStreak=0;u.furnaceRestart=true;}}
 if(u?.eclipseReady){bonus+=effect(b,actor,'switchInAttack');if(consume)u.eclipseReady=false;}
 if(u?.fireReady&&hasType(move,'fire')){bonus+=effect(b,actor,'effectiveNextFireDamage');if(consume)u.fireReady=false;}
 if(u?.nextDamage){bonus+=u.nextDamage;if(consume)u.nextDamage=0;}
 for(const entry of Object.values(u?.buffs||{}))if(entry.remaining>0)bonus+=entry.damage||0;
 const applied=Math.min(.40,bonus);if(consume&&u)u.lastAttackBonus=applied;
 return{bonus:applied,rawBonus:bonus,multiplier:1+applied,capped:bonus>.40};
}
export function equipmentAttackMultiplier(b,actor,target,move,options={}){return equipmentAttackDetails(b,actor,target,move,{consume:true,...options}).multiplier;}

export function equipmentApplyHit(b,actor,target,move,damage,{effectiveness=1,direct=true,enemy=true,ignoreShields=false,attackBonus}={}){
 const a=unit(b,actor),t=unit(b,target),result={damage:0,absorbed:0,reflected:0,healing:0,shield:0,shieldBroken:false,reduction:0};
 if(!alive(target)||!Number.isFinite(damage)||damage<=0)return result;
 let reduction=0;
 const incoming=equipmentEffects(equipmentBattleGear(b,target)),attackTypes=types(move);
 reduction+=attackTypes.reduce((sum,type)=>sum+(incoming.elementResist[type]||0),0)/attackTypes.length;
 if(target.hp/maximum(b,target)<.40)reduction+=effect(b,target,'woundedReduction');
 if(enemy&&direct){
  if(effect(b,target,'firstHitReduction')&&claim(b,target,'firstHitReduction'))reduction+=effect(b,target,'firstHitReduction');
  if(target.hp/maximum(b,target)>.50&&effect(b,target,'healthyFirstHitReduction')&&claim(b,target,'healthyFirstHitReduction'))reduction+=effect(b,target,'healthyFirstHitReduction');
  if(t?.nextReduction){reduction+=t.nextReduction;t.nextReduction=0;}
 }
 result.reduction=Math.min(.30,reduction);damage=Math.max(0,Math.round(damage*(1-result.reduction)*(t?.eclipseReady?1+effect(b,target,'switchInVulnerability'):1)));
 const oldShield=ignoreShields?0:positive(target.shield),normalBonus=positive(attackBonus??a?.lastAttackBonus),shieldBonus=direct?Math.max(0,Math.min(.40-normalBonus,effect(b,actor,'shieldDamage')))/(1+normalBonus):0;
 result.absorbed=Math.min(oldShield,Math.round(damage*(1+shieldBonus)));
 if(!ignoreShields){target.shield=oldShield-result.absorbed;if(t)t.equipmentShield=Math.max(0,Math.min(t.equipmentShield,oldShield)-result.absorbed);}
 if(enemy&&direct&&(result.absorbed>0||target.guard)&&effect(b,target,'blockReflect')&&alive(actor)){
  const token=`${actor.equipmentBattleKey||actor.id}:${a?.turns||0}`;if(t.blockToken!==token){t.blockToken=token;t.blockReflected=0;}
  const reflected=Math.min(actor.hp,Math.max(0,Math.min(Math.round((result.absorbed||damage)*effect(b,target,'blockReflect')),Math.floor(maximum(b,target)*.03)-t.blockReflected)));
  actor.hp-=reflected;t.blockReflected+=reflected;result.reflected+=reflected;
 }
 const healthHit=Math.max(0,Math.round(damage-result.absorbed/(1+shieldBonus))),beforeHp=target.hp;
 let actual=Math.min(beforeHp,healthHit);
 if(enemy&&direct&&healthHit>=beforeHp&&effect(b,target,'surviveFatal')&&claim(b,target,'surviveFatal'))actual=Math.max(0,beforeHp-1);
 target.hp=Math.max(0,beforeHp-actual);result.damage=actual;result.shieldBroken=oldShield>0&&target.shield===0&&result.absorbed>0;
 const actualEnemyDamage=enemy&&(actual>0||result.absorbed>0);
 if(a&&direct&&(actual>0||result.absorbed>0)){a.dealtThisTurn=true;
  if(effect(b,actor,'firstIceShield')&&hasType(move,'ice')&&claim(b,actor,'firstIceShield'))result.shield+=addShield(b,actor,effect(b,actor,'firstIceShield'));
  if(effectiveness>1&&effect(b,actor,'effectiveNextFireDamage')&&claim(b,actor,'effectiveNextFireDamage'))a.fireReady=true;
 }
 if(actualEnemyDamage&&alive(target)){
  const once=key=>effect(b,target,key)&&claim(b,target,key),ratio=target.hp/maximum(b,target);
  if(effectiveness>1){if(once('effectiveHitShield'))result.shield+=addShield(b,target,effect(b,target,'effectiveHitShield'));if(once('effectiveHitNextReduction'))t.nextReduction=effect(b,target,'effectiveHitNextReduction');}
  if(actual>0&&ratio<.35&&once('criticalHeal'))result.healing+=heal(b,target,maximum(b,target)*effect(b,target,'criticalHeal'));
  if(actual>0&&ratio<.40&&once('woundedShield'))result.shield+=addShield(b,target,effect(b,target,'woundedShield'));
  if(actual>0&&direct&&once('delayedRecovery'))t.delayedRecovery={amount:Math.min(maximum(b,target)*.15,actual*effect(b,target,'delayedRecovery')),due:t.turns+1};
  if(result.shieldBroken&&once('shieldBreakHeal')){result.healing+=heal(b,target,maximum(b,target)*effect(b,target,'shieldBreakHeal'));buff(b,target,'shieldBreakAttack',{atk:effect(b,target,'shieldBreakAttack')},2);}
 }
 // Reflection and life steal use actual health damage only; helpers never emit
 // new events. Per-enemy-turn caps use that attacker's completed-turn counter.
 if(enemy&&direct&&actual>0&&alive(actor)&&effect(b,target,'reflectDamage')){
  const token=`${actor.equipmentBattleKey||actor.id}:${a?.turns||0}`;
  if(t.reflectToken!==token){t.reflectToken=token;t.reflected=0;}
  result.reflected=Math.min(actor.hp,Math.max(0,Math.min(Math.round(actual*effect(b,target,'reflectDamage')),Math.floor(maximum(b,target)*.03)-t.reflected)));actor.hp-=result.reflected;t.reflected+=result.reflected;
  if(actor.hp<=0)a.passiveKnockout=true;
 }
 if(direct&&actual>0&&alive(actor)&&effect(b,actor,'limitedLifesteal')){
  if(a.lifestealTurn!==a.turns){a.lifestealTurn=a.turns;a.lifestealThisTurn=0;if((a.lifestealTurns||0)<5)a.lifestealTurns=(a.lifestealTurns||0)+1;else a.lifestealExhausted=true;}
  if(!a.lifestealExhausted){const amount=Math.max(0,Math.min(Math.round(actual*effect(b,actor,'limitedLifesteal')),Math.floor(maximum(b,actor)*.03)-a.lifestealThisTurn));const healed=heal(b,actor,amount);a.lifestealThisTurn+=amount;result.healing+=healed;}
 }
 return result;
}

export function equipmentHealingMultiplier(b,actor,move,{consume=true}={}){
 if(!(move.heal||move.teamHeal)||!effect(b,actor,'firstAbilityHealing')||used(b,actor,'firstAbilityHealing'))return 1;
 if(consume)claim(b,actor,'firstAbilityHealing');return 1+effect(b,actor,'firstAbilityHealing');
}
export function equipmentAfterAbility(b,actor,move,{team=[],recipients,recipient,target,moveId}={}){
 const u=unit(b,actor),result={healing:0,shield:0};if(!u||!alive(actor))return result;
 if(hasType(move,'storm')&&effect(b,actor,'stormSpeed'))buff(b,actor,'stormSpeed',{speed:effect(b,actor,'stormSpeed')},2);
 if(!move.power&&effect(b,actor,'firstSupportHeal')&&claim(b,actor,'firstSupportHeal'))result.healing+=heal(b,actor,maximum(b,actor)*effect(b,actor,'firstSupportHeal'));
 if(move.guard&&effect(b,actor,'braceShield')&&claim(b,actor,'braceShield'))result.shield+=addShield(b,actor,effect(b,actor,'braceShield'));
 if(moveId&&move.cooldown&&actor.cooldowns?.[moveId]>b.round&&effect(b,actor,'firstCooldownReduction')&&claim(b,actor,'firstCooldownReduction'))actor.cooldowns[moveId]=Math.max(b.round,actor.cooldowns[moveId]-1);
 if(target?.status?.weaken&&(move.weaken||move.status==='weaken')&&effect(b,actor,'firstWeakenTurns')&&claim(b,actor,'firstWeakenTurns'))target.status.weaken=Math.min(6,target.status.weaken+1);
 if(move.heal||move.teamHeal){
  if(effect(b,actor,'abilityHealShield')&&(u.healShieldUses||0)<2){u.healShieldUses=(u.healShieldUses||0)+1;for(const p of recipients||[recipient||actor])result.shield+=addShield(b,p,effect(b,actor,'abilityHealShield'));}
  if(effect(b,actor,'reserveHeal')&&claim(b,actor,'reserveHeal')){const reserve=team.filter(p=>p!==actor&&alive(p)).sort((x,y)=>x.hp/maximum(b,x)-y.hp/maximum(b,y))[0];if(reserve)result.healing+=heal(b,reserve,maximum(b,reserve)*effect(b,actor,'reserveHeal'));}
  if(move.teamHeal&&effect(b,actor,'firstTeamHealShield')&&claim(b,actor,'firstTeamHealShield'))for(const p of team)if(alive(p))result.shield+=addShield(b,p,effect(b,actor,'firstTeamHealShield'));
 }
 return result;
}
export function equipmentTurnEnd(b,p,team=[]){
 const u=unit(b,p),result={healing:0,shield:0};if(!u)return result;
 u.turns++;
 if(alive(p)){
  if(u.turns===3&&effect(b,p,'thirdTurnHeal')&&claim(b,p,'thirdTurnHeal'))result.healing+=heal(b,p,maximum(b,p)*effect(b,p,'thirdTurnHeal'));
  if(u.turns%3===0&&effect(b,p,'periodicHeal')&&(u.periodicHeals||0)<2){u.periodicHeals=(u.periodicHeals||0)+1;result.healing+=heal(b,p,maximum(b,p)*effect(b,p,'periodicHeal'));}
  if(u.delayedRecovery&&u.delayedRecovery.due<=u.turns){result.healing+=heal(b,p,u.delayedRecovery.amount);delete u.delayedRecovery;}
 }
 for(const[key,value]of Object.entries(u.buffs))if(--value.remaining<=0)delete u.buffs[key];
 if(u.furnaceRestart){u.furnaceStreak=0;delete u.furnaceRestart;}else if(u.dealtThisTurn){u.furnaceStreak=(u.furnaceStreak||0)+1;if(u.furnaceStreak>=2&&effect(b,p,'consecutiveDamage'))u.furnaceReady=true;}else{u.furnaceStreak=0;u.furnaceReady=false;}
 u.dealtThisTurn=false;return result;
}
export function equipmentKnockout(b,p,team=[],{active=true}={}){
 const u=unit(b,p),result={revived:false,reserveOnly:false};if(!u)return result;if(alive(p)){u.knockoutHandled=false;u.passiveKnockout=false;return result;}if(u.knockoutHandled)return result;u.knockoutHandled=true;
 if(u.passiveKnockout)return result;
 const t=teamState(b,p);
 if(effect(b,p,'knockoutShield')&&claim(b,p,'knockoutShield')&&active)t.replacement={shield:effect(b,p,'knockoutShield'),damage:effect(b,p,'knockoutDamage')};
 if(!t.beacon){const bearer=team.find(other=>other.id!==p.id&&alive(other)&&effect(b,other,'teamRevive'));
  if(bearer){t.beacon=true;p.hp=Math.max(1,Math.round(maximum(b,p)*effect(b,bearer,'teamRevive')));p.status={};p.statusPower={};p.effectUntil={};p.shield=0;u.equipmentShield=0;u.revivedToReserve=true;result.revived=true;result.reserveOnly=true;}
 }
 return result;
}
export function equipmentRevivedToReserve(b,p){const u=unit(b,p);if(!u?.revivedToReserve)return false;delete u.revivedToReserve;return true;}
export function equipmentConsumableHealing(gear={}){let value=0;for(const[id,tier]of Object.entries(safeGear(gear)))value+=positive(GEAR[id].effects?.consumableHealing)*equipmentTierScale(tier);return 1+value;}

export const EQUIPMENT_COMBAT_LIMITS=Object.freeze({damageBonus:.40,damageReduction:.30,shieldCapacity:.30});

// Preview and spend share the same discount. Merely opening a move card cannot
// consume a once-per-battle effect.
export function equipmentMoveCost(b,p,move,{consume=false}={}){
 const cost=positive(move.cost);if(!cost||!hasType(move,'shadow')||!effect(b,p,'firstShadowDiscount')||used(b,p,'firstShadowDiscount'))return cost;
 if(consume)claim(b,p,'firstShadowDiscount');return Math.max(0,cost-Math.min(1,effect(b,p,'firstShadowDiscount')));
}
export function equipmentShieldMultiplier(b,p,move,{consume=true}={}){
 if(!(move.shield||move.teamShield)||!effect(b,p,'firstShieldBoost')||used(b,p,'firstShieldBoost'))return 1;
 if(consume)claim(b,p,'firstShieldBoost');return 1+Math.min(.30,effect(b,p,'firstShieldBoost'));
}
export function equipmentTacticalEvent(b,p,event,{target,move,team=[],maxEnergy}={}){
 const u=unit(b,p),result={energy:0,cooldowns:0};if(!u||!alive(p))return result;
 if(event==='counter'){
  if(effect(b,p,'counterEnergy')&&claim(b,p,'counterEnergy')){const max=maxEnergy||p.combatStats?.maxEnergy||p.maxEnergy||24;result.energy=Math.min(Math.max(0,max-p.energy),Math.round(effect(b,p,'counterEnergy')));p.energy+=result.energy;}
  if(effect(b,p,'counterCooldown')&&claim(b,p,'counterCooldown'))for(const[id,due]of Object.entries(p.cooldowns||{}))if(due>b.round){p.cooldowns[id]=Math.max(b.round,due-1);result.cooldowns++;}
 }
 return result;
}
