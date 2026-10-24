// Deterministic social combat. Every fight uses disposable, healthy team copies.
// The reducers mutate only their supplied social state and return that same object.
import {SPECIES,MOVES,ELEMENTS,GEAR,CONFIG,statsForCombatVersion,equipped,team,unlocked,moveEffect,moveEffectLegacy,effectInfo,random} from './core.mjs';
import {equipmentDamageMultiplier,applyEquipmentRecovery,applyEquipmentLifesteal} from './equipment-effects.mjs';
import {lockEquipmentBattle,equipmentBattleGear,equipmentBattleStats,equipmentEnter,equipmentSwitch,equipmentAttackMultiplier,equipmentApplyHit,equipmentHealingMultiplier,equipmentAfterAbility,equipmentTurnEnd,equipmentKnockout,equipmentRevivedToReserve,equipmentApplyRecovery,equipmentMoveCost,equipmentShieldMultiplier,equipmentTacticalEvent} from './equipment-battle.mjs';

const clone=value=>structuredClone(value),alive=p=>p&&p.hp>0;
const integer=(value,min,max,label)=>{if(!Number.isInteger(value)||value<min||value>max)throw Error(`Invalid ${label}.`);return value;};
const displayName=p=>p.nickname||SPECIES[p.species]?.name||'Companion';
const log=(s,text)=>{s.log.push(text);s.log=s.log.slice(-18);};
const freshSeed=seed=>Number.isInteger(seed)?(seed>>>0)||1234567:1234567;
export const RAID_TURNS=10,DUEL_TURN_LIMIT=80,SOCIAL_COMBAT_VERSION=5;
const balanceVersion=value=>integer(value,1,SOCIAL_COMBAT_VERSION,'combat version');
export const socialMoveEffect=(move,defenders,version=SOCIAL_COMBAT_VERSION)=>version===1?moveEffectLegacy(move,defenders):moveEffect(move,defenders);

function freshMember(source,gear={},version=SOCIAL_COMBAT_VERSION){
  if(!source||!SPECIES[source.species])throw Error('Unknown social-battle companion.');
  const level=integer(source.level,1,CONFIG.maxLevel,'companion level'),stage=integer(source.stage||0,0,3,'evolution stage');
  const types=[...(source.types||[])];if(types.length>2||new Set(types).size!==types.length||types.some(t=>!ELEMENTS[t]))throw Error('Invalid companion elements.');
  const slots=new Set(),capacity=SPECIES[source.species].rarity==='ultra'||SPECIES[source.species].ultra||source.classification==='ultra'?4:3,tiers=Object.fromEntries(Object.entries(gear).filter(([id,tier])=>{if(!GEAR[id]||!Number.isInteger(tier)||tier<1||tier>12)return false;if(version<3)return true;if(version>=4){if(slots.size>=capacity)return false;slots.add(id);return true;}const slot=GEAR[id].slot;if(slots.has(slot))return false;slots.add(slot);return true;}));
  const p={id:String(source.id||source.species).slice(0,80),species:source.species,nickname:String(source.nickname||'').slice(0,24),level,stage,types,
    learned:[...new Set((source.learned||[]).filter(id=>MOVES[id]))].slice(0,200),gear:tiers,
    loadout:[...new Set(source.loadout||[])].slice(0,4),status:{},statusPower:{},effectUntil:{},cooldowns:{},uses:{},shield:0,guard:0,counter:0,prepare:0};
  if(source.mastery&&stage===3&&types.includes(source.mastery)&&CONFIG.masteries[source.mastery])p.mastery=source.mastery;
  if(version>=4&&source.classification==='ultra')p.classification='ultra';
  if(version>=5){p.combatVersion=version;p.healingMomentum=0;}
  const allowed=new Set(unlocked(p));
  if(p.loadout[0]!=='strike'||p.loadout.some(id=>!allowed.has(id)))throw Error('Invalid social-battle loadout.');
  p.combatStats=statsForCombatVersion(p,tiers,version);p.maxHp=p.combatStats.hp;p.maxEnergy=p.combatStats.maxEnergy;p.hp=p.maxHp;p.energy=p.maxEnergy;
  return p;
}
function freshTeam(source,version=SOCIAL_COMBAT_VERSION){
  if(!Array.isArray(source)||!source.length||source.length>3)throw Error('Choose one to three companions.');
  if(new Set(source.map(p=>p.id)).size!==source.length)throw Error('A team cannot repeat the same companion.');
  const gearOwners=new Set();
  return source.map(p=>{const member=freshMember(p,p.gear||{},version);if(version===3)for(const id of Object.keys(member.gear)){if(gearOwners.has(id))throw Error('Equipment needs a unique companion owner.');gearOwners.add(id);}return member;});
}
export function snapshotTeam(state){
 const members=team(state),lead=members.find(p=>p.id===state.run.activeId&&p.hp>0)||members.find(p=>p.hp>0)||members[0];
 return freshTeam([lead,...members.filter(p=>p!==lead)].filter(Boolean).map(p=>({...p,gear:equipped(state,p)})));
}
export function averageLevel(members){if(!Array.isArray(members)||!members.length)return 1;return members.reduce((sum,p)=>sum+integer(p.level,1,CONFIG.maxLevel,'companion level'),0)/members.length;}
export function duelStake(members){return Math.min(10,Math.max(2,2*Math.floor(averageLevel(members))));}
export function bossHealth(level){level=integer(level,1,1000,'tribe boss level');return Math.round(1500+850*(level-1)+150*Math.pow(level-1,1.6));}
export function bossReward(level){level=integer(level,1,1000,'tribe boss level');return{fruit:Math.min(100,8+level*2),xp:Math.min(500,25+level*5),items:{berry:1,...(level%3===0?{bondCharm:1}:{})}};}
export function duelRewards(duel){
  if(duel.status!=='finished')throw Error('Finish the duel before collecting rewards.');
  return Object.fromEntries(duel.players.map((id,index)=>[id,{xp:Math.min(120,12+Math.floor(averageLevel(duel.teams[index]))*2)+(duel.winner===id?8:0),fruitCap:duel.winner===id?duelStake(duel.teams[1-index]):0}]));
}
export function availableMoves(unit,turnCount=0,members=[],battle=null){
  return unit.loadout.map(id=>{
    const move=MOVES[id],modern=(battle?.version||unit.combatVersion||4)>=5,releasing=modern&&unit.charging?.id===id&&unit.charging.resolveTurn<=turnCount,cost=modern&&battle?equipmentMoveCost(battle,unit,move):move.cost||0;let reason='';
    if(!alive(unit))reason='Knocked out';
    else if(modern&&unit.charging&&!['brace','breathe',unit.charging.id].includes(id))reason='Release the prepared technique or switch';
    else if(modern&&unit.charging?.id===id&&!releasing)reason=`Charging · ${unit.charging.resolveTurn-turnCount} turn remaining`;
    else if(!releasing&&unit.energy<cost)reason='Not enough energy';
    else if(!releasing&&(unit.cooldowns[id]||0)>turnCount)reason=`${unit.cooldowns[id]-turnCount} turns of cooldown`;
    else if(!releasing&&move.uses&&(unit.uses[id]||0)>=move.uses)reason='No uses left';
    else if(move.switch&&unit.status.root&&(unit.effectUntil.root??Infinity)>turnCount)reason='Rooted';
    else if(move.switch&&!members.some(p=>alive(p)&&p.id!==unit.id))reason='Needs another conscious teammate';
    return{id,available:!reason,reason,...(modern?{cost:releasing?0:cost,charging:unit.charging?.id===id,releasing}: {})};
  });
}
function applyStatus(p,id,duration,ownerTurns,power){p.status[id]=Math.max(p.status[id]||0,duration);p.effectUntil[id]=Math.max(p.effectUntil[id]||0,ownerTurns+duration);if(power!==undefined)p.statusPower[id]=power;}
function expire(p,ownTurns){
  for(const id of Object.keys(p.status))if((p.effectUntil[id]??Infinity)<=ownTurns){delete p.status[id];delete p.statusPower[id];delete p.effectUntil[id];if(id==='shield')p.shield=0;if(id==='counter')p.counter=0;if(id==='prepare')p.prepare=0;if(id==='evasion')delete p.evasion;}
}
function heal(p,amount){if(!alive(p))return 0;const restored=Math.min(p.maxHp-p.hp,Math.round(amount*(p.species==='langur'?1.25:1)));p.hp+=Math.max(0,restored);return Math.max(0,restored);}
function clearEffects(p){p.status={};p.statusPower={};p.effectUntil={};p.shield=0;p.guard=0;p.counter=0;p.prepare=0;}
function selectedIncoming(members,active,index){
  const chosen=index===undefined?members.findIndex((p,i)=>i!==active&&alive(p)):index;
  if(!Number.isInteger(chosen)||chosen<0||chosen>=members.length||chosen===active||!alive(members[chosen]))throw Error('Choose another conscious teammate.');return chosen;
}
function enter(s,side,index,{forced=false}={}){
  const outgoing=s.teams[side][s.active[side]];
  if(s.version>=5)delete outgoing.charging;
  s.active[side]=index;const incoming=s.teams[side][index],rally=s.rallies[side];
  if(s.version>=4)equipmentSwitch(s,outgoing,incoming,s.teams[side],{voluntary:!forced});
  if(rally&&rally.until>s.turnCounts[side]){
    if(rally.prepare){incoming.prepare=rally.prepare;applyStatus(incoming,'prepare',rally.duration,s.turnCounts[side]);}
    if(rally.shield){incoming.shield=Math.max(incoming.shield,Math.round(incoming.maxHp*rally.shield));applyStatus(incoming,'shield',rally.duration,s.turnCounts[side]);}
  }s.rallies[side]=null;log(s,`${s.names[side]} brings in ${displayName(incoming)}.`);return incoming;
}
function validateAction(s,side,action){
  if(!action||!['move','switch','surrender'].includes(action.type))throw Error('Choose a move or switch.');
  if(action.expectedSequence!==undefined&&action.expectedSequence!==s.sequence)throw Error('The battle changed. Wait for the current turn.');
  const actor=s.teams[side][s.active[side]];
  if(action.type==='surrender')return;
  if(action.type==='switch'){if(actor.status.root)throw Error('Rooted companions cannot switch this turn.');selectedIncoming(s.teams[side],s.active[side],action.index);return;}
  const option=availableMoves(actor,s.turnCounts[side],s.teams[side],s).find(m=>m.id===action.id);
  if(!option||!option.available)throw Error(option?.reason||'This companion does not have that ability.');
  if(MOVES[action.id].switch)selectedIncoming(s.teams[side],s.active[side],action.index);
}
function execute(s,side,action){return s.version>=5?executeLiving(s,side,action):executeLegacy(s,side,action);}
// Versions 1–4 retain their original reducer, including RNG consumption. Live
// timing input never participates in this deterministic alternating-turn path.
function executeLegacy(s,side,action){
  const actor=s.teams[side][s.active[side]],other=1-side,target=s.teams[other][s.active[other]],m=MOVES[action.id],duration=m.duration||2,st=s.version>=4?equipmentBattleStats(s,actor,actor.combatStats,s.teams[side]):actor.combatStats,targetStats=s.version>=4?equipmentBattleStats(s,target,target.combatStats,s.teams[other]):target.combatStats,healMultiplier=s.version>=4?equipmentHealingMultiplier(s,actor,m):1;
  actor.energy-=m.cost||0;actor.uses[action.id]=(actor.uses[action.id]||0)+1;if(m.cooldown)actor.cooldowns[action.id]=s.turnCounts[side]+m.cooldown;
  if(m.cleanse)clearEffects(actor);
  if(m.guard){actor.guard=actor.species==='mandrill'?.3:.45;actor.energy=Math.min(actor.maxEnergy,actor.energy+2);}
  if(m.energy)actor.energy=Math.min(actor.maxEnergy,actor.energy+m.energy);
  if(m.heal)heal(actor,actor.maxHp*m.heal*healMultiplier);
  if(m.teamHeal)for(const p of s.teams[side])heal(p,p.maxHp*m.teamHeal*healMultiplier);
  if(m.shield){actor.shield=Math.max(actor.shield,Math.round(actor.maxHp*m.shield));applyStatus(actor,'shield',duration,s.turnCounts[side]);}
  if(m.counter){actor.counter=m.counter;applyStatus(actor,'counter',duration,s.turnCounts[side]);}
  if(m.prepare){actor.prepare=m.prepare;applyStatus(actor,'prepare',duration,s.turnCounts[side]);}
  if(m.statusSelf)applyStatus(actor,m.statusSelf,duration,s.turnCounts[side]);
  if(m.root)applyStatus(target,'root',typeof m.root==='number'?m.root:duration,s.turnCounts[other]);
  if(m.weaken)applyStatus(target,'weaken',duration,s.turnCounts[other],typeof m.weaken==='number'?m.weaken:.25);
  if(m.rally||m.teamShield&&!m.switch)s.rallies[side]={prepare:m.rally?(typeof m.rally==='number'?m.rally:.18):0,shield:m.teamShield||0,duration,until:s.turnCounts[side]+duration+1};
  let damage=0;
  if(m.power){
    if(m.breakShield){target.shield=0;target.guard=0;delete target.status.shield;delete target.effectUntil.shield;}
    const eff=socialMoveEffect(m,target.types,s.version??1),stab=(m.types||[m.type]).some(t=>actor.types.includes(t))?(s.version===1?1.15:CONFIG.elemental.stab):1,crit=random(s)<(actor.species==='macaque'?.14:.07),howl=actor.species==='howler'&&m.type!=='neutral'?1.12:1;
    let hit=Math.max(1,Math.round((m.power+st.atk*.7-targetStats.def*.42)*eff*stab*howl*(crit?1.4:1)*(.94+random(s)*.12)*(target.guard||1)*(actor.prepare?1+actor.prepare:1)*(actor.status.weaken?1-(actor.statusPower.weaken||.25):1)*(s.version>=4?equipmentAttackMultiplier(s,actor,target,m):s.version>=3?equipmentDamageMultiplier(actor.gear,target.gear,m):1)));
    if(target.species==='spider'&&random(s)<.08)hit=0;
    actor.prepare=0;delete actor.status.prepare;delete actor.effectUntil.prepare;delete actor.status.weaken;delete actor.effectUntil.weaken;delete actor.statusPower.weaken;target.guard=0;
    if(s.version>=4)damage=equipmentApplyHit(s,actor,target,m,hit,{effectiveness:eff}).damage;
    else{const absorbed=Math.min(target.shield,hit);target.shield-=absorbed;hit-=absorbed;damage=Math.min(target.hp,hit);target.hp=Math.max(0,target.hp-damage);}
    if(target.counter&&damage){actor.hp=Math.max(0,actor.hp-Math.max(1,Math.round(damage*target.counter)));target.counter=0;delete target.status.counter;delete target.effectUntil.counter;}
    if(m.status&&alive(target)&&!(m.status==='burn'&&target.types.includes('fire')||m.status==='poison'&&target.types.includes('leaf')))applyStatus(target,m.status,duration,s.turnCounts[other],m.status==='weaken'?(m.weaken||.25):undefined);
    if(m.drain)heal(actor,damage*m.drain);if(s.version>=3)applyEquipmentLifesteal(actor,st,actor.gear,damage);
    if(m.drainEnergy){const drained=Math.min(target.energy,m.drainEnergy);target.energy-=drained;actor.energy=Math.min(actor.maxEnergy,actor.energy+drained);}
    if(s.version!==1)s.lastHit={actorId:actor.id,targetId:target.id,moveId:action.id,side,multiplier:eff,damage,critical:crit,sequence:s.sequence};
    const feedback=s.version===1?'':` · ${effectInfo(eff).label}${eff!==1?` (×${eff})`:''}`;
    log(s,`${displayName(actor)} uses ${m.name}: ${damage} damage${crit?' (critical)':''}${feedback}.`);
  }else log(s,`${displayName(actor)} uses ${m.name}.`);
  if(s.version>=4)equipmentAfterAbility(s,actor,m,{team:s.teams[side],recipients:m.teamHeal?s.teams[side]:[actor]});
  if(m.switch&&alive(actor)){
    const incoming=enter(s,side,selectedIncoming(s.teams[side],s.active[side],action.index));
    if(m.teamShield){incoming.shield=Math.max(incoming.shield,Math.round(incoming.maxHp*m.teamShield));applyStatus(incoming,'shield',duration,s.turnCounts[side]);}
    if(m.transferSpeed){applyStatus(incoming,'speed',Math.max(1,(actor.effectUntil.speed??s.turnCounts[side]+duration)-s.turnCounts[side]),s.turnCounts[side],actor.statusPower.speed);delete actor.status.speed;delete actor.effectUntil.speed;}
    if(m.entryEnergy&&!(incoming.cooldowns.secondWind>s.turnCounts[side])){incoming.energy=Math.min(incoming.maxEnergy,incoming.energy+m.entryEnergy);incoming.cooldowns.secondWind=s.turnCounts[side]+Math.max(4,m.cooldown||0);}
  }
  return damage;
}
function livingMetric(s,side,key,amount=1){s.metrics??=[{},{}];s.metrics[side][key]=(s.metrics[side][key]||0)+amount;}
function removeStatus(p,id){delete p.status[id];delete p.statusPower[id];delete p.effectUntil[id];}
function cleanseTeam(members){for(const p of members)if(alive(p))for(const status of ['burn','poison','slow','root','weaken'])removeStatus(p,status);}
function executeLiving(s,side,action){
 const actor=s.teams[side][s.active[side]],other=1-side,target=s.teams[other][s.active[other]],members=s.teams[side],opponents=s.teams[other],m=MOVES[action.id],duration=m.duration||2,ownTurns=s.turnCounts[side],targetTurns=s.turnCounts[other],st=equipmentBattleStats(s,actor,actor.combatStats,members),targetStats=equipmentBattleStats(s,target,target.combatStats,opponents),releasing=actor.charging?.id===action.id&&actor.charging.resolveTurn<=ownTurns;
 if(!releasing){actor.energy-=equipmentMoveCost(s,actor,m,{consume:true});actor.uses[action.id]=(actor.uses[action.id]||0)+1;if(m.cooldown)actor.cooldowns[action.id]=ownTurns+m.cooldown;}
 if(m.charge&&!releasing){actor.charging={id:action.id,resolveTurn:ownTurns+m.charge+(actor.status.slow?1:0),paid:m.cost||0};log(s,`${displayName(actor)} charges ${m.name}; resolves on its next turn${actor.status.slow?' after a one-turn Slow delay':''}. The opponent can interrupt, guard, or switch.`);return 0;}
 if(releasing)delete actor.charging;
 let restored=0,damage=0;const healMultiplier=equipmentHealingMultiplier(s,actor,m),shieldMultiplier=equipmentShieldMultiplier(s,actor,m);
 s.metrics??=[{},{}];s.metrics[side].elementUses??={};for(const type of m.types||[m.type])if(ELEMENTS[type])s.metrics[side].elementUses[type]=(s.metrics[side].elementUses[type]||0)+1;
 if(m.cleanse){clearEffects(actor);delete actor.evasion;}
 if(m.guard){actor.guard=actor.species==='mandrill'?.3:.45;actor.energy=Math.min(actor.maxEnergy,actor.energy+2);}
 if(m.evasion){actor.evasion=m.evasion;applyStatus(actor,'evasion',duration,ownTurns);}
 if(m.energy)actor.energy=Math.min(actor.maxEnergy,actor.energy+m.energy);
 if(m.heal){let recipient=actor;if(m.healTarget){if(action.targetIndex!==undefined){recipient=members[action.targetIndex];if(!alive(recipient))throw Error('Choose a conscious healing target.');}else if(action.targetId!==undefined){recipient=members.find(p=>p.id===action.targetId);if(!alive(recipient))throw Error('Choose a conscious healing target.');}}restored+=heal(recipient,recipient.maxHp*m.heal*healMultiplier);}
 if(m.teamHeal)for(const p of members)restored+=heal(p,p.maxHp*m.teamHeal*healMultiplier);
 if(m.teamCleanse)cleanseTeam(members);
 if(m.teamEnergy)for(const p of members)if(alive(p))p.energy=Math.min(p.maxEnergy,p.energy+m.teamEnergy);
 if(m.reviveReserve){const reserve=members.find(p=>p!==actor&&!alive(p));if(reserve){reserve.hp=Math.max(1,Math.round(reserve.maxHp*m.reviveReserve));clearEffects(reserve);delete reserve.evasion;delete reserve.charging;log(s,`${displayName(reserve)} returns at ${Math.round(m.reviveReserve*100)}% health.`);}}
 if(m.shield){actor.shield=Math.max(actor.shield,Math.round(actor.maxHp*m.shield*shieldMultiplier));applyStatus(actor,'shield',duration,ownTurns);}
 if(m.counter){actor.counter=m.counter;actor.counterAbsorbed=m.expansion===3;actor.drainOnCounter=m.drainOnCounter||0;applyStatus(actor,'counter',m.duration||1,ownTurns);}
 if(m.reduceCooldown)for(const[id,due]of Object.entries(actor.cooldowns))if(id!==action.id&&due>ownTurns)actor.cooldowns[id]=Math.max(ownTurns,due-m.reduceCooldown);
 if(m.prepare){actor.prepare=m.prepare;applyStatus(actor,'prepare',duration,ownTurns);}
 if(m.statusSelf)applyStatus(actor,m.statusSelf,duration,ownTurns);
 if(m.rally)s.rallies[side]={...s.rallies[side],prepare:typeof m.rally==='number'?m.rally:.18,duration,until:ownTurns+duration+1};
 if(m.teamShield&&!m.switch){if(m.expansion===3){for(const p of members)if(alive(p)){p.shield=Math.max(p.shield,Math.round(p.maxHp*m.teamShield*shieldMultiplier));applyStatus(p,'shield',duration,ownTurns);}}else s.rallies[side]={...s.rallies[side],prepare:s.rallies[side]?.prepare||0,shield:m.teamShield*shieldMultiplier,duration,until:ownTurns+duration+1};}
 if(m.root)applyStatus(target,'root',typeof m.root==='number'?m.root:duration,targetTurns);
 if(m.weaken)applyStatus(target,'weaken',duration,targetTurns,typeof m.weaken==='number'?m.weaken:.25);
 const comboActive=m.comboStatuses?.some(status=>target.status[status]),defensive=!!target.shield||!!target.guard,interrupted=!!m.interrupt&&(!!target.charging||!!target.prepare);
 if(interrupted){delete target.charging;target.prepare=0;removeStatus(target,'prepare');livingMetric(s,side,'interrupts');log(s,`${displayName(actor)} interrupts the prepared technique.`);}
 if(m.delayCharge&&target.charging&&!target.charging.delayed){target.charging.resolveTurn+=m.delayCharge;target.charging.delayed=true;log(s,`${displayName(target)} needs one additional turn to finish charging.`);}
 if(comboActive){for(const status of m.comboStatuses)removeStatus(target,status);livingMetric(s,side,'combos');}
 if(m.power){
  if(m.breakShield){target.shield=0;target.guard=0;removeStatus(target,'shield');}
  const eff=socialMoveEffect(m,target.types,s.version),stab=(m.types||[m.type]).some(t=>actor.types.includes(t))?CONFIG.elemental.stab:1,critical=random(s)<(actor.species==='macaque'?.14:.07),howl=actor.species==='howler'&&m.type!=='neutral'?1.12:1,healingPower=m.healingPower?Math.min(30,(actor.healingMomentum||0)*m.healingPower):0,finisher=m.finisherThreshold&&target.hp<=target.maxHp*m.finisherThreshold?1+m.finisherBonus:1,tacticalMove={...m,perfectTiming:false,comboActive:!!comboActive,interrupted};
  const raw=Math.max(1,Math.round((m.power+healingPower+st.atk*.7-targetStats.def*.42)*eff*stab*howl*(critical?1.4:1)*(.94+random(s)*.12)*(actor.prepare?1+actor.prepare:1)*(actor.status.weaken?1-(actor.statusPower.weaken||.25):1)*(comboActive?1+m.comboBonus:1)*(m.feint&&defensive?1+m.feint:1)*finisher*equipmentAttackMultiplier(s,actor,target,tacticalMove)));
  let hit=Math.max(1,Math.round(raw*(m.feint?1:target.guard||1)*(1-(target.evasion||0))));if(target.species==='spider'&&random(s)<.08)hit=0;
  const blocked=!!target.guard||target.shield>0;if(blocked)livingMetric(s,other,'blocks');
  actor.prepare=0;removeStatus(actor,'prepare');removeStatus(actor,'weaken');target.guard=0;if(m.healingPower)actor.healingMomentum=0;if(target.evasion){delete target.evasion;removeStatus(target,'evasion');}
  damage=equipmentApplyHit(s,actor,target,tacticalMove,hit,{effectiveness:eff}).damage;
  if(target.counter&&(damage||target.counterAbsorbed)){
   const reflected=Math.min(actor.hp,Math.max(1,Math.round((target.counterAbsorbed?Math.max(damage,raw):damage)*target.counter)));actor.hp=Math.max(0,actor.hp-reflected);livingMetric(s,other,'counters');
   equipmentTacticalEvent({...s,round:targetTurns},target,'counter',{maxEnergy:target.maxEnergy});if(target.drainOnCounter){const n=Math.min(actor.energy,target.drainOnCounter);actor.energy-=n;target.energy=Math.min(target.maxEnergy,target.energy+n);}target.counter=0;target.drainOnCounter=0;removeStatus(target,'counter');log(s,`${displayName(target)} counters for ${reflected} damage.`);
  }
  if(m.status&&alive(target)&&!(m.status==='burn'&&target.types.includes('fire')||m.status==='poison'&&target.types.includes('leaf')))applyStatus(target,m.status,duration,targetTurns,m.status==='weaken'?(m.weaken||.25):undefined);
  if(m.drain)restored+=heal(actor,damage*m.drain);applyEquipmentLifesteal(actor,st,actor.gear,damage);
  if(m.splash)for(const reserve of opponents)if(reserve!==target&&alive(reserve))equipmentApplyHit(s,actor,reserve,tacticalMove,Math.round(damage*m.splash),{direct:false,effectiveness:eff});
  s.lastHit={actorId:actor.id,targetId:target.id,moveId:action.id,side,multiplier:eff,damage,critical,sequence:s.sequence,interrupted,combo:!!comboActive,blocked,charged:releasing,feint:!!(m.feint&&defensive),perfectTiming:false};
  log(s,`${displayName(actor)} uses ${m.name}: ${damage} damage${critical?' (critical)':''} · ${effectInfo(eff).label}${interrupted?' · Interrupted':''}${comboActive?' · Elemental combo':''}${blocked?' · Blocked':''}.`);
 }else log(s,`${displayName(actor)} uses ${m.name}.`);
 if(m.drainEnergy){const drained=Math.min(target.energy,m.drainEnergy);target.energy-=drained;actor.energy=Math.min(actor.maxEnergy,actor.energy+drained);if(m.cancelUnfundedCharge&&target.charging&&target.energy<equipmentMoveCost(s,target,MOVES[target.charging.id])){delete target.charging;livingMetric(s,side,'interrupts');log(s,'Energy disruption interrupted the charge.');}}
 actor.healingMomentum=Math.min(100,(actor.healingMomentum||0)+restored);
 equipmentAfterAbility(s,actor,m,{target,moveId:action.id,team:members,recipients:m.teamHeal?members:[actor]});
 if(m.switch&&alive(actor)){
  const incoming=enter(s,side,selectedIncoming(members,s.active[side],action.index));livingMetric(s,side,'switches');
  if(m.teamShield){incoming.shield=Math.max(incoming.shield,Math.round(incoming.maxHp*m.teamShield*shieldMultiplier));applyStatus(incoming,'shield',duration,ownTurns);}
  if(m.transferSpeed){applyStatus(incoming,'speed',Math.max(1,(actor.effectUntil.speed??ownTurns+duration)-ownTurns),ownTurns,actor.statusPower.speed);removeStatus(actor,'speed');}
  if(m.entryEnergy&&!(incoming.cooldowns.secondWind>ownTurns)){incoming.energy=Math.min(incoming.maxEnergy,incoming.energy+m.entryEnergy);incoming.cooldowns.secondWind=ownTurns+Math.max(4,m.cooldown||0);}
 }
 return damage;
}
function tickSide(s,side,turnActor=null){
  let damage=0;
  for(const p of s.teams[side])if(alive(p)){
    for(const id of ['burn','poison'])if(p.status[id]){let hit=Math.min(p.hp,Math.max(1,Math.round(p.maxHp*.04)));if(s.version>=4)hit=equipmentApplyHit(s,s.teams[1-side][s.active[1-side]],p,{type:id==='burn'?'fire':'leaf'},hit,{direct:false,ignoreShields:true}).damage;else p.hp-=hit;damage+=hit;log(s,`${displayName(p)} takes ${hit} ${id} damage.`);}
    if(alive(p)){const energy=s.version>=5?(p.status.slow?0:p.status.speed?2:1):1;p.energy=Math.min(p.maxEnergy,p.energy+energy);if(s.version>=3&&p===s.teams[side][s.active[side]]){const recovered=s.version>=4?equipmentApplyRecovery(s,p,p.combatStats):applyEquipmentRecovery(p,p.combatStats,p.gear);if(recovered.healing)log(s,`${displayName(p)} recovers ${recovered.healing} health from equipment.`);}}
  }
  if(s.version>=4&&turnActor)equipmentTurnEnd(s,turnActor,s.teams[side]);
  s.turnCounts[side]++;return damage;
}
function replaceKnockouts(s){for(let side=0;side<2;side++){
  if(s.version>=4)for(const p of s.teams[side])if(!alive(p))equipmentKnockout(s,p,s.teams[side],{active:p===s.teams[side][s.active[side]]});
  const current=s.teams[side][s.active[side]],reserveOnly=s.version>=4&&equipmentRevivedToReserve(s,current);
  if(!alive(current)||reserveOnly){const next=s.teams[side].findIndex(p=>alive(p)&&(!reserveOnly||p!==current));if(next>=0)enter(s,side,next,{forced:true});}
}}
function finishDuel(s){
  const living=s.teams.map(m=>m.some(alive));
  if(living[0]&&living[1]&&s.sequence<DUEL_TURN_LIMIT)return false;
  s.status='finished';
  if(!living[0]&&!living[1])s.winner=null;
  else if(!living[0]||!living[1])s.winner=s.players[living[0]?0:1];
  else{const ratio=s.teams.map(m=>m.reduce((n,p)=>n+p.hp,0)/m.reduce((n,p)=>n+p.maxHp,0));s.winner=Math.abs(ratio[0]-ratio[1])<1e-9?null:s.players[ratio[0]>ratio[1]?0:1];}
  log(s,s.winner?`${s.names[s.players.indexOf(s.winner)]} wins the friendly duel.`:'The friendly duel is a draw.');return true;
}
export function newDuel(a,b,seed=1234567,version=SOCIAL_COMBAT_VERSION){
  version=balanceVersion(version);
  if(!a?.id||!b?.id||a.id===b.id)throw Error('A duel needs two different players.');
  const battle={version,players:[String(a.id),String(b.id)],names:[String(a.name||'Challenger').slice(0,30),String(b.name||'Challenger').slice(0,30)],teams:[freshTeam(a.team,version),freshTeam(b.team,version)],active:[0,0],turn:0,sequence:0,round:1,turnCounts:[0,0],rallies:[null,null],status:'active',winner:null,rng:freshSeed(seed),log:['Friendly duel: alternating turns; expedition health is protected.']};
  if(version>=5){battle.metrics=[{},{}];battle.log.push('Timing is assisted for both players. Slow pauses energy recovery and delays new charges; Speed restores 2 energy per turn.');}
  if(version>=4){lockEquipmentBattle(battle,battle.teams,p=>p.gear,p=>p.combatStats);for(const members of battle.teams)equipmentEnter(battle,members[0],members);}
  return battle;
}
export function duelAction(duel,playerId,action){
  if(duel.status!=='active')throw Error('This duel has finished.');
  const side=duel.players.indexOf(playerId);if(side<0||side!==duel.turn&&action?.type!=='surrender')throw Error('Wait for your turn.');
  const next=clone(duel),actor=next.teams[side][next.active[side]];
  for(const p of next.teams[side])expire(p,next.turnCounts[side]);validateAction(next,side,action);
  if(action.type==='surrender'){next.status='finished';next.winner=next.players[1-side];next.sequence++;log(next,`${next.names[side]} concedes the friendly duel.`);}
  else{
    actor.guard=0;
    if(action.type==='switch')enter(next,side,selectedIncoming(next.teams[side],next.active[side],action.index));else execute(next,side,action);
    tickSide(next,side,actor);next.sequence++;next.round=Math.floor(next.sequence/2)+1;replaceKnockouts(next);
    if(!finishDuel(next))next.turn=1-side;
  }
  Object.assign(duel,next);return duel;
}
export function newRaidAttempt(members,bossLevel=1,seed=1234567,version=SOCIAL_COMBAT_VERSION,patternVersion=0){
  version=balanceVersion(version);
  bossLevel=integer(bossLevel,1,1000,'tribe boss level');
  const roster=Object.values(SPECIES).filter(p=>p.rarity==='ultra'&&(version>=5||p.number<=90)),species=roster[(bossLevel-1)%roster.length]?.id||'mandrill';
  const target=freshMember({id:'tribe-boss',species,level:Math.min(60,bossLevel+2),stage:Math.min(3,Math.floor((bossLevel-1)/8)),types:SPECIES[species].types||[],loadout:['strike'],learned:[]},{},version);
  target.hp=target.maxHp=bossHealth(bossLevel);target.combatStats.hp=target.hp;
  const attempt={version,team:freshTeam(members,version),active:0,turns:0,totalDamage:0,status:'active',sequence:0,boss:target,bossLevel,turnCounts:[0,0],rallies:[null,null],rng:freshSeed(seed),log:['Ten turns to help your tribe. The boss never attacks.']};
  if(version>=5)attempt.metrics=[{},{}];
  if(version>=4){lockEquipmentBattle(attempt,[attempt.team,[target]],p=>p.gear,p=>p.combatStats);equipmentEnter(attempt,attempt.team[0],attempt.team);}
  // Tickets explicitly opt in; previously started v1/v2 attempts replay unchanged.
  if(patternVersion===1&&version>=2&&bossLevel%3===0){attempt.guardianPattern={version:1};prepareRaidWard(attempt);attempt.log.push('This guardian raises a ward every fourth turn. Break its shield or use the opening to prepare.');}
  return attempt;
}
export function raidIntent(attempt){
  if(!attempt?.guardianPattern)return{label:'Tribe challenge',detail:'The boss never attacks. Use ten actions to contribute damage.',ward:false};
  const slot=(attempt.turns||0)%4;
  return slot===1?{label:'Ward active',detail:'The guardian blocks most damage and carries a small shield this turn. Shield-breaking moves remove both; recovery or preparation can wait out the ward.',ward:true}:
    slot===0?{label:'Ward forming next turn',detail:'The guardian is open now and will raise its ward next turn. Deal damage now or prepare a shield-breaking response.',ward:false}:
    {label:'Recovery opening',detail:'The ward is down while the guardian recovers. Attack, replenish energy, or prepare. The boss never attacks.',ward:false};
}
function prepareRaidWard(attempt){
  if(attempt.guardianPattern?.version!==1)return;
  const ward=attempt.turns%4===1;attempt.boss.guard=ward?.45:0;attempt.boss.shield=ward?Math.min(90,16+attempt.bossLevel*3):0;
}
export function raidAction(attempt,action){
  if(attempt.status!=='active'||attempt.turns>=RAID_TURNS)throw Error('This raid attempt has finished.');
  if(action?.type==='surrender')throw Error('Choose an ability or switch for this raid turn.');
  const next=clone(attempt),battle={...next,teams:[next.team,[next.boss]],active:[next.active,0],names:['You','Tribe boss']},turnActor=next.team[next.active];
  for(const p of next.team)expire(p,battle.turnCounts[0]);expire(next.boss,battle.turnCounts[1]);validateAction(battle,0,action);
  if(action.type==='switch')enter(battle,0,selectedIncoming(next.team,next.active,action.index));else battle.totalDamage+=execute(battle,0,action);
  tickSide(battle,0,turnActor);battle.totalDamage+=tickSide(battle,1);battle.sequence++;battle.turns++;
  next.team=battle.teams[0];next.active=battle.active[0];next.boss=battle.teams[1][0];next.totalDamage=battle.totalDamage;next.sequence=battle.sequence;next.turns=battle.turns;next.turnCounts=battle.turnCounts;next.rallies=battle.rallies;next.rng=battle.rng;next.log=battle.log;if(battle.lastHit)next.lastHit=battle.lastHit;
  if(battle.equipmentBattle)next.equipmentBattle=battle.equipmentBattle;
  if(next.turns===RAID_TURNS){next.status='finished';log(next,`Raid complete: ${Math.round(next.totalDamage)} damage for your tribe.`);}else prepareRaidWard(next);
  Object.assign(attempt,next);return attempt;
}
