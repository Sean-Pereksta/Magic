// Deterministic social combat. Every fight uses disposable, healthy team copies.
// The reducers mutate only their supplied social state and return that same object.
import {SPECIES,MOVES,ELEMENTS,GEAR,CONFIG,stats,statsLegacy,equipped,team,unlocked,moveEffect,moveEffectLegacy,effectInfo,random} from './core.mjs';

const clone=value=>structuredClone(value),alive=p=>p&&p.hp>0;
const integer=(value,min,max,label)=>{if(!Number.isInteger(value)||value<min||value>max)throw Error(`Invalid ${label}.`);return value;};
const displayName=p=>p.nickname||SPECIES[p.species]?.name||'Companion';
const log=(s,text)=>{s.log.push(text);s.log=s.log.slice(-18);};
const freshSeed=seed=>Number.isInteger(seed)?(seed>>>0)||1234567:1234567;
export const RAID_TURNS=10,DUEL_TURN_LIMIT=80,SOCIAL_COMBAT_VERSION=2;
const balanceVersion=value=>integer(value,1,SOCIAL_COMBAT_VERSION,'combat version');
export const socialMoveEffect=(move,defenders,version=SOCIAL_COMBAT_VERSION)=>version===1?moveEffectLegacy(move,defenders):moveEffect(move,defenders);

function freshMember(source,gear={},version=SOCIAL_COMBAT_VERSION){
  if(!source||!SPECIES[source.species])throw Error('Unknown social-battle companion.');
  const level=integer(source.level,1,CONFIG.maxLevel,'companion level'),stage=integer(source.stage||0,0,3,'evolution stage');
  const types=[...(source.types||[])];if(types.length>2||new Set(types).size!==types.length||types.some(t=>!ELEMENTS[t]))throw Error('Invalid companion elements.');
  const tiers=Object.fromEntries(Object.entries(gear).filter(([id,tier])=>GEAR[id]&&Number.isInteger(tier)&&tier>=1&&tier<=12));
  const p={id:String(source.id||source.species).slice(0,80),species:source.species,nickname:String(source.nickname||'').slice(0,24),level,stage,types,
    learned:[...new Set((source.learned||[]).filter(id=>MOVES[id]))].slice(0,200),gear:tiers,
    loadout:[...new Set(source.loadout||[])].slice(0,4),status:{},statusPower:{},effectUntil:{},cooldowns:{},uses:{},shield:0,guard:0,counter:0,prepare:0};
  if(source.mastery&&stage===3&&types.includes(source.mastery)&&CONFIG.masteries[source.mastery])p.mastery=source.mastery;
  const allowed=new Set(unlocked(p));
  if(p.loadout[0]!=='strike'||p.loadout.some(id=>!allowed.has(id)))throw Error('Invalid social-battle loadout.');
  p.combatStats=(version===1?statsLegacy:stats)(p,tiers);p.maxHp=p.combatStats.hp;p.maxEnergy=p.combatStats.maxEnergy;p.hp=p.maxHp;p.energy=p.maxEnergy;
  return p;
}
function freshTeam(source,version=SOCIAL_COMBAT_VERSION){
  if(!Array.isArray(source)||!source.length||source.length>3)throw Error('Choose one to three companions.');
  if(new Set(source.map(p=>p.id)).size!==source.length)throw Error('A team cannot repeat the same companion.');
  return source.map(p=>freshMember(p,p.gear||{},version));
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
export function availableMoves(unit,turnCount=0,members=[]){
  return unit.loadout.map(id=>{
    const move=MOVES[id];let reason='';
    if(!alive(unit))reason='Knocked out';
    else if(unit.energy<(move.cost||0))reason='Not enough energy';
    else if((unit.cooldowns[id]||0)>turnCount)reason=`${unit.cooldowns[id]-turnCount} turns of cooldown`;
    else if(move.uses&&(unit.uses[id]||0)>=move.uses)reason='No uses left';
    else if(move.switch&&unit.status.root&&(unit.effectUntil.root??Infinity)>turnCount)reason='Rooted';
    else if(move.switch&&!members.some(p=>alive(p)&&p.id!==unit.id))reason='Needs another conscious teammate';
    return{id,available:!reason,reason};
  });
}
function applyStatus(p,id,duration,ownerTurns,power){p.status[id]=Math.max(p.status[id]||0,duration);p.effectUntil[id]=Math.max(p.effectUntil[id]||0,ownerTurns+duration);if(power!==undefined)p.statusPower[id]=power;}
function expire(p,ownTurns){
  for(const id of Object.keys(p.status))if((p.effectUntil[id]??Infinity)<=ownTurns){delete p.status[id];delete p.statusPower[id];delete p.effectUntil[id];if(id==='shield')p.shield=0;if(id==='counter')p.counter=0;if(id==='prepare')p.prepare=0;}
}
function heal(p,amount){if(!alive(p))return 0;const restored=Math.min(p.maxHp-p.hp,Math.round(amount*(p.species==='langur'?1.25:1)));p.hp+=Math.max(0,restored);return Math.max(0,restored);}
function clearEffects(p){p.status={};p.statusPower={};p.effectUntil={};p.shield=0;p.guard=0;p.counter=0;p.prepare=0;}
function selectedIncoming(members,active,index){
  const chosen=index===undefined?members.findIndex((p,i)=>i!==active&&alive(p)):index;
  if(!Number.isInteger(chosen)||chosen<0||chosen>=members.length||chosen===active||!alive(members[chosen]))throw Error('Choose another conscious teammate.');return chosen;
}
function enter(s,side,index){
  s.active[side]=index;const incoming=s.teams[side][index],rally=s.rallies[side];
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
  const option=availableMoves(actor,s.turnCounts[side],s.teams[side]).find(m=>m.id===action.id);
  if(!option||!option.available)throw Error(option?.reason||'This companion does not have that ability.');
  if(MOVES[action.id].switch)selectedIncoming(s.teams[side],s.active[side],action.index);
}
function execute(s,side,action){
  const actor=s.teams[side][s.active[side]],other=1-side,target=s.teams[other][s.active[other]],m=MOVES[action.id],duration=m.duration||2,st=actor.combatStats;
  actor.energy-=m.cost||0;actor.uses[action.id]=(actor.uses[action.id]||0)+1;if(m.cooldown)actor.cooldowns[action.id]=s.turnCounts[side]+m.cooldown;
  if(m.cleanse)clearEffects(actor);
  if(m.guard){actor.guard=actor.species==='mandrill'?.3:.45;actor.energy=Math.min(actor.maxEnergy,actor.energy+2);}
  if(m.energy)actor.energy=Math.min(actor.maxEnergy,actor.energy+m.energy);
  if(m.heal)heal(actor,actor.maxHp*m.heal);
  if(m.teamHeal)for(const p of s.teams[side])heal(p,p.maxHp*m.teamHeal);
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
    let hit=Math.max(1,Math.round((m.power+st.atk*.7-target.combatStats.def*.42)*eff*stab*howl*(crit?1.4:1)*(.94+random(s)*.12)*(target.guard||1)*(actor.prepare?1+actor.prepare:1)*(actor.status.weaken?1-(actor.statusPower.weaken||.25):1)));
    if(target.species==='spider'&&random(s)<.08)hit=0;
    actor.prepare=0;delete actor.status.prepare;delete actor.effectUntil.prepare;delete actor.status.weaken;delete actor.effectUntil.weaken;delete actor.statusPower.weaken;target.guard=0;
    const absorbed=Math.min(target.shield,hit);target.shield-=absorbed;hit-=absorbed;damage=Math.min(target.hp,hit);target.hp=Math.max(0,target.hp-damage);
    if(target.counter&&damage){actor.hp=Math.max(0,actor.hp-Math.max(1,Math.round(damage*target.counter)));target.counter=0;delete target.status.counter;delete target.effectUntil.counter;}
    if(m.status&&alive(target)&&!(m.status==='burn'&&target.types.includes('fire')||m.status==='poison'&&target.types.includes('leaf')))applyStatus(target,m.status,duration,s.turnCounts[other],m.status==='weaken'?(m.weaken||.25):undefined);
    if(m.drain)heal(actor,damage*m.drain);
    if(m.drainEnergy){const drained=Math.min(target.energy,m.drainEnergy);target.energy-=drained;actor.energy=Math.min(actor.maxEnergy,actor.energy+drained);}
    if(s.version!==1)s.lastHit={actorId:actor.id,targetId:target.id,moveId:action.id,side,multiplier:eff,damage,critical:crit,sequence:s.sequence};
    const feedback=s.version===1?'':` · ${effectInfo(eff).label}${eff!==1?` (×${eff})`:''}`;
    log(s,`${displayName(actor)} uses ${m.name}: ${damage} damage${crit?' (critical)':''}${feedback}.`);
  }else log(s,`${displayName(actor)} uses ${m.name}.`);
  if(m.switch&&alive(actor)){
    const incoming=enter(s,side,selectedIncoming(s.teams[side],s.active[side],action.index));
    if(m.teamShield){incoming.shield=Math.max(incoming.shield,Math.round(incoming.maxHp*m.teamShield));applyStatus(incoming,'shield',duration,s.turnCounts[side]);}
    if(m.transferSpeed){applyStatus(incoming,'speed',Math.max(1,(actor.effectUntil.speed??s.turnCounts[side]+duration)-s.turnCounts[side]),s.turnCounts[side],actor.statusPower.speed);delete actor.status.speed;delete actor.effectUntil.speed;}
    if(m.entryEnergy&&!(incoming.cooldowns.secondWind>s.turnCounts[side])){incoming.energy=Math.min(incoming.maxEnergy,incoming.energy+m.entryEnergy);incoming.cooldowns.secondWind=s.turnCounts[side]+Math.max(4,m.cooldown||0);}
  }
  return damage;
}
function tickSide(s,side){
  let damage=0;
  for(const p of s.teams[side])if(alive(p)){
    for(const id of ['burn','poison'])if(p.status[id]){const hit=Math.min(p.hp,Math.max(1,Math.round(p.maxHp*.04)));p.hp-=hit;damage+=hit;log(s,`${displayName(p)} takes ${hit} ${id} damage.`);}
    if(alive(p))p.energy=Math.min(p.maxEnergy,p.energy+1);
  }
  s.turnCounts[side]++;return damage;
}
function replaceKnockouts(s){for(let side=0;side<2;side++)if(!alive(s.teams[side][s.active[side]])){const next=s.teams[side].findIndex(alive);if(next>=0)enter(s,side,next);}}
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
  return{version,players:[String(a.id),String(b.id)],names:[String(a.name||'Challenger').slice(0,30),String(b.name||'Challenger').slice(0,30)],teams:[freshTeam(a.team,version),freshTeam(b.team,version)],active:[0,0],turn:0,sequence:0,round:1,turnCounts:[0,0],rallies:[null,null],status:'active',winner:null,rng:freshSeed(seed),log:['Friendly duel: alternating turns; expedition health is protected.']};
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
    tickSide(next,side);next.sequence++;next.round=Math.floor(next.sequence/2)+1;replaceKnockouts(next);
    if(!finishDuel(next))next.turn=1-side;
  }
  Object.assign(duel,next);return duel;
}
export function newRaidAttempt(members,bossLevel=1,seed=1234567,version=SOCIAL_COMBAT_VERSION,patternVersion=0){
  version=balanceVersion(version);
  bossLevel=integer(bossLevel,1,1000,'tribe boss level');
  const roster=Object.values(SPECIES).filter(p=>p.rarity==='ultra'),species=roster[(bossLevel-1)%roster.length]?.id||'mandrill';
  const target=freshMember({id:'tribe-boss',species,level:Math.min(60,bossLevel+2),stage:Math.min(3,Math.floor((bossLevel-1)/8)),types:SPECIES[species].types||[],loadout:['strike'],learned:[]},{},version);
  target.hp=target.maxHp=bossHealth(bossLevel);target.combatStats.hp=target.hp;
  const attempt={version,team:freshTeam(members,version),active:0,turns:0,totalDamage:0,status:'active',sequence:0,boss:target,bossLevel,turnCounts:[0,0],rallies:[null,null],rng:freshSeed(seed),log:['Ten turns to help your tribe. The boss never attacks.']};
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
  const next=clone(attempt),battle={...next,teams:[next.team,[next.boss]],active:[next.active,0],names:['You','Tribe boss']};
  for(const p of next.team)expire(p,battle.turnCounts[0]);expire(next.boss,battle.turnCounts[1]);validateAction(battle,0,action);
  if(action.type==='switch')enter(battle,0,selectedIncoming(next.team,next.active,action.index));else battle.totalDamage+=execute(battle,0,action);
  tickSide(battle,0);battle.totalDamage+=tickSide(battle,1);battle.sequence++;battle.turns++;
  next.team=battle.teams[0];next.active=battle.active[0];next.boss=battle.teams[1][0];next.totalDamage=battle.totalDamage;next.sequence=battle.sequence;next.turns=battle.turns;next.turnCounts=battle.turnCounts;next.rallies=battle.rallies;next.rng=battle.rng;next.log=battle.log;if(battle.lastHit)next.lastHit=battle.lastHit;
  if(next.turns===RAID_TURNS){next.status='finished';log(next,`Raid complete: ${Math.round(next.totalDamage)} damage for your tribe.`);}else prepareRaidWard(next);
  Object.assign(attempt,next);return attempt;
}
