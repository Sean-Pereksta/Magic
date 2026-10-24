import { UNITS } from './data.mjs';
import { atWar, canEnter, capture, kingdom, log, treaty } from './core.mjs';
import { neighbors, hash, seededRandom } from './world-hex.mjs';
import { effectiveArmySpeed, movementBudget } from './movement-timing.mjs';
import { generalForArmy } from './command-state.mjs';
import { emptyUnits, fortMaximum } from './economy.mjs';
import { damageFortifications, resolveFieldBattle, siegePower, troopTotal } from './warfare.mjs';
import { refreshKnowledge, visionTiles } from './fog.mjs';
import { changeRelation } from './living.mjs';
import { emotionalEvent } from './emotions.mjs';
import { BATTLE_SIZE_OPTIONS, DEFAULT_ACTIVE_TROOPS, finishAutomatically, normalizeBattleCheckpoint, validateBattleCheckpoint } from './battle-sim.mjs';

// The campaign stores only JSON. Neither a renderer nor a running worker is
// required to recover a pending decision or resume the strategic turn.
const clone = value => structuredClone(value);
const WALL_SECTIONS = ['wall-west-front','wall-east-front','wall-west','wall-east','wall-rear','gate'];
const MAX_CHECKPOINT_LENGTH = 6000000;
const finite = (value,min,max) => Number.isFinite(value)&&value>=min&&value<=max;
const fail = message => { throw new Error(message); };
export const supportedOfflineCampaign = s => !!s&&!s.controllers&&!s.sequential&&!s.online&&!s.network&&!s.knowledgeView&&!s.projectionOnly;
export function configureOfflineBattles(s,options={}) {
  if(!supportedOfflineCampaign(s))return null;
  const preference=options.preference??s.offlineBattles?.preference??'ask';
  if(!['ask','auto'].includes(preference))fail('Choose Ask before battles or Resolve automatically.');
  const tacticalTime=options.tacticalTime??s.offlineBattles?.tacticalTime??'pause';
  if(!['pause','slow','real'].includes(tacticalTime))fail('Choose paused, slow motion, or real time command.');
  const battleSize=options.battleSize??s.offlineBattles?.battleSize??DEFAULT_ACTIVE_TROOPS;
  if(!BATTLE_SIZE_OPTIONS.includes(battleSize))fail('Choose 100, 150, or 200 troops on the field.');
  s.offlineBattles={...s.offlineBattles,enabled:options.enabled??s.offlineBattles?.enabled??true,preference,tacticalTime,battleSize};
  return s.offlineBattles;
}
export const pendingEncounter = s => supportedOfflineCampaign(s)?s.offlineBattle||null:null;
function requireEncounter(s) {
  if(!supportedOfflineCampaign(s))fail('Personal battle control is only available in offline campaigns.');
  if(!s.offlineBattle)fail('There is no pending encounter.');
  return s.offlineBattle;
}
function allied(s,a,b) { return a===b||['alliance','vassalage'].some(type=>treaty(s,a,b,type)); }
function sideOf(s,army,attacker,defender) {
  if(allied(s,army.owner,attacker.owner)&&atWar(s,army.owner,defender.owner))return 0;
  if(allied(s,army.owner,defender.owner)&&atWar(s,army.owner,attacker.owner))return 1;
  return null;
}
// A bounded breadth-first walk uses the same legal land neighbors and access
// rules as campaign movement. Screen distance never grants military access.
export function reinforcementRoute(s,army,targetId) {
  if(army.embarkedFleetId||army.embarkOrder||!s.tiles[army.tile])return null;
  const queue=[{tile:army.tile,path:[]}],seen=new Set([army.tile]);
  for(let i=0;i<queue.length;i++){
    const row=queue[i];if(row.tile===targetId)return row.path;
    if(row.path.length===2)continue;
    for(const tile of neighbors(s,s.tiles[row.tile])){
      if(seen.has(tile.id)||!canEnter(s,army.owner,tile))continue;
      if(tile.id!==targetId&&s.armies.some(other=>other.tile===tile.id&&troopTotal(other)&&atWar(s,army.owner,other.owner)))continue;
      seen.add(tile.id);queue.push({tile:tile.id,path:[...row.path,tile.id]});
    }
  }
  return null;
}
function direction(s,source,target,side) {
  const a=s.tiles[source],b=s.tiles[target];
  if(!a||!b||a.id===b.id)return side===0?'west':'east';
  const angle=Math.atan2(a.r-b.r,(a.q-b.q)+(a.r-b.r)*.5);
  return ['east','southeast','southwest','west','northwest','northeast'][(Math.round(angle/(Math.PI/3))+6)%6];
}
function snapshot(s,a,side,tile,route=null,isSupport=false) {
  const general=generalForArmy(s,a),steps=isSupport?route?.length||0:0;
  return {armyId:a.id,owner:a.owner,name:a.name||`${kingdom(s,a.owner)?.name||a.owner} army`,houseName:kingdom(s,a.owner)?.name||a.owner,color:kingdom(s,a.owner)?.color||'#888888',commander:general?{id:general.id,name:general.name,personality:general.personality}: {id:`captain:${a.id}`,name:`Captain of ${a.name||a.id}`,personality:'methodical'},side,playerControlled:a.owner==='ashen',sourceTile:a.tile,units:clone(a.units),morale:a.morale,formation:a.formation||'balanced',commandBonus:a.commandBonus||0,distance:steps,entryDirection:direction(s,route?.length>1?route.at(-2):a.tile,tile.id,side),arrivalSeconds:steps===2?50:steps===1?25:0,isSupport};
}
function supportCandidates(s,attacker,defender,tile,primaryIds) {
  return s.armies.filter(a=>!primaryIds.includes(a.id)&&troopTotal(a)>0).flatMap(a=>{
    const side=sideOf(s,a,attacker,defender);if(side===null)return [];
    const opposing=side===0?s.armies.filter(other=>primaryIds.includes(other.id)&&other.id!==attacker.id):[attacker];
    if(opposing.some(other=>!atWar(s,a.owner,other.owner)))return [];
    const route=reinforcementRoute(s,a,tile.id);if(!route)return [];
    const general=generalForArmy(s,a);
    const occupied=!!a.battleCommitment||a.resolvedTurn===s.turn||movementBudget(s,a)<=0;
    const withdrawing=['withdraw','retreat'].includes(general?.objective?.kind)||a.order==='retreat';
    const defendingHome=a.owner!=='ashen'&&a.tile!==tile.id&&['city','fort','town'].includes(s.tiles[a.tile]?.building)&&neighbors(s,s.tiles[a.tile]).some(n=>s.armies.some(enemy=>enemy.tile===n.id&&troopTotal(enemy)>0&&atWar(s,a.owner,enemy.owner)));
    const available=!occupied&&!withdrawing&&!defendingHome;
    return [{...snapshot(s,a,side,tile,route,true),available,joining:available,reason:occupied?'Already spent its movement or committed elsewhere.':withdrawing?'Following withdrawal orders.':defendingHome?'Protecting its threatened settlement.':'Within two legal land steps.'}];
  });
}

// Called only when movement has actually reached an enemy, never at order time.
// Returns null for the unchanged legacy/online path, otherwise a handled flag.
export function interceptOfflineEncounter(s,attacker,defender,tile,continuation) {
  if(!supportedOfflineCampaign(s)||!s.offlineBattles?.enabled)return null;
  if(s.offlineBattle)return {handled:true,paused:true};
  const defenders=s.armies.filter(a=>a.tile===tile.id&&troopTotal(a)>0&&atWar(s,attacker.owner,a.owner));
  const primary=[snapshot(s,attacker,0,tile),...defenders.map(a=>snapshot(s,a,1,tile))];
  const supportOptions=supportCandidates(s,attacker,defender,tile,primary.map(a=>a.armyId));
  const involved=primary.some(p=>p.playerControlled)||supportOptions.some(p=>p.playerControlled&&p.available);
  const id=`offline-${s.turn}-${s.nextId++}`;
  s.offlineBattle={version:1,id,turn:s.turn,status:'pending',seed:hash(s.rng,id,tile.id),tile:clone({...tile,fortIntegrity:tile.fortIntegrity??fortMaximum(tile)}),attackerId:attacker.id,defenderId:defender.id,playerOwner:'ashen',playerSide:primary.find(p=>p.playerControlled)?.side??supportOptions.find(p=>p.playerControlled&&p.available)?.side??0,primaryIds:primary.map(p=>p.armyId),participants:[...primary,...supportOptions.filter(p=>p.joining).map(p=>clone(p))],supportOptions,commitmentsLocked:false,checkpoint:null,result:null,continuation:clone(continuation)};
  if(involved&&s.offlineBattles.preference==='ask')return {handled:true,paused:true};
  resolveEncounterAutomatically(s);returnFromEncounter(s);
  return {handled:true,paused:false};
}
export function commitEncounterSupport(s,includedOwnArmyIds) {
  const e=requireEncounter(s);if(e.commitmentsLocked)return {ok:true};
  if(e.status!=='pending')fail('Support can only be chosen before deployment.');
  const own=e.supportOptions.filter(p=>p.playerControlled&&p.available);
  const selected=new Set(includedOwnArmyIds??own.map(p=>p.armyId));
  if([...selected].some(id=>!own.some(p=>p.armyId===id)))fail('That army is not available for support.');
  for(const p of e.supportOptions)if(p.playerControlled)p.joining=p.available&&selected.has(p.armyId);
  e.participants=[...e.participants.filter(p=>e.primaryIds.includes(p.armyId)),...e.supportOptions.filter(p=>p.joining).map(p=>clone(p))];
  for(const p of e.participants){const a=s.armies.find(a=>a.id===p.armyId);if(!a||a.battleCommitment&&a.battleCommitment!==e.id)fail('A participating army is already committed.');}
  for(const p of e.participants)s.armies.find(a=>a.id===p.armyId).battleCommitment=e.id;
  e.commitmentsLocked=true;return {ok:true};
}
export function beginEncounter(s) {
  const e=requireEncounter(s);if(e.status==='applied')fail('This encounter has already been settled.');
  if(!e.participants.some(p=>p.playerControlled))fail('Include one of your armies to lead this battle.');
  commitEncounterSupport(s);e.status='active';return {ok:true,encounter:e};
}
function validateCounts(units,maximum,label) {
  if(!units||typeof units!=='object'||Array.isArray(units)||Object.keys(units).some(type=>!Object.hasOwn(UNITS,type)))fail(`Invalid ${label} roster.`);
  for(const type of Object.keys(UNITS))if(!Number.isInteger(units[type]??0)||!finite(units[type]??0,0,maximum[type]||0))fail(`Invalid ${label} count for ${type}.`);
}
function validatedResult(e,result) {
  if(!result||result.battleId!==e.id||![0,1,null].includes(result.winner)||!Array.isArray(result.armies)||result.armies.length!==e.participants.length||new Set(result.armies.map(a=>a.armyId)).size!==e.participants.length)fail('Battle result does not match its encounter.');
  const normalized=clone(result);
  for(const p of e.participants){
    const row=normalized.armies.find(a=>a.armyId===p.armyId);if(!row)fail('Battle result is missing a participating army.');
    validateCounts(row.units,p.units,'surviving');row.units={...emptyUnits(),...row.units};
    if(!finite(row.morale,.1,1))fail('Invalid army morale.');
    for(const field of ['escaped','undeployed'])if(row[field])validateCounts(row[field],row.units,field);
    for(const type of Object.keys(UNITS))if((row.escaped?.[type]||0)+(row.undeployed?.[type]||0)>row.units[type])fail('Escaped and undeployed survivors overlap.');
    row.losses=Object.fromEntries(Object.keys(UNITS).map(type=>[type,(p.units[type]||0)-row.units[type]]));
  }
  if(!finite(result.walls,0,e.tile.walls||0)||!finite(result.fortIntegrity,0,e.tile.fortIntegrity||0))fail('Battle damage cannot restore or exceed the saved fortifications.');
  if(result.wallBreaches!==undefined&&(!Array.isArray(result.wallBreaches)||result.wallBreaches.length>6||result.wallBreaches.some(id=>!WALL_SECTIONS.includes(id))||new Set(result.wallBreaches).size!==result.wallBreaches.length))fail('Invalid breached wall sections.');
  if(result.wallSections!==undefined&&(!Array.isArray(result.wallSections)||result.wallSections.length>6||new Set(result.wallSections.map(w=>w.id)).size!==result.wallSections.length||result.wallSections.some(w=>!WALL_SECTIONS.includes(w.id)||!finite(w.maxHp,1,1000)||!finite(w.hp,0,w.maxHp))))fail('Invalid wall section damage.');
  if(e.checkpoint){
    if(!validateBattleCheckpoint(e.checkpoint,e))fail('The saved battlefield checkpoint is damaged.');
    for(const row of normalized.armies){const remaining=emptyUnits();for(const u of e.checkpoint.units)if(u.armyId===row.armyId&&u.status!=='dead')remaining[u.type]++;validateCounts(row.units,remaining,'checkpoint surviving');}
    if(result.walls>e.checkpoint.fortifications.walls||result.fortIntegrity>e.checkpoint.fortifications.fortIntegrity)fail('Battle result restores damage already recorded in its checkpoint.');
  }
  // Campaign construction and save validation use integral integrity points.
  // Round damage outward, never heal a fractional point on leaving the scene.
  normalized.walls=Math.floor(normalized.walls);normalized.fortIntegrity=Math.floor(normalized.fortIntegrity);
  return normalized;
}
function aggregate(e,side) {
  const group=e.participants.filter(a=>a.side===side),units=emptyUnits(),total=group.reduce((n,a)=>n+troopTotal(a),0);
  for(const a of group)for(const [type,count] of Object.entries(a.units))units[type]+=count;
  return {units,morale:group.reduce((n,a)=>n+a.morale*troopTotal(a),0)/Math.max(1,total),owner:group[0].owner,formation:group[0].formation,commandBonus:Math.max(...group.map(a=>a.commandBonus||0))};
}
function automaticResult(e,seed=e.seed) {
  const sides=[aggregate(e,0),aggregate(e,1)],original=sides.map(a=>clone(a.units)),tile=clone(e.tile);
  const outcome=resolveFieldBattle(sides[0],sides[1],tile,{roll:seededRandom(seed)});
  if(outcome.winner===0)damageFortifications(tile,siegePower(sides[0],tile,{assault:true}));
  const armies=e.participants.map(p=>({armyId:p.armyId,units:emptyUnits(),morale:sides[p.side].morale}));
  // Largest remainders allocate each original type across its own source armies.
  for(let side=0;side<2;side++)for(const type of Object.keys(UNITS)){
    const rows=e.participants.map((p,i)=>({p,i})).filter(({p})=>p.side===side&&p.units[type]);
    let left=sides[side].units[type];
    for(const row of rows){const exact=left*(row.p.units[type]/Math.max(1,original[side][type]));row.count=Math.floor(exact);row.fraction=exact-row.count;}
    let extra=left-rows.reduce((n,row)=>n+row.count,0);
    for(const row of rows.sort((a,b)=>b.fraction-a.fraction||a.p.armyId.localeCompare(b.p.armyId))){if(extra>0){row.count++;extra--;}armies[row.i].units[type]=row.count;}
  }
  return {battleId:e.id,winner:outcome.winner,armies,walls:tile.walls||0,fortIntegrity:tile.fortIntegrity||0,phases:outcome.phases,method:'automatic'};
}
export function resolveEncounterAutomatically(s) {
  const e=requireEncounter(s);if(e.status==='applied')return {ok:true,result:e.result,alreadyApplied:true};
  commitEncounterSupport(s);
  if(e.checkpoint){
    if(!validateBattleCheckpoint(e.checkpoint,e))fail('The saved battlefield checkpoint is damaged.');
    return applyEncounterResult(s,finishAutomatically(clone(e.checkpoint)));
  }
  return applyEncounterResult(s,automaticResult(e));
}
export function saveEncounterCheckpoint(s,checkpoint) {
  const e=requireEncounter(s);if(e.status!=='active')fail('Start the encounter before saving battlefield progress.');
  if(JSON.stringify(checkpoint).length>MAX_CHECKPOINT_LENGTH)fail('Battlefield checkpoint is too large.');
  if(!validateBattleCheckpoint(checkpoint,e))fail('The battlefield checkpoint is damaged.');
  if(e.checkpoint){
    if(checkpoint.time<e.checkpoint.time)fail('The checkpoint is older than the last saved battlefield.');
    const byId=new Map(checkpoint.units.map(u=>[u.id,u]));
    if(e.checkpoint.units.some(u=>u.status==='dead'&&byId.get(u.id)?.status!=='dead')||checkpoint.fortifications.walls>e.checkpoint.fortifications.walls||checkpoint.fortifications.fortIntegrity>e.checkpoint.fortifications.fortIntegrity)fail('The checkpoint restores recorded casualties or damage.');
  }
  if(JSON.stringify({...s,offlineBattle:{...e,checkpoint}}).length>12000000)fail('This battlefield checkpoint would exceed the campaign save size limit.');
  e.checkpoint=clone(checkpoint);return {ok:true};
}
function retreatDestination(s,army,from) {
  return neighbors(s,s.tiles[from]).filter(t=>t.id!==army.tile&&canEnter(s,army.owner,t)&&!s.armies.some(enemy=>enemy.tile===t.id&&troopTotal(enemy)>0&&atWar(s,army.owner,enemy.owner))).sort((a,b)=>Number(b.owner===army.owner)-Number(a.owner===army.owner)||a.id.localeCompare(b.id))[0]?.id||null;
}
export function applyEncounterResult(s,result) {
  const e=requireEncounter(s);
  if(e.status==='applied'){if(result?.battleId!==e.id)fail('Result belongs to another battle.');return {ok:true,result:e.result,alreadyApplied:true};}
  const checked=validatedResult(e,result),tile=s.tiles[e.tile.id];
  // Validate every source before any write: the frozen campaign is authoritative.
  for(const p of e.participants){const a=s.armies.find(a=>a.id===p.armyId);if(!a||a.owner!==p.owner||a.tile!==p.sourceTile||Object.keys(UNITS).some(type=>(a.units[type]||0)!==(p.units[type]||0)))fail('Campaign armies changed during the pending encounter.');}
  if((tile.walls||0)!==(e.tile.walls||0)||(tile.fortIntegrity??fortMaximum(tile))!==e.tile.fortIntegrity)fail('Campaign fortifications changed during the pending encounter.');
  commitEncounterSupport(s);
  for(const row of checked.armies){const a=s.armies.find(a=>a.id===row.armyId);a.units=clone(row.units);a.morale=row.morale;a.resolvedTurn=s.turn;a.movementTurn=s.turn;a.movementSpent=effectiveArmySpeed(s,a);a.path=[];a.target=null;a.structureTarget=null;a.order='hold';delete a.battleCommitment;delete a.embarkOrder;}
  tile.walls=checked.walls;tile.fortIntegrity=checked.fortIntegrity;
  if(checked.wallSections){tile.wallSections=clone(checked.wallSections);tile.wallBreaches=clone(checked.wallBreaches||checked.wallSections.filter(w=>w.hp===0).map(w=>w.id));tile.battleDamageIntegrity={walls:checked.walls,fortIntegrity:checked.fortIntegrity};}
  else if(checked.wallBreaches)tile.wallBreaches=clone(checked.wallBreaches);
  // Retreat never inflicts a second casualty calculation. Trapped survivors
  // continue to contest occupation on the campaign board.
  for(const p of e.participants.filter(p=>p.side!==checked.winner)){
    const a=s.armies.find(a=>a.id===p.armyId),row=checked.armies.find(a=>a.armyId===p.armyId);
    if(troopTotal(a)>0&&(!p.isSupport||s.armies.some(enemy=>enemy.tile===a.tile&&troopTotal(enemy)&&atWar(s,a.owner,enemy.owner)))){
      const destination=retreatDestination(s,a,a.tile);if(destination)a.tile=destination;row.retreatDestination=destination;
    }
  }
  s.armies=s.armies.filter(a=>troopTotal(a)>0);
  const attacker=s.armies.find(a=>a.id===e.attackerId),defender=e.participants.find(p=>p.armyId===e.defenderId),winner=e.participants.find(p=>p.side===checked.winner)?.owner||null,loser=e.participants.find(p=>p.side!==checked.winner)?.owner;
  const composition=[0,1].map(side=>aggregate(e,side).units),after=[0,1].map(side=>checked.armies.filter(a=>e.participants.find(p=>p.armyId===a.armyId).side===side).reduce((n,a)=>n+troopTotal(a),0));
  const event={id:s.nextId++,battleId:e.id,turn:s.turn,attacker:e.participants[0].owner,defender:defender.owner,attackerArmyId:e.attackerId,defenderArmyId:e.defenderId,tile:tile.id,from:e.continuation.from||e.participants[0].sourceTile,action:'battle',winner,before:composition.map(units=>troopTotal({units})),after,composition,casualties:[0,1].map(side=>Object.fromEntries(Object.keys(UNITS).map(type=>[type,checked.armies.filter(a=>e.participants.find(p=>p.armyId===a.armyId).side===side).reduce((n,a)=>n+a.losses[type],0)]))),morale:[0,1].map(side=>checked.armies.find(a=>e.participants.find(p=>p.armyId===a.armyId).side===side)?.morale||.1),phases:checked.phases||[{name:'Personal command',loss:composition.map((units,i)=>troopTotal({units})-after[i]),notes:['Casualties and fortification damage supplied by the battlefield.']}],participants:clone(checked.armies),method:checked.method||'manual'};
  s.militaryEvents.push(event);s.militaryEvents=s.militaryEvents.slice(-100);
  if(winner){
    for(const [a,b] of [[winner,loser],[loser,winner]])changeRelation(s,a,b,{respect:3},'Faced this rival in battle.');
    emotionalEvent(s,loser,winner,'defeat',{key:`battle:${event.id}`,text:`Defeated our army at ${tile.name||tile.id}.`});
    emotionalEvent(s,winner,loser,'victory',{key:`battle:${event.id}`,text:`Faced a rival in battle at ${tile.name||tile.id}.`});
  }
  const occupying=checked.armies.find(a=>a.armyId===e.attackerId),occupyingTroops=Object.entries(occupying.units).reduce((n,[type,count])=>n+(UNITS[type].family==='siege'?0:count-(occupying.escaped?.[type]||0)-(occupying.undeployed?.[type]||0)),0);
  if(checked.winner===0&&attacker&&occupyingTroops>0&&checked.occupationSide!==null&&!s.armies.some(a=>a.tile===tile.id&&atWar(s,attacker.owner,a.owner))&&capture(s,attacker,tile)){attacker.tile=tile.id;checked.capture=tile.owner===attacker.owner?tile.id:null;}
  log(s,winner?`${kingdom(s,winner)?.name||winner} wins at ${tile.name||tile.id}.`:`Both armies withdraw at ${tile.name||tile.id}.`,'battle',{audience:[...new Set(e.participants.map(p=>p.owner))]});
  e.result=checked;e.status='applied';e.checkpoint=null;refreshKnowledge(s);
  return {ok:true,result:e.result};
}
export function returnFromEncounter(s) {
  const e=requireEncounter(s);if(e.status!=='applied')fail('Finish the encounter before returning to the campaign.');
  s.lastOfflineBattleId=e.id;s.offlineBattle=null;return {ok:true,resumeTurn:!!s.offlineTurn};
}
export function encounterPreview(s) {
  const e=pendingEncounter(s);if(!e)return null;
  const seen=visionTiles(s,e.playerOwner),visible=p=>p.owner===e.playerOwner||p.side===e.playerSide||seen.has(p.sourceTile);
  const preview=clone(e);preview.participants=preview.participants.filter(visible);preview.supportOptions=preview.supportOptions.filter(visible);
  const visibleIds=new Set(preview.participants.map(p=>p.armyId));preview.primaryIds=preview.primaryIds.filter(id=>visibleIds.has(id));
  if(preview.result)preview.result.armies=preview.result.armies.filter(row=>visibleIds.has(row.armyId));
  delete preview.checkpoint;delete preview.continuation;
  const estimate=automaticResult(e),side=e.playerSide;
  const yours=estimate.armies.filter(a=>e.participants.find(p=>p.armyId===a.armyId).side===side).reduce((n,a)=>n+troopTotal(e.participants.find(p=>p.armyId===a.armyId))-troopTotal(a),0),winner=estimate.winner===side?'favorable':'unfavorable';
  preview.automaticForecast={label:`Strategic automatic-resolution estimate: ${winner}, approximately ${yours} friendly losses. Personal combat can differ.`,yours,winner};
  return preview;
}
export function validateOfflineBattles(s) {
  if(!supportedOfflineCampaign(s)){if(s.offlineBattle||s.offlineTurn)fail('Online campaigns cannot contain a pending local battle.');delete s.offlineBattles;return;}
  if(s.offlineBattles&&(!['ask','auto'].includes(s.offlineBattles.preference)||typeof s.offlineBattles.enabled!=='boolean'||s.offlineBattles.tacticalTime!==undefined&&!['pause','slow','real'].includes(s.offlineBattles.tacticalTime)||s.offlineBattles.battleSize!==undefined&&!BATTLE_SIZE_OPTIONS.includes(s.offlineBattles.battleSize)))fail('Damaged offline battle preferences.');
  if(s.offlineTurn&&(!['movement','aftermath'].includes(s.offlineTurn.phase)||s.offlineTurn.turn!==s.turn))fail('Damaged suspended turn.');
  const e=s.offlineBattle;if(!e)return;
  if(e.version!==1||typeof e.id!=='string'||e.id.length>80||e.turn!==s.turn||!['pending','active','applied'].includes(e.status)||!finite(e.seed,0,4294967295)||!s.tiles[e.tile?.id]||!Array.isArray(e.participants)||e.participants.length<2||e.participants.length>500||new Set(e.participants.map(p=>p.armyId)).size!==e.participants.length||!Array.isArray(e.supportOptions)||e.supportOptions.length>500||!Array.isArray(e.primaryIds)||e.playerOwner!=='ashen'||![0,1].includes(e.playerSide)||!['move','structure'].includes(e.continuation?.kind)||e.continuation.attackerId!==e.attackerId||e.continuation.targetId!==e.tile.id||typeof e.commitmentsLocked!=='boolean')fail('Damaged pending battlefield.');
  if(!e.participants.some(p=>p.armyId===e.attackerId&&p.side===0)||!e.participants.some(p=>p.armyId===e.defenderId&&p.side===1)||!e.primaryIds.includes(e.attackerId)||!e.primaryIds.includes(e.defenderId)||e.primaryIds.some(id=>!e.participants.some(p=>p.armyId===id))||!finite(e.tile.walls,0,180)||!finite(e.tile.fortIntegrity,0,180)||e.status!=='pending'&&!e.commitmentsLocked)fail('Damaged battlefield starting position.');
  for(const p of [...e.participants,...e.supportOptions]){
    if(!kingdom(s,p.owner)||!s.tiles[p.sourceTile]||![0,1].includes(p.side)||!finite(p.morale,.1,1)||!Number.isInteger(p.distance)||!finite(p.distance,0,2)||!['east','southeast','southwest','west','northwest','northeast'].includes(p.entryDirection))fail('Damaged battlefield participant.');
    validateCounts(p.units,Object.fromEntries(Object.keys(UNITS).map(type=>[type,100000])),'starting');
    if(e.status!=='applied'&&e.participants.includes(p)){const a=s.armies.find(a=>a.id===p.armyId);if(!a||a.owner!==p.owner||a.tile!==p.sourceTile||Object.keys(UNITS).some(type=>(a.units[type]||0)!==(p.units[type]||0)))fail('Pending battlefield does not match campaign armies.');}
  }
  if(e.status==='applied')validatedResult(e,e.result);
  if(e.checkpoint&&!validateBattleCheckpoint(e.checkpoint,e))fail('Damaged battlefield checkpoint.');
  if(e.checkpoint&&JSON.stringify(e.checkpoint).length>MAX_CHECKPOINT_LENGTH)fail('Battlefield checkpoint is too large.');
  // Validate the original saved roster before migrating legacy deployment limits.
  // The same normalized checkpoint is then used by both the renderer and worker.
  if(e.checkpoint)normalizeBattleCheckpoint(e.checkpoint);
}
