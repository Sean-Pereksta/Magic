// DOM-free expedition rules. Distances use accepted tracker meters exclusively.
import {ELEMENTS,DIRECTIONS,TYPE_CHART,SPECIES,MOVES,HYBRIDS,GEAR,ITEMS,STARTERS} from './data.mjs';
import {equipmentBonuses,applyEquipmentStatPercentages,equipmentDamageMultiplier,applyEquipmentRecovery,applyEquipmentLifesteal,LEGACY_GEAR_IDS} from './equipment-effects.mjs';
import {normalizeEquipmentInventory,equipmentLoadout,equipmentSlots,assignEquipment,removeEquipment,ownedEquipmentCopies,awardGearCopy,previewGearAward,canChangeEquipment} from './equipment-inventory.mjs';
import {normalizeEquipmentTrail,equipmentDiscoveryReduction,equipmentEncounterWeight,equipmentCaptureChance,equipmentWalkingRecovery,equipmentTrainingExperience,recordEquipmentDiscovery,awardEquipmentCamp,equipmentCraftRefund,applyEquipmentCraftRefund} from './equipment-trail.mjs';
import {lockEquipmentBattle,equipmentBattleGear,equipmentBattleStats,equipmentEnter,equipmentSwitch,equipmentAttackMultiplier,equipmentApplyHit,equipmentHealingMultiplier,equipmentAfterAbility,equipmentTurnEnd,equipmentKnockout,equipmentRevivedToReserve,equipmentConsumableHealing,equipmentApplyRecovery} from './equipment-battle.mjs';
import {validateForgeCheckpoint} from './forge.mjs';
import {normalizeLands,advanceLand} from './lands.mjs';
import {recordRegionEvent} from './challenges.mjs';
import {normalizeAdventures,regionSpeciesPool,regionalWeight,regionSnapshot,advanceAdventures,recordAdventureEvent,consumeReadyHunt,guardianChallenge,speciesMinimumFeet,huntView} from './regional-adventures.mjs';
import {normalizeCompanions,recordCompanionTravel,recordCompanionVictory,recordCompanionRecruit,recordCompanionExpedition,assertDuplicateConvertible} from './companions.mjs';
import {configureGuardian,planGuardian} from './guardian-patterns.mjs';
import {recordTribeProgress} from './tribe-adventures.mjs';
import {normalizeWalkSessions,recordWalkSession} from './walk-sessions.mjs';
import {normalizeSales} from './monkey-sales.mjs';
import {recordLivingWorldBattle} from './living-world.mjs';
import {equipmentMoveCost,equipmentTacticalEvent,equipmentShieldMultiplier} from './equipment-battle.mjs';
import {normalizeLegacy,startLegacyExpedition,expeditionDepth,legacyBonuses,recordLegacyWalking,recordLegacyBattle,recordLegacyCapture,beginLegacyBattle,legacyDamage,legacyDiscoveryBonus,completeLegacyExpedition} from './legacy.mjs';
export {LEGACY_UPGRADES,LEGACY_LIMITS,normalizeLegacy,legacyView,legacyBonuses,purchaseLegacyUpgrade,expeditionRecap,expeditionDepth} from './legacy.mjs';
export {normalizeSales,salePreview,sellMonkey,transferReserve,nextReserveOperation,setMonkeyProtection} from './monkey-sales.mjs';
export {beginWalkSession,finishWalkSession,walkSessionPreview} from './walk-sessions.mjs';
export {ELEMENTS,DIRECTIONS,TYPE_CHART,SPECIES,MOVES,HYBRIDS,GEAR,ITEMS,STARTERS};
export {LANDS,landProgress,selectLand} from './lands.mjs';
export const VERSION=2, FEET=.3048, RESOURCE_STEP=250*FEET, ENCOUNTER_STEP=2000*FEET;
export const CONFIG={partySize:3,discoveryFeet:250,maxDiscoveryAcceleration:.20,encounterFeet:2000,campFeet:6000,
  walkingRecovery:{feet:100,fraction:.02},
  rarity:{common:{stats:1,speed:1,energy:0},uncommon:{stats:1.25,speed:1.03,energy:1},rare:{stats:1.5,speed:1.06,energy:2},ultra:{stats:1.85,speed:1.10,energy:3}},
  elemental:{strong:2.25,resisted:.4,min:.25,max:4,stab:1.25},
  evolution:{firstEssence:6,stats:{hp:20,atk:4,def:3,speed:2,maxEnergy:3}},
  xp:{walkingFeet:100,earlyLevels:3,earlyMultiplier:2,midLevels:5,midMultiplier:1.5,contributionHP:10,maxCombatBase:120},
  captureAttempts:3,capturePity:4,ultraFirstFeet:5000,ultraRepeatFeet:18000,ultraPityFeet:24000,
  ultraApproachMeters:250,ultraChallengeFeet:1300,maxLevel:60,fragmentCost:5,
  tutorial:{hpScales:[.60,.85],loadout:['strike','brace','breathe'],enemyCriticals:false,revivesAfterFirst:1},
  director:{bandsFeet:[4000,10000,20000],levelStepFeet:2600,rareMinFeet:10000,hybridMinFeet:10000,evolutionFeet:[8000,10000,20000],
    pairCycle:2,fullTeamCycle:3,eliteCycle:3,ultraSupportFeet:20000,ultraLevelBonus:2,ultraMinLevel:3},
  masteries:{
    leaf:{name:'Worldroot Vitality',desc:'Maximum health increases by 8%.',multipliers:{hp:1.08}},
    fire:{name:'Crownfire Resolve',desc:'Attack increases by 10%.',multipliers:{atk:1.10}},
    water:{name:'Riverguard Poise',desc:'Defense increases by 10%.',multipliers:{def:1.10}},
    storm:{name:'Skyborne Tempo',desc:'Speed increases by 12%.',multipliers:{speed:1.12}},
    earth:{name:'Bedrock Heart',desc:'Defense increases by 12%.',multipliers:{def:1.12}},
    ice:{name:'Permafrost Mantle',desc:'Maximum health increases by 6% and defense by 4%.',multipliers:{hp:1.06,def:1.04}},
    light:{name:'Radiant Focus',desc:'Maximum energy increases by 2.',flat:{maxEnergy:2}},
    shadow:{name:'Eclipse Prowess',desc:'Attack and speed each increase by 6%.',multipliers:{atk:1.06,speed:1.06}}
  },
  fatigueRound:35,discoveryWeights:{essence:.60,fruit:.25,item:.12,fragment:.03},difficulty:{relaxed:.85,standard:1,challenging:1.18}};
export const ENCOUNTER_STYLES={
  balanced:{roles:[['attacker','caster','utility'],['defender','healer'],['relay','controller']]},
  'guardian-healer':{roles:[['defender'],['healer'],['attacker','caster']]},
  'speed-control':{roles:[['relay','attacker'],['controller'],['utility']]},
  'burn-pressure':{roles:[['caster','attacker'],['drainer'],['controller']],elements:['fire','shadow','leaf']},
  'counter-switch':{roles:[['controller'],['relay','defender'],['attacker']]}
};
export const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
export function random(s){let x=s.rng|0;x^=x<<13;x^=x>>>17;x^=x<<5;s.rng=(x>>>0)||1234567;return s.rng/4294967296;}
const choose=(s,a)=>a[Math.floor(random(s)*a.length)];
const chooseSpecies=(s,ids)=>{const weight=id=>(SPECIES[id].encounterWeight??1)*regionalWeight(s,id)*equipmentEncounterWeight(s,id);const total=ids.reduce((sum,id)=>sum+weight(id),0);let roll=random(s)*total;for(const id of ids){roll-=weight(id);if(roll<0)return id;}return ids.at(-1);};
const copy=o=>JSON.parse(JSON.stringify(o));
const alive=p=>p&&p.hp>0;
const nativeTypes=id=>[...(SPECIES[id]?.types||[])];
const itemId=id=>({charm:'bondCharm',ribbon:'trailRibbon',scent:'treasureScent',feather:'scoutFeather',campSeed:'campfireSeed'})[id]||id;
export function team(s){return(s.run?.party||[]).map(id=>s.collection[id]).filter(Boolean);}
export function sanctuary(s){return Object.values(s.collection||{});}
const getMonkey=(s,id)=>id?s.collection[id]:s.run.monkey;
function syncActive(s){const r=s.run;r.monkey=s.collection[r.activeId]||s.collection[r.party[0]];r.activeId=r.monkey.id;}
function uid(s,prefix){s.meta.serial=(s.meta.serial||0)+1;return`${prefix}-${(s.rng>>>0).toString(36)}-${s.meta.serial.toString(36)}`;}
export function hybrid(types){return HYBRIDS[[...types].sort().join('+')]||null;}
export function formName(p){return`${p.stage===3?'Ascendant ':p.stage===2?'Elder ':p.stage===1?'Awakened ':''}${p.stage?(hybrid(p.types)?.name||ELEMENTS[p.types[0]]?.name||'Trail')+' ':''}${SPECIES[p.species]?.name||'Monkey'}`;}
export function effectiveness(type,defenders=[]){
  if(type==='neutral'||!type)return 1;
  const types=Array.isArray(defenders)?[...new Set(defenders)]:[];
  return clamp(types.reduce((n,t)=>n*(TYPE_CHART[type]?.[t]??1),1),CONFIG.elemental.min,CONFIG.elemental.max);
}
// A dual attack splits its power evenly instead of selecting its best matchup.
export function moveEffect(move,defenders){const types=move?.types?.length?move.types:[move?.type||'neutral'];return types.reduce((n,t)=>n+effectiveness(t,defenders),0)/types.length;}
// Existing online fights replay with the precise rules they began with.
export function moveEffectLegacy(move,defenders=[]){
  const types=move?.types?.length?move.types:[move?.type||'neutral'];
  const oldEffect=type=>!type||type==='neutral'?1:defenders.reduce((n,t)=>{const value=TYPE_CHART[type]?.[t]??1;return n*(value===CONFIG.elemental.strong?2:value===CONFIG.elemental.resisted?.5:value);},1);
  return types.reduce((n,t)=>n+oldEffect(t),0)/types.length;
}
export function effectInfo(multiplier=1){
  const value=Number.isFinite(multiplier)&&multiplier>0?multiplier:1,kind=value>1+1e-9?'strong':value<1-1e-9?'resisted':'neutral';
  return{multiplier:value,kind,label:kind==='strong'?'Very effective!':kind==='resisted'?'Not effective':'Normal effectiveness'};
}
export function stats(p,gear={},options={}){const result=calculatedStats(p,gear,false,true,options),passive=progressionPassive(p);if(passive)for(const[key,n]of Object.entries(passive.flat))result[key]+=n;return result;}
export function statsLegacy(p,gear={}){return calculatedStats(p,gear,true,false);}
export function statsForCombatVersion(p,gear={},version=4){if(version>=5)return stats(p,gear);return calculatedStats(p,gear,version===1,version>=3,{applyPercent:version>=4});}
export const legacyStats=statsLegacy;
function calculatedStats(p,gear={},legacy=false,equipmentModern=true,options={}){
  const b=SPECIES[p.species],lv=Math.max(0,p.level-1),stage=p.stage||0;
  const step=legacy?{hp:14,atk:2.4,def:1.7,speed:1,maxEnergy:2}:CONFIG.evolution.stats;
  const out={hp:b.hp+lv*8+stage*step.hp,atk:b.atk+lv*2+stage*step.atk,def:b.def+lv*1.4+stage*step.def,speed:b.speed+lv*.65+stage*step.speed,maxEnergy:12+stage*step.maxEnergy};
  if(stage&&p.types.length===1){const defensive=['leaf','water','earth','light','ice'].includes(p.types[0]);out[defensive?'def':'atk']+=stage*2;}
  // Rarity strengthens permanent growth, while equipment keeps its listed value.
  if(!legacy){const rarity=CONFIG.rarity[b.rarity]||CONFIG.rarity.common;for(const key of ['hp','atk','def'])out[key]*=rarity.stats;out.speed*=rarity.speed;out.maxEnergy+=rarity.energy;}
  for(const[key,amount]of Object.entries(equipmentBonuses(gear,{legacyOnly:!equipmentModern})))out[key]+=amount;
  if(equipmentModern&&options.applyPercent!==false)Object.assign(out,applyEquipmentStatPercentages(out,gear,{p,...options,hpMaximum:options.includeConditional?calculatedStats(p,gear,legacy,equipmentModern,{applyPercent:true}).hp:out.hp}));
  const mastery=stage===3?CONFIG.masteries[p.mastery]:null;if(mastery){for(const[key,factor]of Object.entries(mastery.multipliers||{}))out[key]*=factor;for(const[key,amount]of Object.entries(mastery.flat||{}))out[key]+=amount;}
  if(p.elite)for(const key of['hp','atk','def'])out[key]*=1.12;if(p.bossOnly)out.hp*=1.45;
  if(Number.isFinite(p.tutorialHpScale))out.hp*=p.tutorialHpScale;
  // Expedition-only crew preparation is deliberately absent from social copies.
  if(p.trailCrew&&!legacy){out.hp+=Math.min(8,Math.max(0,Number(p.trailCrew.hp)||0));out.maxEnergy+=Math.min(2,Math.max(0,Number(p.trailCrew.maxEnergy)||0));}
  for(const k in out)out[k]=Math.round(out[k]);return out;
}
export function equipped(s,p=s.run.monkey){const b=s.run?.battle;return b?.equipmentBattle?.units?.[p?.equipmentBattleKey]?.gear||equipmentLoadout(s,p);}
export const playerStats=s=>stats(s.run.monkey,equipped(s));
const combatGear=(s,p)=>s.run?.battle?.equipmentBattle?.units?.[p?.equipmentBattleKey]?.gear||(s.collection[p.id]?equipped(s,p):p.gear||{});
const battleStats=(s,p)=>equipmentBattleStats(s.run.battle,p,stats(p,combatGear(s,p)),s.collection[p.id]?team(s):s.run.battle.enemies);
function ensureEquipmentBattle(s){const b=s.run.battle;if(!b)return;lockEquipmentBattle(b,[team(s),b.enemies],p=>combatGear(s,p),(p,gear)=>stats(p,gear));}
function beginEquipmentBattle(s){ensureEquipmentBattle(s);const b=s.run.battle;if(b.equipmentStarted)return;b.equipmentStarted=true;equipmentEnter(b,s.run.monkey,team(s));equipmentEnter(b,b.enemy,b.enemies);}
function processEquipmentKnockouts(s){const b=s.run.battle;if(!b)return;for(const members of[team(s),b.enemies])for(const p of members){equipmentKnockout(b,p,members,{active:p.id===s.run.activeId||p.id===b.enemy.id});if(equipmentRevivedToReserve(b,p)){const other=members.find(q=>q.id!==p.id&&alive(q));if(other&&(p.id===s.run.activeId||p.id===b.enemy.id)){const side=s.collection[p.id]?'player':'enemy';equipmentSwitch(b,p,other,members,{voluntary:false});enter(s,other,side);}}}}
export function unlocked(p){
  const curated=new Set(['strike','brace','breathe',...(SPECIES[p.species]?.learnset||[]),...(p.learned||[])]);
  if(STARTERS.includes(p.species))for(const type of p.types){let n=0;for(const[id,m]of Object.entries(MOVES))if(m.type===type&&!m.types&&!m.signature&&n++<2)curated.add(id);}
  for(const[id,m]of Object.entries(MOVES))if(m.family==='progression'&&(m.type==='neutral'||p.types.includes(m.type)))curated.add(id);
  return[...curated].filter(id=>{const m=MOVES[id];return m&&(p.stage||0)>=(m.minStage||0)&&(p.level||1)>=(m.minLevel||1)&&(!m.signature||m.owner===p.species||SPECIES[p.species]?.signature===id||SPECIES[p.species]?.progressionSignature===id)&&(!(m.expansion===3&&m.family==='progression'&&m.type!=='neutral')||p.types.includes(m.type));});
}
export function abilityCatalogue(p){const species=SPECIES[p.species],ids=new Set(['strike','brace','breathe',...(species?.learnset||[]),...(p.learned||[])]),types=new Set([...(species?.types||[]),...(p.types||[]),...(p.highestForm?.types||[])]);if(STARTERS.includes(p.species))for(const t of Object.keys(ELEMENTS))types.add(t);for(const[id,m]of Object.entries(MOVES))if(m.family==='progression'&&(m.type==='neutral'||types.has(m.type)))ids.add(id);return [...ids].filter(id=>MOVES[id]).map(id=>abilityInfo(p,id));}
export function syncProgression(p){p.learned=[...new Set([...(p.learned||[]),...unlocked(p)])];p.highestLevel=Math.max(p.highestLevel||1,p.level||1);return p;}
export function progressionPassive(p){if(!p?.stage)return null;const type=p.mastery||p.types?.[0],stage=Math.min(2,p.stage),bonus={leaf:{hp:3*stage},fire:{atk:stage},water:{def:stage},storm:{speed:stage},earth:{def:stage+1},ice:{hp:2*stage,def:stage},light:{maxEnergy:stage},shadow:{atk:stage,speed:stage}}[type];return bonus?{name:`${ELEMENTS[type].name} ${stage===1?'Affinity':'Specialization'}`,flat:bonus,desc:Object.entries(bonus).map(([k,v])=>`+${v} ${k==='maxEnergy'?'energy':k}`).join(', ')}:null;}
export function progressionMilestones(p){return[3,6,10,20,30,40,50,60].map(level=>({level,stage:level===3?1:level===6?2:3,name:({3:'Awakened',6:'Elder',10:'Ascendant',20:'Tactician',30:'Species mastery',40:'Elemental apex',50:'Legendary',60:'Final mastery'})[level],moves:abilityCatalogue(p).filter(a=>a.requiredLevel===level),active:p.level>=level&&p.stage>=(level===3?1:level===6?2:3)}));}
// UI cards and the equip action share this catalogue/stage eligibility. Learned
// techniques survive evolution; Ultra signatures remain exclusive to their owner.
export function abilityInfo(p,id){
  const move=MOVES[id];if(!p||!move)return null;const species=SPECIES[p.species],requiredStage=move.minStage||0,requiredLevel=move.minLevel||1,eligible=unlocked(p).includes(id),equippedSlot=p.loadout?.indexOf(id)??-1,requirement=`Requires ${['Base','Awakened','Elder','Ascendant'][requiredStage]} + Level ${requiredLevel}`;
  let lockedReason=null;if(!eligible){
    if(move.signature&&move.owner!==p.species&&species?.signature!==id&&species?.progressionSignature!==id){const owner=SPECIES[move.owner]||Object.values(SPECIES).find(s=>s.signature===id);lockedReason=`Exclusive to ${owner?.name||'its species'}.`;}
    else if((p.stage||0)<requiredStage||p.level<requiredLevel)lockedReason=requirement;
    else if(move.expansion===3&&move.family==='progression'&&move.type!=='neutral'&&!p.types.includes(move.type))lockedReason=`Requires an active ${ELEMENTS[move.type]?.name} evolution branch.`;
    else lockedReason='Outside this monkey’s curated learnset.';
  }
  return{id,move,eligible,requiredStage,requiredLevel,requirement,discovered:(p.learned||[]).includes(id)||eligible,lockedReason,required:id==='strike',equippedSlot};
}
export function say(s,text){s.log.push(text);s.log=s.log.slice(-16);}
function monkey(s,species,level=1,types=nativeTypes(species),stage=0,prefix='m'){
  const p={id:uid(s,prefix),species,nickname:'',level,types:[...types],stage,hp:1,energy:12,status:{},statusPower:{},
    loadout:['strike','brace','breathe'],training:0,xpRemainder:0,bond:0,learned:[],discovered:[],highestForm:{stage:0,types:nativeTypes(species)},
    equipment:{weapon:null,armor:null,charm:null},cooldowns:{},uses:{}};
  p.equipment=Object.fromEntries(equipmentSlots(p).map(slot=>[slot,null]));syncProgression(p);const specials=p.learned.filter(id=>!['strike','brace','breathe'].includes(id));
  p.loadout=['strike',...specials.slice(0,2),'brace'];if(p.loadout.length<4)p.loadout.splice(p.loadout.length-1,0,'breathe');const maximum=stats(p);p.hp=maximum.hp;p.energy=maximum.maxEnergy;return p;
}
function resetExpeditionMonkey(p){syncProgression(p);delete p.charging;delete p.evasion;delete p.trailCrew;p.healingMomentum=0;p.level=1;p.types=nativeTypes(p.species);p.stage=0;p.status={};p.statusPower={};p.training=0;p.xpRemainder=0;delete p.statusCredit;p.cooldowns={};p.uses={};p.shield=0;delete p.elite;delete p.bossOnly;delete p.mastery;const available=unlocked(p);p.loadout=['strike',...p.loadout.filter(id=>id!=='strike'&&available.includes(id)),...available].filter((id,i,a)=>a.indexOf(id)===i).slice(0,4);p.hp=stats(p).hp;p.energy=stats(p).maxEnergy;}
export function newSave(name,species='capuchin',mode='gps',seed=Date.now()>>>0){
  if(!STARTERS.includes(species))throw Error('Choose one of the six starters.');
  const s={version:VERSION,name:String(name).slice(0,30),mode,rng:(seed>>>0)||1234567,difficulty:'standard',collection:{},
    meta:{runs:0,legacy:0,best:0,serial:0,gear:{},fragments:{},equipped:{weapon:null,armor:null,charm:null},history:[],claims:{},captureFailures:0,bondMaterials:{}},run:null,log:[]};
  const p=monkey(s,species);s.collection[p.id]=p;restart(s,species);return s;
}
export function restart(s,species=s.run?.monkey.species||'capuchin'){
  if(s.run&&s.run.phase!=='dead')throw Error('Your expedition is still active.');
  normalizeLegacy(s);
  normalizeLands(s);
  const oldParty=s.run?.party?.filter(id=>s.collection[id])||[Object.keys(s.collection)[0]];for(const p of sanctuary(s))resetExpeditionMonkey(p);
  s.meta.runs++;const id=uid(s,'run'),bonus=Math.min(20,s.meta.legacy||0);
  s.run={id,phase:'trail',distance:0,nextCache:RESOURCE_STEP,nextEncounter:ENCOUNTER_STEP,nextCamp:CONFIG.campFeet*FEET,
    discoveryProgress:0,discoveries:0,wins:0,encounters:0,ordinaryWins:0,party:oldParty.slice(0,3),activeId:oldParty[0],monkey:null,
    fruit:8+bonus,essence:Object.fromEntries(Object.keys(ELEMENTS).map(t=>[t,0])),
    items:Object.fromEntries(Object.keys(ITEMS).map(id=>[id,({berry:2,tonic:1,salve:1})[id]||0])),
    vector:{east:0,north:0},weights:{N:0,S:0,E:0,W:0},direction:'N',battle:null,pendingEncounter:null,capture:null,camp:null,events:[],
    bonuses:{ribbon:0,scent:0,feather:0,campSeed:0},injuries:{},ultra:null,nextUltra:CONFIG.ultraFirstFeet*FEET,startedAt:Date.now()};
  s.run.items.charmMaterial=0;const legacyStart=startLegacyExpedition(s);s.run.nextEncounter=Math.max(250,CONFIG.encounterFeet-legacyStart.startingFeet)*FEET;syncActive(s);normalizeWalkSessions(s);normalizeCompanions(s);normalizeAdventures(s);for(const p of sanctuary(s)){const maximum=stats(p,equipped(s,p));p.hp=maximum.hp;p.energy=maximum.maxEnergy;}
  s.log=[];say(s,`Expedition ${s.meta.runs}: choose up to three companions before tracking. Your sanctuary and equipment endure.`);return s;
}
export function direction(east,north,fallback='N'){if(Math.hypot(east,north)<2)return fallback;return Math.abs(north)>=Math.abs(east)?north>=0?'N':'S':east>=0?'E':'W';}
export function odds(s){const w=s.run.weights,total=Object.values(w).reduce((a,b)=>a+b,0);return Object.fromEntries(Object.keys(w).map(d=>[DIRECTIONS[d],total?.1+.6*w[d]/total:.25]));}
function rollElement(s){if(random(s)<.22)return choose(s,['earth','ice','light','shadow']);let n=random(s);for(const[type,p]of Object.entries(odds(s))){n-=p;if(n<=0)return type;}return'leaf';}
export function discoveryInterval(s){const reduction=Math.min(CONFIG.maxDiscoveryAcceleration,equipmentDiscoveryReduction(s)+(s.run.bonuses.ribbon>0?.15:0));return CONFIG.discoveryFeet*FEET*(1-reduction);}
function updateNextCache(s){s.run.nextCache=s.run.distance+(1-s.run.discoveryProgress)*discoveryInterval(s);}
function cache(s){
  const r=s.run;r.discoveries++;let roll=random(s);if(r.bonuses.scent>0){r.bonuses.scent--;roll=Math.min(.999,roll+.06);}const w=CONFIG.discoveryWeights,instinct=legacyDiscoveryBonus(s);if(instinct&&roll>=w.essence+w.fruit-instinct&&roll<w.essence+w.fruit)roll=w.essence+w.fruit+(roll-(w.essence+w.fruit-instinct));
  if(roll<w.essence){const type=random(s)<.22+instinct?choose(s,['earth','ice','light','shadow']):DIRECTIONS[r.direction];r.essence[type]++;say(s,`Discovery: one ${ELEMENTS[type].name} essence.`);r.lastDiscovery={category:'essence',id:type,amount:1};}
  else if(roll<w.essence+w.fruit){r.fruit+=2;say(s,'Discovery: two training fruit.');r.lastDiscovery={category:'fruit',amount:2};}
  else if(roll<w.essence+w.fruit+w.item){const id=choose(s,['berry','tonic','salve','bondCharm','trailRibbon','treasureScent','scoutFeather','campfireSeed']);r.items[id]++;say(s,`Discovery: ${ITEMS[id].name}.`);r.lastDiscovery={category:'item',id,amount:1};}
  else{const id=choose(s,LEGACY_GEAR_IDS);s.meta.fragments[id]=(s.meta.fragments[id]||0)+1;say(s,`Discovery: a ${GEAR[id].name} fragment (${s.meta.fragments[id]}/${CONFIG.fragmentCost}).`);r.lastDiscovery={category:'fragment',id,amount:1};}
  recordEquipmentDiscovery(s,r.lastDiscovery);recordRegionEvent(s,'discoveries');recordAdventureEvent(s,'discoveries');recordTribeProgress(s,{discoveries:1});recordWalkSession(s,{discoveries:1,landId:s.world.activeLand});
}
function acceptedDiscoveries(s,meters,onDiscovery){
  const r=s.run;let remaining=meters;while(remaining>1e-8){const interval=discoveryInterval(s),untilDiscovery=(1-r.discoveryProgress)*interval,untilBonus=r.bonuses.ribbon>0?r.bonuses.ribbon:Infinity;
    const chunk=Math.min(remaining,untilDiscovery,untilBonus);r.discoveryProgress+=chunk/interval;remaining-=chunk;if(r.bonuses.ribbon>0)r.bonuses.ribbon=Math.max(0,r.bonuses.ribbon-chunk);
    if(r.discoveryProgress>=1-1e-9){r.discoveryProgress=Math.max(0,r.discoveryProgress-1);cache(s);onDiscovery?.({...r.lastDiscovery});}if(chunk<1e-10&&r.discoveryProgress<1-1e-9)break;
  }
}
export function activateBonus(s,id){id=itemId(id);const r=s.run;if(!['trail','camp'].includes(r.phase)||!(r.items[id]>0))throw Error('That walking supply is unavailable.');
  if(!['trailRibbon','treasureScent','scoutFeather','campfireSeed'].includes(id))throw Error('Choose a walking bonus.');r.items[id]--;
  if(id==='trailRibbon')r.bonuses.ribbon=Math.max(r.bonuses.ribbon,1000*FEET);if(id==='treasureScent')r.bonuses.scent=Math.min(6,r.bonuses.scent+3);if(id==='scoutFeather'){if(r.ultra&&r.ultra.status!=='claimed'){r.ultra.scouted=true;r.ultra.revealed=true;}else r.bonuses.feather=1;}if(id==='campfireSeed')r.bonuses.campSeed=1;updateNextCache(s);return r.bonuses;
}
function maybeUltra(s){
  const r=s.run;if(r.ultra&&r.ultra.status!=='claimed'||r.ordinaryWins<2||r.distance+1e-6<r.nextUltra)return;
  const pool=regionSpeciesPool(s,Object.keys(SPECIES).filter(id=>(SPECIES[id].rarity==='ultra'||SPECIES[id].ultra)&&r.distance/FEET+1e-6>=speciesMinimumFeet(id)),{ultra:true});if(!pool.length)return;
  const species=choose(s,pool),plan=encounterPlan(s,true);if(MOVES[SPECIES[species].signature]?.switch)plan.size=Math.max(2,plan.size);
  r.ultra={id:uid(s,'ultra'),species,...regionSnapshot(s),difficulty:plan.level,plan,bearing:Math.round(random(s)*359),scouted:!!r.bonuses.feather,revealed:!!r.bonuses.feather,
    spawnDistance:CONFIG.ultraApproachMeters,challengeRemaining:CONFIG.ultraChallengeFeet*FEET,tracked:false,status:'sighting',safeAlternative:false,claimed:false};r.bonuses.feather=0;
  say(s,`Ultra sighting: ${SPECIES[species].name}. Track it or choose your own safe route; there is no countdown.`);
}
function enqueue(s,event){if(!s.run.events.some(e=>e.id===event.id))s.run.events.push(event);}
function pump(s){
  const r=s.run;if(r.phase!=='trail'||r.capture)return;
  // Ordinary encounters become one durable invitation. Process a coincident
  // camp/explicit Ultra too, without stacking a queue of ordinary battles.
  r.events.sort((a,b)=>({encounter:0,ultra:1,camp:2}[a.type]??3)-({encounter:0,ultra:1,camp:2}[b.type]??3));
  while(r.events.length&&r.phase==='trail'){
    const event=r.events.shift();
    if(event.type==='encounter'){if(!r.pendingEncounter)encounter(s,false,{defer:true});}
    else if(event.type==='ultra')encounter(s,true);
    else if(event.type==='camp'){
      r.phase='camp';r.camp={id:event.id,heals:2+(r.bonuses.campSeed?1:0),seedBoost:!!r.bonuses.campSeed,fullRecovery:true,recoveredMonkeys:sanctuary(s).length};
      awardEquipmentCamp(s,event.id);
      // Arrival is the recovery event. Reopening an existing camp/checkpoint
      // never runs this branch again and cannot repeatedly heal equipped HP.
      for(const p of sanctuary(s))p.hp=stats(p,equipped(s,p)).hp;
      r.injuries={};r.bonuses.campSeed=0;say(s,'Trail camp: every collected monkey recovered full health, including fallen companions. Camp crafting and fruit exchange bonuses are available.');
    }
  }
  if(r.phase==='trail')maybeUltra(s);
}
export function advance(s,meters,east=0,north=0,onDiscovery){
  if(s.run.phase!=='trail'||![meters,east,north].every(Number.isFinite)||meters<=0)return 0;
  let accepted=0;
  while(accepted<meters-1e-8&&s.run.phase==='trail'){
    const remaining=meters-accepted,before=s.run.pendingEncounter,used=advanceSegment(s,remaining,east*remaining/meters,north*remaining/meters,onDiscovery);accepted+=used;
    // A newly prepared invitation is nonblocking: retain the rest of this
    // accepted GPS sample. A camp or actual fight still ends consumption.
    if(used<=1e-8&&before===s.run.pendingEncounter)break;
  }return accepted;
}
function advanceSegment(s,meters,east=0,north=0,onDiscovery){
  const r=s.run;if(r.phase!=='trail'||![meters,east,north].every(Number.isFinite)||meters<=0)return 0;
  const next=Math.min(r.pendingEncounter?Infinity:r.nextEncounter,r.nextCamp),used=Math.max(0,Math.min(meters,next-r.distance)),ratio=used/meters;
  advanceLand(s,used,east*ratio,north*ratio);advanceAdventures(s,used);const friendship=recordCompanionTravel(s,used,s.world.activeLand);recordTribeProgress(s,{meters:used});
  let walkingXp=0;
  for(const p of team(s)){
    walkingXp+=grantXp(p,used/(CONFIG.xp.walkingFeet*FEET));
    // Fractional HP preserves exactly 2% per 100 feet, even for low-HP species.
    if(alive(p)){const max=stats(p,equipped(s,p)).hp;p.hp=Math.min(max,p.hp+max*equipmentWalkingRecovery(s,p,CONFIG.walkingRecovery.fraction)*used/(CONFIG.walkingRecovery.feet*FEET));}
  }
  r.vector.east+=east*ratio;r.vector.north+=north*ratio;r.direction=direction(r.vector.east,r.vector.north,r.direction);r.weights[r.direction]+=used;recordWalkSession(s,{distance:used,trainingXp:walkingXp,friendship,landId:s.world.activeLand});acceptedDiscoveries(s,used,onDiscovery);r.distance+=used;recordLegacyWalking(s,used);
  if(r.ultra?.safeAlternative&&r.ultra.status==='challenge'){r.ultra.challengeRemaining=Math.max(0,r.ultra.challengeRemaining-used);if(r.ultra.challengeRemaining<=1e-7){r.ultra.status='ready';say(s,'Your safe-route Ultra challenge is complete. Stop somewhere safe and begin when ready.');}}
  if(!r.pendingEncounter&&r.distance+1e-6>=r.nextEncounter){const milestone=r.nextEncounter;enqueue(s,{type:'encounter',id:`encounter-${milestone}`});r.nextEncounter+=CONFIG.encounterFeet*FEET;}
  if(r.distance+1e-6>=r.nextCamp){const milestone=r.nextCamp;enqueue(s,{type:'camp',id:`camp-${milestone}`});r.nextCamp+=CONFIG.campFeet*FEET;}
  updateNextCache(s);maybeUltra(s);pump(s);return used;
}
export function encounterPool(s,ultraSupport=false){const feet=expeditionDepth(s)/FEET,d=CONFIG.director;return regionSpeciesPool(s,Object.keys(SPECIES).filter(id=>SPECIES[id].rarity!=='ultra'&&!SPECIES[id].ultra&&(ultraSupport||((!SPECIES[id].expansion||feet>=SPECIES[id].minFeet)&&(SPECIES[id].rarity!=='rare'||feet>=d.rareMinFeet)&&(nativeTypes(id).length<2||feet>=d.hybridMinFeet)))));}
export function encounterPlan(s,ultra=false){
  const feet=expeditionDepth(s)/FEET,index=s.run.encounters,scale=CONFIG.difficulty[s.difficulty]||1,d=CONFIG.director,[early,middle,deep]=d.bandsFeet;
  const size=feet<early?1:feet<middle?(index%d.pairCycle?2:1):feet<deep?(index%d.fullTeamCycle===0?3:2):3,
    level=clamp(Math.round((1+Math.floor(feet/d.levelStepFeet))*scale),1,CONFIG.maxLevel),stage=d.evolutionFeet.filter(threshold=>feet>=threshold).length;
  return{size:ultra?(feet>=d.ultraSupportFeet?2:1):size,level:ultra?clamp(Math.max(d.ultraMinLevel,level+d.ultraLevelBonus),1,CONFIG.maxLevel):level,stage,style:choose(s,Object.keys(ENCOUNTER_STYLES)),elite:feet>=deep&&index%d.eliteCycle===0};
}
function encounter(s,isUltra=false,{defer=false,descriptor=null}={}){
  const r=s.run;if(isUltra&&!descriptor)descriptor={species:r.ultra.species,landId:r.ultra.landId||s.world.activeLand,habitat:r.ultra.habitat||regionSnapshot(s).habitat,...(r.ultra.hunt?{hunt:true}:{})};r.encounters++;const plan=isUltra&&r.ultra.plan?r.ultra.plan:encounterPlan(s,isUltra),pool=encounterPool(descriptor?.landId&&descriptor.landId!==s.world.activeLand?{...s,world:{...s.world,activeLand:descriptor.landId}}:s,isUltra),enemies=[];
  if(!descriptor&&!isUltra&&r.startingDepth>0&&r.ordinaryWins<2){plan.size=1;plan.level=Math.min(plan.level,Math.max(1,Math.round(team(s).reduce((n,p)=>n+p.level,0)/team(s).length)));plan.stage=0;plan.elite=false;}
  for(let i=0;i<plan.size;i++){
    const style=ENCOUNTER_STYLES[plan.style],roles=style.roles[i%style.roles.length],rolePool=pool.filter(id=>roles.includes(SPECIES[id].role)),themed=rolePool.filter(id=>!style.elements||nativeTypes(id).some(t=>style.elements.includes(t)));
    let species=descriptor&&i===0?descriptor.species:isUltra&&i===0?r.ultra.species:chooseSpecies(s,themed.length?themed:rolePool.length?rolePool:pool);if(!descriptor&&!isUltra&&r.ordinaryWins<2)species=r.ordinaryWins===0?'mossmop':'cinderpaw';
    const tutorial=r.ordinaryWins<2&&!isUltra&&!descriptor,stage=tutorial?0:plan.stage,p=monkey(s,species,plan.level,nativeTypes(species),stage,'enemy');p.elite=plan.elite;p.bossOnly=isUltra&&i===0;
    if(tutorial)p.tutorialHpScale=CONFIG.tutorial.hpScales[r.ordinaryWins];p.hp=stats(p).hp;p.energy=stats(p).maxEnergy;
    const moves=unlocked(p).filter(id=>!['strike','brace','breathe'].includes(id));
    const priority=m=>MOVES[m].signature?100:SPECIES[species].role==='healer'?(MOVES[m].teamHeal&&plan.size>1?90:MOVES[m].heal?70:0):SPECIES[species].role==='defender'?(MOVES[m].teamShield&&plan.size>1?85:MOVES[m].shield||MOVES[m].counter?65:0):SPECIES[species].role==='relay'?(MOVES[m].switch?60:0):SPECIES[species].role==='controller'?(MOVES[m].root?60:0):0;
    const ranked=moves.sort((a,b)=>priority(b)-priority(a)||(MOVES[b].power||18)-(MOVES[a].power||18));
    // One defining utility, one elemental strike, and an energy recovery action.
    const first=ranked[0],attack=ranked.find(id=>MOVES[id].power&&id!==first)||ranked[1];p.loadout=tutorial?[...CONFIG.tutorial.loadout]:['strike',first,attack,'breathe'].filter((id,index,a)=>id&&a.indexOf(id)===index);enemies.push(p);
  }
  const battle={id:isUltra?r.ultra.id:descriptor?.id||uid(s,'encounter'),originLandId:descriptor?.landId||s.world.activeLand,habitat:descriptor?.habitat||regionSnapshot(s).habitat,originDistance:r.distance,difficulty:s.difficulty,plan:copy(plan),...(descriptor?.hunt?{hunt:true}:{}),...(descriptor?.guardian?{guardian:true}:{}),enemy:enemies[0],enemies,enemyActive:0,turn:1,round:1,ready:false,boss:isUltra,ultra:isUltra,style:plan.style,intent:'strike',enemyAction:{type:'move',id:'strike'},rewarded:false,bondBonuses:{},pendingRally:null,enemyRally:null};
  if(descriptor?.worldLocationId){battle.worldLocationId=descriptor.worldLocationId;battle.worldLocationStage=descriptor.worldLocationStage;}
  if(descriptor?.guardian)configureGuardian(battle,battle.originLandId);
  if(descriptor?.guardian)battle.guardianName=descriptor.name;
  if(defer){r.pendingEncounter=battle;say(s,`Encounter ready: ${enemies.map(p=>formName(p)).join(' · ')}. Keep walking and choose when to battle.`);return battle;}
  activateBattle(s,battle);return battle;
}
function activateBattle(s,battle){
  normalizeEquipmentInventory(s);
  beginLegacyBattle(s,battle);
  const r=s.run;r.battle=battle;battle.openingLocked=false;battle.openingLeadId=r.activeId;for(const p of team(s)){p.cooldowns={};p.uses={};p.healingMomentum=0;delete p.charging;delete p.evasion;}r.phase='battle';ensureEquipmentBattle(s);if(battle.ultra)r.ultra.status='battle';planEnemy(s);say(s,`${battle.ultra?'Ultra arrival':battle.guardian?'Regional guardian':'Wild team'}: ${battle.enemies.map(p=>formName(p)).join(' · ')}. Stop safely before beginning.`);
}
export function beginPendingEncounter(s){
  const r=s.run;if(r.phase!=='trail'||!r.pendingEncounter)throw Error('Return to the trail and choose an available encounter.');const battle=r.pendingEncounter;r.pendingEncounter=null;activateBattle(s,battle);return battle;
}
export function passPendingEncounter(s){
  const r=s.run;if(!['trail','camp'].includes(r.phase)||!r.pendingEncounter)throw Error('There is no waiting ordinary encounter to pass.');const passed=r.pendingEncounter;r.pendingEncounter=null;r.nextEncounter=r.distance+ENCOUNTER_STEP;r.events=r.events.filter(e=>e.type!=='encounter');say(s,'Encounter passed. No battle rewards were earned; the next invitation follows the normal walking interval.');return passed;
}
export function beginLocationBattle(s,descriptor){if(s.run.phase!=='trail'||!descriptor?.id||!SPECIES[descriptor.species]||!s.world.unlocked.includes(descriptor.landId))throw Error('Choose a ready special-location trial on the trail.');return encounter(s,false,{descriptor});}
export function beginRegionalGuardian(s,landId){
  if(s.run.phase!=='trail')throw Error('Return to the trail before challenging a guardian.');const descriptor=guardianChallenge(s,landId);if(s.meta.claims[`reward:${descriptor.id}`])throw Error('This guardian is already defeated. Claim the adventure reward.');return encounter(s,false,{descriptor});
}
export function startRegionalHunt(s){
  const r=s.run;if(r.phase!=='trail')throw Error('Return to the trail before beginning the hunt.');if(huntView(s)?.rarity==='ultra'&&r.ultra&&r.ultra.status!=='claimed')throw Error('Resolve the current Ultra sighting before starting another targeted Ultra hunt.');const descriptor=consumeReadyHunt(s);if(!descriptor)throw Error('Finish the research and required regional progression first.');
  if(!descriptor.ultra)return encounter(s,false,{descriptor});
  const plan=encounterPlan(s,true);if(MOVES[SPECIES[descriptor.species].signature]?.switch)plan.size=Math.max(2,plan.size);r.ultra={...descriptor,id:uid(s,'ultra-hunt'),difficulty:plan.level,plan,bearing:Math.round(random(s)*359),scouted:true,revealed:true,spawnDistance:CONFIG.ultraApproachMeters,challengeRemaining:CONFIG.ultraChallengeFeet*FEET,tracked:true,status:'sighting',safeAlternative:false,claimed:false};say(s,`Research revealed ${SPECIES[descriptor.species].name}. Track the sighting or use the safe-route challenge.`);return r.ultra;
}
export const beginRegionalHunt=startRegionalHunt;
export function trackUltra(s){if(!s.run.ultra||s.run.ultra.status==='claimed')throw Error('No current Ultra sighting.');s.run.ultra.tracked=true;return s.run.ultra;}
export function safeUltraAlternative(s){const u=trackUltra(s);if(u.status==='battle')throw Error('Finish this battle first.');u.safeAlternative=true;u.status=u.challengeRemaining<=0?'ready':'challenge';say(s,'Safe route selected: the same Ultra awaits after additional accepted walking distance.');return u;}
export function beginUltra(s,{gpsFresh=false,withinRadius=false}={}){
  const r=s.run,u=r.ultra;if(!u||u.claimed||!['sighting','challenge','ready'].includes(u.status))throw Error('That Ultra is unavailable.');if(u.safeAlternative?u.challengeRemaining>1e-6:!(gpsFresh&&withinRadius))throw Error(u.safeAlternative?'Finish the accepted-distance challenge first.':'Approach with fresh acceptable GPS, or choose the safe-route alternative.');
  u.tracked=true;enqueue(s,{type:'ultra',id:u.id});if(r.phase==='trail')pump(s);return r.phase==='battle'&&r.battle.ultra;
}
function planEnemy(s){
  const b=s.run.battle,p=s.run.monkey,e=b.enemy;if(!e)return;b.phase='intent';if(e.charging){b.enemyAction={type:'move',id:e.charging.resolveRound<=b.round?e.charging.id:'brace',release:e.charging.resolveRound<=b.round};b.intent=b.enemyAction.id;return;}const tactical=planGuardian(s);if(tactical){b.enemyAction={...tactical};b.intent=tactical.id;b.tacticalIntent=tactical;return;}delete b.tacticalIntent;const options=e.loadout.filter(id=>MOVES[id]&&equipmentMoveCost(b,e,MOVES[id])<=e.energy&&moveAvailable(b,e,id)&&(!MOVES[id].switch||!e.status.root&&b.enemies.some(m=>m.id!==e.id&&alive(m))));
  let id=p.charging?options.find(id=>MOVES[id].interrupt):null;
  if(!id&&p.shield)id=options.find(id=>MOVES[id].breakShield||MOVES[id].feint);
  if(!id)id=options.find(id=>MOVES[id].cleanse&&Object.keys(e.status).some(k=>['burn','poison','weaken','root'].includes(k)));
  if(!id)id=options.find(id=>MOVES[id].heal&&e.hp<stats(e,combatGear(s,e)).hp*.40);
  if(!id)id=options.find(id=>MOVES[id].teamHeal&&b.enemies.some(m=>alive(m)&&m.hp<stats(m,combatGear(s,m)).hp*.55));
  if(!id)id=options.find(id=>MOVES[id].shield&&!e.shield&&e.hp<stats(e,combatGear(s,e)).hp*.65&&random(s)<.6);
  if(!id)id=options.find(id=>MOVES[id].root&&team(s).filter(alive).length>1&&!p.status.root&&random(s)<.55);
  if(!id)id=options.find(id=>MOVES[id].teamShield&&MOVES[id].switch&&e.hp<stats(e,combatGear(s,e)).hp*.45);
  if(!id&&e.energy<3&&options.includes('breathe'))id='breathe';
  if(!id){const ranked=options.filter(id=>MOVES[id].power).sort((a,c)=>MOVES[c].power*moveEffect(MOVES[c],p.types)-MOVES[a].power*moveEffect(MOVES[a],p.types));id=(random(s)<.8?ranked[0]:choose(s,ranked.slice(0,3)))||'strike';}
  const choices=b.enemies.filter(m=>m.id!==e.id&&alive(m));if(!e.status.root&&choices.length&&(e.hp<stats(e,combatGear(s,e)).hp*.23||moveEffect(MOVES[id],p.types)<.8)&&random(s)<.35)b.enemyAction={type:'switch',id:choices[0].id};else b.enemyAction={type:'move',id};b.intent=b.enemyAction.type==='switch'?'brace':id;
}
export function canChooseBattleLead(s){
  const r=s.run,b=r?.battle;return !!r&&(['trail','camp'].includes(r.phase)||r.phase==='battle'&&!!b&&!b.ready&&b.openingLocked===false&&b.round===1&&b.itemUsedRound==null);
}
export function selectBattleLead(s,id){
  if(!canChooseBattleLead(s))throw Error('Choose your first monkey before battle begins. Switching during battle uses a turn.');
  const p=team(s).find(p=>p.id===id);if(!p||!alive(p))throw Error('Choose a conscious monkey from your current team.');
  s.run.activeId=p.id;syncActive(s);return p;
}
export function readyBattle(s){
  const r=s.run,b=r.battle;if(r.phase!=='battle')return;
  if(!alive(r.monkey))throw Error('Choose a conscious monkey before beginning battle.');
  // The opening choice is free once. Reloading pauses input without reopening it.
  if(b.openingLocked===false&&b.openingLeadId!==r.activeId)planEnemy(s);
  beginEquipmentBattle(s);b.openingLocked=true;b.ready=true;
}
export function score(s){const r=s.run;if(r.phase==='dead'&&Number.isFinite(r.finalScore))return r.finalScore;return Math.floor(r.distance/FEET+1e-6)+r.wins*500+team(s).reduce((n,p)=>n+p.stage*300+(p.level-1)*40,0);}
function endRun(s){
  const r=s.run;if(r.phase==='dead')return;recordCompanionExpedition(s);const points=score(s),active=r.monkey,earned=Math.max(1,Math.floor(r.wins/2)+Math.floor(r.distance/(4000*FEET)));
  s.meta.legacy=Math.min(100,s.meta.legacy+earned);s.meta.best=Math.max(s.meta.best,points);completeLegacyExpedition(s,{score:points,level:active.level,form:formName(active)});
  r.finalScore=points;r.phase='dead';r.pendingEncounter=null;r.battle=null;r.capture=null;r.events=[];r.fruit=0;r.forgeMaterials={};for(const k in r.essence)r.essence[k]=0;for(const k in r.items)r.items[k]=0;
  for(const p of sanctuary(s)){resetExpeditionMonkey(p);p.hp=0;}say(s,`The whole expedition team has fallen. ${points} points; sanctuary companions, bonds, branches, and equipment are preserved.`);
}
export function xpMultiplier(p){return p.level<=CONFIG.xp.earlyLevels?CONFIG.xp.earlyMultiplier:p.level<=CONFIG.xp.midLevels?CONFIG.xp.midMultiplier:1;}
function grantXp(p,base){
  const total=(p.xpRemainder||0)+base*xpMultiplier(p),earned=Math.floor(total+1e-9);
  p.training+=earned;p.xpRemainder=Math.max(0,total-earned);return earned;
}
function awardXp(s,pool){
  const members=team(s),max=Math.max(...members.map(p=>p.level)),weights=members.map(p=>(p.id===s.run.activeId?1.1:1)*(p.level<max?1.2:1)),total=weights.reduce((a,b)=>a+b,0);let left=pool,earned=0;
  members.forEach((p,i)=>{const xp=i===members.length-1?left:Math.floor(pool*weights[i]/total);earned+=grantXp(p,xp);left-=xp;p.bond++;});return earned;
}
function contribution(s,p,damage=0,healing=0){
  const b=s.run.battle;if(!b||!p||s.collection[p.id]!==p||!s.run.party.includes(p.id))return;
  b.contributions??={};const c=b.contributions[p.id]??={damage:0,healing:0,base:0,xp:0};
  c.damage+=Math.max(0,damage);c.healing+=Math.max(0,healing);
  // Count actual HP, never overkill/overheal, and bound repeat-healing rewards.
  const cap=Math.min(CONFIG.xp.maxCombatBase,Math.max(10,Math.ceil(b.enemies.reduce((n,e)=>n+stats(e,combatGear(s,e)).hp,0)*.3)));
  const next=Math.min(cap,(c.damage+c.healing)/CONFIG.xp.contributionHP);
  const earned=grantXp(p,Math.max(0,next-c.base));c.xp+=earned;recordWalkSession(s,{trainingXp:earned});c.base=next;
}
function victory(s){
  const r=s.run,b=r.battle;if(!b||b.rewarded||s.meta.claims[`reward:${b.id}`])return;b.rewarded=true;s.meta.claims[`reward:${b.id}`]=true;recordLivingWorldBattle(s,b);recordLegacyBattle(s,b);r.wins++;if(!b.ultra)r.ordinaryWins++;recordRegionEvent(s,'victories',1,b.originLandId||s.world.activeLand,{battleId:b.id});recordAdventureEvent(s,'victories',1,b.originLandId||s.world.activeLand);recordAdventureEvent(s,'seen',1,b.originLandId||s.world.activeLand,{species:b.enemies.map(p=>p.species)});if(b.guardian)recordAdventureEvent(s,'guardian',1,b.originLandId||s.world.activeLand);recordCompanionVictory(s,{guardian:!!b.guardian,regionId:b.originLandId||s.world.activeLand});recordWalkSession(s,{victories:1,friendship:team(s).length,landId:b.originLandId||s.world.activeLand});if(!b.ultra&&!b.guardian&&!b.hunt)r.nextEncounter=r.distance+ENCOUNTER_STEP;
  const baseXp=18+b.enemies.reduce((n,e)=>n+e.level*2,0)+b.enemies.length*4,xp=awardXp(s,baseXp);recordWalkSession(s,{trainingXp:xp});r.fruit+=2;const type=b.enemy.types[0]||rollElement(s);r.essence[type]+=2;r.items.charmMaterial=(r.items.charmMaterial||0)+1;
  const tutorial=!b.ultra&&r.ordinaryWins<=2;if(tutorial)r.items.bondCharm+=2;if(!b.ultra&&r.ordinaryWins===1){r.items.revive+=CONFIG.tutorial.revivesAfterFirst;say(s,'Introductory revival supply: restore a fallen teammate before replacing them at camp.');}
  // An Ultra's support squad cannot consume the guaranteed boss invitation.
  const eligible=b.ultra?[b.enemies.find(e=>e.species===r.ultra.species)||b.enemies[0]]:b.enemies;
  r.capture={id:b.id,candidates:eligible.map(e=>({...copy(e),bondBonus:b.bondBonuses[e.id]||0})),attempts:0,maxAttempts:CONFIG.captureAttempts,guaranteed:tutorial||b.ultra&&!sanctuary(s).some(p=>p.species===r.ultra.species),ultra:b.ultra,claimed:false};
  r.lastBattleFeedback=copy(b.hitFeedback||[]);r.lastBattleHits=r.lastBattleFeedback;
  r.battle=null;r.phase='befriend';r.vector={east:0,north:0};r.weights={N:0,S:0,E:0,W:0};say(s,`Victory: ${xp} experience shared across the team, two fruit, essence, and charm material. Choose one defeated companion to befriend.`);
}
export function captureChance(s,id){const c=s.run.capture,p=c?.candidates.find(p=>p.id===id);if(!p||c.claimed)return 0;if(c.guaranteed||s.meta.captureFailures>=CONFIG.capturePity)return 1;return equipmentCaptureChance(s,clamp((({common:.85,uncommon:.65,rare:.45,ultra:.65})[SPECIES[p.species].rarity]??.85)+(p.bondBonus||0),.05,.99));}
function finishCapture(s){s.run.capture=null;s.run.phase='trail';maybeUltra(s);pump(s);}
export function befriend(s,id,{duplicate='keep',nickname=''}={}){
  const r=s.run,c=r.capture,p=c?.candidates.find(p=>p.id===id);if(r.phase!=='befriend'||!p||c.claimed||s.meta.claims[`capture:${c.id}`])throw Error('This recruitment opportunity is already resolved.');if(c.attempts>=c.maxAttempts)throw Error('No attempts remain.');
  const free=c.ultra&&c.guaranteed;if(!free&&!(r.items.bondCharm>0))throw Error('A Bond Charm is required. Craft one at a camp.');const chance=captureChance(s,id);if(!free)r.items.bondCharm--;c.attempts++;
  if(chance<1&&random(s)>=chance){s.meta.captureFailures++;say(s,`The charm did not connect. Bad-luck protection: ${s.meta.captureFailures}/${CONFIG.capturePity}.`);return{success:false,chance,remaining:c.maxAttempts-c.attempts};}
  const duplicateFound=sanctuary(s).some(m=>m.species===p.species),level=Math.max(1,Math.min(3,Math.floor(team(s).reduce((n,m)=>n+m.level,0)/team(s).length)-1));
  const recruit=monkey(s,p.species,level),crew=legacyBonuses(r.legacy?.ranks);if(crew.crewHp||crew.crewEnergy){recruit.trailCrew={hp:crew.crewHp,maxEnergy:crew.crewEnergy};recruit.hp=stats(recruit).hp;recruit.energy=stats(recruit).maxEnergy;}recruit.nickname=String(nickname).trim().slice(0,24);recruit.bond=1;recruit.capturedEncounter=c.id;s.meta.captureFailures=0;s.meta.claims[`capture:${c.id}`]=true;c.claimed=true;s.collection[recruit.id]=recruit;recordCompanionRecruit(s,recruit.id);recordLegacyCapture(s);recordWalkSession(s,{recruited:1});
  if(r.party.length<CONFIG.partySize){r.party.push(recruit.id);r.injuries[recruit.id]={hp:recruit.hp,energy:recruit.energy};}r.lastCapture={id:recruit.id,species:recruit.species,duplicate:duplicateFound,converted:false};
  if(c.ultra){r.ultra.claimed=true;r.ultra.status='claimed';r.nextUltra=r.distance+CONFIG.ultraRepeatFeet*FEET;}
  const result={success:true,monkey:recruit,duplicate:duplicateFound,bondConverted:false,chance};if(duplicate==='bond'&&duplicateFound){convertDuplicate(s,recruit.id);result.bondConverted=true;}
  say(s,`${SPECIES[recruit.species].name} joined ${r.party.includes(recruit.id)?'your expedition team':'the sanctuary; bring it at a camp or before your next expedition'}.`);finishCapture(s);return result;
}
export function skipBefriend(s){if(s.run.phase!=='befriend')return;const c=s.run.capture;if(c.ultra){s.run.ultra.claimed=true;s.run.ultra.status='claimed';s.run.nextUltra=s.run.distance+CONFIG.ultraRepeatFeet*FEET;}s.meta.claims[`capture:${c.id}`]='passed';finishCapture(s);}
export function renameMonkey(s,id,name){const p=s.collection[id];if(!p)throw Error('Unknown companion.');p.nickname=String(name).trim().slice(0,24);return p;}
export function convertDuplicate(s,id){assertDuplicateConvertible(s,id);const p=s.collection[id],r=s.run,last=r.lastCapture;if(!p||last?.id!==id||last.converted||!last.duplicate||!alive(p)||!sanctuary(s).some(other=>other.id!==id&&other.species===p.species))throw Error('Only the newly recruited conscious duplicate may be converted.');
  if(r.party.includes(id)){if(!r.party.some(other=>other!==id&&alive(s.collection[other])))throw Error('Keep a conscious expedition companion.');r.party=r.party.filter(other=>other!==id);if(r.activeId===id){r.activeId=r.party.find(other=>alive(s.collection[other]));syncActive(s);}}
  s.meta.bondMaterials[p.species]=(s.meta.bondMaterials[p.species]||0)+3;delete s.collection[id];last.converted=true;return 3;}
function heal(p,amount,max){if(!alive(p))return 0;const before=p.hp;p.hp=Math.min(max,p.hp+Math.round(amount*(p.species==='langur'?1.25:1)));return Math.max(0,p.hp-before);}
function moveAvailable(b,p,id){const m=MOVES[id];if(p.charging?.id===id)return p.charging.resolveRound<=b.round;return!(p.cooldowns[id]>b.round)&&!(m.uses&&((p.uses[id]||0)>=m.uses));}
export function timingResult(position,{assisted=false,online=false}={}){const perfect=!online&&(assisted||Number.isFinite(position)&&position>=.42&&position<=.58);return{perfect,bonus:perfect?.10:0,window:[.42,.58]};}
export function battleIntent(s){const b=s.run?.battle,e=b?.enemy;if(!b||!e)return null;const m=MOVES[b.enemyAction?.id||b.intent],charge=e.charging;return{phase:b.phase||'intent',round:b.round,id:m?b.enemyAction?.id||b.intent:'strike',charging:!!charge,resolves:charge?.resolveRound,label:charge?`Charging ${MOVES[charge.id].name} — ${charge.resolveRound<=b.round?'resolves this round':'resolves next round'}`:b.tacticalIntent?.intentLabel||m?.shield||m?.guard?b.tacticalIntent?.intentLabel||`Preparing ${m.name} — vulnerable to Guard Break`:b.enemyAction?.type==='switch'?'Preparing a switch':m?.charge?`Preparing ${m.name} — charges for one round`:m?.name,detail:b.tacticalIntent?.intentDetail||m?.desc};}
export function battleOpportunities(s){const b=s.run?.battle,p=s.run?.monkey,e=b?.enemy;if(!b||!p||!e)return[];const moves=p.loadout.map(id=>({id,move:MOVES[id]}));return moves.flatMap(({id,move:m})=>e.charging&&m.interrupt?[{id,kind:'interrupt',label:'Interrupt opportunity'}]:(e.shield||MOVES[b.intent]?.guard||MOVES[b.intent]?.shield)&&(m.breakShield||m.feint)?[{id,kind:'guardBreak',label:'Break defensive setup'}]:m.comboStatuses?.some(status=>e.status[status])?[{id,kind:'combo',label:'Elemental combo ready'}]:[]);}
function metric(b,key,n=1){b.metrics??={};b.metrics[key]=(b.metrics[key]||0)+n;}
function applyStatus(p,id,duration=2,power){p.status[id]=Math.max(p.status[id]||0,duration);if(power!==undefined)p.statusPower[id]=power;}
function enter(s,p,side){
  const b=s.run.battle;if(side==='player'){s.run.activeId=p.id;syncActive(s);}else{b.enemyActive=b.enemies.findIndex(m=>m.id===p.id);b.enemy=p;}
  const rally=side==='player'?b.pendingRally:b.enemyRally;if(rally&&(!rally.expires||rally.expires>=b.round)){if(rally.shield){p.shield=Math.max(p.shield||0,Math.round(stats(p,combatGear(s,p)).hp*rally.shield));applyStatus(p,'shield',rally.duration||2);}if(rally.prepare){p.prepare=rally.prepare;applyStatus(p,'prepare',rally.duration||2);}if(side==='player')b.pendingRally=null;else b.enemyRally=null;}
}
function replacement(s,id,side='player',forced=false){
  const b=s.run.battle,actor=side==='player'?s.run.monkey:b.enemy,members=side==='player'?team(s):b.enemies,incoming=members.find(p=>p.id===id);
  if(!incoming||incoming.id===actor.id||!alive(incoming))throw Error('Choose another conscious teammate.');if(!forced&&actor.status.root)throw Error('Rooted Challenge briefly prevents voluntary switching.');delete actor.charging;if(side==='player'&&!forced)metric(b,'switches');equipmentSwitch(b,actor,incoming,members,{voluntary:!forced});enter(s,incoming,side);say(s,`${side==='player'?'You':'Rival'} bring in ${incoming.nickname||SPECIES[incoming.species].name}.`);return incoming;
}
function execute(s,actor,target,id,actorStats,targetStats,label,targetId,side='player',timing=null){
  const b=s.run.battle,m=MOVES[id],duration=m.duration||2,members=side==='player'?team(s):b.enemies,releasing=actor.charging?.id===id&&actor.charging.resolveRound<=b.round;
  if(!releasing){actor.energy-=equipmentMoveCost(b,actor,m,{consume:true});actor.uses[id]=(actor.uses[id]||0)+1;if(m.cooldown)actor.cooldowns[id]=b.round+m.cooldown;}
  if(actor.charging&&!releasing&&m.power)delete actor.charging;
  if(m.charge&&!releasing){actor.charging={id,resolveRound:b.round+m.charge,paid:m.cost||0};say(s,`${label} charges ${m.name} — resolves next round. Interrupt, guard, or switch.`);return;}
  if(releasing)delete actor.charging;
  const precision=m.timing&&side==='player'?timingResult(timing?.position,{assisted:timing?.assisted===true,online:!!b.online}):{perfect:false,bonus:0};
  let restored=0;const healingMultiplier=equipmentHealingMultiplier(b,actor,m),shieldMultiplier=equipmentShieldMultiplier(b,actor,m);
  if(side==='player'){b.metrics??={};b.metrics.elementUses??={};for(const t of m.types||[m.type])if(ELEMENTS[t])b.metrics.elementUses[t]=(b.metrics.elementUses[t]||0)+1;if(precision.perfect)metric(b,'perfectTiming');}
  if(m.cleanse){actor.statusCredit={};actor.status={};actor.statusPower={};actor.shield=0;actor.counter=0;actor.prepare=0;}
  if(m.guard){actor.guard=(actor.species==='mandrill'?.3:.45)*(precision.perfect?.9:1);actor.energy=Math.min(actorStats.maxEnergy,actor.energy+2);}
  if(m.evasion){actor.evasion=m.evasion;applyStatus(actor,'evasion',duration);}
  if(m.energy)actor.energy=Math.min(actorStats.maxEnergy,actor.energy+m.energy);if(m.heal){const recipient=m.healTarget?members.find(p=>p.id===targetId&&alive(p))||actor:actor,st=stats(recipient,combatGear(s,recipient));restored+=heal(recipient,st.hp*m.heal*healingMultiplier,st.hp);}
  if(m.teamHeal)for(const p of members)restored+=heal(p,stats(p,combatGear(s,p)).hp*m.teamHeal*healingMultiplier,stats(p,combatGear(s,p)).hp);
  if(m.teamCleanse)for(const p of members)if(alive(p))for(const status of['burn','poison','slow','root','weaken']){delete p.status[status];delete p.statusPower[status];if(p.statusCredit)delete p.statusCredit[status];}
  if(m.teamEnergy)for(const p of members)if(alive(p))p.energy=Math.min(stats(p,combatGear(s,p)).maxEnergy,p.energy+m.teamEnergy);
  if(m.reviveReserve){const p=members.find(p=>p.id!==actor.id&&!alive(p));if(p){p.hp=Math.max(1,Math.round(stats(p,combatGear(s,p)).hp*m.reviveReserve));p.status={};p.statusPower={};p.statusCredit={};p.shield=0;say(s,`${SPECIES[p.species].name} returns at ${Math.round(m.reviveReserve*100)}% health.`);}}
  if(m.shield){actor.shield=Math.max(actor.shield||0,Math.round(actorStats.hp*m.shield*shieldMultiplier));applyStatus(actor,'shield',duration);}
  if(m.counter){actor.counter=m.counter;actor.counterAbsorbed=m.expansion===3;actor.drainOnCounter=m.drainOnCounter||0;applyStatus(actor,'counter',m.duration||1);}
  if(m.reduceCooldown)for(const[mid,due]of Object.entries(actor.cooldowns))if(mid!==id&&due>b.round)actor.cooldowns[mid]=Math.max(b.round,due-m.reduceCooldown);
  if(m.prepare){actor.prepare=m.prepare;applyStatus(actor,'prepare',duration);}
  if(m.statusSelf)applyStatus(actor,m.statusSelf,duration);
  if(m.rally){const value={prepare:typeof m.rally==='number'?m.rally:.18,duration:m.duration||3,expires:b.round+(m.duration||3)};if(side==='player')b.pendingRally=value;else b.enemyRally=value;}
  if(m.teamShield&&!m.switch){if(m.expansion===3){for(const p of members)if(alive(p)){p.shield=Math.max(p.shield||0,Math.round(stats(p,combatGear(s,p)).hp*m.teamShield*shieldMultiplier));applyStatus(p,'shield',duration);}}else{const value={shield:m.teamShield*shieldMultiplier,duration,expires:b.round+duration};if(side==='player')b.pendingRally={...b.pendingRally,...value};else b.enemyRally={...b.enemyRally,...value};}}
  if(m.root)applyStatus(target,'root',typeof m.root==='number'?m.root:2);if(m.weaken)applyStatus(target,'weaken',duration,typeof m.weaken==='number'?m.weaken:.25);
  if(m.bondBonus)b.bondBonuses[target.id]=Math.min(.25,(b.bondBonuses[target.id]||0)+m.bondBonus);
  const plannedDefense=side==='player'&&b.interrupted?.[target.id]!==b.round&&(MOVES[b.enemyAction?.id]?.guard||MOVES[b.enemyAction?.id]?.shield||MOVES[b.enemyAction?.id]?.teamShield),comboActive=m.comboStatuses?.some(status=>target.status[status]),defensive=!!target.shield||!!target.guard||!!plannedDefense;
  const interrupt=!!m.interrupt&&(!!target.charging||!!target.prepare);
  if(interrupt){delete target.charging;target.prepare=0;delete target.status.prepare;b.interrupted??={};b.interrupted[target.id]=b.round;if(side==='player')metric(b,'interrupts');say(s,`${label}: Interrupted!`);}
  if(m.breakShield&&plannedDefense){b.interrupted??={};b.interrupted[target.id]=b.round;if(side==='player')metric(b,'guardBreaks');say(s,'Guard Break interrupts the defensive setup.');}
  if(m.delayCharge&&target.charging&&!target.charging.delayed){target.charging.resolveRound+=m.delayCharge;target.charging.delayed=true;say(s,`${label} delays the charged attack by one round.`);}
  if(comboActive){for(const status of m.comboStatuses){delete target.status[status];delete target.statusPower[status];if(target.statusCredit)delete target.statusCredit[status];}if(side==='player')metric(b,'combos');}
  if(m.power){
    if(m.breakShield){target.shield=0;target.guard=0;delete target.status.shield;}
    const eff=moveEffect(m,target.types),stab=(m.types||[m.type]).some(t=>actor.types.includes(t))?CONFIG.elemental.stab:1,critRoll=random(s),crit=(!actor.tutorialHpScale||CONFIG.tutorial.enemyCriticals)&&critRoll<(actor.species==='macaque'?.14:.07),howl=actor.species==='howler'&&m.type!=='neutral'?1.12:1;
    const actorGear=s.collection[actor.id]?equipped(s,actor):actor.gear||{},targetGear=s.collection[target.id]?equipped(s,target):target.gear||{};
    const healingPower=m.healingPower?Math.min(30,(actor.healingMomentum||0)*m.healingPower):0,finisher=m.finisherThreshold&&target.hp<=targetStats.hp*m.finisherThreshold?1+m.finisherBonus:1,tacticalMove={...m,perfectTiming:precision.perfect,comboActive,interrupted:interrupt};
    let rawDamage=Math.max(1,Math.round((m.power+healingPower+actorStats.atk*.70-targetStats.def*.42)*eff*stab*howl*(crit?1.4:1)*(.94+random(s)*.12)*(actor.prepare?1+actor.prepare:1)*(actor.status.weaken?1-(actor.statusPower.weaken||.25):1)*(1+precision.bonus)*(comboActive?1+m.comboBonus:1)*(m.feint&&defensive?1+m.feint:1)*finisher*equipmentAttackMultiplier(b,actor,target,tacticalMove))),damage=Math.max(1,Math.round(rawDamage*(m.feint?1:target.guard||1)*(1-(target.evasion||0))));
    if(m.healingPower)actor.healingMomentum=0;if(target.evasion){delete target.evasion;delete target.status.evasion;}
    if(actor.prepare){actor.prepare=0;delete actor.status.prepare;}if(actor.status.weaken){delete actor.status.weaken;delete actor.statusPower.weaken;}if(target.species==='spider'&&random(s)<.08)damage=0;
    if(side==='enemy')damage=legacyDamage(s,damage);
    const shieldBefore=target.shield||0,blocked=!!target.guard||shieldBefore>0,hitResult=equipmentApplyHit(b,actor,target,m,damage,{effectiveness:eff}),actualDamage=hitResult.damage;if(side==='player')contribution(s,actor,actualDamage);if(blocked&&side==='enemy')metric(b,'blocks');
    const hit={actorId:actor.id,targetId:target.id,actorName:actor.nickname||SPECIES[actor.species].name,targetName:target.nickname||SPECIES[target.species].name,moveId:id,side,multiplier:eff,damage:actualDamage,critical:crit,round:b.round,perfectTiming:precision.perfect,interrupted:interrupt,combo:!!comboActive,blocked,feint:!!(m.feint&&defensive),charged:releasing};
    b.lastHit=hit;(b.hitFeedback??=[]).push(hit);b.lastHits=b.hitFeedback;
    const feedback=effectInfo(eff);say(s,`${label}: ${m.name} → ${actualDamage}${crit?' critical':''}${feedback.kind!=='neutral'?` · ${feedback.label} (×${eff})`:''}.`);
    if(target.counter&&(actualDamage||target.counterAbsorbed)){const base=target.counterAbsorbed?Math.max(actualDamage,rawDamage):actualDamage,rawReflected=Math.max(1,Math.round(base*target.counter)),reflected=Math.min(actor.hp,side==='player'?legacyDamage(s,rawReflected):rawReflected);actor.hp=Math.max(0,actor.hp-reflected);contribution(s,target,reflected);if(side==='enemy')metric(b,'counters');const proc=equipmentTacticalEvent(b,target,'counter',{maxEnergy:targetStats.maxEnergy});if(proc.energy||proc.cooldowns)say(s,`Counter equipment restores ${proc.energy} energy and reduces ${proc.cooldowns} cooldowns.`);if(target.drainOnCounter){const n=Math.min(actor.energy,target.drainOnCounter);actor.energy-=n;target.energy=Math.min(targetStats.maxEnergy,target.energy+n);}target.counter=0;target.drainOnCounter=0;delete target.status.counter;say(s,`Counter: ${reflected} damage.`);}
    if(m.status&&alive(target)){const immune=m.status==='burn'&&target.types.includes('fire')||m.status==='poison'&&target.types.includes('leaf');if(!immune){applyStatus(target,m.status,duration);target.statusCredit??={};if(side==='player')target.statusCredit[m.status]=actor.id;else delete target.statusCredit[m.status];}}
    if(m.drain)restored+=heal(actor,actualDamage*m.drain,actorStats.hp);applyEquipmentLifesteal(actor,actorStats,actorGear,actualDamage);
    if(m.splash){const opponents=side==='player'?b.enemies:team(s);for(const p of opponents)if(p.id!==target.id&&alive(p)){const damage=equipmentApplyHit(b,actor,p,m,Math.round(actualDamage*m.splash),{effectiveness:eff,direct:false}).damage;if(side==='player')contribution(s,actor,damage);}}
  }else say(s,`${label} uses ${m.name}.`);
  if(m.drainEnergy){const drained=Math.min(target.energy,m.drainEnergy);target.energy-=drained;actor.energy=Math.min(actorStats.maxEnergy,actor.energy+drained);if(m.cancelUnfundedCharge&&target.charging&&target.energy<equipmentMoveCost(b,target,MOVES[target.charging.id])){delete target.charging;b.interrupted??={};b.interrupted[target.id]=b.round;if(side==='player')metric(b,'interrupts');say(s,'Energy disruption interrupted the charge.');}}
  actor.healingMomentum=Math.min(100,(actor.healingMomentum||0)+restored);
  if(precision.perfect)say(s,'Perfect Timing!');
  equipmentAfterAbility(b,actor,m,{target,moveId:id,team:members,recipients:m.teamHeal?members:[actor]});processEquipmentKnockouts(s);
  if(side==='player')contribution(s,actor,0,restored);
  if(m.switch&&alive(actor)&&actor.id===(side==='player'?s.run.activeId:b.enemy.id)){
    const options=members.filter(p=>alive(p)&&p.id!==actor.id),incoming=options.find(p=>p.id===targetId)||options[0];if(incoming){replacement(s,incoming.id,side,false);
      if(m.teamShield){incoming.shield=Math.max(incoming.shield||0,Math.round(stats(incoming,combatGear(s,incoming)).hp*m.teamShield*shieldMultiplier));applyStatus(incoming,'shield',duration);}
      if(m.transferSpeed){const turns=actor.status.speed||duration;applyStatus(incoming,'speed',turns,actor.statusPower.speed);delete actor.status.speed;delete actor.statusPower.speed;}
      if(m.entryEnergy&&!(incoming.cooldowns.secondWind>b.round)){incoming.energy=Math.min(stats(incoming,combatGear(s,incoming)).maxEnergy,incoming.energy+m.entryEnergy);incoming.cooldowns.secondWind=b.round+Math.max(4,m.cooldown||0);}
    }
  }
}
function tick(p,max,label,s){
  if(!alive(p))return;for(const[status,turns]of Object.entries(p.status)){
    if(status==='burn'||status==='poison'){const source=s.collection[p.statusCredit?.[status]]||(s.collection[p.id]?s.run.battle.enemy:s.run.monkey),raw=Math.max(1,Math.round(max*.04)),n=equipmentApplyHit(s.run.battle,source,p,{type:status==='burn'?'fire':'leaf'},Math.min(p.hp,s.collection[p.id]?legacyDamage(s,raw):raw),{direct:false,ignoreShields:true}).damage;contribution(s,s.collection[p.statusCredit?.[status]],n);say(s,`${label} takes ${n} ${status} damage.`);}
    if(turns<=1){if(p.statusCredit)delete p.statusCredit[status];delete p.status[status];delete p.statusPower[status];if(status==='shield')p.shield=0;if(status==='counter')p.counter=0;if(status==='prepare')p.prepare=0;if(status==='evasion')delete p.evasion;}else p.status[status]--;
  }
}
function resolveRound(s){
  const r=s.run,b=r.battle;if(b.round===CONFIG.fatigueRound-3)say(s,`Both teams are tiring. From round ${CONFIG.fatigueRound}, escalating fatigue will end a prolonged battle.`);
  for(const p of[...team(s),...b.enemies]){const st=stats(p,combatGear(s,p));tick(p,st.hp,p.nickname||SPECIES[p.species].name,s);p.guard=0;
    if(!alive(p))delete p.charging;
    if(b.round>=CONFIG.fatigueRound&&alive(p))p.hp=Math.max(0,p.hp-Math.max(1,Math.round(st.hp*Math.min(.6,.04*(b.round-CONFIG.fatigueRound+1)))));
    if(alive(p)){p.energy=Math.min(st.maxEnergy,p.energy+1);if(p.id===r.activeId||p.id===b.enemy.id){const recovered=equipmentApplyRecovery(b,p,st,combatGear(s,p));if(recovered.healing)say(s,`${p.nickname||SPECIES[p.species].name} recovers ${recovered.healing} health from equipment.`);}}}
  processEquipmentKnockouts(s);if(b.pendingRally?.expires<=b.round)b.pendingRally=null;if(b.enemyRally?.expires<=b.round)b.enemyRally=null;
  if(!team(s).some(alive)){endRun(s);return;}if(!b.enemies.some(alive)){
    if(!alive(r.monkey)){b.pendingVictory=true;r.phase='replacement';say(s,'Your team won, and the active companion fell. Choose a conscious teammate before collecting the rewards.');return;}
    victory(s);return;
  }if(!alive(b.enemy)){const incoming=b.enemies.find(alive);equipmentSwitch(b,b.enemy,incoming,b.enemies,{voluntary:false});enter(s,incoming,'enemy');}b.round++;b.turn=b.round;
  if(!alive(r.monkey)){r.phase='replacement';say(s,'Choose a conscious replacement. This forced switch costs no turn and gives no extra enemy attack.');return;}planEnemy(s);
}
function itemApply(s,id,targetId){
  id=itemId(id);const r=s.run,p=getMonkey(s,targetId);if(!p||!r.party.includes(p.id)||!ITEMS[id]||!(r.items[id]>0))throw Error('That item is unavailable for this teammate.');const st=stats(p,equipped(s,p));
  if(id==='revive'){if(p.hp>0)throw Error('That teammate is already conscious.');r.items[id]--;p.hp=Math.max(1,Math.round(st.hp*.30));p.status={};p.statusCredit={};p.statusPower={};say(s,'A revival supply restored your teammate.');return;}if(!alive(p))throw Error('Use an actual revival supply for a knocked-out teammate.');
  if(id==='berry'&&p.hp>=st.hp||id==='tonic'&&p.energy>=st.maxEnergy||id==='salve'&&!Object.keys(p.status).length&&p.hp>=st.hp)throw Error('That supply is not needed.');if(!['berry','tonic','salve'].includes(id))throw Error('Use this utility from the pack or camp.');r.items[id]--;
  const healingBonus=equipmentConsumableHealing(equipped(s,p));if(id==='berry')heal(p,st.hp*.35*healingBonus,st.hp);if(id==='tonic')p.energy=Math.min(st.maxEnergy,p.energy+8);if(id==='salve'){p.statusCredit={};p.status={};p.statusPower={};heal(p,st.hp*.15*healingBonus,st.hp);}say(s,`Used ${ITEMS[id].name}.`);
}
function performTurn(s,action){
  const r=s.run,b=r.battle;if(r.phase!=='battle'||!b?.ready)throw Error('Stop safely and begin the encounter first.');beginEquipmentBattle(s);const p=r.monkey,e=b.enemy,enemyAction=copy(b.enemyAction||{type:'move',id:b.intent}),em=MOVES[enemyAction.id],ps=battleStats(s,p),es=battleStats(s,e),pm=MOVES[action.id];
    if(action.type==='move'){
    if(!p.loadout.includes(action.id)||!pm||!(p.charging?.id===action.id&&p.charging.resolveRound<=b.round)&&p.energy<equipmentMoveCost(b,p,pm)||!moveAvailable(b,p,action.id))throw Error('Ability unavailable, cooling down, or insufficient energy.');if(pm.switch&&p.status.root)throw Error('Rooted Challenge prevents voluntary retreat this round.');if(pm.switch&&!team(s).some(m=>alive(m)&&m.id!==p.id))throw Error('No conscious teammate can receive the relay.');
  }
  if(action.type==='switch'){const incoming=team(s).find(m=>m.id===action.id);if(!incoming||!alive(incoming)||incoming.id===p.id||p.status.root)throw Error('Choose another conscious, unrooted switch.');}
  b.phase='resolution';b.openingLocked=true;b.hitFeedback=[];b.lastHits=b.hitFeedback;delete b.lastHit;
  const initiative=(actor,st,m)=>(m?.priority||0)*1000+st.speed*(actor.status.slow?.65:1)*(actor.status.speed?1.2:1),playerFirst=action.type!=='move'||initiative(p,ps,pm)>=initiative(e,es,em);
  const playerAction=()=>{if(!alive(p)||p.id!==r.activeId||b.interrupted?.[p.id]===b.round)return;if(action.type==='switch')replacement(s,action.id,'player');else execute(s,p,b.enemy,action.id,battleStats(s,p),battleStats(s,b.enemy),'You',action.targetId,'player',action.timing);equipmentTurnEnd(b,p,team(s));processEquipmentKnockouts(s);};
  const enemyAct=()=>{if(!alive(e)||e.id!==b.enemy.id||b.interrupted?.[e.id]===b.round)return;const cannotRetreat=e.status.root||!b.enemies.some(m=>m.id!==e.id&&alive(m));if(enemyAction.type==='switch'&&!cannotRetreat)replacement(s,enemyAction.id,'enemy');else{const chosen=enemyAction.type==='switch'||MOVES[enemyAction.id]?.switch&&cannotRetreat||!(e.charging?.id===enemyAction.id&&e.charging.resolveRound<=b.round)&&e.energy<equipmentMoveCost(b,e,MOVES[enemyAction.id]||MOVES.strike)?'strike':e.charging?.id===enemyAction.id&&e.charging.resolveRound>b.round?'brace':enemyAction.id;execute(s,e,r.monkey,chosen,battleStats(s,e),battleStats(s,r.monkey),'Rival',null,'enemy');}equipmentTurnEnd(b,e,b.enemies);processEquipmentKnockouts(s);};
  if(playerFirst){playerAction();enemyAct();}else{enemyAct();playerAction();}b.phase='results';resolveRound(s);
}
export function turn(s,id,isItem=false,options={}){if(isItem)return useItem(s,id,options.targetId);return performTurn(s,{type:'move',id,targetId:options.targetId,timing:options.timing});}
export function switchMonkey(s,id){return performTurn(s,{type:'switch',id});}
export function chooseReplacement(s,id){if(s.run.phase!=='replacement')throw Error('No forced replacement is pending.');replacement(s,id,'player',true);s.run.phase='battle';if(s.run.battle.pendingVictory)victory(s);else planEnemy(s);return s.run.monkey;}
export function useItem(s,id,targetId){
  const r=s.run;if(r.phase==='dead')throw Error('Begin a new expedition.');
  if(['replacement','befriend'].includes(r.phase))throw Error('Finish choosing your companion first.');
  if(r.phase==='battle'){
    const b=r.battle;if(!b?.ready)throw Error('Stop safely and begin the encounter first.');
    if(b.itemUsedRound===b.round)throw Error('One supply per round. Choose an ability or attack next.');
    itemApply(s,id,targetId);b.openingLocked=true;b.itemUsedRound=b.round;say(s,'Supply used — your attack or ability is still available.');return;
  }itemApply(s,id,targetId);
}
export function setParty(s,ids){
  const r=s.run;if(!['trail','camp'].includes(r.phase))throw Error('Change teammates on the trail or at camp, between encounters.');
  if(!Array.isArray(ids)||!ids.length||ids.length>3||new Set(ids).size!==ids.length||ids.some(id=>!s.collection[id]||!r.party.includes(id)&&!alive(s.collection[id]))||!ids.some(id=>alive(s.collection[id])))throw Error('Choose one to three different companions, with conscious incoming monkeys and at least one conscious teammate.');
  for(const p of team(s))r.injuries[p.id]={hp:p.hp,energy:p.energy,status:copy(p.status)};r.party=[...ids];if(!ids.includes(r.activeId)||!alive(s.collection[r.activeId]))r.activeId=ids.find(id=>alive(s.collection[id]));syncActive(s);say(s,'Expedition team updated. Every returning companion keeps its injuries.');return team(s);
}
export function campHeal(s,id=s.run.activeId){const r=s.run,p=getMonkey(s,id);if(r.phase!=='camp'||!r.party.includes(id)||!r.camp.heals||!alive(p))throw Error('Camp recovery needs a conscious teammate and a remaining recovery action.');r.camp.heals--;heal(p,stats(p,equipped(s,p)).hp*(r.camp.seedBoost?.4:.3),stats(p,equipped(s,p)).hp);p.energy=Math.min(stats(p,equipped(s,p)).maxEnergy,p.energy+3);return p;}
export function leaveCamp(s){if(s.run.phase!=='camp')return;s.run.camp=null;s.run.phase='trail';pump(s);}
export function trainingCost(p){return{fruit:p.level<=3?[0,3,5,8][p.level]:8+p.level*4,xp:12+p.level*10};}
export function train(s,id){
  const r=s.run,p=getMonkey(s,id);if(!p||!alive(p)||!['trail','camp'].includes(r.phase)||!r.party.includes(p.id))throw Error('Train a conscious expedition companion between encounters.');if(p.level>=CONFIG.maxLevel)throw Error('Maximum level reached.');const cost=trainingCost(p);if(r.fruit<cost.fruit||p.training<cost.xp)throw Error(`Leveling needs ${cost.xp} XP and ${cost.fruit} fruit.`);
  const before=stats(p,equipped(s,p)).hp;r.fruit-=cost.fruit;p.training-=cost.xp;grantXp(p,(equipmentTrainingExperience(s,p,cost.xp)-cost.xp)/xpMultiplier(p));p.level++;const known=new Set(p.learned||[]);syncProgression(p);const newlyLearned=p.learned.filter(id=>!known.has(id));if(newlyLearned.length){s.run.progressionNotice={monkeyId:p.id,level:p.level,moves:newlyLearned};say(s,`New techniques: ${newlyLearned.map(id=>MOVES[id].name).join(", ")}.`);}p.hp=Math.min(stats(p,equipped(s,p)).hp,p.hp+stats(p,equipped(s,p)).hp-before);say(s,`${p.nickname||SPECIES[p.species].name} reached level ${p.level}.`);return p;
}
export function evolutionCost(s,type,id){const p=getMonkey(s,id),next=(p?.stage||0)+1,required=[0,3,6,10][next];if(!p||!ELEMENTS[type]||next>3)return null;
  const native=nativeTypes(p.species);if(next===1&&native.length&&!native.includes(type))return null;
  const types=next===1?(native.length?native:[type]):[...new Set([...p.types,type])];if(types.length>2||next===3&&!p.types.includes(type))return null;
  return{level:required,cost:next===1?Object.fromEntries(types.map(t=>[t,CONFIG.evolution.firstEssence])):next===2?{[type]:24}:Object.fromEntries(types.map(t=>[t,types.length===1?40:28])),types};}
export function evolutionPreview(s,type,id){const p=getMonkey(s,id),offer=evolutionCost(s,type,id);if(!offer)return null;const preview={...p,types:offer.types,stage:p.stage+1},mastery=preview.stage===3?CONFIG.masteries[type]:null;if(mastery)preview.mastery=type;
  const beforeStats=stats(p,equipped(s,p)),afterStats=stats(preview,equipped(s,p)),statGains=Object.fromEntries(Object.keys(afterStats).map(key=>[key,afterStats[key]-beforeStats[key]]));
  return{...offer,monkey:preview,name:formName(preview),stats:afterStats,beforeStats,statGains,abilities:unlocked(preview),newlyUnlocked:unlocked(preview).filter(id=>!unlocked(p).includes(id)),future:abilityCatalogue(preview).filter(a=>!a.eligible),minorPassive:progressionPassive(preview),specialization:mastery?.name,desc:mastery?`Ascendant mastery: ${mastery.desc} Choose which existing element to master for this expedition.`:undefined,passive:mastery?`${mastery.name}: ${mastery.desc}`:offer.types.length===1?'Pure harmony: greater specialization in attack or defense.':'Mixed harmony: broader coverage with combined weaknesses and resistances.',matchups:Object.fromEntries(Object.keys(ELEMENTS).map(t=>[t,effectiveness(t,preview.types)]))};}
export function evolve(s,type,id){
  const r=s.run,p=getMonkey(s,id),offer=evolutionCost(s,type,id);if(!p||!alive(p)||!['trail','camp'].includes(r.phase)||!offer||!r.party.includes(p.id))throw Error('That evolution is unavailable for a fallen companion.');if(p.level<offer.level)throw Error(`Reach level ${offer.level} first.`);if(Object.entries(offer.cost).some(([t,n])=>r.essence[t]<n))throw Error('Gather the required essence or use the slower resource exchange.');
  const old=stats(p,equipped(s,p)).hp;for(const[t,n]of Object.entries(offer.cost))r.essence[t]-=n;p.types=[...offer.types];p.stage++;if(p.stage===3)p.mastery=type;p.hp=Math.min(stats(p,equipped(s,p)).hp,p.hp+stats(p,equipped(s,p)).hp-old);const key=`${p.stage}:${[...p.types].sort().join('+')}${p.stage===3?`:mastery=${p.mastery}`:''}`;if(!p.discovered.includes(key))p.discovered.push(key);if(p.stage>=p.highestForm.stage)p.highestForm={stage:p.stage,types:[...p.types],...(p.stage===3?{mastery:p.mastery}:{})};syncProgression(p);s.run.evolutionNotice={monkeyId:p.id,stage:p.stage,name:formName(p),moves:unlocked(p),passive:progressionPassive(p)};say(s,`Evolved into ${formName(p)}${p.stage===3?` with ${CONFIG.masteries[p.mastery].name}`:''}. This branch stays discovered in the sanctuary.`);return p;
}
function clampEquipmentVitals(s){
  for(const p of sanctuary(s)){const maximum=stats(p,equipped(s,p));p.hp=Math.min(p.hp,maximum.hp);p.energy=Math.min(p.energy,maximum.maxEnergy);}
  s.meta.equipped={...s.run.monkey.equipment};updateNextCache(s);
}
export function equip(s,id,monkeyId,slot){
  const p=getMonkey(s,monkeyId);if(!p||!canChangeEquipment(s))throw Error('Equip owned gear between encounters.');
  normalizeEquipmentInventory(s);
  normalizeEquipmentTrail(s);
  const copy=ownedEquipmentCopies(s).find(c=>c.id===id)||ownedEquipmentCopies(s).find(c=>c.itemId===id&&!c.ownerId)||ownedEquipmentCopies(s).find(c=>c.itemId===id);
  if(!copy)throw Error('Choose owned equipment.');
  slot??=GEAR[copy.itemId].slot;
  assignEquipment(s,copy.id,p.id,slot);clampEquipmentVitals(s);return p;
}
export function unequip(s,slot,monkeyId){
  const p=getMonkey(s,monkeyId);if(!p)throw Error('Choose a monkey.');removeEquipment(s,slot,p.id);clampEquipmentVitals(s);return p;
}
export function setMove(s,slot,id,monkeyId){const p=getMonkey(s,monkeyId);if(!p||!['trail','camp'].includes(s.run.phase)||!Number.isInteger(slot)||slot<0||slot>3||(slot===0&&id!=='strike')||!abilityInfo(p,id)?.eligible)throw Error('Change curated abilities between encounters; keep Palm Strike in slot one.');if(p.loadout.includes(id)&&p.loadout[slot]!==id)throw Error('That ability is already equipped.');syncProgression(p);p.loadout[slot]=id;return p;}
export function exchangeEssence(s,from,to){if(!['trail','camp'].includes(s.run.phase)||!ELEMENTS[from]||!ELEMENTS[to]||from===to||s.run.essence[from]<4)throw Error('Exchange four of one essence for one other essence between encounters.');s.run.essence[from]-=4;s.run.essence[to]++;}
export function craft(s,id){
  id=itemId(id);const r=s.run;if(!canChangeEquipment(s))throw Error('Craft supplies outside active battles.');if(GEAR[id]){if((s.meta.fragments[id]||0)<CONFIG.fragmentCost)throw Error(`Collect ${CONFIG.fragmentCost} equipment fragments.`);if(previewGearAward(s,id).kind==='salvaged')throw Error('This equipment is already at maximum tier.');awardGearCopy(s,id,{source:'fragments'});s.meta.fragments[id]-=CONFIG.fragmentCost;applyEquipmentCraftRefund(s,equipmentCraftRefund(s,{[`fragment:${id}`]:CONFIG.fragmentCost}));return;}
  const recipes={bondCharm:{fruit:2,material:1},revive:{fruit:8,material:4},berry:{fruit:3,material:0}},recipe=recipes[id];if(!recipe||r.fruit<recipe.fruit||(r.items.charmMaterial||0)<recipe.material)throw Error('Gather more fruit and charm material for this recipe.');r.fruit-=recipe.fruit;r.items.charmMaterial-=recipe.material;r.items[id]++;applyEquipmentCraftRefund(s,equipmentCraftRefund(s,{fruit:recipe.fruit,'item:charmMaterial':recipe.material}));
}
export function migrate(s){
  if(!s||s.version!==1)return s;const r=s.run,p=r?.monkey;if(!r||!p||!SPECIES[p.species])throw Error('Invalid legacy checkpoint.');s.version=VERSION;s.difficulty=s.difficulty||'standard';s.meta.serial=s.meta.serial||0;s.meta.fragments={};s.meta.claims={};s.meta.captureFailures=0;s.meta.bondMaterials={};s.collection={};
  const fresh=monkey(s,p.species,p.level,p.types,p.stage);Object.assign(fresh,p);fresh.id=uid(s,'legacy');fresh.nickname=p.nickname||'';fresh.equipment={...s.meta.equipped};fresh.learned=[...new Set([...fresh.learned,...p.loadout,...unlocked(fresh)])];if(p.stage===3)fresh.mastery=p.mastery||p.types[0];fresh.discovered=p.stage?[`${p.stage}:${[...p.types].sort().join('+')}${p.stage===3?`:mastery=${fresh.mastery}`:''}`]:[];fresh.highestForm={stage:p.stage,types:[...p.types],...(p.stage===3?{mastery:fresh.mastery}:{})};fresh.statusPower={};fresh.cooldowns={};fresh.uses={};fresh.bond=0;s.collection[fresh.id]=fresh;
  r.monkey=fresh;r.party=[fresh.id];r.activeId=fresh.id;r.ordinaryWins=r.wins||0;r.discoveryProgress=clamp(1-(r.nextCache-r.distance)/RESOURCE_STEP,0,.999999);r.discoveries=Math.floor(r.distance/RESOURCE_STEP);r.nextCamp=(Math.floor(r.distance/(CONFIG.campFeet*FEET))+1)*CONFIG.campFeet*FEET;r.bonuses={ribbon:0,scent:0,feather:0,campSeed:0};r.injuries={};r.events=[];r.capture=null;r.camp=null;r.ultra=null;r.nextUltra=Math.max(CONFIG.ultraFirstFeet*FEET,r.distance+2000*FEET);
  for(const t of Object.keys(ELEMENTS))r.essence[t]??=0;for(const id of Object.keys(ITEMS))r.items[id]??=0;r.items.charmMaterial=0;
  if(r.battle){const b=r.battle;b.id=uid(s,'legacy-battle');b.enemies=[b.enemy];b.enemyActive=0;b.round=b.turn||1;b.ultra=false;b.rewarded=false;b.bondBonuses={};b.pendingRally=null;b.enemyRally=null;b.enemyAction={type:'move',id:b.intent||'strike'};for(const e of b.enemies){e.id=uid(s,'enemy');e.statusPower={};e.cooldowns={};e.uses={};e.equipment={};e.learned=e.loadout||['strike'];}}
  updateNextCache(s);say(s,'Your original companion, expedition, equipment, and progression were restored into sanctuary slot one.');return s;
}
export function validate(s){
  migrate(s);if(!s||s.version!==VERSION||!['gps','practice'].includes(s.mode)||typeof s.name!=='string'||s.name.length>30)throw Error('Unsupported save.');const r=s.run;
  normalizeLegacy(s);
  if(!r||!['trail','battle','replacement','befriend','camp','dead'].includes(r.phase)||!s.collection||!Array.isArray(r.party)||r.party.length<1||r.party.length>3||new Set(r.party).size!==r.party.length||r.party.some(id=>!s.collection[id])||!r.party.includes(r.activeId))throw Error('Invalid expedition team.');syncActive(s);const finite=n=>Number.isFinite(n)&&n>=0;
  for(const n of[r.distance,r.nextCache,r.nextEncounter,r.nextCamp,r.fruit,r.discoveryProgress,s.rng,s.meta?.legacy])if(!finite(n))throw Error('Invalid save numbers.');if(r.discoveryProgress>=1)throw Error('Invalid discovery progress.');
  normalizeEquipmentInventory(s);
  normalizeEquipmentTrail(s);
  for(const[id,p]of Object.entries(s.collection)){
    p.learned??=[];p.discovered??=[];p.highestForm??={stage:p.stage,types:[...p.types]};syncProgression(p);p.xpRemainder??=0;if(!finite(p.xpRemainder)||p.xpRemainder>=1)throw Error('Invalid XP remainder.');
    if(id!==p.id||!SPECIES[p.species]||![p.hp,p.energy,p.level,p.training,p.bond].every(finite)||!Number.isInteger(p.level)||p.level<1||p.level>60||!Number.isInteger(p.stage)||p.stage<0||p.stage>3||!Array.isArray(p.types)||p.types.length>2||p.types.some(t=>!ELEMENTS[t])||new Set(p.types).size!==p.types.length)throw Error('Invalid sanctuary companion.');
    // Earlier v2 checkpoints predate explicit mastery choices; retain the primary path.
    if(p.stage===3&&!p.mastery){p.mastery=p.types[0];if(p.highestForm?.stage===3&&!p.highestForm.mastery)p.highestForm.mastery=p.mastery;const key=`3:${[...p.types].sort().join('+')}:mastery=${p.mastery}`;if(!p.discovered.includes(key))p.discovered.push(key);}
    if(p.mastery&&(!CONFIG.masteries[p.mastery]||!p.types.includes(p.mastery)))throw Error('Invalid mastery choice.');
    if(!Array.isArray(p.loadout)||p.loadout[0]!=='strike'||p.loadout.length>4||new Set(p.loadout).size!==p.loadout.length||p.loadout.some(id=>!MOVES[id]||!unlocked(p).includes(id)))throw Error('Invalid ability loadout.');if(!p.status||Object.values(p.status).some(n=>!finite(n)||n>10)||!p.equipment||Object.values(p.equipment).some(id=>id&&!s.meta.gear[id]))throw Error('Invalid companion equipment or effects.');
    if(Object.entries(p.equipment).some(([slot,id])=>id&&(!GEAR[id]||!equipmentSlots(p).includes(slot))))throw Error('Invalid companion equipment ownership.');
  }
  if(!r.essence||Object.keys(ELEMENTS).some(t=>!finite(r.essence[t]))||!r.items||Object.keys(ITEMS).some(t=>!finite(r.items[t])))throw Error('Invalid inventory.');if(!r.vector||!Object.values(r.vector).every(Number.isFinite)||!r.weights||!Object.values(r.weights).every(finite)||!r.bonuses||!Object.values(r.bonuses).every(finite))throw Error('Invalid walking checkpoint.');
  if(['battle','replacement'].includes(r.phase)){const b=r.battle;if(!b||!Array.isArray(b.enemies)||b.enemies.length<1||b.enemies.length>3||!b.enemies.every(p=>SPECIES[p.species]&&finite(p.hp)&&finite(p.energy))||!b.enemies.some(e=>e.id===b.enemy?.id)||!MOVES[b.intent]||!finite(b.round))throw Error('Invalid battle checkpoint.');b.enemy=b.enemies.find(e=>e.id===b.enemy.id);b.openingLocked??=true;}
  if(r.phase==='befriend'&&(!r.capture?.id||!r.capture.candidates?.length||!finite(r.capture.attempts)||r.capture.attempts>CONFIG.captureAttempts))throw Error('Invalid recruitment checkpoint.');if(r.ultra&&(!SPECIES[r.ultra.species]||!r.ultra.id||!finite(r.ultra.challengeRemaining)||!finite(r.ultra.spawnDistance)))throw Error('Invalid Ultra sighting.');
  if(!s.meta.gear||!s.meta.equipped||!Array.isArray(s.meta.history)||s.meta.history.length>25||Object.entries(s.meta.gear).some(([id,n])=>!GEAR[id]||!Number.isInteger(n)||n<1||n>12)||!s.meta.claims)throw Error('Invalid persistent inventory.');validateForgeCheckpoint(s);normalizeSales(s);normalizeLands(s);normalizeWalkSessions(s);normalizeCompanions(s);normalizeAdventures(s);r.pendingEncounter??=null;if(r.pendingEncounter){const b=r.pendingEncounter;if(!b.id||!Array.isArray(b.enemies)||!b.enemies.length||b.enemies.length>3||b.enemies.some(p=>!SPECIES[p.species]||!finite(p.hp)||!finite(p.energy))||!s.world.unlocked.includes(b.originLandId))throw Error('Invalid pending encounter checkpoint.');b.enemy=b.enemies.find(e=>e.id===b.enemy?.id)||b.enemies[0];}if(!Array.isArray(s.log)||s.log.length>16||JSON.stringify(s).length>650000)throw Error('Save is too large.');return s;
}
