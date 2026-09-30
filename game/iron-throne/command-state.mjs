import { GENERAL_ROSTER } from './general-roster.mjs';
import { allArmies } from './naval-state.mjs';
// Pure state helpers shared by the engine, projections and UI.
export const GENERAL_QUALITIES = [
  {name:'Capable',cost:75,upkeep:1,bonus:.05}, {name:'Veteran',cost:125,upkeep:1,bonus:.10},
  {name:'Distinguished',cost:200,upkeep:2,bonus:.15}, {name:'Renowned',cost:300,upkeep:3,bonus:.20},
  {name:'Legendary',cost:450,upkeep:4,bonus:.25}
];
export const GENERAL_PERSONALITIES=['aggressive','cautious','methodical','opportunistic','protective'];
export const COMMAND_KINDS=['attack','defend','rally','reinforce','siege','withdraw','frontier'];
export const COMMAND_STATUSES=['Preparing','Marching','Engaged','Holding','Blocked','Completed'];
export const activationKey=(s,owner)=>`${s.turn}:${owner}`;
export function initializeCommanders(s) {
  s.commanders ??= {version:2,roster:[],candidates:[],retired:[],nextOffer:{},lastRound:0};
  s.fealty ??= {};
}
export const generalForArmy=(s,a)=>s.commanders?.roster.find(g=>g.owner===a.owner&&g.commandId===a.commandId);
export const generalUpkeep=(s,owner)=>(s.commanders?.roster||[]).filter(g=>g.owner===owner).reduce((n,g)=>n+GENERAL_QUALITIES[g.quality].upkeep,0);
export const manualOverride=(s,a)=>a.playerOverride===activationKey(s,a.owner);
export function markPlayerOverride(s,a) {
  if(!s.controllers?a.owner==='ashen':s.controllers[a.owner]?.kind==='human'&&!s.controllers[a.owner]?.substitute)a.playerOverride=activationKey(s,a.owner);
}
export function syncCommanders(s) {
  initializeCommanders(s);
  for(const a of allArmies(s)){
    const g=generalForArmy(s,a),q=g&&GENERAL_QUALITIES[g.quality];
    if(!g){delete a.commandId;delete a.commandBonus;delete a.commandMove;continue;}
    a.commandBonus=q?.bonus||0;
    a.commandMove=g?.specialty==='movement'?(g.quality>=3?2:1):0;
  }
}
export function validateCommanders(s) {
  initializeCommanders(s);
  const c=s.commanders,fail=()=>{throw new Error('Damaged commander data.');};
  const int=(n,min=0,max=100020)=>Number.isInteger(n)&&n>=min&&n<=max;
  const house=id=>s.kingdoms.some(k=>k.id===id);
  if(!c||!Array.isArray(c.roster)||c.roster.length>48||!Array.isArray(c.candidates)||c.candidates.length>12||!c.nextOffer||typeof c.nextOffer!=='object'||Array.isArray(c.nextOffer)||!int(c.lastRound,0,s.turn)||Object.entries(c.nextOffer).some(([id,n])=>!house(id)||!int(n)))fail();
  if(s.kingdoms.some(k=>c.roster.filter(g=>g?.owner===k.id).length>16||c.candidates.filter(g=>g?.owner===k.id).length>1))fail();
  // Legacy identities are mapped once, preserving runtime IDs, objectives and histories.
  c.retired ??= [];
  if(c.version!==2){
    const existing=[...c.roster,...c.candidates];
    if(existing.length>16)throw new Error('This legacy campaign has more than 16 generals. Keep the original save; it requires an explicit roster migration.');
    existing.forEach((g,i)=>{const identity=GENERAL_ROSTER[i];g.previousName=g.name;g.characterId=identity.characterId;g.name=identity.name;g.personality=identity.personality;});c.version=2;
  }
  if(!Array.isArray(c.retired)||c.retired.length>16||c.roster.length+c.candidates.length+c.retired.length>16)fail();
  const identities=new Set();
  const seen=new Set();
  for(const g of [...c.roster,...c.candidates,...c.retired]){
    const identity=GENERAL_ROSTER.find(x=>x.characterId===g?.characterId);
    if(!identity||identities.has(g.characterId))fail();identities.add(g.characterId);g.name=identity.name;
    if(!g||!/^general-\d+$/.test(g.id)||seen.has(g.id)||!house(g.owner)||!int(g.quality,0,4)||!GENERAL_PERSONALITIES.includes(g.personality)||!['movement','mustering','battle'].includes(g.specialty)||!s.tiles[g.city]||typeof g.name!=='string'||g.name.length>60||g.commandId!==`command-${g.id}`||!int(g.expires)||!Array.isArray(g.history)||g.history.length>40)fail();
    seen.add(g.id);
    if(g.history.some(m=>!m||!['player','general','council'].includes(m.role)||typeof m.text!=='string'||m.text.length>1600||!int(m.turn,0,s.turn)))fail();
    if(g.lastMuster!==undefined&&!int(g.lastMuster,0,s.turn)||g.lastPlanned!==undefined&&!int(g.lastPlanned,0,s.turn))fail();
    if(g.objective){const o=g.objective;if(o.army!==undefined&&(o.kind!=='reinforce'||typeof o.army!=='string'||!/^army-\d+$/.test(o.army)))fail();if(!COMMAND_KINDS.includes(o.kind)||!Array.isArray(o.targets)||o.targets.length<1||o.targets.length>3||new Set(o.targets).size!==o.targets.length||o.targets.some(id=>!s.tiles[id])||!int(o.lossLimit,15,65)||typeof o.allowSplit!=='boolean'||!int(o.approvedTurn,0,s.turn)||!COMMAND_STATUSES.includes(o.status)||typeof o.reason!=='string'||o.reason.length>500)fail();}
  }
  for(const a of allArmies(s)){
    if(a.name!==undefined&&(typeof a.name!=='string'||a.name.length>60))fail();
    if(a.commandId&&!c.roster.some(g=>g.commandId===a.commandId&&g.owner===a.owner))delete a.commandId;
    if(a.commandBaseline!==undefined&&!int(a.commandBaseline,0,100000)||a.regrouping!==undefined&&typeof a.regrouping!=='boolean')fail();
    if(a.movementSpent!==undefined&&(!Number.isFinite(a.movementSpent)||a.movementSpent<0||a.movementSpent>30))fail();
    for(const key of ['movementTurn','resolvedTurn'])if(a[key]!==undefined&&!int(a[key],0,s.turn))fail();
    if(a.playerOverride!==undefined&&(typeof a.playerOverride!=='string'||a.playerOverride.length>40))fail();
  }
  syncCommanders(s);
}
