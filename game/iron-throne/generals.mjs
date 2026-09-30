import { UNITS } from './data.mjs';
import { GENERAL_ROSTER, generalTemperament, generalTraits } from './general-roster.mjs';
import { allArmies } from './naval-state.mjs';
import { hash } from './world-hex.mjs';
import { knowledgeView } from './fog.mjs';
import { planningView } from './ai-knowledge.mjs';
import { isAiHouse } from './house-control.mjs';
import { difficulty } from './difficulty.mjs';
import { GENERAL_QUALITIES, COMMAND_KINDS, initializeCommanders, syncCommanders, manualOverride, markPlayerOverride } from './command-state.mjs';
import { armiesOf, atWar, canAfford, distance, economyProjection, findPath, kingdom, mergeArmies, orderArmy, pay, recruitLocalMustering, settlements, sizeOf, splitArmy, strength, treaty } from './core.mjs';
import { assaultAssessment, dangerousTiles, orderBombardment } from './plans.mjs';

const fail=error=>({ok:false,error});
const own=(s,owner,id)=>s.commanders?.roster.find(g=>g.id===id&&g.owner===owner);
const forces=(s,g)=>armiesOf(s,g.owner).filter(a=>a.commandId===g.commandId);
export function generalMessage(s,g,role,text){g.history.push({turn:s.turn,role,text:String(text).slice(0,1600)});g.history=g.history.slice(-40);}

// One deterministic opportunity per House, bounded by a realm-wide cooldown.
// Called only at a round boundary, never from render/open/reconnect.
export function refreshGeneralCandidates(s) {
  initializeCommanders(s);
  s.commanders.candidates=s.commanders.candidates.filter(g=>g.expires>=s.turn&&s.tiles[g.city]?.owner===g.owner);
  for(const k of s.kingdoms){
    const cities=settlements(s,k.id).filter(t=>t.building==='city').sort((a,b)=>a.id.localeCompare(b.id));
    s.commanders.nextOffer[k.id]??=3+hash(s.seed||s.rng,k.id,'first-general')%6;
    if(!cities.length||s.turn<s.commanders.nextOffer[k.id]||s.commanders.candidates.some(g=>g.owner===k.id)||s.commanders.roster.filter(g=>g.owner===k.id).length>=16)continue;
    const seed=hash(s.seed||s.rng,k.id,s.turn),roll=seed%100;
    const used=[...s.commanders.roster,...s.commanders.candidates,...(s.commanders.retired||[])];
    const pool=GENERAL_ROSTER.filter(x=>!used.some(g=>g.characterId===x.characterId));if(!pool.length)continue;
    const identity=pool[seed%pool.length],quality=roll<65?0:roll<88?1:roll<96?2:roll<99?3:4;
    let id=`general-${identity.slot}`;while(used.some(g=>g.id===id))id=`general-${s.nextId++}`;
    s.commanders.candidates.push({id,commandId:`command-${id}`,owner:k.id,name:identity.name,characterId:identity.characterId,quality,personality:identity.personality,specialty:identity.specialty,city:cities[(seed>>>20)%cities.length].id,expires:s.turn+4,history:[],objective:null,lastMuster:0,lastPlanned:0});
    s.commanders.nextOffer[k.id]=s.turn+14+(seed%10);
  }
}
export function hireGeneral(s,owner,id) {
  initializeCommanders(s);
  const g=s.commanders.candidates.find(g=>g.id===id&&g.owner===owner),k=kingdom(s,owner);
  if(s.outcome||!g||g.expires<s.turn||s.tiles[g.city]?.owner!==owner||s.tiles[g.city]?.building!=='city')return fail('This general is no longer available in an owned city.');
  const q=GENERAL_QUALITIES[g.quality];
  if(!canAfford(k,{gold:q.cost}))return fail(`Recruitment requires ${q.cost} gold; upkeep is ${q.upkeep} gold per round.`);
  pay(k,{gold:q.cost});s.commanders.candidates=s.commanders.candidates.filter(x=>x!==g);s.commanders.roster.push(g);
  generalMessage(s,g,'general',`I am ready for an army and an objective. My upkeep is ${q.upkeep} gold per round, including while unassigned.`);
  return {ok:true,generalId:id};
}
export function assignGeneral(s,owner,id,armyId,transfer=false) {
  const g=own(s,owner,id),a=s.armies.find(a=>a.id===armyId&&a.owner===owner);
  if(s.outcome||!g||!a)return fail('Select your general and an owned army.');
  if(a.commandId&&a.commandId!==g.commandId)return fail('Return this army to manual control before changing commanders.');
  const previous=allArmies(s).filter(x=>x.commandId===g.commandId&&x.id!==a.id);
  if(previous.length&&!transfer)return fail(`${g.name} already commands ${previous.map(x=>x.name||x.id).join(', ')}. Confirm Transfer Command.`);
  for(const x of previous){delete x.commandId;x.path=[];x.order=x.embarkedFleetId?'embarked':'hold';x.target=null;x.structureTarget=null;delete x.embarkOrder;markPlayerOverride(s,x);}
  a.commandId=g.commandId;a.commandBaseline??=sizeOf(a);syncCommanders(s);
  if(g.objective)planGeneral(s,g,{force:true});return {ok:true};
}
export function detachGeneral(s,owner,id,armyId=null,dismiss=false,confirmed=false) {
  const g=own(s,owner,id);if(!g||s.outcome)return fail('This general is unavailable.');
  if(dismiss&&!confirmed)return fail('Confirm dismissal. Troops remain and future general upkeep stops.');
  if(armyId&&!allArmies(s).some(a=>a.commandId===g.commandId&&a.id===armyId))return fail('This army is not in this general’s command.');
  for(const a of allArmies(s).filter(a=>a.commandId===g.commandId&&(!armyId||a.id===armyId))){delete a.commandId;markPlayerOverride(s,a);}
  if(!armyId)g.objective=null;
  if(dismiss){(s.commanders.retired??=[]).push(g);s.commanders.roster=s.commanders.roster.filter(x=>x!==g);}
  syncCommanders(s);return {ok:true};
}
export function validateGeneralOrder(s,owner,id,raw) {
  const g=own(s,owner,id),view=knowledgeView(s,owner);
  if(!g||!raw||!COMMAND_KINDS.includes(raw.kind)||!Array.isArray(raw.targets)||!raw.targets.length||raw.targets.length>3||new Set(raw.targets).size!==raw.targets.length)return fail('Choose a general, objective and one to three known locations.');
  const limit=raw.lossLimit??35;
  if(!Number.isInteger(limit)||limit<15||limit>65||typeof raw.allowSplit!=='boolean')return fail('Choose a loss limit of 15–65% and whether detachments are permitted.');
  if(raw.targets.some(id=>!view.tiles[id]||view.tiles[id].fog==='unknown'))return fail('Scout a location before including it in a campaign.');
  if(raw.army){
    const ally=view.armies.find(a=>a.id===raw.army);
    if(raw.kind!=='reinforce'||!ally||ally.commandId===g.commandId||ally.owner!==owner&&!['alliance','vassalage'].some(type=>treaty(s,owner,ally.owner,type)))return fail('Choose an observed friendly army outside this command to reinforce.');
  }
  if(['attack','siege'].includes(raw.kind)&&raw.targets.some(id=>view.tiles[id].owner!==owner&&(!view.tiles[id].owner||!atWar(s,owner,view.tiles[id].owner))))return fail('This objective requires an existing war. Declare war separately before approving the campaign.');
  const order={kind:raw.kind,targets:[...raw.targets],lossLimit:limit,allowSplit:raw.allowSplit,approvedTurn:s.turn,status:'Preparing',reason:'Approved objective; orders will be prepared during our activation.'};
  if(raw.army)order.army=raw.army;
  return {ok:true,order};
}
export function approveGeneralOrder(s,owner,id,raw) {
  if(s.outcome)return fail('This campaign has ended.');
  const checked=validateGeneralOrder(s,owner,id,raw);if(!checked.ok)return checked;
  const g=own(s,owner,id);g.objective=checked.order;
  for(const a of forces(s,g))a.commandBaseline=sizeOf(a);
  generalMessage(s,g,'council',`Approved: ${describeGeneralOrder(g.objective)} No new war or discretionary spending is authorized.`);
  planGeneral(s,g,{force:true});return {ok:true};
}
export const describeGeneralOrder=o=>`${o.kind} ${o.army?`army ${o.army} (last designated at ${o.targets[0]})`:o.targets.join(' → ')}; regroup at ${o.lossLimit}% losses; ${o.allowSplit?'detachments permitted':'keep forces together'}; hold captured objectives`;
function status(s,g,value,reason){
  const o=g.objective;if(!o)return;
  if(o.status!==value||o.reason!==reason)generalMessage(s,g,'general',`${value}: ${reason}`);
  o.status=value;o.reason=reason;
}
function order(s,g,a,t,kind,avoid=null){
  const result=orderArmy(s,g.owner,a.id,t.id,kind,avoid,'general');
  if(!result.ok)orderArmy(s,g.owner,a.id,a.tile,'hold',null,'general');
  return result;
}
export function planGeneral(s,g,{force=false}={}) {
  if(!g.objective||!force&&g.lastPlanned===s.turn||s.sequential&&s.sequential.order[s.sequential.index]!==g.owner)return;
  g.lastPlanned=s.turn;syncCommanders(s);
  const o=g.objective,view=planningView(s,g.owner),k=kingdom(s,g.owner),all=forces(s,g);
  if(!all.length){status(s,g,'Blocked','No army assigned. Assign forces before the campaign can proceed.');return;}
  if(o.army){
    const ally=view.armies.find(a=>a.id===o.army&&!a.remembered);
    if(!ally||ally.owner!==g.owner&&!['alliance','vassalage'].some(type=>treaty(s,g.owner,ally.owner,type))){for(const a of all)if(!manualOverride(s,a)&&!a.embarkOrder)order(s,g,a,view.tiles[a.tile],'hold');status(s,g,'Blocked','The designated army is no longer observed as a friendly force. Holding position until you review it.');return;}
    o.targets=[ally.tile];
  }
  const targets=o.targets.map(id=>view.tiles[id]),attack=['attack','siege'].includes(o.kind);
  const remaining=attack?targets.filter(t=>t.owner!==g.owner):targets;
  if(attack&&!remaining.length){
    for(const a of all)if(!manualOverride(s,a)&&!a.embarkOrder)order(s,g,a,view.tiles[a.tile],'hold');
    status(s,g,'Completed','The designated settlements are ours; detachments hold their positions.');return;
  }
  if(attack&&remaining.some(t=>!t.owner||!atWar(s,g.owner,t.owner))){for(const a of all)if(!manualOverride(s,a)&&!a.embarkOrder)order(s,g,a,view.tiles[a.tile],'hold');status(s,g,'Blocked','Peace or changed ownership prevents this attack. Review the objective.');return;}
  const editable=all.filter(a=>!manualOverride(s,a)&&!a.embarkOrder);
  if(!editable.length){status(s,g,'Preparing','Player overrides and queued boarding orders remain authoritative.');return;}
  // Rejoin same-command forces only. Core merging conserves the greatest spent budget.
  for(const tile of new Set(editable.map(a=>a.tile))) {
    const here=armiesOf(s,g.owner).filter(a=>a.tile===tile);
    if(here.length>1&&here.every(a=>a.commandId===g.commandId&&!manualOverride(s,a)&&!a.embarkOrder))mergeArmies(s,g.owner,tile,'general');
  }
  const current=forces(s,g).filter(a=>!manualOverride(s,a)&&!a.embarkOrder),maxDetachments=g.quality>=3?3:2;
  // A divided command needs two independently viable forces on legal routes.
  if(o.allowSplit&&attack&&remaining.length>1&&current.length===1&&all.length<maxDetachments){
    const a=current[0],half={...a,units:Object.fromEntries(Object.entries(a.units).map(([u,n])=>[u,Math.floor(n/2)]))};
    const min=g.personality==='opportunistic'?24:32;
    const safe=sizeOf(half)>=min&&remaining.slice(0,2).every(t=>t.fog==='visible'&&assaultAssessment(view,k,half,t).assault&&findPath(view,a.tile,t.id,g.owner).length)&&!view.armies.some(e=>atWar(view,g.owner,e.owner)&&distance(view.tiles[e.tile],view.tiles[a.tile])<4&&strength(e)>strength(half)*.6);
    if(safe)splitArmy(s,g.owner,a.id,'general');
  }
  let blocked='',marching=0,engaged=false,regrouping=false;
  const active=forces(s,g).filter(a=>!manualOverride(s,a)&&!a.embarkOrder);
  for(const [i,a] of active.entries()){
    const tile=view.tiles[a.tile],target=remaining[Math.min(i,remaining.length-1)],loss=1-sizeOf(a)/Math.max(1,a.commandBaseline||sizeOf(a));
    const threatened=settlements(view,g.owner).find(t=>t.capital===g.owner&&view.armies.some(e=>atWar(view,g.owner,e.owner)&&distance(t,view.tiles[e.tile])<=2));
    if(a.morale<.6||loss>=o.lossLimit/100||a.regrouping){
      const refuge=settlements(view,g.owner).filter(t=>!view.armies.some(e=>atWar(view,g.owner,e.owner)&&distance(t,view.tiles[e.tile])<=1)).sort((x,y)=>distance(tile,x)-distance(tile,y))[0];
      const ready=a.morale>=.82&&sizeOf(a)>=Math.max(24,(a.commandBaseline||24)*.8);
      a.regrouping=!ready;
      if(!ready){
        regrouping=true;
        if(refuge){const result=order(s,g,a,refuge,a.tile===refuge.id?'hold':'retreat');if(!result.ok)blocked=`Regrouping route blocked: ${result.error}`;}
        else {order(s,g,a,tile,'hold');blocked='No safe owned settlement is reachable for regrouping.';}
        continue;
      }
      a.commandBaseline=sizeOf(a);
    }
    if(threatened&&g.personality==='protective'&&distance(tile,threatened)<=5){const result=order(s,g,a,threatened,'move');blocked=result.ok?'Covering an immediate threat to our capital; the original objective is retained.':`Capital defense route blocked: ${result.error}`;continue;}
    const caution=g.personality==='cautious'?.82:g.personality==='aggressive'?1.12:g.personality==='protective'?.9:1;
    if(attack&&target.fog==='visible'){
      const estimate=assaultAssessment(view,{...k,aggression:Math.min(1,k.aggression*caution)},a,target);
      if(g.personality==='methodical'&&target.walls>0&&orderBombardment(s,k,a,target,dangerousTiles(view,k,a),'general').ok){marching++;continue;}
      if(estimate.bombard&&orderBombardment(s,k,a,target,dangerousTiles(view,k,a),'general').ok){marching++;continue;}
      if(!estimate.assault||estimate.lossFraction>o.lossLimit/100){
        const rally=settlements(view,g.owner).sort((x,y)=>distance(x,target)-distance(y,target))[0];
        const result=rally?order(s,g,a,rally,'move'):order(s,g,a,tile,'hold');
        blocked=`Known defenses at ${target.name||target.id} exceed the approved loss limit; ${result.ok?'gathering reinforcements or siege support.':`rally route blocked: ${result.error}`}`;continue;
      }
    }
    if(a.tile===target.id){order(s,g,a,target,'hold');continue;}
    const result=order(s,g,a,target,attack?'attack':o.kind==='withdraw'?'retreat':'move',dangerousTiles(view,k,a));
    if(!result.ok)blocked=result.error;else{marching++;engaged ||= attack&&distance(tile,target)<=1;}
  }
  if(blocked)status(s,g,'Blocked',blocked);
  else if(regrouping)status(s,g,'Preparing','Recovering morale and gathering reinforcements before returning to the approved objective.');
  else status(s,g,engaged?'Engaged':marching?'Marching':attack?'Preparing':'Completed',marching?`Orders issued to ${marching} detachment${marching===1?'':'s'}; movement occurs at the end of our activation.`:'Forces are holding the designated position.');
}
export function prepareGenerals(s,owner) {for(const g of s.commanders?.roster||[])if(g.owner===owner)planGeneral(s,g);}
export function recruitAIGeneral(s,owner) {
  const k=kingdom(s,owner),candidate=s.commanders?.candidates.find(g=>g.owner===owner),army=armiesOf(s,owner).filter(a=>!a.commandId).sort((a,b)=>sizeOf(b)-sizeOf(a))[0];
  if(!candidate||!army||sizeOf(army)<24||k.resources.food<70||k.resources.gold<GENERAL_QUALITIES[candidate.quality].cost+140||economyProjection(s,owner).income.gold<GENERAL_QUALITIES[candidate.quality].upkeep+3||difficulty(s).coordination===0&&candidate.quality>1)return;
  if(hireGeneral(s,owner,candidate.id).ok)assignGeneral(s,owner,candidate.id,army.id);
}
export function prepareAIGeneralObjectives(s,owner){
  if(!isAiHouse(s,owner))return;
  const view=planningView(s,owner);
  for(const g of s.commanders?.roster||[]){
    if(g.owner!==owner)continue;
    const tiles=new Set(forces(s,g).map(a=>a.tile));
    for(const a of armiesOf(s,owner).filter(a=>!a.commandId&&tiles.has(a.tile)))assignGeneral(s,owner,g.id,a.id);
    const active=g.objective&&g.objective.status!=='Completed'&&g.objective.targets.every(id=>view.tiles[id]?.owner!==owner&&atWar(view,owner,view.tiles[id]?.owner));
    if(active)continue;
    const plan=s.intrigue.plans.find(p=>p.actor===owner&&['Committed','Executing'].includes(p.status)&&['invasion','jointWar'].includes(p.type)&&atWar(s,owner,p.target));
    g.objective=null;
    if(plan)approveGeneralOrder(s,owner,g.id,{kind:plan.requiredSiege?'siege':'attack',targets:[plan.targetTile],lossLimit:g.personality==='protective'?25:g.personality==='aggressive'?45:35,allowSplit:difficulty(s).coordination>=2&&g.personality==='opportunistic'});
  }
}
export function musterGenerals(s) {
  initializeCommanders(s);if(s.commanders.lastRound>=s.turn)return;
  s.commanders.lastRound=s.turn;
  for(const g of s.commanders.roster){
    if(g.specialty!=='mustering'||g.lastMuster>=s.turn)continue;
    g.lastMuster=s.turn;
    const eligible=forces(s,g).filter(a=>s.tiles[a.tile]?.owner===g.owner&&['city','town'].includes(s.tiles[a.tile].building)).sort((a,b)=>sizeOf(a)-sizeOf(b)||a.id.localeCompare(b.id));
    const a=eligible[0];if(!a)continue;
    const amount=s.tiles[a.tile].building==='city'?Math.min(5,2+g.quality):g.quality>=2?2:1;
    recruitLocalMustering(s,g.owner,a.id,amount);
  }
}
export function interpretGeneralOrder(s,owner,id,message) {
  const g=own(s,owner,id);if(!g)return null;
  const text=String(message).toLowerCase(),view=knowledgeView(s,owner);
  // Explicit verbs + exact known names/coordinates, never a guessed hidden target.
  if(!/\b(take|attack|capture|defend|hold|rally|gather|withdraw|reinforce|besiege|advance)\b/.test(text))return null;
  const support=/reinforce/.test(text)&&view.armies.find(a=>new RegExp(`\\b${a.id}\\b`).test(text));
  const targets=support?[support.tile]:Object.values(view.tiles).filter(t=>t.fog!=='unknown'&&(new RegExp(`(^|[^0-9])${t.id}([^0-9]|$)`).test(text)||t.name&&text.includes(t.name.toLowerCase()))).slice(0,3).map(t=>t.id);
  if(!targets.length)return null;
  const kind=/withdraw|retreat/.test(text)?'withdraw':/defend|hold|pass/.test(text)&&!/take|capture|attack/.test(text)?'defend':/rally|gather/.test(text)?'rally':/reinforce/.test(text)?'reinforce':/besiege/.test(text)?'siege':'attack';
  const raw={kind,targets,lossLimit:g.objective?.lossLimit||35,allowSplit:/divide|split|detach/.test(text),...(support?{army:support.id}:{})};
  return validateGeneralOrder(s,owner,id,raw).ok?raw:null;
}
export function generalContext(s,owner,id,message) {
  const g=own(s,owner,id);if(!g)return null;
  const view=planningView(s,owner),all=allArmies(view).filter(a=>a.commandId===g.commandId);
  const visibleTargets=Object.values(view.tiles).filter(t=>t.fog!=='unknown'&&(t.building==='city'||t.building==='town'||g.objective?.targets.includes(t.id))).sort((a,b)=>Math.min(...all.map(x=>distance(view.tiles[x.tile],a)),99)-Math.min(...all.map(x=>distance(view.tiles[x.tile],b)),99)).slice(0,18);
  return {mode:'general',actorHouseId:owner,generalId:id,turn:s.turn,message:String(message).slice(0,600),history:g.history.slice(-8).map(m=>({...m,text:m.text.slice(0,600)})),world:{general:{name:g.name,personality:generalTemperament(g),traits:generalTraits(g),quality:GENERAL_QUALITIES[g.quality].name,specialty:g.specialty},objective:g.objective,forces:all.slice(0,24).map(a=>({id:a.id,name:a.name||a.id,tile:a.tile,units:{...a.units},strength:Math.round(strength(a)),troops:sizeOf(a),morale:a.morale,formation:a.formation,order:a.order,target:a.target,losses:Math.max(0,(a.commandBaseline||sizeOf(a))-sizeOf(a)),playerOverride:manualOverride(s,a)})),locations:visibleTargets.map(t=>({id:t.id,name:t.name||t.id,owner:t.owner,observedTurn:t.observedTurn??s.turn,visible:t.fog==='visible',enemies:view.armies.filter(a=>a.tile===t.id&&atWar(view,owner,a.owner)).slice(0,6).map(a=>({troops:sizeOf(a),estimated:!!a.remembered}))})),friendlyArmies:view.armies.filter(a=>!a.remembered&&(a.owner===owner||['alliance','vassalage'].some(type=>treaty(view,owner,a.owner,type)))).slice(0,18).map(a=>({id:a.id,owner:a.owner,tile:a.tile})),nearbyEnemies:view.armies.filter(a=>atWar(view,owner,a.owner)&&all.some(f=>distance(view.tiles[f.tile],view.tiles[a.tile])<=5)).slice(0,12).map(a=>({id:a.id,tile:a.tile,troops:sizeOf(a),estimated:!!a.remembered})),wars:view.wars.filter(w=>w.split(':').includes(owner)),supplies:{food:kingdom(view,owner).resources.food},recentBattles:view.militaryEvents.filter(e=>[e.attacker,e.defender].includes(owner)&&s.turn-e.turn<=3).slice(-5).map(e=>({turn:e.turn,tile:e.tile,before:e.before,after:e.after,winner:e.winner})),supportedOrders:COMMAND_KINDS,proposedOrder:interpretGeneralOrder(s,owner,id,message)}};
}
export function localGeneralReply(s,owner,id,message) {
  const g=own(s,owner,id);if(!g)return {reply:'This commander is unavailable.',order:null};
  const order=interpretGeneralOrder(s,owner,id,message),o=g.objective;
  const all=allArmies(s).filter(a=>a.commandId===g.commandId),composition=Object.entries(all.reduce((units,a)=>{for(const [u,n] of Object.entries(a.units))units[u]=(units[u]||0)+n;return units;},{})).filter(([,n])=>n>0).map(([u,n])=>`${n} ${UNITS[u]?.name||u}`).join(', ');
  const summary=all.length?`I command ${composition}. ${all.some(a=>Object.entries(a.units).some(([u,n])=>n>0&&UNITS[u]?.family==='siege'))?'Siege support is available.':'We have no siege engines; fortified assaults need careful review.'} `:'';
  const preference={aggressive:'I favor pressing a confirmed advantage within your loss limit.',cautious:'Fresh observations and a safe route should precede the assault.',methodical:'I prefer concentrating our forces and reducing walls before the decisive assault.',opportunistic:'Exposed objectives may justify two viable detachments; uncertain defenses do not.',protective:'I will preserve a retreat route and cover immediate threats to our capital.'}[g.personality];
  return {reply:summary+(order?`I propose: ${describeGeneralOrder(order)}. Review and approve the order before it changes our campaign.`:o?`${o.status}: ${o.reason} Our objective remains ${o.kind} at ${o.targets.join(', ')}. ${preference} ${/divid|split/.test(message)?'Detachments require two viable forces, current observations and legal routes. Approve splitting in the orders panel to permit it.':'Your manual orders remain authoritative.'}`:`Assign an army, then name a known location and an objective. ${preference} I can prepare an attack, defense, rally, reinforcement, siege support or withdrawal for your approval.`),order};
}
export function validateGeneralResponse(raw) {
  if(!raw||typeof raw.reply!=='string'||!raw.reply.trim()||raw.reply.length>1600)return null;
  let order=null;
  if(raw.order){const o=raw.order;if(COMMAND_KINDS.includes(o.kind)&&Array.isArray(o.targets)&&o.targets.length>0&&o.targets.length<=3&&o.targets.every(id=>typeof id==='string'&&/^\d+,\d+$/.test(id))&&Number.isInteger(o.lossLimit)&&o.lossLimit>=15&&o.lossLimit<=65&&typeof o.allowSplit==='boolean')order={kind:o.kind,targets:o.targets,lossLimit:o.lossLimit,allowSplit:o.allowSplit,...(o.kind==='reinforce'&&typeof o.army==='string'&&/^army-\d+$/.test(o.army)?{army:o.army}:{})};}
  return {reply:raw.reply.trim(),order};
}
export function recordGeneralConversation(s,owner,id,message,response) {
  const g=own(s,owner,id);if(!g||typeof message!=='string'||!message.trim()||message.length>600)return fail('Enter a message of up to 600 characters.');
  let parsed=validateGeneralResponse(response)||localGeneralReply(s,owner,id,message);
  if(parsed.order&&!validateGeneralOrder(s,owner,id,parsed.order).ok)parsed={...parsed,order:null};
  const claimedCapture=/\b(?:we|i|our (?:army|forces)) (?:have |has |already )*(?:captured|conquered|seized|taken)\b/i.test(parsed.reply);
  const capture=s.militaryEvents.some(e=>e.attacker===owner&&e.action==='capture'&&s.turn-e.turn<=2&&g.objective?.targets.includes(e.tile));
  if(claimedCapture&&!capture)parsed=localGeneralReply(s,owner,id,message);
  // Replies never execute. A model proposal is validated again on approval.
  generalMessage(s,g,'player',message);generalMessage(s,g,'general',parsed.reply);return {ok:true,order:parsed.order};
}
