import test from 'node:test';
import assert from 'node:assert/strict';
import {createGame} from './fixtures/legacy-game.mjs';
import {onlineGame,activateForTest} from './fixtures/online-game.mjs';
import {UNITS} from '../data.mjs';
import {parseSave,sizeOf,mergeArmies,orderArmy,splitArmy} from '../core.mjs';
import {createArmyDraft,moveDraftUnits,addDraftFormation,applyDraftPreset,reorganizeArmy,validateArmyDraft} from '../army-organization.mjs';
import {refreshGeneralCandidates,hireGeneral,assignGeneral,generalContext,recordGeneralConversation,prepareGenerals,detachGeneral} from '../generals.mjs';
import {GENERAL_ROSTER} from '../general-roster.mjs';
import {knowledgeView,refreshKnowledge} from '../fog.mjs';
import {splitCampaign,joinCampaign} from '../multiplayer-state.mjs';
import {applyCommand} from '../multiplayer-commands.mjs';
import {generalRoster,generalArmyControls} from '../command-ui.mjs';
import {sanitizeContext} from '../worker/worker.mjs';
const blank=()=>Object.fromEntries(Object.keys(UNITS).map(u=>[u,0]));
function fixture(){const s=createGame(),a=s.armies[0];a.units={...blank(),spearman:45,archer:22,knight:12};return {s,a,d:createArmyDraft(s,a.owner,a.id)};}
function hire(s,owner='ashen'){
 s.turn=Math.max(s.turn,8);s.kingdoms.find(k=>k.id===owner).resources.gold=10000;s.commanders.nextOffer[owner]=s.turn;refreshGeneralCandidates(s);
 const g=s.commanders.candidates.find(g=>g.owner===owner);assert.ok(g);assert.equal(hireGeneral(s,owner,g.id).ok,true);return g;
}
const unchanged=(s,action)=>{const before=JSON.stringify(s);assert.equal(action().ok,false);assert.equal(JSON.stringify(s),before);};
test('45 spearmen split 22/23; reset/cancel leave the entire live campaign untouched',()=>{
 const {s,a,d}=fixture(),before=JSON.stringify(s),initial=structuredClone(d);
 assert.equal(moveDraftUnits(d,0,1,'spearman',23).ok,true);assert.equal(d.formations[0].units.spearman,22);assert.equal(d.formations[1].units.spearman,23);
 assert.equal(JSON.stringify(s),before);const reset=structuredClone(initial);assert.deepEqual(reset,createArmyDraft(s,a.owner,a.id));assert.equal(JSON.stringify(s),before);
 assert.equal(reorganizeArmy(s,a.owner,d).ok,true);assert.equal(s.armies.find(x=>x.id===a.id).units.spearman,22);assert.equal(s.turn,1);
});
test('custom 30/15, entire types and three formations preserve every unit, IDs and spent movement',()=>{
 const {s,a,d}=fixture();a.movementTurn=s.turn;a.movementSpent=2;a.resolvedTurn=s.turn;Object.assign(d,createArmyDraft(s,a.owner,a.id));
 moveDraftUnits(d,0,1,'spearman',15);moveDraftUnits(d,0,1,'archer',22);addDraftFormation(d);moveDraftUnits(d,0,2,'knight',12);d.formations[1].name='Eastern Column';
 const r=reorganizeArmy(s,a.owner,d);assert.equal(r.ok,true,r.error);assert.equal(r.armyId,a.id);assert.equal(new Set(r.armyIds).size,3);
 const forces=s.armies.filter(x=>r.armyIds.includes(x.id));assert.deepEqual(forces.reduce((o,a)=>{for(const [u,n] of Object.entries(a.units))o[u]+=n;return o;},blank()),a.units);
 assert.ok(forces.every(x=>x.tile===a.tile&&x.movementSpent===2&&x.resolvedTurn===s.turn));assert.equal(forces[0].units.spearman,30);assert.equal(forces[1].units.spearman,15);
 const loaded=parseSave(JSON.stringify(s));assert.equal(loaded.armies.find(x=>x.id===r.armyIds[1]).name,'Eastern Column');assert.deepEqual(loaded.armies.filter(x=>r.armyIds.includes(x.id)).map(x=>x.units),forces.map(x=>x.units));
});
test('empty slots are discarded; moving every troop out of A retains the original ID',()=>{
 const {s,a,d}=fixture();addDraftFormation(d);for(const [u,n] of Object.entries(a.units))if(n)moveDraftUnits(d,0,1,u,n);
 const count=s.armies.length,r=reorganizeArmy(s,a.owner,d);assert.equal(r.ok,true);assert.equal(s.armies.length,count);assert.deepEqual(r.armyIds,[a.id]);assert.equal(sizeOf(s.armies.find(x=>x.id===a.id)),79);
});
test('negative, fractional, excessive, unknown, duplicated and relocated payloads are rejected atomically',()=>{
 for(const change of [d=>d.formations[0].units.spearman=-1,d=>d.formations[0].units.spearman=.5,d=>d.formations[0].units.spearman=46,d=>d.formations[1].units.spearman=1,d=>d.formations[0].units.dragon=10,d=>d.formations[1].id='army-999',d=>d.formations[0].name='x'.repeat(61),d=>d.formations[0].formation='flanking']){
  const {s,a,d}=fixture();if(change.toString().includes('flanking')){d.formations[0].units.knight=0;d.formations[1].units.knight=12;}change(d);unchanged(s,()=>reorganizeArmy(s,a.owner,d));
 }
 const {s,a,d}=fixture();d.formations[0].tile='20,20';assert.equal(reorganizeArmy(s,a.owner,d).ok,true);assert.equal(s.armies.find(x=>x.id===a.id).tile,a.tile);
});
test('stale source, other House and draft-only forged troops fail without any state mutation',()=>{
 const {s,a,d}=fixture();unchanged(s,()=>reorganizeArmy(s,'wintermere',d));a.units.spearman++;unchanged(s,()=>reorganizeArmy(s,a.owner,d));
});
test('commander remains in A; a second officer can command B and retains shared history after reload',()=>{
 const {s,a}=fixture(),g=hire(s),h=hire(s);assignGeneral(s,a.owner,g.id,a.id);
 recordGeneralConversation(s,a.owner,g.id,'Report',{source:'gemini',reply:'Ready.',order:null});const history=structuredClone(g.history),d=createArmyDraft(s,a.owner,a.id);
 moveDraftUnits(d,0,1,'archer',22);d.formations[1].generalId=h.id;d.formations[1].name='Bow Company';
 assert.equal(reorganizeArmy(s,a.owner,d).ok,true);const b=s.armies.find(x=>x.commandId===h.commandId);assert.ok(b);assert.equal(s.armies.find(x=>x.id===a.id).commandId,g.commandId);
 const restored=parseSave(JSON.stringify(s));assert.deepEqual(restored.commanders.roster.find(x=>x.id===g.id).history,history);
 assert.ok(generalRoster(restored,a.owner).includes(`data-general-open="${g.id}"`));assert.ok(generalArmyControls(restored,restored.armies.find(x=>x.id===a.id)).includes(`data-general-open="${g.id}"`));
 refreshKnowledge(s);const context=generalContext(s,a.owner,h.id,'What do you command?');assert.equal(context.world.forces[0].units.archer,22);assert.equal(context.world.forces[0].name,'Bow Company');assert.ok(sanitizeContext(context));
});
test('duplicate generals, foreign generals and unacknowledged transfers fail; confirmed transfer removes previous command',()=>{
 const {s,a}=fixture(),g=hire(s),foreign=hire(s,'wintermere');assignGeneral(s,a.owner,g.id,a.id);
 let d=createArmyDraft(s,a.owner,a.id);moveDraftUnits(d,0,1,'spearman',23);d.formations[1].generalId=g.id;unchanged(s,()=>reorganizeArmy(s,a.owner,d));d.formations[1].generalId=foreign.id;unchanged(s,()=>reorganizeArmy(s,a.owner,d));
 d.formations[1].generalId=null;const r=reorganizeArmy(s,a.owner,d),b=s.armies.find(x=>x.id===r.armyIds[1]);d=createArmyDraft(s,a.owner,b.id);d.formations[0].generalId=g.id;unchanged(s,()=>reorganizeArmy(s,a.owner,d));d.transfers=[g.id];assert.equal(reorganizeArmy(s,a.owner,d).ok,true);assert.equal(s.armies.find(x=>x.id===a.id).commandId,undefined);
 assert.equal(assignGeneral(s,a.owner,g.id,a.id).ok,false);assert.equal(assignGeneral(s,a.owner,g.id,a.id,true).ok,true);assert.equal(s.armies.find(x=>x.id===b.id).commandId,undefined);
});
test('merge adopts a sole commander and requires an explicit choice for two commanders',()=>{
 const {s,a}=fixture(),g=hire(s),h=hire(s);assignGeneral(s,a.owner,g.id,a.id);let d=createArmyDraft(s,a.owner,a.id);moveDraftUnits(d,0,1,'spearman',23);d.formations[1].generalId=h.id;reorganizeArmy(s,a.owner,d);
 unchanged(s,()=>mergeArmies(s,a.owner,a.tile));assert.equal(mergeArmies(s,a.owner,a.tile,'player',h.id).ok,true);const merged=s.armies.find(x=>x.id===a.id);assert.equal(merged.commandId,h.commandId);assert.equal(sizeOf(merged),79);assert.equal(s.commanders.roster.length,2);
 const r=splitArmy(s,a.owner,merged.id);assert.equal(r.ok,true);assert.equal(s.armies.find(x=>x.id===r.armyId).commandId,undefined);assert.equal(mergeArmies(s,a.owner,a.tile).ok,true);assert.equal(merged.commandId,h.commandId);
});
test('manual reorganization preserves objectives and cannot be undone by same-activation general planning',()=>{
 const {s,a}=fixture(),g=hire(s);assignGeneral(s,a.owner,g.id,a.id);g.objective={kind:'rally',targets:[a.tile],lossLimit:35,allowSplit:true,approvedTurn:s.turn,status:'Preparing',reason:'Approved'};
 const d=createArmyDraft(s,a.owner,a.id);applyDraftPreset(d,'half');const objective=structuredClone(g.objective);reorganizeArmy(s,a.owner,d);const units=s.armies.map(a=>a.units);prepareGenerals(s,a.owner);assert.deepEqual(s.armies.map(a=>a.units),units);assert.deepEqual(g.objective.targets,objective.targets);
 s.turn++;prepareGenerals(s,a.owner);assert.equal(s.armies.filter(x=>x.owner===a.owner).length,2);
});
test('boarding orders are safely cleared, never duplicate pending embarkation, and save roundtrips',()=>{
 const {s,a}=fixture();a.embarkOrder={fleet:'fleet-55',count:25};const d=createArmyDraft(s,a.owner,a.id);applyDraftPreset(d,'quarter');const r=reorganizeArmy(s,a.owner,d);assert.equal(r.ok,true);assert.ok(s.armies.filter(x=>r.armyIds.includes(x.id)).every(x=>!x.embarkOrder));assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('fixed pool has exactly sixteen characters across recruitment, expiration, dismissal and reload',()=>{
 const s=createGame();for(let turn=8;turn<=130;turn+=6){s.turn=turn;for(const k of s.kingdoms){k.resources.gold=10000;s.commanders.nextOffer[k.id]=turn;}refreshGeneralCandidates(s);for(const g of [...s.commanders.candidates]){hireGeneral(s,g.owner,g.id);detachGeneral(s,g.owner,g.id,null,true,true);}}
 assert.equal(s.commanders.retired.length,16);assert.equal(new Set(s.commanders.retired.map(g=>g.characterId)).size,16);assert.equal(s.commanders.candidates.length,0);assert.equal(s.commanders.roster.length,0);assert.deepEqual(new Set(s.commanders.retired.map(g=>g.name)),new Set(GENERAL_ROSTER.map(g=>g.name)));assert.equal(parseSave(JSON.stringify(s)).commanders.retired.length,16);
 const privateView=knowledgeView(s,'ashen');assert.ok(privateView.commanders.retired.every(g=>g.owner==='ashen'));
});
let serial=0;
const envelope=(s,m,actor,args)=>({id:`sort-${++serial}`,clientId:'sorting-client',sequence:serial,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,activationId:m.activationId,type:'reorganize',args});
test('one authoritative online command conserves troops, projects atomically and rejects replay, forgery, wrong owner and inactive turns',()=>{
 const {state:s,meta:m}=onlineGame(2);activateForTest(s,m,'ashen');const owner=m.activeHouse,a=s.armies.find(a=>a.owner===owner),d=createArmyDraft(knowledgeView(s,owner),owner,a.id);applyDraftPreset(d,'half');const before=s.armies.length;
 const other=Object.keys(m.seats).find(id=>id!==owner&&m.seats[id].kind==='human');unchanged(s,()=>applyCommand(s,m,envelope(s,m,other,d)));
 const forged=structuredClone(d);forged.formations[0].units.levy++;unchanged(s,()=>applyCommand(s,m,envelope(s,m,owner,forged)));
 const stale=envelope(s,m,owner,d);stale.stateVersion--;unchanged(s,()=>applyCommand(s,m,stale));
 const cmd=envelope(s,m,owner,d),r=applyCommand(s,m,cmd);assert.equal(r.ok,true,r.error);assert.equal(s.armies.length,before+1);unchanged(s,()=>applyCommand(s,m,cmd));
 const packed=splitCampaign(s),view=packed.privateByHouse[owner].view;assert.equal(view.armies.filter(x=>x.owner===owner).reduce((n,x)=>n+sizeOf(x),0),sizeOf(a));assert.deepEqual(joinCampaign(packed.canonical,packed.privateByHouse).armies,s.armies);
});
test('legacy identities retain runtime IDs, histories and objectives, while duplicate named identities fail',()=>{
 const {s,a}=fixture(),g=hire(s);assignGeneral(s,a.owner,g.id,a.id);g.history.push({turn:s.turn,role:'player',text:'Keep my previous instructions.'});
 const saved=JSON.parse(JSON.stringify(s));delete saved.commanders.version;delete saved.commanders.retired;for(const x of [...saved.commanders.roster,...saved.commanders.candidates])delete x.characterId;
 const restored=parseSave(JSON.stringify(saved)),mapped=restored.commanders.roster[0];assert.equal(mapped.id,g.id);assert.deepEqual(mapped.history,g.history);assert.equal(restored.armies[0].commandId,g.commandId);assert.ok(GENERAL_ROSTER.some(x=>x.name===mapped.name));
 const duplicate=structuredClone(restored);duplicate.commanders.candidates[0].characterId=mapped.characterId;assert.throws(()=>parseSave(JSON.stringify(duplicate)),/commander/);
});
test('general context reveals only observed threats and own composition, including embarked troops',()=>{
 const {s,a}=fixture(),g=hire(s);assignGeneral(s,a.owner,g.id,a.id);refreshKnowledge(s);
 const enemy=s.armies.find(x=>x.owner==='wintermere');enemy.units.levy=54321;s.wars.push('ashen:wintermere');
 const context=generalContext(s,a.owner,g.id,'What is nearby?');assert.ok(!JSON.stringify(context).includes('54321'));assert.equal(context.world.forces[0].units.spearman,45);
 // A transported command remains readable and transferable without changing its embarkation order.
 const cargo={...structuredClone(a),id:'army-9001',order:'embarked',embarkedFleetId:'fleet-9000'};
 s.fleets.push({id:'fleet-9000',owner:a.owner,tile:a.tile,cargo:[cargo],ships:[]});delete a.commandId;
 assert.equal(generalContext(s,a.owner,g.id,'Report').world.forces[0].id,cargo.id);
 const d=createArmyDraft(s,a.owner,a.id);d.formations[0].generalId=g.id;d.transfers=[g.id];assert.equal(reorganizeArmy(s,a.owner,d).ok,true);assert.equal(cargo.commandId,undefined);assert.equal(cargo.order,'embarked');
 assert.equal(assignGeneral(s,a.owner,g.id,a.id).ok,true);
});
