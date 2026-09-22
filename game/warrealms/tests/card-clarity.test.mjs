import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import vm from 'node:vm';
import { abilityReadiness, comparePrintedForms, readinessSummary } from '../ui/card-clarity.js';
import { loadCardPack } from './test-utils.mjs';
const source = await fs.readFile(new URL('../../warrealms.html', import.meta.url), 'utf8');
const pack = await loadCardPack();
function fn(name) {
  const start = source.indexOf(`  function ${name}(`);
  assert.ok(start >= 0, name);
  const next = source.slice(start + 1).search(/\n  (?:async )?function /);
  return source.slice(start, start + 1 + next);
}
function harness() {
  const owner = {id:'me', played:[], bases:[], attachments:[], hand:[], pendingChoices:[]};
  const game = {turnSerial:3, phase:'playing', players:[owner]};
  const context = {state:game, actionBusy:false, abilityReadiness, CARD_MAP:pack.CARD_MAP, FACTIONS:pack.FACTIONS,
    getCard:value=>pack.getCard(typeof value==='object'?value.id:value), isExpansionBase:card=>!!card?.expansion,
    currentPlayer:()=>owner, myPlayer:()=>owner, pendingChoiceFor:player=>player.pendingChoices[0], forcedDiscardRemaining:()=>0,
    effectDisplaySummary:()=>'', chargeResolvedEffect:action=>action.effect||{}, doubleAllyEffect:card=>card.doubleAlly,
    hasEffect:effect=>!!effect && Object.keys(effect).length>0 };
  vm.createContext(context);
  for (const name of ['chargeActionAvailability','cardAbilityStatuses','activeFactionEntries','cardAllyStatuses','peekTransformProgress','transformProgressKey']) vm.runInContext(fn(name),context);
  return {context,owner,game};
}
test('disabled, used, waiting and ready are distinct with non-color symbols',()=>{
  const cases = [{blocked:'Disabled',stored:10,required:2},{used:true,stored:10,required:2},{stored:1,required:2},{stored:2,required:2}].map(abilityReadiness);
  assert.deepEqual(cases.map(x=>x.kind),['unavailable','used','waiting','ready']);
  assert.equal(new Set(cases.map(x=>x.icon)).size,4);
});
test('different costs preserve independent readiness and never mutate synchronized state',()=>{
  const {context,owner,game}=harness();
  const card={id:'fixture',type:'unit',heat:{actions:[{cost:2,oncePerTurn:true},{cost:6}]}};
  const entry={id:'fixture',instanceId:'x',heat:4,heatActionTurn:0,disabledTurn:0};owner.played.push(entry);
  const snapshot=JSON.stringify(game);
  let rows=context.cardAbilityStatuses(game,owner,entry,card);
  assert.deepEqual(Array.from(rows,row=>row.kind),['ready','waiting']);
  assert.equal(rows[1].reason,'Requires 2 more Heat.');
  assert.equal(readinessSummary(rows).value,'1/2');
  assert.equal(JSON.stringify(game),snapshot);
  entry.heatActionTurn=3;rows=context.cardAbilityStatuses(game,owner,entry,card);assert.equal(rows[0].kind,'used');
  entry.disabledTurn=3;rows=context.cardAbilityStatuses(game,owner,entry,card);assert.ok(rows.every(row=>row.kind==='unavailable'));
  entry.disabledTurn=0;game.turnSerial=4;assert.equal(context.cardAbilityStatuses(game,owner,entry,card)[0].kind,'ready');
});
test('attachments inherit disabled state from their actual host and require a host',()=>{
  const {context,owner,game}=harness();
  const entry={id:'a',instanceId:'a',attachedTo:'b'};owner.attachments.push(entry);owner.bases.push({instanceId:'b',disabledTurn:3});
  const card={type:'attachment',attachment:{action:{oncePerTurn:true}}};
  assert.equal(context.cardAbilityStatuses(game,owner,entry,card)[0].kind,'unavailable');
  owner.bases[0].disabledTurn=0;assert.equal(context.cardAbilityStatuses(game,owner,entry,card)[0].kind,'ready');
  owner.bases=[];assert.equal(context.cardAbilityStatuses(game,owner,entry,card)[0].kind,'unavailable');
});
test('form comparison includes losses and unchanged values',()=>{
 const rows=comparePrintedForms({Defense:'5',Primary:'4 Trade',Ally:'Draw 1'},{Defense:'8',Primary:'4 Trade'});
 assert.deepEqual(rows.find(row=>row.label==='Ally'),{label:'Ally',before:'Draw 1',after:'None',changed:true});
 assert.equal(rows.find(row=>row.label==='Primary').changed,false);
});
test('Ally counts use active rules, ignore disabled cards, and update on repeated snapshots',()=>{
 const {context,owner,game}=harness();
 const card=Object.values(pack.CARD_MAP).find(card=>card.faction!=='neutral'&&card.ally&&card.doubleAlly);
 assert.ok(card);
 const entry={id:card.id,instanceId:'one',disabledTurn:0,allyTurn:0,doubleAllyTurn:0};owner.played.push(entry);
 let rows=context.cardAllyStatuses(game,owner,entry,card);assert.equal(rows[0].kind,'waiting');
 owner.played.push({...entry,instanceId:'two'},{...entry,instanceId:'three'});
 rows=context.cardAllyStatuses(game,owner,entry,card);assert.ok(rows.every(row=>row.kind==='ready'));
 owner.played[2].disabledTurn=game.turnSerial;
 rows=context.cardAllyStatuses(game,owner,entry,card);assert.equal(rows[0].kind,'ready');assert.equal(rows[1].kind,'waiting');
 const snapshot=JSON.stringify(game);
 for(let i=0;i<4;i++) {const copy=JSON.parse(snapshot);context.cardAllyStatuses(copy,copy.players[0],copy.players[0].played[0],card);assert.equal(JSON.stringify(copy),snapshot);}
});
test('transformation choice revalidates source and rejects stale or duplicate submissions',()=>{
 const {context,owner,game}=harness();
 context.OWNER_TURN_TRANSFORM_ZONES=['draw','discard','hand','played','bases'];
 vm.runInContext(fn('locateOwnedEntry'),context);
 let applied=0;context.applyEffect=()=>{applied++};context.triggerChargeEvent=()=>{};context.addLog=()=>{};
 const card=Object.values(pack.CARD_MAP).find(card=>card.transform?.choose?.length>1);
 const target=card.transform.choose[0];const into=typeof target==='string'?target:target.into||target.id;
 owner.bases.push({id:card.id,instanceId:'source'});
 const choice={id:'choice',cardId:card.id,options:[{effect:{__resolveTransformChoice:{sourceCardId:card.id,sourceInstanceId:'source',into}}}]};
 const start=source.indexOf('      case "RESOLVE_CHOICE": {');const end=source.indexOf('\n      case ',start+1);
 vm.runInContext(`function resolve(game,actor,action){switch(action.type){${source.slice(start,end)}}return '';}`,context);
 const action={type:'RESOLVE_CHOICE',choiceId:'choice',optionIndex:0};owner.pendingChoices=[structuredClone(choice)];
 assert.equal(context.resolve(game,owner,action),'');assert.equal(applied,1);
 assert.match(context.resolve(game,owner,action),/no card choice/);assert.equal(applied,1);
 owner.pendingChoices=[structuredClone(choice)];owner.bases=[];
 assert.match(context.resolve(game,owner,action),/no longer available/);assert.equal(owner.pendingChoices.length,1);assert.equal(applied,1);
});
