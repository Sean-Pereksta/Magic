import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import vm from 'node:vm';
import { loadCardPack } from './test-utils.mjs';
import { resolveHeatValue, resolveTriggeredHeatGain, heatThresholdsReached, heatOverloadReady } from '../engine/heat.js';
import { emitSacrificeEvents } from '../engine/sacrifice.js';
import { GAME_EVENT_TYPES } from '../engine/event-system.js';

const pack = await loadCardPack();
const keeper = pack.ALL_CARD_MAP.vharak_keeper_of_the_vowmaw;
const source = await fs.readFile(new URL('../../warrealms.html', import.meta.url), 'utf8');
function fn(name) {
  const start = source.indexOf(`  function ${name}(`);
  assert.ok(start >= 0, name);
  const next = source.slice(start + 1).search(/\n  (?:async )?function /);
  return source.slice(start, start + 1 + next);
}
function harness() {
  const game={turnSerial:1,events:[],log:[]};
  const player={id:'owner',name:'Player',hand:[],draw:[],discard:[],bases:[],attachments:[],played:[],pendingChoices:[]};
  let sequence=0;
  const context={
    state:game, GAME_EVENT_TYPES, resolveHeatValue, resolveTriggeredHeatGain, heatThresholdsReached, heatOverloadReady, emitSacrificeEvents,
    getCard: value => pack.ALL_CARD_MAP[typeof value === 'string' ? value : value?.id],
    cardEntryId:value=>typeof value==='string'?value:value.id,
    makeId:()=>`instance-${++sequence}`,
    isExpansionBase:card=>!!card?.expansion,
    allPlayerEntries:player=>['hand','draw','discard','played','bases','attachments'].flatMap(zone=>player[zone]||[]),
    entryTags:entry=>[entry.id],
    addLog:(game,text)=>game.log.push(text),
    emitGameEvent:(game,event)=>game.events.push(event),
    triggerChargeEvent:()=>{},
    resolveCardThresholds:()=>{},
    resolveHeatTransformIfReady:()=>false,
    queueSpecialChoice:()=>{throw Error('Manual Vowmaw summon must not open an automatic prompt');},
    escapeHtml:String,
    clone:structuredClone
  };
  vm.createContext(context);
  for(const name of ['makeBattleEntry','findOwnedEntry','resolveHeatChange','triggerHeatEvent','resolveHeatOnPlay','createTokenCards','recordSacrifice','heatActionButtonsHtml']) vm.runInContext(fn(name),context);
  context.applyEffect=(game,player,effect,source)=>{
    if(effect.createToken)context.createTokenCards(game,player,effect.createToken,source);
  };
  const start=source.indexOf('      case "USE_HEAT": {');
  const end=source.indexOf('\n      case ',start+1);
  vm.runInContext(`function clickHeat(game,actor,action){switch(action.type){${source.slice(start,end)}}return "";}`,context);
  const entry=context.makeBattleEntry(keeper.id,player.id);
  player.played.push(entry);
  const sacrifice=(cardId='spawn')=>context.recordSacrifice(game,player,{id:cardId,instanceId:`victim-${++sequence}`},pack.ALL_CARD_MAP[cardId]);
  const click=()=>context.clickHeat(game,player,{type:'USE_HEAT',instanceId:entry.instanceId,actionIndex:0});
  return {context,game,player,entry,sacrifice,click};
}

test('Vharak retains Heat and exposes a named five-Heat action, not automatic summoning or charges',()=>{
  assert.ok(keeper.heat);assert.equal(keeper.charge,undefined);
  assert.equal(keeper.heat.trigger,'cardSacrificed');
  assert.equal(keeper.heat.perTurnCap,3);
  assert.equal(keeper.heat.overload,undefined);
  assert.equal(keeper.heat.activated,undefined);
  assert.equal(keeper.heat.actions[0].label,'Summon Vowmaw');
  assert.equal(keeper.heat.actions[0].cost,5);
  assert.deepEqual(keeper.heat.actions[0].effect,{createToken:{id:'vowmaw',count:1,zone:'hand'}});
});

test('playing Vharak does not add free Heat; friendly token and ordinary sacrifices count once each',()=>{
  const h=harness();
  h.context.resolveHeatOnPlay(h.game,h.player,h.entry,keeper);
  assert.equal(h.entry.heat,0);
  h.sacrifice('spawn');assert.equal(h.entry.heat,1);
  h.sacrifice('starter_coin');assert.equal(h.entry.heat,2);
  assert.equal(h.game.events.filter(e=>e.type===GAME_EVENT_TYPES.HEAT_GAINED).length,2);
  assert.equal(h.game.events.filter(e=>e.type===GAME_EVENT_TYPES.TOKEN_SACRIFICED).length,1);
});

test('preserves the original three-Heat-per-turn cap and carries the meter across turns and saves',()=>{
  const h=harness();
  for(let i=0;i<6;i++)h.sacrifice();
  assert.equal(h.entry.heat,3);
  const saved=JSON.parse(JSON.stringify(h.entry));
  h.player.played[0]=saved;
  h.game.turnSerial++;
  h.sacrifice();h.sacrifice();
  assert.equal(saved.heat,5);
  assert.equal(h.player.hand.length,0,'reaching five must wait for a click');
  assert.equal(h.player.pendingChoices.length,0);
});

test('button is disabled below five, enabled at five, and an actual click spends five to create one real Vowmaw',()=>{
  const h=harness();
  h.entry.heat=4;
  assert.match(h.context.heatActionButtonsHtml(h.entry,keeper,true),/disabled/);
  assert.match(h.click(),/enough Heat/);
  assert.equal(h.entry.heat,4);assert.equal(h.player.hand.length,0);
  h.entry.heat=5;
  const button=h.context.heatActionButtonsHtml(h.entry,keeper,true);
  assert.match(button,/Summon Vowmaw/);assert.doesNotMatch(button,/disabled/);
  assert.equal(h.click(),'');
  assert.equal(h.entry.heat,0);assert.equal(h.player.hand.length,1);
  assert.equal(h.player.hand[0].id,'vowmaw');assert.equal(h.player.hand[0].ownerId,h.player.id);
  assert.equal(h.game.events.filter(e=>e.type===GAME_EVENT_TYPES.TOKEN_CREATED).length,1);
  assert.match(h.click(),/enough Heat/);
  assert.equal(h.player.hand.length,1,'a duplicate click cannot summon twice');
});

test('held Heat is not gained by opponents, purges, or a Keeper outside play',()=>{
  const h=harness();
  h.context.triggerHeatEvent(h.game,h.player,'ownCardPurged',{cardId:'spawn'});
  h.context.triggerHeatEvent(h.game,{played:[],bases:[],attachments:[]},'cardSacrificed',{cardId:'spawn'});
  assert.equal(h.entry.heat,0);
  h.player.played=[];h.player.discard.push(h.entry);h.sacrifice();
  assert.equal(h.entry.heat,0);
  h.entry.heat=5;
  assert.match(h.click(),/no longer available/);
  assert.equal(h.player.hand.length,0);
});

test('disabled Keeper cannot gain Heat or activate; separate Keepers keep independent meters',()=>{
  const h=harness();
  h.entry.disabledTurn=h.game.turnSerial;h.entry.heat=5;
  assert.match(h.context.heatActionButtonsHtml(h.entry,keeper,true),/disabled/);
  assert.match(h.click(),/no longer available/);
  h.entry.heat=0;h.sacrifice();assert.equal(h.entry.heat,0);
  h.entry.disabledTurn=0;
  const other=h.context.makeBattleEntry(keeper.id,h.player.id);h.player.played.push(other);
  h.sacrifice();
  assert.equal(h.entry.heat,1);assert.equal(other.heat,1);
});
