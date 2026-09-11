import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import vm from 'node:vm';
import { experimentSchedule, recordDeckResult, groupDeckResults } from '../ui/test-lab-experiments.js';
import { getTestLabDeckPreset } from '../ui/test-lab-deck-presets.js';
import { simulateTestLabGame, getTestLabCard } from '../ui/test-lab-simulator.js';
import { runTestLabWithHistory } from '../ui/test-lab-runner.js';

const source = await fs.readFile(new URL('../../warrealms.html', import.meta.url), 'utf8');
const queueSource = source.slice(source.indexOf('  function queueCardSelection('), source.indexOf('  function applyEffect(', source.indexOf('  function queueCardSelection(')));
const sandbox = { getCard: entry => ({name: entry.id}), queueSpecialChoice: (game, player, options, context) => { (player.pendingChoices ||= []).push({options, ...context}); } };
vm.createContext(sandbox); vm.runInContext(queueSource, sandbox);
const card = (id, instanceId = id) => ({id, instanceId});
const ids = entries => entries.map(entry => entry.instanceId);

test('top inspection exposes exact duplicate instances in next-draw order', () => {
  const player = { draw: [card('x'), card('same', 'copy1'), card('same', 'copy2')], pendingChoices: [] };
  sandbox.queueCardSelection({}, player, {zone:'draw', ids:['copy2','copy1'], selected:[], destinations:['bottom'], reorder:true});
  assert.deepEqual(ids(player.draw), ['x','copy1','copy2']);
  assert.deepEqual(Array.from(player.pendingChoices[0].options.slice(0,2), o => o.instanceId), ['copy2','copy1']);
  assert.ok(player.pendingChoices[0].options.some(o => o.label === 'Swap Order'));
});

test('selection toggles do not move cards; commit moves only selected instances', () => {
  const player = { draw: [card('x'), card('same','copy1'), card('same','copy2')], pendingChoices: [] };
  sandbox.queueCardSelection({}, player, {zone:'draw', ids:['copy2','copy1'], selected:[], destinations:['bottom']});
  const toggle = player.pendingChoices.shift().options[0].effect.__cardSelection;
  sandbox.queueCardSelection({}, player, toggle);
  assert.deepEqual(ids(player.draw), ['x','copy1','copy2']);
  const commit = player.pendingChoices[0].options.find(o => o.effect.__moveSelected?.destination === 'bottom').effect.__moveSelected;
  sandbox.resolveSelectedCards(player, commit);
  assert.deepEqual(ids(player.draw), ['copy2','x','copy1']);
});

test('bottom movement preserves displayed order and all instance metadata', () => {
  const a = {...card('a'), heat:4}, b = {...card('b'), charges:2};
  const player = {draw:[card('x'), b, a]};
  sandbox.resolveSelectedCards(player, {zone:'draw', ids:['a','b'], selected:['a','b'], destination:'bottom'});
  assert.deepEqual(ids(player.draw), ['b','a','x']);
  assert.equal(player.draw[1], a); assert.equal(player.draw[0], b);
});

test('Keep All and swapping top order preserve the rest of the deck', () => {
  const player = {draw:[card('x'),card('b'),card('a')]};
  sandbox.resolveSelectedCards(player, {zone:'draw', ids:['a','b'], selected:[], destination:'top'});
  assert.deepEqual(ids(player.draw), ['x','b','a']);
  sandbox.resolveSelectedCards(player, {zone:'draw', ids:['b','a'], selected:[], destination:'top'});
  assert.deepEqual(ids(player.draw), ['x','a','b']);
});

test('stale selections never substitute another copy of a card', () => {
  const player = {draw:[card('same','new-copy')]};
  sandbox.resolveSelectedCards(player, {zone:'draw', ids:['old-copy'], selected:['old-copy'], destination:'bottom'});
  assert.deepEqual(ids(player.draw), ['new-copy']);
});

for (const destination of ['hand','discard','scrapped','top','bottom']) test(`general selections move discard instances to ${destination}`, () => {
  const entry = {...card('a'), heat:3};
  const player = {draw:[card('x')], hand:[], discard:[entry,card('b')]};
  sandbox.resolveSelectedCards(player, {zone:'discard', ids:['a','b'], selected:['a'], destination});
  const target = ['top','bottom'].includes(destination) ? player.draw : player[destination];
  assert.ok(target.includes(entry));
  assert.equal(target.filter(e => e.instanceId === 'a').length,1);
  assert.equal(player.discard.filter(e => e.instanceId === 'a').length,destination === 'discard' ? 1 : 0);
});

test('inspection defers subsequent draws instead of invalidating the visible cards', () => {
  const drawSource = source.slice(source.indexOf('  function drawCards('), source.indexOf('  function drawMatchingFromDrawPile('));
  const context = {getCard: () => null, pushVisualEvent: () => {}, shuffleWithGame: (_, cards) => cards};
  vm.createContext(context); vm.runInContext(drawSource,context);
  const player={draw:[card('a')], hand:[], pendingChoices:[{source:'cardSelection'}]};
  context.drawCards({},player,1,'source');
  assert.equal(player.draw.length,1); assert.equal(player.hand.length,0);
  assert.equal(player.selectionDeferredDraws.length,1);
  player.pendingChoices=[];
  context.drawCards({},player,player.selectionDeferredDraws[0].amount);
  assert.deepEqual(ids(player.hand),['a']);
});

test('deck schedule includes each variant pair and rejects empty style lists', () => {
  const schedule = experimentSchedule([{id:'vanguard',styles:['vanguard','engine']},{id:'engine',styles:['engine']}]);
  assert.equal(schedule.length,3);
  assert.throws(() => experimentSchedule([{id:'vanguard',styles:[]}]), /Select a playstyle/);
});

test('results group seat outcomes by deck, style, and combination', () => {
  const rows = new Map(), pair = experimentSchedule([{id:'vanguard',styles:['engine']},{id:'engine',styles:['vanguard']}])[0];
  recordDeckResult(rows,pair,{strategies:{a:'engine',b:'vanguard'},winnerId:'a',draw:false});
  recordDeckResult(rows,pair,{strategies:{a:'engine',b:'vanguard'},winnerId:'',draw:true});
  for (const mode of ['deck','style','combined']) {
    const result=groupDeckResults([...rows.values()],mode);
    assert.equal(result.length,2); assert.equal(result.reduce((sum,row)=>sum+row.games,0),4);
    assert.equal(result.reduce((sum,row)=>sum+row.wins,0),1);
  }
});

test('mutations change simulation outcomes and never alter real card definitions', () => {
  const preset=getTestLabDeckPreset('vanguard');
  const id="starter_coin", before=structuredClone(getTestLabCard(id));
  const options={seed:924, commandDeckA:preset.cardIds, commandDeckB:preset.cardIds, maxTurns:100};
  const normal=simulateTestLabGame(options);
  const changed=simulateTestLabGame({...options,mutations:{[id]:{cost:0,'effect.combat':100}}});
  assert.notDeepEqual(changed,normal);
  assert.deepEqual(getTestLabCard(id),before);
  assert.deepEqual(simulateTestLabGame(options),normal);
  assert.throws(()=>simulateTestLabGame({...options,mutations:{[id]:{'heat.overload.at':-1}}}), /Invalid mutation/);
  assert.deepEqual(simulateTestLabGame(options),normal);
});

test('runner reports all selected variants and cancellation returns partial results', async () => {
  const controller=new AbortController();
  const result=await runTestLabWithHistory({games:12,batchSize:3,decks:[{id:'vanguard',styles:['engine','vanguard']},{id:'engine',styles:['engine']}]},{signal:controller.signal,onProgress:()=>controller.abort()});
  assert.equal(result.summary.games,3);
  assert.equal(result.deckRows.length,3);
  assert.equal(result.deckRows.reduce((sum,row)=>sum+row.games,0),6);
});

test('required multi-card topdeck choices finish for bots without exceeding the count', () => {
  const chooseSource = source.slice(source.indexOf('  function chooseBotChoiceIndex('),source.indexOf('  function cardAiValue('));
  vm.runInContext(chooseSource,sandbox);
  const player={hand:[card('a'),card('b'),card('c')],draw:[],pendingChoices:[]};
  sandbox.queueCardSelection({},player,{zone:'hand',ids:['a','b','c'],selected:[],destinations:['top'],min:2,max:2});
  for(let step=0;step<3;step++){
    const choice=player.pendingChoices.shift();
    const index=sandbox.chooseBotChoiceIndex(choice,player,'hard');
    assert.ok(index>=0);
    const effect=choice.options[index].effect;
    if(effect.__cardSelection) sandbox.queueCardSelection({},player,effect.__cardSelection);
    else sandbox.resolveSelectedCards(player,effect.__moveSelected);
  }
  assert.equal(player.draw.length,2); assert.equal(player.hand.length,1); assert.equal(player.pendingChoices.length,0);
});
