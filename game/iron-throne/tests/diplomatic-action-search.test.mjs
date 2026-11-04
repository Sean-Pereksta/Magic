import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { diplomaticActions, searchDiplomaticActions } from '../diplomatic-action-search.mjs';
import { ownCouncil } from '../council-state.mjs';
import { pair } from '../core.mjs';

test('resource search opens a request for the bought resource and an aid offer for gifts', () => {
  const s = createGame(), before = JSON.stringify(s);
  const actions = diplomaticActions(s, {actor:'ashen',ruler:'wintermere'});
  const buy = searchDiplomaticActions(actions, 'Buy iron');
  assert.deepEqual(buy.map(a=>a.type), ['EXCHANGE','RECURRING']);
  assert.ok(buy.every(a=>a.direction==='request' && a.intent.giveResource==='iron'));
  const gift = searchDiplomaticActions(actions, 'Gift food')[0];
  assert.equal(gift.type, 'AID'); assert.equal(gift.direction, 'offer');
  assert.equal(gift.intent.giveResource, 'food');
  assert.deepEqual(gift.requestedHouses, ['wintermere']);
  assert.equal(JSON.stringify(s), before, 'search neither sends nor mutates campaign state');
});
test('passage and declarations search across categories with distinct results', () => {
  const actions = diplomaticActions(createGame(), {actor:'ashen',ruler:'wintermere'});
  assert.equal(searchDiplomaticActions(actions, 'Passage')[0].type, 'ACCESS');
  assert.deepEqual(searchDiplomaticActions(actions, 'Declare').map(a=>a.type), ['WAR','BETRAY','PLEDGE_WAR']);
});
test('peace identifies both warring Houses and only appropriate council actions are offered', () => {
  const s = createGame();
  s.wars.push(pair('ashen','wintermere'), pair('wintermere','sunspire'));
  const privateActions = diplomaticActions(s,{actor:'ashen',ruler:'wintermere'});
  const peace = searchDiplomaticActions(privateActions,'Peace').filter(a=>a.type==='PEACE');
  assert.ok(peace.some(a=>a.label.includes('Ashen') && a.label.includes('Wintermere')));
  assert.ok(peace.some(a=>a.intent.targetId==='sunspire' && a.label.includes('Sunspire')));
  s.wars = [pair('wintermere','sunspire')];
  s.treaties.push({id:'ally',type:'alliance',parties:['ashen','wintermere'],expires:100});
  const council = ownCouncil(s,'ashen',true);
  const actions = diplomaticActions(s,{actor:'ashen',councilId:council.id});
  assert.ok(actions.every(a=>!['WAR','BETRAY','VASSALAGE','MARRIAGE','INTELLIGENCE'].includes(a.type)));
  assert.ok(searchDiplomaticActions(actions,'Peace').some(a=>a.intent.targetId==='sunspire'));
  assert.deepEqual(diplomaticActions(s,{actor:'ashen',councilId:'missing'}),[]);
});

test('war and embargo hide unavailable commercial actions while keeping permitted review forms', () => {
  for(const restriction of ['war','embargo']){
    const s=createGame();
    if(restriction==='war')s.wars.push(pair('ashen','wintermere'));
    else s.treaties.push({type:'embargo',parties:['ashen','vesper'],targetId:'wintermere',expires:10});
    const actions=diplomaticActions(s,{actor:'ashen',ruler:'wintermere'});
    assert.deepEqual(searchDiplomaticActions(actions,'Buy iron'),[]);
    assert.ok(actions.every(a=>!['TRADE','EXCHANGE','RECURRING','LOAN'].includes(a.type)));
    assert.equal(searchDiplomaticActions(actions,'Gift food')[0].type,'AID');
    if(restriction==='war'){
      assert.ok(actions.some(a=>a.type==='PEACE'));
      assert.ok(actions.every(a=>a.type!=='NON_AGGRESSION'));
    }else assert.equal(searchDiplomaticActions(actions,'Passage')[0].type,'ACCESS');
  }
  const s=createGame();s.kingdoms[0].resources.gold=0;
  assert.equal(searchDiplomaticActions(diplomaticActions(s,{actor:'ashen',ruler:'wintermere'}),'Buy iron').length,2,'affordability remains adjustable in review');
});

test('council commercial results select only eligible recipients and can mediate between members', () => {
  const s=createGame();
  for(const house of ['wintermere','thornwall','sunspire'])s.treaties.push({type:'alliance',parties:['ashen',house],expires:100});
  s.wars.push(pair('wintermere','thornwall'));
  s.treaties.push({type:'embargo',parties:['wintermere','vesper'],targetId:'ashen',expires:10});
  const council=ownCouncil(s,'ashen',true),actions=diplomaticActions(s,{actor:'wintermere',councilId:council.id});
  for(const action of actions.filter(a=>['TRADE','EXCHANGE','RECURRING','LOAN'].includes(a.type))){
    assert.deepEqual(action.requestedHouses,['sunspire']);assert.equal(action.detail,'House Sunspire');
  }
  assert.equal(searchDiplomaticActions(actions,'Buy iron').length,2);
  const hostActions=diplomaticActions(s,{actor:'ashen',councilId:council.id});
  const peace=hostActions.find(a=>a.type==='PEACE'&&a.intent.targetId==='thornwall');
  assert.ok(peace);assert.deepEqual(peace.requestedHouses,['wintermere']);
  assert.match(peace.label,/Wintermere.*Thornwall/);
});
