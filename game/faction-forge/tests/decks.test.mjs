import test from 'node:test';
import assert from 'node:assert/strict';
import {STARTER_DECKS,starterDeck,validateDeck,normalizeDecks,deckForState,sanitizeRoster,sanitizeBattleUnits,rosterCount,deckAnalysis,MAX_CUSTOM_DECKS} from '../decks.js';
import {applyDraftPick,createDraftPack,LEGACY_DRAFT_CARDS} from '../drafting.js';
import {COMBOS} from '../combos.js';

test('deck validation rejects wrong slot counts, duplicates, foreign faction, unknown and locked troops',()=>{
  const deck=starterDeck('Vanir');assert.equal(validateDeck(deck).valid,true);
  for(const value of [null,{}, {...deck,troops:deck.troops.slice(1)},{...deck,troops:[...deck.troops,'rifleman']},{...deck,troops:['vguard','vguard','medic','marksman','vrocket']},{...deck,troops:['guardian',...deck.troops.slice(1)]},{...deck,troops:['missing',...deck.troops.slice(1)]},{...deck,name:''},{...deck,id:'bad/path'}])assert.equal(validateDeck(value).valid,false,JSON.stringify(value));
  assert.equal(validateDeck(deck,{unlockedTypes:deck.troops.slice(1)}).valid,false);assert.equal(validateDeck(deck,{unlockedTypes:deck.troops}).valid,true);
});
test('migration always grants three canonical starters, preserves valid custom decks and bounds storage',()=>{
  const custom={...starterDeck('Vanir'),id:'my-deck',name:'Custom'},before=structuredClone(custom),decks=normalizeDecks([custom,{...starterDeck('Aesir'),troops:['invalid']},custom]);
  assert.equal(decks.length,4);assert.deepEqual(decks.slice(0,3),STARTER_DECKS.map(({id,name,faction,troops})=>({id,name,faction,troops:[...troops]})));assert.deepEqual(custom,before);
  assert.equal(normalizeDecks(Array.from({length:100},(_,i)=>({...custom,id:'deck-'+i}))).length,MAX_CUSTOM_DECKS+3);
  assert.equal(deckForState({factions:{p0:'Aesir'}},'p0').id,'starter-aesir');assert.equal(deckForState({decks:{p0:{}}},'p0'),null);
});
test('legacy and hostile rosters cannot carry off-deck bodies or more than five recruits',()=>{
  assert.deepEqual(sanitizeRoster([{type:'vguard',count:9,lvl:99},{type:'guardian',count:1,lvl:1}],starterDeck('Vanir')),[{type:'vguard',count:5,lvl:3}]);
  const deck=starterDeck('Vanir'),state={factions:{p0:'Vanir'},decks:{p0:deck},roster:{p0:deck.troops.map(type=>({type,count:1,lvl:1})),p1:[]}};
  const original=structuredClone(state.roster.p0);
  for(const card of [{type:'troop',key:'guardian'},{type:'troop',key:'vguard'},{type:'special',special:'double',for:'vguard'},{type:'special',special:'upgrade',for:'rifleman'},{type:'evolution',key:'rifleman',path:'a'},{type:'combo',key:'precision-execution'},{type:'tactic',key:'invented'}])applyDraftPick(state,'p0',card);
  assert.deepEqual(state.roster.p0,original);assert.equal(state.comboEnhancements,undefined);assert.equal(state.tactics,undefined);
  state.roster.p0.pop();applyDraftPick(state,'p0',{type:'special',special:'double',for:'vguard'});assert.equal(rosterCount(state.roster.p0),5);
});
test('fully upgraded decks with every relic and tactic still have three usable formation choices',()=>{
  const deck=starterDeck('Vanir'),troops=Object.fromEntries(deck.troops.map(key=>[key,{faction:'Vanir'}]));
  const pack=createDraftPack({deck,faction:'Vanir',troops,roster:deck.troops.map(type=>({type,count:1,lvl:3})),evolutions:Object.fromEntries(deck.troops.map(key=>[key,'b'])),comboEnhancements:COMBOS.map(combo=>combo.id),ownedRelics:LEGACY_DRAFT_CARDS.filter(c=>c.type==='relic').map(c=>c.key),ownedTactics:LEGACY_DRAFT_CARDS.filter(c=>c.type==='tactic').map(c=>c.key),formation:'wide'},()=>.5);
  assert.equal(pack.length,3);assert.ok(pack.every(card=>card.type==='formation'&&card.key!=='wide'));
});
test('starter analysis exposes only possible combos and useful faction strengths',()=>{
  for(const deck of STARTER_DECKS){const analysis=deckAnalysis(deck);assert.ok(analysis.strengths.length);assert.ok(analysis.weaknesses.length);assert.ok(analysis.combos.length);assert.ok(analysis.combos.every(combo=>combo.units.every(key=>deck.troops.includes(key))));}
});
test('received old or malicious battle snapshots respect decks and five slots including decoys',()=>{
  const state={decks:{p0:starterDeck('Vanir'),p1:starterDeck('Aesir')}};
  const units=Array.from({length:12},(_,i)=>({id:'unit-'+i,p:i<7?0:1,type:i<7?'vguard':'phaser',hp:10}));
  units.unshift({id:'offdeck',p:0,type:'guardian',hp:99},{id:'illegal-decoy',p:0,type:'miragewarden',illusion:true,hp:3});
  units.push({id:'valid-decoy',p:1,type:'miragewarden',illusion:true,summoned:true,hp:3});
  const filtered=sanitizeBattleUnits(units,state);assert.equal(filtered.length,10);assert.equal(filtered.filter(u=>u.p===0).length,5);assert.equal(filtered.filter(u=>u.p===1).length,5);assert.ok(!filtered.some(u=>u.id.startsWith('offdeck')||u.id==='illegal-decoy'));
  const short=sanitizeBattleUnits([units.at(-1)],state);assert.equal(short.length,1);
});
