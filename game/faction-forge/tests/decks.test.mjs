import test from 'node:test';
import assert from 'node:assert/strict';
import {STARTER_DECKS,starterDeck,validateDeck,normalizeDecks,deckForState,sanitizeRoster,sanitizeBattleUnits,encodeBattleUnits,decodeBattleUnits,rosterCount,deckAnalysis,MAX_CUSTOM_DECKS} from '../decks.js';
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
test('five deck types allow growing armies while hostile roster rows are rejected',()=>{
  assert.deepEqual(sanitizeRoster([{type:'vguard',count:9,lvl:99},{type:'guardian',count:1,lvl:1},{type:'medic',count:Infinity,lvl:1},{type:'vrocket',count:-2,lvl:1},{type:'marksman',count:Number.MAX_SAFE_INTEGER+1,lvl:1}],starterDeck('Vanir')),[{type:'vguard',count:9,lvl:3}]);
  const deck=starterDeck('Vanir'),state={factions:{p0:'Vanir'},decks:{p0:deck},roster:{p0:deck.troops.map(type=>({type,count:1,lvl:1})),p1:[]}};
  const original=structuredClone(state.roster.p0);
  for(const card of [{type:'troop',key:'guardian'},{type:'special',special:'upgrade',for:'rifleman'},{type:'evolution',key:'rifleman',path:'a'},{type:'combo',key:'precision-execution'},{type:'tactic',key:'invented'}])applyDraftPick(state,'p0',card);
  assert.deepEqual(state.roster.p0,original);assert.equal(state.comboEnhancements,undefined);assert.equal(state.tactics,undefined);
  applyDraftPick(state,'p0',{type:'troop',key:'vguard'});assert.equal(rosterCount(state.roster.p0),6);
  applyDraftPick(state,'p0',{type:'special',special:'double',for:'vguard'});assert.equal(rosterCount(state.roster.p0),8);assert.equal(state.roster.p0.find(row=>row.type==='vguard').count,4);
});
test('fully upgraded armies continue receiving recruits after every relic and tactic',()=>{
  const deck=starterDeck('Vanir'),troops=Object.fromEntries(deck.troops.map(key=>[key,{faction:'Vanir'}]));
  const pack=createDraftPack({deck,faction:'Vanir',troops,roster:deck.troops.map(type=>({type,count:1,lvl:3})),evolutions:Object.fromEntries(deck.troops.map(key=>[key,'b'])),comboEnhancements:COMBOS.map(combo=>combo.id),ownedRelics:LEGACY_DRAFT_CARDS.filter(c=>c.type==='relic').map(c=>c.key),ownedTactics:LEGACY_DRAFT_CARDS.filter(c=>c.type==='tactic').map(c=>c.key),formation:'wide'},()=>.5);
  assert.equal(pack.length,3);assert.ok(pack.filter(card=>card.type==='troop').length>=2);assert.ok(pack.every(card=>card.type==='troop'||card.type==='formation'&&card.key!=='wide'));
});
test('starter analysis exposes only possible combos and useful faction strengths',()=>{
  for(const deck of STARTER_DECKS){const analysis=deckAnalysis(deck);assert.ok(analysis.strengths.length);assert.ok(analysis.weaknesses.length);assert.ok(analysis.combos.length);assert.ok(analysis.combos.every(combo=>combo.units.every(key=>deck.troops.includes(key))));}
});
test('received old or malicious battle snapshots respect deck types without dropping extra copies',()=>{
  const state={decks:{p0:starterDeck('Vanir'),p1:starterDeck('Aesir')}};
  const units=Array.from({length:12},(_,i)=>({id:'unit-'+i,p:i<7?0:1,type:i<7?'vguard':'phaser',hp:10}));
  units.unshift({id:'offdeck',p:0,type:'guardian',hp:99},{id:'illegal-decoy',p:0,type:'miragewarden',illusion:true,hp:3});
  units.push({id:'valid-decoy',p:1,type:'miragewarden',illusion:true,summoned:true,hp:3});
  const filtered=sanitizeBattleUnits(units,state);assert.equal(filtered.length,13);assert.equal(filtered.filter(u=>u.p===0).length,7);assert.equal(filtered.filter(u=>u.p===1).length,6);assert.ok(!filtered.some(u=>u.id.startsWith('offdeck')||u.id==='illegal-decoy'));
  const short=sanitizeBattleUnits([units.at(-1)],state);assert.equal(short.length,1);
});

test('grouped battle transport preserves growing armies and derives side and type from validated keys',()=>{
  const state={decks:{p0:starterDeck('Vanir'),p1:starterDeck('Aesir')}};
  const units=Array.from({length:44},(_,i)=>({id:'copy-'+i,p:i<23?0:1,type:i<23?'vguard':'phaser',hp:10,x:i%8,y:Math.floor(i/8)}));
  const unitGroups=encodeBattleUnits(units,state);
  assert.equal(unitGroups.p0.vguard.length,23);assert.equal(unitGroups.p1.phaser.length,21);assert.ok(unitGroups.p0.vguard.every(unit=>!('p' in unit)&&!('type' in unit)));
  const decoded=decodeBattleUnits({unitGroups},state);assert.equal(decoded.length,44);assert.deepEqual(decoded,units);
  assert.deepEqual(decodeBattleUnits({units},state),units);
  const hostile={unitGroups:{p0:{vguard:[{id:'forged-side',p:1,type:'guardian',hp:10}],guardian:[{id:'offdeck',hp:10}]},p1:{miragewarden:[{id:'decoy',p:0,type:'blood',hp:3,dmg:999,illusion:false}]}}};
  const safe=decodeBattleUnits(hostile,state);assert.equal(safe.length,2);assert.equal(safe[0].p,0);assert.equal(safe[0].type,'vguard');assert.equal(safe[1].p,1);assert.equal(safe[1].type,'miragewarden');assert.equal(safe[1].illusion,true);assert.equal(safe[1].dmg,0);
});

test('grouped recovery preserves interleaved types, sides and summons in simulation order',()=>{
  const broodDeck={...starterDeck('Helions'),troops:['roach','matron','defiler','spitter','carrion']},state={decks:{p0:starterDeck('Aesir'),p1:broodDeck}};
  const units=[{id:'phaser',p:0,type:'phaser',hp:10},{id:'matron',p:1,type:'matron',hp:10},{id:'storm',p:0,type:'storm',hp:10},{id:'brood',p:1,type:'blood',hp:4,summoned:true},{id:'decoy',p:0,type:'miragewarden',hp:3,illusion:true,summoned:true,dmg:0},{id:'second-phaser',p:0,type:'phaser',hp:10},{id:'roach',p:1,type:'roach',hp:10}];
  const groups=encodeBattleUnits(units,state);assert.equal(groups.p0.phaser[1].syncOrder,5);
  assert.deepEqual(decodeBattleUnits({unitGroups:groups},state),units);
});

test('received bodies cannot exceed their growing roster plus the summon budget',()=>{
  const state={decks:{p0:starterDeck('Vanir'),p1:starterDeck('Aesir')},roster:{p0:[{type:'vguard',count:19,lvl:1}],p1:[{type:'phaser',count:6,lvl:1}]}};
  const units=[...Array.from({length:22},(_,i)=>({id:'v-'+i,p:0,type:'vguard',hp:10})),...Array.from({length:9},(_,i)=>({id:'a-'+i,p:1,type:'phaser',hp:10}))];
  assert.equal(decodeBattleUnits({unitGroups:encodeBattleUnits(units,state)},state).length,31);
  const giant=[...Array.from({length:256},(_,i)=>({id:'forged-'+i,p:0,type:'vguard',hp:10})),...units.filter(unit=>unit.p===1)];
  for(const payload of [{units:giant},{unitGroups:encodeBattleUnits(giant,state)}]){const safe=decodeBattleUnits(payload,state);assert.equal(safe.length,9);assert.ok(safe.every(unit=>unit.p===1));}
});
