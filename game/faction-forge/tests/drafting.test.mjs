import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import * as units from '../units.js';
import {COMBOS} from '../combos.js';
import {starterDeck,FACTION_TROOPS,rosterCount} from '../decks.js';
import {EVOLUTIONS} from '../evolutions.js';
import {createDraftPack,applyDraftPick,draftCardPresentation,relevantComboStates,DRAFT_WEIGHTS} from '../drafting.js';
import {scoreAIDraft,chooseAIDraft,AI_PLAYSTYLES,practiceOpponent} from '../campaign.js';
const require=createRequire(import.meta.url),{rng}=require('../../../server/faction-forge/simulator.cjs');
const html=readFileSync(new URL('../../crowncouncil.html',import.meta.url),'utf8'),context=vm.createContext({FFUnits:units});
vm.runInContext(html.slice(html.indexOf('const R ='),html.indexOf('const TROOP_IMAGE_VERSION'))+'\nthis.TROOPS=TROOPS;',context);
const troops=context.TROOPS;
units.installUnits({TROOPS:troops,FACTIONS:{Vanir:[],Aesir:[],Helions:[]}});
const types=faction=>Object.keys(troops).filter(key=>troops[key].faction===faction);
const rows=(keys,lvl=1)=>keys.map(type=>({type,count:1,lvl}));
const deckWith=(faction,keys=[])=>({id:'test-'+faction,name:'Test deck',faction,troops:[...new Set([...keys,...starterDeck(faction).troops,...FACTION_TROOPS[faction]])].slice(0,5)});
const options=(faction,roster=[],deck=deckWith(faction,roster.map(r=>r.type)))=>({troops,faction,deck,roster,availableTypes:deck.troops});

test('all 32 troops can recruit, upgrade and specialize within a valid five-type deck',()=>{
  assert.equal(Object.keys(troops).length,32);
  for(const [key,troop] of Object.entries(troops)){
    const opts=options(troop.faction,rows([key])),seen=new Set();
    for(let seed=0;seed<250;seed++)for(const card of createDraftPack(opts,rng(key+seed)))seen.add(card.type==='special'?'upgrade:'+card.for:card.type+':'+card.key+':'+(card.path||''));
    assert.ok(seen.has('troop:'+key+':'),key+' recruits');assert.ok(seen.has('upgrade:'+key),key+' upgrades');
    for(const path of ['a','b'])assert.ok(seen.has('evolution:'+key+':'+path),key+' '+path);
  }
});

test('issued choices remain faction legal, owned upgrades are below III, and paths/enhancements never repeat',()=>{
  const roster=rows(['guardian','storm','runebinder'],3),opts={...options('Aesir',roster),evolutions:{guardian:'a',storm:'b',runebinder:'b'},comboEnhancements:['static-tempest','thunderclap'],availableTypes:['guardian','storm','runebinder','phaser']};
  for(let seed=0;seed<200;seed++){
    const pack=createDraftPack(opts,rng(seed));assert.equal(pack.length,3);
    const identities=pack.map(card=>card.type==='troop'?'troop:'+card.key:card.type+':'+(card.for||card.key));assert.equal(new Set(identities).size,pack.length);
    for(const card of pack){assert.notEqual(card.type,'evolution');assert.notEqual(card.type,'special');assert.ok(card.type!=='troop'||opts.availableTypes.includes(card.key));assert.ok(card.type!=='combo'||!opts.comboEnhancements.includes(card.key));}
  }
  assert.deepEqual(createDraftPack({...opts,availableTypes:[]},rng('none')),[]);
});

test('empty, partial and complete five-type armies always receive legal deterministic recruitment choices',()=>{
  for(const faction of ['Vanir','Aesir','Helions'])for(const count of [0,2,5,12])for(const lvl of [1,2,3])for(let seed=0;seed<100;seed++){
    const deck=starterDeck(faction),roster=rows(deck.troops.slice(0,count),lvl),opts={...options(faction,roster,deck),evolutions:lvl===3?Object.fromEntries(deck.troops.map(key=>[key,'b'])):{},comboEnhancements:lvl===3?COMBOS.map(c=>c.id):[]};
    if(count>deck.troops.length)roster[0].count+=count-deck.troops.length;
    const pack=createDraftPack(opts,rng(seed));assert.equal(pack.length,3);assert.deepEqual(pack,createDraftPack(opts,rng(seed)));
    for(const card of pack){if(card.type==='troop')assert.ok(deck.troops.includes(card.key));if(card.for)assert.ok(deck.troops.includes(card.for));if(card.type==='combo')assert.ok(COMBOS.find(c=>c.id===card.key).units.every(k=>deck.troops.includes(k)));}
    assert.ok(pack.filter(c=>c.type==='troop').length>=2);
  }
});

test('packs reserve recruitment choices and introduce unowned types across sizes, ranks and factions',()=>{
  for(const faction of ['Vanir','Aesir','Helions'])for(const count of [1,2,4])for(const lvl of [1,2,3])for(const size of [1,2,3,4,5]){
    const deck=starterDeck(faction),roster=rows(deck.troops.slice(0,count),lvl),opts={...options(faction,roster,deck),size};
    for(let seed=0;seed<30;seed++){
      const random=seed===0?()=>0:seed===1?()=>.999999:rng('recruit-guarantee-'+faction+count+lvl+size+seed);
      const pack=createDraftPack(opts,random),recruits=pack.filter(card=>card.type==='troop');
      assert.equal(pack.length,size);
      assert.ok(recruits.length>=Math.ceil(size*2/3),`${faction} ${count} bodies, rank ${lvl}, size ${size}`);
      assert.ok(recruits.some(card=>!roster.some(row=>row.type===card.key)),'offer a genuinely new troop');
      assert.equal(new Set(recruits.map(card=>card.key)).size,recruits.length,'recruit choices are distinct');
    }
  }
});

test('recruitment guarantees respect available subsets and remain available in larger armies',()=>{
  for(const faction of ['Vanir','Aesir','Helions']){
    const deck=deckWith(faction,FACTION_TROOPS[faction].slice(-2)),roster=rows(deck.troops.slice(0,2));
    for(const availableTypes of [[deck.troops[0]],[deck.troops[2]],[deck.troops[0],deck.troops[2]],deck.troops])for(const size of [1,2,3,4,5])for(let seed=0;seed<25;seed++){
      const opts={...options(faction,roster,deck),size,availableTypes:[...availableTypes,availableTypes[0],'invented',faction==='Vanir'?'guardian':'vguard']};
      const pack=createDraftPack(opts,rng('recruit-subset-'+faction+size+availableTypes.length+seed)),recruits=pack.filter(card=>card.type==='troop');
      assert.ok(recruits.length>=Math.min(availableTypes.length,Math.ceil(size*2/3)));
      assert.ok(recruits.every(card=>availableTypes.includes(card.key)));
      assert.equal(new Set(recruits.map(card=>card.key)).size,recruits.length);
      if(availableTypes.some(key=>!roster.some(row=>row.type===key)))assert.ok(recruits.some(card=>!roster.some(row=>row.type===card.key)));
    }
    for(const count of [5,12,32])for(let seed=0;seed<25;seed++){
      const roster=[{type:deck.troops[0],count,lvl:1}],pack=createDraftPack(options(faction,roster,deck),rng('large-'+seed));
      assert.ok(pack.filter(card=>card.type==='troop').length>=2);
      assert.ok(pack.some(card=>card.type==='troop'&&card.key!==deck.troops[0]));
    }
  }
});

test('recruitment and legacy doubles grow beyond five bodies without admitting a sixth deck type',()=>{
  for(const faction of ['Vanir','Aesir','Helions']){
    const deck=starterDeck(faction),state={factions:{p0:faction},decks:{p0:deck},roster:{p0:rows(deck.troops),p1:[]}};
    for(let pick=0;pick<20;pick++){
      const before=rosterCount(state.roster.p0),card=createDraftPack(options(faction,state.roster.p0,deck),rng('growth-'+faction+pick)).find(card=>card.type==='troop');
      applyDraftPick(state,'p0',card);assert.equal(rosterCount(state.roster.p0),before+1);
    }
    const unit=state.roster.p0[0],before=unit.count;
    applyDraftPick(state,'p0',{type:'special',special:'double',for:unit.type});
    assert.equal(state.roster.p0[0].count,before*2);
    const snapshot=structuredClone(state.roster.p0),foreign=FACTION_TROOPS[faction].find(key=>!deck.troops.includes(key));
    applyDraftPick(state,'p0',{type:'troop',key:foreign});assert.deepEqual(state.roster.p0,snapshot);
    assert.equal(new Set(state.roster.p0.map(row=>row.type)).size,5);
  }
});

test('seeded recruitment distribution strongly favors bodies while preserving copies and upgrades',()=>{
  assert.equal(Object.values(DRAFT_WEIGHTS).reduce((sum,weight)=>sum+weight,0),100);
  for(const faction of ['Vanir','Aesir','Helions'])for(const count of [2,5,15]){
    const deck=starterDeck(faction),roster=rows(deck.troops.slice(0,count)),opts=options(faction,roster,deck);
    if(count>deck.troops.length)for(const row of roster)row.count=3;
    const categories=new Set(),newPositions=new Set();let recruits=0,copies=0,unowned=0;
    const samples=2000;
    for(let seed=0;seed<samples;seed++){
      const pack=createDraftPack(opts,rng('recruit-frequency-'+faction+count+seed));
      for(const [index,card]of pack.entries()){
        categories.add(card.draftCategory);
        if(card.type!=='troop')continue;
        recruits++;
        if(roster.some(row=>row.type===card.key))copies++;else{unowned++;newPositions.add(index);}
      }
    }
    const share=recruits/(samples*3);
    assert.ok(share>=.82&&share<=.93,`${faction}, ${count} bodies: actual recruit share ${share}`);
    if(count<deck.troops.length)assert.ok(unowned/recruits>=.8,`${faction}: new types dominate recruitment`);
    assert.ok(copies>0,'additional copies remain available');
    if(count<deck.troops.length)assert.deepEqual([...newPositions].sort(),[0,1,2],'new troops can appear in every card position');
    for(const category of ['troop','upgrade','evolution','legacy'])assert.ok(categories.has(category),`${faction}: ${category} remains available`);
  }
});

test('missing combo partners stay in the recruitment pool and can still complete relevant combinations',()=>{
  for(const combo of COMBOS){
    if(new Set(combo.units).size<2)continue;
    const missing=combo.units.at(-1),roster=rows(combo.units.filter(key=>key!==missing)),seen=[];
    for(let seed=0;seed<150;seed++)for(const card of createDraftPack(options(combo.faction,roster,deckWith(combo.faction,combo.units)),rng(combo.id+'-partner-'+seed)))if(card.type==='troop'&&card.key===missing)seen.push(card);
    assert.ok(seen.length,combo.id+' partner remains draftable');
    assert.ok(seen.every(card=>card.draftCategory==='troop'));
    assert.ok(seen.some(card=>draftCardPresentation(card,roster,{faction:combo.faction,troops,availableTypes:types(combo.faction)}).comboPreview.some(preview=>preview.id===combo.id&&preview.unlocks)),combo.id+' partner preview');
  }
});

test('evolution selections raise I to II once, preserve rank III, reject stale paths and leave permanent profiles untouched',()=>{
  const profile={troops:{storm:{forgeTier:3,cards:12}}},state={factions:{p0:'Aesir'},roster:{p0:rows(['storm']),p1:[]},profiles:{p0:profile}};
  const snapshot=structuredClone(profile);
  assert.equal(applyDraftPick(state,'p0',{type:'evolution',key:'storm',path:'b'}),true);assert.equal(state.roster.p0[0].lvl,2);assert.equal(state.evolutions.p0.storm,'b');
  applyDraftPick(state,'p0',{type:'evolution',key:'storm',path:'a'});assert.equal(state.evolutions.p0.storm,'b');
  state.roster.p0[0].lvl=3;delete state.evolutions.p0.storm;applyDraftPick(state,'p0',{type:'evolution',key:'storm',path:'a'});assert.equal(state.roster.p0[0].lvl,3);
  applyDraftPick(state,'p0',{type:'evolution',key:'guardian',path:'a'});assert.equal(state.evolutions.p0.guardian,undefined);
  assert.deepEqual(profile,snapshot);assert.equal(applyDraftPick(state,'p0',{type:'special',special:'double',for:'storm'}),true,'stale issued cards use shared bounded processing');
});

test('every faction combo has a relevant partner preview and a legal enhancement with no duplicate unlock',()=>{
  for(const combo of COMBOS){
    const roster=rows(combo.units),state={factions:{p0:combo.faction},decks:{p0:deckWith(combo.faction,combo.units)},roster:{p0:roster,p1:[]}};
    assert.equal(applyDraftPick(state,'p0',{type:'combo',key:combo.id}),true);assert.deepEqual(state.comboEnhancements.p0,[combo.id]);
    applyDraftPick(state,'p0',{type:'combo',key:combo.id});assert.deepEqual(state.comboEnhancements.p0,[combo.id]);
    const tracker=relevantComboStates(roster,combo.faction,{availableTypes:types(combo.faction),limit:20});assert.ok(tracker.some(value=>value.id===combo.id&&value.level===1),combo.id);
    if(new Set(combo.units).size>1){const without=roster.filter(row=>row.type!==combo.units.at(-1));const preview=draftCardPresentation({type:'troop',key:combo.units.at(-1)},without,{faction:combo.faction,troops});assert.ok(preview.comboPreview.some(value=>value.id===combo.id&&value.unlocks),combo.id);}
  }
  const state={factions:{p0:'Vanir'},roster:{p0:rows(['vrocket']),p1:[]}};applyDraftPick(state,'p0',{type:'combo',key:'guided-barrage'});assert.equal(state.comboEnhancements,undefined);
  assert.ok(relevantComboStates(rows(['storm']),'Aesir',{availableTypes:['storm','guardian']}).every(combo=>combo.units.every(key=>['storm','guardian'].includes(key))));
});

test('card metadata describes current and resulting battle ranks and the selected path tradeoff',()=>{
  for(const [key,paths]of Object.entries(EVOLUTIONS)){
    const faction=troops[key].faction,roster=rows([key],2);
    const card=draftCardPresentation({type:'special',special:'upgrade',for:key},roster,{faction,troops,evolutions:{[key]:'b'}});
    assert.equal(card.currentRank,2);assert.equal(card.resultingRank,3);assert.match(card.rankLabel,/II → III/);assert.ok(card.improvement.includes(paths.b.description));assert.equal(card.tradeoff,paths.b.tradeoff);
  }
});

test('authoritative and local draft helpers issue identical packs and persist new choices identically',async()=>{
  const {loadModules,createCombatAdapter}=require('../../../server/faction-forge/simulator.cjs'),modules=await loadModules();
  const roster=rows(['vguard','vrocket','suppressor'],1),all=types('Vanir');
  const state={factions:{p0:'Vanir',p1:'Helions'},decks:{p0:starterDeck('Vanir'),p1:starterDeck('Helions')},roster:{p0:roster,p1:[]},evolutions:{p0:{vguard:'b'},p1:{}},comboEnhancements:{p0:[],p1:[]},relics:{p0:['anti_armor_rounds'],p1:[]},tactics:{p0:['bodyguard'],p1:[]},formations:{p0:'wide',p1:'balanced'},profiles:{p0:{troops:Object.fromEntries(all.map(key=>[key,{unlocked:true}]))}}};
  for(let seed=0;seed<20;seed++){
    const adapter=createCombatAdapter({seed:'shared-draft'+seed,modules}),issued=adapter.pack(state,'p0',all);
    const expected=createDraftPack({...options('Vanir',roster,starterDeck('Vanir')),evolutions:state.evolutions.p0,comboEnhancements:[],ownedRelics:state.relics.p0,ownedTactics:state.tactics.p0,formation:'wide'},rng('shared-draft'+seed));
    assert.deepEqual(issued,expected);
  }
  for(const card of [{type:'evolution',key:'vrocket',for:'vrocket',path:'b'},{type:'combo',key:'suppression-bombardment'}]){
    const local=structuredClone(state);applyDraftPick(local,'p0',card);
    assert.deepEqual(createCombatAdapter({seed:'pick-parity',modules}).pick(state,'p0',card),local);
  }
});

test('bots assemble early partners, counter real enemy engines, and keep a functional frontline',()=>{
  for(const [faction,mine,pack,wanted]of [
    ['Aesir',['guardian','storm'],['runebinder','chronoweaver','miragewarden'],'runebinder'],
    ['Helions',['defiler','blood'],['burrower','spitter','matron'],'burrower'],
    ['Vanir',['vguard','vrocket'],['suppressor','rifleman','medic'],'suppressor']
  ]){const cards=pack.map(key=>({type:'troop',key}));assert.equal(cards[chooseAIDraft(cards,{myRoster:rows(mine),enemyRoster:[],aiStyle:'early_combos',difficulty:1},rng('bot-'+faction))].key,wanted);}
  const assassin={type:'troop',key:'miner'},base={myRoster:rows(['vguard','rifleman']),aiStyle:'sustained_upgrades'};
  assert.ok(scoreAIDraft(assassin,{...base,enemyRoster:rows(['vrocket','suppressor'])})>scoreAIDraft(assassin,{...base,enemyRoster:rows(['vrocket'])}));
  const pack=[{type:'special',special:'upgrade',for:'storm'},{type:'troop',key:'guardian'},{type:'evolution',key:'storm',path:'a'}];
  assert.equal(chooseAIDraft(pack,{myRoster:rows(['storm']),difficulty:1,aiStyle:'legendary_army'},rng('frontline')),1);
  assert.equal(Object.keys(AI_PLAYSTYLES).length,4);assert.ok(Object.keys(AI_PLAYSTYLES).includes(practiceOpponent(1200,rng('practice')).aiStyle));
  for(const aiStyle of Object.keys(AI_PLAYSTYLES)){const ctx={myRoster:rows(['guardian','storm'],2),enemyRoster:rows(['vrocket','suppressor']),aiStyle,difficulty:.7};assert.equal(chooseAIDraft(pack,ctx,rng('same')),chooseAIDraft(pack,ctx,rng('same')));}
  const stylePack=[{type:'troop',key:'runebinder'},{type:'special',special:'upgrade',for:'storm'},{type:'evolution',key:'guardian',path:'b'}],core=[{type:'guardian',count:2,lvl:2},{type:'storm',count:1,lvl:2}];
  assert.equal(chooseAIDraft(stylePack,{myRoster:core,difficulty:1,aiStyle:'early_combos'},rng('style')),0);
  assert.equal(chooseAIDraft(stylePack,{myRoster:core,difficulty:1,aiStyle:'defensive_evolution'},rng('style')),2);
  assert.equal(chooseAIDraft(stylePack,{myRoster:core,difficulty:1,aiStyle:'legendary_army'},rng('style')),1);
  assert.ok(scoreAIDraft(stylePack[1],{myRoster:core,aiStyle:'legendary_army'})>scoreAIDraft(stylePack[1],{myRoster:core,aiStyle:'sustained_upgrades'}));
});
