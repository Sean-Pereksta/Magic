import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { emptyUnits } from '../economy.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { recordKingdomDevelopments, kingdomDevelopments, validateKingdomDevelopments } from '../kingdom-developments.mjs';
import { premiumMapMarkers, premiumRegionPanel, openingCouncilBriefing, royalOpeningPanel, kingdomDevelopmentsPanel } from '../ambition-ui.mjs';

function field() {
  const s = createGame(513);
  for (const t of Object.values(s.tiles)) Object.assign(t,{terrain:'plains',owner:null,building:null,resource:null,name:null,capital:null,levels:{},project:null,road:false,river:false});
  const home = s.tiles['5,6'];Object.assign(home,{owner:'ashen',building:'city',capital:'ashen',name:'Emberkeep',levels:{city:1}});
  s.armies = [{id:'own-scout',owner:'ashen',tile:'5,6',units:{...emptyUnits(),scout:2},path:[],target:null,order:'hold',formation:'balanced',morale:1}];
  s.phase='playing';s.turn=1;s.fog={version:1,houses:{}};s.events=[];s.treaties=[];s.wars=[];
  s.ambitionVersion=1;for(const k of s.kingdoms)k.opening={charterUsed:false,decree:null,decreeTurn:null};
  s.premiumRegions={};
  return s;
}
function observe(s) {refreshKnowledge(s,['ashen']);recordKingdomDevelopments(s,['ashen']);}
function rich(s,id='9,6') {
  const t=s.tiles[id];Object.assign(t,{premiumRegion:'rich-iron',premiumYield:'iron',premiumFactor:2.5,resource:'iron'});
  s.premiumRegions['rich-iron']={id:'rich-iron',kind:'iron-mountains',name:'Great Iron Mountains',center:id,tiles:[id],resource:'iron',multiplier:2.5,building:'mine',access:'SECRET HIDDEN PASS',protected:true};return t;
}

test('opening choices show exact bonuses, charter and campaign-turn expiration; legacy saves get none',()=>{
  const s=field();let html=royalOpeningPanel(knowledgeView(s),'ashen');
  assert.equal((html.match(/data-decree=/g)||[]).length,3);assert.match(html,/25%/);assert.match(html,/after turn 6/);assert.match(html,/10% fewer/);
  s.kingdoms[0].opening.decree='influence';s.kingdoms[0].opening.decreeTurn=1;s.turn=6;html=royalOpeningPanel(knowledgeView(s),'ashen');assert.match(html,/1 turn left/);assert.doesNotMatch(html,/data-decree=/);
  s.turn=7;assert.match(royalOpeningPanel(knowledgeView(s),'ashen'),/concluded/);delete s.ambitionVersion;assert.equal(royalOpeningPanel(knowledgeView(s),'ashen'),'');
});

test('premium markers, inspection and opening briefing never reveal undiscovered regions or hidden access',()=>{
  const s=field(),t=rich(s,'22,18');observe(s);let v=knowledgeView(s);
  assert.equal(premiumMapMarkers(s).length,0);assert.equal(premiumRegionPanel(s,t),'');assert.deepEqual(openingCouncilBriefing(s,'ashen'),[]);
  assert.equal(premiumMapMarkers(v).length,0);assert.equal(premiumRegionPanel(v,v.tiles[t.id]),'');assert.doesNotMatch(JSON.stringify(openingCouncilBriefing(v,'ashen')),/Great Iron|22,18|SECRET/);
  s.armies[0].tile=t.id;observe(s);v=knowledgeView(s);assert.equal(premiumMapMarkers(v).length,1);assert.match(premiumRegionPanel(v,v.tiles[t.id]),/2.5/);assert.doesNotMatch(premiumRegionPanel(v,v.tiles[t.id]),/SECRET/);
  s.armies[0].tile='5,6';s.turn++;t.owner='wintermere';t.building='fort';t.levels={fort:1};observe(s);v=knowledgeView(s);
  const html=premiumRegionPanel(v,v.tiles[t.id]);assert.match(html,/Last|last observed/);assert.doesNotMatch(html,/House Wintermere/);
});

test('important own construction is reported from addressed administrative records without reporting ordinary farms',()=>{
  const s=field();observe(s);s.turn=2;
  Object.assign(s.tiles['8,6'],{owner:'ashen',building:'fort',levels:{fort:1}});
  Object.assign(s.tiles['6,6'],{owner:'ashen',building:'farm',levels:{farm:1}});
  s.events.push({turn:1,kind:'economy',audience:['ashen'],message:'Fort completed at 8,6.'},{turn:1,kind:'economy',audience:['ashen'],message:'Farmstead completed at 6,6.'});observe(s);
  const events=kingdomDevelopments(knowledgeView(s),'ashen',24);
  assert.ok(events.some(e=>e.kind==='complete'&&e.tile==='8,6'));assert.ok(!events.some(e=>e.tile==='6,6'));
});

test('developments report only new legitimate observations and preserve dated reports after sight is lost',()=>{
  const s=field(),near=s.tiles['9,6'],hidden=s.tiles['24,18'];observe(s);
  s.turn=2;Object.assign(near,{owner:'wintermere',building:'town',name:'Visible Frontier',levels:{town:1},project:{type:'wall',level:1,remaining:2,total:2}});
  Object.assign(hidden,{owner:'vesper',building:'town',name:'SECRET HIDDEN TOWN',levels:{town:1},project:{type:'fort',level:1}});
  s.armies.push({id:'enemy',owner:'wintermere',tile:'7,6',units:{...emptyUnits(),levy:30},path:[],target:'SECRET',order:'attack',formation:'balanced',morale:1});observe(s);
  let events=kingdomDevelopments(knowledgeView(s),'ashen',24);assert.ok(events.some(e=>e.kind==='settlement'));assert.ok(events.some(e=>e.kind==='defense'));assert.ok(events.some(e=>e.kind==='army'));
  assert.doesNotMatch(JSON.stringify(events),/SECRET HIDDEN|24,18|SECRET/);assert.ok(events.find(e=>e.kind==='army').priority>events.find(e=>e.kind==='settlement').priority);
  const saved=structuredClone(events);s.armies[0].tile='1,1';s.armies[1].tile='25,18';s.turn=3;near.name='SECRET RENAMED TOWN';near.owner='vesper';observe(s);events=kingdomDevelopments(knowledgeView(s),'ashen',24);
  assert.doesNotMatch(JSON.stringify(events),/SECRET RENAMED|25,18/);assert.ok(events.some(e=>e.body.includes('Visible Frontier')));assert.equal(events.filter(e=>e.kind==='army').length,saved.filter(e=>e.kind==='army').length);
  assert.doesNotThrow(()=>validateKingdomDevelopments(s));
});

test('an old sighting from this turn does not allow a new hidden project through a development summary',()=>{
  const s=field(),t=s.tiles['10,6'];Object.assign(t,{owner:'wintermere',building:'town',name:'Known Town',levels:{town:1}});observe(s);
  s.armies[0].tile='1,1';t.project={type:'fort',level:1};recordKingdomDevelopments(s,['ashen']);
  assert.equal(kingdomDevelopments(knowledgeView(s),'ashen',24).filter(e=>e.kind==='defense').length,0);
});

test('closer observed frontier settlements outrank distant settlements of the same kind',()=>{
  const s=field();observe(s);s.turn=2;
  Object.assign(s.tiles['7,6'],{owner:'wintermere',building:'town',name:'Near Frontier',levels:{town:1}});
  Object.assign(s.tiles['10,6'],{owner:'vesper',building:'town',name:'Distant Frontier',levels:{town:1}});
  observe(s);
  const events=kingdomDevelopments(knowledgeView(s),'ashen',24).filter(e=>e.kind==='settlement');
  assert.equal(events.length,2);assert.equal(events[0].tile,'7,6');assert.equal(events[1].tile,'10,6');assert.ok(events[0].priority>events[1].priority);
});

test('rich ownership changes and regional treaties are interactive, deduplicated and private to the viewer',()=>{
  const s=field(),t=rich(s);observe(s);s.turn=2;t.owner='wintermere';s.treaties.push({type:'trade',parties:['ashen','wintermere'],expires:10});s.wars.push('ashen:vesper');observe(s);observe(s);
  const v=knowledgeView(s),events=kingdomDevelopments(v,'ashen',24),html=kingdomDevelopmentsPanel(v,'ashen',24);
  assert.equal(events.filter(e=>e.kind==='region-owner').length,1);assert.equal(events.filter(e=>e.kind==='treaty').length,1);assert.equal(events.filter(e=>e.kind==='war').length,1);
  assert.match(html,/data-goto="9,6"/);assert.match(html,/data-talk="wintermere"/);assert.deepEqual(kingdomDevelopments(v,'vesper',24),[]);
  const loaded=JSON.parse(JSON.stringify(s));assert.deepEqual(kingdomDevelopments(loaded,'ashen',24),events);assert.doesNotThrow(()=>validateKingdomDevelopments(loaded));
});

test('development validation rejects hidden references, forged owners, malformed turns and oversized histories',()=>{
  const s=field();observe(s);s.turn=2;s.wars.push('ashen:vesper');observe(s);assert.doesNotThrow(()=>validateKingdomDevelopments(s));
  for(const mutate of [x=>x.kingdomDevelopments.houses.ashen.events[0].tile='24,18',x=>x.kingdomDevelopments.houses.ashen.events[0].house='unknown-house',x=>x.kingdomDevelopments.houses.ashen.events[0].turn=100,x=>x.kingdomDevelopments.houses.ashen.events=Array(25).fill(x.kingdomDevelopments.houses.ashen.events[0])]){const bad=structuredClone(s);mutate(bad);assert.throws(()=>validateKingdomDevelopments(bad),/Damaged Kingdom/);}
});
