import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame, parseSave, checkVictory, rebuildTerritory, economyProjection, build, recruit } from '../core.mjs';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';
import { endTurn } from '../diplomacy.mjs';
import { MAP_PROFILES, selectMapProfile } from '../map-profiles.mjs';
import { validateWorld } from '../world-generation.mjs';
import { HOUSES, BUILDINGS } from '../data.mjs';
import { distance, neighbors, passable } from '../world-hex.mjs';
import { foundCity, foundingCheck, startingFootprint, planFoundings, foundAIKingdoms, foundedCapitals, foundingOutlook, STARTING_BUILDINGS } from '../founding.mjs';
import { tileProduction } from '../economy.mjs';
import { buildGeography, maskEdges, neighborId, oppositeEdge } from '../geography.mjs';
import { setupMeta, claimSeat, startCampaign, resolutionDue, resolveRound, configureCampaign } from '../multiplayer-rounds.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { packCampaign, decodePayload, playerView, joinCampaign } from '../multiplayer-state.mjs';
const worlds=new Map();
const world=(seed=8147,profile='heartlands')=>{const key=`${seed}:${profile}`;if(!worlds.has(key))worlds.set(key,createGame(seed,profile));return structuredClone(worlds.get(key));};
const natural=s=>Object.values(s.tiles).map(t=>[t.id,t.terrain,t.resource,t.quality,t.region,t.river]);
const allAI=s=>{s.controllers=Object.fromEntries(HOUSES.map(h=>[h.id,{kind:'ai',uid:null,name:''}]));return s;};
function components(s,terrain){const seen=new Set(),sizes=[];for(const t of Object.values(s.tiles).filter(t=>t.terrain===terrain)){if(seen.has(t.id))continue;const q=[t];seen.add(t.id);for(let i=0;i<q.length;i++)for(const n of neighbors(s,q[i]))if(n.terrain===terrain&&!seen.has(n.id)){seen.add(n.id);q.push(n);}sizes.push(q.length);}return sizes;}
for(const profile of Object.keys(MAP_PROFILES))test(`${profile}: three seeds have connected regions, natural resource patterns, six viable starts and deterministic retries`,()=>{
 for(const seed of [1,42,8147]){
  const s=world(seed,profile),tiles=Object.values(s.tiles);
  assert.equal(s.phase,'founding');assert.equal(s.turn,0);assert.equal(s.armies.length,0);assert.ok(tiles.every(t=>!t.owner&&!t.building));
  assert.equal(validateWorld(s).ok,true);
  for(const [terrain,size]of [['mountain',15],['forest',45],['plains',55],['hills',40]]){
    assert.ok(Math.max(...components(s,terrain))>=size,`${terrain} region`);
    const group=tiles.filter(t=>t.terrain===terrain);assert.ok(group.filter(t=>neighbors(s,t).some(n=>n.terrain===terrain)).length/group.length>.9);
  }
  assert.ok(tiles.filter(t=>t.terrain==='hills').some(t=>neighbors(s,t).some(n=>n.terrain==='mountain')));
  assert.ok(tiles.some(t=>t.mountainPass&&passable(t)));
  assert.ok(tiles.filter(t=>t.resource==='wood').every(t=>t.terrain==='forest'));
  assert.ok(tiles.filter(t=>['iron','stone'].includes(t.resource)).every(t=>t.terrain==='hills'));
  assert.ok(tiles.filter(t=>t.resource==='food').every(t=>t.terrain==='plains'));
  const original=natural(s);allAI(s);assert.equal(foundAIKingdoms(s).ok,true);assert.equal(s.turn,1);
  const caps=foundedCapitals(s);assert.equal(caps.length,6);for(const a of caps)for(const b of caps)if(a!==b)assert.ok(distance(a,b)>=8);
  for(const h of HOUSES){
    const c=s.tiles[s.founding.houses[h.id].capital],buildings=tiles.filter(t=>t.owner===h.id&&t.building&&t!==c);
    const actual=buildings.map(t=>t.building).sort();assert.deepEqual(actual,[...STARTING_BUILDINGS[h.id]].sort());
    for(const t of buildings){assert.ok(distance(c,t)<=4);assert.ok(BUILDINGS[t.building].terrain.includes(t.terrain));if(BUILDINGS[t.building].resource)assert.equal(t.resource,BUILDINGS[t.building].resource);assert.equal(t.road,true);}
    assert.ok(economyProjection(s,h.id).income.food>0,`${h.id} sustainable food`);
    assert.equal(s.armies.find(a=>a.owner===h.id).units.levy,20);
    assert.deepEqual(s.kingdoms.find(k=>k.id===h.id).resources,{food:140,wood:110,stone:90,iron:50,gold:200,horses:12,tools:0,arms:0});
  }
  assert.deepEqual(natural(s),original,'founding must never edit geography');
  rebuildTerritory(s);for(const t of tiles.filter(t=>t.building))assert.ok(t.owner);
  assert.deepEqual(parseSave(JSON.stringify(s)),s);
 }
 assert.deepEqual(world(42,profile),createGame(42,profile));
 const first=natural(world(1,profile)),second=natural(world(42,profile));assert.ok(first.filter((t,i)=>t[1]!==second[i][1]).length>200);
});
test('random map selection derives from each campaign seed and explicit profiles win',()=>{
 const selected=new Set(Array.from({length:100},(_,i)=>selectMapProfile(i+1)));assert.equal(selected.size,6);
 const m=setupMeta({hostUid:'a'},1),other=setupMeta({hostUid:'a'},2);assert.notEqual(m.options.seed,other.options.seed);
 assert.equal(m.options.preset,'random');claimSeat(m,'a','A','ashen');configureCampaign(m,'a',{seed:42,preset:'great-basin',timerSeconds:120,absent:'hold'});
 const s=startCampaign(m,'a',1);assert.equal(s.mapProfile,'great-basin');assert.equal(m.mapProfile,s.mapProfile);assert.equal(m.seed,42);assert.equal(m.deadline,0);
});
test('water, mountains, occupied land, isolated enclaves and capitals closer than 8 are rejected without mutations',()=>{
 const s=world(),tiles=Object.values(s.tiles);
 for(const terrain of ['water','mountain']){const before=JSON.stringify(s);assert.match(foundCity(s,'ashen',tiles.find(t=>t.terrain===terrain).id).error,new RegExp(terrain));assert.equal(JSON.stringify(s),before);}
 const site=planFoundings(s)[0].capital;assert.equal(foundCity(s,'ashen',site).ok,true);
 const near=tiles.find(t=>passable(t)&&!t.building&&distance(t,s.tiles[site])===7);assert.match(foundingCheck(s,'wintermere',near.id),/8 hexes/);
 const atEight=tiles.find(t=>distance(t,s.tiles[site])===8&&!foundingCheck(s,'wintermere',t.id));assert.ok(atEight,'exactly 8 hexes must remain legal');
 assert.equal(foundCity(s,'wintermere',atEight.id).ok,true);
 assert.match(foundCity(s,'ashen',site).error,/already founded/);
 const isolated=world(),c=isolated.tiles['20,15'];c.terrain='plains';for(const n of neighbors(isolated,c))n.terrain='mountain';
 assert.equal(startingFootprint(isolated,'ashen',c),null);assert.match(foundCity(isolated,'ashen',c.id).error,/nearby usable land/);
 const foreign=tiles.find(t=>passable(t)&&!t.building&&distance(t,s.tiles[site])>10&&distance(t,atEight)>8);foreign.owner='vesper';assert.match(foundingCheck(s,'thornwall',foreign.id),/territory/);
});
test('outlook describes the radius, resources are independent of House, and AI founding is deterministic for identical claims',()=>{
 const s=world(),plan=planFoundings(s),c=s.tiles[plan[0].capital],view=foundingOutlook(s,c.id);assert.ok(Object.values(view.terrain).reduce((a,b)=>a+b,0)>1);
 assert.equal(foundCity(s,'ashen',c.id).ok,true);const other=structuredClone(s);
 assert.equal(foundAIKingdoms(s).ok,true);assert.equal(foundAIKingdoms(other).ok,true);assert.deepEqual(s,other);
 const t=Object.values(s.tiles).find(t=>t.building==='mine');assert.deepEqual(tileProduction(t,'ashen'),tileProduction(t,'thornwall'));
 const next=structuredClone(s);endTurn(s);endTurn(next);assert.deepEqual(s,next);assert.equal(s.turn,2);
});
test('pre-Turn 1 freezes economy, victory and commands and resumes from the exact saved founding state',()=>{
 const s=world(),before=JSON.stringify(s);endTurn(s);checkVictory(s);assert.equal(JSON.stringify(s),before);
 assert.match(build(s,'ashen','20,15','farm').error,/Found all/);assert.match(recruit(s,'ashen','20,15','levy').error,/Found all/);
 const site=planFoundings(s)[0].capital;assert.equal(foundCity(s,'ashen',site).ok,true);const resumed=parseSave(JSON.stringify(s));assert.deepEqual(resumed,s);
 assert.equal(foundAIKingdoms(resumed).ok,true);assert.equal(resumed.turn,1);
 const bad=world();bad.founding.houses.ashen={founded:true,capital:'0,0'};assert.throws(()=>parseSave(JSON.stringify(bad)),/founding/);
 const old=legacyGame(555),snapshot=JSON.stringify(old);assert.deepEqual(parseSave(snapshot),old);assert.equal(JSON.stringify(old),snapshot);
});
test('generated rivers start in highlands and their directional graph reaches a coast with reciprocal links',()=>{
 const s=world(),graph=buildGeography(s.tiles),rivers=Object.values(s.tiles).filter(t=>t.river);assert.ok(rivers.length>8);
 assert.ok(rivers.some(t=>t.terrain==='hills'&&neighbors(s,t).some(n=>n.terrain==='mountain')));
 const seen=new Set();for(const source of rivers){if(seen.has(source.id))continue;const q=[source];seen.add(source.id);let outlet=false;
  for(let i=0;i<q.length;i++){const t=q[i],g=graph.get(t.id);outlet ||= !!g.mouthMask;for(const edge of maskEdges(g.riverMask)){const id=neighborId(t,edge),n=s.tiles[id];if(g.mouthMask&(1<<edge))continue;assert.ok(graph.get(id).riverMask&(1<<oppositeEdge(edge)));if(n&&!seen.has(id)){seen.add(id);q.push(n);}}}
  assert.ok(outlet,'each river component reaches the sea/lake');
 }
});
test('authoritative founding rejects conflicting stale claims and reconnect snapshots preserve all map and founding fields',async()=>{
 const m=setupMeta({hostUid:'a'},1000);claimSeat(m,'a','A','ashen');claimSeat(m,'b','B','wintermere');m.options.seed=8147;m.options.preset='heartlands';
 const s=startCampaign(m,'a',1000);m.epoch=1;
 const c=(actor,tile,sequence=1)=>({id:`found-${actor}-${sequence}`,clientId:'founding-test',sequence,uid:m.seats[actor].uid,actorHouseId:actor,turn:s.turn,stateVersion:1,epoch:1,type:'found',args:{tile}});
 const plan=planFoundings(s),chosen=plan[0].capital,conflict=c('wintermere',chosen);
 assert.equal(resolutionDue(m,{a:{at:1000},b:{at:1000}},1000,s),false);
 const forbidden={...c('ashen',chosen),type:'ready',args:{ready:true}};assert.equal(applyCommand(s,m,forbidden).ok,false);
 assert.equal(applyCommand(s,m,c('ashen',chosen)).ok,true);m.stateVersion++;
 const before=JSON.stringify(s);assert.equal(applyCommand(s,m,conflict).ok,false);assert.equal(JSON.stringify(s),before);assert.equal(s.turn,0);
 assert.throws(()=>resolveRound(s,{...m,phase:'resolving'},{},1000),/not locked/);
 const packed=await packCampaign(s,m),canon=await decodePayload(packed.canonical.payload),publicState=await decodePayload(packed.world.payload);
 const priv=Object.fromEntries(await Promise.all(Object.entries(packed.privateByHouse).map(async([id,p])=>[id,await decodePayload(p.payload)])));
 const restored=joinCampaign(canon,priv),view=playerView(publicState,priv.wintermere,'wintermere');
 for(const field of ['seed','mapProfile','phase','founding'])assert.deepEqual(view[field],s[field]);
 assert.deepEqual(Object.fromEntries(Object.entries(view.tiles).map(([id,{fog,observedTurn,...t}])=>[id,t])),s.tiles);
 assert.ok(Object.values(playerView(publicState,priv.ashen,'ashen').tiles).some(t=>t.fog==='unknown'));
 assert.deepEqual(restored.founding,s.founding);
 const second=planFoundings(restored).find(p=>p.owner==='wintermere').capital;
 assert.equal(applyCommand(restored,m,c('wintermere',second,2),{now:2000}).ok,true);
 assert.equal(restored.turn,1);assert.equal(foundedCapitals(restored).length,6);assert.equal(m.phase,'planning');
});
