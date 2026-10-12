// Bounded campaign acceptance probe; run separately from isolated rule tests.
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { createGame, parseSave, settlements } from '../core.mjs';
import { foundCity, foundAIKingdoms, planFoundings } from '../founding.mjs';
import { chooseRoyalDecree } from '../opening.mjs';
import { endTurn } from '../diplomacy.mjs';

const scenarios=[
  {seed:42,profile:'heartlands',houses:6,difficulty:'easy'},
  {seed:8147,profile:'great-divide',houses:8,difficulty:'medium'},
  {seed:92,profile:'shattered-realms',houses:12,difficulty:'hard'},
  {seed:1,profile:'highland-crown',houses:10,difficulty:'insane'}
];
const results=[];
for(const scenario of scenarios){
  const s=createGame(scenario.seed,scenario.profile,scenario.houses,scenario.difficulty),site=planFoundings(s)[0].capital;
  assert.equal(foundCity(s,'ashen',site).ok,true);assert.equal(foundAIKingdoms(s).ok,true);
  assert.equal(chooseRoyalDecree(s,'ashen','prosperity').ok,true);
  assert.ok(Object.values(s.tiles).filter(t=>t.premiumRegion).every(t=>!t.owner));
  let resumed=parseSave(JSON.stringify(s));const times=[];
  for(let round=0;round<5;round++){
    const start=performance.now();endTurn(s);times.push(performance.now()-start);
    endTurn(resumed);assert.deepEqual(resumed,s,'save/load must preserve deterministic campaign results');
    resumed=parseSave(JSON.stringify(s));
    for(const k of s.kingdoms){assert.ok(k.opening.decree);assert.ok(Object.values(k.resources).every(n=>Number.isFinite(n)&&n>=0));}
    assert.ok(s.armies.every(a=>Object.values(a.units).every(n=>Number.isInteger(n)&&n>=0)));
  }
  const towns=settlements(s).length,used=s.kingdoms.filter(k=>k.opening.charterUsed).length;
  assert.ok(towns>scenario.houses,'AI must turn accelerated opening resources into actual expansion');
  assert.ok(used>0,'AI must make use of the First Frontier Charter');
  results.push({...scenario,turn:s.turn,settlements:towns,chartersUsed:used,scouts:s.armies.filter(a=>a.scout).length,
    decrees:Object.fromEntries(['prosperity','mobilization','influence'].map(id=>[id,s.kingdoms.filter(k=>k.opening.decree===id).length])),
    averageTurnMs:Math.round(times.reduce((a,b)=>a+b,0)/times.length),maxTurnMs:Math.round(Math.max(...times))});
  console.log(`PASS ${scenario.houses} Houses / ${scenario.profile} / ${scenario.difficulty}: ${towns} settlements, ${used} charters used; deterministic through turn ${s.turn}.`);
}
console.log(JSON.stringify(results,null,2));
