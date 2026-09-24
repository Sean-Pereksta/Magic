import test from 'node:test';
import assert from 'node:assert/strict';
import {createGame,kingdom,populationProjection,resolveEconomy,recruit,parseSave} from '../core.mjs';
import {calculatePopulationChange,populationCapacity,MAX_GROWTH_PER_TURN} from '../population.mjs';

test('a healthy starting kingdom gains six population and the forecast matches resolution for every House',()=>{
 const s=createGame(),expected=s.kingdoms.map(k=>populationProjection(s,k.id));assert.equal(expected[0].change,6);
 resolveEconomy(s);s.kingdoms.forEach((k,i)=>assert.equal(k.population,expected[i].nextPopulation));
});
test('cities and towns add growth while farms and settlements independently add capacity',()=>{
 const s=createGame(),k=kingdom(s,'ashen'),income={food:18,gold:10},base=calculatePopulationChange(s,'ashen',income);
 const t=s.tiles['9,9'];Object.assign(t,{owner:'ashen',building:'town',levels:{town:1}});
 const town=calculatePopulationChange(s,'ashen',income);assert.equal(town.change,base.change+2);assert.equal(town.capacity,base.capacity+80);
 t.building='city';t.levels={city:1};const city=calculatePopulationChange(s,'ashen',income);assert.equal(city.change,base.change+3);assert.equal(city.capacity,base.capacity+150);
 const farm=Object.values(s.tiles).find(t=>t.owner==='ashen'&&t.building==='farm');farm.levels.farm++;
 const upgraded=calculatePopulationChange(s,'ashen',income);assert.equal(upgraded.capacity,city.capacity+8);assert.equal(upgraded.change,city.change);
});
test('food surplus, happiness and low taxes grant bounded bonuses and high taxes slow growth',()=>{
 const s=createGame(),k=kingdom(s,'ashen');k.happiness=90;k.tax='low';
 const abundant=calculatePopulationChange(s,'ashen',{food:999999,gold:20});assert.equal(abundant.bonuses.find(b=>b.label==='Food surplus').amount,3);assert.equal(abundant.change,10);
 k.tax='high';assert.equal(calculatePopulationChange(s,'ashen',{food:999999,gold:20}).change,7);
 for(const t of Object.values(s.tiles).slice(0,100))Object.assign(t,{owner:'ashen',building:'city',levels:{city:1}});
 assert.equal(calculatePopulationChange(s,'ashen',{food:999999,gold:20}).change,MAX_GROWTH_PER_TURN);
});
test('capacity clips growth without destroying over-cap populations in old saves',()=>{
 const s=createGame(),k=kingdom(s,'ashen');k.population=populationCapacity(s,'ashen')-1;
 assert.equal(calculatePopulationChange(s,'ashen',{food:20,gold:10}).change,1);
 k.population+=20;const p=calculatePopulationChange(s,'ashen',{food:20,gold:10});assert.equal(p.change,0);assert.match(p.blockers.join(' '),/capacity/);
 const old=parseSave(JSON.stringify(s));assert.equal(kingdom(old,'ashen').population,k.population);
});
test('food reserve and happiness blockers are visible and the existing shortage loss remains three civilians',()=>{
 const s=createGame(),k=kingdom(s,'ashen');k.resources.food=10;let p=calculatePopulationChange(s,'ashen',{food:0,gold:0});assert.equal(p.change,0);assert.match(p.blockers.join(' '),/Food reserves/);
 k.resources.food=100;k.happiness=20;p=calculatePopulationChange(s,'ashen',{food:0,gold:0});assert.equal(p.change,0);assert.match(p.blockers.join(' '),/Happiness/);
 k.happiness=90;p=calculatePopulationChange(s,'ashen',{food:-101,gold:0});assert.equal(p.change,-3);assert.equal(p.happinessAfter,84);assert.ok(p.shortage);
 k.population=20;assert.equal(calculatePopulationChange(s,'ashen',{food:-101,gold:0}).change,0);
 for(const t of Object.values(s.tiles))if(t.owner==='ashen')t.owner='wintermere';
 const fallen=calculatePopulationChange(s,'ashen',{food:-101,gold:-1000});assert.equal(fallen.change,0);assert.match(fallen.blockers.join(' '),/No surviving settlement/);
});
test('recruitment retains its population and material cost while healthy growth replenishes it',()=>{
 const s=createGame(),k=kingdom(s,'ashen'),before={...k.resources};
 const result=recruit(s,'ashen','5,6','levy');assert.equal(result.ok,true);assert.equal(k.population,72);assert.ok(k.resources.food<before.food||k.resources.gold<before.gold);
 resolveEconomy(s);resolveEconomy(s);assert.ok(k.population>=80);
});
test('the forecast includes capacity and production from construction completing next turn',()=>{
 const s=createGame(),t=Object.values(s.tiles).find(t=>t.owner==='ashen'&&!t.building&&t.terrain==='plains');
 t.project={type:'farm',owner:'ashen',remaining:1,total:1,level:1};const before=JSON.stringify(s),expected=populationProjection(s,'ashen');
 assert.equal(JSON.stringify(s),before);assert.equal(expected.nextCapacity,expected.capacity+8);resolveEconomy(s);assert.equal(kingdom(s,'ashen').population,expected.nextPopulation);
});
