import test from 'node:test';
import assert from 'node:assert/strict';
import {BUILDINGS,RESOURCES} from '../data.mjs';
import {createGame,kingdom,build,buildCheck,commandLimit,populationProjection,resolveEconomy,recruit,parseSave,strategyTurn,declareWar} from '../core.mjs';
import {buildingLevel,cityBenefits,constructionSpec,tileProduction} from '../economy.mjs';
import {calculatePopulationChange,populationCapacity} from '../population.mjs';
import {ART,ALL_ART_PATHS,fallbackArtURL} from '../asset-manifest.mjs';
import {buildingInspection,constructionBrowser,musterBrowser} from '../expansion-ui.mjs';
import {setupMeta,claimSeat,startCampaign} from '../multiplayer-rounds.mjs';
import {applyCommand} from '../multiplayer-commands.mjs';

function stock(s,owner='ashen'){
 const k=kingdom(s,owner);for(const r of RESOURCES)k.resources[r]=5000;k.population=140;k.commands=8;return k;
}
test('base cities can complete exactly three paid upgrades; benefits activate only on completion',()=>{
 const s=createGame(),k=stock(s),t=s.tiles['5,6'];assert.equal(BUILDINGS.city.maxLevel,4);
 for(let level=2;level<=4;level++){
  const spec=constructionSpec(t,'city'),before={...k.resources},production=tileProduction(t,'ashen'),capacity=populationCapacity(s,'ashen'),orders=commandLimit(s,'ashen');
  assert.equal(spec.level,level);assert.equal(build(s,'ashen',t.id,'city').ok,true);
  for(const [r,n]of Object.entries(spec.cost))assert.equal(k.resources[r],before[r]-n);
  const paid=JSON.stringify(s);assert.equal(build(s,'ashen',t.id,'city').ok,false);assert.equal(JSON.stringify(s),paid);
  for(let turn=1;turn<t.project.total;turn++)resolveEconomy(s);
  assert.equal(buildingLevel(t,'city'),level-1);assert.deepEqual(tileProduction(t,'ashen'),production);assert.equal(commandLimit(s,'ashen'),orders);
  const forecast=populationProjection(s,'ashen');assert.equal(forecast.nextCapacity,capacity+50);
  resolveEconomy(s);assert.equal(k.population,forecast.nextPopulation);assert.equal(buildingLevel(t,'city'),level);
  assert.equal(populationCapacity(s,'ashen'),capacity+50);assert.equal(tileProduction(t,'ashen').food,production.food+4);assert.equal(tileProduction(t,'ashen').gold,production.gold+4);
  assert.equal(k.commands,orders+1);assert.equal(commandLimit(s,'ashen'),orders+1);
 }
 const before=JSON.stringify(s);assert.match(buildCheck(s,'ashen',t.id,'city'),/Maximum/);assert.equal(build(s,'ashen',t.id,'city').ok,false);assert.equal(JSON.stringify(s),before);
});
test('city tiers add growth independently of capacity, retain global limits, and do not unlock unit prerequisites',()=>{
 const s=createGame(),k=stock(s),t=s.tiles['5,6'],income={food:18,gold:10};
 const base=calculatePopulationChange(s,'ashen',income).change;
 for(let level=1;level<=4;level++){t.levels.city=level;const p=calculatePopulationChange(s,'ashen',income);assert.equal(p.change,base+level-1);assert.equal(p.bonuses.find(b=>b.label==='City upgrades').amount,level-1);}
 t.workshop=true;t.levels.workshop=3;assert.equal(commandLimit(s,'ashen'),8);
 assert.match(recruit(s,'ashen',t.id,'knight').error,/Knightly Hall/);
 for(const t of Object.values(s.tiles).slice(0,40))Object.assign(t,{owner:'ashen',building:'city',levels:{city:4}});
 assert.equal(commandLimit(s,'ashen'),8);assert.equal(calculatePopulationChange(s,'ashen',income).change,24);
});
test('higher-tier cities allow more paid musters with unchanged per-order troop and population costs',()=>{
 const s=createGame(),k=stock(s),t=s.tiles['5,6'];t.levels.city=4;resolveEconomy(s);
 const before={population:k.population,food:k.resources.food,gold:k.resources.gold,levy:s.armies[0].units.levy};
 assert.equal(k.commands,6);for(let n=0;n<6;n++)assert.equal(recruit(s,'ashen',t.id,'levy').ok,true);
 assert.equal(k.population,before.population-48);assert.equal(k.resources.food,before.food-72);assert.equal(k.resources.gold,before.gold-84);assert.equal(s.armies[0].units.levy,before.levy+48);
 assert.equal(recruit(s,'ashen',t.id,'levy').ok,false);
});
test('town promotion, old cities, completed tiers and pending upgrades survive schema-3 saves',()=>{
 const old=createGame();delete old.tiles['5,6'].levels.city;const resumed=parseSave(JSON.stringify(old));assert.equal(buildingLevel(resumed.tiles['5,6'],'city'),1);assert.equal(cityBenefits(resumed.tiles['5,6']).orders,0);
 const s=createGame();stock(s);const t=s.tiles['5,6'];t.building='town';delete t.levels.city;t.levels.town=1;
 assert.equal(build(s,'ashen',t.id,'city').ok,true);assert.equal(t.project.level,1);while(t.project)resolveEconomy(s);
 assert.equal(t.building,'city');assert.equal(cityBenefits(t).growth,3);
 t.levels.city=3;assert.equal(build(s,'ashen',t.id,'city').ok,true);
 const copy=parseSave(JSON.stringify(s));assert.deepEqual(copy.tiles[t.id],t);while(copy.tiles[t.id].project)resolveEconomy(copy);
 assert.equal(cityBenefits(parseSave(JSON.stringify(copy)).tiles[t.id]).level,4);
 copy.tiles[t.id].levels.city=5;assert.throws(()=>parseSave(JSON.stringify(copy)));
});
test('foreign or occupied cities cannot be upgraded, and online commands use the same paid upgrade',()=>{
 const meta=setupMeta({hostUid:'u0'},1000);claimSeat(meta,'u0','A','ashen');claimSeat(meta,'u1','B','wintermere');const s=startCampaign(meta,'u0',1000);meta.epoch=1;stock(s,'wintermere');
 const c={id:'city-upgrade',clientId:'city-test-client',sequence:1,uid:'u1',actorHouseId:'wintermere',turn:1,stateVersion:1,epoch:1,type:'build',args:{tile:'17,4',building:'city'}};
 const before=JSON.stringify(s);assert.equal(applyCommand(s,meta,{...c,args:{tile:'5,6',building:'city'}}).ok,false);assert.equal(JSON.stringify(s),before);
 assert.equal(applyCommand(s,meta,c).ok,true);assert.equal(s.tiles['17,4'].project.level,2);assert.equal(applyCommand(s,meta,c).ok,false);
 const lone=createGame();stock(lone);declareWar(lone,'ashen','wintermere');lone.armies[1].tile='5,6';assert.match(buildCheck(lone,'ashen','5,6','city'),/Enemy troops/);
});
test('AI recruits with upgraded city orders while retaining its resource and population constraints',()=>{
 const s=createGame(),id='wintermere',k=stock(s,id),home=s.tiles['17,4'];home.levels.city=4;k.commands=commandLimit(s,id);
 s.armies=s.armies.filter(a=>a.owner!==id);for(const other of s.kingdoms)if(other.id!==id)other.commands=0;
 for(const t of Object.values(s.tiles))if(t.owner===id&&!t.project)t.project={type:t.building||'farm',owner:id,level:1,total:2,remaining:2};
 strategyTurn(s);const report=s.strategy.history.at(-1).houses.find(h=>h.owner===id),mustering=report.actions.filter(a=>a.kind==='recruit');
 assert.ok(mustering.length>1);assert.ok(mustering.length<=4);assert.ok(k.population>=20);assert.ok(RESOURCES.every(r=>k.resources[r]>=0));
});
test('a healthy AI can plan and buy another city tier through the normal construction rules',()=>{
 const s=createGame(),id='wintermere',k=stock(s,id),home=s.tiles['17,4'];s.turn=24;k.population=25;k.commands=1;
 for(const other of s.kingdoms)if(other.id!==id)other.commands=0;
 for(const [type,b]of Object.entries(BUILDINGS))if(b.settlement){home[type]=true;home.levels[type]=b.maxLevel;}
 home.walls=180;
 for(const t of Object.values(s.tiles))if(t.owner===id&&t!==home)t.project={type:t.building||'farm',owner:id,level:1,total:2,remaining:2};
 const cost=constructionSpec(home,'city').cost,before={...k.resources};strategyTurn(s);
 assert.equal(home.project?.type,'city');assert.equal(home.project.level,2);
 for(const [r,n]of Object.entries(cost))assert.equal(k.resources[r],before[r]-n);
});
test('city cards describe tiers and muster orders using the existing city artwork',()=>{
 const s=createGame();stock(s);const t=s.tiles['5,6'];t.levels.city=4;
 assert.match(buildingInspection(s,t),/Royal City.*Level 4/);assert.match(buildingInspection(s,t),/3 \/ 3 city upgrades/);assert.match(musterBrowser(s,t),/\+3 kingdom build\/recruit orders/);
 assert.match(constructionBrowser(s,t),/Maximum level reached/);
 for(let level=1;level<=4;level++){assert.equal(ART.structures.city[level],ART.structures.city[1]);assert.match(fallbackArtURL(ART.structures.city[level]),/city_1.svg$/);}
 assert.equal(ALL_ART_PATHS.filter(p=>/^buildings\/city_/.test(p)).length,1);
});
