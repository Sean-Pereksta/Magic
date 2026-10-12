import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame, parseSave, rebuildTerritory } from '../core.mjs';
import { foundAIKingdoms, foundingCheck, planFoundings, STARTING_RADIUS, startingFootprint } from '../founding.mjs';
import { MAP_PROFILES } from '../map-profiles.mjs';
import { BUILDINGS } from '../data.mjs';
import { distance, neighbors, passable } from '../world-hex.mjs';
import { tileProduction, productionPlan } from '../economy.mjs';
import { shoreNodes } from '../naval-graph.mjs';
import { knowledgeView, refreshKnowledge } from '../fog.mjs';
import { knownPremiumRegions, premiumProductionFactor, projectPremiumRegions, validatePremiumRegions } from '../premium-regions.mjs';

for(const profile of Object.keys(MAP_PROFILES))test(`${profile}: premium regions retain valid starts and usable approaches for every House count`,()=>{
  for(const count of [6,8,10,12]){
    const s=createGame(42,profile,count),regions=Object.values(s.premiumRegions),plan=planFoundings(s);
    assert.ok(regions.length>=4,'rare premium objectives appear reliably');
    assert.equal(plan?.length,count,'all Houses still have viable separated starts');
    const starts=plan.map(f=>s.tiles[f.capital]),reachable=new Set(starts.map(t=>t.id)),queue=[...starts];
    for(let i=0;i<queue.length;i++)for(const n of neighbors(s,queue[i]))if(passable(n)&&!reachable.has(n.id)){reachable.add(n.id);queue.push(n);}
    for(const region of regions){
      const center=s.tiles[region.center];
      assert.ok(reachable.has(center.id),'center is reachable by ordinary land movement from a viable start');
      assert.ok(passable(center));assert.ok(neighbors(s,center).some(passable));
      assert.match(foundingCheck(s,s.kingdoms[0].id,center.id),/premium/);
      assert.equal(startingFootprint(s,s.kingdoms[0].id,center),null);
      for(const id of region.tiles){
        const t=s.tiles[id];assert.ok(starts.every(c=>distance(c,t)>STARTING_RADIUS));
        assert.ok(plan.every(f=>!f.territory.includes(id)));
        assert.ok(BUILDINGS[region.building].terrain?.includes(t.terrain)||region.resource==='gold');
        if(region.resource!=='gold')assert.equal(t.resource,region.resource);
      }
    }
    s.controllers=Object.fromEntries(s.kingdoms.map(h=>[h.id,{kind:'ai'}]));
    assert.equal(foundAIKingdoms(s).ok,true);rebuildTerritory(s);
    assert.ok(regions.every(r=>r.tiles.every(id=>s.tiles[id].owner===null)),'starting claims and territory rebuild leave premiums unowned');
    validatePremiumRegions(s);
  }
});

test('premium generation is deterministic and bounded production replaces deposit quality at every tier',()=>{
  const s=createGame(8147,'heartlands'),copy=createGame(8147,'heartlands');
  assert.deepEqual(s.premiumRegions,copy.premiumRegions);
  assert.ok(Object.values(s.premiumRegions).some(r=>r.protected&&r.multiplier===2.5));
  for(const region of Object.values(s.premiumRegions))for(let level=1;level<=3;level++){
    const original=s.tiles[region.center],type=region.building;
    const built={...original,building:type,road:true,levels:{[type]:level},...(BUILDINGS[type].settlement?{building:'town',[type]:true}:{})};
    const normal={...built,quality:'normal'};delete normal.premiumRegion;delete normal.premiumYield;delete normal.premiumFactor;
    const baseline=tileProduction(normal,'ashen')[region.resource]-(normal.building==='town'&&region.resource==='gold'?5:0);
    for(const quality of ['poor','normal','rich','exceptional']){
      const output=tileProduction({...built,quality},'ashen')[region.resource]-(built.building==='town'&&region.resource==='gold'?5:0);
      assert.ok(output/baseline>=1.5&&output/baseline<=2.5,`${region.name} tier ${level}: ${output}/${baseline}`);
      assert.equal(output,tileProduction(built,'ashen')[region.resource]-(built.building==='town'&&region.resource==='gold'?5:0),'quality cannot stack on premium production');
    }
    assert.equal(tileProduction({...original,building:null,levels:{}},'ashen')[region.resource],0,'discovery alone gives no production');
  }
  const crossroads=Object.values(s.premiumRegions).find(r=>r.kind==='trade-crossroads'),t=s.tiles[crossroads.center];
  assert.equal(premiumProductionFactor({...t,road:false},'gold','tradeOutpost'),null);
  assert.equal(premiumProductionFactor({...t,road:true},'gold','tradeOutpost'),crossroads.multiplier);
  assert.equal(premiumProductionFactor({...t,road:true},'gold','chancery'),null,'advanced secondary producers receive no premium multiplier');
});

test('fog projection exposes only discovered premium geography and remembered ownership',()=>{
  const s=createGame(8147,'heartlands');
  s.controllers=Object.fromEntries(s.kingdoms.map(h=>[h.id,{kind:'ai'}]));foundAIKingdoms(s);refreshKnowledge(s);
  const view=knowledgeView(s,'ashen'),known=knownPremiumRegions(s,'ashen');
  for(const region of Object.values(projectPremiumRegions(view,'ashen'))){
    assert.ok(region.tiles.every(id=>view.tiles[id].fog!=='unknown'));
    assert.ok(view.tiles[region.center].fog!=='unknown');
  }
  const secret=Object.values(s.premiumRegions).find(r=>r.tiles.every(id=>view.tiles[id].fog==='unknown'));
  assert.ok(secret);assert.ok(!known.some(r=>r.id===secret.id));
  const sample=Object.values(s.premiumRegions).find(r=>r.tiles.length===2),satellite=sample.tiles.find(id=>id!==sample.center);
  const projected=structuredClone(view);
  projected.premiumRegions=structuredClone(s.premiumRegions);
  for(const t of Object.values(projected.tiles))t.fog='unknown';
  projected.tiles[satellite]={...s.tiles[satellite],fog:'explored',owner:'wintermere'};
  const partial=projectPremiumRegions(projected,'ashen')[sample.id];
  assert.deepEqual(partial.tiles,[satellite]);assert.equal(partial.center,satellite);assert.equal(partial.centerKnown,false);assert.equal(partial.owner,'wintermere');
  assert.equal(partial.protected,null);assert.match(partial.access,/not yet fully scouted/);
});

test('premium saves round-trip and invalid output metadata is rejected',()=>{
  const s=createGame(88,'great-divide');assert.deepEqual(parseSave(JSON.stringify(s)),s);
  const id=Object.values(s.premiumRegions)[0].center;s.tiles[id].premiumFactor=50;
  assert.throws(()=>parseSave(JSON.stringify(s)),/premium/);
  const orphan=createGame(88,'great-divide');delete orphan.premiumRegions;
  assert.throws(()=>parseSave(JSON.stringify(orphan)),/premium/);
});

test('a naval blockade suspends the entire premium harbor yield at higher tiers',()=>{
  const s=createGame(8147,'heartlands'),r=Object.values(s.premiumRegions).find(r=>r.kind==='natural-harbor'),t=s.tiles[r.center];
  s.phase='playing';s.turn=1;
  Object.assign(t,{owner:'ashen',building:'town',harbor:true,levels:{town:1,harbor:3}});
  const before=productionPlan(s,'ashen').income.gold;
  assert.ok(before>BUILDINGS.harbor.yield.gold+5);
  s.wars=['ashen:wintermere'];
  s.fleets.push({owner:'wintermere',node:shoreNodes(s,t.id)[0],order:'blockade',path:[],ships:[{type:'warship'}]});
  const blocked=productionPlan(s,'ashen');
  assert.equal(blocked.income.gold,5,'town tax income remains, all boosted maritime income stops');
  assert.equal(blocked.stalls[0].type,'blockade');
});
