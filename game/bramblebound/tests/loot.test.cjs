const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');
const tiers=['common','uncommon','rare','epic','mythic'];
function load(){
  const context=vm.createContext({console});context.window=context;
  for(const name of ['catalog','progression','engine'])vm.runInContext(fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8'),context);
  return {C:context.Catalog,E:context.GameEngine};
}
const tally=()=>Object.fromEntries(tiers.map(t=>[t,0]));
function near(actual,expected,tolerance,label){assert.ok(Math.abs(actual-expected)<=tolerance,label+': observed '+actual.toFixed(4)+', expected '+expected.toFixed(4));}

// Complete encounters through the actual kill/drop/collection path. Combat is
// already over in this fixture, so loot distribution is measured independently
// of weapon damage, death timing, and talent selection.
function clearedWaves(game,{samples=5000,guardian=false,count=5,carryMythic=false}={}){
  const {C,E}=game,p=E.freshProfile(),round=guardian?140:137,region=34;
  const mythic=Object.values(C.items).find(d=>d.rarity==='mythic'&&!d.recipe);
  const result={waves:samples,withDrop:0,total:0,counts:tally(),normal:tally(),guardian:tally(),normalTotal:0,guardianTotal:0};
  const defs=new Map(C.enemies.map(e=>[e.id,e]));
  for(let wave=0;wave<samples;wave++){
    p.run=null;E.start(p);const r=p.run;
    r.seed=(Math.imul(wave+1,0x9e3779b9)>>>0)||1;r.round=round;r.region=region;r.kills=round-1;r.foes=r.kills;p.best=r.kills;
    r.walkLeft=0;E.tick(p,.001);
    r.enemies=r.enemies.slice(0,count);r.waveSize=count;r.enemy=r.enemies[0];
    r.active=[];r.pack=carryMythic?[{uid:1,id:mythic.id,level:1,cd:0,slot:0}]:[];r.nextUid=2;
    for(const e of r.enemies)e.hp=0;
    const loot=E.tick(p,.001).filter(e=>e.kind==='loot');
    if(loot.length)result.withDrop++;
    for(const event of loot){
      const def=C.items[event.id],boss=defs.get(event.enemyId).boss;
      assert.ok(tiers.includes(def.rarity),'unknown dropped tier '+def.rarity);
      assert.equal(E.dropEligible(def,boss,round),true,'locked relic dropped: '+def.id);
      // A later escort drop may copy the level of a guardian's earlier find.
      if(!carryMythic){const level=1+Math.floor((round-1)/5);assert.ok(event.level===level||(guardian&&event.level===level+1),'rarity must not silently change item levels');}
      result.counts[def.rarity]++;result[boss?'guardian':'normal'][def.rarity]++;result[boss?'guardianTotal':'normalTotal']++;result.total++;
    }
  }
  return result;
}

test('five rarity tiers are represented in real gear and grant progressively stronger damage',()=>{
  const {C,E}=load();assert.deepEqual(Object.keys(C.rarities).sort(),[...tiers].sort());
  const scales={common:1,uncommon:1.12,rare:1.28,epic:1.5,mythic:1.8};
  const p=E.freshProfile();C.items.__rarityProbe={id:'__rarityProbe',name:'Rarity comparison',power:10,cooldown:1,element:'stone',style:'strike',rarity:'common'};
  for(const tier of tiers){
    assert.ok(Object.values(C.items).some(d=>d.id!=='__rarityProbe'&&d.rarity===tier),'no actual '+tier+' equipment');
    assert.equal(C.rarities[tier].powerScale,scales[tier]);
  }
  for(const level of [1,3,10])for(const rank of [0,9]){
    p.perks.claw=rank;C.items.__rarityProbe.rarity='common';const common=E.itemPower(p,{id:'__rarityProbe',level});
    let previous=0;
    for(const tier of tiers){C.items.__rarityProbe.rarity=tier;const power=E.itemPower(p,{id:'__rarityProbe',level});near(power/common,scales[tier],1e-10,tier+' damage scale');assert.ok(power>previous);previous=power;}
  }
});

test('tier odds sum to one, respect unlocks, and make stronger tiers rarer',()=>{
  const {C,E}=load();
  for(const [round,region] of [[1,0],[33,8],[81,20],[137,34]])for(const guardian of [false,true]){
    const r={round,region},odds=E.lootOdds(r,guardian);assert.deepEqual(Object.keys(odds).sort(),[...tiers].sort());
    near(Object.values(odds).reduce((a,b)=>a+b,0),1,1e-12,'total tier probability');
    for(const tier of tiers){const eligible=Object.values(C.items).filter(d=>d.rarity===tier&&E.dropEligible(d,guardian,round));assert.ok(odds[tier]>=0&&odds[tier]<=1);if(!eligible.length)assert.equal(odds[tier],0,tier+' must be unavailable before its first unlock');}
  }
  for(const guardian of [false,true]){
    const odds=E.lootOdds({round:140,region:34},guardian);
    for(let i=1;i<tiers.length;i++)assert.ok(odds[tiers[i]]<odds[tiers[i-1]],'stronger '+tiers[i]+' gear must be rarer than '+tiers[i-1]);
  }
  const early=E.lootOdds({round:137,region:0}),late=E.lootOdds({round:137,region:34});
  assert.ok(late.epic+late.mythic>early.epic+early.mythic,'later regions should improve high-tier finds');
});

test('ten thousand complete waves confirm increased drops and the real tier distribution',()=>{
  const game=load(),{E}=game;
  assert.equal(E.dropChances.normal,.24);assert.equal(E.dropChances.guardian,.48);
  for(const guardian of [false,true]){
    const result=clearedWaves(game,{samples:5000,guardian});near(result.withDrop/result.waves,guardian?.48:.24,.025,(guardian?'guardian':'normal')+' wave with loot');
    console.log((guardian?'Guardian':'Normal')+' waves with loot: '+result.withDrop+'/'+result.waves+' ('+(100*result.withDrop/result.waves).toFixed(2)+'%); tier totals '+JSON.stringify(result.counts));
    for(const tier of tiers)assert.ok(result.counts[tier]>0,'simulation never dropped '+tier);
    for(const boss of [false,true]){
      const count=result[boss?'guardianTotal':'normalTotal'];if(!count)continue;
      const observed=result[boss?'guardian':'normal'],odds=E.lootOdds({round:guardian?140:137,region:34},boss);
      for(const tier of tiers)near(observed[tier]/count,odds[tier],.035,(boss?'guardian':'ordinary')+' foe '+tier+' rarity');
    }
  }
});

test('the chance of finding something does not collapse or multiply with encounter headcount',()=>{
  const game=load();
  for(const guardian of [false,true])for(const count of [1,3]){
    const result=clearedWaves(game,{samples:2000,guardian,count});near(result.withDrop/result.waves,guardian?.48:.24,.04,(guardian?'guardian':'normal')+' '+count+'-foe encounter');
  }
});

test('carrying a mythic relic does not make more mythic tiers roll',()=>{
  const game=load(),r={round:137,region:34,active:[],pack:[]};
  const before=JSON.parse(JSON.stringify(game.E.lootOdds(r)));
  const item=Object.values(game.C.items).find(d=>d.rarity==='mythic');r.pack.push({id:item.id,level:1,uid:1});
  assert.deepEqual(JSON.parse(JSON.stringify(game.E.lootOdds(r))),before);
  const baseline=clearedWaves(game,{samples:5000}),carried=clearedWaves(game,{samples:5000,carryMythic:true});
  for(const tier of tiers)near(carried.counts[tier]/carried.total,baseline.counts[tier]/baseline.total,.035,'carried mythic effect on '+tier+' odds');
});
