const test=require('node:test'),assert=require('node:assert/strict');
const P=require('./progression.js'),D=require('./development.js'),UI=require('./development-ui.js');
const player=(id,overrides={})=>P.migratePlayer({id,name:'Athlete '+id,speed:65,cutting:65,turning:65,evasion:65,catching:65,strength:65,price:250,archetype:'Possession Receiver',...overrides});
const legacy=()=>({cash:100000,round:1,wins:0,team:[0,1,2,3].map(i=>player('t'+i)),market:[0,1,2,3,4].map(i=>player('m'+i)),checkpoint:{seriesOffense:0,seriesDefense:0,ballSpotYards:0,down:1,lineToGainYards:25}});
const franchise=()=>D.normalize(legacy());
const json=x=>JSON.parse(JSON.stringify(x));

test('legacy four starters and five listed prospects migrate without rerolls or economic resets',()=>{
  const f=legacy();f.team[0].prestige=7;f.team[0].strength=177;f.market[0].price=140;const before=json(f);
  D.normalize(f);assert.equal(f.team.length,4);assert.equal(f.market.length,48);assert.equal(f.development.pool.length,256);
  assert.equal(f.cash,before.cash);assert.deepEqual(f.checkpoint,before.checkpoint);assert.equal(f.team[0].prestige,7);assert.equal(f.team[0].strength,177);assert.equal(f.market[0].price,140);
  assert.deepEqual(f.market.slice(0,5).map(p=>p.id),before.market.map(p=>p.id));assert.deepEqual(f.team.map(p=>p.id),before.team.map(p=>p.id));
  const snapshot=JSON.stringify(f);D.normalize(f);assert.equal(JSON.stringify(f),snapshot,'Migration must be idempotent');
});
test('the persistent generated pool has distinct IDs, all positions and stable save reloads',()=>{
  const f=franchise(),players=[...f.team,...f.market,...f.development.pool];assert.equal(new Set(players.map(p=>p.id)).size,players.length);assert.deepEqual(new Set(f.development.pool.map(p=>p.position)),new Set(Object.keys(D.positions)));
  const restored=P.validateSave(JSON.stringify(f));assert.deepEqual(restored.development,f.development);assert.deepEqual(restored.team,f.team);assert.deepEqual(restored.market,f.market);assert.deepEqual(P.validateSave(restored),restored);assert.ok(JSON.stringify(f).length<700000,'Normal development saves stay below the cloud envelope cap before compression');
  assert.ok(f.market.some(p=>p.age<23));assert.ok(f.market.some(p=>p.age>25));assert.ok(new Set(f.market.map(p=>p.modelId)).size>=10);
});
test('market queries combine physical, rating, ability and price filters without mutating prospects',()=>{
  const f=franchise(),p=f.market[0];p.position='WR';p.name='Target Specialist';p.abilities=['Sure Hands'];p.strength=92;p.speed=88;p.catching=95;p.athleticism=90;p.age=21;p.price=3400;p.physical.heightM=1.98;
  const before=JSON.stringify(f.market),rows=D.queryMarket(f,{search:'specialist',position:'WR',ability:'Sure Hands',minHeight:1.95,maxHeight:2,minStrength:90,minSpeed:85,minJumping:88,minCatching:94,maxPrice:3500,maxAge:22});
  assert.deepEqual(rows.map(x=>x.id),[p.id]);assert.equal(JSON.stringify(f.market),before);
  const sorted=D.queryMarket(f,{sort:'price',direction:'asc'});assert.ok(sorted.every((p,i)=>!i||p.price>=sorted[i-1].price));assert.equal(D.queryMarket(f,{maxPrice:1}).length,0);
});
test('signing charges the displayed price, retains the replaced athlete and replenishes only that listing',()=>{
  const f=franchise(),p=f.market[0],old=f.team[2],cash=f.cash,remaining=f.market.slice(1).map(p=>p.id);
  const result=D.sign(f,p.id,{starterSlot:2,betweenDrives:true});assert.equal(result.ok,true);assert.equal(f.cash,cash-p.price);assert.equal(f.team[2].id,p.id);assert.equal(f.bench[0].id,old.id);assert.equal(f.market.length,48);assert.equal(f.development.pool.length,256);
  assert.deepEqual(f.market.slice(0,47).map(p=>p.id),remaining);assert.equal(f.market.some(x=>x.id===p.id),false);assert.equal(P.validateSave(f).bench[0].id,old.id);
});
test('failed signings and live substitutions leave cash and personnel unchanged',()=>{
  const f=franchise(),before=json(f);assert.equal(D.sign(f,f.market[0].id,{starterSlot:0}).ok,false);assert.deepEqual(f,before);
  const qb=f.market.find(p=>p.position==='QB');assert.equal(D.sign(f,qb.id,{starterSlot:0,betweenDrives:true}).ok,false);assert.deepEqual(f,before);
  f.cash=0;const ids=f.market.map(p=>p.id);assert.equal(D.sign(f,f.market[0].id).ok,false);assert.equal(f.cash,0);assert.deepEqual(f.market.map(p=>p.id),ids);
  assert.equal(D.substitute(f,'missing',1).ok,false);
});
test('bench capacity prevents charging for a recruit with nowhere to go',()=>{
  const f=franchise();f.bench=Array.from({length:32},(_,i)=>player('bench'+i));D.normalize(f);const cash=f.cash,id=f.market[0].id;assert.equal(D.sign(f,id).ok,false);assert.equal(f.cash,cash);assert.ok(f.market.some(p=>p.id===id));
});
test('substitutions and named formation lineups preserve four slots, depth, IDs and chemistry',()=>{
  const f=franchise();D.sign(f,f.market[0].id);const bench=f.bench[0],original=f.team.map(p=>p.id);bench.chemistry={xp:543,concepts:{Mesh:12}};
  assert.equal(D.saveLineup(f,'Quick Game').ok,true);assert.equal(D.substitute(f,bench.id,1,{betweenDrives:true}).ok,true);assert.equal(f.team[1].chemistry.xp,543);
  D.saveLineup(f,'Screens');assert.equal(D.activateLineup(f,'Quick Game').ok,false);assert.equal(D.activateLineup(f,'Quick Game',{betweenDrives:true}).ok,true);assert.deepEqual(f.team.map(p=>p.id),original);assert.equal(f.bench.find(p=>p.id===bench.id).chemistry.xp,543);
  assert.equal(D.activateLineup(f,'Screens',{betweenDrives:true}).ok,true);assert.equal(f.team[1].id,bench.id);assert.equal(new Set(D.roster(f).map(p=>p.id)).size,5);
});
test('saved bench chemistry is safe when a substitute enters the box-score roster',()=>{
  const F=require('./franchise.js'),f=F.normalize(franchise());D.sign(f,f.market[0].id);const id=f.bench[0].id;
  f.bench[0].chemistry={xp:'bad',concepts:null};const loaded=P.validateSave(f);
  assert.deepEqual(loaded.bench[0].chemistry,{xp:0,concepts:{}});assert.equal(D.substitute(loaded,id,0,{betweenDrives:true}).ok,true);
  const play=F.newPlay('Mesh');play.attempt=true;play.target=id;play.receiver=id;assert.equal(F.commitPlay(loaded,play,{spot:12}),true);
  assert.equal(loaded.team[0].chemistry.concepts.Mesh,1);assert.equal(loaded.team[0].chemistry.xp,7);
  loaded.bench[0].chemistry={xp:15000,concepts:{Mesh:2000,Stick:-8}};D.normalize(loaded);
  assert.deepEqual(loaded.bench[0].chemistry,{xp:10000,concepts:{Mesh:1000,Stick:0}});
});
test('managed quarterback and defensive units require owned position fits and release cleans references',()=>{
  const f=franchise(),qb=f.market.find(p=>p.position==='QB'),cb=f.market.find(p=>p.position==='CB');D.sign(f,qb.id);D.sign(f,cb.id);
  assert.equal(D.assignUnit(f,'QB',cb.id,{betweenDrives:true}).ok,false);assert.equal(D.assignUnit(f,'QB',qb.id,{betweenDrives:true}).ok,true);assert.equal(D.unitPlayer(f,'QB').id,qb.id);
  D.assignUnit(f,'CB1',cb.id,{betweenDrives:true});D.assignUnit(f,'CB2',cb.id,{betweenDrives:true});assert.equal(f.development.units.CB1,undefined);assert.equal(f.development.units.CB2,cb.id);
  assert.equal(D.release(f,cb.id,{betweenDrives:true}).ok,true);assert.equal(f.development.units.CB2,undefined);assert.equal(D.release(f,f.team[0].id,{betweenDrives:true}).ok,false);
});
test('specialized development respects per-program and team cycle limits without resetting prestige',()=>{
  const f=franchise(),p=f.team[0];for(const key of P.stats)p.development.ceilings[key]=200;p.prestige=4;p.trainingByStat.strength=2;const oldLegacy=json(p.trainingByStat);
  for(let i=0;i<3;i++)assert.equal(D.train(f,p.id,'strength').ok,true);const cash=f.cash;
  assert.equal(D.train(f,p.id,'strength').ok,false);assert.equal(f.cash,cash);assert.deepEqual(p.trainingByStat,oldLegacy);assert.equal(p.prestige,4);
  D.normalize(f);assert.equal(D.train(f,p.id,'strength').ok,false,'Reload/loss cannot renew sessions');f.round++;D.normalize(f);assert.equal(D.train(f,p.id,'strength').ok,true);
  let successes=1;for(const person of f.team)for(const program of ['strength','routes','vertical','contested','balance'])for(let i=0;i<3;i++)if(D.train(f,person.id,program).ok)successes++;
  assert.equal(successes,16);const stopped=f.cash;assert.equal(D.train(f,f.team[2].id,'routes').ok,false);assert.equal(f.cash,stopped);
});
test('legacy uncapped rating training remains independent and refreshes visible strength measurements',()=>{
  const f=franchise(),p=f.team[0],before=D.physicalProfile(p),height=before.heightM;p.strength=130;D.refreshPhysical(p);const current=D.physicalProfile(p);
  assert.ok(current.muscularity>before.muscularity);assert.ok(current.shoulderWidthM>before.shoulderWidthM);assert.ok(current.weightKg>before.weightKg);
  const result=P.train(p,'strength',10000,8);assert.equal(result.ok,true);assert.equal(p.strength,138);assert.equal(p.physical.heightM,height);
  p.trainingByStat.size=0;P.train(p,'size',10000,8);assert.equal(p.physical.heightM,height,'Adult height cannot grow through legacy SIZE sessions');
  p.trainingByStat.strength=5;const saved=p.strength;assert.equal(P.prestige(p,1e9).ok,true);assert.equal(p.strength,saved);assert.equal(p.prestige,1);
});
test('realistic height stays fixed across training and seasons; optional arcade growth is bounded',()=>{
  const f=franchise(),p=f.team[0],adult=p.physical.adultHeightM;for(const key of P.stats)p.development.ceilings[key]=500;
  D.train(f,p.id,'vertical');assert.equal(p.physical.heightM,adult);D.beginSeason(f,2);assert.equal(p.age,24);assert.equal(p.physical.heightM,adult);assert.equal(D.beginSeason(f,2).changed,false);assert.equal(p.age,24);assert.equal(D.beginSeason(f,4).ok,false);
  D.setHeightMode(f,'arcade');for(let round=0;round<6;round++){f.round++;D.normalize(f);for(let i=0;i<3;i++)D.train(f,p.id,'vertical');}
  assert.ok(p.physical.heightM>adult);assert.ok(p.physical.heightM<=adult+.031);assert.ok(p.physical.heightGrowthM<=.03);D.setHeightMode(f,'realistic');assert.equal(p.physical.heightM,adult);assert.equal(p.physical.adultHeightM,adult);
});
test('academy rewards pay only medal improvements, retain personal bests and cannot be farmed by reload',()=>{
  const f=franchise(),id=f.team[0].id,cash=f.cash;const result=score=>D.awardAcademy(f,{challengeId:'precision',score,playerId:id});
  assert.equal(result(65).cash,30);assert.equal(result(85).cash,40);assert.equal(result(97).cash,50);assert.equal(f.cash-cash,120);assert.equal(f.team[0].development.academyPoints,3);
  const loaded=P.validateSave(f),duplicate=D.awardAcademy(loaded,{challengeId:'precision',score:100,playerId:id});assert.equal(duplicate.cash,0);assert.equal(loaded.cash,f.cash);assert.equal(duplicate.personalBest,100);
  assert.equal(D.awardAcademy(loaded,{challengeId:'arbitrary-new-id',score:100}).ok,false);assert.equal(D.awardAcademy(loaded,{challengeId:'routes',score:NaN}).ok,false);
  loaded.round++;D.normalize(loaded);const next=D.awardAcademy(loaded,{challengeId:'precision',score:62,playerId:id});assert.equal(next.cash,30);assert.equal(next.personalBest,100);
});
test('academy credits discount a finite training session and are consumed once',()=>{
  const f=franchise(),p=f.team[0];D.awardAcademy(f,{challengeId:'routes',score:96,playerId:p.id});const before=p.development.academyPoints,status=D.trainingStatus(f,p,'routes');assert.equal(status.cost,90);const trained=D.train(f,p.id,'routes');assert.equal(trained.cost,90);assert.equal(p.development.academyPoints,before-1);
});
test('damaged expanded saves reject bad ratings, impossible arrays and invalid measurements before replacement',()=>{
  const f=franchise();let broken=json(f);broken.bench=Array.from({length:33},(_,i)=>player('bad'+i));assert.throws(()=>P.validateSave(broken));broken=json(f);broken.development.pool[0].speed=null;assert.throws(()=>P.validateSave(broken));broken=json(f);broken.team[0].physical.heightM=null;assert.throws(()=>P.validateSave(broken));broken=json(f);broken.market.pop();assert.throws(()=>P.validateSave(broken));
  assert.equal(f.market.length,48);assert.equal(f.team[0].speed,65);
});
test('the player builder enforces position measurements and charges exactly once for an owned bench athlete',()=>{
  const f=franchise(),preview=D.builderPreview({name:'Custom Power',position:'TE',heightM:9,weightKg:900,build:'power'});assert.equal(preview.physical.heightM,D.positions.TE.height[1]);assert.equal(preview.physical.weightKg,D.positions.TE.weight[1]);
  const cash=f.cash,result=D.buildPlayer(f,{name:'Custom Power',position:'TE',heightM:2.05,weightKg:120,build:'power'});assert.equal(result.ok,true);assert.equal(f.cash,cash-result.player.price);assert.equal(f.bench[0].position,'TE');assert.equal(f.bench[0].physical.adultHeightM,2.05);
  const loaded=P.validateSave(f);assert.equal(loaded.bench[0].id,result.player.id);assert.equal(loaded.bench[0].strength,80);
});
test('comparison output uses actual relative heights and escapes player text',()=>{
  const f=franchise(),a=f.team[0],b=f.team[1];a.physical.heightM=1.7;b.physical.heightM=2.05;a.name='<script>alert(1)</script>';const html=UI.comparisonSVG([a,b]);assert.ok(html.includes('170 cm'));assert.ok(html.includes('205 cm'));assert.equal(html.includes('<script>'),false);assert.ok(html.includes('same height scale'));
});
