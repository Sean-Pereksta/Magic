const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {gunzipSync}=require('node:zlib');
const root=path.resolve(__dirname,'../../..');
const dir=path.resolve(__dirname,'..');
function load(){
  const context=vm.createContext({console});context.window=context;
  for(const name of ['catalog','progression','engine'])vm.runInContext(fs.readFileSync(path.join(dir,name+'.js'),'utf8'),context);
  return {C:context.Catalog,E:context.GameEngine,P:context.Progression};
}
const copy=value=>JSON.parse(JSON.stringify(value));
// Frozen, validated profiles generated with the published 20-region engine
// (a28fea3). Compressed fixture data keeps CI independent of Git history.
const LEGACY=JSON.parse(gunzipSync(Buffer.from('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','base64')));
function legacySave(round,fight=false){return copy(LEGACY[round+'-'+fight]);}
function encounter(element='leaf',count=1,extra={}){
  const game=load(),{C,E}=game,p=E.freshProfile();E.start(p);E.tick(p,3.3);
  C.items.__probe={id:'__probe',name:'Combat probe',element,style:'strike',power:10,cooldown:1,rarity:'common',...extra};
  const r=p.run,template=copy(r.enemies[0]);
  r.active=[{uid:1,id:'__probe',level:1,cd:0,slot:0}];r.pack=[];r.hp=50;r.shield=0;r.xp=0;r.phase='fight';r.waveSize=count;r.targetCursor=0;r.enemies=[];
  for(let i=0;i<count;i++)r.enemies.push({...copy(template),uid:i+1,lane:i,hp:10000,maxHp:10000,damage:0,period:9999,trait:'plain',weak:i===0?element:'none',resist:'none',ward:0,attackProgress:0,echoes:[],...{burn:0,poison:0,chill:0,stun:0}});
  r.enemy=r.enemies[0];
  const activate=()=>{r.active[0].cd=0;return E.tick(p,.01);};
  return {...game,p,r,e:r.enemies[0],activate};
}
function three(f){return [0,1,2].flatMap(()=>f.activate());}

test('expansion provides 15 complete regions, 72 new relics, specials, and practical early counters',()=>{
  const {C,E}=load();assert.equal(C.regions.length,35);assert.equal(Object.keys(C.items).length,284);
  for(let region=20;region<35;region++){
    assert.ok(C.enemies.filter(e=>e.region===region&&!e.boss).length>=5,C.regions[region].name+' needs a full group roster');
    assert.equal(C.enemies.filter(e=>e.region===region&&e.boss).length,1);assert.ok(Object.values(C.items).filter(d=>d.dropRegion===region).length>=2);
  }
  for(const special of ['ricochet','execute','rupture','chain','leech','wardbreak','bloom','permafrost','aftershock','soulmark','renewal','overload'])assert.ok(Object.values(C.items).some(d=>d.special===special&&d.specialDescription),special);
  for(const counter of ['cleanse','antivenom','insulated','thaw','unbinding','purify','blastguard','wardbreaker'])assert.ok(Object.values(C.items).some(d=>d.counter===counter&&d.counterDescription&&d.unlockRound<=16),counter+' should be available before the new lands');
  for(const affliction of ['corrosive','entangling','hexed','scorching','freezing','shocking','bulwark'])assert.ok(C.enemies.some(e=>e.affliction===affliction),affliction);
  const fresh=E.freshProfile();assert.equal(fresh.worldRegions,35);assert.equal(fresh.routeMarks.length,140);E.start(fresh);assert.equal(fresh.run.routeRegions,35);
  const visited=new Set();
  for(let round=1;round<=420;round++){
    const info=E.routeInfo(fresh.run,round);visited.add(info.region);
    assert.equal(info.region,Math.floor(((round-1)%140)/4));assert.equal(info.circuit,1+Math.floor((round-1)/140));assert.equal(info.waveInRegion,1+(round-1)%4);
  }
  assert.equal(visited.size,35,'all 35 regions must be reachable in each circuit');
});

test('existing 20-region saves keep their wave, kit, discoveries and replay records',()=>{
  const {E}=load();
  for(const round of [1,37,77,85,162])for(const fighting of [false,true]){
    const old=legacySave(round,fighting),p=E.validate(old);
    assert.equal(p.run.round,round);assert.equal(p.run.region,old.run.region);assert.equal(p.run.phase,old.run.phase);assert.equal(p.run.hp,old.run.hp);
    assert.equal(p.run.routeRegions,20);assert.equal(p.routeMarks.length,140);assert.deepEqual(Array.from(p.routeMarks.slice(0,80)),old.routeMarks);assert.ok(p.routeMarks.slice(80).every(mark=>mark===0));
    assert.deepEqual(copy(p.run.active),old.run.active);assert.deepEqual(copy(p.seenItems),old.seenItems);assert.equal(p.best,old.best);
    assert.equal(E.validate(copy(p)).run.region,old.run.region,'migration must be stable on subsequent reloads');
  }
});

test('an existing circuit finishes once and the next circuit includes all 35 regions',()=>{
  const {E}=load(),old=legacySave(80,true),p=E.validate(old),r=p.run;
  for(const e of r.enemies)e.hp=0;
  r.draft=[];E.tick(p,.001);
  assert.equal(r.round,81);assert.equal(r.region,0);assert.equal(r.routeRegions,35);assert.equal(E.routeInfo(r,161).region,20);assert.equal(E.routeInfo(r,220).region,34);assert.equal(E.routeInfo(r,221).region,0);
  assert.equal(E.routeInfo(r,80).region,19,'old waves retain their original region for historical loot validation');
  assert.equal(E.validate(copy(p)).run.round,81);
});

test('earliest eight-region profiles remain importable without resetting their active road',()=>{
  const {E}=load(),old=legacySave(37);
  delete old.routeMarks;delete old.run.routeEpoch;old.run.region=1;
  const p=E.validate(old);
  assert.equal(p.run.round,37);assert.equal(p.run.region,1);assert.equal(p.run.routeRegions,8);assert.equal(p.routeMarks.length,140);
  assert.equal(E.validate(copy(p)).run.region,1);
});

test('every new region spawns valid encounters and its active effects survive a save roundtrip',()=>{
  const {C,E}=load();
  for(let region=20;region<35;region++){
    const p=E.freshProfile();E.start(p);const r=p.run;
    r.round=region*4+1;r.region=region;r.kills=r.round-1;r.foes=r.kills;p.best=r.kills;r.walkLeft=.001;
    E.tick(p,.002);assert.equal(r.phase,'fight');assert.equal(r.enemies.length,5);
    Object.assign(r.ailments,{corrosion:2,snare:2,hex:2,burn:12,chill:2,shock:2});
    const e=r.enemies[0];Object.assign(e,{rooted:.8,fracture:5,haunt:2,hauntTime:4,leafHits:2,frostHits:2,stormHits:2});
    e.echoes.push({delay:.6,damage:7,el:'stone',pattern:'aftershock'},{delay:.4,damage:3,el:'spirit',pattern:'soulmark'});
    const restored=E.validate(copy(p));assert.deepEqual(copy(restored.run.ailments),copy(r.ailments));
    for(const field of ['rooted','fracture','haunt','hauntTime','leafHits','frostHits','stormHits'])assert.equal(restored.run.enemies[0][field],e[field]);
    assert.deepEqual(copy(restored.run.enemies[0].echoes),copy(e.echoes));assert.equal(restored.run.enemies[0].affliction,C.enemies.find(d=>d.id===e.id).affliction);
    const bad=copy(p);bad.run.enemies[0].rooted=100;assert.throws(()=>E.validate(bad));
    const badAilment=copy(p);badAilment.run.ailments.snare=Infinity;assert.throws(()=>E.validate(badAilment));
  }
});

test('Leaf poisons and roots, Fire burns, Frost freezes, Storm chains, Stone fractures, and Spirit haunts',()=>{
  let f=encounter('leaf'),ev=three(f);assert.ok(f.e.poison>0);assert.ok(f.e.rooted>0);assert.ok(ev.some(e=>e.pattern==='root'));
  f=encounter('fire');f.activate();assert.ok(f.e.burn>0,'a normal fire strike must burn without requiring burn style');
  f=encounter('frost');ev=three(f);assert.ok(f.e.chill>0);assert.ok(f.e.stun>0);assert.ok(ev.some(e=>e.pattern==='freeze'));
  f=encounter('storm',2);ev=three(f);assert.ok(f.r.enemies[1].hp<10000);assert.ok(ev.some(e=>e.pattern==='chain'));
  f=encounter('stone');f.e.attackProgress=.75;f.activate();assert.ok(f.e.fracture>0);assert.ok(f.e.attackProgress<.75);
  f=encounter('spirit');ev=three(f);assert.ok(f.r.shield>0);assert.ok(f.e.echoes.length>0||ev.some(e=>/haunt|soul|echo/.test(e.pattern)));
});

test('element combinations produce extra group effects rather than only recolored hits',()=>{
  const f=encounter('fire',2);f.e.poison=6;const before=f.r.enemies[1].hp,ev=f.activate();
  assert.ok(ev.some(e=>e.pattern==='wildfire'));assert.ok(f.e.poison<6);assert.ok(f.r.enemies[1].burn>0||f.r.enemies[1].hp<before);
});

test('special weapons have mechanically different outcomes and identify themselves in events',()=>{
  const impacts={};
  for(const special of ['ricochet','execute','rupture','chain','leech','wardbreak','bloom','permafrost','aftershock','soulmark','renewal','overload']){
    const f=encounter('stone',3,{special});f.e.hp=special==='execute'?2000:10000;f.e.poison=4;f.e.chill=1;f.e.ward=special==='wardbreak'?1:0;
    const ev=three(f);impacts[special]={hp:f.e.hp,others:f.r.enemies.slice(1).map(e=>e.hp),hero:f.r.hp,shield:f.r.shield,echoes:f.e.echoes.length};
    assert.ok(ev.some(e=>e.special===special||e.specialId===special||e.pattern===special),special+' must provide a distinguishable proc event');
  }
  assert.ok(new Set(Object.values(impacts).map(x=>JSON.stringify(x))).size>=7,'specials should create distinct combat outcomes');
});

test('overload charges every third activation for Fire, Stone and Storm weapons independently of their target',()=>{
  for(const element of ['fire','stone','storm']){
    const f=encounter(element,3,{special:'overload'});
    for(let hit=1;hit<=6;hit++){
      // Changing the target's intrinsic Storm counter must not charge the relic.
      f.e.stormHits=1;
      const events=f.activate(),bursts=events.filter(e=>e.kind==='special'&&e.pattern==='overload');
      assert.equal(bursts.length>0,hit%3===0,element+' overload activation '+hit);
    }
  }
});

test('cleanse gear removes existing ailments, and passive counters prevent enemy control effects',()=>{
  const f=encounter('leaf',1,{counter:'cleanse'});Object.assign(f.r.ailments,{corrosion:2,snare:2,hex:2,burn:2,chill:2,shock:2});f.r.venom=3;f.activate();
  assert.ok(Object.values(f.r.ailments).every(n=>n===0));assert.equal(f.r.venom,0);
  for(const [affliction,counter,key] of [['entangling','unbinding','snare'],['hexed','purify','hex'],['freezing','thaw','chill'],['shocking','insulated','shock'],['corrosive','purify','corrosion']]){
    const control=encounter('leaf'),protectedRun=encounter('leaf',1,{counter});
    for(const fixture of [control,protectedRun]){fixture.r.active[0].cd=100;fixture.e.affliction=affliction;fixture.e.damage=4;fixture.e.attackProgress=1;fixture.E.tick(fixture.p,.01);}
    assert.ok(control.r.ailments[key]>0,affliction+' needs an actual mechanical effect');assert.equal(protectedRun.r.ailments[key],0,counter+' should prevent '+affliction);
  }
});

test('enemy ailments change combat timing, damage and protection, and bulwarks rebuild their ward',()=>{
  for(const ailment of ['snare','chill']){
    const normal=encounter('leaf'),afflicted=encounter('leaf');
    for(const f of [normal,afflicted])f.r.active[0].cd=1;
    afflicted.r.ailments[ailment]=2;
    normal.E.tick(normal.p,.5);afflicted.E.tick(afflicted.p,.5);
    assert.ok(afflicted.r.active[0].cd>normal.r.active[0].cd,ailment+' must slow relic recovery');
  }
  let normal=encounter('leaf'),afflicted=encounter('leaf');afflicted.r.ailments.hex=2;normal.activate();afflicted.activate();assert.ok(afflicted.e.hp>normal.e.hp,'hex must reduce outgoing damage');
  normal=encounter('leaf');afflicted=encounter('leaf');
  for(const f of [normal,afflicted]){f.r.active[0].cd=100;f.r.shield=10;}afflicted.r.ailments.corrosion=2;
  normal.E.tick(normal.p,.5);afflicted.E.tick(afflicted.p,.5);assert.ok(afflicted.r.shield<normal.r.shield,'corrosion must erode shielding');
  normal=encounter('leaf');afflicted=encounter('leaf');
  for(const f of [normal,afflicted]){f.r.active[0].cd=100;f.P.apply(f.r,'whiteout');f.r.abilityTimers.whiteout=1;}afflicted.r.ailments.shock=2;
  normal.E.tick(normal.p,.5);afflicted.E.tick(afflicted.p,.5);assert.ok(afflicted.r.abilityTimers.whiteout>normal.r.abilityTimers.whiteout,'shock must slow spell recovery');
  afflicted=encounter('leaf');afflicted.r.active[0].cd=100;afflicted.r.ailments.burn=8;afflicted.E.tick(afflicted.p,1.05);assert.ok(afflicted.r.hp<50,'scorch must deal lingering damage');
  const bulwark=encounter('leaf');bulwark.r.active[0].cd=100;bulwark.e.affliction='bulwark';bulwark.e.swings=1;bulwark.e.attackProgress=1;bulwark.E.tick(bulwark.p,.01);assert.equal(bulwark.e.ward,1);
});

test('antivenom and blast guard reduce poison and volatile damage while ward breakers penetrate armor',()=>{
  let baseline=encounter('leaf'),counter=encounter('leaf',1,{counter:'antivenom'});
  for(const f of [baseline,counter]){f.r.active[0].cd=100;f.r.venom=8;f.E.tick(f.p,1.05);}
  assert.ok(counter.r.hp>baseline.r.hp);
  baseline=encounter('leaf');counter=encounter('leaf',1,{counter:'blastguard'});
  for(const f of [baseline,counter]){f.e.hp=0;f.e.trait='volatile';f.e.damage=20;f.r.active[0].cd=100;f.E.tick(f.p,.01);}
  assert.ok(counter.r.hp>baseline.r.hp,'death-burst protection must reduce actual HP loss');
  baseline=encounter('leaf');counter=encounter('leaf',1,{counter:'wardbreaker'});
  for(const f of [baseline,counter]){f.e.trait='armored';f.e.ward=1;f.activate();}
  assert.ok(counter.e.hp<baseline.e.hp);assert.equal(counter.e.ward,0);
});
