'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){const c=loadEngine(),g=new c.ATSGame('TACTICAL-APES');g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.world.lineClear=()=>true;g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;return{c,g}}
function object(g,fields){const o={id:'tactic-prop-'+g.nextId++,x:0,y:0,hp:500,maxHp:500,solid:true,...fields};g.world.objects.set(o.id,o);g.world._indexObject(o);return o}
function wall(g,fields={}){return object(g,{type:'wall',faction:'human',collision:'rect',w:18,h:140,wallTier:1,climbable:true,visualHeight:42,...fields})}
function ape(g,fields={}){const a=g.makeApe(-24,0,'charge');Object.assign(a,{species:'gibbon',attackCD:0,trainingLevel:0,chargeTime:13,target:{x:80,y:0},...fields});return a}
function grids(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);g.navigation.beginFrame(g.time);g.performance.beginStep(g)}
function climb(g,a,w){assert.equal(g.tactics.tryClimb(a,100,0,w),true);for(let i=0;i<10;i++){g.time+=.1;g.updateApe(a,.1)}assert.equal(a.onWallId,w.id);return a.wallClimb}

test('stable personalities use no random draws and real cage counts inform rescue choices',()=>{
 const {c,g}=arena(),a=ape(g,{x:0}),human=g.makeHuman(80,0,null),cage=object(g,{type:'cage',x:140,r:18,count:8});grids(g);a.personality='rescuer';assert.equal(g.tactics.pick(a,220).id,cage.id);a.commandStyle='attackNearest';assert.equal(g.tactics.pick(a,220).id,human.id);delete a.commandStyle;
 const random=c.Math.random;let draws=0;c.Math.random=()=>{draws++;return .5};delete a.personality;a.trainingLevel=Infinity;g.tactics.ensure(a);const personality=a.personality;g.tactics.ensure(a);c.Math.random=random;assert.equal(draws,0);assert.equal(a.personality,personality);assert.ok(c.ATSApePersonalities.includes(personality));assert.equal(a.trainingLevel,0);
});

test('saboteurs, guardians, bold apes and skirmishers choose concrete tactical targets',()=>{
 const {g}=arena(),a=ape(g,{x:0}),near=g.makeHuman(75,0,null),special=g.makeHuman(150,0,null),vehicle={id:'vehicle-tactical',kind:'tank',x:140,y:0,hp:1100};g.vehicles.push(vehicle);grids(g);
 a.personality='saboteur';assert.equal(g.tactics.pick(a,220).id,vehicle.id);a.personality='bold';assert.equal(g.tactics.pick(a,220).id,vehicle.id);vehicle.hp=0;special.role='sniper';a.personality='skirmisher';assert.equal(g.tactics.pick(a,220).id,special.id);
 near.x=170;special.x=200;special.targetId='king';special.role='guard';grids(g);a.personality='guardian';assert.equal(g.tactics.pick(a,220).id,special.id);
});

test('spread charge creates stable wide lanes and nearest attack overrides preferences in real movement',()=>{
 const {g}=arena();for(let i=0;i<24;i++)ape(g,{x:-24-i,state:'follow'});const young=g.makeApe(0,0,'young',null,true);g.makeApe(0,0,'settled','home');grids(g);g.commandCD=0;assert.equal(g.command('spreadCharge',{x:1,y:0}),true);const ordered=g.apes.filter(a=>a.commandStyle==='spreadCharge');assert.equal(ordered.length,24);assert.ok(Math.max(...ordered.map(a=>a.target.y))-Math.min(...ordered.map(a=>a.target.y))>800);assert.equal(young.state,'young');const targets=ordered.map(a=>({...a.target}));g.tactics.order('spreadCharge',{x:1,y:0});assert.deepEqual(ordered.map(a=>({...a.target})),targets);
 const a=ordered[0];a.x=a.y=0;a.personality='rescuer';const human=g.makeHuman(100,0,null);object(g,{type:'cage',x:180,r:20,count:5});grids(g);g.commandCD=0;assert.equal(g.command('attackNearest',{x:1,y:0}),true);const before=Math.hypot(a.x-human.x,a.y-human.y);g.updateApe(a,.2);assert.equal(a._tacticTarget.id,human.id);assert.ok(Math.hypot(a.x-human.x,a.y-human.y)<before,'actual navigation approaches the nearest opponent');g.commandCD=0;g.command('recall');assert.equal(a.commandStyle,undefined);assert.equal(a.state,'follow');
});

test('ordinary walls stay solid and climbing eligibility denies children, gates and heavy walls',()=>{
 const {g}=arena(),w=wall(g),a=ape(g,{species:'gorilla'});grids(g);g.move(a,100,0,90,.2);assert.equal(a.wallClimb,undefined);assert.ok(a.x<0&&g.navigation.blocked(a.x,a.y,10,'ape')===false);assert.equal(g.navigation.blocked(0,0,10,'ape'),true);
 for(const fields of [{id:'king'},{state:'young'},{state:'free'},{species:'gorilla',trainingLevel:0}])assert.equal(g.tactics.canClimb({...a,species:'gibbon',...fields},w),false);assert.equal(g.tactics.canClimb({...a,species:'gorilla',trainingLevel:1},w),true);assert.equal(g.tactics.canClimb({...a,species:'gibbon'}, {...w,wallTier:2}),true);assert.equal(g.tactics.canClimb({...a,species:'gibbon'}, {...w,wallTier:3}),false);assert.equal(g.tactics.canClimb({...a,species:'gibbon'}, {...w,type:'gate'}),false);
});

test('a valid finite climb crests, damages the actual wall and exits to collision-safe ground',()=>{
 const {g}=arena(),w=wall(g),a=ape(g);grids(g);const hp=w.hp,c=climb(g,a,w);assert.equal(a.x,0);assert.equal(a.wallClimbHeight,42);assert.ok(w.hp<hp);for(let i=0;i<20;i++){g.time+=.1;g.updateApe(a,.1)}assert.equal(a.wallClimb,undefined);assert.equal(a.onWallId,undefined);assert.equal(a.wallClimbHeight,0);assert.ok(a.x>=c.exitX);assert.equal(g.navigation.blocked(a.x,a.y,10,'ape'),false);
});

test('water, obstacles, wrong approach and newly blocked exits prevent unsafe wall crossings',()=>{
 const {g}=arena(),w=wall(g),a=ape(g);grids(g);assert.equal(g.tactics.tryClimb(a,-100,0,w),false);assert.equal(g.tactics.tryClimb(a,0,100,w),false);g.world.waterBlocked=(x)=>x>20;assert.equal(g.tactics.tryClimb(a,100,0,w),false);g.world.waterBlocked=()=>false;g.time=1;const blocker=object(g,{type:'rock',x:33,y:0,r:18});assert.equal(g.tactics.tryClimb(a,100,0,w),false);blocker.dead=true;blocker.hp=0;g.time=2;climb(g,a,w);blocker.hp=500;blocker.dead=false;for(let i=0;i<20&&a.wallClimb;i++){g.time+=.1;g.updateApe(a,.1)}assert.equal(a.wallClimb,undefined);assert.equal(g.navigation.blocked(a.x,a.y,10,'ape'),false);assert.ok(a.x<0,'a newly obstructed exit returns the ape to its safe approach');
});

test('recall safely detaches climbers and blast launches from preserved wall height',()=>{
 const {g}=arena(),w=wall(g),a=ape(g);grids(g);climb(g,a,w);g.commandCD=0;assert.equal(g.command('recall'),true);assert.equal(a.wallClimb,undefined);assert.equal(a.state,'follow');assert.equal(g.navigation.blocked(a.x,a.y,10,'ape'),false);
 a.state='charge';a.x=-24;a.y=0;g.time+=1;climb(g,a,w);g.launchBlastReaction(a,{x:35,y:0,type:'grenade',radius:100},{power:.7});assert.equal(a.wallClimb,undefined);assert.equal(a.onWallId,undefined);assert.ok(a.blastReaction.height0>=42);assert.ok(g.blastActive(a));
});

test('saved climbing geometry rejects stale obstacles and unrelated far anchors without teleporting',()=>{
 const {g}=arena(),w=wall(g),a=ape(g);grids(g);climb(g,a,w);const saved=JSON.parse(JSON.stringify(a));g.tactics.restore(a);assert.ok(a.wallClimb,'a legitimate crest survives validation');object(g,{type:'rock',x:33,y:0,r:18});g.tactics.restore(a);assert.equal(a.wallClimb,undefined);assert.equal(g.navigation.blocked(a.x,a.y,10,'ape'),false);
 Object.assign(a,saved,{x:-24,y:0,wallClimb:{...saved.wallClimb,fromX:1000,crestX:1024,exitX:1057}});g.tactics.restore(a);assert.equal(a.wallClimb,undefined);assert.ok(Math.abs(a.x)<80,'invalid anchors cannot move the actor across the map');assert.equal(a.wallClimbHeight,0);assert.equal(a.onWallId,undefined);
});

test('a legitimate saved wall crest resumes the finite climb after a game reload',()=>{
 const {c,g}=arena(),w=wall(g),a=ape(g,{personality:'saboteur'});grids(g);climb(g,a,w);const saved=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(saved),copy=loaded.apes.find(p=>p.id===a.id);assert.ok(copy.wallClimb,'saved wall anchors rebind to the real wall');assert.equal(copy.onWallId,w.id);assert.equal(copy.personality,'saboteur');const targetWall=loaded.world.objects.get(w.id),hp=targetWall.hp;for(let i=0;i<20;i++){loaded.time+=.1;loaded.updateApe(copy,.1)}assert.equal(copy.wallClimb,undefined);assert.equal(copy.wallClimbHeight,0);assert.ok(targetWall.hp<hp,'the saved wall remains an actual crest combat target');assert.equal(loaded.navigation.blocked(copy.x,copy.y,10,'ape'),false);
});

test('training and personalities survive saves without repeated max-HP gains or wound healing',()=>{
 const {c,g}=arena(),a=ape(g,{state:'follow',personality:'guardian'}),base=a.maxHp;a.hp=43;g.applyTraining(a,1);assert.equal(a.maxHp,base+12);assert.equal(a.hp,43);for(let i=0;i<100;i++)g.applyTraining(a,1);assert.equal(a.maxHp,base+12);g.applyTraining(a,3);assert.equal(a.maxHp,base+36);assert.equal(g.apeDamage(a,25),31);const loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),copy=loaded.apes.find(p=>p.id===a.id);assert.equal(copy.personality,'guardian');assert.equal(copy.trainingLevel,3);assert.equal(copy.trainedAppliedLevel,3);assert.equal(copy.maxHp,base+36);assert.equal(copy.hp,43);loaded.applyTraining(copy,3);assert.equal(copy.maxHp,base+36);
});

test('forest climbing discovery is throttled while explicit combat wall checks remain immediate',()=>{
 const {g}=arena(),a=ape(g);let queries=0;const get=g.world.getObjects;g.world.getObjects=(...args)=>{queries++;return get.apply(g.world,args)};for(let i=0;i<100;i++)assert.equal(g.tactics.tryClimb(a,100,0),false);assert.equal(queries,1);g.time=.21;g.tactics.tryClimb(a,100,0);assert.equal(queries,2);const w=wall(g);assert.equal(g.tactics.tryClimb(a,100,0,w),true,'explicit nearby wall does not wait for the discovery throttle');assert.equal(queries,2);
});

test('a fatal blast leaves a flying body rather than a corpse frozen on the wall crest',()=>{
 const {g}=arena(),w=wall(g),a=ape(g);grids(g);climb(g,a,w);a.hp=1;const blast={x:35,y:0,type:'grenade',radius:100};g.hurt(a,1000,blast);const body=g.corpses.find(c=>c.id===a.id);assert.ok(body);assert.equal(body.wallClimb,undefined);assert.equal(body.onWallId,undefined);assert.ok(body.blastZ>=42);g.blastBodies(blast,100,{power:.7});assert.equal(body.blastReaction.stage,'flight');assert.ok(body.blastReaction.height0>=42);
});

test('500 light followers moving on clear routes do not spend wall-discovery queries',()=>{
 const {g}=arena();for(let i=0;i<500;i++)ape(g,{x:(i%25-12)*18,y:(Math.floor(i/25)-10)*18,state:'follow',species:['gibbon','capuchin','chimpanzee'][i%3]});let discovery=0;const get=g.world.getObjects;g.world.getObjects=(x,y,r)=>{if(r===65)discovery++;return get.call(g.world,x,y,r)};const starts=g.apes.map(a=>a.x);
 for(let frame=0;frame<30;frame++){g.time+=1/60;g.navigation.beginFrame(g.time);for(const a of g.apes)g.move(a,100,0,90,1/60);assert.ok(g.navigation.stats.frameSearches<=3&&g.navigation.stats.frameExpanded<=192)}assert.equal(discovery,0,'clear-route Game.move skips speculative wall scans');assert.ok(g.apes.every((a,i)=>a.x-starts[i]>=44),'every actual follower keeps walking');assert.ok(g.apes.every(a=>!a.wallClimb&&g.navigation.blocked(a.x,a.y,10,'ape')===false));
});

test('a physically stalled follower discovers its wall and still completes a safe climb',()=>{
 const {g}=arena(),w=wall(g),a=ape(g,{state:'follow'});object(g,{type:'rock',collision:'rect',x:0,y:17,w:120,h:14});object(g,{type:'rock',collision:'rect',x:0,y:-17,w:120,h:14});object(g,{type:'rock',collision:'rect',x:-60,y:0,w:14,h:34});grids(g);let discovery=0;const get=g.world.getObjects;g.world.getObjects=(x,y,r)=>{if(r===65)discovery++;return get.call(g.world,x,y,r)};
 for(let frame=0;frame<90&&!a.wallClimb;frame++){g.time+=1/60;g.navigation.beginFrame(g.time);g.move(a,100,0,90,1/60)}assert.ok(a.wallClimb,'real navigation stall enables wall discovery for an ordinary follower');assert.equal(a.wallClimb.wallId,w.id);assert.ok(discovery>0&&discovery<=3,'the blocked follower uses throttled discovery');for(let frame=0;frame<26;frame++){g.time+=.1;g.tactics.updateClimb(a,.1)}assert.equal(a.wallClimb,undefined);assert.ok(a.x>20);assert.equal(g.navigation.blocked(a.x,a.y,10,'ape'),false);
});

test('charging Game.move attempts a nearby eligible wall before navigation stalls',()=>{
 const {g}=arena(),w=wall(g),a=ape(g);grids(g);assert.equal(a._nav,undefined);g.move(a,100,0,90,1/60);assert.ok(a.wallClimb);assert.equal(a.wallClimb.wallId,w.id);assert.equal(a._nav,null,'charging wall contact remains immediate');
});
