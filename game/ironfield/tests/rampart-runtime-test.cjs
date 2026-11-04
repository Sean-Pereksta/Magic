'use strict';
const assert=require('node:assert/strict');
const DT=1/30,round=n=>Math.round(n*1000)/1000;
function register(test){
  const D=global.GameData,Engine=global.BattleEngine;
  function setup(kind='curtain',count=4,initial=false,inner=false,nearEnemy=false,enemyType='guardian',insideSource=false,troopType='archer',friendlySupport=false){
    const spec=D.FORTRESS_TYPES[kind],size=kind==='wall'?1600:kind==='keep'?D.STRUCTURE_TYPES.keep.defaultWidth:kind==='classic'?1200:spec.minSize;
    const parts=kind==='wall'?[D.createRampartWall(4200,2600,120,size,{team:0})]:kind==='keep'?D.createBattlefieldStructure('keep',4200,2600,size,size,{team:0}):D.createFortress(kind,4200,2600,size,size,{team:0}),owners=parts.filter(p=>(p.stairs||[]).some(s=>s.id.startsWith('stair-west-'))),owner=owners[inner?owners.length-1:0],stair=owner.stairs.find(s=>s.id.startsWith('stair-west-'));
    const f={team:0,type:troopType,count,x:stair.groundX+120,y:stair.groundY,formation:'line',facing:Math.PI};if(initial)f.rampart={ranks:2,spacing:'normal'};
    const enemyCount=insideSource?1:nearEnemy?8:1,groups=[f,{team:1,type:enemyType,count:enemyCount,x:nearEnemy?4020:15000,y:nearEnemy?owner.y+80:10000,formation:'line'}];
    if(insideSource)groups.push({team:1,type:enemyType,count:1,x:owner.x+owner.w+160,y:owner.y+80,formation:'line'});
    if(friendlySupport)groups.push({team:0,type:'guardian',count:1,x:12000,y:1000,formation:'line'});
    const e=new Engine({map:'custom',seed:46127,customMap:{name:'Moving rampart company',width:16000,height:11000,biome:'grassland',regions:parts},armies:[{name:'Wall company',doctrine:'defensive',roster:{[troopType]:count,...(friendlySupport?{guardian:1}:{})}},{name:'Distant observer',doctrine:'defensive',roster:{[enemyType]:enemyCount+(insideSource?1:0)}}],formations:groups});
    assert(e.ramparts,'Walkable rampart module was not loaded');const r=e.regiments.find(r=>r.team===0),wall=e.structures.find(s=>(s.stairs||[]).some(s=>s.id===stair.id));
    const s={e,r,wall,ids:r.unitIds.slice(),kind:kind+(inner?' inner ring':''),maxStep:0,climbed:new Set(),intermediate:new Set(),climbFrames:0};e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');return s;
  }
  function checkStep(s){
    const before=s.ids.map(id=>({x:s.e.units[id].x,y:s.e.units[id].y,z:s.e.units[id]._rampartZ}));s.e.step(DT);
    for(let i=0;i<s.ids.length;i++){const u=s.e.units[s.ids[i]],p=before[i];if(!u.alive)continue;
      assert([u.x,u.y,u.hp,u._rampartZ].every(Number.isFinite),s.kind+' produced a nonfinite rampart body');assert(s.e.ramparts.isSupported(u),s.kind+' left a body without ground, a real stair, or an intact deck: '+JSON.stringify({time:s.e.time,id:u.id,x:u.x,y:u.y,z:u._rampartZ,stage:u._rampartStage}));
      const d=Math.hypot(u.x-p.x,u.y-p.y);s.maxStep=Math.max(s.maxStep,d);assert(d<=D.TYPES[u.type].speed*DT*1.8+3,s.kind+' teleported a soldier between ground and deck');
      assert(Math.abs(u._rampartZ-p.z)<=70*DT+.1,s.kind+' skipped physical stair heights');
      assert(u.x>=u._radius&&u.y>=u._radius&&u.x<=s.e.width-u._radius&&u.y<=s.e.height-u._radius);
      if(u._rampartZ>0&&u._rampartZ<s.wall.deckHeight-.01){s.climbFrames++;s.intermediate.add(u.id);}
      if(u._onRampart)s.climbed.add(u.id);
    }
    for(let a=0;a<s.ids.length;a++)for(let b=0;b<a;b++){const u=s.e.units[s.ids[a]],v=s.e.units[s.ids[b]];if(!u.alive||!v.alive||Math.abs(u._rampartZ-v._rampartZ)>18||u.routed&&v.routed&&!s.e.regiments[u.regiment]._rampartJob&&!s.e.regiments[v.regiment]._rampartJob)continue;assert(Math.hypot(u.x-v.x,u.y-v.y)/(u._radius+v._radius)>=.88,s.kind+' stair or wall bodies passed through one another: '+JSON.stringify({time:s.e.time,a:{id:u.id,x:u.x,y:u.y,z:u._rampartZ,stage:u._rampartStage},b:{id:v.id,x:v.x,y:v.y,z:v._rampartZ,stage:v._rampartStage}}));}
  }
  function until(s,predicate,seconds=90){for(let tick=0;tick<seconds*30&&s.e.phase==='running';tick++){checkStep(s);if(predicate())return true;}return predicate();}
  const settled=s=>s.ids.every(id=>{const u=s.e.units[id];return u._onRampart&&u._rampartStage==='deck'&&u._rampartPath.length===0;});
  function begin(s,options={ranks:2,spacing:'normal'}){assert(s.ids.every(id=>!s.e.units[id]._onRampart&&!s.e.units[id]._rampartZ));assert.equal(s.e.siege.manRampart([s.r.id],s.wall.id,options),1);s.e.start();}
  const detail=s=>({kind:s.kind,seconds:round(s.e.time),climbed:s.climbed.size,climbFrames:s.climbFrames,maximumStep:round(s.maxStep),cohesion:round(s.r.cohesion),status:s.r.status,stages:s.ids.map(id=>s.e.units[id]._rampartStage)});
  const blockage=s=>({time:round(s.e.time),job:s.r._rampartJob&&{leaving:s.r._rampartJob.leaving,courtyard:s.r._rampartJob.courtyard},units:s.ids.map(id=>s.e.units[id]).filter(u=>['approach','climbing','descending','waiting','courtyard'].includes(u._rampartStage)||u._rampartPath.length).map(u=>({id:u.id,position:[round(u.x),round(u.y),round(u._rampartZ)],stage:u._rampartStage,onDeck:u._onRampart,climb:round(u._rampartClimb),slot:s.e.regiments[u.regiment]._rampartSlots[u._rampartSlot],court:u._rampartCourtGoal,next:u._rampartPath[0],stair:u._rampartAccess&&{id:u._rampartAccess.id,lane:u._rampartAccess.lane,ground:[u._rampartAccess.groundX,u._rampartAccess.groundY],deck:[u._rampartAccess.deckX,u._rampartAccess.deckY]}}))});
  test('Small ranged companies physically climb real stairs onto every fortress deck without ground teleportation',()=>{
    const results=[];for(const kind of Object.keys(D.FORTRESS_TYPES)){
      const s=setup(kind);begin(s);assert(until(s,()=>settled(s)),kind+' could not man a reachable wall: '+JSON.stringify(detail(s)));assert.equal(s.climbed.size,4);assert.equal(s.intermediate.size,4,'A stair ascent skipped intermediate heights');assert(s.ids.every(id=>s.e.units[id].travelDistance>100));results.push(detail(s));
    }return results;
  });
  test('The inner citadel ring has its own usable physical courtyard-to-deck ascent',()=>{
    const s=setup('concentric',4,false,true);begin(s);assert(until(s,()=>settled(s)),'The inner citadel stairs stranded their wall company: '+JSON.stringify(detail(s)));assert.equal(s.climbed.size,4);assert.equal(s.intermediate.size,4);return detail(s);
  });
  test('A gate keep provides real stair access to its higher connected defensive platform',()=>{
    const s=setup('keep');begin(s);assert(until(s,()=>settled(s)),'The gate keep stairs stranded their wall company: '+JSON.stringify(detail(s)));assert.equal(s.wall.deckHeight,56);assert(s.ids.every(id=>s.e.units[id]._rampartZ===56));return detail(s);
  });
  test('An individually drawn rampart wall has a usable physical ground-to-deck stair',()=>{
    const s=setup('wall');begin(s);assert(until(s,()=>settled(s)),'A custom rampart wall stranded its ranged company: '+JSON.stringify(blockage(s)));assert.equal(s.climbed.size,4);assert.equal(s.intermediate.size,4);return detail(s);
  });
  test('A manned wall fires real elevated missiles over its own parapet and damages an outside force',()=>{
    const s=setup('curtain',4,false,false,true),launched=[],launch=s.e._launchProjectile;
    s.e._launchProjectile=function(p){const u=this.units[p.attacker],onWall=!!u._onRampart;launch.call(this,p);if(onWall)launched.push({attacker:p.attacker,launchZ:p.launchZ,targetZ:p.targetZ,rampartTrajectory:p.rampartTrajectory});};
    const enemies=s.e.units.filter(u=>u.team===1),hp=enemies.reduce((n,u)=>n+u.hp,0),wallHp=s.e.structures.reduce((n,p)=>n+p.hp,0);begin(s);assert(until(s,()=>settled(s)),'The ranged company failed to settle before its firing check');
    assert(until(s,()=>enemies.reduce((n,u)=>n+u.hp,0)<hp,15),'Wall archers could not hit outside enemies through the elevated firing lane');assert(launched.length>0);assert(launched.every(p=>p.launchZ===s.wall.deckHeight+20&&p.targetZ===14&&p.rampartTrajectory),'A wall missile omitted physical launch or target height');assert.equal(s.e.structures.reduce((n,p)=>n+p.hp,0),wallHp,'Wall archers hit their own supporting masonry');
    return{...detail(s),elevatedShots:launched.length,enemyDamage:round(hp-enemies.reduce((n,u)=>n+u.hp,0))};
  });
  test('Outside missiles hit a manned wall less often than courtyard missiles while deck cover also reduces damage per hit',()=>{
    const s=setup('curtain',4,false,false,true,'archer',true);for(const u of s.e.units)if(u.team===1){u._cooldown=1000000;u.hp=100000;}begin(s);assert(until(s,()=>settled(s)));const target=s.e.units[s.ids[0]],source=s.e.units.find(u=>u.team===1&&u.x<s.wall.x),inside=s.e.units.find(u=>u.team===1&&u.x>s.wall.x+s.wall.w);
    assert(source&&inside);const exposed=new Engine({map:'custom',seed:46127,customMap:{width:16000,height:11000,regions:[]},armies:[{roster:{archer:1}},{roster:{archer:1}}],formations:[{team:0,type:'archer',count:1,x:target.x,y:target.y},{team:1,type:'archer',count:1,x:source.x,y:source.y}]});
    function incoming(e,defender,attacker){
      // Hold the same two physical positions and resolve independent incoming
      // arrow flights. High fixture HP prevents casualties changing the sample.
      defender.hp=100000;const initial=defender.hp;let hits=0;
      for(let i=0;i<200;i++){const hp=defender.hp,p={x:attacker.x,y:attacker.y,z:0,sx:attacker.x,sy:attacker.y,tx:defender.x,ty:defender.y,team:attacker.team,kind:'arrow',target:defender.id,attacker:attacker.id,type:'archer',regiment:attacker.regiment,age:0,duration:1,damage:20,arc:55};e._launchProjectile(p);e._traceOrdnance(p,0,1);e._impact(p);e.projectiles.length=0;if(defender.hp<hp)hits++;}
      return{flights:200,hits,damage:initial-defender.hp};
    }
    const wall=incoming(s.e,target,source),courtyard=incoming(s.e,target,inside),ground=incoming(exposed,exposed.units.find(u=>u.team===0),exposed.units.find(u=>u.team===1));assert(ground.hits>=190,'The exposed control failed its clear incoming flight lane');assert(courtyard.hits>=190,'The exterior wall accuracy penalty incorrectly applied to a courtyard shooter');assert(wall.hits>=60&&wall.hits<courtyard.hits*.75,'The same wall target was not harder to hit from outside than inside');assert(wall.damage/wall.hits<ground.damage/ground.hits*.9,'The deck did not provide missile damage cover after a successful hit');
    return{outside:{...wall,damage:round(wall.damage)},courtyard:{...courtyard,damage:round(courtyard.damage)},exposed:{...ground,damage:round(ground.damage)}};
  });
  test('Wall musketeers discharge complete physical ranks together from elevated settled positions',()=>{
    const s=setup('curtain',12,false,false,true,'guardian',false,'musketeer'),shots=[],launch=s.e._launchProjectile;
    s.e._launchProjectile=function(p){launch.call(this,p);if(p.type!=='musketeer'||p.regiment!==s.r.id)return;const u=this.units[p.attacker];shots.push({time:this.time,id:u.id,rank:u._rampartRank,settled:u._onRampart&&!u._rampartPath.length,z:p.launchZ,expected:s.r.unitIds.filter(id=>{const v=this.units[id];return v.alive&&!v.routed&&v._rampartRank===u._rampartRank;}).length});};
    begin(s,{ranks:2,spacing:'normal'});assert(until(s,()=>settled(s),100),'The musket company failed to man its wall: '+JSON.stringify(blockage(s)));assert(until(s,()=>shots.length>=24,60),'The wall musket ranks did not fire and reload');
    const batches=new Map();for(const shot of shots){const key=Math.round(shot.time*1000000);if(!batches.has(key))batches.set(key,[]);batches.get(key).push(shot);assert(shot.settled&&shot.z===s.wall.deckHeight+20,'A queued or climbing musketeer fired from ground level');}
    for(const batch of batches.values()){assert.equal(new Set(batch.map(v=>v.rank)).size,1,'Two physical wall ranks fired in one volley');assert.equal(batch.length,batch[0].expected,'A wall row fired piecemeal before all its living members could discharge');}
    assert.equal(new Set(shots.map(v=>v.rank)).size,2);return{...detail(s),volleys:batches.size,shots:shots.length,physicalRanks:2};
  });
  test('A full72-archer company mans connected wall segments with stable wall cohesion and physical separation',()=>{
    const s=setup('curtain',72);begin(s,{ranks:2,spacing:'normal'});assert.equal(s.r._rampartSlots.length,72,'The ring could not provide its requested real deck capacity');assert(until(s,()=>settled(s),200),'A larger wall company jammed on its stairs or connected platforms: '+JSON.stringify(blockage(s)));assert.equal(s.climbed.size,72);assert.equal(s.intermediate.size,72);assert(new Set(s.ids.map(id=>s.e.units[id]._rampartId)).size>1,'The stress company did not exercise connected deck segments');
    assert(s.r.cohesion>=70&&!s.r._routed&&s.r.status!=='shaken','A formed wall company was judged against its old ground layout');return detail(s);
  });
  test('Rampart rank and spacing previews are pure and actual wall companies adopt revised supported slots',()=>{
    const s=setup('curtain',8);begin(s,{ranks:1,spacing:'compact'});assert(until(s,()=>settled(s)),'Initial wall formation did not settle');
    const before=s.ids.map(id=>({x:s.e.units[id].x,y:s.e.units[id].y,z:s.e.units[id]._rampartZ})),epoch=s.r._commandEpoch,slots=JSON.stringify(s.r._rampartSlots),job=s.r._rampartJob;
    const preview=s.e.siege.previewRampart([s.r.id],s.wall.id,{ranks:3,spacing:'wide'});assert.equal(preview.length,8);assert.deepEqual(s.ids.map(id=>({x:s.e.units[id].x,y:s.e.units[id].y,z:s.e.units[id]._rampartZ})),before);assert.equal(s.r._commandEpoch,epoch);assert.equal(JSON.stringify(s.r._rampartSlots),slots);assert.equal(s.r._rampartJob,job,'A layout preview changed the accepted wall order');
    assert.equal(s.e.siege.setRampartFormation([s.r.id],{ranks:3,spacing:'wide'}),1);assert.equal(s.r.rampartRanks,3);assert.equal(s.r.rampartSpacing,'wide');assert.deepEqual(s.r._rampartSlots,preview);assert.notEqual(JSON.stringify(s.r._rampartSlots),slots);assert(until(s,()=>settled(s),60),'Wall troops did not walk into the revised formation');
    for(let a=0;a<s.r._rampartSlots.length;a++)for(let b=0;b<a;b++)assert(Math.hypot(s.r._rampartSlots[a].x-s.r._rampartSlots[b].x,s.r._rampartSlots[a].y-s.r._rampartSlots[b].y)>=D.TYPES.archer.radius*2+3);
    return detail(s);
  });
  test('A wall company descends the actual stairs and reforms on clear courtyard ground',()=>{
    const s=setup();begin(s);assert(until(s,()=>settled(s)));const distance=s.ids.reduce((n,id)=>n+s.e.units[id].travelDistance,0);assert.equal(s.e.siege.leaveRamparts([s.r.id]),1);
    assert(until(s,()=>!s.r._rampartJob,90),'The company remained stuck between deck and courtyard: '+JSON.stringify(detail(s)));assert(s.ids.every(id=>!s.e.units[id]._onRampart&&s.e.units[id]._rampartZ===0&&!s.e._bodyBlocked(s.e.units[id].x,s.e.units[id].y,s.e.units[id]._radius)));assert(s.ids.reduce((n,id)=>n+s.e.units[id].travelDistance,0)>distance+100);assert.equal(s.r.order,'hold');return detail(s);
  });
  test('A24-man classic keep garrison clears its real stair route and reforms after the west gate is breached',()=>{
    const e=new Engine({map:'custom',seed:46127,customMap:{width:6400,height:4400,regions:D.castleWalls(2800,1200,2000,2000,{team:0})},armies:[{roster:{archer:24}},{roster:{archer:4}}],formations:[{team:0,type:'archer',count:24,x:3650,y:2200,formation:'line'},{team:1,type:'archer',count:4,x:1200,y:2200,formation:'line'}]}),r=e.regiments.find(r=>r.team===0),wall=e.structures.find(p=>p.type==='wall'&&p.rampartSide==='west'),s={e,r,wall,kind:'Classic24-man gate response',ids:r.unitIds.slice(),maxStep:0,climbed:new Set(),intermediate:new Set(),climbFrames:0};
    e.command(e.regiments.map(r=>r.id),'hold');for(const u of e.units)u._cooldown=1000000;begin(s,{ranks:2,spacing:'normal'});assert(until(s,()=>settled(s),150),'The classic keep company could not complete its physical ascent: '+JSON.stringify(blockage(s)));assert.equal(s.climbed.size,24);const ascent=round(e.time),gate=e.structures.find(p=>p.type==='gate');e.siege.damage(gate,gate.hp);
    assert(r._rampartJob.leaving&&r._rampartJob.courtyard);assert(until(s,()=>!r._rampartJob,180),'The24-man classic keep descent jammed after its gate was breached: '+JSON.stringify(blockage(s)));assert(s.ids.every(id=>!e.units[id]._onRampart&&e.units[id]._rampartZ===0&&!e._bodyBlocked(e.units[id].x,e.units[id].y,e.units[id]._radius)));assert.equal(r.order,'hold');return{...detail(s),ascentSeconds:ascent,descentSeconds:round(e.time-ascent),garrison:24};
  });
  test('A breached occupied deck drops its supporting troops and releases the company into the courtyard response',()=>{
    const s=setup();begin(s);assert(until(s,()=>settled(s)));const occupied=s.e.siege.byId.get(s.e.units[s.ids[0]]._rampartId),previousHp=s.ids.map(id=>s.e.units[id].hp);
    assert.equal(s.e.siege.damage(occupied,occupied.hp),occupied.maxHp);assert(occupied.breached);assert(s.r._rampartJob.leaving&&s.r._rampartJob.courtyard);assert(s.ids.some(id=>s.e.units[id]._rampartStage==='falling'),'An occupied destroyed deck kept floating troops');
    assert(until(s,()=>!s.r._rampartJob,90),'The breached wall company failed to reform: '+JSON.stringify(detail(s)));assert(s.ids.every(id=>!s.e.units[id]._onRampart&&s.e.units[id]._rampartZ===0));assert(s.ids.some((id,i)=>s.e.units[id].hp<previousHp[i]),'The collapse omitted its physical fall consequence');return detail(s);
  });
  test('Breaching an adjacent gate withdraws intact wall troops by their stairs into a courtyard defense',()=>{
    const s=setup();begin(s);assert(until(s,()=>settled(s)));const gate=s.e.structures.find(g=>g.type==='gate'&&g.rampartSide==='west'),hp=s.ids.map(id=>s.e.units[id].hp);
    assert(gate);assert.equal(s.e.siege.damage(gate,gate.hp),gate.maxHp);assert(gate.breached&&!s.wall.breached);assert(s.r._rampartJob.leaving&&s.r._rampartJob.courtyard,'The neighboring wall garrison ignored its breached gateway');assert(s.ids.every(id=>s.e.units[id]._rampartStage!=='falling'),'An intact wall dropped troops when only its adjacent gate was breached');
    assert(until(s,()=>!s.r._rampartJob,100),'The adjacent gate breach stranded the intact wall company: '+JSON.stringify(detail(s)));assert.deepEqual(s.ids.map(id=>s.e.units[id].hp),hp);assert(s.ids.every(id=>!s.e.units[id]._onRampart&&s.e.units[id]._rampartZ===0));assert.equal(s.r.order,'hold');return detail(s);
  });
  test('Two wall companies reform around an existing ground reserve in separate courtyard blocks facing the breached gate',()=>{
    const parts=D.createFortress('curtain',4200,2600,1800,1800,{team:0}),e=new Engine({map:'custom',seed:46127,customMap:{width:16000,height:11000,regions:parts},armies:[{roster:{archer:4,crossbow:4,hoplite:8}},{roster:{guardian:1}}],formations:[{team:0,type:'archer',count:4,x:4640,y:3000,formation:'line'},{team:0,type:'crossbow',count:4,x:4640,y:3100,formation:'line'},{team:0,type:'hoplite',count:8,x:5100,y:3500,formation:'column'},{team:1,type:'guardian',count:1,x:15000,y:10000,formation:'line'}]});
    const companies=e.regiments.filter(r=>r.type==='archer'||r.type==='crossbow'),reserve=e.regiments.find(r=>r.type==='hoplite'),wall=e.structures.find(p=>(p.stairs||[]).some(a=>a.id.startsWith('stair-west-'))),s={e,r:companies[0],wall,kind:'Two-company courtyard response',ids:e.units.filter(u=>u.team===0).map(u=>u.id),maxStep:0,climbed:new Set(),intermediate:new Set(),climbFrames:0};
    e.command(e.regiments.map(r=>r.id),'hold');assert.equal(e.siege.manRampart(companies.map(r=>r.id),wall.id,{ranks:2,spacing:'normal'}),2);e.start();assert(until(s,()=>companies.every(r=>r.unitIds.every(id=>e.units[id]._onRampart&&!e.units[id]._rampartPath.length)),130),'The two companies could not share the wall access: '+JSON.stringify(blockage(s)));
    const reservePoint={x:wall.courtyardX,y:wall.courtyardY};assert.equal(e.command([reserve.id],'move',reservePoint.x,reservePoint.y),1);assert(until(s,()=>reserve.unitIds.every(id=>Math.hypot(e.units[id].x-reservePoint.x,e.units[id].y-reservePoint.y)<90),70),'The physical ground reserve could not reach the future regrouping sector');e.command([reserve.id],'hold');
    const gate=e.structures.find(p=>p.type==='gate'&&p.rampartSide==='west');e.siege.damage(gate,gate.hp);assert(companies.every(r=>r._rampartJob&&r._rampartJob.courtyard));
    const goals=companies.flatMap(r=>r._rampartJob.courtSlots);assert.equal(goals.length,8);for(let a=0;a<goals.length;a++){const p=goals[a];for(let b=0;b<a;b++)assert(Math.hypot(p.x-goals[b].x,p.y-goals[b].y)>=p.radius+goals[b].radius+8,'Two retreating companies were assigned overlapping courtyard slots');for(const id of reserve.unitIds){const u=e.units[id];assert(Math.hypot(p.x-u.x,p.y-u.y)>=p.radius+u._radius+7,'A courtyard slot was assigned inside the existing reserve');}}
    assert(until(s,()=>companies.every(r=>!r._rampartJob),150),'The joint breach response did not finish physical descent and court formation');for(const r of companies){assert.equal(r.order,'hold');const angle=Math.atan2(gate.y+gate.h/2-r.y,gate.x+gate.w/2-r.x);assert(Math.abs(Math.atan2(Math.sin(r.facing-angle),Math.cos(r.facing-angle)))<.05,'A reorganized company did not face the actual breached gate');assert(r.unitIds.every(id=>!e.units[id]._onRampart&&e.units[id]._rampartZ===0));}
    return{seconds:round(e.time),wallCompanies:2,returnedGarrison:8,groundReserve:8,separateCourtSlots:goals.length,maximumStep:round(s.maxStep)};
  });
  test('A direct Move order persists while the company physically descends then marches to its chosen ground destination',()=>{
    const s=setup();begin(s);assert(until(s,()=>settled(s)));const goal={x:s.wall.courtyardX+350,y:s.wall.courtyardY+400};assert(!s.e._bodyBlocked(goal.x,goal.y,20));assert.equal(s.e.command([s.r.id],'move',goal.x,goal.y),1);const epoch=s.r._commandEpoch;
    assert(until(s,()=>!s.r._rampartJob&&s.ids.every(id=>Math.hypot(s.e.units[id].x-goal.x,s.e.units[id].y-goal.y)<80),110),'The accepted Move order was lost during stair descent: '+JSON.stringify(blockage(s)));assert.equal(s.r.order,'move');assert.equal(s.r._commandEpoch,epoch);assert.equal(s.r.targetX,goal.x);assert.equal(s.r.targetY,goal.y);return detail(s);
  });
  test('An accepted Retreat order physically descends before continuing its withdrawal',()=>{
    const s=setup();begin(s);assert(until(s,()=>settled(s)));assert.equal(s.e.command([s.r.id],'retreat'),1);const epoch=s.r._commandEpoch;assert(s.r.fallingBack&&s.r._manualFallback&&s.r._rampartJob.leaving);
    assert(until(s,()=>!s.r._rampartJob,110),'Retreat stranded a company above ground: '+JSON.stringify(blockage(s)));assert(s.ids.every(id=>!s.e.units[id]._onRampart&&s.e.units[id]._rampartZ===0));assert.equal(s.r._commandEpoch,epoch,'Stair descent replaced the accepted withdrawal');assert.equal(s.r.order,'retreat');assert(s.r.fallingBack&&s.r._manualFallback,'The wall descent canceled the manual withdrawal');return detail(s);
  });
  test('Routed wall survivors use the real stairs before ordinary ground flight',()=>{
    const s=setup('curtain',4,false,false,false,'guardian',false,'archer',true);s.e.command(s.e.regiments.filter(r=>r.team===0&&r.id!==s.r.id).map(r=>r.id),'hold');begin(s);assert(until(s,()=>settled(s)));s.e._route(s.r,'breaks in the routed rampart trial');assert(s.r._routed);assert(s.ids.every(id=>s.e.units[id].routed&&s.e.units[id]._onRampart));
    assert(until(s,()=>!s.r._rampartJob,110),'Routed survivors never cleared their elevated wall state: '+JSON.stringify(blockage(s)));assert(s.ids.every(id=>{const u=s.e.units[id];return u.alive&&u.routed&&!u._onRampart&&u._rampartZ===0&&!s.e._bodyBlocked(u.x,u.y,u._radius);}));const before=s.ids.map(id=>[s.e.units[id].x,s.e.units[id].y]);for(let tick=0;tick<60;tick++)checkStep(s);assert(s.ids.some((id,i)=>Math.hypot(s.e.units[id].x-before[i][0],s.e.units[id].y-before[i][1])>10),'Routed guards never resumed ordinary ground flight');return detail(s);
  });
  test('An authored rampart assignment starts on courtyard ground and walks to a friendly deck after play begins',()=>{
    const s=setup('curtain',4,true),start=s.ids.map(id=>[s.e.units[id].x,s.e.units[id].y]);assert(s.r._rampartJob&&!s.r._rampartJob.leaving);assert(s.ids.every(id=>!s.e.units[id]._onRampart&&s.e.units[id]._rampartZ===0));assert.equal(s.e.time,0);assert.deepEqual(s.ids.map(id=>[s.e.units[id].x,s.e.units[id].y]),start);s.e.start();assert(until(s,()=>settled(s)));return detail(s);
  });
}
module.exports=register;
if(require.main===module){global.window=global;for(const name of ['data','tactics','impacts','siege','ramparts','engine'])require('../src/'+name+'.js');register((name,fn)=>{try{console.log('PASS '+name+' '+JSON.stringify(fn()));}catch(error){console.error('FAIL '+name+'\n'+error.stack);process.exitCode=1;}});}
