'use strict';
const assert=require('node:assert/strict');
const DT=1/30;

function register(test){
  const D=global.GameData,Engine=global.BattleEngine;
  function encounter(regions,formations,seed=49163){
    const rosters=[{},{}];for(const f of formations)rosters[f.team][f.type]=(rosters[f.team][f.type]||0)+f.count;
    const e=new Engine({map:'custom',seed,customMap:{name:'Rampart command regression',width:16000,height:11000,biome:'grassland',regions},armies:rosters.map((roster,team)=>({name:team?'Eastern company':'Western company',doctrine:'defensive',roster})),formations});
    assert(e.ramparts,'The physical rampart module is required');e.command(e.regiments.map(r=>r.id),'hold');return e;
  }
  const group=(team,type,count,x,y)=>({team,type,count,x,y,formation:'line'});
  const settled=(e,r,component)=>r.unitIds.every(id=>{const u=e.units[id];return u.alive&&u._onRampart&&u._rampartStage==='deck'&&!u._rampartPath.length&&(!component||component.has(u._rampartId));});
  function advance(e,r,predicate,seconds=180,observe=()=>{}){
    const ids=r.unitIds.slice();let maxStep=0;
    for(let tick=0;tick<seconds*30&&e.phase==='running';tick++){
      const before=ids.map(id=>({x:e.units[id].x,y:e.units[id].y,z:e.units[id]._rampartZ}));e.step(DT);
      for(let i=0;i<ids.length;i++){
        const u=e.units[ids[i]],p=before[i];if(!u.alive)continue;
        assert([u.x,u.y,u._rampartZ,u.hp].every(Number.isFinite),'A wall command produced nonfinite body state');
        assert(e.ramparts.isSupported(u),'A wall command left a body without a deck, stair or clear ground: '+JSON.stringify({time:e.time,id:u.id,x:u.x,y:u.y,z:u._rampartZ,stage:u._rampartStage}));
        const distance=Math.hypot(u.x-p.x,u.y-p.y);maxStep=Math.max(maxStep,distance);
        assert(distance<=D.TYPES[u.type].speed*DT*1.8+3,'A wall command teleported a soldier across its route');
        assert(Math.abs(u._rampartZ-p.z)<=70*DT+.1,'A wall command skipped intermediate stair heights');
        observe(u,p,e);
      }
      if(predicate())return {completed:true,maxStep};
    }
    return {completed:predicate(),maxStep};
  }
  function castlePair(){
    const first=D.createFortress('classic',1500,800,1400,1400,{team:0}),second=D.createFortress('classic',3600,800,1400,1400,{team:0});
    const entry=first.find(s=>s.rampartSide==='west'&&s.stairs.length),stair=entry.stairs[0];
    const e=encounter([...first,...second],[group(0,'archer',1,stair.groundX+90,stair.groundY),group(1,'guardian',1,15000,10000)]);
    for(const gate of e.structures.filter(s=>s.castleGate))assert(e.siege.setGateOpen(gate.id,true,0),'The fixture requires real open ground routes between its castles');
    const r=e.regiments.find(r=>r.team===0),wall=e.structures.find(s=>s.x===entry.x&&s.y===entry.y),destination=e.structures.find(s=>s.rampartSide==='west'&&s.x===3600&&s.stairs.length);
    assert(wall&&destination);assert.equal(e.siege.manRampart([r.id],wall.id,{ranks:1,spacing:'normal'}),1);e.start();
    assert(advance(e,r,()=>settled(e,r)).completed,'The source castle fixture did not physically man its wall');
    return {e,r,wall,destination,u:e.units[r.unitIds[0]]};
  }

  test('Multiple regiment wall previews reserve distinct posts and exactly match accepted sequential assignments without mutation',()=>{
    const parts=D.createFortress('classic',4200,2600,1600,1600,{team:0}),owner=parts.find(s=>s.rampartSide==='west'&&s.stairs.length),stair=owner.stairs[0];
    const e=encounter(parts,[group(0,'archer',2,stair.groundX+100,stair.groundY-110),group(0,'crossbow',3,stair.groundX+180,stair.groundY),group(0,'militia',2,stair.groundX+100,stair.groundY+110),group(0,'archer',1,stair.groundX+260,stair.groundY+200),group(1,'guardian',1,15000,10000)]);
    const selected=e.regiments.filter(r=>r.team===0).slice(0,3),reserve=e.regiments.filter(r=>r.team===0)[3],wall=e.structures.find(s=>s.x===owner.x&&s.y===owner.y),ids=selected.map(r=>r.id);
    assert.equal(e.siege.manRampart([reserve.id],wall.id,{ranks:1,spacing:'compact'}),1);
    const snapshot=()=>JSON.stringify({config:e.config,units:e.units,regiments:e.regiments.map(r=>({id:r.id,order:r.order,epoch:r._commandEpoch,ranks:r.rampartRanks,spacing:r.rampartSpacing,slots:r._rampartSlots,job:r._rampartJob}))});
    const before=snapshot(),jobs=e.regiments.map(r=>r._rampartJob),options={ranks:2,spacing:'normal'},preview=e.siege.previewRampart(ids,wall.id,options);
    assert.equal(snapshot(),before,'Preview mutated bodies, accepted jobs, settings, or configuration');
    assert(e.regiments.every((r,i)=>r._rampartJob===jobs[i]),'Preview replaced an existing accepted wall order');
    assert.equal(preview.length,7);assert.equal(new Set(preview.map(p=>[p.x,p.y,p.z].join(':'))).size,7,'Several previewed companies were assigned the same post');
    assert.equal(e.siege.manRampart(ids,wall.id,options),3);
    const actual=selected.flatMap(r=>r._rampartSlots);assert.deepEqual(actual,preview,'Hover preview disagreed with real sequential slot reservation');
    const all=[...actual,...reserve._rampartSlots];for(let i=0;i<all.length;i++)for(let j=0;j<i;j++)assert(Math.hypot(all[i].x-all[j].x,all[i].y-all[j].y)>=15-1e-7,'Accepted companies overlap one another or the existing reserve');
    return {companies:3,previewedPosts:preview.length,existingReservedPosts:reserve._rampartSlots.length,purePreview:true};
  });

  test('An accepted guard transfer between disconnected friendly castles descends and reapproaches through real open gates and stairs',()=>{
    const {e,r,destination,u}=castlePair(),oldWall=u._rampartId,old={x:u.x,y:u.y,z:u._rampartZ},component=new Set(e.ramparts.component(destination.id).map(s=>s.id));
    assert(!component.has(oldWall),'The transfer fixture accidentally uses connected deck sections');
    assert.equal(e.siege.manRampart([r.id],destination.id,{ranks:2,spacing:'wide'}),1,'A reachable friendly castle transfer should be accepted');
    assert.deepEqual({x:u.x,y:u.y,z:u._rampartZ},old,'Accepting the transfer instantly moved the guard');
    let descended=false,ground=false,reascended=false;
    const result=advance(e,r,()=>settled(e,r,component),360,v=>{
      if(v._rampartStage==='descending')descended=true;
      if(!v._onRampart&&v._rampartZ===0){assert(!e._bodyBlocked(v.x,v.y,v._radius));ground=true;}
      if(ground&&v._rampartStage==='climbing'&&v._rampartZ>0&&v._rampartZ<destination.deckHeight)reascended=true;
    });
    assert(result.completed,'The accepted transfer stayed on the old castle or became stuck between castles');
    assert(descended&&ground&&reascended,'The transfer skipped physical descent, open ground, or the destination stairs');
    assert.notEqual(u._rampartId,oldWall);assert.equal(r.rampartRanks,2);assert.equal(r.rampartSpacing,'wide');
    return {seconds:Math.round(e.time*100)/100,from:oldWall,to:u._rampartId,physicalDescentAndReascent:true,maximumStep:result.maxStep};
  });

  test('Melee wall guards retaliate against an attacker on a deployed assault deck while ground troops cannot strike through the floor',()=>{
    const blue=D.createRampartWall(4200,2600,120,900,{team:0,courtyardSide:'east'}),a=blue.stairs[0];
    const e=encounter([blue],[group(0,'swordsman',1,a.groundX+90,a.groundY),group(1,'guardian',1,3900,2950),group(1,'siegeTower',1,4000,3000)]),r=e.regiments.find(r=>r.team===0),enemyReg=e.regiments.find(r=>r.type==='guardian'),machine=e.regiments.find(r=>r.type==='siegeTower');
    const own=e.structures[0];
    for(const u of e.units)u.hp=u.maxHp=10000;
    assert.equal(e.siege.manRampart([r.id],own.id,{ranks:1,spacing:'normal'}),1);e.start();
    assert(advance(e,r,()=>settled(e,r),180).completed,'The defender did not physically reach its guard post');
    const guard=e.units[r.unitIds[0]],attacker=e.units[enemyReg.unitIds[0]];
    assert.equal(e.siege.command([machine.id],own.id),1);assert(advance(e,r,()=>e.siege.ramps.length>0,40).completed,'The attacking tower never physically deployed its assault deck');
    assert.equal(e.command([enemyReg.id],'move',guard.x-16,guard.y),1);
    assert(advance(e,r,()=>Math.abs(e.unitAltitude(guard)-e.unitAltitude(attacker))<3&&Math.hypot(guard.x-attacker.x,guard.y-attacker.y)<=D.TYPES.swordsman.range*1.12+attacker._radius,60).completed,'The opponent never physically reached the elevated assault-deck combat position');
    assert(e.siege.heightAt(attacker.x,attacker.y)>30&&!attacker._onRampart,'The control did not use the real legacy assault-deck height');
    const guardHp=guard.hp,attackerHp=attacker.hp,started=e.time;for(let i=0;i<90;i++)e.step(DT);
    assert(attacker.hp<attackerHp&&guard._lastAttack>started,'A melee guard never retaliated against the same-height attacker');assert(guard.hp<guardHp,'The same-height enemy did not provide a real opposing melee control');

    const gate=D.createRampartWall(4200,2600,120,900,{team:0,courtyardSide:'east',type:'gate'}),access=gate.stairs[0],ground=encounter([gate],[group(0,'swordsman',1,access.groundX+90,access.groundY),group(1,'guardian',1,3900,3000)]),defenders=ground.regiments.find(r=>r.team===0),enemy=ground.regiments.find(r=>r.team===1);
    const support=ground.structures[0];for(const u of ground.units)u.hp=u.maxHp=10000;
    assert.equal(ground.siege.manRampart([defenders.id],support.id,{ranks:1,spacing:'normal'}),1);ground.start();assert(advance(ground,defenders,()=>settled(ground,defenders),180).completed);
    assert(ground.siege.setGateOpen(support.id,true,0),'Opening the gate must retain its physical upper walkway');
    const elevated=ground.units[defenders.unitIds[0]],below=ground.units[enemy.unitIds[0]],destination={x:elevated.x,y:elevated.y},hp=[elevated.hp,below.hp];
    assert(!ground._bodyBlocked(destination.x,destination.y,below._radius));assert.equal(ground.command([enemy.id],'move',destination.x,destination.y),1);
    assert(advance(ground,defenders,()=>Math.hypot(below.x-destination.x,below.y-destination.y)<12,40).completed,'The ground control could not reach its legal position beneath the open gate');ground.command([enemy.id],'hold');
    assert(Math.hypot(elevated.x-below.x,elevated.y-below.y)<=D.TYPES.guardian.range+elevated._radius,'The through-floor control did not overlap horizontal melee reach');assert(elevated._rampartZ-below._rampartZ>18);
    assert.equal(ground._contactTarget(below,D.TYPES.guardian.range),-1);for(let i=0;i<90;i++)ground.step(DT);
    assert.deepEqual([elevated.hp,below.hp],hp,'Ground and wall troops exchanged through-floor melee damage');
    return {sameHeightEnemyDamage:attackerHp-attacker.hp,sameHeightGuardDamage:guardHp-guard.hp,groundControlDamage:0,groundControlElevationGap:elevated._rampartZ-below._rampartZ};
  });

  test('A manual Retreat order leaves the rampart by physical stairs before continuing on safe ground',()=>{
    const {e,r,u}=castlePair(),height=u._rampartZ;
    assert.equal(e.command([r.id],'retreat'),1);assert(u._onRampart&&u._rampartZ===height,'Retreat instantly dropped the guard to ground level');
    assert(r._rampartJob&&r._rampartJob.leaving,'Retreat bypassed the rampart departure order');
    let descended=false,intermediate=false;
    const result=advance(e,r,()=>!r._rampartJob&&!u._onRampart&&u._rampartZ===0,150,v=>{if(v._rampartStage==='descending')descended=true;if(v._rampartZ>0&&v._rampartZ<height)intermediate=true;});
    assert(result.completed,'Manual Retreat stranded its guard on the wall');assert(descended&&intermediate,'Manual Retreat skipped the actual stair descent');assert(!e._bodyBlocked(u.x,u.y,u._radius));
    return {seconds:Math.round(e.time*100)/100,safelyDescended:true,maximumStep:result.maxStep};
  });

  test('Guards on an intact wall escape with a physical fall when collapse removes their only connected stairs',()=>{
    const accessWall=D.createRampartWall(4200,2600,120,500,{team:0,courtyardSide:'east'}),guardWall=D.createRampartWall(4200,3100,120,500,{team:0,courtyardSide:'east'});
    guardWall.stairs=[];
    const access=accessWall.stairs[0],e=encounter([accessWall,guardWall],[group(0,'swordsman',4,access.groundX+90,access.groundY),group(1,'guardian',1,15000,10000)]),r=e.regiments.find(r=>r.team===0),entry=e.structures[0],post=e.structures[1];
    assert.equal(e.ramparts.stairs(e.ramparts.component(post.id)).length,1,'The fixture requires exactly one connected stair access');
    assert.equal(e.siege.manRampart([r.id],post.id,{ranks:1,spacing:'normal'}),1);e.start();
    assert(advance(e,r,()=>settled(e,r,new Set([post.id])),180).completed,'The guard company did not physically reach the intact adjoining section');
    const hp=r.unitIds.map(id=>e.units[id].hp),positions=r.unitIds.map(id=>({x:e.units[id].x,y:e.units[id].y,z:e.units[id]._rampartZ})),started=e.time;
    assert.equal(e.siege.damage(entry,entry.hp),entry.maxHp);assert(entry.breached&&!post.breached&&post.hp===post.maxHp,'The collapse must destroy only the stair-bearing section');
    assert.equal(e.ramparts.stairs(e.ramparts.component(post.id)).length,0,'The surviving platform still has an unintended stair route');
    assert.deepEqual(r.unitIds.map(id=>({x:e.units[id].x,y:e.units[id].y,z:e.units[id]._rampartZ})),positions,'Losing the stairs instantly moved or dropped a guard');
    const falling=new Set(),intermediate=new Set();
    for(const id of r.unitIds)if(e.units[id]._rampartStage==='falling')falling.add(id);
    const result=advance(e,r,()=>!r._rampartJob&&r.unitIds.every(id=>{const u=e.units[id];return u.alive&&!u._onRampart&&u._rampartZ===0;}),90,u=>{if(u._rampartStage==='falling')falling.add(u.id);if(u._rampartZ>0&&u._rampartZ<post.deckHeight)intermediate.add(u.id);});
    assert(result.completed,'The loss of the sole stairs stranded guards on the surviving wall: '+JSON.stringify({job:r._rampartJob,bodies:r.unitIds.map(id=>{const u=e.units[id];return{id,x:u.x,y:u.y,z:u._rampartZ,hp:u.hp,stage:u._rampartStage,fall:u._rampartFallTarget,goal:u._rampartCourtGoal,path:u._rampartGroundPath};})}));
    assert.equal(falling.size,r.unitIds.length,'A stranded guard bypassed the emergency fall');assert.equal(intermediate.size,r.unitIds.length,'A guard skipped the physical descent to ground level');
    let totalDamage=0;
    for(let i=0;i<r.unitIds.length;i++){
      const u=e.units[r.unitIds[i]];assert(!e._bodyBlocked(u.x,u.y,u._radius)&&e.ramparts.isSupported(u),'An evacuated guard remained floating or embedded in the intact wall');
      assert(u.x>post.x+post.w+u._radius,'A guard escaped toward the exterior instead of the courtyard');assert(u.hp<hp[i],'Emergency evacuation skipped its fall consequence');totalDamage+=hp[i]-u.hp;
    }
    return {guards:r.unitIds.length,intactWallStillStanding:true,secondsAfterCollapse:Math.round((e.time-started)*100)/100,physicalFalls:falling.size,totalFallDamage:Math.round(totalDamage*100)/100,maximumStep:result.maxStep};
  });
}
module.exports=register;
if(require.main===module){
  global.window=global;for(const name of ['data','tactics','impacts','siege','ramparts','engine'])require('../src/'+name+'.js');
  register((name,fn)=>{try{console.log('PASS '+name+' '+JSON.stringify(fn()));}catch(error){console.error('FAIL '+name+'\n'+error.stack);process.exitCode=1;}});
}
