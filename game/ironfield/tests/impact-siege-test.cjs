'use strict';
// Public-configuration checks for cavalry momentum and functional sieges.
// Run: node tests/impact-siege-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const { performance } = require('node:perf_hooks');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine, DT = 1 / 30;
  const round = x => Math.round(x * 1000) / 1000;
  const mean = xs => xs.reduce((a,b) => a+b,0) / Math.max(1,xs.length);
  const copy = x => JSON.parse(JSON.stringify(x));
  const active = (e,r) => r.unitIds.map(id=>e.units[id]).filter(u=>u.alive&&!u.routed);
  const center = (e,r) => { const us=active(e,r);return {x:mean(us.map(u=>u.x)),y:mean(us.map(u=>u.y))}; };
  const positions = e => e.units.map(u=>({x:u.x,y:u.y,hp:u.hp,angle:u.angle,impacts:u._impactCount||0}));
  const cfg = (blue,red,formations,regions=[],seed=61273) => ({map:'custom',seed,customMap:{name:'Physical siege trial',biome:'grassland',regions},
    armies:[{name:'Assault',doctrine:'balanced',roster:blue},{name:'Defense',doctrine:'balanced',roster:red}],formations});
  const gapToRect = (u,r) => Math.hypot(Math.max(r.x-u.x,0,u.x-r.x-r.w),Math.max(r.y-u.y,0,u.y-r.y-r.h));
  const inRect = (x,y,r,inset=0) => x>=r.x+inset&&x<=r.x+r.w-inset&&y>=r.y+inset&&y<=r.y+r.h-inset;
  function solidFortificationPieces(e) {
    const result=[];
    for(const structure of e.structures||[]){
      if(structure.breached)continue;
      let pieces=[{x:structure.x,y:structure.y,w:structure.w,h:structure.h}];
      for(const passage of structure.passages||[]){
        const next=[];
        for(const p of pieces){
          const x1=Math.max(p.x,passage.x),x2=Math.min(p.x+p.w,passage.x+passage.w),y1=Math.max(p.y,passage.y),y2=Math.min(p.y+p.h,passage.y+passage.h);
          if(x1>=x2||y1>=y2){next.push(p);continue;}
          if(y1>p.y)next.push({x:p.x,y:p.y,w:p.w,h:y1-p.y});
          if(y2<p.y+p.h)next.push({x:p.x,y:y2,w:p.w,h:p.y+p.h-y2});
          if(x1>p.x)next.push({x:p.x,y:y1,w:x1-p.x,h:y2-y1});
          if(x2<p.x+p.w)next.push({x:x2,y:y1,w:p.x+p.w-x2,h:y2-y1});
        }
        pieces=next;
      }
      result.push(...pieces);
    }
    return result;
  }
  function onDeployedPlatform(e,u) {
    // A deployed tower becomes an engineered walking deck. Its troop body is
    // exempt only when the actual siege system records that deployed platform.
    return u.type==='siegeTower' && u._siegeDeployed && e.siege && (e.siege.ramps||[]).some(r=>r.unitId===u.id||r.towerId===u.id);
  }
  function audit(e,before) {
    const us=e.units.filter(u=>u.alive&&!u.routed&&!u.escaped),buckets=new Map(),fortPieces=solidFortificationPieces(e);
    let minimum=Infinity,maximumStep=0;
    for(const u of us){
      const t=D.TYPES[u.type],p=before[u.id],radius=t.radius;
      assert([u.x,u.y,u.hp,u.angle].every(Number.isFinite),'Impact or siege produced nonfinite physical state');
      assert(u.x>=radius&&u.y>=radius&&u.x<=e.width-radius&&u.y<=e.height-radius,'A body left the physical battlefield');
      assert.equal(e.terrainAt(u.x,u.y).blocked,false,'A living body occupied solid terrain');
      for(const r of e.map.regions)if(r.type==='rock')assert(gapToRect(u,r)>=radius-.1,'An impact swept a body into a rock');
      for(const r of fortPieces){
        // Reconstruct surviving solid rectangles independently from terrainAt.
        // Sample the swept center as well as the final body clearance.
        for(let part=0;part<=4;part++)assert(gapToRect({x:p.x+(u.x-p.x)*part/4,y:p.y+(u.y-p.y)*part/4},r)>=radius-.1,'A body or its swept path intersected surviving wall masonry');
      }
      for(const water of e.map.regions.filter(r=>r.type==='water'))if(gapToRect(u,water)<radius){
        const bridges=e.map.regions.filter(r=>r.type==='bridge');
        for(let point=0;point<16;point++){
          const x=u.x+Math.cos(point*Math.PI/8)*(radius-.1),y=u.y+Math.sin(point*Math.PI/8)*(radius-.1);
          assert(!inRect(x,y,water)||bridges.some(b=>inRect(x,y,b)),'A body edge entered water outside an authored causeway');
        }
      }
      const moved=Math.hypot(u.x-p.x,u.y-p.y);maximumStep=Math.max(maximumStep,moved);
      // Forced motion has its own force direction and duration, but retains the
      // previous global displacement bound; visual height is not ground travel.
      assert(moved<=t.speed*DT*1.8+.25,'Impact or siege movement teleported a body');
      if(onDeployedPlatform(e,u))continue;
      const gx=Math.floor(p.x/96),gy=Math.floor(p.y/96);
      for(let y=gy-2;y<=gy+2;y++)for(let x=gx-2;x<=gx+2;x++)for(const v of buckets.get(x+':'+y)||[]){
        const q=before[v.id],sum=radius+D.TYPES[v.type].radius;
        if(Math.abs(p.x-q.x)>sum+18||Math.abs(p.y-q.y)>sum+18)continue;
        const dx=p.x-q.x,dy=p.y-q.y,vx=u.x-p.x-v.x+q.x,vy=u.y-p.y-v.y+q.y,vv=vx*vx+vy*vy;
        const a=vv?Math.max(0,Math.min(1,-(dx*vx+dy*vy)/vv)):0;
        minimum=Math.min(minimum,Math.hypot(dx+vx*a,dy+vy*a)/sum);
      }
      const key=gx+':'+gy;if(!buckets.has(key))buckets.set(key,[]);buckets.get(key).push(u);
    }
    assert(minimum>=.90,'A charging, pushed, or marching body crossed an occupied body');
    for(const team of e.getStats().teams)assert.equal(team.initial,team.alive+team.dead+team.routed,'Siege lost or duplicated combatants');
    return {minimum,maximumStep};
  }
  function step(e,metrics){const before=positions(e);e.step(DT);const m=audit(e,before);if(metrics){metrics.minimum=Math.min(metrics.minimum,m.minimum);metrics.maximumStep=Math.max(metrics.maximumStep,m.maximumStep);}return before;}
  const metrics = ()=>({minimum:Infinity,maximumStep:0});
  const measured = m=>({minimumBodyRatio:Number.isFinite(m.minimum)?round(m.minimum):null,maximumStep:round(m.maximumStep)});

  test('Mounted movement accelerates from rest and fresh Hold and Retreat commands remain responsive',()=>{
    const e=new Engine(cfg({cavalry:1},{guardian:1},[
      {team:0,type:'cavalry',count:1,x:1500,y:2200,facing:0},
      {team:1,type:'guardian',count:1,x:5600,y:2200,facing:Math.PI}
    ])),r=e.regiments.find(r=>r.team===0),u=e.units[r.unitIds[0]],m=metrics(),samples={};
    e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();e.command([r.id],'move',4000,2200);
    for(let tick=1;tick<=120;tick++){const before=step(e,m);if([3,15,30,60,120].includes(tick))samples[tick/30]={speed:round(Math.hypot(u.x-before[u.id].x,u.y-before[u.id].y)/DT),storedSpeed:round(u._mountSpeed)};}
    assert(samples[.1].speed<D.TYPES.cavalry.speed*.3,'Cavalry instantly reached marching speed');
    assert(samples[.5].speed>samples[.1].speed+8&&samples[1].speed>samples[.5].speed+8,'The acceleration did not produce increasing physical speed');
    assert(samples[2].speed>D.TYPES.cavalry.speed*.65,'Acceleration never led to a useful gallop');
    const held={x:u.x,y:u.y};assert.equal(e.command([r.id],'hold'),1);for(let i=0;i<30;i++)step(e,m);
    assert(Math.hypot(u.x-held.x,u.y-held.y)<.01,'Hold allowed an old gallop to continue');
    assert((u._mountSpeed||0)<1,'Hold retained stored charge momentum');
    assert.equal(e.command([r.id],'retreat'),1);const start={x:u.x,y:u.y};let firstTenth=0;
    for(let i=0;i<60;i++){step(e,m);if(i===2)firstTenth=start.x-u.x;}
    assert(firstTenth>.01,'Retreat did not start moving the rider away from the enemy immediately');
    assert(start.x-u.x>20&&r.fallingBack,'Retreat failed to withdraw the mounted body');
    return {samples,holdMotion:round(Math.hypot(held.x-start.x,held.y-start.y)),retreatProgress:round(start.x-u.x),firstTenthWithdrawal:round(firstTenth),...measured(m)};
  });

  function impactTrial({distance=650,type='militia',backing=0,wall=false,formation='line',seed=61273}={}){
    const offsets=Array.from({length:backing+1},(_,i)=>({x:i*18,y:0}));
    const regions=wall?[{type:'rock',x:2818,y:2070,w:150,h:260}]:[];
    const roster={ [type]:backing+1,guardian: type==='guardian'?backing+2:1 };
    const e=new Engine(cfg({cavalry:1},roster,[
      {team:0,type:'cavalry',count:1,x:2800-distance,y:2200,facing:0},
      {team:1,type,count:backing+1,x:2800,y:2200,facing:Math.PI,formation,offsets},
      {team:1,type:'guardian',count:1,x:5500,y:3900,facing:Math.PI}
    ],regions,seed));
    const r=e.regiments.find(r=>r.team===0),defender=e.regiments.find(r=>r.team===1&&r.type===type&&r.y<3000);
    const rider=e.units[r.unitIds[0]],victims=defender.unitIds.map(id=>e.units[id]).sort((a,b)=>a.x-b.x),front=victims[0],initial=positions(e),m=metrics();
    const events=[];let first=null,preSpeed=0,previousPhysicalSpeed=0,minimumAfter=Infinity,peakLift=0,maximumCavalryX=rider.x;
    const displacement=new Float64Array(victims.length);let expired=false;
    e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.command([r.id],'charge',undefined,undefined,{targetRegimentId:defender.id});e.start();
    for(let tick=0;tick<30*30&&e.phase==='running';tick++){
      const previous=positions(e);if(first===null)preSpeed=Math.max(preSpeed,previousPhysicalSpeed);
      step(e,m);previousPhysicalSpeed=Math.hypot(rider.x-previous[rider.id].x,rider.y-previous[rider.id].y)/DT;
      maximumCavalryX=Math.max(maximumCavalryX,rider.x);
      for(let j=0;j<victims.length;j++)displacement[j]=Math.max(displacement[j],victims[j].x-initial[victims[j].id].x);
      if((front._impactCount||0)>previous[front.id].impacts){
        if(first===null)first=e.time;
        const loss=previous[front.id].hp-front.hp;
        assert(loss>0,'Impact bookkeeping changed without actual target health loss');
        events.push({time:e.time,loss,actualDamage:front._lastImpactDamage||loss,source:front._impactSource});
      }
      if(first!==null){
        if(e.time-first<.25)minimumAfter=Math.min(minimumAfter,rider._mountSpeed||0);
        const progress=Math.max(0,Math.min(1,(e.time-(front._impactStart||0))/Math.max(.001,front._impactDuration||.001)));
        const lift=(front._impactUntil>e.time?front._impactLift:0)*Math.sin(Math.PI*progress);peakLift=Math.max(peakLift,lift);
        if(e.time>front._impactUntil+.1)expired=true;
        if(e.time-first>=2)break;
      }
    }
    return {type,backing,wall,distance,firstContact:first===null?null:round(first),preContactSpeed:round(preSpeed),minimumSpeedAfterImpact:round(minimumAfter),
      impactEvents:events.map(x=>({...x,time:round(x.time),loss:round(x.loss),actualDamage:round(x.actualDamage)})),frontDisplacement:round(displacement[0]),
      rankDisplacements:Array.from(displacement,round),cavalryPenetration:round(maximumCavalryX-2800),peakLift:round(peakLift),visualExpired:expired,
      lastTargetX:round(front.x),targetStillAlive:front.alive,targetRemainingHP:round(front.hp),...measured(m)};
  }

  test('A running cavalry charge damages and displaces infantry, brakes on contact, and has temporary airborne motion',()=>{
    const short=impactTrial({distance:65}),full=impactTrial({distance:650}),sustained=impactTrial({type:'guardian'});
    assert(full.impactEvents.length>0&&full.firstContact!==null,'A full-speed cavalry body never delivered a real contact impact');
    assert(full.preContactSpeed>D.TYPES.cavalry.speed*.7,'The long approach did not reach high physical speed');
    const shortDamage=short.impactEvents[0]?.actualDamage||0,fullDamage=full.impactEvents[0].actualDamage;
    assert(fullDamage>shortDamage*1.2+1,'A full run-up did not produce a stronger physical charge');
    assert(full.frontDisplacement>10&&full.cavalryPenetration>5,'The rider never occupied ground vacated by the displaced enemy');
    assert(full.minimumSpeedAfterImpact<full.preContactSpeed*.9,'Contact failed to consume mounted momentum');
    assert(full.peakLift>6&&full.visualExpired,'A hard impact had no temporary rise-and-land visual state');
    assert(sustained.targetStillAlive&&sustained.impactEvents.length===1,'Sustained contact with a surviving heavy body generated repeated free charge impacts');
    for(const outcome of [short,full,sustained])for(let i=1;i<outcome.impactEvents.length;i++)assert(outcome.impactEvents[i].time-outcome.impactEvents[i-1].time>=1,'Persistent touching applied impact damage without a fresh charge interval');
    return {shortApproach:short,fullApproach:full,sustainedHeavyContact:sustained};
  });

  test('Hoplites and backing ranks resist cavalry impacts while pressure reaches rear bodies and rocks stop it',()=>{
    const open=impactTrial(),hoplite=impactTrial({type:'hoplite',formation:'shieldwall'}),backed=impactTrial({backing:3}),wall=impactTrial({wall:true});
    assert(open.impactEvents.length&&hoplite.impactEvents.length&&backed.impactEvents.length&&wall.impactEvents.length,'A resistance comparison never developed actual impact');
    assert(hoplite.frontDisplacement>=.5&&hoplite.frontDisplacement<open.frontDisplacement*.85,'Formed hoplites did not partly hold against a real charge');
    assert(backed.frontDisplacement<open.frontDisplacement*.9,'Supporting ranks did not dissipate the front impact');
    assert(backed.rankDisplacements.slice(1).filter(x=>x>.3).length>=2,'Force stopped at the first body instead of propagating through the occupied ranks');
    assert(wall.lastTargetX+D.TYPES.militia.radius<=2818+.05&&wall.frontDisplacement<open.frontDisplacement,'Knockback passed through a solid rock boundary');
    return {open,hoplite,backed,wall};
  });

  test('Fresh Move and Retreat orders survive a real knockback and take effect after its brief recovery',()=>{
    const outcomes=[];
    for(const order of ['move','retreat']){
      // Put the intended impact victim on the actual charge axis. The default
      // one-body formation has a lateral slot offset; at the faster gallop it
      // can turn this recovery fixture into weapon strikes without body contact.
      // Keep the ordinary militia health and require a real surviving impact.
      const e=new Engine(cfg({cavalry:1},{militia:1,guardian:1},[
        {team:0,type:'cavalry',count:1,x:2150,y:2200,facing:0},
        {team:1,type:'militia',count:1,x:2800,y:2200,facing:Math.PI,offsets:[{x:0,y:0}]},
        {team:1,type:'guardian',count:1,x:5500,y:3900,facing:Math.PI}
      ])),rider=e.regiments.find(r=>r.team===0),foot=e.regiments.find(r=>r.type==='militia'),u=e.units[foot.unitIds[0]],m=metrics();
      e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');
      e.command([rider.id],'charge',undefined,undefined,{targetRegimentId:foot.id});e.start();
      for(let tick=0;tick<20*30&&!u._impactCount&&e.phase==='running';tick++)step(e,m);
      assert(u.alive&&u._impactCount>0&&u._impactUntil>e.time,'The command fixture never experienced a surviving physical impact');
      e.command([rider.id],'hold');
      const start={x:u.x,y:u.y,time:e.time},landing=u._impactUntil;
      assert.equal(e.command([foot.id],order,3100,2500),1,'A knocked soldier rejected a fresh player order');
      assert.equal(foot.order,order);
      assert(u._impactUntil>=landing,'A new order erased physical impact recovery');
      let turningFrames=0,afterRecoveryDistance=0,groundAtRecovery=null;
      for(let tick=0;tick<3*30&&e.phase==='running';tick++){
        step(e,m);if(u.state==='turning')turningFrames++;
        if(e.time>=landing&&groundAtRecovery===null)groundAtRecovery={x:u.x,y:u.y};
        if(groundAtRecovery)afterRecoveryDistance=Math.hypot(u.x-groundAtRecovery.x,u.y-groundAtRecovery.y);
      }
      assert(u.alive&&!u.routed,'The test order lost its subject before command responsiveness could be measured');
      assert(afterRecoveryDistance>40,'A completed knockback stranded the fresh command');
      if(order==='move'){
        assert(u.y-start.y>40,'The active Move never changed direction away from the old charge axis');
        assert.equal(foot.order,'move','Recovery silently replaced the fresh Move order');
      }else{
        assert(u.x-start.x>50&&foot.fallingBack,'The impacted formation failed to withdraw away from the attacker');
      }
      outcomes.push({order,impactAt:round(start.time),recoverySeconds:round(landing-start.time),turningFrames,
        movementAfterRecovery:round(afterRecoveryDistance),xProgress:round(u.x-start.x),yProgress:round(u.y-start.y),...measured(m)});
    }
    return {outcomes};
  });

  // Siege fixtures below are deliberately small public encounters. They keep a
  // distant defending unit alive so that deployment and passage can finish.
  const wallLine=(gate=true,hp=200)=>gate?[
    {type:'wall',x:3200,y:0,w:80,h:2080,team:1,hp:2200},
    {type:'gate',x:3200,y:2080,w:80,h:240,team:1,hp},
    {type:'wall',x:3200,y:2320,w:80,h:2080,team:1,hp:2200}
  ]:[{type:'wall',x:3200,y:0,w:80,h:4400,team:1,hp:2200}];

  test('Siege catalog, equipment caps and authored fortifications are complete and independently cloned',()=>{
    assert.equal(D.TYPE_ORDER.length,23);assert.equal(D.MAPS.length,23+D.EXPANSION_MAPS.length);assert.equal(D.PRESETS.filter(p=>!p.historical).length,24+D.EXPANSION_PRESETS.length);assert.equal(D.PRESETS.length,24+D.EXPANSION_PRESETS.length+(D.HISTORICAL_BATTLES||[]).length);
    for(const id of ['siegeTower','batteringRam']){assert(D.TYPES[id].equipment&&D.TYPES[id].groupSize===1);assert.equal(D.TYPES[id].maxCount,12);}
    const initial=JSON.stringify(D.MAPS),source=cfg({siegeTower:999,batteringRam:999},{guardian:1},undefined,wallLine());
    const e=new Engine(source);assert(e.siege&&e.structures.length===3,'Fortification metadata produced no functional structures');
    for(const id of ['siegeTower','batteringRam']){
      assert.equal(e.units.filter(u=>u.type===id).length,12,'Imported siege counts ignored their bounded equipment cap');
      assert(e.regiments.filter(r=>r.type===id).every(r=>r.initial===1),'Siege engines were bundled into a dense troop formation');
    }
    source.customMap.regions[0].hp=9999;source.customMap.regions[0].x=100;
    assert.equal(e.structures[0].x,3200);assert.equal(e.structures[0].maxHp,2200);
    assert.equal(JSON.stringify(D.MAPS),initial,'Runtime structures mutated the shared stock maps');
    for(const id of ['stonekeep','moat','mountainfort']){
      const map=D.MAPS.find(m=>m.id===id);assert(map&&map.regions.some(r=>r.type==='wall')&&map.regions.some(r=>r.type==='gate'));
      const preset=D.PRESETS.find(p=>p.map===id);assert(preset&&preset.formations?.length,'A siege battlefield has no authored assault/defense deployment');
      const battle=new Engine(copy(preset));assert(battle.structures.length>0&&battle.units.some(u=>D.TYPES[u.type].equipment));
      audit(battle,positions(battle));
    }
    return {types:D.TYPE_ORDER.length,maps:D.MAPS.length,presets:D.PRESETS.length,fortificationCount:e.structures.length,equipmentCaps:12};
  });

  function siegeFixture(equipment,gate=true){
    const formations=[{team:0,type:'swordsman',count:64,x:2700,y:2200,facing:0},
      {team:1,type:'guardian',count:1,x:5400,y:3900,facing:Math.PI}];
    const roster={swordsman:64};if(equipment){roster[equipment]=1;formations.splice(1,0,{team:0,type:equipment,count:1,x:2920,y:2200,facing:0});}
    const e=new Engine(cfg(roster,{guardian:1},formations,wallLine(gate))),foot=e.regiments.find(r=>r.type==='swordsman'),machine=e.regiments.find(r=>r.type===equipment);
    e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();e.command([foot.id],'move',3650,2200);
    return {e,foot,machine};
  }

  test('A closed gate blocks foot troops until a ram physically reaches and breaches it, then the column crosses',()=>{
    const control=siegeFixture(null),cm=metrics();for(let tick=0;tick<20*30;tick++)step(control.e,cm);
    assert(active(control.e,control.foot).every(u=>u.x<3200-D.TYPES[u.type].radius),'Foot troops walked through a closed gate');
    const {e,foot,machine}=siegeFixture('batteringRam'),ram=e.units[machine.unitIds[0]],gate=e.structures.find(s=>s.type==='gate'),m=metrics();
    const initialHP=gate.hp,startX=ram.x,revision=e.terrainRevision;let firstStrike=null,breachedAt=null,maxCrossed=0,strikes=0;
    for(let tick=0;tick<70*30&&e.phase==='running';tick++){
      const beforeHP=gate.hp;step(e,m);
      if(gate.hp<beforeHP){strikes++;if(firstStrike===null)firstStrike=e.time;assert(gapToRect(ram,gate)<=D.TYPES.batteringRam.range+ram._radius+.05,'A ram damaged the gate from outside physical strike range');}
      if(gate.breached&&breachedAt===null)breachedAt=e.time;
      maxCrossed=Math.max(maxCrossed,active(e,foot).filter(u=>u.x>3280+D.TYPES[u.type].radius).length);
      if(maxCrossed===64)break;
    }
    assert(firstStrike!==null&&ram.travelDistance>100&&firstStrike>2,'The gate opened without the ram physically approaching');
    assert(gate.hp<initialHP&&gate.breached&&breachedAt!==null&&strikes>=1,'Ram blows never produced a real breach');
    assert.equal(e.terrainAt(3240,2200).blocked,false,'A breached gate retained stale solid terrain');
    assert(e.terrainRevision>revision,'A breach did not invalidate the rendered/navigation terrain');
    assert(maxCrossed>=48,'A narrow breached gate stranded most of its marching infantry');
    return {initialGateHP:initialHP,firstStrike:round(firstStrike),breachedAt:round(breachedAt),ramTravel:round(ram.travelDistance),ramAdvance:round(ram.x-startX),realStrikes:strikes,infantryAcross:maxCrossed,...measured(m)};
  });

  test('A siege tower approaches a wall, visibly deploys a real passage, and lets a narrow infantry column cross',()=>{
    const {e,foot,machine}=siegeFixture('siegeTower',false),tower=e.units[machine.unitIds[0]],m=metrics(),start={x:tower.x,y:tower.y};
    const stages=new Set();let firstRamp=null,firstNearWall=null,maxCrossed=0,maximumDeckHeight=0;
    for(let tick=0;tick<90*30&&e.phase==='running';tick++){
      step(e,m);if(machine._siegeJob?.stage)stages.add(machine._siegeJob.stage);
      if(firstNearWall===null&&gapToRect(tower,e.structures[0])<tower._radius+35)firstNearWall=e.time;
      if(e.siege.ramps.length&&firstRamp===null)firstRamp=e.time;
      for(const u of active(e,foot))if(e.siege.heightAt)maximumDeckHeight=Math.max(maximumDeckHeight,e.siege.heightAt(u.x,u.y)||0);
      maxCrossed=Math.max(maxCrossed,active(e,foot).filter(u=>u.x>3280+D.TYPES[u.type].radius).length);
      if(maxCrossed===64)break;
    }
    assert(firstRamp!==null&&firstNearWall!==null&&firstRamp-firstNearWall>=2.5,'The assault ramp appeared without a physical approach and deployment');
    assert(Math.hypot(tower.x-start.x,tower.y-start.y)>100,'A stationary distant tower created a wall passage');
    assert(e.siege.ramps.length>=1&&maxCrossed>=40,'The deployed tower did not provide a usable crossing for the infantry');
    assert(maximumDeckHeight>3,'Troops crossed the tower passage without a physical elevated deck surface');
    assert(e.structures[0].hp>0,'The tower test only worked by destroying the wall');
    return {firstNearWall:round(firstNearWall),rampDeployed:round(firstRamp),infantryAcross:maxCrossed,maximumDeckHeight:round(maximumDeckHeight),stages:[...stages],...measured(m)};
  });

  test('A departing siege tower waits for real deck occupants before obeying Move or fresh Auto without overlap',()=>{
    const outcomes=[];
    for(const order of ['move','auto']){
      const {e,foot,machine}=siegeFixture('siegeTower',false),tower=e.units[machine.unitIds[0]],m=metrics();
      const occupants=()=>active(e,foot).filter(u=>Math.hypot(u.x-tower.x,u.y-tower.y)<(u._radius+tower._radius)*.96+.6);
      let occupiedAt=null;
      for(let tick=0;tick<45*30&&e.phase==='running';tick++){
        step(e,m);
        if(tower._siegeDeployed&&occupants().some(u=>Math.hypot(u.x-tower.x,u.y-tower.y)<(u._radius+tower._radius)*.92)){occupiedAt=e.time;break;}
      }
      assert(occupiedAt!==null,'The departure fixture never placed real marching infantry underneath the deployed tower');
      const initialOccupants=new Set(occupants().map(u=>u.id)),initialDistances=new Map(active(e,foot).map(u=>[u.id,Math.hypot(u.x-tower.x,u.y-tower.y)])),anchor={x:tower.x,y:tower.y},ramps=e.siege.ramps.length;
      assert.equal(e.command([machine.id],order,3050,1900),1);
      assert.equal(machine.order,order,'A fresh command was rejected during deck use');
      let waitingFrames=0,undockedAt=null,maxProgress=0;
      for(let tick=0;tick<30*30&&e.phase==='running';tick++){
        step(e,m);
        if(tower._siegeUndocking){
          waitingFrames++;
          assert(Math.hypot(tower.x-anchor.x,tower.y-anchor.y)<.01,'The tower dragged its occupied walking deck through marching bodies');
          for(const u of occupants())if(Math.hypot(u.x-tower.x,u.y-tower.y)<(u._radius+tower._radius)*.90)assert(initialOccupants.has(u.id),`A departing tower kept admitting new bodies underneath it (${order}, time ${round(e.time)}, unit ${u.id}, initial gap ${round(initialDistances.get(u.id))}, current gap ${round(Math.hypot(u.x-tower.x,u.y-tower.y))})`);
        }
        if(!tower._siegeDeployed&&undockedAt===null)undockedAt=e.time;
        maxProgress=Math.max(maxProgress,Math.hypot(tower.x-anchor.x,tower.y-anchor.y));
        if(maxProgress>50)break;
      }
      assert(waitingFrames>0&&undockedAt!==null,'The tower did not complete its bounded safe-undocking phase');
      assert(maxProgress>25,'Clear ground never released the new tower movement command');
      assert.equal(machine.order,order);assert.equal(e.siege.ramps.length,ramps,'Withdrawing the machine removed an occupied permanent passage');
      assert.equal(e.terrainAt(3240,2200).blocked,false,'Tower departure made its completed assault corridor impassable');
      outcomes.push({order,occupiedAt:round(occupiedAt),actualDeckOccupants:initialOccupants.size,waitingFrames,undockedAt:round(undockedAt),departureDistance:round(maxProgress),permanentRamps:ramps,...measured(m)});
    }
    return {outcomes};
  });

  test('Catapults and ballistas damage fortifications only after a real projectile flight and Hold stops additional releases',()=>{
    const outcomes=[];
    for(const type of ['catapult','ballista']){
      const e=new Engine(cfg({[type]:1},{guardian:1},[
        {team:0,type,count:1,x:2450,y:2200,facing:0},
        {team:1,type:'guardian',count:1,x:5500,y:3900,facing:Math.PI}
      ],wallLine(true,1000))),gun=e.regiments.find(r=>r.team===0),u=e.units[gun.unitIds[0]],gate=e.structures.find(s=>s.type==='gate'),m=metrics();
      e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();
      assert.equal(e.siege.command([gun.id],gate.id),1,'Artillery rejected an explicit hostile fortification');
      const seen=new Set();let firstRelease=null,firstDamage=null,peakHeight=0,flightSamples=0,damageEvents=0,afterDamage=0;
      const hp=gate.hp;
      for(let tick=0;tick<60*30&&e.phase==='running';tick++){
        const oldHP=gate.hp,prior=e.projectiles.slice();step(e,m);
        for(const p of e.projectiles)if(p.structureTarget===gate.id){
          if(!seen.has(p)){
            seen.add(p);if(firstRelease===null){firstRelease=e.time;assert(gapToRect(u,gate)<=D.TYPES[type].range+.05,'Artillery released beyond its real attack range');e.command([gun.id],'hold');}
            assert(Math.hypot(p.sx-u.x,p.sy-u.y)<D.TYPES[type].speed*DT+.1,'The projectile was not released from its actual artillery body');
          }
          if(p.age>0&&p.age<p.duration){flightSamples++;peakHeight=Math.max(peakHeight,p.z);assert(p.x>p.sx&&p.x<p.tx,'The siege projectile skipped its visible path');}
        }
        if(gate.hp<oldHP){
          damageEvents++;if(firstDamage===null)firstDamage=e.time;
          assert(prior.some(p=>p.structureTarget===gate.id&&p.age>=p.duration),'Structure damage preceded projectile arrival');
        }
        if(firstDamage!==null&&++afterDamage>=60)break;
      }
      assert(firstRelease!==null&&firstDamage>firstRelease+.1&&flightSamples>=3&&peakHeight>2,'The bombardment skipped a real airborne flight');
      assert(gate.hp<hp&&damageEvents===1&&seen.size===1,'Hold allowed additional structural attacks after the already-fired projectile');
      assert.equal(gun.order,'hold');assert.equal(gun._siegeJob,null);
      outcomes.push({type,releaseAt:round(firstRelease),damageAt:round(firstDamage),flightSamples,peakHeight:round(peakHeight),actualStructureDamage:round(hp-gate.hp),projectilesReleased:seen.size,...measured(m)});
    }
    return {outcomes};
  });

  test('Deployed tower bowmen fire real close-range arrows and damage a nearby defender after flight',()=>{
    const e=new Engine(cfg({siegeTower:1},{guardian:2},[
      {team:0,type:'siegeTower',count:1,x:2920,y:2200,facing:0},
      {team:1,type:'guardian',count:1,x:3165,y:2270,facing:Math.PI},
      {team:1,type:'guardian',count:1,x:5500,y:3900,facing:Math.PI}
    ],wallLine(false))),tower=e.units.find(u=>u.type==='siegeTower'),defender=e.units.find(u=>u.type==='guardian'&&u.x<4000),m=metrics(),seen=new Set();
    e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();
    const hp=defender.hp;let deployedAt=null,firstRelease=null,firstDamage=null,releaseDistance=null;
    for(let tick=0;tick<35*30&&e.phase==='running';tick++){
      step(e,m);if(tower._siegeDeployed&&deployedAt===null)deployedAt=e.time;
      for(const p of e.projectiles)if(p.type==='siegeTower'&&p.target===defender.id){
        assert.equal(p.kind,'arrow');seen.add(p);
        if(firstRelease===null){firstRelease=e.time;releaseDistance=Math.hypot(tower.x-defender.x,tower.y-defender.y);}
      }
      if(defender.hp<hp&&firstDamage===null)firstDamage=e.time;
      if(seen.size>=2&&firstDamage!==null)break;
    }
    assert(deployedAt!==null&&firstRelease>=deployedAt,'The bowmen did not use a physically deployed firing tower');
    assert(releaseDistance>tower._radius+defender._radius&&releaseDistance<85,'The fixture did not isolate the former close-range artillery fallback boundary');
    assert(seen.size>=2&&firstDamage>firstRelease+.1&&defender.hp<hp,'Tower bowmen substituted a cosmetic or instantaneous close-range strike for actual arrows');
    return {deployedAt:round(deployedAt),firstArrowAt:round(firstRelease),firstDamageAt:round(firstDamage),releaseDistance:round(releaseDistance),actualArrows:seen.size,defenderHPRemoved:round(hp-defender.hp),...measured(m)};
  });

  test('A ram and its infantry sustain progress through the authored narrow moat causeway and breached gate',()=>{
    const source=cfg({swordsman:96,batteringRam:1},{guardian:1},[
      {team:0,type:'swordsman',count:96,x:2680,y:1320,facing:0},
      {team:0,type:'batteringRam',count:1,x:2950,y:1320,facing:0},
      {team:1,type:'guardian',count:1,x:5500,y:3900,facing:Math.PI}
    ]);source.map='moat';delete source.customMap;
    const e=new Engine(source),foot=e.regiments.find(r=>r.type==='swordsman'),ram=e.units.find(u=>u.type==='batteringRam'),gate=e.structures.find(s=>s.type==='gate'&&s.y<2000),m=metrics();
    e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();e.command([foot.id],'move',4200,1320);
    let crossed=0,breachedAt=null;const samples=[];
    for(let tick=0;tick<110*30&&e.phase==='running';tick++){
      step(e,m);if(gate.breached&&breachedAt===null)breachedAt=e.time;
      crossed=active(e,foot).filter(u=>u.x>3640+u._radius).length;
      if((tick+1)%300===0)samples.push({time:round(e.time),crossed,centerX:round(center(e,foot).x)});
      if(crossed===96)break;
    }
    assert(breachedAt!==null&&ram.travelDistance>500,'The engine did not really traverse the causeway and breach its gate');
    assert(crossed>=72,'The narrow causeway stranded most of the assault column');
    assert(samples.some(s=>s.centerX>3120),'The physical formation never progressed onto the moat crossing');
    return {map:'moat',units:e.units.length,gateBreachedAt:round(breachedAt),infantryAcross:crossed,ramTravel:round(ram.travelDistance),samples,...measured(m)};
  });
}

if(require.main===module){
  const root=process.argv[2]||path.resolve(__dirname,'..');global.window=global;
  for(const name of ['data','tactics','impacts','siege','engine']){const file=path.join(root,'src',name+'.js');if(fs.existsSync(file))require(file);}
  const results=[],filter=(process.env.IRONFIELD_TEST_FILTER||'').toLowerCase();
  register((name,body)=>{if(filter&&!name.toLowerCase().includes(filter))return;const start=performance.now();try{const detail=body();results.push({name,passed:true,milliseconds:round(performance.now()-start),...detail});console.log('PASS '+name+' '+JSON.stringify(detail));}catch(error){results.push({name,passed:false,error:error.message});console.error('FAIL '+name+' '+error.stack);process.exitCode=1;}});
  function round(x){return Math.round(x*1000)/1000;}
  console.log(JSON.stringify({passed:results.filter(r=>r.passed).length,failed:results.filter(r=>!r.passed).length,results},null,2));
}
module.exports=register;
