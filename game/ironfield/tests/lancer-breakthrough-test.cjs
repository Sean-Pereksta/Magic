'use strict';
// Physical regression for shock cavalry entering the interior of weak ranks.
// Run: node tests/lancer-breakthrough-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { performance } = require('node:perf_hooks');
const DT = 1 / 30, FRONT = 2800, CENTER = 2200, ROWS = 4, COLS = 14, RIDERS = 12;
const round = value => Math.round(value * 1000) / 1000;
const mean = xs => xs.reduce((a,b)=>a+b,0) / Math.max(1,xs.length);

function configuration({type='militia',distance=650,seed=7137,wall=false,autoplay=false}={}) {
  const defenders=Array.from({length:ROWS*COLS},(_,i)=>({x:Math.floor(i/COLS)*18,y:(i%COLS-(COLS-1)/2)*18}));
  const riders=Array.from({length:RIDERS},(_,i)=>({x:-Math.floor(i/6)*28,y:(i%6-2.5)*32}));
  return {map:'custom',seed,customMap:{name:'The Levy Line',biome:'grassland',regions:wall?[{type:'rock',x:2880,y:1950,w:100,h:500}]:[]},
    armies:[{name:'Banner Lancers',doctrine:autoplay?'aggressive':'balanced',roster:{lancer:RIDERS,guardian:1}},
      {name:'The Levy Line',doctrine:'defensive',roster:type==='guardian'?{guardian:ROWS*COLS+1}:{[type]:ROWS*COLS,guardian:1}}],
    formations:[{team:0,type:'lancer',count:RIDERS,x:FRONT-distance,y:CENTER,facing:0,formation:'line',offsets:riders},
      {team:0,type:'guardian',count:1,x:700,y:700,facing:0,formation:'line'},
      {team:1,type,count:ROWS*COLS,x:FRONT,y:CENTER,facing:Math.PI,formation:'line',offsets:defenders},
      {team:1,type:'guardian',count:1,x:5500,y:3900,facing:Math.PI,formation:'line'}]};
}
const snapshot = e => e.units.map(u=>({x:u.x,y:u.y,hp:u.hp,alive:u.alive,impacts:u._impactCount||0,impactUntil:u._impactUntil}));
const rectGap = (x,y,r)=>Math.hypot(Math.max(r.x-x,0,x-r.x-r.w),Math.max(r.y-y,0,y-r.y-r.h));
function audit(e,before,metrics) {
  const D=global.GameData,active=e.units.filter(u=>u.alive&&!u.routed&&!u.escaped);
  for(let i=0;i<active.length;i++){
    const u=active[i],p=before[u.id],t=D.TYPES[u.type],radius=t.radius;
    assert([u.x,u.y,u.hp,u.angle].every(Number.isFinite),'Lancer pressure produced invalid physical state');
    assert(u.x>=radius&&u.y>=radius&&u.x<=e.width-radius&&u.y<=e.height-radius,'A body left the battlefield');
    assert.equal(e.terrainAt(u.x,u.y).blocked,false,'A pushed body entered solid terrain');
    const travel=Math.hypot(u.x-p.x,u.y-p.y);metrics.maximumStep=Math.max(metrics.maximumStep,travel);
    assert(travel<=t.speed*DT*1.8+.25,'Stronger charge pressure teleported a body');
    for(const rock of e.map.regions.filter(r=>r.type==='rock'))for(let part=0;part<=4;part++){
      assert(rectGap(p.x+(u.x-p.x)*part/4,p.y+(u.y-p.y)*part/4,rock)>=radius-.1,'An impact swept a body through rock');
    }
    for(let j=0;j<i;j++){
      const v=active[j],q=before[v.id],sum=radius+D.TYPES[v.type].radius;
      if(Math.abs(p.x-q.x)>sum+18||Math.abs(p.y-q.y)>sum+18)continue;
      const dx=p.x-q.x,dy=p.y-q.y,vx=u.x-p.x-v.x+q.x,vy=u.y-p.y-v.y+q.y,vv=vx*vx+vy*vy;
      const f=vv?Math.max(0,Math.min(1,-(dx*vx+dy*vy)/vv)):0;
      const ratio=Math.hypot(dx+vx*f,dy+vy*f)/sum;
      metrics.minimumBodyRatio=Math.min(metrics.minimumBodyRatio,ratio);
      assert(ratio>=.90,'A lancer or displaced soldier crossed an occupied body');
    }
  }
  for(const team of e.getStats().teams)assert.equal(team.initial,team.alive+team.dead+team.routed,'A charge lost or duplicated soldiers');
}

function trial(options={}) {
  const type=options.type||'militia',distance=options.distance??650,seed=options.seed??7137;
  const e=new global.BattleEngine(configuration(options)),r=e.regiments.find(r=>r.type==='lancer');
  const defense=e.regiments.find(r=>r.team===1&&r.y===CENTER),riderIds=new Set(r.unitIds),defenderIds=new Set(defense.unitIds);
  const initial=snapshot(e),rear=FRONT+(ROWS-1)*18,halfWidth=(COLS-1)*9-global.GameData.TYPES.lancer.radius;
  const rowById=new Map(defense.unitIds.map((id,i)=>[id,Math.floor(i/COLS)]));
  const flankers=new Set(),hitters=new Set(),crossed=new Set(),impactRows=new Set(),hitRows=new Set(),impacted=new Set();
  const forwardByRow=Array(ROWS).fill(0),forcedById=new Float64Array(e.units.length),impactEvents=[];
  const metrics={minimumBodyRatio:Infinity,maximumStep:0};
  let firstHit=null,firstImpact=null,damage=0,preHitSpeed=0,peakDisplacement=0,maximumRiderX=-Infinity,firstCrossing=null;
  let retreatAt=null,retreatPositions=null,firstWithdrawal=0,retreatAccepted=0;
  const realHit=e._hit;e._hit=function(attacker,target,...args){
    const hp=target.hp;realHit.call(e,attacker,target,...args);
    if(riderIds.has(attacker.id)&&defenderIds.has(target.id)&&target.hp<hp){
      hitters.add(attacker.id);hitRows.add(rowById.get(target.id));damage+=hp-Math.max(0,target.hp);
      if(firstHit===null)firstHit=e.time;
    }
  };
  const realContact=e.impacts.beforeMove;e.impacts.beforeMove=function(u,...args){
    const count=this.stats.contacts,speed=u._mountSpeed;
    realContact.call(this,u,...args);
    if(this.stats.contacts>count&&riderIds.has(u.id)){
      const target=e.units[u._lastImpactTarget];
      if(defenderIds.has(target.id))impactEvents.push({time:e.time,speedBefore:speed,speedAfter:u._mountSpeed,damage:u._lastImpactDamage,row:rowById.get(target.id)});
    }
  };
  if(!options.autoplay){
    e.command(e.regiments.filter(group=>group.team===1||group.type!=='lancer').map(group=>group.id),'hold');
    e.command([r.id],'charge',undefined,undefined,{targetRegimentId:defense.id});
  }
  e.start();
  for(let tick=0;tick<30*35&&e.phase==='running';tick++){
    const before=snapshot(e);
    if(firstHit===null)for(const id of r.unitIds)preHitSpeed=Math.max(preHitSpeed,e.units[id]._mountSpeed);
    e.step(DT);audit(e,before,metrics);
    for(const id of r.unitIds){
      const u=e.units[id];maximumRiderX=Math.max(maximumRiderX,u.x);
      // A rider which bypasses an end of the original formation is never a
      // breakthrough, even if it later turns back behind the defenders.
      if(u.x>FRONT-50&&u.x<rear+u._radius+3&&Math.abs(u.y-CENTER)>halfWidth)flankers.add(id);
      if(u.alive&&!flankers.has(id)&&hitters.has(id)&&u.x>rear+u._radius&&Math.abs(u.y-CENTER)<=halfWidth){
        crossed.add(id);if(firstCrossing===null)firstCrossing=e.time;
      }
      if(retreatPositions&&e.time-retreatAt<=.1+.00001)firstWithdrawal=Math.max(firstWithdrawal,retreatPositions[id].x-u.x);
    }
    for(const id of defense.unitIds){
      const u=e.units[id],p=initial[id],row=rowById.get(id);
      forwardByRow[row]=Math.max(forwardByRow[row],u.x-p.x);
      peakDisplacement=Math.max(peakDisplacement,Math.hypot(u.x-p.x,u.y-p.y));
      if((u._impactUntil>e.time||before[id].impactUntil>e.time-DT)&&riderIds.has(u._impactSource))forcedById[id]+=Math.hypot(u.x-before[id].x,u.y-before[id].y);
      if(u._impactCount>before[id].impacts){
        impactRows.add(row);impacted.add(id);if(firstImpact===null)firstImpact=e.time;
      }
    }
    if(options.retreat&&firstImpact!==null&&retreatAt===null&&e.time-firstImpact>=.3){
      retreatAt=e.time;retreatPositions=snapshot(e);retreatAccepted=e.command([r.id],'retreat');
    }
    if(retreatAt!==null&&e.time-retreatAt>=4)break;
    if(!options.retreat&&firstHit!==null&&e.time-firstHit>=6)break;
  }
  const result={type,distance,seed,wall:!!options.wall,riders:RIDERS,defenders:ROWS*COLS,defenderRanks:ROWS,
    firstHit:firstHit===null?null:round(firstHit),firstImpact:firstImpact===null?null:round(firstImpact),firstInteriorCrossing:firstCrossing===null?null:round(firstCrossing),
    observedSecondsAfterHit:firstHit===null?null:round(e.time-firstHit),interiorBreakthroughs:crossed.size,excludedFlankers:flankers.size,
    actualHitters:hitters.size,distinctImpacted:impacted.size,impactRows:[...impactRows].sort(),hitRows:[...hitRows].sort(),
    maximumRiderPenetration:round(maximumRiderX-FRONT),peakDefenderDisplacement:round(peakDisplacement),peakForcedTravel:round(Math.max(...forcedById)),
    rankForwardDisplacements:forwardByRow.map(round),precontactSpeed:round(preHitSpeed),actualDamage:round(damage),
    impactEvents:impactEvents.map(event=>Object.fromEntries(Object.entries(event).map(([k,v])=>[k,round(v)]))),
    survivingRiders:r.unitIds.filter(id=>e.units[id].alive&&!e.units[id].routed).length,
    survivingDefenders:defense.unitIds.filter(id=>e.units[id].alive&&!e.units[id].routed).length,
    minimumBodyRatio:Number.isFinite(metrics.minimumBodyRatio)?round(metrics.minimumBodyRatio):null,maximumStep:round(metrics.maximumStep)};
  if(options.retreat)Object.assign(result,{retreatAccepted,retreatAt:round(retreatAt),firstWithdrawal:round(firstWithdrawal),
    meanWithdrawal:round(mean(r.unitIds.filter(id=>e.units[id].alive&&!e.units[id].routed).map(id=>retreatPositions[id].x-e.units[id].x))),retreatOrder:r.order,fallingBack:r.fallingBack});
  return result;
}

function register(test) {
  const cache=new Map(),run=options=>{const key=JSON.stringify(options||{});if(!cache.has(key))cache.set(key,trial(options));return cache.get(key);};
  test('Fast lancers physically break through the interior of four levy ranks',()=>{
    const outcomes=[run({seed:7137}),run({seed:4719})];
    for(const result of outcomes){
      assert(result.firstImpact!==null&&result.distinctImpacted>=4,'The run-up produced no real mass contact');
      assert(result.interiorBreakthroughs>=3,'Fewer than three real lancers penetrated all four levy ranks through their interior');
      assert(result.hitRows.length>=3,'Lancers did not reach and damage deeper levy ranks');
      assert(result.peakForcedTravel>30,'Weak troops still receive only a small contact nudge');
      assert(result.impactEvents.every(event=>event.speedBefore>event.speedAfter&&event.damage>0),'A contact failed to deal damage and consume momentum');
    }
    return {outcomes};
  });
  test('Lancer penetration needs a run-up and formed hoplites pikes and heavy troops retain their resistance',()=>{
    const full=run({seed:7137}),short=run({distance:65}),hoplites=run({type:'hoplite'}),pikes=run({type:'pikeman'}),heavy=run({type:'guardian'});
    assert(full.precontactSpeed>short.precontactSpeed*1.5,'A long approach does not build greater physical speed');
    assert(full.interiorBreakthroughs>=short.interiorBreakthroughs+2,'Lancers penetrate dense weak ranks equally well without a run-up');
    assert(full.peakForcedTravel>short.peakForcedTravel*1.2+5,'Fast contact does not create noticeably stronger displacement');
    for(const result of [hoplites,pikes,heavy]){
      assert.equal(result.interiorBreakthroughs,0,'Lancers freely penetrated a prepared counter through its interior');
      assert(result.peakForcedTravel<full.peakForcedTravel*.55,'Armored or braced bodies lost their impact resistance');
    }
    assert(pikes.actualDamage<full.actualDamage*.55,'Prepared pikes no longer blunt the frontal charge');
    return {full,short,hoplites,pikes,heavy};
  });
  test('Stronger lancer pressure still stops at rock and a fresh retreat interrupts penetration',()=>{
    const wall=run({wall:true}),retreat=run({retreat:true});
    assert(wall.firstImpact!==null&&wall.peakForcedTravel>5,'The rock check did not exercise an actual strong charge');
    assert.equal(retreat.retreatAccepted,1,'The charging formation rejected the fresh retreat');
    assert(retreat.firstWithdrawal>.01,'Lancers did not start backing out of the charge immediately');
    assert(retreat.meanWithdrawal>60&&retreat.fallingBack,'Lancers continued their old plunge instead of withdrawing');
    return {wall,retreat};
  });
}

if(require.main===module){
  const root=process.argv[2]||path.resolve(__dirname,'..');global.window=global;
  for(const name of ['data','tactics','impacts','siege','engine']){const file=path.join(root,'src',name+'.js');if(fs.existsSync(file))require(file);}
  const results=[],filter=(process.env.IRONFIELD_TEST_FILTER||'').toLowerCase();
  register((name,body)=>{if(filter&&!name.toLowerCase().includes(filter))return;const began=performance.now();try{const detail=body();results.push({name,passed:true,milliseconds:round(performance.now()-began),...detail});console.log('PASS '+name+' '+JSON.stringify(detail));}catch(error){results.push({name,passed:false,error:error.message});console.error('FAIL '+name+' '+error.stack);process.exitCode=1;}});
  console.log(JSON.stringify({passed:results.filter(r=>r.passed).length,failed:results.filter(r=>!r.passed).length,results},null,2));
}
module.exports=register;
module.exports.trial=trial;
module.exports.configuration=configuration;
