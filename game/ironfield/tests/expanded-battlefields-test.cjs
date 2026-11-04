'use strict';
// Large-map deployment, route, and physical siege checks for the 2.2 catalog.
// Run: node tests/expanded-battlefields-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const { performance } = require('node:perf_hooks');
const NEW_MAPS = ['grandplain','longgrass','silverwood','blackforest','fivefords','redcanyon','dunesea','frostlake','reeddelta','imperialwall','floodgate','highcitadel'];
const DT = 1 / 30, round = n => Math.round(n * 1000) / 1000;
const rosterTotal = config => config.armies.reduce((sum,a) => sum + Object.values(a.roster).reduce((n,v) => n+v,0),0);
const alive = (e,r) => r.unitIds.map(id=>e.units[id]).filter(u=>u.alive&&!u.routed);
function configuration(map,groups) {
  const counts=[{},{}], formations=groups.map(([team,type,count,x,y])=>{
    counts[team][type]=(counts[team][type]||0)+count;
    return {team,type,count,x,y,facing:team?Math.PI:0,formation:'line'};
  });
  return {map,seed:282390,armies:[{name:'Route trial',doctrine:'balanced',roster:counts[0]},{name:'Distant garrison',doctrine:'defensive',roster:counts[1]}],formations};
}
function physical(e,before) {
  const units=e.units.filter(u=>u.alive&&!u.routed&&!u.escaped);let minimum=Infinity,maxStep=0;
  for(let i=0;i<units.length;i++){
    const u=units[i],p=before[u.id];
    assert([u.x,u.y,u.angle,u.hp].every(Number.isFinite),'A large-map body became nonfinite');
    assert(!e._bodyBlocked(u.x,u.y,u._radius),'A large-map body entered solid terrain');
    const travel=Math.hypot(u.x-p.x,u.y-p.y);maxStep=Math.max(maxStep,travel);
    assert(travel<=global.GameData.TYPES[u.type].speed*DT*1.8+.25,'A large-map body teleported');
    for(let part=0;part<=4;part++){
      const x=p.x+(u.x-p.x)*part/4,y=p.y+(u.y-p.y)*part/4;
      for(const block of e._blocks){
        if(block.breached)continue;
        const gap=Math.hypot(Math.max(block.x-x,0,x-block.x-block.w),Math.max(block.y-y,0,y-block.y-block.h));
        if(gap>=u._radius-.1)continue;
        if(block.type==='rock')assert.fail('Movement swept a body through rock');
        const openings=block.type==='water'?e.map.regions.filter(r=>r.type==='bridge'):block.passages||[];
        for(let sample=0;sample<16;sample++){
          const px=x+Math.cos(sample*Math.PI/8)*(u._radius-.1),py=y+Math.sin(sample*Math.PI/8)*(u._radius-.1);
          const inside=r=>px>r.x&&px<r.x+r.w&&py>r.y&&py<r.y+r.h;
          assert(!inside(block)||openings.some(inside),'Movement swept a body outside its bridge or breach');
        }
      }
    }
    if(u._siegeDeployed)continue;
    for(let j=0;j<i;j++){
      const v=units[j];if(v._siegeDeployed)continue;const q=before[v.id],sum=u._radius+v._radius;
      if(Math.abs(p.x-q.x)>sum+16||Math.abs(p.y-q.y)>sum+16)continue;
      const dx=p.x-q.x,dy=p.y-q.y,vx=u.x-p.x-v.x+q.x,vy=u.y-p.y-v.y+q.y,vv=vx*vx+vy*vy;
      const f=vv?Math.max(0,Math.min(1,-(dx*vx+dy*vy)/vv)):0;
      const ratio=Math.hypot(dx+vx*f,dy+vy*f)/sum;minimum=Math.min(minimum,ratio);
      assert(ratio>=.90,'Large-map marching crossed an occupied body');
    }
  }
  return {minimum,maxStep};
}
function step(e,m) {
  const before=e.units.map(u=>({x:u.x,y:u.y}));e.step(DT);
  const result=physical(e,before);m.minimum=Math.min(m.minimum,result.minimum);m.maxStep=Math.max(m.maxStep,result.maxStep);
}
function register(test) {
  const D=global.GameData,Engine=global.BattleEngine;
  test('Twelve larger battlefields and their full scenarios deploy bounded separated armies',()=>{
    assert.equal(D.MAPS.length,23);assert.equal(D.PRESETS.length,24);assert.equal(new Set(D.MAPS.map(m=>m.id)).size,23);
    assert.deepEqual(D.WORLD_SIZES.map(s=>[s.width,s.height]),[[6400,4400],[9600,6600],[12800,8800],[16000,11000]]);
    const outcomes=[];
    for(const id of NEW_MAPS){
      const map=D.MAPS.find(m=>m.id===id),preset=D.PRESETS.find(p=>p.map===id);
      assert(map&&preset,'A larger field has no authored encounter');
      assert(map.width*map.height>=6400*4400*2.25,'A new field only relabeled the old map size');
      assert(map.regions.length<=32,'A large field uses excessive fine-grained terrain');
      for(const r of map.regions)assert(r.x>=0&&r.y>=0&&r.w>0&&r.h>0&&r.x+r.w<=map.width&&r.y+r.h<=map.height,'Authored terrain exceeds its map');
      const e=new Engine(preset),buckets=new Map();let minimum=Infinity;
      assert.equal(e.width,map.width);assert.equal(e.height,map.height);assert.equal(e.units.length,rosterTotal(preset),'Large scenario silently lost soldiers');
      for(const u of e.units){
        assert([u.x,u.y,u.angle].every(Number.isFinite));assert(!e._bodyBlocked(u.x,u.y,u._radius),'Authored deployment intersects terrain: '+id+' '+u.id+' '+u.type+' '+u.x+','+u.y);
        assert(u.x>=u._radius&&u.y>=u._radius&&u.x<=e.width-u._radius&&u.y<=e.height-u._radius,'Authored deployment exceeds world bounds');
        const cx=Math.floor(u.x/64),cy=Math.floor(u.y/64);
        for(let y=cy-1;y<=cy+1;y++)for(let x=cx-1;x<=cx+1;x++)for(const v of buckets.get(x+':'+y)||[]){
          const ratio=Math.hypot(u.x-v.x,u.y-v.y)/(u._radius+v._radius);minimum=Math.min(minimum,ratio);
          assert(ratio>=.90,'Large scenario spawned overlapping bodies');
        }
        const key=cx+':'+cy;if(!buckets.has(key))buckets.set(key,[]);buckets.get(key).push(u);
      }
      outcomes.push({map:id,width:e.width,height:e.height,units:e.units.length,formations:e.regiments.length,minimumBodyRatio:round(minimum)});
    }
    return {outcomes};
  });
  test('Large field routes connect both armies at mounted clearance through rivers rocks and woods',()=>{
    const routes=[];
    for(const id of NEW_MAPS.filter(id=>!['imperialwall','floodgate','highcitadel'].includes(id))){
      const map=D.MAPS.find(m=>m.id===id),e=new Engine(configuration(id,[[0,'guardian',1,map.width*.1,map.height*.1],[1,'guardian',1,map.width*.9,map.height*.9]]));
      for(const fraction of [.2,.5,.8]){
        const from={x:map.width*.25,y:map.height*fraction},to={x:map.width*.75,y:map.height*fraction};
        const clearance=D.TYPES.cavalry.radius+3,waypoints=e._findPath(from.x,from.y,to.x,to.y,clearance);
        assert(waypoints.length>0,id+' has disconnected army approaches');
        let previous=from,total=0;
        for(const next of waypoints){assert(e._lineClear(previous.x,previous.y,next.x,next.y,clearance),id+' returned a route through terrain');total+=Math.hypot(next.x-previous.x,next.y-previous.y);previous=next;}
        assert(Math.hypot(previous.x-to.x,previous.y-to.y)<1,id+' path did not reach its destination');
        routes.push({map:id,sector:fraction,waypoints:waypoints.length,length:round(total)});
      }
    }
    return {routes};
  });
  test('Infantry physically clears the new delta crossings and canyon passages without teleporting',()=>{
    const outcomes=[];
    for(const fixture of [{map:'reeddelta',start:[3300,3300],goal:[6250,3600],seconds:120},{map:'redcanyon',start:[3900,3300],goal:[5550,3300],seconds:95}]){
      const e=new Engine(configuration(fixture.map,[[0,'swordsman',24,...fixture.start],[1,'guardian',1,9000,6000]])),foot=e.regiments.find(r=>r.team===0),m={minimum:Infinity,maxStep:0};
      e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();e.command([foot.id],'move',...fixture.goal);
      let arrived=0;
      for(let tick=0;tick<fixture.seconds*30&&e.phase==='running';tick++){
        step(e,m);arrived=alive(e,foot).filter(u=>Math.hypot(u.x-fixture.goal[0],u.y-fixture.goal[1])<180).length;if(arrived===24)break;
      }
      assert(arrived>=22,fixture.map+' stranded a moving infantry formation');
      outcomes.push({map:fixture.map,arrived,seconds:round(e.time),minimumBodyRatio:round(m.minimum),maximumStep:round(m.maxStep)});
    }
    return {outcomes};
  });
  test('All three new fortresses accept a real ram approach breach and infantry passage',()=>{
    const outcomes=[];
    for(const id of ['imperialwall','floodgate','highcitadel']){
      const map=D.MAPS.find(m=>m.id===id),sourceGate=map.regions.find(r=>r.type==='gate'),y=sourceGate.y+sourceGate.h/2;
      // Small cohorts exercise the authored obstacle itself, without changing
      // its health, wall geometry, bridge width, or body movement rules.
      const ramStart=id==='floodgate'?4500:sourceGate.x-550,footStart=id==='floodgate'?4200:sourceGate.x-800;
      const e=new Engine(configuration(id,[[0,'swordsman',24,footStart,y],[0,'batteringRam',1,ramStart,y],[1,'guardian',1,map.width-800,map.height-700]]));
      const foot=e.regiments.find(r=>r.type==='swordsman'),machine=e.regiments.find(r=>r.type==='batteringRam'),ram=e.units[machine.unitIds[0]],gate=e.structures.find(s=>s.type==='gate'),m={minimum:Infinity,maxStep:0};
      e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();e.siege.command([machine.id],gate.id);e.command([foot.id],'move',gate.x+gate.w+500,y);
      let firstStrike=null,breachedAt=null,crossed=0;
      for(let tick=0;tick<150*30&&e.phase==='running';tick++){
        const hp=gate.hp;step(e,m);
        if(gate.hp<hp&&firstStrike===null)firstStrike=e.time;if(gate.breached&&breachedAt===null)breachedAt=e.time;
        crossed=alive(e,foot).filter(u=>u.x>gate.x+gate.w+u._radius).length;if(crossed===24)break;
      }
      assert(firstStrike>2&&ram.travelDistance>300,id+' ram did not physically approach');assert(breachedAt!==null,id+' gate never opened');assert(crossed>=22,id+' breach stranded the assault column');
      outcomes.push({map:id,firstStrike:round(firstStrike),breachedAt:round(breachedAt),crossed,ramTravel:round(ram.travelDistance),minimumBodyRatio:round(m.minimum),maximumStep:round(m.maxStep)});
    }
    return {outcomes};
  });
  test('New rampart and mountain walls support physically deployed tower passages',()=>{
    const outcomes=[];
    for(const id of ['imperialwall','highcitadel']){
      const map=D.MAPS.find(m=>m.id===id),wall=map.regions.find(r=>r.type==='wall'&&r.y>2200),y=wall.y+wall.h/2;
      const e=new Engine(configuration(id,[[0,'swordsman',20,wall.x-390,y],[0,'siegeTower',1,wall.x-230,y],[1,'guardian',1,map.width-800,map.height-700]]));
      const foot=e.regiments.find(r=>r.type==='swordsman'),machine=e.regiments.find(r=>r.type==='siegeTower'),tower=e.units[machine.unitIds[0]],target=e.structures.find(s=>s.x===wall.x&&s.y===wall.y),m={minimum:Infinity,maxStep:0};
      const start={x:tower.x,y:tower.y};e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();e.siege.command([machine.id],target.id);e.command([foot.id],'move',wall.x+wall.w+450,y);
      let deployedAt=null,crossed=0,deck=0;
      for(let tick=0;tick<90*30&&e.phase==='running';tick++){
        step(e,m);if(e.siege.ramps.length&&deployedAt===null)deployedAt=e.time;
        for(const u of alive(e,foot))deck=Math.max(deck,e.siege.heightAt(u.x,u.y)||0);
        crossed=alive(e,foot).filter(u=>u.x>wall.x+wall.w+u._radius).length;if(crossed===20)break;
      }
      assert(deployedAt!==null&&deployedAt>5,id+' tower did not take time to approach and deploy');assert(Math.hypot(tower.x-start.x,tower.y-start.y)>100,id+' tower deployed remotely');
      assert(target.hp>0&&crossed>=18&&deck>3,id+' tower failed to provide a real elevated infantry passage');
      outcomes.push({map:id,deployedAt:round(deployedAt),crossed,deckHeight:round(deck),minimumBodyRatio:round(m.minimum),maximumStep:round(m.maxStep)});
    }
    return {outcomes};
  });
}
if(require.main===module){
  const root=process.argv[2]||path.resolve(__dirname,'..');global.window=global;
  for(const name of ['data','tactics','impacts','siege','engine']){const file=path.join(root,'src',name+'.js');if(fs.existsSync(file))require(file);}
  const results=[],filter=(process.env.IRONFIELD_TEST_FILTER||'').toLowerCase();
  register((name,body)=>{if(filter&&!name.toLowerCase().includes(filter))return;const start=performance.now();try{const detail=body();results.push({name,passed:true,milliseconds:round(performance.now()-start),...detail});console.log('PASS '+name+' '+JSON.stringify(detail));}catch(error){results.push({name,passed:false,error:error.message});console.error('FAIL '+name+' '+error.stack);process.exitCode=1;}});
  console.log(JSON.stringify({passed:results.filter(r=>r.passed).length,failed:results.filter(r=>!r.passed).length,results},null,2));
}
module.exports=register;
