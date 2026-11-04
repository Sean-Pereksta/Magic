'use strict';
const assert=require('node:assert/strict');
const DT=1/30;
const round=n=>Math.round(n*1000)/1000;
function register(test){
  const D=global.GameData,Engine=global.BattleEngine;
  function setup(regions,foot,horse){
    const groups=[{team:0,type:'swordsman',count:2,x:foot.x,y:foot.y,facing:foot.facing||0,formation:'line'},
      {team:0,type:'cavalry',count:2,x:horse.x,y:horse.y,facing:horse.facing||0,formation:'line'},
      {team:1,type:'guardian',count:1,x:15000,y:10000,formation:'line'}];
    const e=new Engine({map:'custom',seed:401731,customMap:{name:'Physical fortress navigation trial',width:16000,height:11000,biome:'grassland',regions},
      armies:[{name:'Moving company',doctrine:'balanced',roster:{swordsman:2,cavalry:2}},{name:'Distant observer',doctrine:'defensive',roster:{guardian:1}}],formations:groups});
    const infantry=e.regiments.find(r=>r.type==='swordsman'),cavalry=e.regiments.find(r=>r.type==='cavalry');
    e.command(e.regiments.filter(r=>r.team===1).map(r=>r.id),'hold');e.start();
    return{e,infantry,cavalry,actors:[...infantry.unitIds,...cavalry.unitIds],maximumStep:0,minimumBodyRatio:Infinity};
  }
  function move(s,footGoal,horseGoal=footGoal){
    assert.equal(s.e.command([s.infantry.id],'move',footGoal.x,footGoal.y),1);
    assert.equal(s.e.command([s.cavalry.id],'move',horseGoal.x,horseGoal.y),1);
  }
  function step(s){
    const e=s.e,before=s.actors.map(id=>({id,x:e.units[id].x,y:e.units[id].y}));e.step(DT);
    for(const p of before){const u=e.units[p.id];assert(u.alive&&!u.routed,'A navigation company unexpectedly lost a soldier');assert([u.x,u.y,u.angle,u.hp].every(Number.isFinite),'A moving body became nonfinite');assert(u.x>=u._radius&&u.y>=u._radius&&u.x<=e.width-u._radius&&u.y<=e.height-u._radius,'A moving body left the field');assert(!e._bodyBlocked(u.x,u.y,u._radius),'A moving body entered a closed wall, building, or deep water');
      const distance=Math.hypot(u.x-p.x,u.y-p.y);s.maximumStep=Math.max(s.maximumStep,distance);assert(distance<=D.TYPES[u.type].speed*DT*1.8+.25,'A body teleported instead of moving through a real passage');
      // A body is circular. The engine's padded AABB clearance can reject a
      // harmless rounded-corner sweep even when the circle never hits solid
      // ground; test the actual radius here and strict engine clearance above.
      for(let sample=0;sample<=5;sample++){
        const f=sample/5,x=p.x+(u.x-p.x)*f,y=p.y+(u.y-p.y)*f,radius=u._radius-.1;
        for(const block of e._blocks){
          if(block.open||block.breached)continue;
          const gap=Math.hypot(Math.max(block.x-x,0,x-block.x-block.w),Math.max(block.y-y,0,y-block.y-block.h));
          if(gap>=radius)continue;
          const openings=block.type==='water'?e._bridges:block.passages||[];
          const clear=openings.some(r=>x>=r.x+radius&&x<=r.x+r.w-radius&&y>=r.y+radius&&y<=r.y+r.h-radius);
          assert(clear,'A movement sweep crossed solid terrain or left its causeway: '+JSON.stringify({trial:s.label,time:e.time,type:u.type,id:u.id,from:[p.x,p.y],to:[u.x,u.y],radius:u._radius,sample:f}));
        }
      }
    }
    for(let a=0;a<before.length;a++)for(let b=0;b<a;b++){
      const p=before[a],q=before[b],u=e.units[p.id],v=e.units[q.id],dx=p.x-q.x,dy=p.y-q.y,vx=u.x-p.x-v.x+q.x,vy=u.y-p.y-v.y+q.y,vv=vx*vx+vy*vy;
      const f=vv?Math.max(0,Math.min(1,-(dx*vx+dy*vy)/vv)):0,ratio=Math.hypot(dx+vx*f,dy+vy*f)/(u._radius+v._radius);s.minimumBodyRatio=Math.min(s.minimumBodyRatio,ratio);assert(ratio>=.90,'Infantry and horses passed through one another');
    }
  }
  function until(s,predicate,seconds){for(let tick=0;tick<seconds*30&&s.e.phase==='running';tick++){step(s);if(predicate())return true;}return predicate();}
  function arrived(s,footGoal,horseGoal=footGoal){return [s.infantry,s.cavalry].every((r,i)=>r.unitIds.every(id=>Math.hypot(s.e.units[id].x-[footGoal,horseGoal][i].x,s.e.units[id].y-[footGoal,horseGoal][i].y)<95));}
  function openGates(s,predicate=()=>true){const gates=s.e.structures.filter(g=>g.type==='gate'&&predicate(g));assert(gates.length,'No physical gate matched the trial');for(const g of gates)assert(s.e.siege.setGateOpen(g.id,true,g.team),'The test could not open its owned gate');return gates.length;}
  function result(s,extra={}){return{seconds:round(s.e.time),maximumStep:round(s.maximumStep),minimumBodyRatio:round(s.minimumBodyRatio),...extra};}
  test('Every new fortress style carries actual infantry and cavalry through an opened physical entrance',()=>{
    const outcomes=[];
    for(const kind of ['palisade','curtain','moated','star','concentric']){
      const spec=D.FORTRESS_TYPES[kind],x=4200,y=2400,w=spec.defaultWidth,h=spec.defaultHeight,cy=y+h/2;
      const s=setup(D.createFortress(kind,x,y,w,h,{team:1}),{x:x-700,y:cy-75},{x:x-800,y:cy+75});
      const footGoal={x:kind==='concentric'?x+w/2:x+750,y:cy-75},horseGoal={x:footGoal.x,y:cy+75};
      const gates=openGates(s,g=>g.h>g.w&&g.x<x+w/2);assert.equal(gates,kind==='concentric'?2:1,'Expected both concentric west gates or one outer west gate');
      move(s,footGoal,horseGoal);assert(until(s,()=>arrived(s,footGoal,horseGoal),130),kind+' stranded an infantry or cavalry company at an open entrance');
      outcomes.push(result(s,{kind,gatesCrossed:gates,infantry:2,cavalry:2}));
    }
    return outcomes;
  });
  test('All four moated causeways provide real infantry and cavalry crossings without water tunneling',()=>{
    const outcomes=[],x=4200,y=2600,w=3200,h=3200,cx=x+w/2,cy=y+h/2;
    const sides=[{side:'west',start:{x:x-700,y:cy},goal:{x:x+650,y:cy},vertical:false,facing:0},{side:'east',start:{x:x+w+700,y:cy},goal:{x:x+w-650,y:cy},vertical:false,facing:Math.PI},
      {side:'north',start:{x:cx,y:y-700},goal:{x:cx,y:y+650},vertical:true,facing:Math.PI/2},{side:'south',start:{x:cx,y:y+h+700},goal:{x:cx,y:y+h-650},vertical:true,facing:-Math.PI/2}];
    for(const q of sides){
      const foot={x:q.start.x+(q.vertical?-70:0),y:q.start.y+(q.vertical?0:-70),facing:q.facing},horse={x:q.start.x+(q.vertical?70:0),y:q.start.y+(q.vertical?0:70),facing:q.facing};
      const fg={x:q.goal.x+(q.vertical?-70:0),y:q.goal.y+(q.vertical?0:-70)},hg={x:q.goal.x+(q.vertical?70:0),y:q.goal.y+(q.vertical?0:70)};
      const s=setup(D.createFortress('moated',x,y,w,h,{team:1}),foot,horse);s.label='moat '+q.side;openGates(s);move(s,fg,hg);
      assert(until(s,()=>arrived(s,fg,hg),100),q.side+' causeway stranded real troops '+JSON.stringify([s.infantry,s.cavalry].map(r=>({type:r.type,center:[r.x,r.y],target:[r.targetX,r.targetY],bridge:r._bridge,path:r._path,units:r.unitIds.map(id=>[s.e.units[id].x,s.e.units[id].y,s.e.units[id]._localPath])}))));outcomes.push(result(s,{side:q.side,infantry:2,cavalry:2}));
    }
    return outcomes;
  });
  test('Opening a closed gate replans the same accepted Move order and preserves its destination',()=>{
    const x=4200,y=2600,w=2800,h=2800,cy=y+h/2,s=setup(D.createFortress('curtain',x,y,w,h,{team:1}),{x:x-500,y:cy-70},{x:x-650,y:cy+70});
    const fg={x:x+650,y:cy-70},hg={x:x+650,y:cy+70};move(s,fg,hg);const epochs=[s.infantry._commandEpoch,s.cavalry._commandEpoch],revision=s.e.terrainRevision;
    until(s,()=>false,20);assert(s.actors.every(id=>s.e.units[id].x<x),'A company crossed the closed castle gate');assert.equal(s.infantry.order,'move');assert.equal(s.cavalry.order,'move');
    openGates(s,g=>g.h>g.w&&g.x<x+w/2);assert(s.e.terrainRevision>revision);assert.deepEqual([s.infantry._commandEpoch,s.cavalry._commandEpoch],epochs,'Opening a gate replaced the accepted troop order');assert.equal(s.infantry.targetX,fg.x);assert.equal(s.cavalry.targetX,hg.x);
    assert(until(s,()=>arrived(s,fg,hg),80),'Troops did not resume through the newly opened gate');return result(s,{ordersPreserved:true});
  });
  test('Houses watchtowers and barricades cause physical detours while keeping the chosen destination',()=>{
    const outcomes=[];
    for(const [kind,w,h]of [['house',500,600],['watchtower',240,240],['barricade',100,700]]){
      const x=4400,y=4200,cy=y+h/2,s=setup(D.createBattlefieldStructure(kind,x,y,w,h,{team:1}),{x:x-450,y:cy-70},{x:x-600,y:cy+70});s.label=kind;
      const fg={x:x+w+550,y:cy-70},hg={x:x+w+550,y:cy+70};move(s,fg,hg);let excursion=0;
      assert(until(s,()=>{for(const id of s.actors)excursion=Math.max(excursion,Math.abs(s.e.units[id].y-cy));return arrived(s,fg,hg);},100),kind+' stranded a moving company');
      assert(excursion>h/2,'Bodies did not route around the solid '+kind);outcomes.push(result(s,{kind,maximumSidewaysDetour:round(excursion)}));
    }
    return outcomes;
  });
  test('A destroyed earthwork opens a physical passage for the existing infantry and cavalry order',()=>{
    const x=4400,cy=5500,wall={type:'wall',structureKind:'earthwork',name:'Breachable line',team:1,x,y:0,w:160,h:11000,hp:600};
    const s=setup([wall],{x:x-350,y:cy-70},{x:x-500,y:cy+70}),fg={x:x+650,y:cy-70},hg={x:x+650,y:cy+70};move(s,fg,hg);until(s,()=>false,15);
    assert(s.actors.every(id=>s.e.units[id].x<x),'Troops crossed an intact earthwork');const epochs=[s.infantry._commandEpoch,s.cavalry._commandEpoch],structure=s.e.structures[0];
    assert(s.e.siege.damage(structure,structure.hp+1,s.e.units[s.actors[0]])>0);assert(structure.breached);assert.deepEqual([s.infantry._commandEpoch,s.cavalry._commandEpoch],epochs);
    assert(until(s,()=>arrived(s,fg,hg),70),'Breaching a structure failed to release the existing movement route');return result(s,{breached:true,ordersPreserved:true});
  });
}
module.exports=register;
if(require.main===module){global.window=global;for(const name of ['data','tactics','impacts','siege','engine'])require('../src/'+name+'.js');const filter=(process.env.IRONFIELD_TEST_FILTER||'').toLowerCase(),results=[];register((name,fn)=>{if(filter&&!name.toLowerCase().includes(filter))return;const start=performance.now();try{const result=fn();results.push({name,passed:true,milliseconds:round(performance.now()-start)});console.log('PASS '+name+' '+JSON.stringify(result));}catch(error){results.push({name,passed:false,error:error.message});console.error('FAIL '+name+'\n'+error.stack);process.exitCode=1;}});console.log(JSON.stringify(results));}
