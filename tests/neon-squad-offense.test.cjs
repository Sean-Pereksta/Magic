'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const C=require('../game/neon-arena/neon-conquest.js'),AI=require('../game/neon-arena/neon-tactical-ai.js');
const actor=(id,x=0,z=0,team=1)=>({entityId:id,x,z,y:1.86,team,yaw:0,faceYaw:0,hp:100,maxHp:100,r:.52,style:'rifler',onGround:true,vx:0,vz:0});
const map=()=>({sectors:[{id:'A',x:0,z:40,links:['cat_hq','B']},{id:'B',x:0,z:80,links:['A','C']},{id:'C',x:0,z:120,links:['B','mouse_hq']}],hq:{1:{x:0,z:0,y:1.86},2:{x:0,z:160,y:1.86}}});
const troops=(team=1,n=8)=>Array.from({length:n},(_,i)=>actor('unit-'+i,(i-3.5)*.8,team===1?0:160,team));
const orders=(state,list)=>list.map((unit,i)=>C.assignment(state,unit,i));
function advance(state,list,seconds,clear){for(let i=0;i<seconds*10;i++)C.step(state,.1,list,clear);}

test('both factions form stable fireteams and release a rally after a physical friendly quorum arrives',()=>{
  for(const team of [1,2]){
    const state=C.create(map()),list=troops(team);C.step(state,.1,list);
    const first=orders(state,list),rally=first[0].rally;
    assert.ok(first.every(order=>order.phase==='assemble'&&order.operation===first[0].operation&&order.leader==='unit-0'));
    assert.deepEqual(first.map(order=>order.fireteam),[0,1,0,1,0,1,0,1]);
    for(let i=0;i<5;i++)Object.assign(list[i],rally);
    advance(state,list,2.1);
    assert.ok(orders(state,list).every(order=>order.phase==='assault'));
    assert.equal(orders(state,list)[0].operation,first[0].operation,'release preserves the committed operation');
  }
});

test('rally readiness rejects wrong floors and blocked rays, but a missing straggler can never deadlock an assault',()=>{
  const state=C.create(map()),list=troops();C.step(state,.1,list);const rally=orders(state,list)[0].rally;
  for(const unit of list)Object.assign(unit,rally,{y:rally.y+5});
  advance(state,list,2.1);assert.equal(orders(state,list)[0].phase,'assemble');
  for(const unit of list)unit.y=rally.y;
  advance(state,list,2.1,()=>false);assert.equal(orders(state,list)[0].phase,'assemble');
  advance(state,list,5,()=>false);assert.equal(orders(state,list)[0].phase,'assault');
});

test('severe casualties trigger one brief regroup, replacements release it early, and capture presence bypasses it',()=>{
  const state=C.create(map()),list=troops();C.step(state,.1,list);advance(state,list,9);
  for(let i=3;i<8;i++)list[i].dead=true;
  advance(state,list,2.1);assert.equal(orders(state,list.slice(0,3))[0].phase,'regroup');
  for(let i=3;i<6;i++)list[i].dead=false;
  advance(state,list,2.1);assert.equal(orders(state,list.slice(0,6))[0].phase,'assault');
  for(let i=3;i<6;i++)list[i].dead=true;
  advance(state,list,6);assert.equal(orders(state,list.slice(0,3))[0].phase,'assault','the same losses cannot cause endless retreats');
  const holding=C.create(map()),guards=troops();C.step(holding,.1,guards);
  for(let i=1;i<8;i++)guards[i].dead=true;
  Object.assign(guards[0],{x:0,z:40});advance(holding,guards,2.1);
  assert.equal(C.assignment(holding,guards[0]).phase,'assault','an active capturer stays on the objective');
});

test('a public lost-flag counterattack launches immediately instead of waiting at a rally',()=>{
  const state=C.create(map()),list=troops();state.sectors[0].owner=1;C.step(state,.1,list);
  state.sectors[0].owner=0;C.step(state,.1,list);
  assert.ok(orders(state,list).every(order=>order.intent==='counterattack'&&order.phase==='assault'));
});

function squadFixture(options={}){
  let now=0,clear=true;
  const units=Array.from({length:4},(_,i)=>actor('unit-'+i,(i-1.5)*4)),enemy=actor('enemy',0,32,2),actors=[...units,enemy];
  const order={x:0,y:1.86,z:60,radius:6,role:'attack',intent:'capture',strategic:true,squad:'one',operation:'one:A:0',phase:'assault'};
  const host={time:()=>now,random:()=>.5,actors:()=>actors,sameTeam:(a,b)=>a.team===b.team,lineClear:()=>clear,canTraverse:()=>true,pointInfo:()=>({walkable:true}),navigation:()=>[],objective:a=>({...order,slot:units.indexOf(a),fireteam:units.indexOf(a)%2}),...options.host};
  const controller=AI.createController(host);
  return {units,enemy,actors,order,host,controller,get now(){return now;},visible:value=>{clear=value;},tick(dt=.35){now+=dt;return units.filter(a=>!a.dead).map(a=>{const decision=controller.update(a,dt);a.yaw=a.faceYaw=decision.yaw;return decision;});}};
}

test('one fireteam covers a visible contact while the other advances, then they exchange roles within seconds',()=>{
  const f=squadFixture();f.tick();f.tick();let decisions=f.tick();
  assert.deepEqual(decisions.map(d=>d.tactic),['suppress','bound','suppress','bound']);
  assert.ok(decisions.filter((_,i)=>i%2===0).every(d=>d.move.x===0&&d.move.z===0&&d.aim.fire));
  assert.ok(decisions.filter((_,i)=>i%2===1).every(d=>d.move.z>.8&&d.goal.z>55));
  while(f.now<3.6)decisions=f.tick();
  assert.deepEqual(decisions.map(d=>d.tactic),['bound','suppress','bound','suppress']);
});

test('covering fire uses a longer burst and still has a real pause',()=>{
  const f=squadFixture();f.tick();f.tick();f.tick();const unit=f.units[0];
  for(let i=0;i<AI.DIFFICULTY.normal.burst;i++)f.controller.recordShot(unit);
  assert.equal(f.controller.update(unit,.01).aim.fire,true,'covering fire continues through the ordinary burst length');
  f.controller.recordShot(unit);f.controller.recordShot(unit);
  assert.equal(f.controller.update(unit,.01).aim.fire,false,'covering fire cannot become a continuous perfect stream');
  assert.equal(f.tick(.4)[0].aim.fire,true);
});

test('covering soldiers choose a reachable exposed corner of real cover rather than shooting through it',()=>{
  const box={x:0,z:5,y:1.5,w:8,d:2,h:3,structure:{active:true}};
  const hit=(a,b,pad=0)=>{
    let enter=0,exit=1;
    for(const axis of ['x','z']){
      const half=(axis==='x'?box.w:box.d)/2+pad,delta=b[axis]-a[axis];
      if(Math.abs(delta)<1e-9){if(a[axis]<box[axis]-half||a[axis]>box[axis]+half)return false;continue;}
      const p=(box[axis]-half-a[axis])/delta,q=(box[axis]+half-a[axis])/delta;
      enter=Math.max(enter,Math.min(p,q));exit=Math.min(exit,Math.max(p,q));if(enter>exit)return false;
    }
    return exit>=0&&enter<=1;
  };
  const f=squadFixture({host:{coverBlocks:()=>[box],lineClear:(ax,ay,az,bx,by,bz)=>!hit({x:ax,z:az},{x:bx,z:bz}),canTraverse:(a,b)=>!hit(a,b,.58),pointInfo:(x,z)=>({walkable:!(Math.abs(x-box.x)<box.w/2+.58&&Math.abs(z-box.z)<box.d/2+.58)})}});
  f.units.forEach((a,i)=>{a.x=7+i*2;});f.tick();f.tick();const d=f.tick()[0];
  const cover=f.controller.findCover(f.units[0],f.enemy,f.order);assert.ok(cover);
  assert.ok(hit(cover.hide,f.enemy),'the hiding point is genuinely occluded');assert.ok(!hit(cover.peek,f.enemy),'the firing corner really sees the target');
  assert.equal(d.tactic,'suppress');assert.deepEqual(d.goal,cover.peek);assert.ok(f.host.canTraverse(cover.hide,cover.peek));
});

test('an isolated or depleted fireteam keeps advancing instead of waiting for nonexistent covering partners',()=>{
  const f=squadFixture();f.units[2].dead=f.units[3].dead=true;f.tick();f.tick();
  assert.ok(f.tick().every(d=>d.tactic==='push'&&d.move.z>.8));
  const full=squadFixture();full.tick();full.tick();full.tick();
  full.units[1].reload=full.units[3].reload=2;
  const d=full.tick();assert.equal(d[0].tactic,'push');assert.equal(d[2].tactic,'push');
});

test('capturers do not abandon public territory to take their turn as a covering team',()=>{
  const f=squadFixture();f.order.z=0;f.units.forEach((a,i)=>{a.x=(i-1.5)*1.5;});
  for(let i=0;i<24;i++)for(const d of f.tick()){
    assert.ok(Math.hypot(d.goal.x,d.goal.z)<f.order.radius);assert.equal(d.move.x,0);assert.equal(d.move.z,0);assert.notEqual(d.tactic,'suppress');
  }
});

test('breaking personal LOS ends covering fire immediately and hidden motion never updates the remembered target',()=>{
  const f=squadFixture();f.tick();f.tick();f.tick();f.visible(false);
  f.enemy.x=70;f.enemy.z=100;
  for(const d of f.tick(.01)){assert.equal(d.enemy,null);assert.equal(d.aim,null);assert.equal(d.memory.x,0);assert.equal(d.memory.z,32);}
  f.enemy.x=-100;f.enemy.z=-70;
  for(const d of f.tick()){assert.equal(d.enemy,null);assert.equal(d.aim,null);assert.equal(d.memory.x,0);assert.equal(d.memory.z,32);}
});

test('a squadmate sight report cannot authorize covering fire for a soldier with an occluded personal view',()=>{
  const f=squadFixture({host:{lineClear:ax=>ax>0}});f.tick();f.tick();const d=f.tick();
  assert.equal(d[0].enemy,null);assert.equal(d[0].aim,null);assert.equal(d[0].memory.source,'team');
  assert.equal(d[1].enemy,null);assert.equal(d[1].aim,null);
  assert.equal(d[2].enemy,f.enemy);assert.equal(d[2].tactic,'suppress');
});

test('a friendly crossing the muzzle blocks shots and causes a short lateral step without leaving a capture circle',()=>{
  const f=squadFixture();f.units[0].x=0;f.units[0].z=0;f.order.z=0;
  f.units[1].x=0;f.units[1].z=6;f.units[2].dead=f.units[3].dead=true;
  f.tick();let d=f.tick()[0];assert.equal(d.aim.fire,false);
  d=f.tick()[0];assert.equal(d.tactic,'clear-lane');assert.ok(Math.abs(d.goal.x)>2);assert.ok(Math.hypot(d.goal.x,d.goal.z)<f.order.radius);
  f.units[1].y=8;
  assert.equal(f.tick()[0].aim.fire,true,'a friendly on a separate floor does not block the muzzle ray');
});

test('rally formation splits lateral positions and preserves real traversal failures',()=>{
  const f=squadFixture({host:{canTraverse:()=>false}});f.visible(false);
  Object.assign(f.order,{phase:'assemble',phaseUntil:10,rally:{x:0,y:1.86,z:24},approachYaw:0});
  const d=f.tick();assert.ok(d[0].goal.x<0&&d[1].goal.x>0);
  assert.ok(d.every(result=>result.move.x===0&&result.move.z===0),'no squad order grants passage through a solid obstruction');
});
