const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

class Vector3{
  constructor(x=0,y=0,z=0){this.set(x,y,z);}
  set(x,y,z){this.x=x;this.y=y;this.z=z;return this;}
  setY(y){this.y=y;return this;}
  clone(){return new Vector3(this.x,this.y,this.z);}
  copy(v){return this.set(v.x,v.y,v.z);}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  sub(v){return this.addScaledVector(v,-1);}
  length(){return Math.hypot(this.x,this.y,this.z);}
  normalize(){const length=this.length()||1;this.x/=length;this.y/=length;this.z/=length;return this;}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
  lerp(v,t){this.x+=(v.x-this.x)*t;this.y+=(v.y-this.y)*t;this.z+=(v.z-this.z)*t;return this;}
}

function fixture(withCourtReads=true){
  const ctx=vm.createContext({Math,THREE:{Vector3,MathUtils:{clamp:(v,a,b)=>Math.max(a,Math.min(b,v))}},
    HOOP_POS:new Vector3(0,3.28,0),teamAITime:1,aiDefensePlans:{},shotCharging:false,chargingPlayer:null,
    flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),
    keepInCourt(v){v.x=Math.max(-8.2,Math.min(8.2,v.x));v.z=Math.max(-1.4,Math.min(23,v.z));},
    aiIsHuman:player=>!!player.controllerSlot,
    getForward:a=>new Vector3(Math.sin(a),0,Math.cos(a))});
  function player(id,teamId,x,z,controllerSlot=null){
    const position=new Vector3(x,0,z);
    return {id,teamId,controllerSlot,group:{position},velocity:new Vector3(),profile:{shot:1},angle:Math.PI,
      aiBrain:{target:position.clone(),role:'space',roleUntil:0,moveCooldown:0,sprint:false,protect:false}};
  }
  ctx.handler=player('handler',0,2.8,5.4,'p1');ctx.handler.velocity.set(0,0,-3);
  ctx.support=[player('left',0,-4.8,4.5),player('right',0,5.8,4.2)];
  ctx.defenders=[player('onball',1,4.3,5.2),player('weak',1,-5.8,7),player('strong',1,6.8,7)];
  ctx.roster=[ctx.handler,...ctx.support,...ctx.defenders];
  ctx.aiRoster=()=>ctx.roster;
  ctx.getPlayerById=id=>ctx.roster.find(player=>player.id===id);
  ctx.getOpponents=char=>ctx.roster.filter(player=>player.teamId!==char.teamId);
  ctx.aiAssignedOpponent=char=>[ctx.handler,...ctx.support][ctx.defenders.indexOf(char)];
  ctx.plan={style:'drive_kick',phase:'drive',actorId:ctx.support[0].id};
  ctx.settings={offensiveIQ:1,spacingIQ:1,defensiveIQ:1,courtVision:1,lookAhead:.4};
  if(withCourtReads) vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/ai-reads.js'),'utf8'),ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/ai-coordination.js'),'utf8'),ctx);
  ctx.resetTargets=()=>{
    for(const defender of ctx.defenders){
      const assigned=ctx.aiAssignedOpponent(defender);
      defender.aiBrain.role='guard';
      defender.aiBrain.target.copy(assigned.group.position).addScaledVector(ctx.HOOP_POS.clone().sub(assigned.group.position).setY(0).normalize(),1.8);
    }
  };
  ctx.coordinate=(reverse=false)=>{
    for(const player of reverse?[...ctx.support].reverse():ctx.support)
      ctx.aiCoordinateOffBall(player,player.aiBrain,ctx.handler,ctx.settings,ctx.plan);
  };
  return ctx;
}

test('high IQ creates exactly one open dump-off pocket and retains a weak-side outlet regardless of update order',()=>{
  const selections=[];
  for(const reverse of [false,true]){
    const ctx=fixture();ctx.coordinate(reverse);
    const receivers=ctx.support.filter(player=>player.aiBrain.role==='dump_off');
    assert.equal(receivers.length,1);
    const receiver=receivers[0],target=receiver.aiBrain.target;
    assert.ok(ctx.flatDistance(target,ctx.HOOP_POS)<2.8);
    assert.ok(ctx.aiCoordDistanceToLane(target,ctx.handler.group.position,ctx.HOOP_POS)>1.05,'does not block the dribbler');
    assert.ok(ctx.aiCoordLaneSpace(ctx.handler,ctx.handler.group.position,target,ctx.settings)>=1);
    assert.ok(ctx.support.filter(player=>player!==receiver).every(player=>ctx.flatDistance(player.aiBrain.target,ctx.HOOP_POS)>4));
    selections.push([receiver.id,target.x,target.z]);
    // A persistent read must not alternate cutters because the selected player
    // now has an action cooldown; reaching the spot still takes normal movement.
    ctx.teamAITime+=.4;ctx.coordinate(reverse);
    assert.equal(ctx.plan.receivingRead.selected.playerId,receiver.id);
  }
  assert.deepEqual(selections[0],selections[1]);
});

test('basic decisions stay simple; a stationary or unpressured handler does not summon a dump-off cutter',()=>{
  for(const mode of ['basic','stationary','unpressured']){
    const ctx=fixture();
    if(mode==='basic') ctx.settings.offensiveIQ=.4;
    if(mode==='stationary') ctx.handler.velocity.set(0,0,0);
    if(mode==='unpressured') ctx.defenders[0].group.position.set(7,0,9);
    const before=ctx.support.map(player=>[player.aiBrain.role,player.aiBrain.target.x,player.aiBrain.target.z]);
    ctx.coordinate();
    assert.deepEqual(ctx.support.map(player=>[player.aiBrain.role,player.aiBrain.target.x,player.aiBrain.target.z]),before);
  }
});

test('a roll uses a clear receiving pocket while screen setup and motion keep their single authored actor',()=>{
  const pick=fixture();pick.plan.style='pick_roll';pick.plan.phase='setup';
  const actor=pick.support[0];actor.aiBrain.role='screen_setup';actor.aiBrain.target.set(2,0,5);
  pick.coordinate();assert.equal(actor.aiBrain.role,'screen_setup');assert.equal(actor.aiBrain.target.z,5);
  assert.equal(pick.plan.receivingRead.selected,null);
  pick.plan.phase='roll';actor.aiBrain.role='roll';actor.aiBrain.roleUntil=3;pick.teamAITime+=.4;
  pick.coordinate();assert.equal(actor.aiBrain.role,'roll');assert.ok(pick.flatDistance(actor.aiBrain.target,pick.HOOP_POS)<2.8);
  assert.equal(pick.support[1].aiBrain.role,'space');

  const motion=fixture();motion.plan.style='motion';motion.plan.phase='cut';
  motion.support[0].aiBrain.role='cut';motion.support[0].aiBrain.roleUntil=3;
  motion.coordinate();
  assert.equal(motion.support.filter(player=>['cut','roll','dump_off'].includes(player.aiBrain.role)).length,1);
  const passCut=fixture();passCut.plan.style='motion';passCut.plan.phase='cut';
  passCut.plan.passerId=passCut.support[1].id;passCut.plan.passCutUntil=3;
  passCut.support[1].aiBrain.role='cut';passCut.support[1].aiBrain.roleUntil=3;
  passCut.coordinate();assert.equal(passCut.plan.receivingRead.selected.playerId,passCut.support[1].id);
  assert.equal(passCut.support[0].aiBrain.role,'space');
});

test('occupied rim pockets, covered catches and human support never produce an extra rim receiver',()=>{
  const occupied=fixture();occupied.support[1].controllerSlot='p2';occupied.support[1].group.position.set(-.8,0,1.2);
  occupied.coordinate();assert.equal(occupied.plan.receivingRead.selected,null);
  assert.equal(occupied.support[1].aiBrain.role,'space');
  const blocked=fixture();blocked.plan.style='motion';blocked.plan.phase='cut';
  blocked.support[0].aiBrain.role='cut';blocked.support[0].aiBrain.roleUntil=3;
  blocked.defenders[1].group.position.set(-1.4,0,1.7);blocked.defenders[2].group.position.set(1.4,0,1.7);
  blocked.coordinate();assert.equal(blocked.plan.receivingRead.selected,null);
  assert.equal(blocked.support[1].aiBrain.role,'space');
});

test('expert on-ball defense contains visible running momentum with bounded anticipation instead of speed boosts',()=>{
  const ctx=fixture(),defender=ctx.defenders[0];ctx.handler.group.position.set(-.8,0,5);ctx.handler.velocity.set(4,0,-3);ctx.resetTargets();
  const originalSpeed=defender.velocity.clone();
  assert.equal(ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings),true);
  assert.equal(defender.aiBrain.role,'contain');assert.ok(defender.aiBrain.target.x>ctx.handler.group.position.x+.3);
  assert.ok(defender.aiBrain.target.z<ctx.handler.group.position.z);
  assert.deepEqual(defender.velocity,originalSpeed);
  ctx.handler.velocity.set(400,0,-300);ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings);
  assert.ok(ctx.flatDistance(defender.aiBrain.target,ctx.handler.group.position)<3,'cannot project beyond a bounded visible step');
  const target=defender.aiBrain.target.clone();
  assert.equal(ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,{...ctx.settings,defensiveIQ:.4}),false);
  assert.deepEqual(defender.aiBrain.target,target);
});

test('assigned defenders tag rim receivers and close likely catch targets without abandoning other assignments',()=>{
  const ctx=fixture();ctx.support[0].group.position.set(-1.5,0,1.8);ctx.support[0].velocity.set(.2,0,-1.5);
  ctx.support[1].group.position.set(5.4,0,3);ctx.handler.angle=Math.atan2(5.4-2.8,3-5.4);ctx.resetTargets();
  for(const defender of ctx.defenders) ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings);
  assert.equal(ctx.defenders[1].aiBrain.role,'tag');
  assert.equal(ctx.defenders[2].aiBrain.role,'deny');
  assert.ok(ctx.flatDistance(ctx.defenders[1].aiBrain.target,ctx.support[0].group.position)<1.6);
  assert.ok(ctx.defenders[1].aiBrain.target.z<ctx.support[0].group.position.z,'tracks the visible cut toward the rim');
  assert.ok(ctx.flatDistance(ctx.defenders[2].aiBrain.target,ctx.support[1].group.position)<1.3);
  assert.ok(ctx.flatDistance(ctx.defenders[1].aiBrain.target,ctx.defenders[2].aiBrain.target)>4);
});

test('one eligible weak-side defender shows in a beaten driving gap and recovers when the drive stops',()=>{
  const ctx=fixture();ctx.handler.group.position.set(0,0,5.8);ctx.handler.velocity.set(0,0,-3);ctx.handler.angle=Math.PI;
  ctx.defenders[0].group.position.set(0,0,7.5);
  ctx.support[0].group.position.set(-4.4,0,5);ctx.support[1].group.position.set(4.4,0,5);
  ctx.defenders[1].group.position.set(-2.4,0,3.8);ctx.defenders[2].group.position.set(3.2,0,4);
  ctx.resetTargets();
  for(const defender of ctx.defenders) ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings);
  assert.equal(ctx.defenders.filter(player=>player.aiBrain.role==='gap_help').length,1);
  const helper=ctx.defenders.find(player=>player.aiBrain.role==='gap_help');
  assert.ok(ctx.flatDistance(helper.aiBrain.target,ctx.handler.group.position)>1.85);
  ctx.handler.velocity.set(0,0,0);ctx.resetTargets();
  for(const defender of ctx.defenders) ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings);
  assert.equal(ctx.defenders.filter(player=>player.aiBrain.role==='gap_help').length,0);
  ctx.handler.velocity.set(0,0,-3);ctx.settings.readError=.4;ctx.defenders[2].group.position.set(2.4,0,3.8);
  for(const reverse of [false,true]){
    ctx.resetTargets();
    for(const defender of reverse?[...ctx.defenders].reverse():ctx.defenders)
      ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings);
    assert.equal(ctx.defenders.filter(player=>player.aiBrain.role==='gap_help').length,1,'pair-specific perception errors still elect only one helper');
  }
  // A valuable shooter is not abandoned just because a lane becomes available.
  ctx.handler.velocity.set(0,0,-3);ctx.support.forEach(player=>player.profile.shot=1.2);ctx.resetTargets();
  for(const defender of ctx.defenders) ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings);
  assert.equal(ctx.defenders.filter(player=>player.aiBrain.role==='gap_help').length,0);
});

test('existing traps and human defenders retain their explicit coverage untouched',()=>{
  const ctx=fixture();ctx.resetTargets();ctx.aiDefensePlans[1]={trap:{helperId:ctx.defenders[1].id}};
  const targets=ctx.defenders.map(player=>player.aiBrain.target.clone());
  for(const [index,defender] of ctx.defenders.entries()){
    assert.equal(ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings),false);
    assert.deepEqual(defender.aiBrain.target,targets[index]);
  }
  ctx.aiDefensePlans[1].trap=null;ctx.defenders[1].controllerSlot='p2';
  assert.equal(ctx.aiCoordinateDefense(ctx.defenders[1],ctx.defenders[1].aiBrain,ctx.handler,ctx.settings),false);
  assert.deepEqual(ctx.defenders[1].aiBrain.target,targets[1]);
});

test('coordination also retains bounded decisions if optional court-read helpers are unavailable',()=>{
  const ctx=fixture(false);ctx.coordinate();assert.ok(ctx.plan.receivingRead.selected);
  ctx.handler.velocity.set(500,0,-500);ctx.resetTargets();
  const defender=ctx.defenders[0];ctx.aiCoordinateDefense(defender,defender.aiBrain,ctx.handler,ctx.settings);
  assert.ok(ctx.flatDistance(defender.aiBrain.target,ctx.handler.group.position)<3);
});
